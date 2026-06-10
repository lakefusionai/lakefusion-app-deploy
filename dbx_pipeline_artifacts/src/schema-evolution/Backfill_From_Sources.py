# Databricks notebook source
import json

# COMMAND ----------

# MAGIC %run ../utils/taskvalues_enum

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("se_job_id", "", "Schema Evolution Job ID")
dbutils.widgets.text("entity_id", "", "Entity ID")
dbutils.widgets.text("catalog_name", "", "Catalog Name")

# COMMAND ----------

# DBTITLE 1,Get parameters
catalog_name = dbutils.widgets.get("catalog_name")
experiment_id = "prod"  # Schema evolution always runs against prod

# COMMAND ----------

# DBTITLE 1,Get task values from Parse step
edits_json = dbutils.jobs.taskValues.get(
    taskKey="Parse_Schema_Evolution_JSON",
    key=TaskValueKey.SCHEMA_EVOLUTION_EDITS.value,
    debugValue="[]"
)

master_table = dbutils.jobs.taskValues.get(
    taskKey="Parse_Schema_Evolution_JSON",
    key=TaskValueKey.SCHEMA_EVOLUTION_MASTER_TABLE.value,
    debugValue=""
)

unified_table = dbutils.jobs.taskValues.get(
    taskKey="Parse_Schema_Evolution_JSON",
    key=TaskValueKey.SCHEMA_EVOLUTION_UNIFIED_TABLE.value,
    debugValue=""
)

id_key = dbutils.jobs.taskValues.get(
    taskKey="Parse_Schema_Evolution_JSON",
    key=TaskValueKey.SCHEMA_EVOLUTION_ID_KEY.value,
    debugValue="lakefusion_id"
)

# COMMAND ----------

# MAGIC %run ../utils/execute_utils

# COMMAND ----------

setup_lakefusion_engine()

# COMMAND ----------

# DBTITLE 1,Parse edits and log configuration
edits = json.loads(edits_json) if isinstance(edits_json, str) else edits_json

logger.info("=" * 80)
logger.info("SCHEMA EVOLUTION - BACKFILL FROM SOURCES")
logger.info("=" * 80)
logger.info(f"Master Table: {master_table}")
logger.info(f"Unified Table: {unified_table}")
logger.info(f"ID Key: {id_key}")
logger.info(f"Edits count: {len(edits)}")
logger.info("=" * 80)

# COMMAND ----------

# DBTITLE 1,Pre-process edits: collect attributes, build SVR config, group by source
add_edits = [e for e in edits if e.get('action_type') == 'ADD_ATTRIBUTE']

if not add_edits:
    logger.info("No ADD_ATTRIBUTE edits to process. Skipping backfill.")
    logger_instance.shutdown()
    dbutils.notebook.exit("SKIPPED: No ADD_ATTRIBUTE edits to process")

all_attr_names = []
all_attr_types = {}
svr_config = []
dataset_attrs = {}  # { dataset_path: { attr_name: info } }

for edit in add_edits:
    attr_name = edit['attribute_name']
    attr_type = edit['attribute_type'].upper()
    all_attr_names.append(attr_name)
    all_attr_types[attr_name] = attr_type

    survivorship_rule = edit.get('survivorship_rule')
    if survivorship_rule:
        svr_config.append({
            "attribute": attr_name,
            "strategy": survivorship_rule.get('strategy'),
            "strategyRule": survivorship_rule.get('strategy_rule'),
            "ignoreNull": survivorship_rule.get('ignore_null', False),
        })

# Group ALL source_mappings by dataset_path across all edits
for edit in add_edits:
    attr_name = edit['attribute_name']
    attr_type = edit['attribute_type'].upper()
    for mapping in edit.get('source_mappings', []):
        dp = mapping['dataset_path']
        dataset_attrs.setdefault(dp, {})[attr_name] = {
            'dataset_attribute': mapping['dataset_attribute'],
            'dataset_primary_key': mapping['dataset_primary_key'],
            'attr_type': attr_type,
        }

logger.info(f"Attributes to backfill: {all_attr_names}")
logger.info(f"Unique source tables: {list(dataset_attrs.keys())}")
logger.info(f"SVR rules configured: {len(svr_config)}")

# COMMAND ----------

# DBTITLE 1,Phase 1: Backfill unified table from sources
from pyspark.sql.functions import col
from delta.tables import DeltaTable

logger.info(f"\nPHASE 1: Backfilling unified table from {len(dataset_attrs)} source(s)")

merge_count = 0
for dataset_path, attrs_info in dataset_attrs.items():
    dataset_pk = next(iter(attrs_info.values()))['dataset_primary_key']
    attr_names_from_source = list(attrs_info.keys())

    logger.info(f"\n  Source: {dataset_path}")
    logger.info(f"  PK: {dataset_pk}")
    logger.info(f"  Attributes: {attr_names_from_source}")

    # Build select: PK + all attribute columns from this source
    select_cols = [col(dataset_pk).cast("string").alias("_source_id")]
    update_set = {}

    for attr_name, info in attrs_info.items():
        select_cols.append(
            col(info['dataset_attribute']).cast(info['attr_type']).alias(attr_name)
        )
        update_set[attr_name] = f"s.{attr_name}"

    # Read source table ONCE, project all needed columns
    source_df = spark.read.table(dataset_path).select(*select_cols)

    # Single MERGE updates ALL attributes from this source
    DeltaTable.forName(spark, unified_table).alias("t").merge(
        source_df.alias("s"),
        f"t.source_path = '{dataset_path}' AND t.source_id = s._source_id"
    ).whenMatchedUpdate(set=update_set).execute()

    logger.info(f"  Merged {len(attr_names_from_source)} attribute(s) in single operation")
    merge_count += 1

logger.info(f"\nPhase 1 complete: {merge_count} source merge(s)")

# COMMAND ----------

# DBTITLE 1,Phase 2: SVR resolution + master table update
from pyspark.sql.functions import col, collect_list, struct, udf
from pyspark.sql.types import StructType, StructField, StringType
from delta.tables import DeltaTable
from lakefusion_core_engine.survivorship import SurvivorshipEngine

logger.info(f"\nPHASE 2: SVR resolution for {len(all_attr_names)} attribute(s) + master update")

# UDF return type: one StringType field per attribute
resolve_return_type = StructType([
    StructField(attr_name, StringType()) for attr_name in all_attr_names
])

# Closure variables for UDF serialization (must be plain Python types)
_svr_config = svr_config
_all_attr_names = all_attr_names
_all_attr_types = all_attr_types

@udf(resolve_return_type)
def resolve_svr_batch(contributors):
    """UDF: resolve ALL new attributes at once via SurvivorshipEngine."""
    engine = SurvivorshipEngine(
        survivorship_config=_svr_config,
        entity_attributes=_all_attr_names,
        entity_attributes_datatype=_all_attr_types,
        id_key="surrogate_key",
    )

    records = [row.asDict() for row in contributors]

    try:
        result = engine.apply_survivorship(unified_records=records)
        values = tuple(result.resultant_record.get(attr) for attr in _all_attr_names)
        # Return None if ALL values are None (no resolution happened)
        if all(v is None for v in values):
            return None
        return values
    except Exception:
        return None

# COMMAND ----------

# DBTITLE 1,Query unified table and apply SVR resolution
# Query unified table for ALL new attribute columns at once
attr_cols = ", ".join([f"`{a}`" for a in all_attr_names])
or_clause = " OR ".join([f"`{a}` IS NOT NULL" for a in all_attr_names])

contributors_df = spark.sql(f"""
    SELECT master_lakefusion_id, surrogate_key, source_path, {attr_cols}
    FROM {unified_table}
    WHERE master_lakefusion_id IS NOT NULL
    AND record_status = 'MERGED'
    AND ({or_clause})
""")

# Group by master, collect ALL contributor structs
struct_cols = ["surrogate_key", "source_path"] + [col(f"`{a}`") for a in all_attr_names]

resolved_df = (
    contributors_df
    .groupBy("master_lakefusion_id")
    .agg(collect_list(struct(*struct_cols)).alias("contributors"))
    .select(
        col("master_lakefusion_id").alias("lakefusion_id"),
        resolve_svr_batch(col("contributors")).alias("result"),
    )
    .filter(col("result").isNotNull())
)

# Flatten result struct into individual columns for the MERGE
select_cols = [col("lakefusion_id")]
update_set = {}
for attr_name in all_attr_names:
    select_cols.append(col(f"result.{attr_name}").alias(attr_name))
    update_set[attr_name] = f"s.{attr_name}"

merge_source = resolved_df.select(*select_cols)

logger.info(f"Resolving survivorship and updating master table...")

# Single MERGE updates ALL resolved attributes at once
DeltaTable.forName(spark, master_table).alias("t").merge(
    merge_source.alias("s"),
    "t.lakefusion_id = s.lakefusion_id"
).whenMatchedUpdate(set=update_set).execute()

logger.info(f"Updated {len(all_attr_names)} attribute(s) in single master MERGE")
logger.info(f"\nPhase 2 complete")

# COMMAND ----------

# DBTITLE 1,Summary
logger.info("\n" + "=" * 80)
logger.info("SCHEMA EVOLUTION - BACKFILL FROM SOURCES - COMPLETE")
logger.info("=" * 80)
logger.info(f"Attributes processed: {all_attr_names}")
logger.info(f"Source merges: {merge_count}")
logger.info(f"Master merges: 1")
logger.info("=" * 80)

# COMMAND ----------

# Shutdown logger
logger_instance.shutdown()
