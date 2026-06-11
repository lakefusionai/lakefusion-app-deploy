# Databricks notebook source
# DBTITLE 1,Import required libraries
import json
from pyspark.sql.functions import col, lit, udf, concat_ws, coalesce, current_timestamp, struct
from pyspark.sql.types import StringType, IntegerType, LongType, DoubleType, FloatType, BooleanType, DateType, TimestampType, ArrayType, StructType, StructField, ShortType, ByteType
from delta.tables import DeltaTable



# COMMAND ----------

def get_spark_data_type(dtype_str):
    """
    Convert string data type to Spark DataType.
    Handles both old lowercase types and new uppercase types.
    """
    dtype_map = {
        # New uppercase types (primary)
        'BIGINT': LongType(),
        'BOOLEAN': BooleanType(),
        'DATE': DateType(),
        'DOUBLE': DoubleType(),
        'FLOAT': FloatType(),
        'INT': IntegerType(),
        'SMALLINT': ShortType(),
        'STRING': StringType(),
        'TINYINT': ByteType(),
        'TIMESTAMP': TimestampType(),
        
        # Legacy lowercase types (backward compatibility)
        'bigint': LongType(),
        'boolean': BooleanType(),
        'char': StringType(),
        'varchar': StringType(),
        'date': DateType(),
        'double precision': DoubleType(),
        'double': DoubleType(),
        'integer': IntegerType(),
        'int': IntegerType(),
        'long': LongType(),
        'numeric': FloatType(),
        'real': FloatType(),
        'smallint': ShortType(),
        'text': StringType(),
        'string': StringType(),
        'timestamp': TimestampType(),
        'float': FloatType(),
        'decimal': DoubleType(),
    }
    
    # Try exact match first, then lowercase fallback
    return dtype_map.get(dtype_str, dtype_map.get(dtype_str.lower(), StringType()))

# COMMAND ----------

# DBTITLE 1,Get parameters
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("primary_table", "", "Primary Table")
dbutils.widgets.text("primary_key", "", "Primary Key")
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")
dbutils.widgets.text("entity_attributes_datatype", "", "Entity Attributes Datatype")
dbutils.widgets.text("match_attributes", "", "Match Attributes")
dbutils.widgets.text("attributes_mapping", "", "Attributes Mapping")

# COMMAND ----------

# DBTITLE 1,Get task values
entity = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="entity",
    debugValue=dbutils.widgets.get("entity")
)

catalog_name = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="catalog_name",
    debugValue=dbutils.widgets.get("catalog_name")
)

primary_table = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="primary_table",
    debugValue=dbutils.widgets.get("primary_table")
)

primary_key = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="primary_key",
    debugValue=dbutils.widgets.get("primary_key")
)

entity_attributes = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="entity_attributes",
    debugValue=dbutils.widgets.get("entity_attributes")
)

entity_attributes_datatype = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="entity_attributes_datatype",
    debugValue=dbutils.widgets.get("entity_attributes_datatype")
)

match_attributes = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="match_attributes",
    debugValue=dbutils.widgets.get("match_attributes")
)

attributes_mapping = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="attributes_mapping",
    debugValue=dbutils.widgets.get("attributes_mapping")
)

experiment_id = dbutils.widgets.get("experiment_id")

# RDM configs drive inline mapping resolution at ingestion (creates mapping
# table, resolves new source values, splits approved vs PENDING/NO_MATCH rows).
rdm_configs = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="rdm_configs",
    debugValue="[]"
)

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

# MAGIC %run ../../utils/rdm_resolver

# COMMAND ----------

# DBTITLE 1,Import core engine components
from lakefusion_core_engine.identifiers import generate_lakefusion_id, generate_surrogate_key
from lakefusion_core_engine.models import RecordStatus, ActionType

# COMMAND ----------

# DBTITLE 1,Parse JSON strings
entity_attributes = json.loads(entity_attributes)
entity_attributes_datatype = json.loads(entity_attributes_datatype)
match_attributes = json.loads(match_attributes)
attributes_mapping = json.loads(attributes_mapping)
rdm_configs = json.loads(rdm_configs) if isinstance(rdm_configs, str) else (rdm_configs or [])

# run_id used by UnifiedErrorHandler when routing PENDING / NO_MATCH rows
try:
    run_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().toString()
except Exception:
    import uuid as _uuid_mod
    run_id = str(_uuid_mod.uuid4())

from lakefusion_core_engine.services.unified_error_handler import UnifiedErrorHandler

# COMMAND ----------

# DBTITLE 1,Construct table names
experiment_suffix = f"_{experiment_id}" if experiment_id else ""

master_table = f"{catalog_name}.gold.{entity}_master{experiment_suffix}"
unified_table = f"{catalog_name}.silver.{entity}_unified{experiment_suffix}"
merge_activities_table = f"{master_table}_merge_activities"
attribute_version_sources_table = f"{master_table}_attribute_version_sources"

# COMMAND ----------

logger.info("="*60)
logger.info("LOAD PRIMARY SOURCE")
logger.info("="*60)
logger.info(f"Entity: {entity}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment: {experiment_id if experiment_id else 'prod'}")
logger.info(f"Primary Table: {primary_table}")
logger.info(f"Primary Key: {primary_key}")
logger.info(f"Entity Attributes: {len(entity_attributes)}")
logger.info("\nConstructed Table Names:")
logger.info(f"  Master: {master_table}")
logger.info(f"  Unified: {unified_table}")
logger.info(f"  Merge Activities: {merge_activities_table}")
logger.info(f"  Attribute Version Sources: {attribute_version_sources_table}")
logger.info("="*60)

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 1: READ PRIMARY SOURCE DATA")
logger.info("="*60)

# Read primary source table
primary_df = spark.read.table(primary_table)

logger.info(f"Read {primary_df.count()} records from {primary_table}")
logger.info(f"Columns: {len(primary_df.columns)}")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 2: APPLY ATTRIBUTE MAPPING WITH TYPE CASTING")
logger.info("="*60)

# Find the mapping for this primary table
primary_table_mapping = None
for mapping_entry in attributes_mapping:
    if primary_table in mapping_entry:
        primary_table_mapping = mapping_entry[primary_table]
        break

if not primary_table_mapping:
    raise ValueError(f"No attribute mapping found for primary table: {primary_table}")

logger.info(f"Found attribute mapping for {primary_table}")
logger.info(f"Mapping: {len(primary_table_mapping)} attributes")

# COMMAND ----------
# MAGIC %run ../../utils/attributes_combined

# COMMAND ----------

# MAGIC %run ../../utils/complex_type_mapping

# COMMAND ----------

# MAGIC %run ../../utils/spark_types


# COMMAND ----------

# ── complex-type projection ───────────────────────────────────
# If the entity has any STRUCT or ARRAY (is_array=true) attributes AND the
# pipeline received the full mapping payload, delegate to the centralized
# projector. It handles direct_column / subfield_assembly / scalar-to-array
# auto-wrap and raises StructSchemaMismatchError on schema mismatch.
import os as _pp_os, sys as _pp_sys
_pp_parts = _pp_os.getcwd().split(_pp_os.sep)
for _i in range(len(_pp_parts) - 1, -1, -1):
    if _pp_parts[_i] == "src":
        _src_path = _pp_os.sep.join(_pp_parts[: _i + 1])
        if _src_path not in _pp_sys.path:
            _pp_sys.path.insert(0, _src_path)
        break

try:
    _full_mapping_raw = dbutils.jobs.taskValues.get(
        taskKey="Parse_Entity_Model_JSON",
        key="attributes_mapping_full",
        debugValue="[]",
    )
    attributes_mapping_full = json.loads(_full_mapping_raw) if _full_mapping_raw else []
except Exception:
    attributes_mapping_full = []

try:
    _records_raw = dbutils.jobs.taskValues.get(
        taskKey="Parse_Entity_Model_JSON",
        key="entity_attribute_records",
        debugValue="[]",
    )
    entity_attribute_records = json.loads(_records_raw) if _records_raw else []
except Exception:
    entity_attribute_records = []

_has_complex_attrs = any(
    (rec.get("is_array") or (rec.get("type") or "").strip().upper() == "STRUCT")
    for rec in entity_attribute_records
)
_primary_full_records = None
for _entry in attributes_mapping_full:
    if primary_table in _entry:
        _primary_full_records = _entry[primary_table]
        break

if _has_complex_attrs and _primary_full_records:

    logger.info("Using complex-type projection")
    mapped_df = project_source_to_target(
        primary_df, _primary_full_records, entity_attribute_records
    )
    mapped_columns = set(mapped_df.columns)
else:
    # Legacy scalar-only projection path.
    select_exprs = []
    mapped_columns = set()

    for entity_attr, dataset_attr in primary_table_mapping.items():
        if dataset_attr in primary_df.columns:
            target_dtype_str = entity_attributes_datatype.get(entity_attr, 'string')
            target_spark_dtype = get_spark_data_type(target_dtype_str)

            source_field = [f for f in primary_df.schema.fields if f.name == dataset_attr][0]
            source_dtype = source_field.dataType

            select_exprs.append(col(dataset_attr).cast(target_spark_dtype).alias(entity_attr))
            mapped_columns.add(entity_attr)

            if str(source_dtype) != str(target_spark_dtype):
                logger.info(f"  Mapped: {dataset_attr} ({source_dtype}) -> {entity_attr} ({target_spark_dtype}) [CAST]")
            else:
                logger.info(f"  Mapped: {dataset_attr} -> {entity_attr} ({target_spark_dtype})")
        else:
            logger.warning(f"Dataset column '{dataset_attr}' not found in source table")

    logger.info(f"\nMapped {len(select_exprs)} columns")

    mapped_df = primary_df.select(*select_exprs)

# Add any missing entity attributes as NULL with CORRECT data type from entity model.
# Uses the centralized create_schema_fields helper so STRUCT / ARRAY columns
# get nested nulls of the right type.

_resolve_complex_dtype=get_complex_spark_data_type
_records_by_name = {r["name"]: r for r in entity_attribute_records if isinstance(r, dict) and r.get("name")}
missing_attrs = 0
for attr in entity_attributes:
    if attr in mapped_columns or attr == "lakefusion_id":
        continue
    rec = _records_by_name.get(attr)
    if rec and (rec.get("is_array") or (rec.get("type") or "").strip().upper() == "STRUCT"):
        spark_dtype = _resolve_complex_dtype(rec)
    else:
        dtype_str = entity_attributes_datatype.get(attr, 'string')
        spark_dtype = get_spark_data_type(dtype_str)
    mapped_df = mapped_df.withColumn(attr, lit(None).cast(spark_dtype))
    missing_attrs += 1

if missing_attrs > 0:
    logger.info(f"Added {missing_attrs} missing attributes as NULL (with correct types)")

logger.info(f"\nMapped DataFrame has {mapped_df.count()} records")
logger.info(f"Resulting columns: {mapped_df.columns}")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 3: GENERATE IDS")
logger.info("="*60)

# Create UDF for surrogate key generation
generate_lakefusion_id_udf = udf(
    lambda source_path, source_id: generate_lakefusion_id(source_path, str(source_id)), 
    StringType()
)
generate_surrogate_key_udf = udf(
    lambda source_path, source_id: generate_surrogate_key(source_path, str(source_id)), 
    StringType()
)

# Add source metadata
id_df = mapped_df \
    .withColumn("source_path", lit(primary_table)) \
    .withColumn("source_id", col(primary_key).cast("string"))

# Generate both IDs deterministically based on source
id_df = id_df \
    .withColumn("lakefusion_id", generate_lakefusion_id_udf(col("source_path"), col("source_id"))) \
    .withColumn("surrogate_key", generate_surrogate_key_udf(col("source_path"), col("source_id")))

record_count = id_df.count()

logger.info(f"Generated lakefusion_id for {record_count} records")
logger.info(f"Generated surrogate_key for {record_count} records")
logger.info(f"IDs are deterministic - same input produces same ID")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 4: CREATE ATTRIBUTES_COMBINED")
logger.info("="*60)

# Get list of attributes to combine
combine_attrs = [attr for attr in match_attributes]

# Resolve REFERENCE_ENTITY attrs inline: ensures the mapping table exists, MERGEs
# any newly-resolved source_values, and splits the batch into approved (with
# ref_lakefusion_id baked into the column and ref_value in <attr>__display) and
# pending (PENDING / NO_MATCH — routed to the pending-reference table).
primary_source_id = next(
    (cfg["source_id"] for cfg in rdm_configs if cfg.get("source_table") == primary_table),
    None,
)
id_df, id_pending_df = resolve_reference_attributes(
    spark, id_df, rdm_configs, source_id=primary_source_id
)

# Build the concat list: use resolved display value for REF attrs, raw otherwise
concat_cols = []
for attr in combine_attrs:
    display_col = f"{attr}__display"
    src = display_col if display_col in id_df.columns else attr
    concat_cols.append(col(src))

# complex-aware attributes_combined. Falls back to the legacy
# concat_ws when no STRUCT / ARRAY attributes are present so vector search
# behaviour is byte-identical for scalar-only entities.
if _has_complex_attrs:
    id_df = id_df.withColumn(
        "attributes_combined",
        build_attributes_combined_column(id_df, combine_attrs, entity_attribute_records),
    )
else:
    id_df = id_df.withColumn(
        "attributes_combined",
        concat_ws(" | ", *[coalesce(col(attr).cast("string"), lit("")) for attr in combine_attrs])
    )

# Drop the resolver's __display columns before downstream writes
for attr in combine_attrs:
    display_col = f"{attr}__display"
    if display_col in id_df.columns:
        id_df = id_df.drop(display_col)

logger.info(f"Created attributes_combined from {len(combine_attrs)} attributes")

# Route PENDING / NO_MATCH rows to the unified error log
if id_pending_df is not None and not id_pending_df.isEmpty():
    unified_error_handler = UnifiedErrorHandler(spark, unified_table)
    unified_error_handler.log_errors(
        id_pending_df.select(
            col("surrogate_key"),
            col("_rdm_pending_reason").alias("error_message"),
        ),
        stage="RDM",
        run_id=run_id,
    )
    logger.info(f"  Logged {id_pending_df.count()} PENDING / NO_MATCH rows to unified error table (stage=RDM)")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 5: INSERT INTO MASTER TABLE")
logger.info("="*60)

# Prepare master insert DataFrame
master_columns = ["lakefusion_id"] + [attr for attr in entity_attributes if attr != "lakefusion_id"] + ["attributes_combined"]
master_insert_df = id_df.select(*master_columns)

# Get count before insert
before_count = spark.read.table(master_table).count()

# Insert into master table
master_insert_df.write.format("delta").mode("append").saveAsTable(master_table)

optimise_res = spark.sql(f"OPTIMIZE {master_table} ZORDER BY (lakefusion_id)")
spark.sql(f"""
    ALTER TABLE {unified_table}
    CLUSTER BY (surrogate_key, search_results)
""")
optimise_res = spark.sql(f"OPTIMIZE {unified_table}")


# Verify
after_count = spark.read.table(master_table).count()
inserted_count = after_count - before_count

logger.info(f"Inserted {inserted_count} records into Master table")
logger.info(f"  Before: {before_count}, After: {after_count}")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 6: INSERT INTO UNIFIED TABLE")
logger.info("="*60)

# Build select list for unified table schema
unified_select_cols = [
    "surrogate_key",
    "source_path",
    "source_id",
    col("lakefusion_id").alias("master_lakefusion_id"),  # NEW: Link to master
    lit(RecordStatus.MERGED.value).alias("record_status")
] + [col(attr) for attr in entity_attributes if attr != "lakefusion_id"] + [
    "attributes_combined",
    lit("").alias("search_results"),
    lit("").alias("scoring_results"),
    # Reserved system audit columns; tie-breaker for survivorship engine.
    current_timestamp().alias("__lf_created_at"),
    current_timestamp().alias("__lf_modified_at"),
]

# Create unified DataFrame in one select operation
unified_insert_df = id_df.select(*unified_select_cols)

# Get count before insert
before_count = spark.read.table(unified_table).count()

# Insert into unified table
unified_insert_df.write.format("delta").mode("append").saveAsTable(unified_table)

# Verify
after_count = spark.read.table(unified_table).count()
inserted_count = after_count - before_count

logger.info(f"Inserted {inserted_count} records into Unified table")
logger.info(f"  Status: {RecordStatus.MERGED.value} (primary records are pre-merged)")
logger.info(f"  master_lakefusion_id: Linked to Master table")
logger.info(f"  Before: {before_count}, After: {after_count}")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 7: LOG TO MERGE ACTIVITIES")
logger.info("="*60)

# Get current version of master table (should be 0 or 1 for initial load)
master_version = spark.sql(f"DESCRIBE HISTORY {master_table}").selectExpr("max(version)").first()[0]

logger.info(f"Master table version: {master_version}")

# Create merge activities records
# Use master_id and match_id column names to match table schema
merge_activities_df = id_df.select(
    col("lakefusion_id").alias("master_id"),
    col("surrogate_key").alias("match_id"),
    col("source_path").alias("source"),
    lit(master_version).alias("version"),
    lit(ActionType.INITIAL_LOAD.value).alias("action_type"),
    current_timestamp().alias("created_at")
)

# Get count before insert
before_count = spark.read.table(merge_activities_table).count()

# Insert into merge activities
merge_activities_df.write.format("delta").mode("append").saveAsTable(merge_activities_table)

# Verify
after_count = spark.read.table(merge_activities_table).count()
inserted_count = after_count - before_count

logger.info(f"Logged {inserted_count} activities to Merge Activities table")
logger.info(f"  Action Type: {ActionType.INITIAL_LOAD.value}")
logger.info(f"  master_id = lakefusion_id, match_id = surrogate_key")
logger.info(f"  Before: {before_count}, After: {after_count}")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 8: LOG TO ATTRIBUTE VERSION SOURCES")
logger.info("="*60)

# Create attribute source mapping for each record
# This tracks which source contributed which attribute value

def create_attribute_mapping(row):
    """
    Create attribute source mapping for a single record.
    
    Returns list of dicts: [{attribute_name, attribute_value, source}, ...]
    """
    mapping = []
    source_path = row["source_path"]
    
    for attr in entity_attributes:
        if attr == "lakefusion_id":
            continue
        
        attr_value = row[attr]
        if attr_value is not None:
            mapping.append({
                "attribute_name": attr,
                "attribute_value": str(attr_value),
                "source": source_path
            })
    
    return mapping

# Register UDF for creating attribute mapping
create_mapping_udf = udf(
    create_attribute_mapping,
    ArrayType(StructType([
        StructField("attribute_name", StringType()),
        StructField("attribute_value", StringType()),
        StructField("source", StringType())
    ]))
)

# Create attribute version sources records DIRECTLY from id_df
# No need for complex joins - apply UDF and select needed columns in one go
attr_version_df = id_df.select(
    col("lakefusion_id"),
    lit(master_version).alias("version"),
    create_mapping_udf(struct(*id_df.columns)).alias("attribute_source_mapping")
)

# Get count before insert
before_count = spark.read.table(attribute_version_sources_table).count()

# Insert into attribute version sources
attr_version_df.write.format("delta").mode("append").saveAsTable(attribute_version_sources_table)

# Verify
after_count = spark.read.table(attribute_version_sources_table).count()
inserted_count = after_count - before_count

logger.info(f"Logged {inserted_count} attribute version records")
logger.info(f"  Version: {master_version}")
logger.info(f"  Before: {before_count}, After: {after_count}")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("VERIFICATION")
logger.info("="*60)

# Count records in each table
master_count = spark.read.table(master_table).count()
unified_count = spark.read.table(unified_table).count()
merge_activities_count = spark.read.table(merge_activities_table).count()
attr_version_count = spark.read.table(attribute_version_sources_table).count()

logger.info(f"Master Table: {master_count} records")
logger.info(f"Unified Table: {unified_count} records (status=MERGED)")
logger.info(f"Merge Activities: {merge_activities_count} records (action=INITIAL_LOAD)")
logger.info(f"Attribute Version Sources: {attr_version_count} records")

# Verify counts match
if master_count == unified_count == merge_activities_count == attr_version_count:
    logger.info(f"\nAll counts match: {master_count} records")
else:
    logger.warning(f"Counts don't match!")
    logger.warning(f"   Master: {master_count}")
    logger.warning(f"   Unified: {unified_count}")
    logger.warning(f"   Merge Activities: {merge_activities_count}")
    logger.warning(f"   Attr Version Sources: {attr_version_count}")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("LAKEFUSION_ID CONSISTENCY CHECK")
logger.info("="*60)

# Get lakefusion_ids from each table
master_ids = spark.read.table(master_table).select("lakefusion_id").distinct()
merge_master_ids = spark.read.table(merge_activities_table).select(col("master_id").alias("lakefusion_id")).distinct()
attr_version_ids = spark.read.table(attribute_version_sources_table).select("lakefusion_id").distinct()

master_id_count = master_ids.count()
merge_id_count = merge_master_ids.count()
attr_id_count = attr_version_ids.count()

logger.info(f"Unique lakefusion_ids in Master: {master_id_count}")
logger.info(f"Unique master_ids in Merge Activities: {merge_id_count}")
logger.info(f"Unique lakefusion_ids in Attr Version Sources: {attr_id_count}")

# Check if they match
if master_id_count == merge_id_count == attr_id_count:
    # Now check if the actual IDs are the same
    master_ids_set = set([row.lakefusion_id for row in master_ids.collect()])
    merge_ids_set = set([row.lakefusion_id for row in merge_master_ids.collect()])
    attr_ids_set = set([row.lakefusion_id for row in attr_version_ids.collect()])
    
    if master_ids_set == merge_ids_set == attr_ids_set:
        logger.info("All lakefusion_ids are consistent across tables!")
    else:
        logger.error("ERROR: lakefusion_ids don't match across tables!")
        logger.error(f"   IDs only in Master: {master_ids_set - merge_ids_set - attr_ids_set}")
        logger.error(f"   IDs only in Merge Activities: {merge_ids_set - master_ids_set - attr_ids_set}")
        logger.error(f"   IDs only in Attr Version Sources: {attr_ids_set - master_ids_set - merge_ids_set}")
else:
    logger.error("ERROR: Different number of lakefusion_ids across tables!")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("LOAD PRIMARY SOURCE - COMPLETE")
logger.info("="*60)
logger.info(f"Entity: {entity}")
logger.info(f"Source: {primary_table}")
logger.info(f"Records Loaded: {master_count}")
logger.info(f"Master Version: {master_version}")
logger.info("="*60)
logger.info("Next: Load Secondary Sources")
logger.info("="*60)
