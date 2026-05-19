# Databricks notebook source
import json

# COMMAND ----------

# MAGIC %run ../utils/taskvalues_enum

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("se_job_id", "", "Schema Evolution Job ID")
dbutils.widgets.text("entity_id", "", "Entity ID")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "prod", "Experiment ID")

# COMMAND ----------

# DBTITLE 1,Get parameters
catalog_name = dbutils.widgets.get("catalog_name")
entity_id = dbutils.widgets.get("entity_id")
experiment_id = dbutils.widgets.get("experiment_id") or "prod"

# COMMAND ----------

# DBTITLE 1,Get task values from Parse step
entity_name = dbutils.jobs.taskValues.get(
    taskKey="Parse_Schema_Evolution_JSON",
    key=TaskValueKey.SCHEMA_EVOLUTION_ENTITY_NAME.value,
    debugValue=""
)

edits_json = dbutils.jobs.taskValues.get(
    taskKey="Parse_Schema_Evolution_JSON",
    key=TaskValueKey.SCHEMA_EVOLUTION_EDITS.value,
    debugValue="[]"
)

# COMMAND ----------

# MAGIC %run ../utils/execute_utils

# COMMAND ----------

setup_lakefusion_engine()

# COMMAND ----------

# DBTITLE 1,Read entity.json and augment with SE edits
entity_json_path = f"/Volumes/{catalog_name}/metadata/metadata_files/entity_{entity_id}_prod_entity.json"

try:
    with open(entity_json_path, 'r') as f:
        entity_json = json.loads(f.read())
except Exception as e:
    logger.error(f"Failed to read entity JSON: {e}")
    logger_instance.shutdown()
    raise ValueError(f"Failed to read entity JSON: {e}")

entity_name_parsed = entity_json.get("name", "").lower().replace(" ", "_")
entity_attributes = [item["name"] for item in entity_json.get("attributes", [])]
id_key = "lakefusion_id"

# entity.json hasn't been updated yet (happens at complete_job()),
# so augment entity_attributes with new attributes from SE edits
edits = json.loads(edits_json) if isinstance(edits_json, str) else edits_json
new_se_attrs = []
for edit in edits:
    if edit.get('action_type') == 'ADD_ATTRIBUTE':
        attr_name = edit['attribute_name']
        if attr_name not in entity_attributes:
            entity_attributes.append(attr_name)
            new_se_attrs.append(attr_name)

# COMMAND ----------

# DBTITLE 1,Build table names and log configuration
experiment_suffix = f"_{experiment_id}" if experiment_id else ""
master_table = f"{catalog_name}.gold.{entity_name_parsed}_master{experiment_suffix}"
unified_dedup_table = f"{catalog_name}.silver.{entity_name_parsed}_unified_deduplicate{experiment_suffix}"
potential_match_table = f"{catalog_name}.gold.{entity_name_parsed}_master_potential_match{experiment_suffix}"
potential_match_dedup_table = f"{catalog_name}.gold.{entity_name_parsed}_master_potential_match_deduplicate{experiment_suffix}"

logger.info("=" * 80)
logger.info("SCHEMA EVOLUTION - SYNC DERIVED TABLES")
logger.info("=" * 80)
logger.info(f"Entity ID: {entity_id}")
logger.info(f"Entity Name: {entity_name_parsed}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment ID: {experiment_id}")
logger.info(f"Entity Attributes (from entity.json): {len(entity_attributes) - len(new_se_attrs)}")
logger.info(f"New SE Attributes (from edits): {new_se_attrs}")
logger.info(f"Total Entity Attributes: {len(entity_attributes)}")
logger.info(f"Unified Dedup Table: {unified_dedup_table}")
logger.info(f"Potential Match Table: {potential_match_table}")
logger.info(f"Potential Match Dedup Table: {potential_match_dedup_table}")
logger.info("=" * 80)

# COMMAND ----------

# DBTITLE 1,Step 1: Sync unified_deduplicate from master
stats = {}

if not spark.catalog.tableExists(unified_dedup_table):
    logger.info(f"Unified dedup table does not exist - skipping: {unified_dedup_table}")
    stats['unified_dedup_synced'] = False
else:
    logger.info(f"Syncing unified_dedup from master (preserving dedup-only columns)")
    logger.info(f"  Source: {master_table}")
    logger.info(f"  Target: {unified_dedup_table}")

    # Columns in both tables -> update from master
    # Columns only in dedup (search_results, scoring_results, etc.) -> preserved
    dedup_col_set = set(f.name for f in spark.table(unified_dedup_table).schema)
    master_col_set = set(f.name for f in spark.table(master_table).schema)
    shared_cols = dedup_col_set & master_col_set
    dedup_only_cols = dedup_col_set - master_col_set

    # Don't include lakefusion_id in SET clause (it's the join key)
    update_cols = [c for c in shared_cols if c != 'lakefusion_id']

    logger.info(f"  Columns to update from master: {len(update_cols)}")
    logger.info(f"  Columns preserved in dedup: {dedup_only_cols}")

    set_clause = ", ".join([f"t.`{c}` = s.`{c}`" for c in update_cols])

    spark.sql(f"""
        MERGE INTO {unified_dedup_table} t
        USING {master_table} s
        ON t.lakefusion_id = s.lakefusion_id
        WHEN MATCHED THEN UPDATE SET {set_clause}
    """)

    logger.info(f"  Unified dedup synced from master (dedup-only columns preserved)")
    stats['unified_dedup_synced'] = True

# COMMAND ----------

# DBTITLE 1,Step 2: Rebuild potential_match table (if exists)
from lakefusion_core_engine.executors import TaskContext
from lakefusion_core_engine.executors.tasks.integration_core.normal_deduplication.process_potential_match import ProcessPotentialMatchExecutor

if not spark.catalog.tableExists(potential_match_table):
    logger.info(f"Potential match table does not exist - skipping: {potential_match_table}")
    stats['potential_match_rebuilt'] = False
else:
    logger.info(f"Rebuilding potential_match table: {potential_match_table}")

    sub_context = TaskContext(
        task_name="Process_Potential_Match_SE",
        entity=entity_name_parsed,
        experiment_id=experiment_id,
        catalog_name=catalog_name,
        spark=spark,
        dbutils=dbutils,
        params={
            'entity_attributes': entity_attributes,
            'logger': logger,
        }
    )

    executor = ProcessPotentialMatchExecutor(sub_context, override_instance=None)
    result = executor.run()

    logger.info(f"  Potential match rebuild result: {result.status.value} - {result.message}")
    stats['potential_match_rebuilt'] = True

# COMMAND ----------

# DBTITLE 1,Step 3: Rebuild potential_match_deduplicate table (if exists)
from lakefusion_core_engine.executors.tasks.integration_core.golden_deduplication.process_potential_match import GoldenProcessPotentialMatchExecutor

if not spark.catalog.tableExists(potential_match_dedup_table):
    logger.info(f"Potential match dedup table does not exist - skipping: {potential_match_dedup_table}")
    stats['potential_match_dedup_rebuilt'] = False
else:
    logger.info(f"Rebuilding potential_match_deduplicate table: {potential_match_dedup_table}")

    sub_context = TaskContext(
        task_name="Process_Potential_Match_Dedup_SE",
        entity=entity_name_parsed,
        experiment_id=experiment_id,
        catalog_name=catalog_name,
        spark=spark,
        dbutils=dbutils,
        params={
            'entity_attributes': entity_attributes,
            'logger': logger,
        }
    )

    executor = GoldenProcessPotentialMatchExecutor(sub_context, override_instance=None)
    result = executor.run()

    logger.info(f"  Potential match dedup rebuild result: {result.status.value} - {result.message}")
    stats['potential_match_dedup_rebuilt'] = True

# COMMAND ----------

# DBTITLE 1,Summary
synced_count = sum(1 for v in [
    stats.get('unified_dedup_synced'),
    stats.get('potential_match_rebuilt'),
    stats.get('potential_match_dedup_rebuilt'),
] if v)

logger.info("\n" + "=" * 80)
logger.info("SCHEMA EVOLUTION - SYNC DERIVED TABLES - COMPLETE")
logger.info("=" * 80)
logger.info(f"Tables synced/rebuilt: {synced_count}")
for key, value in stats.items():
    logger.info(f"  {key}: {value}")
logger.info("=" * 80)

# COMMAND ----------

# Shutdown logger
logger_instance.shutdown()
