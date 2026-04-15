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

# COMMAND ----------

# MAGIC %run ../utils/execute_utils

# COMMAND ----------

setup_lakefusion_engine()

# COMMAND ----------

# DBTITLE 1,Parse edits and check for match attributes
edits = json.loads(edits_json) if isinstance(edits_json, str) else edits_json

new_match_attrs = [
    edit['attribute_name']
    for edit in edits
    if edit.get('action_type') == 'ADD_ATTRIBUTE' and edit.get('is_match_attribute')
]

has_match_attrs = len(new_match_attrs) > 0

# Build unified_deduplicate table name from unified table name
unified_dedup_table = unified_table.replace('_unified_', '_unified_deduplicate_')

# Build model.json path
model_json_path = f"/Volumes/{catalog_name}/metadata/metadata_files/entity_{entity_id}_{experiment_id}_model.json"

logger.info("=" * 80)
logger.info("SCHEMA EVOLUTION - UPDATE MATCH CONFIG")
logger.info("=" * 80)
logger.info(f"Entity ID: {entity_id}")
logger.info(f"Master Table: {master_table}")
logger.info(f"Unified Table: {unified_table}")
logger.info(f"Unified Dedup Table: {unified_dedup_table}")
logger.info(f"New match attributes: {new_match_attrs}")
logger.info(f"Has match attributes: {has_match_attrs}")
logger.info("=" * 80)

# COMMAND ----------

# DBTITLE 1,Early exit if no match attributes
if not has_match_attrs:
    logger.info("No match attributes in edits - skipping entire update")
    logger_instance.shutdown()
    dbutils.notebook.exit("SKIPPED: No match attributes in edits")

# COMMAND ----------

# DBTITLE 1,Step 1: Read model.json for current match attributes and VS endpoint
existing_match_attrs = []
vs_endpoint = ''

try:
    with open(model_json_path, 'r') as f:
        model_json = json.loads(f.read())

    attribute_objects = model_json.get('attributes', [])
    existing_match_attrs = [
        attr.get('name') for attr in attribute_objects if attr.get('name')
    ]
    vs_endpoint = model_json.get('vs_endpoint', '')

    logger.info(f"Existing match attributes: {existing_match_attrs}")
    logger.info(f"VS endpoint: {vs_endpoint}")

except FileNotFoundError:
    logger.warning(f"Model JSON not found at {model_json_path} - using only new match attrs")
except Exception as e:
    logger.warning(f"Failed to read model JSON: {e} - using only new match attrs")

# Build full match attribute list: existing + new (deduped, preserving order)
all_match_attrs = list(existing_match_attrs)
for attr in new_match_attrs:
    if attr not in all_match_attrs:
        all_match_attrs.append(attr)

logger.info(f"All match attributes (combined): {all_match_attrs}")

# COMMAND ----------

# DBTITLE 1,Step 2: Recompute attributes_combined on master and unified tables
if not all_match_attrs:
    logger.info("No match attributes to combine - skipping")
else:
    match_cols = ", ".join([f"CAST(`{a}` AS STRING)" for a in all_match_attrs])

    # Update master table
    logger.info(f"Updating attributes_combined on master table: {master_table}")
    master_sql = f"UPDATE {master_table} SET attributes_combined = concat_ws(' | ', {match_cols})"
    spark.sql(master_sql)
    logger.info(f"  Master table attributes_combined updated")

    # Update unified table
    logger.info(f"Updating attributes_combined on unified table: {unified_table}")
    spark.sql(f"UPDATE {unified_table} SET attributes_combined = concat_ws(' | ', {match_cols})")
    logger.info(f"  Unified table attributes_combined updated")

    # Update unified_deduplicate table (if exists)
    tables_updated = [master_table, unified_table]
    if unified_dedup_table and spark.catalog.tableExists(unified_dedup_table):
        logger.info(f"Updating attributes_combined on unified_dedup table: {unified_dedup_table}")
        spark.sql(f"UPDATE {unified_dedup_table} SET attributes_combined = concat_ws(' | ', {match_cols})")
        logger.info(f"  Unified dedup table attributes_combined updated")
        tables_updated.append(unified_dedup_table)

# COMMAND ----------

# DBTITLE 1,Step 3: Drop Vector Search index
index_name = f"{master_table}_index"

if not vs_endpoint:
    logger.info(f"No VS endpoint configured - skipping index drop")
else:
    logger.info(f"Dropping VS index: {index_name} (endpoint: {vs_endpoint})")

    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        w.vector_search_indexes.delete_index(index_name=index_name)
        logger.info(f"  VS index dropped successfully: {index_name}")

    except Exception as e:
        error_msg = str(e).lower()
        if "not found" in error_msg or "does not exist" in error_msg or "resource_does_not_exist" in error_msg:
            logger.info(f"  VS index does not exist - skipping: {index_name}")
        else:
            logger.error(f"  Failed to drop VS index: {e}")
            logger_instance.shutdown()
            raise

# COMMAND ----------

# DBTITLE 1,Summary
logger.info("\n" + "=" * 80)
logger.info("SCHEMA EVOLUTION - UPDATE MATCH CONFIG - COMPLETE")
logger.info("=" * 80)
logger.info(f"New match attributes: {len(new_match_attrs)}")
logger.info(f"Total match attributes: {len(all_match_attrs)}")
logger.info(f"VS index drop attempted: {bool(vs_endpoint)}")
logger.info("=" * 80)

# COMMAND ----------

# Shutdown logger
logger_instance.shutdown()
