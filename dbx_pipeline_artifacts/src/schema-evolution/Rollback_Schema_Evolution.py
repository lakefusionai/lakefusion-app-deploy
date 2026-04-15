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
se_job_id = dbutils.widgets.get("se_job_id")
entity_id = dbutils.widgets.get("entity_id")
catalog_name = dbutils.widgets.get("catalog_name")
experiment_id = "prod"  # Schema evolution always runs against prod

# COMMAND ----------

# MAGIC %run ../utils/execute_utils

# COMMAND ----------

setup_lakefusion_engine()

# COMMAND ----------

# DBTITLE 1,Build paths and log configuration
json_path = f"/Volumes/{catalog_name}/metadata/metadata_files/entity_{entity_id}_schema_evolution_{se_job_id}.json"
entity_json_path = f"/Volumes/{catalog_name}/metadata/metadata_files/entity_{entity_id}_prod_entity.json"

logger.info("=" * 80)
logger.info("SCHEMA EVOLUTION - ROLLBACK")
logger.info("=" * 80)
logger.info(f"Job ID: {se_job_id}")
logger.info(f"Entity ID: {entity_id}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"JSON Path: {json_path}")
logger.info("=" * 80)

# COMMAND ----------

# DBTITLE 1,Step 1: Read schema evolution JSON from Volume
evolution_json_str = dbutils.fs.head(json_path)
config = json.loads(evolution_json_str)

edits = config.get('edits', [])
master_table_name = config.get('master_table', '')
unified_table_name = config.get('unified_table', '')
entity_name = config.get('entity_name', '')
experiment_id = config.get('experiment_id', 'prod')

logger.info(f"Entity name: {entity_name}")
logger.info(f"Experiment ID: {experiment_id}")
logger.info(f"Master table: {master_table_name}")
logger.info(f"Unified table: {unified_table_name}")
logger.info(f"Edits to rollback: {len(edits)}")

# COMMAND ----------

# DBTITLE 1,Build derived table names
experiment_suffix = f"_{experiment_id}" if experiment_id else ""
unified_dedup_table = ""
potential_match_table = ""
potential_match_dedup_table = ""
model_json_path = ""

if entity_name:
    unified_dedup_table = f"{catalog_name}.silver.{entity_name}_unified_deduplicate{experiment_suffix}"
    potential_match_table = f"{catalog_name}.gold.{entity_name}_master_potential_match{experiment_suffix}"
    potential_match_dedup_table = f"{catalog_name}.gold.{entity_name}_master_potential_match_deduplicate{experiment_suffix}"
    model_json_path = f"/Volumes/{catalog_name}/metadata/metadata_files/entity_{entity_id}_{experiment_id}_model.json"

# COMMAND ----------

# DBTITLE 1,Step 2: Enable column mapping and drop columns
def enable_column_mapping(table_name):
    """Enable Delta column mapping so DROP COLUMN is supported. Idempotent."""
    try:
        spark.sql(f"""
            ALTER TABLE {table_name} SET TBLPROPERTIES (
                'delta.columnMapping.mode' = 'name',
                'delta.minReaderVersion' = '2',
                'delta.minWriterVersion' = '5'
            )
        """)
        logger.info(f"  Enabled column mapping on {table_name}")
    except Exception as e:
        if "already enabled" in str(e).lower() or "already set" in str(e).lower():
            logger.info(f"  Column mapping already enabled on {table_name}")
        else:
            raise

logger.info("Enabling column mapping on target tables...")
enable_column_mapping(master_table_name)
enable_column_mapping(unified_table_name)
if unified_dedup_table and spark.catalog.tableExists(unified_dedup_table):
    enable_column_mapping(unified_dedup_table)

rollback_count = 0

for edit in edits:
    if edit.get('action_type') != 'ADD_ATTRIBUTE':
        continue

    attr_name = edit['attribute_name']
    logger.info(f"\n  Rolling back attribute: `{attr_name}`")

    # -- Drop from master table (idempotent) --
    master_cols = [f.name for f in spark.table(master_table_name).schema]
    if attr_name in master_cols:
        spark.sql(f"ALTER TABLE {master_table_name} DROP COLUMN (`{attr_name}`)")
        logger.info(f"  Dropped `{attr_name}` from master table")
    else:
        logger.info(f"  `{attr_name}` not found in master table - already removed")

    # -- Drop from unified table (idempotent) --
    unified_cols = [f.name for f in spark.table(unified_table_name).schema]
    if attr_name in unified_cols:
        spark.sql(f"ALTER TABLE {unified_table_name} DROP COLUMN (`{attr_name}`)")
        logger.info(f"  Dropped `{attr_name}` from unified table")
    else:
        logger.info(f"  `{attr_name}` not found in unified table - already removed")

    # -- Drop from unified_deduplicate table (if exists, idempotent) --
    if unified_dedup_table and spark.catalog.tableExists(unified_dedup_table):
        dedup_cols = [f.name for f in spark.table(unified_dedup_table).schema]
        if attr_name in dedup_cols:
            spark.sql(f"ALTER TABLE {unified_dedup_table} DROP COLUMN (`{attr_name}`)")
            logger.info(f"  Dropped `{attr_name}` from unified_deduplicate table")
        else:
            logger.info(f"  `{attr_name}` not found in unified_deduplicate table - already removed")

    rollback_count += 1

logger.info(f"\nDropped columns for {rollback_count} attribute(s)")

# COMMAND ----------

# DBTITLE 1,Step 3: Rebuild potential_match tables (if they exist)
from lakefusion_core_engine.executors import TaskContext
from lakefusion_core_engine.executors.tasks.integration_core.normal_deduplication.process_potential_match import ProcessPotentialMatchExecutor
from lakefusion_core_engine.executors.tasks.integration_core.golden_deduplication.process_potential_match import GoldenProcessPotentialMatchExecutor

if not entity_name:
    logger.info("No entity_name available - skipping derived table rebuild")
else:
    # Get the rolled-back attribute names
    rolled_back_attrs = {
        edit['attribute_name']
        for edit in edits
        if edit.get('action_type') == 'ADD_ATTRIBUTE'
    }

    if not rolled_back_attrs:
        logger.info("No ADD_ATTRIBUTE edits - skipping derived table rebuild")
    else:
        # Read entity.json to get the current entity_attributes
        # Subtract rolled-back attrs to get the post-rollback list
        try:
            with open(entity_json_path, 'r') as f:
                entity_json = json.loads(f.read())
            all_attrs = [item["name"] for item in entity_json.get("attributes", [])]
        except Exception as e:
            logger.warning(f"  Could not read entity.json: {e}. Falling back to master table schema.")
            # Fallback: derive from master table schema (post-drop), excluding system columns
            system_cols = {
                'lakefusion_id', 'attributes_combined', 'source_path', 'source_id',
                'record_status', 'scoring_results', 'search_results', 'master_lakefusion_id',
                'surrogate_key', 'created_at', 'updated_at', 'version',
            }
            all_attrs = [
                f.name for f in spark.table(master_table_name).schema
                if f.name not in system_cols
            ]

        # Subtract the rolled-back attributes
        entity_attributes = [attr for attr in all_attrs if attr not in rolled_back_attrs]
        logger.info(f"Post-rollback entity attributes: {len(entity_attributes)}")

        # -- Rebuild potential_match (if exists) --
        if potential_match_table and spark.catalog.tableExists(potential_match_table):
            logger.info(f"  Dropping potential_match table for clean rebuild: {potential_match_table}")
            spark.sql(f"DROP TABLE IF EXISTS {potential_match_table}")
            logger.info(f"  Rebuilding potential_match table: {potential_match_table}")

            sub_context = TaskContext(
                task_name="Process_Potential_Match_Rollback",
                entity=entity_name,
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
            logger.info(f"  Potential match rebuild: {result.status.value} - {result.message}")
        else:
            logger.info(f"  Potential match table does not exist - skipping")

        # -- Rebuild potential_match_deduplicate (if exists — golden dedup may not be enabled) --
        if potential_match_dedup_table and spark.catalog.tableExists(potential_match_dedup_table):
            try:
                logger.info(f"  Dropping potential_match_dedup table for clean rebuild: {potential_match_dedup_table}")
                spark.sql(f"DROP TABLE IF EXISTS {potential_match_dedup_table}")
                logger.info(f"  Rebuilding potential_match_deduplicate table: {potential_match_dedup_table}")

                sub_context = TaskContext(
                    task_name="Process_Potential_Match_Dedup_Rollback",
                    entity=entity_name,
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
                logger.info(f"  Potential match dedup rebuild: {result.status.value} - {result.message}")
            except Exception as e:
                logger.warning(f"  Golden dedup potential_match rebuild failed (golden dedup may not be enabled): {e}")
        else:
            logger.info(f"  Potential match dedup table does not exist - skipping")

# COMMAND ----------

# DBTITLE 1,Step 4: Revert match config (always recompute attributes_combined)
# Always recompute - the forward SE path may have modified attributes_combined
# even for non-match attributes when the job was cancelled mid-execution
rolled_back_attrs = {
    edit['attribute_name']
    for edit in edits
    if edit.get('action_type') == 'ADD_ATTRIBUTE'
}

# Read model.json to get current match attributes
# model.json was NOT updated (job failed/cancelled before complete_job ran)
current_match_attrs = []
try:
    with open(model_json_path, 'r') as f:
        model_json_data = json.loads(f.read())
    attribute_objects = model_json_data.get('attributes', [])
    current_match_attrs = [attr.get('name') for attr in attribute_objects if attr.get('name')]
    logger.info(f"  Current match attributes from model.json: {current_match_attrs}")
except FileNotFoundError:
    logger.warning(f"  Model JSON not found at {model_json_path} - clearing attributes_combined")
except Exception as e:
    logger.warning(f"  Failed to read model JSON: {e} - clearing attributes_combined")

# Filter out any rolled-back attrs (safety - model.json should already exclude them)
final_match_attrs = [a for a in current_match_attrs if a not in rolled_back_attrs]
logger.info(f"  Final match attributes after rollback: {final_match_attrs}")

if final_match_attrs:
    match_cols = ", ".join([f"CAST(`{a}` AS STRING)" for a in final_match_attrs])
    concat_expr = f"concat_ws(' | ', {match_cols})"
else:
    # No match attrs left - clear attributes_combined
    concat_expr = "''"

# Update master table
logger.info(f"  Recomputing attributes_combined on master table: {master_table_name}")
spark.sql(f"UPDATE {master_table_name} SET attributes_combined = {concat_expr}")
logger.info(f"  Master table attributes_combined recomputed")

# Update unified table
logger.info(f"  Recomputing attributes_combined on unified table: {unified_table_name}")
spark.sql(f"UPDATE {unified_table_name} SET attributes_combined = {concat_expr}")
logger.info(f"  Unified table attributes_combined recomputed")

# Update unified_deduplicate table (if exists)
if unified_dedup_table and spark.catalog.tableExists(unified_dedup_table):
    logger.info(f"  Recomputing attributes_combined on unified_dedup table: {unified_dedup_table}")
    spark.sql(f"UPDATE {unified_dedup_table} SET attributes_combined = {concat_expr}")
    logger.info(f"  Unified dedup table attributes_combined recomputed")

# COMMAND ----------

# DBTITLE 1,Step 5: Drop Vector Search index (schema change breaks CDF sync)
index_name = f"{master_table_name}_index"
logger.info(f"Dropping VS index: {index_name}")

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
        logger.warning(f"  Failed to drop VS index (non-fatal): {e}")

# COMMAND ----------

# DBTITLE 1,Summary
logger.info("\n" + "=" * 80)
logger.info("SCHEMA EVOLUTION - ROLLBACK - COMPLETE")
logger.info("=" * 80)
logger.info(f"Attributes rolled back: {rollback_count}")
logger.info("=" * 80)

# COMMAND ----------

# Shutdown logger
logger_instance.shutdown()
