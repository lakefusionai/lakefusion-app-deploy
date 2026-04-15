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

# DBTITLE 1,Build JSON path
file_name = f"entity_{entity_id}_schema_evolution_{se_job_id}.json"
json_path = f"/Volumes/{catalog_name}/metadata/metadata_files/{file_name}"

logger.info("=" * 80)
logger.info("PARSING SCHEMA EVOLUTION JSON")
logger.info("=" * 80)
logger.info(f"Job ID: {se_job_id}")
logger.info(f"Entity ID: {entity_id}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"JSON Path: {json_path}")
logger.info("=" * 80)

# COMMAND ----------

# DBTITLE 1,Read JSON file from Volume
try:
    with open(json_path, 'r') as f:
        evolution_json_str = f.read()
    evolution_json = json.loads(evolution_json_str)
    logger.info(f"Schema evolution JSON loaded from: {json_path}")
except Exception as e:
    logger.error(f"Failed to read schema evolution JSON: {e}")
    logger_instance.shutdown()
    raise ValueError(f"Failed to read schema evolution JSON: {e}")

# COMMAND ----------

# DBTITLE 1,Extract configuration
parsed_values = {
    'schema_evolution_job_id': str(evolution_json['job_id']),
    'schema_evolution_edits': json.dumps(evolution_json['edits']),
    'schema_evolution_master_table': evolution_json['master_table'],
    'schema_evolution_unified_table': evolution_json['unified_table'],
    'schema_evolution_avs_table': evolution_json['attribute_version_sources_table'],
    'schema_evolution_entity_name': evolution_json['entity_name'],
    'schema_evolution_catalog_name': evolution_json['catalog_name'],
    'schema_evolution_id_key': evolution_json['id_key'],
    'schema_evolution_experiment_id': evolution_json['experiment_id'],
}

logger.info(f"Entity: {parsed_values['schema_evolution_entity_name']}")
logger.info(f"Job ID: {parsed_values['schema_evolution_job_id']}")
logger.info(f"Edits: {len(evolution_json['edits'])}")
logger.info(f"Master Table: {parsed_values['schema_evolution_master_table']}")
logger.info(f"Unified Table: {parsed_values['schema_evolution_unified_table']}")
logger.info(f"AVS Table: {parsed_values['schema_evolution_avs_table']}")

# COMMAND ----------

# DBTITLE 1,Set task values for downstream tasks
dbutils.jobs.taskValues.set(TaskValueKey.SCHEMA_EVOLUTION_JOB_ID.value, parsed_values['schema_evolution_job_id'])
dbutils.jobs.taskValues.set(TaskValueKey.SCHEMA_EVOLUTION_EDITS.value, parsed_values['schema_evolution_edits'])
dbutils.jobs.taskValues.set(TaskValueKey.SCHEMA_EVOLUTION_MASTER_TABLE.value, parsed_values['schema_evolution_master_table'])
dbutils.jobs.taskValues.set(TaskValueKey.SCHEMA_EVOLUTION_UNIFIED_TABLE.value, parsed_values['schema_evolution_unified_table'])
dbutils.jobs.taskValues.set(TaskValueKey.SCHEMA_EVOLUTION_AVS_TABLE.value, parsed_values['schema_evolution_avs_table'])
dbutils.jobs.taskValues.set(TaskValueKey.SCHEMA_EVOLUTION_ENTITY_NAME.value, parsed_values['schema_evolution_entity_name'])
dbutils.jobs.taskValues.set(TaskValueKey.SCHEMA_EVOLUTION_CATALOG_NAME.value, parsed_values['schema_evolution_catalog_name'])
dbutils.jobs.taskValues.set(TaskValueKey.SCHEMA_EVOLUTION_ID_KEY.value, parsed_values['schema_evolution_id_key'])
dbutils.jobs.taskValues.set(TaskValueKey.SCHEMA_EVOLUTION_EXPERIMENT_ID.value, parsed_values['schema_evolution_experiment_id'])

logger.info(f"\nAll task values set for downstream tasks")

# COMMAND ----------

# DBTITLE 1,Summary
logger.info("=" * 80)
logger.info("PARSE SCHEMA EVOLUTION JSON - COMPLETE")
logger.info("=" * 80)
logger.info(f"Entity: {parsed_values['schema_evolution_entity_name']}")
logger.info(f"Job ID: {parsed_values['schema_evolution_job_id']}")
logger.info(f"Edits: {len(evolution_json['edits'])}")
logger.info("=" * 80)

# COMMAND ----------

# Shutdown logger
logger_instance.shutdown()
