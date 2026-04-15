# Databricks notebook source
# MAGIC %md
# MAGIC # Validate Incremental Load (SDK Version)
# MAGIC
# MAGIC Validates CDF (Change Data Feed) data quality before processing incremental
# MAGIC changes. Performs 8 validation checks to ensure data integrity before
# MAGIC proceeding with insert/update/delete processing.
# MAGIC
# MAGIC **Customization Points:**
# MAGIC - `PRE-EXECUTE`: Add custom logic before validation
# MAGIC - `POST-EXECUTE`: Add custom logic after validation

# COMMAND ----------

import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("primary_table", "", "Primary Table")
dbutils.widgets.text("primary_key", "", "Primary Key Column")
dbutils.widgets.text("dataset_tables", "[]", "Dataset Tables (JSON)")
dbutils.widgets.text("attributes_mapping_json", "[]", "Attributes Mapping (JSON)")
dbutils.widgets.text("experiment_id", "", "Experiment ID")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameter Extraction

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
entity = dbutils.widgets.get("entity")
primary_table = dbutils.widgets.get("primary_table")
primary_key = dbutils.widgets.get("primary_key")
dataset_tables = dbutils.widgets.get("dataset_tables")
attributes_mapping_json = dbutils.widgets.get("attributes_mapping_json")
experiment_id = dbutils.widgets.get("experiment_id")

# COMMAND ----------

# Get values from Parse_Entity_Model_JSON
entity = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="entity", debugValue=entity)
primary_table = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="primary_table", debugValue=primary_table)
primary_key = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="primary_key", debugValue=primary_key)
dataset_tables = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="dataset_tables", debugValue=dataset_tables)
attributes_mapping_json = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="attributes_mapping", debugValue=attributes_mapping_json)
catalog_name = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="catalog_name", debugValue=catalog_name)

# Get values from Check_Increments_Exists
has_inserts = dbutils.jobs.taskValues.get(taskKey="Check_Increments_Exists", key="has_inserts", debugValue=False)
has_updates = dbutils.jobs.taskValues.get(taskKey="Check_Increments_Exists", key="has_updates", debugValue=False)
has_deletes = dbutils.jobs.taskValues.get(taskKey="Check_Increments_Exists", key="has_deletes", debugValue=False)
tables_with_inserts = dbutils.jobs.taskValues.get(taskKey="Check_Increments_Exists", key="tables_with_inserts", debugValue="[]")
tables_with_updates = dbutils.jobs.taskValues.get(taskKey="Check_Increments_Exists", key="tables_with_updates", debugValue="[]")
tables_with_deletes = dbutils.jobs.taskValues.get(taskKey="Check_Increments_Exists", key="tables_with_deletes", debugValue="[]")
table_version_info = dbutils.jobs.taskValues.get(taskKey="Check_Increments_Exists", key="table_version_info", debugValue="{}")

# COMMAND ----------

# Parse JSON parameters
dataset_tables = json.loads(dataset_tables) if isinstance(dataset_tables, str) and dataset_tables else []
attributes_mapping_json = json.loads(attributes_mapping_json) if isinstance(attributes_mapping_json, str) else attributes_mapping_json
tables_with_inserts = json.loads(tables_with_inserts) if isinstance(tables_with_inserts, str) else tables_with_inserts
tables_with_updates = json.loads(tables_with_updates) if isinstance(tables_with_updates, str) else tables_with_updates
tables_with_deletes = json.loads(tables_with_deletes) if isinstance(tables_with_deletes, str) else tables_with_deletes
table_version_info = json.loads(table_version_info) if isinstance(table_version_info, str) else table_version_info

# Convert boolean flags
if isinstance(has_inserts, str):
    has_inserts = has_inserts.lower() == 'true'
if isinstance(has_updates, str):
    has_updates = has_updates.lower() == 'true'
if isinstance(has_deletes, str):
    has_deletes = has_deletes.lower() == 'true'

# Remove experiment_id hyphens
if experiment_id:
    experiment_id = experiment_id.replace("-", "")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup LakeFusion Core Engine

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils



# COMMAND ----------

logger.info("=" * 60)
logger.info("VALIDATE INCREMENTAL LOAD (SDK VERSION)")
logger.info("=" * 60)
logger.info(f"Entity: {entity}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment ID: {experiment_id or 'None'}")
logger.info(f"Has Inserts: {has_inserts}")
logger.info(f"Has Updates: {has_updates}")
logger.info(f"Has Deletes: {has_deletes}")
logger.info(f"Tables with Inserts: {len(tables_with_inserts)}")
logger.info(f"Tables with Updates: {len(tables_with_updates)}")
logger.info(f"Tables with Deletes: {len(tables_with_deletes)}")
logger.info("=" * 60)



# COMMAND ----------

# DBTITLE 1,Configuration
ENABLE_INTERACTIVE = False  # Set to True for step-by-step debugging
if ENABLE_INTERACTIVE:
    logger = _PrintLogger()
    logger.info("Interactive logging enabled — logger.info will print to console")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task Setup

# COMMAND ----------

from lakefusion_core_engine.executors import TaskContext
from lakefusion_core_engine.executors.tasks.integration_core.incremental_load import ValidateIncrementalLoadExecutor

# Create task context
context = TaskContext(
    task_name="validate_incremental_load",
    entity=entity,
    experiment_id=experiment_id or None,
    catalog_name=catalog_name,
    spark=spark,
    dbutils=dbutils,
    params={
        "primary_table": primary_table,
        "primary_key": primary_key,
        "dataset_tables": dataset_tables,
        "attributes_mapping": attributes_mapping_json,
        "has_inserts": has_inserts,
        "has_updates": has_updates,
        "has_deletes": has_deletes,
        "tables_with_inserts": tables_with_inserts,
        "tables_with_updates": tables_with_updates,
        "tables_with_deletes": tables_with_deletes,
        "table_version_info": table_version_info,
        "logger": logger
    }
)

# DBTITLE 1,Execute task
executor = ValidateIncrementalLoadExecutor(context)

try:
    if not ENABLE_INTERACTIVE:
        result = executor.run()
    else:
        executor.interactive()
except Exception as e:
    logger.error(f"Error running executor: {e}")
    logger_instance.shutdown()
    raise
    # In subsequent cells, use:
    # executor.print_steps()
    # executor.run_step(1)
    # executor.run_step(2)

# COMMAND ----------

# DBTITLE 1,Display result
if not ENABLE_INTERACTIVE:
    logger.info(f"Status: {result.status.value}")
    logger.info(f"Message: {result.message}")
    if result.metrics:
        logger.info(f"Metrics: {result.metrics}")


logger_instance.shutdown()