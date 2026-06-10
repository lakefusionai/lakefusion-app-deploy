# Databricks notebook source
import json

# COMMAND ----------

# Get widgets
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("dataset_tables", "", "Dataset Tables")
dbutils.widgets.text("experiment_id", "", "Match Maven Experiment ID")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("meta_info_table", "", "Meta Info Table")

# COMMAND ----------

# Get widget values
entity = dbutils.widgets.get("entity")
dataset_tables = dbutils.widgets.get("dataset_tables")
experiment_id = dbutils.widgets.get("experiment_id")
catalog_name = dbutils.widgets.get("catalog_name")
meta_info_table = dbutils.widgets.get("meta_info_table")

# COMMAND ----------

# Get task values with fallbacks
entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
dataset_tables = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "dataset_tables", debugValue=dataset_tables)

# COMMAND ----------

# Parse JSON parameters
dataset_tables = json.loads(dataset_tables) if isinstance(dataset_tables, str) else dataset_tables
if experiment_id:
    experiment_id = experiment_id.replace("-", "")

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils


# COMMAND ----------

# DBTITLE 1,Configuration
ENABLE_INTERACTIVE = False  # Set to True for step-by-step debugging
if ENABLE_INTERACTIVE:
    logger = _PrintLogger()
    logger.info("Interactive logging enabled — logger.info will print to console")


# COMMAND ----------

# Import SDK components - use qualified imports for namespace clarity
from lakefusion_core_engine.executors import TaskContext
from lakefusion_core_engine.executors.tasks.integration_core.normal_deduplication import UpdateTableMetadataExecutor

# COMMAND ----------

# Build context with all required parameters
context = TaskContext(
    task_name="Update_Table_Metadata",
    entity=entity,
    experiment_id=experiment_id if experiment_id else None,
    catalog_name=catalog_name,
    dataset_objects=[],
    spark=spark,
    dbutils=dbutils,
    params={
        'dataset_tables': dataset_tables,
        'meta_info_table': meta_info_table,
        "logger": logger
    }
)

# COMMAND ----------

# DBTITLE 1,Execute task
executor = UpdateTableMetadataExecutor(context)

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

# COMMAND ----------

# Exit with result (failures already raise exception in BaseTask.run)
logger_instance.shutdown()
dbutils.notebook.exit(json.dumps({
    "status": "success",
    "message": result.message,
    "metrics": result.metrics
}))
