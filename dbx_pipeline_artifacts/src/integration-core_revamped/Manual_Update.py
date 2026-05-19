# Databricks notebook source
import json

# COMMAND ----------

# Widget parameters
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("entity_name", "", "Entity Name")
dbutils.widgets.text("lakefusion_id", "", "LakeFusion ID")
dbutils.widgets.text("update_attributes", "{}", "Update Attributes JSON")
dbutils.widgets.text("entity_attributes", "[]", "Entity Attributes JSON")
dbutils.widgets.text("experiment_id", "", "Experiment ID")


# COMMAND ----------

# Get parameters
catalog_name = dbutils.widgets.get("catalog_name")
entity_name = dbutils.widgets.get("entity_name")
lakefusion_id = dbutils.widgets.get("lakefusion_id")
update_attributes = json.loads(dbutils.widgets.get("update_attributes"))
entity_attributes = json.loads(dbutils.widgets.get("entity_attributes"))
experiment_id = dbutils.widgets.get("experiment_id")

# COMMAND ----------

# MAGIC %run ../utils/execute_utils



# COMMAND ----------

# DBTITLE 1,Configuration
ENABLE_INTERACTIVE = False  # Set to True for step-by-step debugging
if ENABLE_INTERACTIVE:
    logger = _PrintLogger()
    logger.info("Interactive logging enabled — logger.info will print to console")


# COMMAND ----------

# Import SDK components
from lakefusion_core_engine.executors import TaskContext
from lakefusion_core_engine.executors.tasks.integration_core import ManualUpdateExecutor

# COMMAND ----------

# Build context
context = TaskContext(
    task_name="Manual_Update",
    entity=entity_name,
    experiment_id=None,
    catalog_name=catalog_name,
    dataset_objects=[],
    spark=spark,
    dbutils=dbutils,
    params={
        'lakefusion_id': lakefusion_id,
        'update_attributes': update_attributes,
        'entity_attributes': entity_attributes,
        "logger": logger
    }
)

# COMMAND ----------

# DBTITLE 1,Execute task
executor = ManualUpdateExecutor(context)

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
if not ENABLE_INTERACTIVE:
    logger_instance.shutdown()
    dbutils.notebook.exit(json.dumps({
        "status": "success",
        "message": result.message,
        "metrics": result.metrics
    }))
