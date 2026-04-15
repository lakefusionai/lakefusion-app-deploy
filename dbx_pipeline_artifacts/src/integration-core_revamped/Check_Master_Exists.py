# Databricks notebook source
import json

# COMMAND ----------

# Get parameters from widgets
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")

# COMMAND ----------

# Get task values from Parse_Entity_Model
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
from lakefusion_core_engine.executors.tasks.integration_core import CheckMasterExistsExecutor

# COMMAND ----------

# Build context
context = TaskContext(
    task_name="Check_Master_Exists",
    entity=entity,
    experiment_id=experiment_id if experiment_id else None,
    catalog_name=catalog_name,
    dataset_objects=[],
    spark=spark,
    dbutils=dbutils,
    params={"logger": logger}
)

# COMMAND ----------

# DBTITLE 1,Execute task
executor = CheckMasterExistsExecutor(context)

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

print("Quiting")
