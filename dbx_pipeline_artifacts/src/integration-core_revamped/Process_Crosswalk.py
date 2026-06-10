# Databricks notebook source
import json







# COMMAND ----------

dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("dataset_objects", "", "Dataset Objects")



# COMMAND ----------

entity = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "entity",
    debugValue=dbutils.widgets.get("entity")
)
catalog_name = dbutils.widgets.get("catalog_name")
experiment_id = dbutils.widgets.get("experiment_id") or None
dataset_objects = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "dataset_objects",
    debugValue=dbutils.widgets.get("dataset_objects")
)

# Parse JSON parameters
dataset_objects = json.loads(dataset_objects)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup LakeFusion Core Engine

# COMMAND ----------

# MAGIC %run ../utils/execute_utils

# COMMAND ----------

logger.info("="*60)
logger.info("PROCESS CROSSWALK")
logger.info("="*60)
logger.info(f"Entity: {entity}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment ID: {experiment_id or 'None'}")
logger.info("="*60)





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

# Use qualified imports for namespace clarity
from lakefusion_core_engine.executors import TaskContext
from lakefusion_core_engine.executors.tasks.integration_core import ProcessCrosswalkTask

# Create task context
context = TaskContext(
    task_name="process_crosswalk",
    entity=entity,
    experiment_id=experiment_id,
    catalog_name=catalog_name,
    dataset_objects=dataset_objects,
    spark=spark,
    dbutils=dbutils,
    params={"logger": logger}
)

# Create task instance
executor = ProcessCrosswalkTask(context)

logger.info(f"Unified Table: {context.unified_table}")
logger.info(f"Master Table: {context.master_table}")
logger.info(f"Crosswalk View: {context.crosswalk_view}")

# COMMAND ----------

# DBTITLE 1,Execute task
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

# MAGIC %md
# MAGIC ## Completion

# COMMAND ----------

# Display final status
if not ENABLE_INTERACTIVE:
    logger.info("\n" + "=" * 80)
    logger.info("CROSSWALK GENERATION COMPLETE")
    logger.info("=" * 80)

    logger.info(f"\nFinal Status: {result.status}")
    logger.info(f"Message: {result.message}")

    if result.metrics:
        logger.info(f"\nMetrics:")
        for key, value in result.metrics.items():
            logger.info(f"  {key}: {value}")

    if result.task_values:
        logger.info(f"\nTask Values (for downstream):")
        for key, value in result.task_values.items():
            logger.info(f"  {key}: {value}")

    if result.artifacts:
        logger.info(f"\nArtifacts:")
        for key, value in result.artifacts.items():
            logger.info(f"  {key}: {value}")

logger_instance.shutdown()