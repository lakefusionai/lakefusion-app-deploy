# Databricks notebook source
import json

# COMMAND ----------

# MAGIC %run ../utils/taskvalues_enum

# COMMAND ----------

print("Parsing Entity and Model JSON configuration")

# COMMAND ----------

# Get widgets
dbutils.widgets.text('entity_id', '', 'Entity ID')
dbutils.widgets.text('experiment_id', '', 'Experiment ID')
dbutils.widgets.text('process_records', '', 'No of records to be processed')
dbutils.widgets.text("is_integration_hub", "", "Integration Hub Pipeline")
dbutils.widgets.text("catalog_name", "", "lakefusion catalog name")

# COMMAND ----------

# Get widget values
entity_id = dbutils.widgets.get('entity_id')
experiment_id = dbutils.widgets.get('experiment_id')
process_records = dbutils.widgets.get('process_records')
is_integration_hub = dbutils.widgets.get('is_integration_hub')
catalog_name = dbutils.widgets.get('catalog_name')

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
from lakefusion_core_engine.executors.tasks.integration_core import ParseEntityModelJsonExecutor

# COMMAND ----------

# Build context
context = TaskContext(
    task_name="Parse_Entity_Model_JSON",
    entity="",
    experiment_id=experiment_id if experiment_id else None,
    catalog_name=catalog_name,
    dataset_objects=[],
    spark=spark,
    dbutils=dbutils,
    params={
        'entity_id': entity_id,
        'experiment_id': experiment_id,
        'process_records': process_records,
        'is_integration_hub': is_integration_hub,
        "logger": logger
    }
)

# COMMAND ----------

# DBTITLE 1,Execute task
executor = ParseEntityModelJsonExecutor(context)

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

# Shutdown logger (failures already raise exception in BaseTask.run)
logger_instance.shutdown()