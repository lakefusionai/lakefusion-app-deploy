# Databricks notebook source
import json

# COMMAND ----------

# Get widgets
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")
dbutils.widgets.text("match_attributes", "", "Match Attributes")

# COMMAND ----------

# Get widget values
entity = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "entity",
    debugValue=dbutils.widgets.get("entity")
)
catalog_name = dbutils.widgets.get("catalog_name")
experiment_id = dbutils.widgets.get("experiment_id")
entity_attributes = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "entity_attributes",
    debugValue=dbutils.widgets.get("entity_attributes")
)
match_attributes = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="match_attributes",
    debugValue=dbutils.widgets.get("match_attributes")
)

# COMMAND ----------

# Parse JSON parameters
entity_attributes = json.loads(entity_attributes) if isinstance(entity_attributes, str) else entity_attributes
match_attributes = json.loads(match_attributes) if isinstance(match_attributes, str) else match_attributes

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
from lakefusion_core_engine.executors.tasks.integration_core.normal_deduplication import ProcessUnmatchedRecordsExecutor

# COMMAND ----------

# Build context with all required parameters
context = TaskContext(
    task_name="Process_Unmatched_Records",
    entity=entity,
    experiment_id=experiment_id if experiment_id else None,
    catalog_name=catalog_name,
    dataset_objects=[],
    spark=spark,
    dbutils=dbutils,
    params={
        'entity_attributes': entity_attributes,
        'match_attributes': match_attributes,
        'unified_id_key': 'surrogate_key',
        'master_id_key': 'lakefusion_id',
        "logger": logger
    }
)

# COMMAND ----------

# DBTITLE 1,Execute task
executor = ProcessUnmatchedRecordsExecutor(context)

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
