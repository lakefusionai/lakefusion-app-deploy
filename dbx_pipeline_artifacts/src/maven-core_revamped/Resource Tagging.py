# Databricks notebook source
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("experiment_id", "", "Match Maven Experiment ID")

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
entity = dbutils.widgets.get("entity")
experiment_id = dbutils.widgets.get("experiment_id")

# COMMAND ----------

# Get entity from task values if available
entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)

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
from lakefusion_core_engine.executors.tasks.maven_core import ResourceTaggingExecutor

# COMMAND ----------

# Build context
context = TaskContext(
    task_name="Resource_Tagging",
    entity=entity,
    experiment_id=experiment_id,
    catalog_name=catalog_name,
    dataset_objects=[],
    spark=spark,
    dbutils=dbutils,
    params={
        'catalog_name': catalog_name,
        'entity': entity,
        'experiment_id': experiment_id,
        "logger": logger
    }
)

# COMMAND ----------

# DBTITLE 1,Execute task
executor = ResourceTaggingExecutor(context)

try:
    if not ENABLE_INTERACTIVE:
        result = executor.run()
    else:
        executor.interactive()
except Exception as e:
    logger.error(f"Error running executor: {e}")
    logger_instance.shutdown()
    raise

# COMMAND ----------

# DBTITLE 1,Display result
if not ENABLE_INTERACTIVE:
    logger.info(f"Status: {result.status.value}")
    logger_instance.shutdown()
    output_json = {
        "resource_tagging_complete": result.task_values.get('resource_tagging_complete', False),
        "schemas_tagged": result.metrics.get('schemas_tagged', 0),
        "tables_tagged": result.metrics.get('tables_tagged', 0)
    }
    dbutils.notebook.exit(output_json)

logger_instance.shutdown()
