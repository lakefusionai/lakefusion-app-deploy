# Databricks notebook source
import json

# COMMAND ----------

dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("id_key", "", "lakefusion Primary Key")
dbutils.widgets.text("primary_key", "", "Primary Key")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text('validation_functions', '', 'Validation Functions')
dbutils.widgets.text("experiment_id", "", "Experiment ID")


# COMMAND ----------

entity = dbutils.widgets.get("entity")
id_key = dbutils.widgets.get("id_key")
primary_key = dbutils.widgets.get("primary_key")
catalog_name = dbutils.widgets.get("catalog_name")
validation_functions = dbutils.widgets.get("validation_functions")
experiment_id = dbutils.widgets.get("experiment_id")


# COMMAND ----------

# Get from task values if available
validation_functions = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "validation_functions", debugValue=validation_functions)
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
from lakefusion_core_engine.executors.tasks.maven_core import ProcessWarningRecordsExecutor

# COMMAND ----------

# Build context
context = TaskContext(
    task_name="Process_Warning_Records",
    entity=entity,
    experiment_id=None,
    catalog_name=catalog_name,
    dataset_objects=[],
    spark=spark,
    dbutils=dbutils,
    params={
        'entity': entity,
        'catalog_name': catalog_name,
        'id_key': id_key,
        'primary_key': primary_key,
        'validation_functions': validation_functions,
        "logger": logger
    }
)

# COMMAND ----------

# DBTITLE 1,Execute task
executor = ProcessWarningRecordsExecutor(context)

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
        "warnings_processed": result.task_values.get('warnings_processed', 0)
    }
    dbutils.notebook.exit(output_json)
logger_instance.shutdown()