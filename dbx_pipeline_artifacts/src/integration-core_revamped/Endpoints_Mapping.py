# Databricks notebook source
import json

# COMMAND ----------

# Get widgets
dbutils.widgets.text("embedding_model", "", "Embedding Model")
dbutils.widgets.text("llm_model", "", "LLM Model")
dbutils.widgets.dropdown("llm_model_source", "databricks_foundation", ["databricks_custom_hugging_face", "databricks_foundation"], "LLM Model Source")
dbutils.widgets.dropdown("embedding_model_source", "databricks_foundation", ["databricks_custom_hugging_face", "databricks_foundation"], "Embedding Model Source")
dbutils.widgets.text("llm_provisionless", "", "LLM Model Foundational Provisionless?")
dbutils.widgets.text("embedding_provisionless", "", "Embedding Model Foundational Provisionless?")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")

# COMMAND ----------

# Get widget values
embedding_model = dbutils.widgets.get("embedding_model")
embedding_model_source = dbutils.widgets.get("embedding_model_source")
llm_model = dbutils.widgets.get("llm_model")
llm_model_source = dbutils.widgets.get("llm_model_source")
llm_provisionless = dbutils.widgets.get("llm_provisionless")
embedding_provisionless = dbutils.widgets.get("embedding_provisionless")
catalog_name = dbutils.widgets.get("catalog_name")
experiment_id = dbutils.widgets.get("experiment_id")

# COMMAND ----------

# Get task values with fallbacks
embedding_model = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "embedding_model", debugValue=embedding_model)
embedding_model_source = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "embedding_model_source", debugValue=embedding_model_source)
llm_model = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "llm_model", debugValue=llm_model)
llm_model_source = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "llm_model_source", debugValue=llm_model_source)
llm_provisionless = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "llm_provisionless", debugValue=llm_provisionless)
embedding_provisionless = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "embedding_provisionless", debugValue=embedding_provisionless)

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
from lakefusion_core_engine.executors.tasks.integration_core import EndpointsMappingExecutor

# COMMAND ----------

# Build context
context = TaskContext(
    task_name="Endpoints_Mapping",
    entity="",
    experiment_id=None,
    catalog_name="",
    dataset_objects=[],
    spark=spark,
    dbutils=dbutils,
    params={
        'embedding_model': embedding_model,
        'embedding_model_source': embedding_model_source,
        'llm_model': llm_model,
        'llm_model_source': llm_model_source,
        'llm_provisionless': llm_provisionless,
        'embedding_provisionless': embedding_provisionless,
        "logger": logger
    }
)

# COMMAND ----------

# DBTITLE 1,Execute task
executor = EndpointsMappingExecutor(context)

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

# Exit with result
if not ENABLE_INTERACTIVE:
    logger_instance.shutdown()
    output_json = {
        "embedding_model_endpoint": result.task_values.get('embedding_model_endpoint', ''),
        "llm_model_endpoint": result.task_values.get('llm_model_endpoint', '')
    }
    dbutils.notebook.exit(output_json)
