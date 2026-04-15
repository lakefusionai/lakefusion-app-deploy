# Databricks notebook source
# MAGIC %pip install --upgrade "mlflow-skinny[databricks]" databricks-vectorsearch "databricks-sdk>=0.85.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

dbutils.widgets.text("embedding_model", "", "Embedding Model Foundational")
dbutils.widgets.text("embedding_provisionless", "", "Embedding Model Foundational Provisionless?")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")

# COMMAND ----------

embedding_model = dbutils.widgets.get("embedding_model")
embedding_provisionless = dbutils.widgets.get("embedding_provisionless")


# COMMAND ----------

# Get from task values if available
embedding_model = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "embedding_model", debugValue=embedding_model)
embedding_provisionless = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "embedding_provisionless", debugValue=embedding_provisionless)
pt_models_config = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "pt_models_config", debugValue="{}")
catalog_name = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="catalog_name",
    debugValue=dbutils.widgets.get("catalog_name")
)

experiment_id = dbutils.widgets.get("experiment_id")

# COMMAND ----------

import json
pt_models_config = json.loads(pt_models_config) if isinstance(pt_models_config, str) else pt_models_config

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
from lakefusion_core_engine.executors.tasks.maven_core import EmbeddingFoundationalExecutor

# COMMAND ----------

# Build context
context = TaskContext(
    task_name="Embedding_Foundational",
    entity="",
    experiment_id=None,
    catalog_name="",
    dataset_objects=[],
    spark=spark,
    dbutils=dbutils,
    params={
        'embedding_model': embedding_model,
        'embedding_provisionless': embedding_provisionless,
        'pt_models_config': json.dumps(pt_models_config),
        "logger": logger
    }
)

# COMMAND ----------

# DBTITLE 1,Execute task
executor = EmbeddingFoundationalExecutor(context)

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
        "served_entity": result.task_values.get('served_entity', ''),
        "embedding_model_endpoint": result.task_values.get('embedding_model_endpoint', '')
    }
    dbutils.notebook.exit(output_json)

logger_instance.shutdown()