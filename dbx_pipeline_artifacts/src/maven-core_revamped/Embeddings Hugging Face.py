# Databricks notebook source
# MAGIC %pip install "transformers>=4.51.0,<5.0.0"
# MAGIC %pip install llama-index
# MAGIC %pip install --upgrade mlflow-skinny
# MAGIC %pip install sentence-transformers
# MAGIC %pip install databricks-vectorsearch
# MAGIC %pip install "accelerate>=1.12.0"
# MAGIC %pip install torchvision
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("embedding_model", "", "Embedding Model Hugging Face")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")

# COMMAND ----------

embedding_model = dbutils.widgets.get("embedding_model")
catalog_name = dbutils.widgets.get("catalog_name")
experiment_id = dbutils.widgets.get("experiment_id")

# COMMAND ----------

# Get from task values if available
embedding_model = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "embedding_model", debugValue=embedding_model)


# COMMAND ----------

# MAGIC %run ../utils/execute_utils


# COMMAND ----------

# Manual flush
for handler in logger.handlers:
    handler.flush()


# COMMAND ----------

# DBTITLE 1,Configuration
ENABLE_INTERACTIVE = False  # Set to True for step-by-step debugging
if ENABLE_INTERACTIVE:
    logger = _PrintLogger()
    logger.info("Interactive logging enabled — logger.info will print to console")


# COMMAND ----------

# Import SDK components
from lakefusion_core_engine.executors import TaskContext
from lakefusion_core_engine.executors.tasks.maven_core import EmbeddingHuggingFaceExecutor

# COMMAND ----------

# Build context
context = TaskContext(
    task_name="Embeddings_Hugging_Face",
    entity="",
    experiment_id=None,
    catalog_name=catalog_name,
    dataset_objects=[],
    spark=spark,
    dbutils=dbutils,
    params={
        'embedding_model': embedding_model,
        'catalog_name': catalog_name,
        "logger": logger
    }
)

# COMMAND ----------

# DBTITLE 1,Execute task
executor = EmbeddingHuggingFaceExecutor(context)

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
    output_json = {
        "served_entity": result.task_values.get('served_entity', ''),
        "served_entity_version": result.task_values.get('served_entity_version', ''),
        "embedding_model_endpoint": result.task_values.get('embedding_model_endpoint', '')
    }
    logger_instance.shutdown()
    dbutils.notebook.exit(output_json)

logger_instance.shutdown()