# Databricks notebook source
# MAGIC %md
# MAGIC # Match Maven - Vector Search Experiment (SDK Version)
# MAGIC
# MAGIC Performs vector similarity search for Match Maven experiments.
# MAGIC Supports both single-source (golden dedup) and multi-source (normal dedup) modes.
# MAGIC
# MAGIC **Customization Points:**
# MAGIC - `PRE-EXECUTE`: Add custom logic before vector search
# MAGIC - `POST-EXECUTE`: Add custom logic after vector search

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("embedding_endpoint", "", "Embedding Endpoint Name")
dbutils.widgets.text("experiment_id", "", "Match Maven Experiment")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("attributes", "", "Merged Description Attributes")
dbutils.widgets.dropdown("embedding_model_source", "databricks_foundation", ["databricks_custom_hugging_face", "databricks_foundation"], "Embedding Model Source")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("vs_endpoint", "", "VS Endpoint Name")
dbutils.widgets.text("max_potential_matches", "5", "Max Potential Matches")
dbutils.widgets.text("is_single_source", "", "Is Single Source")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameter Extraction

# COMMAND ----------

embedding_endpoint = dbutils.widgets.get("embedding_endpoint")
attributes = dbutils.widgets.get("attributes")
experiment_id = dbutils.widgets.get("experiment_id")
entity = dbutils.widgets.get("entity")
embedding_model_source = dbutils.widgets.get("embedding_model_source")
catalog_name = dbutils.widgets.get("catalog_name")
vs_endpoint = dbutils.widgets.get("vs_endpoint")
max_potential_matches = dbutils.widgets.get("max_potential_matches")
is_single_source = dbutils.widgets.get("is_single_source")

# COMMAND ----------

# Get values from upstream tasks
entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
vs_endpoint = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="vs_endpoint", debugValue=vs_endpoint)
embedding_endpoint = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="embedding_model_endpoint", debugValue=embedding_endpoint)
attributes = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="match_attributes", debugValue=attributes)
max_potential_matches = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="max_potential_matches", debugValue=max_potential_matches)
embedding_model_source = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="embedding_model_source", debugValue=embedding_model_source)
is_single_source = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="is_single_source", debugValue=is_single_source)

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

# Check for endpoint mapping override
try:
    embedding_endpoint_from_mapping = dbutils.jobs.taskValues.get(
        taskKey="Endpoints_Mapping",
        key="embedding_model_endpoint",
        debugValue=None
    )
    if embedding_endpoint_from_mapping:
        embedding_endpoint = embedding_endpoint_from_mapping
except Exception as e:
    logger.info(f"Endpoints_Mapping task not available, using configured endpoint: {e}")

# Fallback to foundation or hugging face endpoints if not set
if not embedding_endpoint:
    if embedding_model_source == "databricks_foundation":
        embedding_endpoint = dbutils.jobs.taskValues.get(
            taskKey="Embedding_Foundational_Serving_Endpoint",
            key="embedding_model_endpoint",
            debugValue=embedding_endpoint
        )
    else:
        embedding_endpoint = dbutils.jobs.taskValues.get(
            taskKey="Embedding_Hugging_Face_Serving_Endpoint",
            key="embedding_model_endpoint",
            default="mpnetv2",
            debugValue=embedding_endpoint
        )

# COMMAND ----------

# Parse parameters
attributes = json.loads(attributes) if isinstance(attributes, str) else attributes
max_potential_matches = int(max_potential_matches) if max_potential_matches else 5

# Convert is_single_source to boolean
if isinstance(is_single_source, str):
    is_single_source = is_single_source.lower() == "true"
else:
    is_single_source = bool(is_single_source)



# COMMAND ----------

mode_name = "GOLDEN DEDUP (Single Source)" if is_single_source else "NORMAL DEDUP (Multi Source)"

logger.info("="*60)
logger.info(f"VECTOR SEARCH EXPERIMENT - {mode_name} (SDK VERSION)")
logger.info("="*60)
logger.info(f"Entity: {entity}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment ID: {experiment_id}")
logger.info(f"Is Single Source: {is_single_source}")
logger.info(f"Embedding Endpoint: {embedding_endpoint}")
logger.info(f"VS Endpoint: {vs_endpoint}")
logger.info(f"Max Potential Matches: {max_potential_matches}")
logger.info("="*60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup LakeFusion Core Engine





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

from lakefusion_core_engine.executors import TaskContext
from lakefusion_core_engine.executors.tasks.integration_core.match_maven import VectorSearchExperimentExecutor

# Create task context
context = TaskContext(
    task_name="vector_search_experiment",
    entity=entity,
    experiment_id=experiment_id or None,
    catalog_name=catalog_name,
    spark=spark,
    dbutils=dbutils,
    params={
        "embedding_endpoint": embedding_endpoint,
        "vs_endpoint": vs_endpoint,
        "max_potential_matches": max_potential_matches,
        "is_single_source": is_single_source,
        "logger": logger
    }
)

# DBTITLE 1,Execute task
executor = VectorSearchExperimentExecutor(context)

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

if not ENABLE_INTERACTIVE:
    logger.info(f"Status: {result.status.value}")
    logger.info(f"Message: {result.message}")

    if result.metrics:
        logger.info(f"\nMetrics:")
        for key, value in result.metrics.items():
            logger.info(f"  {key}: {value}")

    if result.task_values:
        logger.info(f"\nTask Values (for downstream):")
        for key, value in result.task_values.items():
            logger.info(f"  {key}: {value}")

try:
    logger_instance.shutdown()
except OSError:
    pass  # Ignore flush errors on non-seekable streams in notebooks