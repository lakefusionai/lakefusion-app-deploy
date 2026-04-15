# Databricks notebook source
# MAGIC %md
# MAGIC # Match Maven - Process Potential Match Experiment (SDK Version)
# MAGIC
# MAGIC Creates potential match table for Match Maven experiment analysis.
# MAGIC Shows ALL matches without threshold-based filtering.
# MAGIC
# MAGIC **Customization Points:**
# MAGIC - `PRE-EXECUTE`: Add custom logic before processing
# MAGIC - `POST-EXECUTE`: Add custom logic after processing

# COMMAND ----------

import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("attributes", "[]", "Attributes JSON")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("is_single_source", "false", "Is Single Source (Golden Dedup)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameter Extraction

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
entity = dbutils.widgets.get("entity")
attributes = dbutils.widgets.get("attributes")
experiment_id = dbutils.widgets.get("experiment_id")
is_single_source = dbutils.widgets.get("is_single_source")

# COMMAND ----------

# Get values from upstream tasks
entity = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="entity", debugValue=entity)
attributes = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="match_attributes", debugValue=attributes)
is_single_source = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="is_single_source", debugValue=is_single_source)
catalog_name = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="catalog_name", debugValue=catalog_name)

# COMMAND ----------

# Parse parameters
attributes = json.loads(attributes) if isinstance(attributes, str) and attributes else []

# Convert is_single_source to boolean
if isinstance(is_single_source, str):
    is_single_source = is_single_source.lower() == "true"
else:
    is_single_source = bool(is_single_source)


# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

mode_name = "GOLDEN DEDUP (Single Source)" if is_single_source else "NORMAL DEDUP (Multi Source)"

logger.info("="*60)
logger.info(f"PROCESS POTENTIAL MATCH EXPERIMENT - {mode_name} (SDK VERSION)")
logger.info("="*60)
logger.info(f"Entity: {entity}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment ID: {experiment_id}")
logger.info(f"Is Single Source: {is_single_source}")
logger.info("EXPERIMENT MODE: Shows ALL matches (no match status filtering)")
logger.info("="*60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup LakeFusion Core Engine




# COMMAND ----------

# DBTITLE 1,Configuration
ENABLE_INTERACTIVE = False  # Set to True for step-by-step debugging

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task Setup

# COMMAND ----------

from lakefusion_core_engine.executors import TaskContext
from lakefusion_core_engine.executors.tasks.integration_core.match_maven import ProcessDeterministicPotentialMatchExperimentExecutor

# Create task context
context = TaskContext(
    task_name="process_deterministic_potential_match_experiment",
    entity=entity,
    experiment_id=experiment_id or None,
    catalog_name=catalog_name,
    spark=spark,
    dbutils=dbutils,
    params={
        "attributes": attributes,
        "is_single_source": is_single_source,
        "logger": logger
    }
)

# DBTITLE 1,Execute task
executor = ProcessDeterministicPotentialMatchExperimentExecutor(context)

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

    if result.artifacts:
        logger.info(f"\nArtifacts:")
        for key, value in result.artifacts.items():
            logger.info(f"  {key}: {value}")

    logger.info("EXPERIMENT MODE:")
    logger.info("  - ALL matches shown (no match status filtering)")
    logger.info("  - Use scores to analyze match quality")

logger_instance.shutdown()