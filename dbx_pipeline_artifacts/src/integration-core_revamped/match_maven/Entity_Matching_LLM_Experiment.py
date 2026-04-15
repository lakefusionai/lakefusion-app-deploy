# Databricks notebook source
# MAGIC %md
# MAGIC # Match Maven - LLM Matching Experiment (SDK Version)
# MAGIC
# MAGIC Performs LLM-based entity matching for Match Maven experiments.
# MAGIC Experiment mode does NOT apply threshold-based classification.
# MAGIC
# MAGIC **Customization Points:**
# MAGIC - `PRE-EXECUTE`: Add custom logic before LLM matching
# MAGIC - `POST-EXECUTE`: Add custom logic after LLM matching

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("attributes", "[]", "Attributes JSON")
dbutils.widgets.text("max_potential_matches", "3", "Max Potential Matches")
dbutils.widgets.text("llm_endpoint", "", "LLM Endpoint")
dbutils.widgets.text("llm_model_source", "", "LLM Model Source")
dbutils.widgets.text("additional_instructions", "", "Additional Instructions")
dbutils.widgets.text("base_prompt", "", "Base Prompt")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("is_single_source", "false", "Is Single Source (Golden Dedup)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameter Extraction

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
entity = dbutils.widgets.get("entity")
attributes = dbutils.widgets.get("attributes")
max_potential_matches = dbutils.widgets.get("max_potential_matches")
llm_endpoint = dbutils.widgets.get("llm_endpoint")
llm_model_source = dbutils.widgets.get("llm_model_source")
additional_instructions = dbutils.widgets.get("additional_instructions")
base_prompt = dbutils.widgets.get("base_prompt")
experiment_id = dbutils.widgets.get("experiment_id")
is_single_source = dbutils.widgets.get("is_single_source")

# COMMAND ----------

# Get values from upstream tasks
entity = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="entity", debugValue=entity)
attributes = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="match_attributes", debugValue=attributes)
is_single_source = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="is_single_source", debugValue=is_single_source)
catalog_name = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="catalog_name", debugValue=catalog_name)
llm_temperature = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="llm_temperature", debugValue="0.0")
llm_temperature = float(llm_temperature) if llm_temperature else 0.0

base_prompt = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="base_prompt", debugValue=base_prompt)
additional_instructions = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="additional_instructions", debugValue=additional_instructions)
max_potential_matches = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="max_potential_matches", debugValue=max_potential_matches)

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

# Check for endpoint mapping override
try:
    llm_endpoint_from_mapping = dbutils.jobs.taskValues.get(
        taskKey="Endpoints_Mapping",
        key="llm_model_endpoint",
        debugValue=None
    )
    if llm_endpoint_from_mapping:
        llm_endpoint = llm_endpoint_from_mapping
except Exception as e:
    logger.info(f"Endpoints_Mapping task not available: {e}")

if not llm_endpoint:
    if llm_model_source == "databricks_foundation":
        llm_endpoint = dbutils.jobs.taskValues.get(
            taskKey="LLM_Foundational_Serving_Endpoint",
            key="llm_model_endpoint",
            debugValue=llm_endpoint
        )
    else:
        llm_endpoint = dbutils.jobs.taskValues.get(
            taskKey="LLM_Hugging_Face_Serving_Endpoint",
            key="llm_model_endpoint",
            debugValue=llm_endpoint
        )

# COMMAND ----------

# Parse parameters
attributes = json.loads(attributes) if isinstance(attributes, str) and attributes else []
max_potential_matches = int(max_potential_matches) if max_potential_matches else 3

# Convert is_single_source to boolean
if isinstance(is_single_source, str):
    is_single_source = is_single_source.lower() == "true"
else:
    is_single_source = bool(is_single_source)



# COMMAND ----------

mode_name = "GOLDEN DEDUP (Single Source)" if is_single_source else "NORMAL DEDUP (Multi Source)"

logger.info("="*60)
logger.info(f"LLM MATCHING EXPERIMENT - {mode_name} (SDK VERSION)")
logger.info("="*60)
logger.info(f"Entity: {entity}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment ID: {experiment_id}")
logger.info(f"Is Single Source: {is_single_source}")
logger.info(f"LLM Endpoint: {llm_endpoint}")
logger.info(f"Max Potential Matches: {max_potential_matches}")
logger.info("EXPERIMENT MODE: No threshold classification")
logger.info("  All matches will be scored without MATCH/POTENTIAL_MATCH/NO_MATCH status")
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
from lakefusion_core_engine.executors.tasks.integration_core.match_maven import LLMMatchingExperimentExecutor

# Create task context
context = TaskContext(
    task_name="llm_matching_experiment",
    entity=entity,
    experiment_id=experiment_id or None,
    catalog_name=catalog_name,
    spark=spark,
    dbutils=dbutils,
    params={
        "llm_endpoint": llm_endpoint,
        "max_potential_matches": max_potential_matches,
        "is_single_source": is_single_source,
        "attributes": attributes,
        "additional_instructions": additional_instructions,
        "base_prompt": base_prompt,
        "llm_temperature": llm_temperature,
        "logger": logger
    }
)

# DBTITLE 1,Execute task
executor = LLMMatchingExperimentExecutor(context)

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

    
    logger.info("EXPERIMENT MODE:")
    logger.info("  - No threshold classification applied")
    logger.info("  - All scores are raw LLM similarity scores")

logger_instance.shutdown()