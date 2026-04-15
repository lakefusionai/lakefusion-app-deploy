# Databricks notebook source
# MAGIC %md
# MAGIC # Golden Dedup - Entity Matching LLM (SDK Version)
# MAGIC
# MAGIC Uses LLM to score potential master-to-master matches identified by vector search.
# MAGIC
# MAGIC **Customization Points:**
# MAGIC - `PRE-EXECUTE`: Add custom logic before LLM matching
# MAGIC - `POST-EXECUTE`: Add custom logic after LLM matching

# COMMAND ----------

import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("llm_endpoint", "", "LLM Endpoint Name")
dbutils.widgets.text("experiment_id", "", "Match Maven Experiment")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("attributes", "", "Merged Description Attributes")
dbutils.widgets.dropdown("llm_model_source", "databricks_foundation", ["databricks_custom_hugging_face", "databricks_foundation"], "LLM Model Source")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("additional_instructions", "", "Additional Instructions")
dbutils.widgets.text("config_thresholds", "", "Match Thresholds Config")
dbutils.widgets.text("max_potential_matches", "3", "Max Potential Matches")
dbutils.widgets.text("base_prompt", "", "Base Prompt")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameter Extraction

# COMMAND ----------

llm_endpoint = dbutils.widgets.get("llm_endpoint")
attributes = dbutils.widgets.get("attributes")
experiment_id = dbutils.widgets.get("experiment_id")
entity = dbutils.widgets.get("entity")
llm_model_source = dbutils.widgets.get("llm_model_source")
catalog_name = dbutils.widgets.get("catalog_name")
additional_instructions = dbutils.widgets.get("additional_instructions")
config_thresholds = dbutils.widgets.get("config_thresholds")
max_potential_matches = dbutils.widgets.get("max_potential_matches")
base_prompt = dbutils.widgets.get("base_prompt")

# COMMAND ----------

# Get values from upstream tasks
entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
llm_endpoint = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="llm_model_endpoint", debugValue=llm_endpoint)
attributes = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="match_attributes", debugValue=attributes)
llm_model_source = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="llm_model_source", debugValue=llm_model_source)
additional_instructions = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="additional_instructions", debugValue=additional_instructions)
config_thresholds = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="config_thresholds", debugValue=config_thresholds)
max_potential_matches = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="max_potential_matches", debugValue=max_potential_matches)
base_prompt = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "base_prompt", debugValue=base_prompt)
llm_temperature = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "llm_temperature", debugValue="0.0")
llm_temperature = float(llm_temperature) if llm_temperature else 0.0


# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup LakeFusion Core Engine

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
    logger.info(f"Endpoints_Mapping task not available, using configured endpoint: {e}")

# Fallback to foundation or hugging face endpoints if not set
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

# Parse JSON parameters
attributes = json.loads(attributes)
config_thresholds = json.loads(config_thresholds)
max_potential_matches = int(max_potential_matches)



# COMMAND ----------

logger.info("="*60)
logger.info("GOLDEN DEDUP - LLM MATCHING (SDK VERSION)")
logger.info("="*60)
logger.info(f"Entity: {entity}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment: {experiment_id if experiment_id else 'prod'}")
logger.info(f"LLM Endpoint: {llm_endpoint}")
logger.info(f"Max Potential Matches: {max_potential_matches}")
logger.info(f"Config Thresholds: {config_thresholds}")
logger.info("="*60)



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

# Use qualified imports for namespace clarity
from lakefusion_core_engine.executors import TaskContext
from lakefusion_core_engine.executors.tasks.integration_core.golden_deduplication import GoldenLLMMatchingExecutor

# Create task context
context = TaskContext(
    task_name="golden_llm_matching",
    entity=entity,
    experiment_id=experiment_id or None,
    catalog_name=catalog_name,
    spark=spark,
    dbutils=dbutils,
    params={
        "llm_endpoint": llm_endpoint,
        "scoring_endpoint": llm_endpoint,
        "base_prompt": base_prompt,
        "match_attributes": attributes,
        "additional_instructions": additional_instructions,
        "config_thresholds": config_thresholds,
        "max_potential_matches": max_potential_matches,
        "master_id_key": "lakefusion_id",
        "llm_temperature": llm_temperature,
        "logger": logger
    }
)

logger.info(f"Master Table: {context.master_table}")

# COMMAND ----------

# DBTITLE 1,Execute task
executor = GoldenLLMMatchingExecutor(context)

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

logger_instance.shutdown()
