# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import json

# COMMAND ----------

# Get widgets
dbutils.widgets.text("llm_endpoint", "", "LLM Endpoint Name")
dbutils.widgets.text("experiment_id", "", "Match Maven Experiment")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("attributes", "", "Merged Description Attributes")
dbutils.widgets.dropdown("llm_model_source", "databricks_foundation", ["databricks_custom_hugging_face", "databricks_foundation"], "LLM Model Source")
dbutils.widgets.text("catalog_name", "", "lakefusion catalog name")
dbutils.widgets.text("additional_instructions", "", "Additional Instruction")
dbutils.widgets.text("config_thresholds", "", "Match Thresholds Config")
dbutils.widgets.text("max_potential_matches", "3", "Max Potential Matches")
dbutils.widgets.text("base_prompt", "","Base Prompt")

# COMMAND ----------

# Get widget values
llm_endpoint = dbutils.widgets.get("llm_endpoint")
attributes = dbutils.widgets.get("attributes")
experiment_id = dbutils.widgets.get("experiment_id")
entity = dbutils.widgets.get("entity")
llm_model_source = dbutils.widgets.get("llm_model_source")
catalog_name = dbutils.widgets.get("catalog_name")
additional_instructions = dbutils.widgets.get("additional_instructions")
config_thresholds = dbutils.widgets.get("config_thresholds")
max_potential_matches = int(dbutils.widgets.get("max_potential_matches"))
base_prompt = dbutils.widgets.get("base_prompt")

# COMMAND ----------

# Get task values from upstream tasks
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
    logger.info(f"Endpoints_Mapping task not available, falling back to model source endpoints: {e}")

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
attributes = json.loads(attributes) if isinstance(attributes, str) else attributes
config_thresholds = json.loads(config_thresholds) if isinstance(config_thresholds, str) else config_thresholds
max_potential_matches = int(max_potential_matches)




# COMMAND ----------

# DBTITLE 1,Configuration
ENABLE_INTERACTIVE = False  # Set to True for step-by-step debugging
if ENABLE_INTERACTIVE:
    logger = _PrintLogger()
    logger.info("Interactive logging enabled — logger.info will print to console")


# COMMAND ----------

# Import SDK components - use qualified imports for namespace clarity
from lakefusion_core_engine.executors import TaskContext
from lakefusion_core_engine.executors.tasks.integration_core.normal_deduplication import LLMMatchingExecutor

# COMMAND ----------

# Build context with all required parameters
context = TaskContext(
    task_name="Entity_Matching_LLM",
    entity=entity,
    experiment_id=experiment_id if experiment_id else None,
    catalog_name=catalog_name,
    spark=spark,
    dbutils=dbutils,
    params={
        'llm_endpoint': llm_endpoint,
        'max_potential_matches': max_potential_matches,
        'attributes': attributes,
        'additional_instructions': additional_instructions,
        'base_prompt': base_prompt,
        'config_thresholds': config_thresholds,
        'unified_id_key': 'surrogate_key',
        'master_id_key': 'lakefusion_id',
        'merged_desc_column': 'attributes_combined',
        'llm_temperature': llm_temperature,
        "logger": logger
    }
)

# COMMAND ----------

# DBTITLE 1,Execute task
executor = LLMMatchingExecutor(context)

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
