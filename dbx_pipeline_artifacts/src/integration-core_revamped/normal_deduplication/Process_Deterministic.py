# Databricks notebook source
import json

# COMMAND ----------

# Get widgets
dbutils.widgets.text("experiment_id", "", "Match Maven Experiment")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("attributes", "", "Merged Description Attributes")
dbutils.widgets.text("catalog_name", "", "catalog name")
dbutils.widgets.text("entity_attributes_datatype", "", "entity_attributes_datatype")
dbutils.widgets.text("is_golden_deduplication", "", "is_golden_deduplication")
dbutils.widgets.text("deterministic_rules", "", "Deterministic Rules JSON")

# COMMAND ----------

# Get widget values
attributes = dbutils.widgets.get("attributes")
catalog_name = dbutils.widgets.get("catalog_name")
entity = dbutils.widgets.get("entity")
experiment_id = dbutils.widgets.get("experiment_id")
entity_attributes_datatype = dbutils.widgets.get("entity_attributes_datatype")
is_golden_deduplication = dbutils.widgets.get("is_golden_deduplication")
deterministic_rules = dbutils.widgets.get("deterministic_rules")

# COMMAND ----------

# Get task values from upstream tasks
entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
entity_attributes_datatype = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes_datatype", debugValue=entity_attributes_datatype)
attributes = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="match_attributes", debugValue=attributes)
is_golden_deduplication = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="is_golden_deduplication", debugValue=is_golden_deduplication)
rules_config = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="deterministic_rules", debugValue=deterministic_rules)

# COMMAND ----------

# Parse JSON parameters
entity_attributes_datatype = json.loads(entity_attributes_datatype) if isinstance(entity_attributes_datatype, str) else entity_attributes_datatype
attributes = json.loads(attributes) if isinstance(attributes, str) else attributes
rules_config = json.loads(rules_config)

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils



# COMMAND ----------

# DBTITLE 1,Configuration
ENABLE_INTERACTIVE = False  # Set to True for step-by-step debugging
if ENABLE_INTERACTIVE:
    logger = _PrintLogger()
    logger.info("Interactive logging enabled — logger.info will print to console")


# COMMAND ----------

# Import SDK components - use qualified imports for namespace clarity
from lakefusion_core_engine.executors import TaskContext
from lakefusion_core_engine.executors.tasks.integration_core.normal_deduplication import ProcessDeterministicExecutor

# COMMAND ----------

# Build context with all required parameters
context = TaskContext(
    task_name="Process_Deterministic",
    entity=entity,
    experiment_id=experiment_id if experiment_id else None,
    catalog_name=catalog_name,
    spark=spark,
    dbutils=dbutils,
    params={
        'attributes': attributes,
        "logger": logger,
        'entity_attributes_datatype': entity_attributes_datatype,
        'is_golden_deduplication': is_golden_deduplication,
        'rules_config': rules_config
    }
)

# COMMAND ----------

# DBTITLE 1,Execute task
executor = ProcessDeterministicExecutor(context)

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
