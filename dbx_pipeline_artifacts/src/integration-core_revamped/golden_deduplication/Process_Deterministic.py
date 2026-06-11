# Databricks notebook source
# MAGIC %md
# MAGIC # Golden Dedup - Process Deterministic (SDK Version)
# MAGIC
# MAGIC Applies match rules for master-to-master deduplication.
# MAGIC
# MAGIC **Customization Points:**
# MAGIC - `PRE-EXECUTE`: Add custom logic before processing deterministic rules
# MAGIC - `POST-EXECUTE`: Add custom logic after processing deterministic rules

# COMMAND ----------

import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("experiment_id", "", "Match Maven Experiment")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("attributes", "", "Merged Description Attributes")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("entity_attributes_datatype", "", "Entity Attributes Datatype")
dbutils.widgets.text("is_golden_deduplication", "", "Is Golden Deduplication")
dbutils.widgets.text("deterministic_rules", "", "Deterministic Rules JSON")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameter Extraction

# COMMAND ----------

attributes = dbutils.widgets.get("attributes")
catalog_name = dbutils.widgets.get("catalog_name")
entity = dbutils.widgets.get("entity")
experiment_id = dbutils.widgets.get("experiment_id")
entity_attributes_datatype = dbutils.widgets.get("entity_attributes_datatype")
deterministic_rules = dbutils.widgets.get("deterministic_rules")

# COMMAND ----------

entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
entity_attributes_datatype = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes_datatype", debugValue=entity_attributes_datatype)
attributes = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="match_attributes", debugValue=attributes)
rules_config = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="deterministic_rules", debugValue=deterministic_rules)
is_golden_deduplication = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="is_golden_deduplication", debugValue=dbutils.widgets.get("is_golden_deduplication"))

# COMMAND ----------

# Parse JSON parameters
entity_attributes_datatype = json.loads(entity_attributes_datatype)
attributes = json.loads(attributes)
rules_config = json.loads(rules_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup LakeFusion Core Engine

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils



# COMMAND ----------

logger.info("="*60)
logger.info("GOLDEN DEDUP - PROCESS DETERMINISTIC (SDK VERSION)")
logger.info("="*60)
logger.info(f"Entity: {entity}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment: {experiment_id if experiment_id else 'prod'}")
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
from lakefusion_core_engine.executors.tasks.integration_core.golden_deduplication import GoldenProcessDeterministicExecutor

# Create task context
context = TaskContext(
    task_name="Process_Golden_Deterministic",
    entity=entity,
    experiment_id=experiment_id or None,
    catalog_name=catalog_name,
    spark=spark,
    dbutils=dbutils,
    params={
        "attributes": attributes,
        "entity_attributes_datatype": entity_attributes_datatype,
        "master_id_key": "lakefusion_id",
        "rules_config": rules_config,
        "logger": logger
    }
)

logger.info(f"Master Table: {context.master_table}")

# COMMAND ----------

# DBTITLE 1,Execute task
executor = GoldenProcessDeterministicExecutor(context)

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
