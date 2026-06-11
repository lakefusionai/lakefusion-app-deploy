# Databricks notebook source
# MAGIC %md
# MAGIC # Golden Dedup - Process Potential Match (SDK Version)
# MAGIC
# MAGIC Identifies and stages potential master-to-master matches for review.
# MAGIC
# MAGIC **Customization Points:**
# MAGIC - `PRE-EXECUTE`: Add custom logic before processing potential matches
# MAGIC - `POST-EXECUTE`: Add custom logic after processing potential matches

# COMMAND ----------

import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameter Extraction

# COMMAND ----------

entity = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "entity",
    debugValue=dbutils.widgets.get("entity")
)
catalog_name = dbutils.widgets.get("catalog_name")
experiment_id = dbutils.widgets.get("experiment_id")
entity_attributes = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "entity_attributes",
    debugValue=dbutils.widgets.get("entity_attributes")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup LakeFusion Core Engine

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils



# COMMAND ----------

# Parse JSON parameters
entity_attributes = json.loads(entity_attributes)

# COMMAND ----------

logger.info("="*60)
logger.info("GOLDEN DEDUP - PROCESS POTENTIAL MATCH (SDK VERSION)")
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
from lakefusion_core_engine.executors.tasks.integration_core.golden_deduplication import GoldenProcessPotentialMatchExecutor

# Create task context
context = TaskContext(
    task_name="golden_process_potential_match",
    entity=entity,
    experiment_id=experiment_id or None,
    catalog_name=catalog_name,
    spark=spark,
    dbutils=dbutils,
    params={
        "entity_attributes": entity_attributes,
        "master_id_key": "lakefusion_id",
        "logger": logger
    }
)

logger.info(f"Master Table: {context.master_table}")

# COMMAND ----------

# DBTITLE 1,Execute task
executor = GoldenProcessPotentialMatchExecutor(context)

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