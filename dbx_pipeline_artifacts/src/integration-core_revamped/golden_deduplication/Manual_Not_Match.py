# Databricks notebook source
# MAGIC %md
# MAGIC # Golden Dedup - Manual Not Match (SDK Version)
# MAGIC
# MAGIC Processes manual not-match decisions to exclude specific master pairs from matching.
# MAGIC
# MAGIC **Customization Points:**
# MAGIC - `PRE-EXECUTE`: Add custom logic before processing not-match decisions
# MAGIC - `POST-EXECUTE`: Add custom logic after processing not-match decisions

# COMMAND ----------

import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("master_id", "", "Master Record ID")
dbutils.widgets.text("match_record_id", "", "Match Record ID to mark as NOT A MATCH")
dbutils.widgets.text("operation_type", "", "Action Type")

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
master_id = dbutils.widgets.get("master_id")
match_record_id = dbutils.widgets.get("match_record_id")
action_type = dbutils.widgets.get("operation_type")


%md
## Setup LakeFusion Core Engine

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils
# MAGIC
# MAGIC

# COMMAND ----------

logger.info("="*80)
logger.info("GOLDEN DEDUP - MANUAL NOT MATCH (SDK VERSION)")
logger.info("="*80)
logger.info(f"Entity: {entity}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment: {experiment_id if experiment_id else 'prod'}")
logger.info(f"Master ID: {master_id}")
logger.info(f"Match Record ID: {match_record_id}")
logger.info(f"Action Type: {action_type}")
logger.info("="*80)



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
from lakefusion_core_engine.executors.tasks.integration_core.golden_deduplication import GoldenManualNotMatchExecutor

# Create task context
context = TaskContext(
    task_name="golden_manual_not_match",
    entity=entity,
    experiment_id=experiment_id or None,
    catalog_name=catalog_name,
    spark=spark,
    dbutils=dbutils,
    params={
        "master_id": master_id,
        "match_record_id": match_record_id,
        "action_type": action_type or "MANUAL_NOT_A_MATCH",
        "logger": logger
    }
)

logger.info(f"Master Table: {context.master_table}")

# COMMAND ----------

# DBTITLE 1,Execute task
executor = GoldenManualNotMatchExecutor(context)

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
