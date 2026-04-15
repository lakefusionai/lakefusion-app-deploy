# Databricks notebook source
# MAGIC %md
# MAGIC # Golden Dedup - Process Unified Dedup Table (SDK Version)
# MAGIC
# MAGIC Creates and maintains the unified_deduplicate table for golden deduplication.
# MAGIC This table mirrors the master table with additional search_results and scoring_results columns.
# MAGIC
# MAGIC **Customization Points:**
# MAGIC - `PRE-EXECUTE`: Add custom logic before processing unified dedup table
# MAGIC - `POST-EXECUTE`: Add custom logic after processing unified dedup table

# COMMAND ----------

import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("experiment_id", "", "Match Maven Experiment")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("meta_info_table", "", "Metadata Table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameter Extraction

# COMMAND ----------

experiment_id = dbutils.widgets.get("experiment_id")
entity = dbutils.widgets.get("entity")
catalog_name = dbutils.widgets.get("catalog_name")
meta_info_table = dbutils.widgets.get("meta_info_table")

# COMMAND ----------

# Get values from upstream tasks
entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
meta_info_table = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="meta_info_table", debugValue=meta_info_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup LakeFusion Core Engine

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils



# COMMAND ----------

logger.info("="*60)
logger.info("GOLDEN DEDUP - PROCESS UNIFIED DEDUP TABLE (SDK VERSION)")
logger.info("="*60)
logger.info(f"Entity: {entity}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment: {experiment_id if experiment_id else 'prod'}")
logger.info(f"Meta Info Table: {meta_info_table}")
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
from lakefusion_core_engine.executors.tasks.integration_core.golden_deduplication import ProcessUnifiedDedupTableExecutor

# Create task context
context = TaskContext(
    task_name="process_unified_dedup_table",
    entity=entity,
    experiment_id=experiment_id or None,
    catalog_name=catalog_name,
    spark=spark,
    dbutils=dbutils,
    params={
        "meta_info_table": meta_info_table,
        "master_id_key": "lakefusion_id",
        "logger": logger
    }
)

logger.info(f"Master Table: {context.master_table}")

# COMMAND ----------

# DBTITLE 1,Execute task
executor = ProcessUnifiedDedupTableExecutor(context)

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