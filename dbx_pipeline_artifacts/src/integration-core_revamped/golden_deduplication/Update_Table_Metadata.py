# Databricks notebook source
# MAGIC %md
# MAGIC # Golden Dedup - Update Table Metadata (SDK Version)
# MAGIC
# MAGIC Updates the last processed version in metadata table for all tables
# MAGIC involved in golden deduplication pipeline.
# MAGIC
# MAGIC **Customization Points:**
# MAGIC - `PRE-EXECUTE`: Add custom logic before updating metadata
# MAGIC - `POST-EXECUTE`: Add custom logic after updating metadata

# COMMAND ----------

import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("dataset_tables", "", "Dataset Tables")
dbutils.widgets.text("experiment_id", "", "Match Maven Experiment ID")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("is_golden_deduplication", "", "Is Golden Deduplication")
dbutils.widgets.text("meta_info_table", "", "Meta Info Table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameter Extraction

# COMMAND ----------

entity = dbutils.widgets.get("entity")
dataset_tables = dbutils.widgets.get("dataset_tables")
experiment_id = dbutils.widgets.get("experiment_id")
catalog_name = dbutils.widgets.get("catalog_name")
is_golden_deduplication = dbutils.widgets.get("is_golden_deduplication")
meta_info_table = dbutils.widgets.get("meta_info_table")

# COMMAND ----------

# Get values from upstream tasks
entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
dataset_tables = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "dataset_tables", debugValue=dataset_tables)
is_golden_deduplication = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "is_golden_deduplication", debugValue=is_golden_deduplication)

# COMMAND ----------

# Parse dataset tables if provided
if dataset_tables and isinstance(dataset_tables, str):
    dataset_tables = json.loads(dataset_tables)
elif not dataset_tables:
    dataset_tables = []


# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup LakeFusion Core Engine

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils



# COMMAND ----------

logger.info("="*60)
logger.info("GOLDEN DEDUP - UPDATE TABLE METADATA (SDK VERSION)")
logger.info("="*60)
logger.info(f"Entity: {entity}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment: {experiment_id if experiment_id else 'prod'}")
logger.info(f"Dataset Tables: {len(dataset_tables)}")
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
from lakefusion_core_engine.executors.tasks.integration_core.golden_deduplication import GoldenUpdateTableMetadataExecutor

# Create task context
context = TaskContext(
    task_name="golden_update_table_metadata",
    entity=entity,
    experiment_id=experiment_id or None,
    catalog_name=catalog_name,
    spark=spark,
    dbutils=dbutils,
    params={
        "dataset_tables": dataset_tables,
        "is_golden_deduplication": is_golden_deduplication,
        "meta_info_table": meta_info_table,
        "logger": logger
    }
)

logger.info(f"Master Table: {context.master_table}")

# COMMAND ----------

# DBTITLE 1,Execute task
executor = GoldenUpdateTableMetadataExecutor(context)

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