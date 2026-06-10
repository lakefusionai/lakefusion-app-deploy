# Databricks notebook source
# MAGIC %md
# MAGIC # Check Increment Exists (SDK Version)
# MAGIC
# MAGIC Checks if ANY source table has new changes (INSERT/UPDATE/DELETE) since last processed version.
# MAGIC Sets flags to control which downstream notebooks should run.
# MAGIC
# MAGIC **Customization Points:**
# MAGIC - `PRE-EXECUTE`: Add custom logic before checking
# MAGIC - `POST-EXECUTE`: Add custom logic after checking

# COMMAND ----------

import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("primary_table", "", "Primary Table")
dbutils.widgets.text("dataset_tables", "[]", "Dataset Tables (JSON)")
dbutils.widgets.text("experiment_id", "", "Experiment ID")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameter Extraction

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
entity = dbutils.widgets.get("entity")
dataset_tables = dbutils.widgets.get("dataset_tables")
experiment_id = dbutils.widgets.get("experiment_id")

# COMMAND ----------

# Get values from upstream tasks
entity = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="entity", debugValue=entity)
primary_table = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="primary_table", debugValue=dbutils.widgets.get("primary_table"))
dataset_tables = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="dataset_tables", debugValue=dataset_tables)
catalog_name = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="catalog_name", debugValue=catalog_name)

# COMMAND ----------

# Parse parameters
dataset_tables = json.loads(dataset_tables) if isinstance(dataset_tables, str) and dataset_tables else []

# Remove experiment_id hyphens
if experiment_id:
    experiment_id = experiment_id.replace("-", "")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup LakeFusion Core Engine

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils



# COMMAND ----------

logger.info("=" * 60)
logger.info("CHECK INCREMENT EXISTS (SDK VERSION)")
logger.info("=" * 60)
logger.info(f"Entity: {entity}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment ID: {experiment_id or 'None'}")
logger.info(f"Source Tables: {len(dataset_tables)}")
logger.info("=" * 60)



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
from lakefusion_core_engine.executors.tasks.integration_core.incremental_load import CheckIncrementExistsExecutor

# Create task context
context = TaskContext(
    task_name="check_increment_exists",
    entity=entity,
    experiment_id=experiment_id or None,
    catalog_name=catalog_name,
    spark=spark,
    dbutils=dbutils,
    params={
        "dataset_tables": dataset_tables,
        "logger": logger
    }
)

# DBTITLE 1,Execute task
executor = CheckIncrementExistsExecutor(context)

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