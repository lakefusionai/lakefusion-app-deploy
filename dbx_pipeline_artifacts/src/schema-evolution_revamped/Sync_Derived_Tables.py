# Databricks notebook source
import json

# COMMAND ----------

# MAGIC %run ../utils/taskvalues_enum

# COMMAND ----------

# Widgets
dbutils.widgets.text("se_job_id", "", "Schema Evolution Job ID")
dbutils.widgets.text("entity_id", "", "Entity ID")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "prod", "Experiment ID")

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
entity_id = dbutils.widgets.get("entity_id")
experiment_id = dbutils.widgets.get("experiment_id") or "prod"

# COMMAND ----------

# Get task values from Parse step
entity_name = dbutils.jobs.taskValues.get(
    taskKey="Parse_Schema_Evolution_JSON",
    key=TaskValueKey.SCHEMA_EVOLUTION_ENTITY_NAME.value,
    debugValue=""
)

edits_json = dbutils.jobs.taskValues.get(
    taskKey="Parse_Schema_Evolution_JSON",
    key=TaskValueKey.SCHEMA_EVOLUTION_EDITS.value,
    debugValue="[]"
)

# COMMAND ----------

# MAGIC %run ../utils/execute_utils

# COMMAND ----------

setup_lakefusion_engine()

# COMMAND ----------

# DBTITLE 1,Configuration
ENABLE_INTERACTIVE = False  # Set to True for step-by-step debugging

# COMMAND ----------

# Import SDK components
from lakefusion_core_engine.executors import TaskContext
from lakefusion_core_engine.executors.tasks.schema_evolution import SyncDerivedTablesExecutor

# COMMAND ----------

# Build context
context = TaskContext(
    task_name="Sync_Derived_Tables",
    entity=entity_name,
    experiment_id=experiment_id,
    catalog_name=catalog_name,
    spark=spark,
    dbutils=dbutils,
    params={
        'entity_id': entity_id,
        'edits': edits_json,
        "logger": logger,
    }
)

# COMMAND ----------

# DBTITLE 1,Execute task
executor = SyncDerivedTablesExecutor(context)

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

# DBTITLE 1,Display result
if not ENABLE_INTERACTIVE:
    logger.info(f"Status: {result.status.value}")
    logger.info(f"Message: {result.message}")
    if result.metrics:
        logger.info(f"Metrics: {result.metrics}")

# COMMAND ----------

# Shutdown logger
logger_instance.shutdown()
