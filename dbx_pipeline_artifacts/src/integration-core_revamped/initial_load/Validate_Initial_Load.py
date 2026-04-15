# Databricks notebook source
import json





# COMMAND ----------

# DBTITLE 1,Get parameters
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("dataset_tables", "", "Dataset Tables")
dbutils.widgets.text("attributes_mapping", "", "Attributes Mapping")
dbutils.widgets.text("primary_table", "", "Primary Table")
dbutils.widgets.text("primary_key", "", "Primary Key")
dbutils.widgets.text("experiment_id", "", "Experiment ID")

# COMMAND ----------

# DBTITLE 1,Get task values
entity = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="entity",
    debugValue=dbutils.widgets.get("entity")
)

catalog_name = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="catalog_name",
    debugValue=dbutils.widgets.get("catalog_name")
)

dataset_tables = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="dataset_tables",
    debugValue=dbutils.widgets.get("dataset_tables")
)

attributes_mapping = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="attributes_mapping",
    debugValue=dbutils.widgets.get("attributes_mapping")
)

primary_table = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="primary_table",
    debugValue=dbutils.widgets.get("primary_table")
)

primary_key = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="primary_key",
    debugValue=dbutils.widgets.get("primary_key")
)
experiment_id = dbutils.widgets.get("experiment_id")

# COMMAND ----------

# DBTITLE 1,Setup lakefusion core engine
# MAGIC %run ../../utils/execute_utils


# COMMAND ----------

# DBTITLE 1,Configuration
ENABLE_INTERACTIVE = False  # Set to True for step-by-step debugging
if ENABLE_INTERACTIVE:
    logger = _PrintLogger()
    logger.info("Interactive logging enabled — logger.info will print to console")


# COMMAND ----------

# DBTITLE 1,Import SDK components
from lakefusion_core_engine.executors import TaskContext
from lakefusion_core_engine.executors.tasks.integration_core.initial_load import ValidateInitialLoadExecutor


# COMMAND ----------

# DBTITLE 1,Create task context
context = TaskContext(
    spark=spark,
    dbutils=dbutils,
    catalog_name=catalog_name,
    entity=entity,
    task_name="Validate_Initial_Load",
    params={
        'entity': entity,
        'catalog_name': catalog_name,
        'dataset_tables': dataset_tables,
        'attributes_mapping': attributes_mapping,
        'primary_table': primary_table,
        'primary_key': primary_key,
        "logger": logger
    }
)

# COMMAND ----------

# DBTITLE 1,Execute task
executor = ValidateInitialLoadExecutor(context)

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