# Databricks notebook source
# MAGIC %md
# MAGIC # Match Maven - Load Experiment Data (SDK Version)
# MAGIC
# MAGIC Loads data for Match Maven experiments with record range limits.
# MAGIC Supports both single-source (golden dedup) and multi-source (normal dedup) modes.
# MAGIC
# MAGIC **Customization Points:**
# MAGIC - `PRE-EXECUTE`: Add custom logic before loading data
# MAGIC - `POST-EXECUTE`: Add custom logic after loading data

# COMMAND ----------

import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("processed_records", "", "Processed Records Range")
dbutils.widgets.text("primary_table", "", "Primary Table")
dbutils.widgets.text("primary_key", "", "Primary Key")
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")
dbutils.widgets.text("entity_attributes_datatype", "", "Entity Attributes Datatype")
dbutils.widgets.text("match_attributes", "", "Match Attributes")
dbutils.widgets.text("attributes_mapping", "", "Attributes Mapping")
dbutils.widgets.text("dataset_tables", "", "Dataset Tables")
dbutils.widgets.text("is_single_source", "", "Is Single Source")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameter Extraction

# COMMAND ----------

entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=dbutils.widgets.get("entity"))
catalog_name = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "catalog_name", debugValue=dbutils.widgets.get("catalog_name"))
primary_table = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "primary_table", debugValue=dbutils.widgets.get("primary_table"))
primary_key = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "primary_key", debugValue=dbutils.widgets.get("primary_key"))
entity_attributes = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes", debugValue=dbutils.widgets.get("entity_attributes"))
entity_attributes_datatype = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes_datatype", debugValue=dbutils.widgets.get("entity_attributes_datatype"))
match_attributes = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "match_attributes", debugValue=dbutils.widgets.get("match_attributes"))
attributes_mapping = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "attributes_mapping", debugValue=dbutils.widgets.get("attributes_mapping"))
dataset_tables = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "dataset_tables", debugValue=dbutils.widgets.get("dataset_tables"))
is_single_source = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "is_single_source", debugValue=dbutils.widgets.get("is_single_source"))
processed_records = dbutils.widgets.get("processed_records")
experiment_id = dbutils.widgets.get("experiment_id")

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

logger.info("="*60)
logger.info("LOAD EXPERIMENT DATA (SDK VERSION)")
logger.info("="*60)
logger.info(f"Entity: {entity}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment ID: {experiment_id}")
logger.info(f"Is Single Source: {is_single_source}")
logger.info(f"Processed Records: {processed_records}")
logger.info("="*60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup LakeFusion Core Engine




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
from lakefusion_core_engine.executors.tasks.integration_core.match_maven import LoadExperimentDataExecutor

# Create task context
context = TaskContext(
    task_name="load_experiment_data",
    entity=entity,
    experiment_id=experiment_id or None,
    catalog_name=catalog_name,
    spark=spark,
    dbutils=dbutils,
    params={
        "primary_table": primary_table,
        "primary_key": primary_key,
        "entity_attributes": entity_attributes,
        "entity_attributes_datatype": entity_attributes_datatype,
        "match_attributes": match_attributes,
        "attributes_mapping": attributes_mapping,
        "dataset_tables": dataset_tables,
        "is_single_source": is_single_source,
        "processed_records": processed_records,
        "logger": logger
    }
)

# DBTITLE 1,Execute task
executor = LoadExperimentDataExecutor(context)

try:
    if not ENABLE_INTERACTIVE:
        result = executor.run()
        logger.info("running executor.run")
    else:
        executor.interactive()
        logger.info("running executor.interactive")
except Exception as e:
    logger.error(f"Error running executor: {e}")
    logger_instance.shutdown()
    raise

# COMMAND ----------

if not ENABLE_INTERACTIVE:
    logger.info(f"Status: {result.status.value}")
    logger.info(f"Message: {result.message}")

    if result.metrics:
        logger.info(f"\nMetrics:")
        for key, value in result.metrics.items():
            logger.info(f"  {key}: {value}")

    if result.task_values:
        logger.info(f"\nTask Values (for downstream):")
        for key, value in result.task_values.items():
            logger.info(f"  {key}: {value}")

logger_instance.shutdown()

