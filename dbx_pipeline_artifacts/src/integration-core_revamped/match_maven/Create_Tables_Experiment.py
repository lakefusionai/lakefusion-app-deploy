# Databricks notebook source
# MAGIC %md
# MAGIC # Match Maven - Create Tables Experiment (SDK Version)
# MAGIC
# MAGIC Creates experiment tables for Match Maven testing with experiment ID suffix.
# MAGIC Supports both single-source (golden dedup) and multi-source (normal dedup) modes.
# MAGIC
# MAGIC **Customization Points:**
# MAGIC - `PRE-EXECUTE`: Add custom logic before table creation
# MAGIC - `POST-EXECUTE`: Add custom logic after table creation

# COMMAND ----------

import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("id_key", "", "ID Key")
dbutils.widgets.text("primary_key", "", "Primary Key")
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")
dbutils.widgets.text("entity_attributes_datatype", "", "Entity Attributes Datatype")
dbutils.widgets.text("dataset_tables", "", "Dataset Tables")
dbutils.widgets.text("is_single_source", "", "Is Single Source")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameter Extraction

# COMMAND ----------

entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=dbutils.widgets.get("entity"))
catalog_name = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "catalog_name", debugValue=dbutils.widgets.get("catalog_name"))
id_key = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "id_key", debugValue=dbutils.widgets.get("id_key"))
primary_key = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "primary_key", debugValue=dbutils.widgets.get("primary_key"))
entity_attributes = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes", debugValue=dbutils.widgets.get("entity_attributes"))
entity_attributes_datatype = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes_datatype", debugValue=dbutils.widgets.get("entity_attributes_datatype"))
dataset_tables = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "dataset_tables", debugValue=dbutils.widgets.get("dataset_tables"))
is_single_source = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "is_single_source", debugValue=dbutils.widgets.get("is_single_source"))
experiment_id = dbutils.widgets.get("experiment_id")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup LakeFusion Core Engine

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

logger.info("="*60)
logger.info("CREATE TABLES - MATCH MAVEN EXPERIMENT (SDK VERSION)")
logger.info("="*60)
logger.info(f"Entity: {entity}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment ID: {experiment_id}")
logger.info(f"Is Single Source: {is_single_source}")
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

from lakefusion_core_engine.executors import TaskContext
from lakefusion_core_engine.executors.tasks.integration_core.match_maven import CreateTablesExperimentExecutor

# Create task context
context = TaskContext(
    task_name="create_tables_experiment",
    entity=entity,
    experiment_id=experiment_id or None,
    catalog_name=catalog_name,
    spark=spark,
    dbutils=dbutils,
    params={
        "id_key": id_key or "lakefusion_id",
        "primary_key": primary_key,
        "entity_attributes": entity_attributes,
        "entity_attributes_datatype": entity_attributes_datatype,
        "dataset_tables": dataset_tables,
        "is_single_source": is_single_source,
        "logger": logger
    }
)

# DBTITLE 1,Execute task
executor = CreateTablesExperimentExecutor(context)

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

    if result.artifacts:
        logger.info(f"\nArtifacts:")
        for key, value in result.artifacts.items():
            logger.info(f"  {key}: {value}")

logger_instance.shutdown()
