# Databricks notebook source
# MAGIC %md
# MAGIC # Golden Dedup - Manual Merge (SDK Version)
# MAGIC
# MAGIC Processes manual merge requests for master-to-master merges in golden deduplication.
# MAGIC
# MAGIC **Customization Points:**
# MAGIC - `PRE-EXECUTE`: Add custom logic before processing merges
# MAGIC - `POST-EXECUTE`: Add custom logic after processing merges

# COMMAND ----------

import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("experiment_id", "", "Match Maven Experiment")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")
dbutils.widgets.text("entity_attributes_datatype", "", "Entity Attributes Datatype")
dbutils.widgets.text("default_survivorship_rules", "", "Survivorship Rules")
dbutils.widgets.text("dataset_objects", "", "Dataset Objects")
dbutils.widgets.text("master_id", "", "Master Record ID")
dbutils.widgets.text("match_record_id", "", "Match Record ID to merge into master")
dbutils.widgets.text("match_attributes", "", "Match Attributes")
dbutils.widgets.text("operation_type", "", "Action Type")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameter Extraction

# COMMAND ----------

experiment_id = dbutils.widgets.get("experiment_id")
entity = dbutils.widgets.get("entity")
catalog_name = dbutils.widgets.get("catalog_name")
master_id = dbutils.widgets.get("master_id")
match_record_id = dbutils.widgets.get("match_record_id")
operation_type = dbutils.widgets.get("operation_type")

# COMMAND ----------

# Get values from upstream tasks
entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
entity_attributes = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "entity_attributes",
    debugValue=dbutils.widgets.get("entity_attributes")
)
entity_attributes_datatype = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "entity_attributes_datatype",
    debugValue=dbutils.widgets.get("entity_attributes_datatype")
)
default_survivorship_rules = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "default_survivorship_rules",
    debugValue=dbutils.widgets.get("default_survivorship_rules")
)
dataset_objects = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "dataset_objects",
    debugValue=dbutils.widgets.get("dataset_objects")
)
match_attributes = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "match_attributes",
    debugValue=dbutils.widgets.get("match_attributes")
)

# COMMAND ----------

# Parse JSON parameters
entity_attributes = json.loads(entity_attributes) if isinstance(entity_attributes, str) else entity_attributes
entity_attributes_datatype = json.loads(entity_attributes_datatype) if isinstance(entity_attributes_datatype, str) else entity_attributes_datatype
default_survivorship_rules = json.loads(default_survivorship_rules) if isinstance(default_survivorship_rules, str) else default_survivorship_rules
dataset_objects = json.loads(dataset_objects) if isinstance(dataset_objects, str) else dataset_objects
match_attributes = json.loads(match_attributes) if isinstance(match_attributes, str) else match_attributes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup LakeFusion Core Engine

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils



# COMMAND ----------

logger.info("="*60)
logger.info("GOLDEN DEDUP - MANUAL MERGE (SDK VERSION)")
logger.info("="*60)
logger.info(f"Entity: {entity}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment: {experiment_id if experiment_id else 'prod'}")
logger.info(f"Master ID: {master_id}")
logger.info(f"Match Record ID: {match_record_id}")
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
from lakefusion_core_engine.executors.tasks.integration_core.golden_deduplication import GoldenManualMergeExecutor

# Create task context
context = TaskContext(
    task_name="golden_manual_merge",
    entity=entity,
    experiment_id=experiment_id or None,
    catalog_name=catalog_name,
    spark=spark,
    dbutils=dbutils,
    params={
        "master_id": master_id,
        "match_record_id": match_record_id,
        "operation_type": operation_type or "MASTER_MANUAL_MERGE",
        "entity_attributes": entity_attributes,
        "entity_attributes_datatype": entity_attributes_datatype,
        "default_survivorship_rules": default_survivorship_rules,
        "dataset_objects": dataset_objects,
        "match_attributes": match_attributes,
        "logger": logger
    }
)

logger.info(f"Master Table: {context.master_table}")

# COMMAND ----------

# DBTITLE 1,Execute task
executor = GoldenManualMergeExecutor(context)

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
