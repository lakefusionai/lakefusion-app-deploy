# Databricks notebook source
# MAGIC %md
# MAGIC # Increment Inserts to Unified (SDK Version)
# MAGIC
# MAGIC Processes INSERT incrementals from ALL source tables, transforms to unified schema,
# MAGIC and merges into unified table with proper deduplication.
# MAGIC
# MAGIC **Customization Points:**
# MAGIC - `PRE-EXECUTE`: Add custom logic before processing inserts
# MAGIC - `POST-EXECUTE`: Add custom logic after processing inserts

# COMMAND ----------

import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("primary_table", "", "Primary Table")
dbutils.widgets.text("primary_key", "", "Primary Key Column")
dbutils.widgets.text("entity_attributes", "[]", "Entity Attributes (JSON)")
dbutils.widgets.text("entity_attributes_datatype", "{}", "Entity Attributes Datatype (JSON)")
dbutils.widgets.text("attributes_mapping_json", "[]", "Attributes Mapping (JSON)")
dbutils.widgets.text("attributes", "[]", "Match Attributes (JSON)")
dbutils.widgets.text("experiment_id", "", "Experiment ID")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameter Extraction

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
entity = dbutils.widgets.get("entity")
primary_table = dbutils.widgets.get("primary_table")
primary_key = dbutils.widgets.get("primary_key")
entity_attributes = dbutils.widgets.get("entity_attributes")
entity_attributes_datatype = dbutils.widgets.get("entity_attributes_datatype")
attributes_mapping_json = dbutils.widgets.get("attributes_mapping_json")
attributes = dbutils.widgets.get("attributes")
experiment_id = dbutils.widgets.get("experiment_id")

# COMMAND ----------

# Get values from upstream tasks
entity = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="entity", debugValue=entity)
primary_table = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="primary_table", debugValue=primary_table)
primary_key = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="primary_key", debugValue=primary_key)
entity_attributes = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="entity_attributes", debugValue=entity_attributes)
entity_attributes_datatype = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="entity_attributes_datatype", debugValue=entity_attributes_datatype)
attributes_mapping_json = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="attributes_mapping", debugValue=attributes_mapping_json)
attributes = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="match_attributes", debugValue=attributes)
catalog_name = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="catalog_name", debugValue=catalog_name)

# Get info from Check_Increment_Exists
has_inserts = dbutils.jobs.taskValues.get(taskKey="Check_Increments_Exists", key="has_inserts", debugValue=True)
tables_with_inserts = dbutils.jobs.taskValues.get(taskKey="Check_Increments_Exists", key="tables_with_inserts", debugValue="[]")
table_version_info = dbutils.jobs.taskValues.get(taskKey="Check_Increments_Exists", key="table_version_info", debugValue="{}")

# COMMAND ----------

# Parse JSON parameters
entity_attributes = json.loads(entity_attributes) if isinstance(entity_attributes, str) else entity_attributes
entity_attributes_datatype = json.loads(entity_attributes_datatype) if isinstance(entity_attributes_datatype, str) else entity_attributes_datatype
attributes_mapping_json = json.loads(attributes_mapping_json) if isinstance(attributes_mapping_json, str) else attributes_mapping_json
attributes = json.loads(attributes) if isinstance(attributes, str) else attributes
tables_with_inserts = json.loads(tables_with_inserts) if isinstance(tables_with_inserts, str) else tables_with_inserts
table_version_info = json.loads(table_version_info) if isinstance(table_version_info, str) else table_version_info

# Convert has_inserts to boolean
if isinstance(has_inserts, str):
    has_inserts = has_inserts.lower() == 'true'

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
logger.info("INCREMENT INSERTS TO UNIFIED (SDK VERSION)")
logger.info("=" * 60)
logger.info(f"Entity: {entity}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment ID: {experiment_id or 'None'}")
logger.info(f"Has Inserts: {has_inserts}")
logger.info(f"Tables with Inserts: {len(tables_with_inserts)}")
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
from lakefusion_core_engine.executors.tasks.integration_core.incremental_load import IncrementInsertsExecutor

# Create task context
context = TaskContext(
    task_name="increment_inserts_to_unified",
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
        "attributes_mapping": attributes_mapping_json,
        "match_attributes": attributes,
        "has_inserts": has_inserts,
        "tables_with_inserts": tables_with_inserts,
        "table_version_info": table_version_info,
        "logger": logger
    }
)

# DBTITLE 1,Execute task
executor = IncrementInsertsExecutor(context)

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