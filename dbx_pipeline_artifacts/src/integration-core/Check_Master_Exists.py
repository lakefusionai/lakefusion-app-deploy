# Databricks notebook source
# Get parameters from widgets
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")

# COMMAND ----------

# Get task values from Parse_Entity_Model
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

experiment_id = dbutils.widgets.get("experiment_id")

# COMMAND ----------

# MAGIC %run ../utils/execute_utils

# COMMAND ----------

logger.info("="*60)
logger.info("CHECK MASTER EXISTS")
logger.info("="*60)
logger.info(f"Entity: {entity}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment: {experiment_id if experiment_id else 'prod'}")
logger.info("="*60)

# COMMAND ----------

# Construct table names
experiment_suffix = f"_{experiment_id}" if experiment_id else ""

master_table = f"{catalog_name}.gold.{entity}_master{experiment_suffix}"
unified_table = f"{catalog_name}.silver.{entity}_unified{experiment_suffix}"
merge_activities_table = f"{master_table}_merge_activities"
attribute_version_sources_table = f"{master_table}_attribute_version_sources"

logger.info(f"\nTable Names:")
logger.info(f"  Master: {master_table}")
logger.info(f"  Unified: {unified_table}")
logger.info(f"  Merge Activities: {merge_activities_table}")
logger.info(f"  Attribute Version Sources: {attribute_version_sources_table}")

# COMMAND ----------

def table_exists(table_name):
    """
    Check if a table exists in the catalog.
    
    Args:
        table_name: Fully qualified table name (catalog.schema.table)
        
    Returns:
        True if table exists, False otherwise
    """
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}")
        return True
    except Exception as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "does not exist" in str(e).lower():
            return False
        else:
            # Some other error occurred
            raise e

# COMMAND ----------

# Check if master table exists
master_exists = table_exists(master_table)
unified_exists = table_exists(unified_table)

logger.info(f"\nTable Existence Check:")
logger.info(f"  Master Table Exists: {master_exists}")
logger.info(f"  Unified Table Exists: {unified_exists}")

# COMMAND ----------

# Determine if this is initial load
is_initial_load = not master_exists

# Validation: If master exists, unified should also exist
if master_exists and not unified_exists and experiment_id == 'prod':
    raise ValueError(
        f"⚠️ Inconsistent state: Master table exists but Unified table does not!\n"
        f"   Master: {master_table} (exists)\n"
        f"   Unified: {unified_table} (missing)\n"
        f"   This indicates a corrupted setup. Please investigate."
    )

# Validation: If unified exists, master should also exist
if unified_exists and not master_exists:
    raise ValueError(
        f"⚠️ Inconsistent state: Unified table exists but Master table does not!\n"
        f"   Unified: {unified_table} (exists)\n"
        f"   Master: {master_table} (missing)\n"
        f"   This indicates a corrupted setup. Please investigate."
    )

# COMMAND ----------

# Set task value for downstream notebooks
dbutils.jobs.taskValues.set("is_initial_load", is_initial_load)

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("LOAD TYPE DETERMINATION")
logger.info("="*60)

if is_initial_load:
    logger.info("INITIAL LOAD")
    logger.info("   → Master table does not exist")
    logger.info("   → Will create all tables")
    logger.info("   → Will load primary and secondary sources")
    logger.info("   → Proceeding to table creation...")
else:
    logger.info("INCREMENTAL LOAD")
    logger.info("   → Master table exists")
    logger.info("   → Will skip table creation")
    logger.info("   → Will process incremental changes")
    logger.info("   → Exiting initial load pipeline...")

logger.info("="*60)

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("CHECK MASTER EXISTS - COMPLETE")
logger.info("="*60)
logger.info(f"Result: {'INITIAL LOAD' if is_initial_load else 'INCREMENTAL LOAD'}")
logger.info(f"Next Step: {'Create Tables' if is_initial_load else 'Incremental Pipeline'}")
logger.info("="*60)

# COMMAND ----------

logger_instance.shutdown()
