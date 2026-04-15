# Databricks notebook source
"""
PURPOSE: Check if ANY source table (primary + secondary) has new changes (INSERT/UPDATE/DELETE)
         Exit early if no incrementals exist to avoid unnecessary processing
         
OPTIMIZATION: Single pass through CDF to check all change types at once
              Sets flags to control which downstream notebooks should run
"""

import json
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, count, when
from datetime import datetime

# COMMAND ----------

# WIDGETS
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("primary_table", "", "Primary Table")
dbutils.widgets.text("dataset_tables", "", "Dataset Tables (JSON)")
dbutils.widgets.text("dataset_objects", "{}", "Dataset Objects (JSON)")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("catalog_name", "", "Catalog Name")

# COMMAND ----------

# READ PARAMETERS
entity = dbutils.widgets.get("entity")
primary_table = dbutils.widgets.get("primary_table")
dataset_tables = dbutils.widgets.get("dataset_tables")
dataset_objects = dbutils.widgets.get("dataset_objects")
experiment_id = dbutils.widgets.get("experiment_id")
catalog_name = dbutils.widgets.get("catalog_name")

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

logger.info(f"Entity: {entity}")
logger.info(f"Primary Table: {primary_table}")
logger.info(f"Dataset Tables: {dataset_tables}")
logger.info(f"Dataset Objects: {dataset_objects}")
logger.info(f"Experiment ID: {experiment_id}")
logger.info(f"Catalog Name: {catalog_name}")

# COMMAND ----------

# OVERRIDE FROM PARSE TASK
entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
primary_table = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "primary_table", debugValue=primary_table)
dataset_tables = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "dataset_tables", debugValue=dataset_tables)
dataset_objects = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "dataset_objects", debugValue=dataset_objects)

logger.info(f"Entity: {entity}")
logger.info(f"Primary Table: {primary_table}")
logger.info(f"Dataset Tables: {dataset_tables}")
logger.info(f"Dataset Objects: {dataset_objects}")

# COMMAND ----------

# PARSE JSON
dataset_tables = json.loads(dataset_tables)
dataset_objects = json.loads(dataset_objects) if isinstance(dataset_objects, str) else dataset_objects

# Remove experiment_id hyphens
if experiment_id:
    experiment_id = experiment_id.replace("-", "")

# Resolve table paths: use _cleaned suffix when quality_task_enabled is true
def resolve_table_path(source_table):
    """If DQ is enabled for this dataset, check the _cleaned table instead."""
    obj_info = dataset_objects.get(source_table, {})
    if obj_info.get("quality_task_enabled", False):
        return f"{source_table}_cleaned"
    return source_table

resolved_dataset_tables = [resolve_table_path(t) for t in dataset_tables]

logger.info(f"All Source Tables (original): {dataset_tables}")
logger.info(f"All Source Tables (resolved): {resolved_dataset_tables}")
dq_enabled_tables = [t for t in dataset_tables if resolve_table_path(t) != t]
if dq_enabled_tables:
    logger.info(f"DQ enabled tables (using _cleaned): {dq_enabled_tables}")


# COMMAND ----------

# TABLE NAMES
meta_info_table = f"{catalog_name}.silver.table_meta_info"
unified_table = f"{catalog_name}.silver.{entity}_unified"
if experiment_id:
    unified_table += f"_{experiment_id}"

logger.info(f"Meta Info Table: {meta_info_table}")
logger.info(f"Unified Table: {unified_table}")

# COMMAND ----------

# HELPER FUNCTIONS

def get_last_processed_version(table_path: str, entity_name: str):
    """Get last processed version from meta_info_table"""
    try:
        if not spark.catalog.tableExists(meta_info_table):
            logger.error(f"Meta info table doesn't exist: {meta_info_table}")
            logger.error("Meta info table must be created by Prepare Tables notebook first")
            raise Exception(f"Meta info table {meta_info_table} does not exist. Run Prepare Tables notebook first.")
        
        meta_df = spark.read.table(meta_info_table) \
            .filter(col("table_name") == table_path) \
            .filter(col("entity_name") == entity_name)
        
        if meta_df.count() > 0:
            return meta_df.select("last_processed_version").first()[0]
        return None
    except Exception as e:
        logger.error(f"Error reading last_processed_version for {table_path}: {e}")
        raise

def get_current_version(table_path: str):
    """Get current version of Delta table"""
    try:
        history = spark.sql(f"DESCRIBE HISTORY {table_path}").selectExpr("max(version)").first()[0]
        return history
    except Exception as e:
        logger.error(f"Error getting version for {table_path}: {e}")
        return None

def is_cdf_enabled(table_path: str):
    """Check if Change Data Feed is enabled on table"""
    try:
        props = spark.sql(f"SHOW TBLPROPERTIES {table_path}").collect()
        for prop in props:
            if prop['key'] == 'delta.enableChangeDataFeed' and prop['value'].lower() == 'true':
                return True
        return False
    except Exception as e:
        logger.error(f"Error checking CDF status for {table_path}: {e}")
        return False

def check_table_changes(table_path: str, entity_name: str):
    """
    Check if table has any changes (INSERT/UPDATE/DELETE) since last processed version
    Returns: (has_inserts, has_updates, has_deletes, starting_version, current_version, counts)
    """
    try:
        # Check if table exists
        if not spark.catalog.tableExists(table_path):
            logger.warning(f"Table {table_path} doesn't exist")
            return False, False, False, 0, 0, {"inserts": 0, "updates": 0, "deletes": 0}
        
        # Check if CDF is enabled
        if not is_cdf_enabled(table_path):
            logger.warning(f"CDF not enabled on {table_path} - skipping")
            return False, False, False, 0, 0, {"inserts": 0, "updates": 0, "deletes": 0}
        
        last_version = get_last_processed_version(table_path, entity_name)
        current_version = get_current_version(table_path)
        
        if current_version is None:
            logger.warning(f"Table {table_path} has no versions")
            return False, False, False, 0, 0, {"inserts": 0, "updates": 0, "deletes": 0}
        
        # If no last version, start from 0 (initial load already done, this is first incremental)
        starting_version = (last_version + 1) if last_version is not None else 0
        
        # No new versions
        if current_version < starting_version:
            logger.info(f"{table_path}: No new versions (current={current_version}, last_processed={last_version})")
            return False, False, False, starting_version, current_version, {"inserts": 0, "updates": 0, "deletes": 0}
        
        logger.info(f"{table_path}: Checking versions {starting_version} to {current_version}")
        
        # Read CDF once for all change types
        df_cdf = spark.read.format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingVersion", starting_version) \
            .option("endingVersion", current_version) \
            .table(table_path)
        
        # Count all change types in a single pass
        change_counts = df_cdf.groupBy().agg(
            count(when(col("_change_type") == "insert", 1)).alias("inserts"),
            count(when(col("_change_type") == "update_postimage", 1)).alias("updates"),
            count(when(col("_change_type") == "delete", 1)).alias("deletes")
        ).collect()[0]
        
        insert_count = change_counts["inserts"]
        update_count = change_counts["updates"]
        delete_count = change_counts["deletes"]
        
        has_inserts = insert_count > 0
        has_updates = update_count > 0
        has_deletes = delete_count > 0
        
        # Log results
        if has_inserts or has_updates or has_deletes:
            logger.info(f"{table_path}: Found changes:")
            if has_inserts:
                logger.info(f"   - {insert_count} INSERT records")
            if has_updates:
                logger.info(f"   - {update_count} UPDATE records")
            if has_deletes:
                logger.info(f"   - {delete_count} DELETE records")
        else:
            logger.info(f"{table_path}: No changes found")
        
        counts = {
            "inserts": insert_count,
            "updates": update_count,
            "deletes": delete_count
        }
        
        return has_inserts, has_updates, has_deletes, starting_version, current_version, counts
            
    except Exception as e:
        logger.error(f"Error checking {table_path}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False, False, False, 0, 0, {"inserts": 0, "updates": 0, "deletes": 0}

# COMMAND ----------

# MAIN LOGIC: CHECK ALL SOURCE TABLES FOR ALL CHANGE TYPES
logger.info("=" * 80)
logger.info(f"Checking incrementals for entity: {entity}")
logger.info(f"Source tables to check: {len(resolved_dataset_tables)}")
logger.info("=" * 80)

tables_with_inserts = []
tables_with_updates = []
tables_with_deletes = []
table_version_info = {}

total_inserts = 0
total_updates = 0
total_deletes = 0



# COMMAND ----------

for source_table in resolved_dataset_tables:
    has_inserts, has_updates, has_deletes, start_ver, current_ver, counts = check_table_changes(source_table, entity)
    
    # CRITICAL: Store both last_processed_version AND starting_version
    # last_processed_version = the version that was already processed
    # starting_version = the NEXT version to read (last_processed + 1)
    last_processed = start_ver - 1 if start_ver > 0 else None
    
    table_version_info[source_table] = {
        "last_processed_version": last_processed,
        "starting_version": start_ver,  # ADDED for deduplication logic
        "current_version": current_ver,
        "has_inserts": has_inserts,
        "has_updates": has_updates,
        "has_deletes": has_deletes,
        "insert_count": counts["inserts"],
        "update_count": counts["updates"],
        "delete_count": counts["deletes"]
    }
    
    if has_inserts:
        tables_with_inserts.append(source_table)
        total_inserts += counts["inserts"]
    
    if has_updates:
        tables_with_updates.append(source_table)
        total_updates += counts["updates"]
    
    if has_deletes:
        tables_with_deletes.append(source_table)
        total_deletes += counts["deletes"]

# COMMAND ----------

# SUMMARY AND DECISION
logger.info("\n" + "=" * 80)
logger.info("INCREMENTAL CHECK SUMMARY")
logger.info("=" * 80)

has_any_changes = len(tables_with_inserts) > 0 or len(tables_with_updates) > 0 or len(tables_with_deletes) > 0

if not has_any_changes:
    logger.info("NO CHANGES found in any source table")
    logger.info("Exiting early - no processing needed")
    logger.info("=" * 80)
    
    dbutils.jobs.taskValues.set(key="status", value="NO_CHANGES")
    dbutils.jobs.taskValues.set(key="has_inserts", value=False)
    dbutils.jobs.taskValues.set(key="has_updates", value=False)
    dbutils.jobs.taskValues.set(key="has_deletes", value=False)
    dbutils.jobs.taskValues.set(key="tables_with_inserts", value=json.dumps([]))
    dbutils.jobs.taskValues.set(key="tables_with_updates", value=json.dumps([]))
    dbutils.jobs.taskValues.set(key="tables_with_deletes", value=json.dumps([]))
    dbutils.jobs.taskValues.set(key="table_version_info", value=json.dumps({}))

    logger_instance.shutdown()
    dbutils.notebook.exit("NO_CHANGES")

# Log what was found
logger.info(f"Found changes in {len(set(tables_with_inserts + tables_with_updates + tables_with_deletes))} unique table(s):\n")

if len(tables_with_inserts) > 0:
    logger.info(f"INSERTS: {len(tables_with_inserts)} table(s), {total_inserts} total records")
    for tbl in tables_with_inserts:
        info = table_version_info[tbl]
        logger.info(f"   - {tbl}: {info['insert_count']} inserts")

if len(tables_with_updates) > 0:
    logger.info(f"\nUPDATES: {len(tables_with_updates)} table(s), {total_updates} total records")
    for tbl in tables_with_updates:
        info = table_version_info[tbl]
        logger.info(f"   - {tbl}: {info['update_count']} updates")

if len(tables_with_deletes) > 0:
    logger.info(f"\nDELETES: {len(tables_with_deletes)} table(s), {total_deletes} total records")
    for tbl in tables_with_deletes:
        info = table_version_info[tbl]
        logger.info(f"   - {tbl}: {info['delete_count']} deletes")

logger.info("\n" + "=" * 80)
logger.info("Proceeding to incremental processing")
logger.info("=" * 80)

# COMMAND ----------

# SET TASK VALUES FOR DOWNSTREAM NOTEBOOKS
dbutils.jobs.taskValues.set(key="status", value="CHANGES_FOUND")

# Flags to control which notebooks should run
dbutils.jobs.taskValues.set(key="has_inserts", value=len(tables_with_inserts) > 0)
dbutils.jobs.taskValues.set(key="has_updates", value=len(tables_with_updates) > 0)
dbutils.jobs.taskValues.set(key="has_deletes", value=len(tables_with_deletes) > 0)

# Tables by change type
dbutils.jobs.taskValues.set(key="tables_with_inserts", value=json.dumps(tables_with_inserts))
dbutils.jobs.taskValues.set(key="tables_with_updates", value=json.dumps(tables_with_updates))
dbutils.jobs.taskValues.set(key="tables_with_deletes", value=json.dumps(tables_with_deletes))

# Complete version info for all tables (NOW INCLUDES starting_version)
dbutils.jobs.taskValues.set(key="table_version_info", value=json.dumps(table_version_info))

# Metadata
dbutils.jobs.taskValues.set(key="entity", value=entity)
dbutils.jobs.taskValues.set(key="unified_table", value=unified_table)

# Summary counts
dbutils.jobs.taskValues.set(key="total_inserts", value=total_inserts)
dbutils.jobs.taskValues.set(key="total_updates", value=total_updates)
dbutils.jobs.taskValues.set(key="total_deletes", value=total_deletes)

logger_instance.shutdown()

dbutils.notebook.exit("CHANGES_FOUND")
