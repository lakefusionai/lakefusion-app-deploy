# Databricks notebook source
import json
from datetime import datetime
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("master_id", "", "Master Record ID")
dbutils.widgets.text("match_record_id", "", "Match Record ID to mark as NOT A MATCH")
dbutils.widgets.text("operation_type", "", "Action Type")

# COMMAND ----------

entity = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "entity",
    debugValue=dbutils.widgets.get("entity")
)
catalog_name = dbutils.widgets.get("catalog_name")
experiment_id = dbutils.widgets.get("experiment_id")
master_id = dbutils.widgets.get("master_id")
match_record_id = dbutils.widgets.get("match_record_id")
action_type = dbutils.widgets.get("operation_type")

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

logger.info(f"Master ID: {master_id}")
logger.info(f"Match Record ID to reject: {match_record_id}")

# COMMAND ----------

id_key = 'lakefusion_id'

# COMMAND ----------

# Base table names
master_table = f"{catalog_name}.gold.{entity}_master"

# Add experiment suffix if exists
if experiment_id:
    master_table += f"_{experiment_id}"

# Related tables
merge_activities_table = f"{master_table}_merge_activities"

# COMMAND ----------

logger.info("="*80)
logger.info("GOLDEN DEDUP MANUAL NOT A MATCH")
logger.info("="*80)
logger.info(f"Entity: {entity}")
logger.info(f"Master ID Key: {id_key}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment ID: {experiment_id or 'None'}")
logger.info(f"Master Table: {master_table}")
logger.info(f"Merge Activities Table: {merge_activities_table}")
logger.info(f"\nOperation:")
logger.info(f"  Master: {master_id}")
logger.info(f"  Rejecting match: {match_record_id}")
logger.info("="*80)

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 1: VALIDATE BOTH MASTERS EXIST")
logger.info("="*80)

# Validate that both master_id and match_record_id exist in master table
validation_query = f"""
    SELECT {id_key}, COUNT(*) as cnt
    FROM {master_table}
    WHERE {id_key} IN ('{master_id}', '{match_record_id}')
    GROUP BY {id_key}
"""

validation_result = spark.sql(validation_query).collect()

if len(validation_result) != 2:
    existing_ids = [row[id_key] for row in validation_result]
    if master_id not in existing_ids:
        error_msg = f"ERROR: Master ID {master_id} does not exist in master table"
        logger.error(error_msg)
        dbutils.notebook.exit(json.dumps({
            "status": "error",
            "message": error_msg
        }))
    if match_record_id not in existing_ids:
        error_msg = f"ERROR: Match record ID {match_record_id} does not exist in master table"
        logger.error(error_msg)
        dbutils.notebook.exit(json.dumps({
            "status": "error",
            "message": error_msg
        }))

logger.info(f"Master {master_id} exists")
logger.info(f"Match record {match_record_id} exists")
logger.info(f"Both records validated")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 2: CHECK IF ALREADY MARKED AS NOT A MATCH")
logger.info("="*80)

# Check if this pair is already marked as NOT A MATCH
existing_not_match_query = f"""
    SELECT COUNT(*) as cnt
    FROM {merge_activities_table}
    WHERE master_id = '{master_id}'
        AND match_id = '{match_record_id}'
        AND action_type = '{action_type}'
"""

existing_count = spark.sql(existing_not_match_query).collect()[0]['cnt']

if existing_count > 0:
    warning_msg = f"WARNING: This pair is already marked as NOT A MATCH"
    logger.warning(f"{warning_msg}")
    logger.warning(f"  Existing entries: {existing_count}")
    logger.warning(f"  Will skip logging to avoid duplicates")
    
    dbutils.notebook.exit(json.dumps({
        "status": "success",
        "message": "Pair already marked as NOT A MATCH",
        "skipped": True
    }))

logger.info(f"Pair not previously marked as NOT A MATCH")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 3: LOG MANUAL_NOT_A_MATCH ACTIVITY")
logger.info("="*80)

master_table_version = spark.sql(f"DESCRIBE HISTORY {master_table} LIMIT 1").collect()[0]["version"]

logger.info(f"Master table current version: {master_table_version}")

# Prepare NOT_A_MATCH activity
# For golden dedup (master-to-master), source is empty string
not_match_activity = [{
    "master_id": master_id,
    "match_id": match_record_id,
    "source": "",  # Empty string for master-to-master
    "version": master_table_version,
    "action_type": action_type,
    "created_at": datetime.now()
}]

# Define schema
not_match_schema = StructType([
    StructField("master_id", StringType(), True),
    StructField("match_id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("version", IntegerType(), True),
    StructField("action_type", StringType(), False),
    StructField("created_at", TimestampType(), False)
])

# Create DataFrame
df_not_match_activity = spark.createDataFrame(not_match_activity, schema=not_match_schema)

logger.info(f"  Prepared MANUAL_NOT_A_MATCH activity")
logger.info(f"    Master: {master_id}")
logger.info(f"    Match: {match_record_id}")

# Append to merge_activities table
df_not_match_activity.write.mode("append").saveAsTable(merge_activities_table)

logger.info(f"MANUAL_NOT_A_MATCH activity logged")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("GOLDEN DEDUP MANUAL NOT A MATCH COMPLETED SUCCESSFULLY")
logger.info("="*80)
logger.info(f"Master: {master_id}")
logger.info(f"Match rejected: {match_record_id}")
logger.info(f"Action: MANUAL_NOT_A_MATCH")
logger.info(f"\nNote: Potential match table will be regenerated in next task")
logger.info(f"      This pair will be excluded from future matches")

# COMMAND ----------

logger_instance.shutdown()
