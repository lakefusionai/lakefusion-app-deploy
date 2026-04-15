# Databricks notebook source
import json
from delta.tables import DeltaTable
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("primary_table", "", "Primary Table")
dbutils.widgets.text("dataset_tables", "", "Dataset Tables")
dbutils.widgets.text("experiment_id", "", "Match Maven Experiment ID")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("is_golden_deduplication", "", "Is Golden Deduplication")
dbutils.widgets.text("meta_info_table", "", "Meta Info Table")

# COMMAND ----------

entity = dbutils.widgets.get("entity")
primary_table = dbutils.widgets.get("primary_table")
dataset_tables = dbutils.widgets.get("dataset_tables")
experiment_id = dbutils.widgets.get("experiment_id")
catalog_name = dbutils.widgets.get("catalog_name")
is_golden_deduplication = dbutils.widgets.get("is_golden_deduplication")
meta_info_table = dbutils.widgets.get("meta_info_table")

# COMMAND ----------

entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
primary_table = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "primary_table", debugValue=primary_table)
dataset_tables = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "dataset_tables", debugValue=dataset_tables)
is_golden_deduplication = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "is_golden_deduplication", debugValue=is_golden_deduplication)

# COMMAND ----------

dataset_tables = json.loads(dataset_tables)
if experiment_id:
    experiment_id = experiment_id.replace("-", "")

# COMMAND ----------

# MAGIC %run ../utils/execute_utils

# COMMAND ----------

unified_table = f"{catalog_name}.silver.{entity}_unified"
master_table = f"{catalog_name}.gold.{entity}_master"
meta_info_table = f"{catalog_name}.silver.table_meta_info"

if experiment_id:
    unified_table += f"_{experiment_id}"
    master_table += f"_{experiment_id}"

logger.info(f"Entity: {entity}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Meta info table: {meta_info_table}")
logger.info(f"Master table: {master_table}")
logger.info(f"Unified table: {unified_table}")
logger.info(f"Primary table: {primary_table}")

# COMMAND ----------

def update_last_processed_version(table_meta_info, entity, table_name, version):
    """
    Update or insert last processed version into metadata table.
    """
    if not spark.catalog.tableExists(table_meta_info):
        # Create the meta info table
        schema = "table_name STRING, entity_name STRING, last_processed_version LONG, last_processed_timestamp TIMESTAMP"
        spark.sql(f"CREATE TABLE {table_meta_info} ({schema}) USING DELTA")
    
    # Create DataFrame to upsert
    new_row = spark.createDataFrame([(table_name, entity, version)], ["table_name", "entity_name", "last_processed_version"])
    new_row = new_row.withColumn("last_processed_timestamp", current_timestamp())
    
    delta_meta = DeltaTable.forName(spark, table_meta_info)
    
    # Merge
    delta_meta.alias("target").merge(
        new_row.alias("source"),
        condition=f"target.table_name = source.table_name AND target.entity_name = source.entity_name"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# 1. Update metadata for master table
latest_version = spark.sql(f"DESCRIBE HISTORY {master_table}").selectExpr("max(version)").first()[0]
update_last_processed_version(meta_info_table, entity, master_table, latest_version)
logger.info(f"Updated master table {master_table}: version {latest_version}")

# 2. Update metadata for unified table (regular or deduplicate based on flag)
latest_version = spark.sql(f"DESCRIBE HISTORY {unified_table}").selectExpr("max(version)").first()[0]
update_last_processed_version(meta_info_table, entity, unified_table, latest_version)
logger.info(f"Updated unified table {unified_table}: version {latest_version}")

# 3. Update all secondary tables (excluding primary table) for regular processing
for dataset in dataset_tables:
    if dataset != primary_table:  # Skip primary table
        latest_version = spark.sql(f"DESCRIBE HISTORY {dataset}").selectExpr("max(version)").first()[0]
        update_last_processed_version(meta_info_table, entity, dataset, latest_version)
        logger.info(f"Updated secondary table {dataset}: version {latest_version}")

# 4. Update primary table
latest_version = spark.sql(f"DESCRIBE HISTORY {primary_table}").selectExpr("max(version)").first()[0]
update_last_processed_version(meta_info_table, entity, primary_table, latest_version)
logger.info(f"Updated primary table {primary_table}: version {latest_version}")

# COMMAND ----------

logger.info("Successfully updated all last processed versions")
