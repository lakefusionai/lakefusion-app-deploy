# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json
from typing import Dict, List, Any

# COMMAND ----------

class HttpResponse:
    def __init__(self, message=None, status=None, data=None, file=None, has_more=False, totalCount=None, session_token=None):
        self.message = message
        self.status = status
        self.data = data
        self.file = file
        self.has_more = has_more
        self.totalCount = totalCount
        self.session_token = session_token

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__)

def exit_with_response(message: str, status: int, data=None, file=None, has_more=False, totalCount=None, session_token=None):
    response = HttpResponse(
        message=message,
        status=status,
        data=data,
        file=file,
        has_more=has_more,
        totalCount=totalCount,
        session_token=session_token
    )
    response_json = response.to_json()

    if status >= 400:
        # Fail the notebook
        raise Exception(response_json)
    else:
        # Exit cleanly with success message
        dbutils.notebook.exit(response_json)
   

# COMMAND ----------

# Accept input parameters
dbutils.widgets.text("input_table", "")
dbutils.widgets.text("table_primary_key", "")
dbutils.widgets.text("transformations_config", "")

# COMMAND ----------

# Get and validate input parameters
input_table = dbutils.widgets.get("input_table")
table_primary_key = dbutils.widgets.get("table_primary_key")
transformations_config = dbutils.widgets.get("transformations_config")

if not input_table:
    exit_with_response("Error: input_table is required", 400)
if not table_primary_key:
    exit_with_response("Error: table_primary_key is required", 400)
if not transformations_config:
    exit_with_response("Error: transformations_config is required", 400)

# Parse transformations configuration
try:
    transformations = json.loads(transformations_config)
except json.JSONDecodeError as e:
    exit_with_response(f"Error: Invalid transformations configuration JSON: {str(e)}", 400)

# COMMAND ----------

# MAGIC %py
# MAGIC sql(f"ALTER TABLE {input_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# COMMAND ----------

# Read input table
try:
    current_df = spark.table(input_table)
except Exception as e:
    exit_with_response(f"Error reading input table: {str(e)}", 500)

# COMMAND ----------

# MAGIC %run "./diagram_utils"

# COMMAND ----------

# Process transformations
try:
    # Initialize transformation handler
    handler = TransformationHandler()
    
    # Process each transformation in sequence
    for index, transform in enumerate(transformations):
        if "type" not in transform:
            raise ValueError(f"Missing 'type' in transformation {index + 1}")
        if "config" not in transform:
            raise ValueError(f"Missing 'config' in transformation {index + 1}")
        
        transform_type = transform["type"]
        transform_config = transform["config"]
        
        # Apply transformation based on type
        if transform_type == "null_handler":
            current_df = handler.handle_null_values(current_df, transform_config)
            print("Null handler transformation completed")
        elif transform_type == "concat_columns":
            current_df = handler.concat_columns(current_df, transform_config)
            print("Column concatenation transformation completed")
        elif transform_type == "string_cleaner":
            current_df = handler.string_cleaner(current_df, transform_config)
            print("String cleaner transformation completed")
        elif transform_type == "filter":
            current_df = handler.filter_data(current_df, transform_config)
            print("Filter transformation completed")
        elif transform_type == "value_mapper":
            current_df = handler.value_mapper(current_df, transform_config)
            print("Value mapper transformation completed")
        elif transform_type == "lookup":
            current_df = handler.lookup_table(current_df, transform_config)
            print("lookup transformation completed")
        else:
            print(f"Warning: Unsupported transformation type: {transform_type}")

except Exception as e:
    exit_with_response(f"Error processing transformations: {str(e)}", 500)

# COMMAND ----------

# Define output table name
output_table = f"{input_table}_cleaned"

# COMMAND ----------

spark.sql(f"ALTER TABLE {input_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from delta.tables import DeltaTable

source_table=input_table
metadata_table = "lakefusion_ai.silver.metadata_version_tracking"

# -------------------------------------------
# Helper: Create metadata tracking table if not exists

try:
# -------------------------------------------
    if not spark.catalog.tableExists(metadata_table):
        print(f"Creating metadata tracking table: {metadata_table}")
        version_df = spark.createDataFrame(
            [(source_table, 0)],  # Initial version = 0
            ["source_table_name", "last_version_processed"]
        )
        version_df.write.format("delta").saveAsTable(metadata_table)
    
    # -------------------------------------------
    # Check if output table exists
    # -------------------------------------------
    if not spark.catalog.tableExists(output_table):
        print(f"Output table does not exist. Creating: {output_table}")
    
        # Read full data
        current_df = spark.read.table(source_table)
    
        # Save new Delta table
        current_df.write.format("delta").saveAsTable(output_table)
    
        # Enable CDF
        spark.sql(f"ALTER TABLE {output_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
        print(f"Created and enabled CDF on: {output_table}")
    
        # Update metadata to latest version
        latest_version = spark.sql(f"DESCRIBE HISTORY {source_table}").selectExpr("max(version)").collect()[0][0]
        update_df = spark.createDataFrame([(source_table, latest_version)], ["source_table_name", "last_version_processed"])
        update_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(metadata_table)
    
    else:
        print(f"Output table exists. Using CDF for incremental merge.")
    
        # Get last processed version
        meta_df = spark.read.table(metadata_table).filter(f"source_table_name = '{source_table}'")
        last_version_processed = meta_df.select("last_version_processed").collect()[0][0]
    
        # Get latest version of source table
        latest_version = spark.sql(f"DESCRIBE HISTORY {source_table}").selectExpr("max(version)").collect()[0][0]
    
        if last_version_processed < latest_version:
            print(f"Processing changes from version {last_version_processed + 1} to {latest_version}")
    
            # Read CDF changes
            cdf_df = spark.sql(f"""
                SELECT *
                FROM table_changes('{source_table}', {last_version_processed + 1}, {latest_version})
                WHERE _change_type IN ('insert', 'update_postimage', 'delete')
            """)
    
            # Prepare Delta table for merge
            target_dt = DeltaTable.forName(spark, output_table)

            # Define merge condition using primary key
            merge_condition = f"target.{table_primary_key} = source.{table_primary_key}"  # <-- Replace Id with your actual key
    
            # Build merge
            merge_builder = (
                target_dt.alias("target")
                .merge(
                    cdf_df.alias("source"),
                    merge_condition
                )
                .whenMatchedUpdateAll(condition="source._change_type != 'delete'")
                .whenMatchedDelete(condition="source._change_type = 'delete'")
                .whenNotMatchedInsertAll(condition="source._change_type != 'delete'")
            )
    
            merge_builder.execute()
            print("Merge with upserts and deletes completed.")

            # Update metadata
            update_df = spark.createDataFrame([(source_table, latest_version)], ["source_table_name", "last_version_processed"])
            update_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(metadata_table)
            print("Metadata updated.")

        else:
           print("No new changes detected. Skipping merge.")
except Exception as e :
    exit_with_response(f"Error processing table: {str(e)}", 500)


# COMMAND ----------

# Exit with success
exit_with_response("Transformations completed successfully", 200)
