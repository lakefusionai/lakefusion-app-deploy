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
    handler = TransformationHandler_Validation()
    
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
            current_df = handler.validate_null_handler_config( transform_config)
            print("Null handler transformation completed")
        elif transform_type == "concat_columns":
            current_df = handler.validate_concat_columns_config(transform_config)
            print("Column concatenation transformation completed")
        elif transform_type == "string_cleaner":
            current_df = handler.validate_string_cleaner(transform_config)
            print("String cleaner transformation completed")
        elif transform_type == "filter":
            current_df = handler.validate_filter_data(transform_config)
            print("Filter transformation completed")
        elif transform_type == "value_mapper":
            current_df = handler.validate_value_mapper( transform_config)
            print("Value mapper transformation completed")
        elif transform_type == "lookup":
            current_df = handler.validate_lookup_config(transform_config)
            print("LookUp transformation completed")
        else:
            print(f"Warning: Unsupported transformation type: {transform_type}")

except Exception as e:
    exit_with_response(f"Error processing transformations: {str(e)}", 500)

# COMMAND ----------

for i in transformations:
    if(i['type']=='lookup'):
        is_lookup_present='1'
        break
    else:
        is_lookup_present='0'


# COMMAND ----------

# Exit with success
dbutils.jobs.taskValues.set("is_lookup_present", is_lookup_present)
exit_with_response("Transformations completed successfully", 200)
