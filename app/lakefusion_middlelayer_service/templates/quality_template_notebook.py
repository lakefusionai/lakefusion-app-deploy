# Databricks notebook source
# Databricks Notebook Template
# Purpose: Template to process and clean a UC table with primary key, save results, and exit with JSON response.

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
import json
import re

# COMMAND ----------

# Define HttpResponse class for structured JSON output
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

# Reusable function to exit notebook with a JSON response
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
    dbutils.notebook.exit(response.to_json())

# COMMAND ----------

# Accept input parameters
dbutils.widgets.text("input_table", "")
dbutils.widgets.text("entity", "")
dbutils.widgets.text("table_primary_key", "")

# COMMAND ----------

input_table = dbutils.widgets.get("input_table")
entity = dbutils.widgets.get("entity")
table_primary_key = dbutils.widgets.get("table_primary_key")


# COMMAND ----------

# Check input parameters
if not input_table:
    exit_with_response("Error: input_table is required", 400)

# COMMAND ----------

catalog_name = input_table.split(".")[0]
meta_info_table = f"{catalog_name}.silver.table_meta_info"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add your data transformation or cleansing logic below
# MAGIC - Ensure to write the output to `output_df`.
# MAGIC - Define `table_primary_key=<table_primary_key>` as well

# COMMAND ----------

import pyspark.sql.functions as F
# Add your custom transformation logic here
# Remove trailing spaces from description
input_df = spark.read.table(input_table)

output_df = input_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Custom logic ends here

# COMMAND ----------

output_table = f"{input_table}_cleaned"

# COMMAND ----------

# Step 3: Check if the output table already exists
first_run = False
if not spark.catalog.tableExists(output_table):
    first_run = True
    # If output_table doesn't exist, create it with the same schema as input_df
    output_df.write.format("delta").saveAsTable(output_table)
    spark.sql(f"ALTER TABLE {output_table} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")

# COMMAND ----------

if not first_run:   
    try:
        # Write output_df into output table using Delta merge operation
        output_df.createOrReplaceTempView("output_df_view")
        spark.sql(f"""
            MERGE INTO {output_table} AS target
        USING output_df_view AS source
        ON target.{table_primary_key} = source.{table_primary_key}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """)

    except Exception as e:
        # Handle any exceptions and exit with error response
        exit_with_response(f"Error processing table: {str(e)}", 500)

# COMMAND ----------

# Exit with success response
exit_with_response("Table processed and saved successfully.", 200)