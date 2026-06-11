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

from pyspark.sql.functions import col, lit, when, concat_ws, current_timestamp
from delta.tables import DeltaTable

def get_last_processed_version(table_meta_info, table_name,entity):
    """
    Get the last processed version of the table from metadata table.
    """
    if spark.catalog.tableExists(table_meta_info):
        meta_df = spark.read.table(table_meta_info).filter(col("table_name") == table_name).filter(col("entity_name") == entity)
        if meta_df.count() > 0:
            return meta_df.select("last_processed_version").first()[0]
    return None

def update_last_processed_version(table_meta_info, entity, table_name, version):
    """
    Update or insert last processed version into metadata table.
    """
    if not spark.catalog.tableExists(table_meta_info):
        # Create the meta info table
        schema = "table_name STRING,entity_name string,last_processed_version LONG, last_processed_timestamp TIMESTAMP"
        spark.sql(f"CREATE TABLE {table_meta_info} ({schema}) USING DELTA")
    
    # Create DataFrame to upsert
    new_row = spark.createDataFrame([(table_name, entity, version, )], ["table_name","entity_name","last_processed_version"])
    new_row = new_row.withColumn("last_processed_timestamp", current_timestamp())
    
    delta_meta = DeltaTable.forName(spark, table_meta_info)
    
    # Merge
    delta_meta.alias("target").merge(
        new_row.alias("source"),
        condition=f"target.table_name = source.table_name and target.entity_name = source.entity_name"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

def check_and_enable_cdf(table_name):
    try:
        # Check if CDF is already enabled
        table_properties = spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()
        cdf_enabled = False
        for prop in table_properties:
            if prop['key'] == 'delta.enableChangeDataFeed' and prop['value'].lower() == 'true':
                cdf_enabled = True
                break
        if not cdf_enabled:
            spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
            print(f"CDF enabled on {table_name}")
        else:
            print(f"CDF already enabled on {table_name}")
    except Exception as e:
        print(f"Failed to check/enable CDF on {table_name}: {str(e)}")

# COMMAND ----------

check_and_enable_cdf(input_table)

# COMMAND ----------

latest_version = spark.sql(f"DESCRIBE HISTORY {input_table}").selectExpr("max(version)").first()[0]
print(f"latest_version: {latest_version}")
last_processed_version = get_last_processed_version(meta_info_table, input_table,entity)
print(f"last_processed_version: {last_processed_version}")
if latest_version == last_processed_version:
    exit_with_response("Table processed and saved successfully.", 200)
if last_processed_version is None:
    input_df = spark.table(input_table)
else:
    input_df = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", last_processed_version+1) \
    .option("endingVersion", latest_version) \
    .table(input_table).filter((col("_change_type") == "delete")|(col("_change_type") == "insert")|(col("_change_type") == "update_postimage"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add your data transformation or cleansing logic below
# MAGIC - Ensure to write the output to `output_df`.
# MAGIC - Define `table_primary_key=<table_primary_key>` as well

# COMMAND ----------

# Add your custom transformation logic here


from pyspark.sql import functions as F

# Remove trailing spaces from description
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

# COMMAND ----------

if first_run != True:   
    try:
        # Write output_df into output table using Delta merge operation
        output_df.createOrReplaceTempView("output_df_view")
        spark.sql(f"""
            MERGE INTO {output_table} AS target
            USING output_df_view AS source
            ON target.{table_primary_key} = source.{table_primary_key}
            WHEN MATCHED AND source._change_type = 'delete' THEN DELETE
            WHEN MATCHED AND source._change_type = 'update_postimage' THEN UPDATE SET *
            WHEN NOT MATCHED AND source._change_type = 'insert' THEN INSERT *
        """)

    except Exception as e:
        # Handle any exceptions and exit with error response
        exit_with_response(f"Error processing table: {str(e)}", 500)

# COMMAND ----------

update_last_processed_version(meta_info_table, entity, input_table, latest_version)

# COMMAND ----------

# Exit with success response
exit_with_response("Table processed and saved successfully.", 200)
