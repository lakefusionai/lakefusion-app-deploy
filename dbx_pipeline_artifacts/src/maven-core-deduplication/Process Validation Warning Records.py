# Databricks notebook source
import json,ast
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("primary_table", "", "Primary Table")
dbutils.widgets.text("dataset_tables", "", "Dataset Tables")
dbutils.widgets.text("dataset_objects", "", "Dataset Objects")
dbutils.widgets.text("id_key", "", "Primary Key")
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")
dbutils.widgets.text("experiment_id", "", "Match Maven Experiment ID")
dbutils.widgets.text("attributes", "", "Merged Description Attributes")
dbutils.widgets.text("is_integration_hub", "", "Integration Hub Pipeline")
dbutils.widgets.text('validation_functions', '', 'Validation Functions')
dbutils.widgets.text("catalog_name", "", "Catalog Name")


# COMMAND ----------

entity = dbutils.widgets.get("entity")
primary_table = dbutils.widgets.get("primary_table")
dataset_tables = dbutils.widgets.get("dataset_tables")
dataset_objects = dbutils.widgets.get("dataset_objects")
id_key = dbutils.widgets.get("id_key")
entity_attributes = dbutils.widgets.get("entity_attributes")
attributes = dbutils.widgets.get("attributes")
#below inputs are to be passed as notebook params OR add them to entity_json itself
experiment_id = dbutils.widgets.get("experiment_id") # cannot contain "-" for table name
experiment_id = experiment_id.replace("-", "") #remove "-" which is invalid for table name
incremental_load = False
merged_desc_column = "attributes_combined"
validation_functions=dbutils.widgets.get("validation_functions")
is_integration_hub = dbutils.widgets.get("is_integration_hub")
catalog_name=dbutils.widgets.get("catalog_name")

# COMMAND ----------

entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
primary_table = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "primary_table", debugValue=primary_table
)
dataset_tables = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "dataset_tables", debugValue=dataset_tables)
dataset_objects = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "dataset_objects", debugValue=dataset_objects)
id_key = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "id_key", debugValue=id_key
)
entity_attributes = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes", debugValue=entity_attributes)
attributes = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="match_attributes", debugValue=attributes)
validation_functions_json=dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "validation_functions", debugValue=validation_functions)
is_integration_hub = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="is_integration_hub", debugValue=is_integration_hub)

# COMMAND ----------

dataset_tables = json.loads(dataset_tables)
dataset_objects = json.loads(dataset_objects)
entity_attributes = json.loads(entity_attributes)
attributes = json.loads(attributes)
validation_function_json = json.loads(validation_functions_json)

# COMMAND ----------

# MAGIC %run ../utils/match_merge_utils

# COMMAND ----------

# MAGIC %run ../utils/validation_functions_constant

# COMMAND ----------

unified_table = f"{catalog_name}.silver.{entity}_unified"
master_table = f"{catalog_name}.gold.{entity}_master"
validation_warning_master_table = f"{catalog_name}.gold.{entity}_master_validation_warning"
processed_unified_table = f"{catalog_name}.silver.{entity}_processed_unified"
if experiment_id:
  unified_table += f"_{experiment_id}"
  validation_warning_master_table+=f"_{experiment_id}"
  master_table += f"_{experiment_id}"
  processed_unified_table += f"_{experiment_id}"

merge_activities_table = f"{master_table}_merge_activities"
master_attribute_version_sources_table = f"{master_table}_attribute_version_sources"

master_table_list = master_table.split(".")
master_catalog, master_schema, master_table_name = master_table_list[0], master_table_list[1], master_table_list[2]

# COMMAND ----------

import json


validation_function_warning = []
# Process each validation function
for item in validation_function_json:
    attribute_name = item["attribute_name"]
    function_name = item["function_name"]
    function_definition = item["function_definition"]["function_definition"]
    
    if item["validation_level"] == "warning":
        validation_function_warning.append({
            "attribute_name": attribute_name,
            "function_name": function_name,
            "function_definition": function_definition
        })

# COMMAND ----------

# MAGIC %run ../utils/parse_utils

# COMMAND ----------

if is_integration_hub == '1':
    df_master = spark.read.table(master_table)
    validation_warning_master_table_exists = spark.catalog.tableExists(validation_warning_master_table)
    
    df_master_validation_warning = df_master.select(
        col(id_key), 
        *[i['attribute_name'] for i in validation_function_warning]
    )
    
    
    df_master_validation_warning = validation_function_exec(
        validation_function_warning, 
        df_master_validation_warning
    )
   
    
    if not validation_warning_master_table_exists:
        df_master_validation_warning.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(validation_warning_master_table)
    else:
        # Define the Delta table
        delta_table = DeltaTable.forName(spark, validation_warning_master_table)
        
        # Perform merge operation
        delta_table.alias("target").merge(
            source=df_master_validation_warning.alias("source"),
            condition=f"target.{id_key} = source.{id_key}"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
