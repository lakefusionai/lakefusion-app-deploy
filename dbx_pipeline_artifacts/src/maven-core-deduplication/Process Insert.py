# Databricks notebook source
#take newly changes from primary and put in master table --generate lf id also for newly record

# COMMAND ----------

import json
import ast
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,lit
import uuid

# COMMAND ----------

dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("primary_table", "", "Primary Table")
dbutils.widgets.text("dataset_tables", "", "Dataset Tables")
dbutils.widgets.text("dataset_objects", "", "Dataset Objects")
dbutils.widgets.text("id_key", "", "Primary Key")
dbutils.widgets.text("primary_key", "", "Primary Key")
dbutils.widgets.text("attributes_mapping_json", "{}", "Attributes Mapping (JSON)")
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")
dbutils.widgets.text("experiment_id", "", "Match Maven Experiment ID")
dbutils.widgets.text("attributes", "", "Merged Description Attributes")
dbutils.widgets.text('processed_records', '', 'No of records to be proceesed')
dbutils.widgets.text("is_integration_hub", "", "Integration Hub Pipeline")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("entity_attributes_datatype","","entity_attributes_datatype")
dbutils.widgets.text("is_golden_deduplication","","is_golden_deduplication")
#dbutils.widgets.text('validation_functions', '', 'Validation Functions')


# COMMAND ----------

entity = dbutils.widgets.get("entity")
primary_table = dbutils.widgets.get("primary_table")
dataset_tables = dbutils.widgets.get("dataset_tables")
dataset_objects = dbutils.widgets.get("dataset_objects")
id_key = dbutils.widgets.get("id_key")
primary_key = dbutils.widgets.get("primary_key")
attributes_mapping_json = dbutils.widgets.get("attributes_mapping_json")
attributes_mapping_json_deduplicate = dbutils.widgets.get("attributes_mapping_json")
entity_attributes = dbutils.widgets.get("entity_attributes")
entity_attributes_datatype=dbutils.widgets.get("entity_attributes_datatype")
attributes = dbutils.widgets.get("attributes")
if(dbutils.widgets.get("processed_records")!=""):
    process_records=ast.literal_eval(dbutils.widgets.get("processed_records"))
else:
    process_records=dbutils.widgets.get("processed_records")
#below inputs are to be passed as notebook params OR add them to entity_json itself
experiment_id = dbutils.widgets.get("experiment_id") # cannot contain "-" for table name
experiment_id = experiment_id.replace("-", "") #remove "-" which is invalid for table name
incremental_load = False
merged_desc_column = "attributes_combined"
catalog_name=dbutils.widgets.get("catalog_name")
is_golden_deduplication=dbutils.widgets.get("is_golden_deduplication")

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
primary_key = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "primary_key", debugValue=primary_key
)
entity_attributes = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes", debugValue=entity_attributes)
attributes_mapping_json = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "attributes_mapping", debugValue=attributes_mapping_json
)
attributes_mapping_json_deduplicate = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "attributes_mapping", debugValue=attributes_mapping_json_deduplicate
)
# validation_functions_json=dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "validation_functions", debugValue=validation_functions)
attributes = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="match_attributes", debugValue=attributes)
entity_attributes_datatype = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes_datatype", debugValue=entity_attributes_datatype)
is_golden_deduplication = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "is_golden_deduplication", debugValue=is_golden_deduplication)

# COMMAND ----------

dataset_tables = json.loads(dataset_tables)
dataset_objects = json.loads(dataset_objects)
entity_attributes = json.loads(entity_attributes)
entity_attributes.append("lakefusion_id")
attributes_mapping_json = json.loads(attributes_mapping_json)
entity_attributes_datatype = json.loads(entity_attributes_datatype)
attributes_mapping_json_deduplicate = json.loads(attributes_mapping_json_deduplicate)
attributes = json.loads(attributes)
# validation_function_json = json.loads(validation_functions_json)

# COMMAND ----------

# MAGIC %run ../utils/parse_utils

# COMMAND ----------

# MAGIC %run ../utils/match_merge_utils

# COMMAND ----------

import logging
if 'logger' not in dir():
    logger = logging.getLogger(__name__)

# COMMAND ----------

unified_table = f"{catalog_name}.silver.{entity}_unified_deduplicate"
master_table = f"{catalog_name}.gold.{entity}_master"
processed_unified_table = f"{catalog_name}.silver.{entity}_processed_unified_deduplicate"
validation_error_unified_table = f"{catalog_name}.silver.{entity}_unified_validation_error"
if experiment_id:
  unified_table += f"_{experiment_id}"
  master_table += f"_{experiment_id}"
  validation_error_unified_table+=f"_{experiment_id}"
  processed_unified_table += f"_{experiment_id}"

merge_activities_table = f"{master_table}_merge_activities"
master_attribute_version_sources_table = f"{master_table}_attribute_version_sources"

master_table_list = master_table.split(".")
master_catalog, master_schema, master_table_name = master_table_list[0], master_table_list[1], master_table_list[2]

# COMMAND ----------

uuid_udf = udf(lambda: uuid.uuid4().hex.lower(), StringType())

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp
from delta.tables import DeltaTable

meta_info_table = f"{catalog_name}.silver.table_meta_info"

def get_last_processed_version(table_meta_info,entity_name, table_name):
    if spark.catalog.tableExists(table_meta_info):
        meta_df = spark.read.table(table_meta_info).filter(col("table_name") == table_name).filter(col("entity_name")==entity_name)
        if meta_df.count() > 0:
            return meta_df.select("last_processed_version").first()[0]
    return None

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col, concat_ws, sha2, current_timestamp

table_meta_info = f"{catalog_name}.silver.table_meta_info"

# --- Step 1: Get Last Processed Version ---
last_version = get_last_processed_version(table_meta_info, entity, primary_table)

# --- Step 2: Get Current Version of the primary table ---
current_version = spark.sql(f"DESCRIBE HISTORY {primary_table} LIMIT 1").select("version").first()[0]

# --- Step 3: Check if there are new versions to process ---
if last_version is not None and current_version <= last_version:
    logger.info(f"No new versions to process. Last processed: {last_version}, Current: {current_version}")
    dbutils.notebook.exit("No new versions to process")
    
logger.info(f"Processing Inserts. Last processed: {last_version}, Current: {current_version}")

# --- Step 4: Read CDF changes ---
read_options = {
    "readChangeData": "true"
}

if last_version is not None:
    # Start from the NEXT version after what was last processed
    read_options["startingVersion"] = last_version + 1
else:
    # This case shouldn't happen since you always set last_processed_version before this task
    logger.info("No last processed version found. Using version 0.")
    read_options["startingVersion"] = 0
    
# Load only inserts
df_cdf = (
    spark.read.format("delta")
    .options(**read_options)
    .table(primary_table)
    .filter(col("_change_type") == "insert")
)
display(df_cdf)
primary_table_attr_mapping = get_primary_attr_mapping(attributes_mapping_json)

# COMMAND ----------

if df_cdf.isEmpty():
    logger.info("No new records to process.")
    dbutils.notebook.exit("No inserts to process")

# COMMAND ----------

# --- Step 3: Generate lakefusion_id using hash of source_pk ---
df_transformed = df_cdf.withColumn("lakefusion_id",uuid_udf())

df_to_merge = df_transformed.drop("_change_type", "_commit_version", "_commit_timestamp")
primary_table_attr_mapping["lakefusion_id"]="lakefusion_id"
df_to_merge=df_to_merge.selectExpr(*[f"{k} as {v}" for k, v in primary_table_attr_mapping.items()]).withColumn(merged_desc_column, concat_ws(" | ", *[col(c) for c in attributes]))

# CRITICAL FIX: Materialize the UUID values by collecting and recreating the dataframe
# This ensures the same UUID is used consistently across all operations
# Without this, each dataframe evaluation generates new UUIDs
schema = df_to_merge.schema
materialized_data = df_to_merge.collect()
df_to_merge = spark.createDataFrame(materialized_data, schema)


# --- Step 4: Merge into master table on source_pk ---
DeltaTable.forName(spark, master_table).alias("target") \
    .merge(
        df_to_merge.alias("source"),
        f"target.{primary_key} = source.{primary_key}"
    ).whenNotMatchedInsertAll() \
    .execute()

# COMMAND ----------

master_table_created=True
if master_table_created and experiment_id == 'prod':
    logger.info(
        f"Creating or syncing master attribute version mapping and merge activities tables for NEWLY INSERTED records only"
    )
    
    # Get the current master version (this is the version AFTER the merge)
    master_version = get_current_table_version(master_table)
    
    # CRITICAL FIX: Only process the newly inserted records (df_to_merge), NOT the entire master table
    # df_to_merge contains only the records that were just inserted
    df_newly_inserted = df_to_merge

    # Get the list of columns to include in the mapping (excluding ID Key)
    columns_to_map = [col_name for col_name in df_newly_inserted.columns if col_name != id_key]
    source = primary_table

    try:
        # Dynamically create the attribute_source_mapping using array of structs
        attribute_source_mapping = array(
            *[
                struct(
                    lit(col_name).alias("attribute_name"),
                    col(col_name).cast("string").alias("attribute_value"),  # Ensure attr_value is string
                    lit(source).alias("source")
                )
                for col_name in columns_to_map
            ]
        )

        df_master_attribute_version_mapping = df_newly_inserted.select(
            col(id_key),
            lit(master_version).alias("version"),
            attribute_source_mapping.alias("attribute_source_mapping")
        )
        
        logger.info(f"Logging attribute version sources for {df_master_attribute_version_mapping.count()} newly inserted records")
        display(df_master_attribute_version_mapping)

        master_attribute_version_sources_table_exists = spark.catalog.tableExists(master_attribute_version_sources_table)
        
        if not master_attribute_version_sources_table_exists:
            df_master_attribute_version_mapping.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(master_attribute_version_sources_table)
        else:
            # Define the Delta table
            delta_table = DeltaTable.forName(spark, master_attribute_version_sources_table)

            # Perform merge operation - this should only insert new records, not update existing ones
            delta_table.alias("target").merge(
                source=df_master_attribute_version_mapping.alias("source"),
                condition=f"target.{id_key} = source.{id_key} and target.version = source.version"
            ).whenNotMatchedInsertAll().execute()
            
        logger.info(f"Successfully logged attribute version sources for newly inserted records at version {master_version}")
    except Exception as e:
        logger.error(
            f"Master attribute version mapping failed. Reason - {str(e)}"
        )

    try:
        df_master_merge_activities = df_newly_inserted.select(
            col(id_key).alias(f'master_{id_key}'),
            lit("").alias(id_key),
            lit(source).alias("source"),
            lit(master_version).alias("version"),
            lit(ActionType.JOB_INSERT.value).alias("action_type")
        )
        
        logger.info(f"Logging merge activities for {df_master_merge_activities.count()} newly inserted records")
        display(df_master_merge_activities)

        merge_activities_table_exists = spark.catalog.tableExists(merge_activities_table)
        if not merge_activities_table_exists:
            df_master_merge_activities.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(merge_activities_table)
        else:
            # Define the Delta table
            delta_table = DeltaTable.forName(spark, merge_activities_table)

            # Perform merge operation - this should only insert new records, not update existing ones
            delta_table.alias("target").merge(
                source=df_master_merge_activities.alias("source"),
                condition=f"target.{id_key} = source.{id_key} and target.master_{id_key} = source.master_{id_key} and target.version = source.version and target.source = source.source"
            ).whenNotMatchedInsertAll().execute()
            
        logger.info(f"Successfully logged merge activities for newly inserted records at version {master_version}")
    except Exception as e:
        logger.error(
            f"Master merge activities failed. Reason - {str(e)}"
        )

# COMMAND ----------

dbutils.jobs.taskValues.set("master_table", master_table)
dbutils.jobs.taskValues.set("unified_table", unified_table)
dbutils.jobs.taskValues.set("entity", entity)
dbutils.jobs.taskValues.set("experiment_id", experiment_id)
dbutils.jobs.taskValues.set("id_key", id_key)
dbutils.jobs.taskValues.set("incremental_load", incremental_load)
