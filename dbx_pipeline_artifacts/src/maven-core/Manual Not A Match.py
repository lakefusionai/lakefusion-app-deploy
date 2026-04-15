# Databricks notebook source
from uuid import uuid4
import json
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from databricks.sdk import WorkspaceClient
import requests
from dbruntime.databricks_repl_context import get_context
import re
from functools import reduce

# COMMAND ----------

dbutils.widgets.text("id_key", "", "Primary Key")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("dataset_objects", "", "Dataset Objects")
dbutils.widgets.text('experiment_id', '', 'Experiment ID')
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")
dbutils.widgets.text("master_id", "", "Master Record ID")
dbutils.widgets.text("unified_dataset_ids", "", "Unified Record IDs with Dataset IDs")
dbutils.widgets.text("catalog_name", "lakefusion_ai_uat", "Catalog Name")
dbutils.widgets.text("config_thresold", "", "Model Config Thresholds")

# COMMAND ----------

id_key = dbutils.widgets.get("id_key")
entity = dbutils.widgets.get("entity")
dataset_objects = dbutils.widgets.get("dataset_objects")
experiment_id = dbutils.widgets.get('experiment_id')
entity_attributes = dbutils.widgets.get("entity_attributes")
master_id = dbutils.widgets.get("master_id")
unified_dataset_ids = dbutils.widgets.get("unified_dataset_ids")
config_thresold = dbutils.widgets.get("config_thresold")
catalog_name = dbutils.widgets.get("catalog_name")

# COMMAND ----------

entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
id_key = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="id_key", debugValue=id_key)
dataset_objects = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="dataset_objects", debugValue=dataset_objects)
entity_attributes = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes", debugValue=entity_attributes)
config_thresold = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "config_thresold", debugValue=config_thresold)

# COMMAND ----------

dataset_objects = json.loads(dataset_objects)
entity_attributes = json.loads(entity_attributes)
entity_attributes.append("lakefusion_id")
unified_dataset_ids = json.loads(unified_dataset_ids)
config_thresold=json.loads(config_thresold)

# COMMAND ----------

not_match_min_max = config_thresold.get('not_match')
not_match_min = not_match_min_max[0]
not_match_max = not_match_min_max[1]

# COMMAND ----------

unified_record_ids = []
dataset_ids = []
for unified_dataset_id in unified_dataset_ids:
    unified_record_ids.append(unified_dataset_id.get("id"))
    dataset_ids.append(unified_dataset_id.get("dataset_id"))

# COMMAND ----------

# MAGIC %run ../utils/execute_utils

# COMMAND ----------

# MAGIC %run ../utils/match_merge_utils

# COMMAND ----------

unified_table = f"{catalog_name}.silver.{entity}_unified"
master_table = f"{catalog_name}.gold.{entity}_master"
processed_unified_table = f"{catalog_name}.silver.{entity}_processed_unified"
if experiment_id:
  unified_table += f"_{experiment_id}"
  master_table += f"_{experiment_id}"
  processed_unified_table += f"_{experiment_id}"

merge_activities_table = f"{master_table}_merge_activities"
master_attribute_version_sources_table = f"{master_table}_attribute_version_sources"

# COMMAND ----------

# Dynamically build the SELECT fields for the entity attributes
attribute_select = ",\n      ".join(
    [f"up.{attr} as unified_{attr}" for attr in entity_attributes]
)

query = f"""
WITH RankedRecords AS (
    SELECT {id_key}, version, attribute_source_mapping,
        ROW_NUMBER() OVER (PARTITION BY {id_key} ORDER BY version DESC) AS rn
    FROM {catalog_name}.gold.{entity}_master_{experiment_id}_attribute_version_sources
)
SELECT
  ma.*,
  pu.*,
  asm.attribute_source_mapping as master_attribute_source_mapping
FROM
  (
    SELECT
      {attribute_select},  -- This will insert dynamic attributes here
      int(up.dataset.id) as unified_dataset_id,
      up.dataset.path as unified_dataset_path,
      u.master_{id_key},
      u.exploded_result.score as match_score
    FROM
      {catalog_name}.silver.{entity}_processed_unified_{experiment_id} u inner join {catalog_name}.silver.{entity}_unified_{experiment_id} up on u.{id_key}=up.{id_key}
    WHERE
      u.exploded_result.match = 'MATCH'
  ) pu,
  {catalog_name}.gold.{entity}_master_{experiment_id} ma,
  (
    SELECT {id_key}, version, attribute_source_mapping
    FROM RankedRecords
    WHERE rn = 1
  ) asm
WHERE
  pu.master_{id_key} = ma.{id_key}
  AND pu.master_{id_key} = asm.{id_key};
"""

# COMMAND ----------

potential_matches_df = spark.sql(query)

# COMMAND ----------

# Create filter expressions for each dictionary in the list
filter_expr = reduce(
    lambda acc, item: acc | (
        (col(f'unified_{id_key}') == item['id']) & 
        (col('unified_dataset_id') == item['dataset_id']) &
        (col(id_key) == master_id)
    ),
    unified_dataset_ids,
    lit(False)  # Initial accumulator for the reduce function
)

# Apply the filter to the DataFrame
filtered_df = potential_matches_df.filter(filter_expr)

# COMMAND ----------

logger.info(f"Records to mark as NOT a match: {filtered_df.count()}")

# COMMAND ----------

master_version = get_current_table_version(master_table)

# COMMAND ----------

logger.info(f"master_version--- {master_version}")

# COMMAND ----------

current_merge_activities_table = spark.read.table(merge_activities_table)

# COMMAND ----------

resultant_merge_activities_survivorship = (
    filtered_df
    .select(
        col(id_key).alias(f'master_{id_key}'),
        col(f'unified_{id_key}').alias(id_key),
        col(f'unified_dataset_path').alias('source'),
        lit(master_version).alias('version'),
        lit(ActionType.MANUAL_NOT_A_MATCH.value).alias("action_type")
    )
)

# COMMAND ----------

# Define the Delta table
delta_table = DeltaTable.forName(spark, merge_activities_table)

# Perform merge operation
delta_table.alias("target").merge(
    source=resultant_merge_activities_survivorship.alias("source"),
    condition=f"target.{id_key} = source.{id_key} and target.version = source.version and target.master_{id_key} = source.master_{id_key} and target.action_type = source.action_type and target.source = source.source"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# Extract unified records marked as not a match
unified_records_marked_not_match = (
    filtered_df
    .select(
        col(f'unified_{id_key}').alias(id_key),
        col('unified_dataset_path'),
        *[col(f'unified_{attr}').alias(attr) for attr in entity_attributes if attr != id_key]
    )
)

# COMMAND ----------

# Check for other potential matches
entity_lower = entity.lower().replace(' ', '_')
potential_matches_table = f"{catalog_name}.gold.{entity_lower}_master_potential_match"
if experiment_id:
    potential_matches_table += f"_{experiment_id}"

marked_unified_ids = [item['id'] for item in unified_dataset_ids]

# Get all master records that have these unified records as potential matches
all_masters_with_these_unified = (
    spark.read.table(potential_matches_table)
    .select(col(id_key).alias('master_id'), explode(col('potential_matches')).alias('potential_match'))
    .filter(
        col('potential_match.lakefusion_id').isin(marked_unified_ids) &
        (col('potential_match.__mergestatus__') == 'PENDING')
    )
    .select('master_id', col('potential_match.lakefusion_id').alias('unified_id'))
)

# Exclude the masters that were just marked as not-a-match
masters_being_marked = filtered_df.select(col(id_key)).distinct()

# Find unified records that have OTHER pending matches (excluding the master being marked)
unified_with_other_matches = (
    all_masters_with_these_unified
    .join(masters_being_marked, all_masters_with_these_unified['master_id'] == masters_being_marked[id_key], how='left_anti')
    .select('unified_id')
    .distinct()
)

# These are unified records that do NOT have other pending matches
records_without_other_matches = (
    unified_records_marked_not_match
    .select(col(id_key).alias('check_id'))
    .distinct()
    .join(unified_with_other_matches, col('check_id') == col('unified_id'), how='left_anti')
    .select(col('check_id').alias(id_key))
)

# COMMAND ----------

# Filter records and perform golden deduplication
records_to_insert = (
    unified_records_marked_not_match
    .join(records_without_other_matches, on=id_key, how='inner')
    .join(spark.read.table(master_table).select(id_key), on=id_key, how='left_anti')
)

logger.info(f"Records to insert: {records_to_insert.count()}")

# COMMAND ----------

# Filter out records that have already been merged into other records
# Check merge_activities for any records where this id_key appears (meaning it was merged)
already_merged_records = (
    spark.read.table(merge_activities_table)
    .filter(
        (col(id_key).isNotNull()) & 
        (col(id_key) != '') &
        (col('action_type').isin(['JOB_MERGE', 'MANUAL_MERGE']))
    )
    .select(col(id_key))
    .distinct()
)

# Remove already merged records from records_to_insert
records_to_insert = (
    records_to_insert
    .join(already_merged_records, on=id_key, how='left_anti')
)

# Deduplicate to ensure no duplicates in source
records_to_insert = records_to_insert.dropDuplicates([id_key])

logger.info(f"Records to insert after all filters: {records_to_insert.count()}")

# COMMAND ----------

# Exit if no records to process
if records_to_insert.count() == 0:
    logger.info("No records to insert - exiting")
    dbutils.notebook.exit("No records to process")

# COMMAND ----------

# Prepare master table insert
master_columns = spark.read.table(master_table).columns

master_insert_df = (
    records_to_insert
    .withColumn("attributes_combined", concat_ws(" | ", *[col(attr) for attr in entity_attributes if attr in records_to_insert.columns]))
)

for col_name in master_columns:
    if col_name not in master_insert_df.columns:
        master_insert_df = master_insert_df.withColumn(col_name, lit(None))

master_insert_df = master_insert_df.select(*master_columns)

# Final deduplication check before insert
master_insert_df = master_insert_df.dropDuplicates([id_key])

records_collected = records_to_insert.collect()

# COMMAND ----------

# Insert into master table
logger.info("Inserting records into master table")
DeltaTable.forName(spark, master_table).alias("target").merge(
    source=master_insert_df.alias("source"),
    condition=f"target.{id_key} = source.{id_key}"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# Insert merge activities
logger.info("Inserting merge activities")
merge_activities_data = [
    {
        f'master_{id_key}': record[id_key],
        id_key: '',
        'source': record['unified_dataset_path'],
        'version': master_version,
        'action_type': ActionType.JOB_INSERT.value
    }
    for record in records_collected
]

merge_activities_df = spark.createDataFrame(merge_activities_data, spark.read.table(merge_activities_table).schema)

# COMMAND ----------

DeltaTable.forName(spark, merge_activities_table).alias("target").merge(
    source=merge_activities_df.alias("source"),
    condition=f"""
        target.{id_key} = source.{id_key} 
        and target.master_{id_key} = source.master_{id_key}
        and target.version = source.version 
        and target.action_type = source.action_type 
        and target.source = source.source
    """
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# Insert attribute version sources
logger.info("Inserting attribute version sources")
attribute_data = []
for record in records_collected:
    attribute_mapping = [
        {
            'attribute_name': attr,
            'attribute_value': str(record[attr]) if record[attr] is not None else None,
            'source': record['unified_dataset_path']
        }
        for attr in entity_attributes if attr != id_key
    ]
    
    if 'attributes_combined' in entity_attributes:
        combined_value = " | ".join([str(record[attr]) for attr in attributes if attr in record and record[attr] is not None])
        attribute_mapping.append({
            'attribute_name': 'attributes_combined',
            'attribute_value': combined_value if combined_value else None,
            'source': record['unified_dataset_path']
        })
    
    attribute_data.append({
        id_key: record[id_key],
        'version': master_version,
        'attribute_source_mapping': attribute_mapping
    })

attribute_df = spark.createDataFrame(attribute_data, spark.read.table(master_attribute_version_sources_table).schema)


# COMMAND ----------

DeltaTable.forName(spark, master_attribute_version_sources_table).alias("target").merge(
    source=attribute_df.alias("source"),
    condition=f"target.{id_key} = source.{id_key} and target.version = source.version"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
