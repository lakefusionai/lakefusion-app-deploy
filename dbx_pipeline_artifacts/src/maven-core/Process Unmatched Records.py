# Databricks notebook source
from uuid import uuid4
import json
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from dbruntime.databricks_repl_context import get_context

# COMMAND ----------

dbutils.widgets.text("id_key", "", "Primary Key")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("dataset_objects", "", "Dataset Objects")
dbutils.widgets.text('experiment_id', '', 'Experiment ID')
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")
dbutils.widgets.text("config_thresold", "", "Model Config Thresholds")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("attributes", "", "Merged Description Attributes")

# COMMAND ----------

id_key = dbutils.widgets.get("id_key")
entity = dbutils.widgets.get("entity")
dataset_objects = dbutils.widgets.get("dataset_objects")
experiment_id = dbutils.widgets.get('experiment_id')
entity_attributes = dbutils.widgets.get("entity_attributes")
attributes = dbutils.widgets.get("attributes")
config_thresold = dbutils.widgets.get("config_thresold")
catalog_name=dbutils.widgets.get("catalog_name")

# COMMAND ----------

entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
id_key = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="id_key", debugValue=id_key)
dataset_objects = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="dataset_objects", debugValue=dataset_objects)
entity_attributes = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes", debugValue=entity_attributes)
config_thresold = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "config_thresold", debugValue=config_thresold)
attributes = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="match_attributes", debugValue=attributes)

# COMMAND ----------

dataset_objects = json.loads(dataset_objects)
entity_attributes = json.loads(entity_attributes)
entity_attributes.append("lakefusion_id")
config_thresold = json.loads(config_thresold)
attributes = json.loads(attributes)

# COMMAND ----------

not_match_min_max = config_thresold.get('not_match')
not_match_min = not_match_min_max[0]
not_match_max = not_match_min_max[1]

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

master_table_list = master_table.split(".")
master_catalog, master_schema, master_table_name = master_table_list[0], master_table_list[1], master_table_list[2]
merged_desc_column = "attributes_combined"

# COMMAND ----------

processed_unified_query = f"""
select * from {processed_unified_table}
where exploded_result.`match` == 'MATCH'
and exploded_result.score >= {not_match_max}
"""


# COMMAND ----------

query = f"""
SELECT u.*
FROM {unified_table} u
LEFT ANTI JOIN ({processed_unified_query}) pu 
    ON u.{id_key} = pu.{id_key}
LEFT ANTI JOIN {master_table} m 
    ON u.{id_key} = m.{id_key}
WHERE u.search_results != ''
"""

unmatched_records_df = spark.sql(query)

# COMMAND ----------

master_columns = [column for column in unmatched_records_df.columns if column in entity_attributes]
new_master_records_df = unmatched_records_df.select(*master_columns).withColumn(merged_desc_column, concat_ws(" | ", *[col(c) for c in attributes]))

# COMMAND ----------

master_version = get_current_table_version(master_table)

# COMMAND ----------

# Prepare attribute version sources for new records
attribute_source_mappings_schema = spark.read.table(master_attribute_version_sources_table).schema
attribute_source_mappings = []
for record in unmatched_records_df.collect():
    mapping = {
        id_key: record[id_key],
        'version': master_version,
        'attribute_source_mapping': [
            {
                'attribute_name': attr,
                'attribute_value': str(record[attr]) if record[attr] is not None else None,
                'source': record['dataset']['path']
            }
            for attr in entity_attributes if attr != id_key
        ]
    }
    attribute_source_mappings.append(mapping)

attribute_sources_df = spark.createDataFrame(attribute_source_mappings, schema=attribute_source_mappings_schema)

# COMMAND ----------

merge_activities_schema = spark.read.table(merge_activities_table).schema
merge_activities = []
for record in unmatched_records_df.collect():
    activity = {
        'master_' + id_key: record[id_key],
        id_key: '',
        'source': record['dataset']['path'],
        'version': master_version,
        'action_type': ActionType.JOB_INSERT.value
    }
    merge_activities.append(activity)

merge_activities_df = spark.createDataFrame(merge_activities, schema=merge_activities_schema)

# COMMAND ----------

delta_table = DeltaTable.forName(spark, master_table)
delta_table.alias("target").merge(
    source=new_master_records_df.alias("source"),
    condition=f"target.{id_key} = source.{id_key}"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

delta_table = DeltaTable.forName(spark, master_attribute_version_sources_table)
delta_table.alias("target").merge(
    source=attribute_sources_df.alias("source"),
    condition=f"target.{id_key} = source.{id_key} and target.version = source.version"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

delta_table = DeltaTable.forName(spark, merge_activities_table)
delta_table.alias("target").merge(
    source=merge_activities_df.alias("source"),
    condition=f"""
        target.{id_key} = source.{id_key} 
        and target.master_{id_key} = source.master_{id_key}
        and target.version = source.version 
        and target.action_type = source.action_type 
        and target.source = source.source
    """
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()    
