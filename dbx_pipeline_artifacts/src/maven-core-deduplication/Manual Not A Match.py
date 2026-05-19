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
dbutils.widgets.text("default_survivorship_rules", "", "Default Survivorship Rules")
dbutils.widgets.text('experiment_id', '', 'Experiment ID')
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")
dbutils.widgets.text("config_thresold", "", "Model Config Thresholds")
dbutils.widgets.text("master_id", "", "Master Record ID")
dbutils.widgets.text("match_record_id", "", "Match Record ID to be merged")  # Changed from unified_dataset_ids
dbutils.widgets.text("catalog_name", "lakefusion_ai_uat", "Catalog Name")
dbutils.widgets.text("primary_table", "", "Primary Table")

# COMMAND ----------

id_key = dbutils.widgets.get("id_key")
entity = dbutils.widgets.get("entity")
dataset_objects = dbutils.widgets.get("dataset_objects")
default_survivorship_rules = dbutils.widgets.get("default_survivorship_rules")
experiment_id = dbutils.widgets.get('experiment_id')
entity_attributes = dbutils.widgets.get("entity_attributes")
config_thresold = dbutils.widgets.get("config_thresold")
master_id = dbutils.widgets.get("master_id")
match_record_id = dbutils.widgets.get("match_record_id")  # The record to be merged into master
catalog_name = dbutils.widgets.get("catalog_name")
primary_table = dbutils.widgets.get("primary_table")

# COMMAND ----------

entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
id_key = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="id_key", debugValue=id_key)
dataset_objects = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="dataset_objects", debugValue=dataset_objects)
entity_attributes = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes", debugValue=entity_attributes)
primary_table = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="primary_table", debugValue=primary_table)

# COMMAND ----------

dataset_objects = json.loads(dataset_objects)
entity_attributes = json.loads(entity_attributes)

# COMMAND ----------

# MAGIC %run ../utils/match_merge_utils

# COMMAND ----------

unified_table = f"{catalog_name}.silver.{entity}_unified_deduplicate"
master_table = f"{catalog_name}.gold.{entity}_master"
processed_unified_table = f"{catalog_name}.silver.{entity}_processed_unified_deduplicate"
if experiment_id:
  unified_table += f"_{experiment_id}"
  master_table += f"_{experiment_id}"
  processed_unified_table += f"_{experiment_id}"

merge_activities_table = f"{master_table}_merge_activities"
master_attribute_version_sources_table = f"{master_table}_attribute_version_sources"

# COMMAND ----------

query = f"""
WITH RankedRecords AS (
    SELECT {id_key}, version, attribute_source_mapping,
        ROW_NUMBER() OVER (PARTITION BY {id_key} ORDER BY version DESC) AS rn
    FROM {master_attribute_version_sources_table}
    WHERE {id_key} IN ('{master_id}', '{match_record_id}')
)
SELECT
  ma.*,
  asm.attribute_source_mapping as master_attribute_source_mapping
FROM
  {master_table} ma
  INNER JOIN (
    SELECT {id_key}, version, attribute_source_mapping
    FROM RankedRecords
    WHERE rn = 1
  ) asm ON ma.{id_key} = asm.{id_key}
WHERE
  ma.{id_key} IN ('{master_id}', '{match_record_id}')
"""

# COMMAND ----------

potential_matches_df = spark.sql(query)

# COMMAND ----------

potential_matches_df.display()

# COMMAND ----------

filtered_df = potential_matches_df.filter(
    col(id_key).isin([master_id, match_record_id])
)

# COMMAND ----------

master_version = get_current_table_version(master_table)

# COMMAND ----------

current_merge_activities_table = spark.read.table(merge_activities_table)

# COMMAND ----------

resultant_merge_activities_not_match = spark.createDataFrame([{
    f'master_{id_key}': master_id,
    id_key: match_record_id,
    'source': primary_table,
    'version': master_version,
    'action_type': ActionType.MANUAL_NOT_A_MATCH.value
}])

# COMMAND ----------

delta_table = DeltaTable.forName(spark, merge_activities_table)

# Perform merge operation
delta_table.alias("target").merge(
    source=resultant_merge_activities_not_match.alias("source"),
    condition=f"target.{id_key} = source.{id_key} and target.version = source.version and target.master_{id_key} = source.master_{id_key} and target.action_type = source.action_type and target.source = source.source"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %run "/Lakefusion_Notebooks/match-maven-notebooks/src/maven-core-deduplication/Process Potential Match Table"
