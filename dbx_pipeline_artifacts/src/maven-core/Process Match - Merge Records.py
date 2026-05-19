# Databricks notebook source
from uuid import uuid4
import json
from delta.tables import DeltaTable
from pyspark.sql.functions import (
    col, explode, lit, collect_list, first, struct, count, 
    arrays_zip, least, greatest, udf
)
from pyspark.sql.types import *
import pandas as pd
from databricks.sdk import WorkspaceClient
import requests
from dbruntime.databricks_repl_context import get_context
import re

# COMMAND ----------

dbutils.widgets.text("id_key", "", "Primary Key")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("dataset_objects", "", "Dataset Objects")
dbutils.widgets.text("default_survivorship_rules", "", "Default Survivorship Rules")
dbutils.widgets.text('experiment_id', '', 'Experiment ID')
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")
dbutils.widgets.text("config_thresold", "", "Model Config Thresholds")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("entity_attributes_datatype", "", "Entity Attributes Datatype")

# COMMAND ----------

id_key = dbutils.widgets.get("id_key")
entity = dbutils.widgets.get("entity")
dataset_objects = dbutils.widgets.get("dataset_objects")
default_survivorship_rules = dbutils.widgets.get("default_survivorship_rules")
experiment_id = dbutils.widgets.get('experiment_id')
entity_attributes = dbutils.widgets.get("entity_attributes")
config_thresold = dbutils.widgets.get("config_thresold")
catalog_name = dbutils.widgets.get("catalog_name")
entity_attributes_datatype = dbutils.widgets.get("entity_attributes_datatype")

# COMMAND ----------

entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
id_key = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="id_key", debugValue=id_key)
dataset_objects = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="dataset_objects", debugValue=dataset_objects)
default_survivorship_rules = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="default_survivorship_rules", debugValue=default_survivorship_rules)
entity_attributes = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes", debugValue=entity_attributes)
config_thresold = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "config_thresold", debugValue=config_thresold)
entity_attributes_datatype = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", 
    "entity_attributes_datatype", 
    debugValue=dbutils.widgets.get("entity_attributes_datatype")
)

# COMMAND ----------

dataset_objects = json.loads(dataset_objects)
default_survivorship_rules = json.loads(default_survivorship_rules)
entity_attributes = json.loads(entity_attributes)
entity_attributes.append("lakefusion_id")
config_thresold = json.loads(config_thresold)
entity_attributes_datatype = json.loads(entity_attributes_datatype)

# COMMAND ----------

merge_min_max = config_thresold.get('merge')
merge_min = merge_min_max[0]
merge_max = merge_min_max[1]

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

master_table_list = master_table.split(".")
master_catalog, master_schema, master_table_name = master_table_list[0], master_table_list[1], master_table_list[2]

# COMMAND ----------

# Create a mapping of id to path from dataset_objects
dataset_id_to_path = {dataset['id']: dataset['path'] for dataset in dataset_objects.values()}

# Iterate through the survivorship rules
for rule in default_survivorship_rules:
    if rule.get('strategy') == 'Source System' and 'strategyRule' in rule:
        # Replace the strategyRule with dataset paths
        rule['strategyRule'] = [dataset_id_to_path.get(dataset_id) for dataset_id in rule['strategyRule']]

# COMMAND ----------

# # Create a mapping of id to path from dataset_objects
# dataset_id_to_path = {dataset['id']: dataset['path'] for dataset in dataset_objects.values()}

# # Iterate through the survivorship rules
# for rule in default_survivorship_rules:
#     if rule.get('strategy') == 'Source System' and 'strategyRule' in rule:
#         # Replace the strategyRule with dataset paths
#         rule['strategyRule'] = [dataset_id_to_path.get(dataset_id) for dataset_id in rule['strategyRule']]

# COMMAND ----------

# Dynamically build the SELECT fields for the entity attributes
attribute_select = ",\n      ".join(
    [f"u.{attr} as unified_{attr}" for attr in entity_attributes]
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
      int(u.dataset.id) as unified_dataset_id,
      u.dataset.path as unified_dataset_path,
      u.master_{id_key},
      u.exploded_result.score as match_score
    FROM
      (
        select pu.*, uo.* except ({id_key}) from 
        {catalog_name}.silver.{entity}_processed_unified_{experiment_id} pu
        inner join {catalog_name}.silver.{entity}_unified_{experiment_id} uo
        on pu.{id_key} = uo.{id_key}
      ) u
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
logger.info(f"Potential matches count: {potential_matches_df.count()}")

# COMMAND ----------

# Filter out already merged pairs
try:
    # Get already merged pairs (master_id, unified_id)
    merged_pairs_query = f"""
    SELECT DISTINCT 
        master_{id_key} as master_id,
        {id_key} as unified_id
    FROM {merge_activities_table}
    WHERE action_type IN ('JOB_MERGE', 'MANUAL_MERGE')
    AND {id_key} IS NOT NULL
    AND master_{id_key} IS NOT NULL
    """
    
    merged_pairs_df = spark.sql(merged_pairs_query)
    merged_pairs_count = merged_pairs_df.count()
    
    if merged_pairs_count > 0:
        logger.info(f"Found {merged_pairs_count} already merged pairs to exclude")
        
        # Left anti join to exclude already merged pairs
        potential_matches_df = potential_matches_df.join(
            merged_pairs_df,
            (potential_matches_df[id_key] == merged_pairs_df['master_id']) &
            (potential_matches_df[f'unified_{id_key}'] == merged_pairs_df['unified_id']),
            how='left_anti'
        )
        
        remaining_count = potential_matches_df.count()
        logger.info(f"After filtering already merged pairs. Remaining count: {remaining_count}")
    else:
        logger.info("No previously merged pairs found")

except Exception as e:
    logger.error(f"Error during filtering: {e}")
    logger.info("Continuing without filtering")

# COMMAND ----------

mergable_records_df = potential_matches_df.filter(potential_matches_df.match_score.between(merge_min, merge_max))

# COMMAND ----------

mergable_records_df_grouped = (
    mergable_records_df
    .select(
        col(id_key).alias(f'master_{id_key}'),
        struct(*[col(f'unified_{col_name}').alias(col_name) for col_name in entity_attributes]).alias('unified_record'),
        struct(*[col(f'{col_name}') for col_name in entity_attributes]).alias('master_record'),
        col('unified_dataset_path'),
        col('master_attribute_source_mapping')
    )
    .groupBy(f'master_{id_key}')
    .agg(
        collect_list('unified_record').alias('unified_records'),
        first('master_record').alias('master_record'),
        collect_list('unified_dataset_path').alias('unified_dataset_paths'),
        first('master_attribute_source_mapping').alias('master_attribute_source_mapping')
    )
)

# COMMAND ----------

# Updated function for applying the source system strategy
def apply_source_system_strategy(attribute, strategyRule, unified_records, master_record, unified_datasets, master_attribute_source_mapping_column, ignore_null=False):
    # Initialize result as None (fallback if nothing is found)
    result_value = None
    result_source = master_attribute_source_mapping_column.get('source')
    
    # First, check if the value exists in any of the unified datasets, respecting the order in strategyRule
    unified_column_values_in_order = [None] * len(unified_datasets)
    for index, dataset in enumerate(strategyRule):
        # Check in the unified_records for the matching dataset
        try:
            unified_dataset_index = unified_datasets.index(dataset)
            unified_record_column_value = unified_records[unified_dataset_index][attribute]
            
            # Apply ignore_null logic
            if ignore_null and (unified_record_column_value is None or unified_record_column_value == ""):
                continue
                
            unified_column_values_in_order[index] = unified_record_column_value
            result_source = dataset
        except:
            pass

    # Find the first non-null value if ignore_null is True, otherwise take the first value
    if ignore_null:
        result_value = next((val for val in unified_column_values_in_order if val is not None and val != ""), None)
    else:
        result_value = unified_column_values_in_order[0]

    # If no matching unified dataset is found, fall back to master record
    if result_value is None and master_record.get(attribute):
        master_value = master_record[attribute]
        if not ignore_null or (master_value is not None and master_value != ""):
            result_value = master_value
    
    return result_value, result_source  # Return the final value (either from unified or master record)

# COMMAND ----------

def apply_aggregation_strategy(attribute, master_record, unified_records, entity_attributes_datatype, ignore_null=False, aggregate_unique_only=False):
    attr_datatype = entity_attributes_datatype.get(attribute, 'string')
    
    # Get unified values, handle None values based on ignore_null
    unified_values = []
    for record in unified_records:
        val = record.get(attribute)
        if ignore_null:
            if val is not None and val != '':
                unified_values.append(val)
        else:
            if val is not None:
                unified_values.append(val)
    
    # Get master value and handle if it's already aggregated
    master_value = master_record.get(attribute)
    master_values = []
    
    if master_value is not None:
        if ignore_null and master_value == '':
            master_values = []
        else:
            # Check if master value is already aggregated (contains bullet separator)
            if is_string_type(attr_datatype) and ' • ' in str(master_value):
                # Split the aggregated value back into individual values
                master_values = [v.strip() for v in str(master_value).split(' • ')]
            else:
                master_values = [master_value]

    all_values = master_values + unified_values
    if not all_values:
        return None
    
    if is_numeric_type(attr_datatype):
        # For numeric types, calculate the average
        try:
            numeric_values = [float(val) for val in all_values if val is not None]
            if not numeric_values:
                return None
            
            avg_value = sum(numeric_values) / len(numeric_values)
            if 'int' in attr_datatype.lower():
                return int(round(avg_value))
            else:
                return avg_value
        except (ValueError, TypeError):
            return all_values[0]
    
    elif is_timestamp_type(attr_datatype):
        try:
            import pandas as pd
            datetime_values = []
            for val in all_values:
                if val is not None:
                    if isinstance(val, str):
                        try:
                            datetime_values.append(pd.to_datetime(val))
                        except:
                            continue
                    else:
                        datetime_values.append(val)
            
            if datetime_values:
                return max(datetime_values)
            return all_values[0]
        except:
            return all_values[0]
    
    elif is_string_type(attr_datatype):
        all_values = [str(val) for val in all_values if val and str(val).strip() != '']
        
        if aggregate_unique_only:
            seen = set()
            unique_values = []
            for val in all_values:
                if val not in seen:
                    seen.add(val)
                    unique_values.append(val)
            all_values = unique_values
        else:
            # Remove duplicates while preserving order
            all_values = list(dict.fromkeys(all_values))
        
        all_values = [val for val in all_values if val]
        
        if not all_values:
            return None
        
        result_value = ' • '.join(all_values)
        return result_value
    
    else:
        return all_values[0]

# COMMAND ----------

def apply_recency_strategy(attribute, strategy_rule, unified_records, master_record, unified_datasets, master_attribute_source_mapping_column, ignore_null=False):
    result_value = master_record.get(attribute)
    result_source = master_attribute_source_mapping_column.get('source') if master_attribute_source_mapping_column else None
    
    # Apply ignore_null to master record
    if ignore_null and (result_value is None or result_value == ""):
        result_value = None
    
    record_by_dataset_id = {}
    for i, dataset_path in enumerate(unified_datasets):
        if i < len(unified_records):
            try:
                dataset_id = int(dataset_path.split('/')[-1])
                record_by_dataset_id[dataset_id] = {
                    'record': unified_records[i],
                    'path': dataset_path
                }
            except (ValueError, IndexError):
                pass
    
    most_recent_timestamp = None
    most_recent_record = None
    most_recent_dataset_path = None
    
    records_with_values = []
    
    # strategy_rule is now just the date attribute name
    date_attr = strategy_rule
    
    if not date_attr or date_attr == "__none__":
        return result_value, result_source
    
    # Check all records for the most recent timestamp
    for dataset_id, record_info in record_by_dataset_id.items():
        record = record_info['record']
        dataset_path = record_info['path']
        
        if attribute not in record or record[attribute] is None:
            continue
            
        # Apply ignore_null logic
        value = record[attribute]
        if ignore_null and (value is None or value == ""):
            continue
        
        records_with_values.append({
            'value': value,
            'source': dataset_path
        })
        
        timestamp_value = record.get(date_attr)
        
        if timestamp_value is None:
            continue
            
        if isinstance(timestamp_value, str):
            try:
                timestamp_value = pd.to_datetime(timestamp_value)
            except:
                continue
        
        if most_recent_timestamp is None or timestamp_value > most_recent_timestamp:
            most_recent_timestamp = timestamp_value
            most_recent_record = record
            most_recent_dataset_path = dataset_path
    
    if most_recent_record is not None:
        recent_value = most_recent_record[attribute]
        if not ignore_null or (recent_value is not None and recent_value != ""):
            result_value = recent_value
            result_source = most_recent_dataset_path
    elif records_with_values:
        result_value = records_with_values[0]['value']
        result_source = records_with_values[0]['source']
    
    return result_value, result_source

# COMMAND ----------

# UDF to process the survivorship rules
def process_survivorship_rules(unified_records, master_record, unified_datasets, master_attribute_source_mapping):
    master_record = master_record.asDict()
    unified_records = [record.asDict() for record in unified_records]
    master_attribute_source_mapping = [record.asDict() for record in master_attribute_source_mapping]

    resultant_record = {}
    resultant_master_attribute_source_mapping = []

    # Define the columns that are in entity_attributes
    entity_attributes_to_be_process = [entity_attribute for entity_attribute in entity_attributes if entity_attribute != id_key]

    # Always take id_key from master_record
    resultant_record[id_key] = master_record.get(id_key)

    for attribute in entity_attributes:
        master_attribute_source_mapping_column = next((item for item in master_attribute_source_mapping if item.get('attribute_name') == attribute), None)

        if attribute == id_key:
            continue  # Skip id_key as it is handled above

        # Get the corresponding survivorship rule for the attribute
        rule = next((item for item in default_survivorship_rules if item.get('attribute') == attribute or item.get('entity_attribute') == attribute), None)

        if rule:
            # Extract toggle properties
            ignore_null = rule.get('ignoreNull', False)
            
            if rule['strategy'] == 'Source System':
                # Apply source system strategy
                strategyRule = rule['strategyRule']
                value, source = apply_source_system_strategy(
                    attribute, strategyRule, unified_records, master_record, 
                    unified_datasets, master_attribute_source_mapping_column, ignore_null
                )
                resultant_record[attribute] = value
                
                # Update the master_attribute_source_mapping
                resultant_master_attribute_source_mapping.append({
                    'attribute_name': attribute,
                    'attribute_value': str(value),
                    'source': source
                })
            elif rule['strategy'] == 'Aggregation':
                # Apply aggregation strategy
                aggregate_unique_only = rule.get('aggregateUniqueOnly', False)
                aggregated_value = apply_aggregation_strategy(
                    attribute, master_record, unified_records, entity_attributes_datatype, ignore_null, aggregate_unique_only
                )
                resultant_record[attribute] = aggregated_value
                
                # Update the master_attribute_source_mapping for aggregation
                resultant_master_attribute_source_mapping.append({
                    'attribute_name': attribute,
                    'attribute_value': str(aggregated_value) if aggregated_value is not None else '',
                    'source': ','.join([master_attribute_source_mapping_column.get('source')] + unified_datasets)
                })
            elif rule['strategy'] == 'Recency':
                # Apply recency strategy with simplified structure
                strategy_rule = rule.get('strategyRule', '')
                value, source = apply_recency_strategy(
                    attribute, strategy_rule, unified_records, master_record, 
                    unified_datasets, master_attribute_source_mapping_column, ignore_null
                )
                resultant_record[attribute] = value
                
                # Update the master_attribute_source_mapping
                resultant_master_attribute_source_mapping.append({
                    'attribute_name': attribute,
                    'attribute_value': str(value) if value is not None else '',
                    'source': source
                })
        else:
            # Default: use master record value
            result_value = master_record.get(attribute)
            result_source = master_attribute_source_mapping_column.get('source') if master_attribute_source_mapping_column else ''
            
            resultant_record[attribute] = result_value
            
            resultant_master_attribute_source_mapping.append({
                'attribute_name': attribute,
                'attribute_value': str(result_value) if result_value is not None else '',
                'source': result_source
            })
    
    return {
        'resultant_record': resultant_record,
        'resultant_master_attribute_source_mapping': resultant_master_attribute_source_mapping
    }

# COMMAND ----------

# Extract the schema for resultant_record from master_record
resultant_record_schema = mergable_records_df_grouped.select("master_record").schema["master_record"].dataType

# Extract the schema for resultant_master_attribute_source_mapping
attribute_source_mapping_schema = mergable_records_df_grouped.select("master_attribute_source_mapping").schema["master_attribute_source_mapping"].dataType

# Define the UDF output schema using the extracted schemas
udf_output_schema = StructType([
    StructField("resultant_record", resultant_record_schema, True),
    StructField("resultant_master_attribute_source_mapping", attribute_source_mapping_schema, True)
])

# COMMAND ----------

def process_survivorship_rules_wrapper(unified_records, master_record, unified_datasets, master_attribute_source_mapping):
    return process_survivorship_rules(
        unified_records, 
        master_record, 
        unified_datasets, 
        master_attribute_source_mapping
    )

# COMMAND ----------

# Register the UDF
process_survivorship_udf = udf(process_survivorship_rules_wrapper, udf_output_schema)

# COMMAND ----------

mergable_records_df_grouped_survivorship = (
    mergable_records_df_grouped
    .withColumn(
        'survivorship_result', 
        process_survivorship_udf(
            col('unified_records'),
            col('master_record'),
            col('unified_dataset_paths'),
            col('master_attribute_source_mapping')
        )
    )
    .withColumn('resultant_record', col('survivorship_result.resultant_record'))
    .withColumn('resultant_master_attribute_source_mapping', col('survivorship_result.resultant_master_attribute_source_mapping'))
)

# COMMAND ----------

mergable_records_survivorship_one_col = (
    mergable_records_df_grouped_survivorship
    .select('resultant_record')
)

# Get the field names of the 'resultant_record' struct
nested_fields = mergable_records_survivorship_one_col.schema["resultant_record"].dataType.fields

# Dynamically create a list of column expressions
columns_to_select = [col(f"resultant_record.{field.name}").alias(field.name) for field in nested_fields]

# Select the columns dynamically into 'mergable_records_survivorship'
mergable_records_survivorship = mergable_records_survivorship_one_col.select(*columns_to_select)

# COMMAND ----------

current_master_table = spark.read.table(master_table)
# List of columns in current_master_table excluding the id_key
remaining_cols = [
    col_name for col_name in current_master_table.columns if col_name not in mergable_records_survivorship.columns
]
# Perform the join
joined_df = mergable_records_survivorship.join(
    current_master_table, on=id_key, how='inner'
)
# Select all columns from mergable_records_survivorship and remaining columns from current_master_table
resultant_merged_df = joined_df.select(
    [mergable_records_survivorship[col] for col in mergable_records_survivorship.columns] +
    [current_master_table[col] for col in remaining_cols]
)

# COMMAND ----------

# Define the Delta table
delta_table = DeltaTable.forName(spark, master_table)

# Perform merge operation
delta_table.alias("target").merge(
    source=resultant_merged_df.alias("source"),
    condition=f"target.{id_key} = source.{id_key}"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

current_master_attribute_source_mapping = spark.read.table(master_attribute_version_sources_table)

# COMMAND ----------

master_attribute_version_sources_table

# COMMAND ----------

master_version = get_current_table_version(master_table)

resultant_master_attribute_source_mapping_survivorship = (
    mergable_records_df_grouped_survivorship
    .select(
        col(f'resultant_record.{id_key}').alias(id_key),
        lit(master_version).alias('version'),
        col('resultant_master_attribute_source_mapping').alias('attribute_source_mapping')
    )
)

# COMMAND ----------

# Define the Delta table
delta_table = DeltaTable.forName(spark, master_attribute_version_sources_table)

# Perform merge operation
delta_table.alias("target").merge(
    source=resultant_master_attribute_source_mapping_survivorship.alias("source"),
    condition=f"target.{id_key} = source.{id_key} and target.version = source.version"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

current_merge_activities_table = spark.read.table(merge_activities_table)

# COMMAND ----------

resultant_merge_activities_survivorship = (
    mergable_records_df
    .select(
        col(id_key).alias(f'master_{id_key}'),
        col(f'unified_{id_key}').alias(id_key),
        col(f'unified_dataset_path').alias('source'),
        lit(master_version).alias('version'),
        lit(ActionType.JOB_MERGE.value).alias("action_type")
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
