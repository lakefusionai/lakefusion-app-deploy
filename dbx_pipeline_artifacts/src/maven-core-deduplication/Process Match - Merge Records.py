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
dbutils.widgets.text("primary_table", "", "Primary Table")
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
primary_table = dbutils.widgets.get("primary_table")
entity_attributes_datatype = dbutils.widgets.get("entity_attributes_datatype")

# COMMAND ----------

entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
id_key = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="id_key", debugValue=id_key)
dataset_objects = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="dataset_objects", debugValue=dataset_objects)
default_survivorship_rules = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="default_survivorship_rules", debugValue=default_survivorship_rules)
entity_attributes = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes", debugValue=entity_attributes)
config_thresold = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "config_thresold", debugValue=config_thresold)
primary_table = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "primary_table", debugValue=primary_table)
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

aggregation_strategy_separator = ' • '

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

# Create a mapping of id to path from dataset_objects
dataset_id_to_path = {dataset['id']: dataset['path'] for dataset in dataset_objects.values()}

# Iterate through the survivorship rules
for rule in default_survivorship_rules:
    if rule.get('strategy') == 'Source System' and 'strategyRule' in rule:
        # Replace the strategyRule with dataset paths
        rule['strategyRule'] = [dataset_id_to_path.get(dataset_id) for dataset_id in rule['strategyRule']]

# COMMAND ----------

potential_matches_query = f"""
WITH MasterMatches AS (
    SELECT 
        pu.master_{id_key} as master_id,
        pu.{id_key} as match_id,
        pu.exploded_result.score as match_score
    FROM {processed_unified_table} pu
    WHERE pu.exploded_result.match = 'MATCH'
    AND pu.master_{id_key} != pu.{id_key}
    AND pu.exploded_result.score BETWEEN {merge_min} AND {merge_max}
),
-- Remove bidirectional duplicates: keep only master_id < match_id
UniquePairs AS (
    SELECT 
        CASE WHEN master_id < match_id THEN master_id ELSE match_id END as master_id,
        CASE WHEN master_id < match_id THEN match_id ELSE master_id END as match_id,
        MAX(match_score) as match_score
    FROM MasterMatches
    GROUP BY 
        CASE WHEN master_id < match_id THEN master_id ELSE match_id END,
        CASE WHEN master_id < match_id THEN match_id ELSE master_id END
)
SELECT DISTINCT master_id, match_id, match_score 
FROM UniquePairs
"""

potential_matches_df = spark.sql(potential_matches_query)

# COMMAND ----------

# Get already merged record pairs from merge activities table
merged_pairs_query = f"""
SELECT DISTINCT 
    master_{id_key} as id1,
    {id_key} as id2
FROM {merge_activities_table}
WHERE action_type IN ('JOB_MERGE', 'MANUAL_MERGE')
AND {id_key} IS NOT NULL
AND master_{id_key} IS NOT NULL
"""

try:
    merged_pairs_df = spark.sql(merged_pairs_query)
    
    if merged_pairs_df.count() > 0:
        logger.info(f"Found {merged_pairs_df.count()} already merged record pairs")
        
        merged_pairs_normalized = merged_pairs_df.select(
            least(col('id1'), col('id2')).alias('pair_id1'),
            greatest(col('id1'), col('id2')).alias('pair_id2')
        ).distinct()
        
        potential_matches_normalized = potential_matches_df.select(
            col('*'),
            least(col('master_id'), col('match_id')).alias('pair_id1'),
            greatest(col('master_id'), col('match_id')).alias('pair_id2')
        )
        
        potential_matches_df = potential_matches_normalized.join(
            merged_pairs_normalized,
            on=['pair_id1', 'pair_id2'],
            how='left_anti'
        ).select(potential_matches_df.columns)
        
        logger.info(f"After filtering merged pairs. Remaining count: {potential_matches_df.count()}")
    else:
        logger.info("No previously merged record pairs found")
        
except Exception as e:
    logger.error(f"Error fetching merged pairs: {e}")
    logger.info("Continuing without filtering merged pairs")

# COMMAND ----------

master_groups_df = potential_matches_df.groupBy("master_id").agg(
    collect_list("match_id").alias("match_ids"),
    collect_list("match_score").alias("match_scores")
)
master_records_query = f"""
WITH LatestAttributes AS (
    SELECT 
        {id_key}, 
        attribute_source_mapping,
        ROW_NUMBER() OVER (PARTITION BY {id_key} ORDER BY version DESC) AS rn
    FROM {master_attribute_version_sources_table}
),
LatestAttrs AS (
    SELECT {id_key}, attribute_source_mapping
    FROM LatestAttributes
    WHERE rn = 1
)
SELECT 
    m.*,
    COALESCE(la.attribute_source_mapping, array()) as attribute_source_mapping
FROM {master_table} m
LEFT JOIN LatestAttrs la ON m.{id_key} = la.{id_key}
"""

all_master_records_df = spark.sql(master_records_query)

# COMMAND ----------

mergable_records_df_grouped = (
    master_groups_df
    .select(
        col('master_id'),
        explode(arrays_zip(col('match_ids'), col('match_scores'))).alias('match_info')
    )
    .select(
        col('master_id'),
        col('match_info.match_ids').alias('match_id'),
        col('match_info.match_scores').alias('match_score')
    )
    .join(all_master_records_df.alias('master'), col('master_id') == col(f'master.{id_key}'), 'inner')
    .join(all_master_records_df.alias('match'), col('match_id') == col(f'match.{id_key}'), 'inner')
    .groupBy('master_id')
    .agg(
        first(struct(*[col(f'master.{attr}') for attr in entity_attributes])).alias('master_record'),
        first(col('master.attribute_source_mapping')).alias('master_attribute_source_mapping'),
        collect_list(struct(*[col(f'match.{attr}') for attr in entity_attributes])).alias('match_records'),
        collect_list(col('match.attribute_source_mapping')).alias('match_attribute_source_mappings'),
        collect_list('match_id').alias('match_ids'),
        collect_list('match_score').alias('match_scores'),
        count('match_id').alias('match_count')
    )
    .select(
        col('master_id').alias(f'master_{id_key}'),
        col('master_record'),
        col('master_attribute_source_mapping'),
        col('match_records'),
        col('match_attribute_source_mappings'),
        col('match_ids'),
        col('match_scores'),
        col('match_count')
    )
)

# COMMAND ----------

def get_attribute_source_info(attribute_source_mapping, attribute_name, record):
    """Extract source info for a specific attribute from the mapping array"""
    if attribute_source_mapping:
        for mapping in attribute_source_mapping:
            if mapping.get('attribute_name') == attribute_name:
                return {
                    'value': record[attribute_name],
                    'source': mapping.get('source')
                }
    return {'value': None, 'source': None}

# COMMAND ----------

def apply_source_system_strategy_multi_merge(attribute, strategyRule, all_attr_infos, ignore_null=False):
    """
    Apply source system strategy for multi-record merge
    Compare sources based on survivorship rule priority across all records
    """
    best_value = None
    best_source = None
    best_priority = float('inf')
    
    for attr_info in all_attr_infos:
        source = attr_info.get('source')
        value = attr_info.get('value')
        
        # Apply ignore_null logic
        if ignore_null and (value is None or value == ""):
            continue
        
        if source in strategyRule:
            priority = strategyRule.index(source)
            if priority < best_priority:
                best_priority = priority
                best_value = value
                best_source = source
    
    # If no value found in priority sources, look for any non-null value
    if best_value is None:
        for attr_info in all_attr_infos:
            value = attr_info.get('value')
            if value is not None:
                if not ignore_null or value != "":
                    best_value = value
                    best_source = attr_info.get('source')
                    break
    
    return best_value, best_source

# COMMAND ----------

def apply_aggregation_strategy_multi_merge(attribute, all_attr_infos, entity_attributes_datatype, ignore_null=False, aggregate_unique_only=False):
    """Apply aggregation strategy for multi-record merge with datatype awareness"""
    attr_datatype = entity_attributes_datatype.get(attribute, 'string')
    
    all_values = []
    all_sources = []
    
    for attr_info in all_attr_infos:
        value = attr_info.get('value')
        source = attr_info.get('source')
        
        # Apply ignore_null logic
        if ignore_null and (value is None or value == ""):
            continue
        
        if value is not None:
            # For string types, split by separator; for others, treat as single value
            if is_string_type(attr_datatype) and isinstance(value, str):
                all_values.extend(value.split(aggregation_strategy_separator))
            else:
                all_values.append(value)
        
        if source:
            all_sources.append(source)
    
    if not all_values:
        return None, None
    
    # Handle based on datatype
    if is_numeric_type(attr_datatype):
        # For numeric types, calculate the average
        try:
            numeric_values = [float(val) for val in all_values if val is not None]
            if not numeric_values:
                return None, None
            
            avg_value = sum(numeric_values) / len(numeric_values)
            if 'int' in attr_datatype.lower():
                result_value = int(round(avg_value))
            else:
                result_value = avg_value
        except (ValueError, TypeError):
            result_value = all_values[0]
    
    elif is_timestamp_type(attr_datatype):
        # For datetime types, return the most recent value
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
                result_value = max(datetime_values)
            else:
                result_value = all_values[0] if all_values else None
        except:
            result_value = all_values[0] if all_values else None
    
    elif is_string_type(attr_datatype):
        # For string types, concatenate with separator
        clean_values = [str(v).strip() for v in all_values if v and str(v).strip()]
        
        if aggregate_unique_only:
            # Preserve order while removing duplicates
            seen = set()
            unique_values = []
            for value in clean_values:
                if value not in seen:
                    seen.add(value)
                    unique_values.append(value)
            clean_values = unique_values
        else:
            # Use dict.fromkeys to preserve order while removing duplicates
            clean_values = list(dict.fromkeys(clean_values))
        
        result_value = aggregation_strategy_separator.join(clean_values) if clean_values else None
    
    else:
        # For other types, return the first value
        result_value = all_values[0]
    
    # Combine sources
    unique_sources = list(dict.fromkeys(all_sources))  # Preserve order, remove duplicates
    result_source = ','.join(unique_sources) if unique_sources else None
    
    return result_value, result_source

# COMMAND ----------

def apply_recency_strategy_multi_merge(attribute, strategyRule, all_attr_infos, all_records, ignore_null=False):
    """
    Apply recency strategy for multi-record merge
    Compare date/timestamp values across all records to determine most recent
    Falls back to master record value when strategy fails
    """
    def get_master_fallback():
        if all_attr_infos and len(all_attr_infos) > 0:
            master_attr_info = all_attr_infos[0]
            master_value = master_attr_info.get('value')
            master_source = master_attr_info.get('source')
            
            # Apply ignore_null to fallback
            if ignore_null and (master_value is None or master_value == ""):
                return None, None
            
            return master_value, master_source
        return None, None
    
    # strategyRule should be the date attribute name
    if not strategyRule or strategyRule == "__none__":
        logger.info(f"No recency attribute specified for attribute '{attribute}', falling back to master")
        return get_master_fallback()
    
    date_attr = strategyRule
    
    # Check if the date attribute exists in any record
    date_attr_exists = False
    for record in all_records:
        record_dict = record.asDict() if hasattr(record, 'asDict') else record
        if date_attr in record_dict and record_dict.get(date_attr) is not None:
            date_attr_exists = True
            break
    
    if not date_attr_exists:
        logger.info(f"Date attribute '{date_attr}' not found in any records or all values are None, falling back to master")
        return get_master_fallback()
    
    best_value = None
    best_source = None
    best_date = None
    best_record_index = -1
    
    # Iterate through all records to find the one with the most recent date
    for i, attr_info in enumerate(all_attr_infos):
        value = attr_info.get('value')
        
        # Apply ignore_null logic
        if ignore_null and (value is None or value == ""):
            continue
            
        if value is None:
            continue
            
        record = all_records[i]
        record_dict = record.asDict() if hasattr(record, 'asDict') else record
        date_value = record_dict.get(date_attr)
        
        if date_value is None:
            continue
            
        try:
            import pandas as pd
            
            # Parse the date value
            if isinstance(date_value, str):
                parsed_date = pd.to_datetime(date_value)
            else:
                parsed_date = date_value
            
            # Check if this is the most recent date so far
            if best_date is None or parsed_date > best_date:
                best_date = parsed_date
                best_value = value
                best_source = attr_info.get('source')
                best_record_index = i
                logger.info(f"Record {i} has more recent date for '{date_attr}': {parsed_date}")
                
        except Exception as e:
            logger.error(f"Error parsing date value '{date_value}' for record {i}: {e}")
            continue
    
    if best_value is not None:
        logger.info(f"Selected record {best_record_index} with date {best_date} for attribute '{attribute}'")
        return best_value, best_source
    
    logger.info(f"No valid dates found for attribute '{date_attr}', falling back to master")
    return get_master_fallback()

# COMMAND ----------

def process_multi_master_merge_survivorship(master_record, match_records, master_attribute_source_mapping, match_attribute_source_mappings):
    """
    Process survivorship rules for multi-master merge using attribute version sources
    """
    master_dict = master_record.asDict() if hasattr(master_record, 'asDict') else master_record
    match_dicts = []
    for match in match_records:
        match_dicts.append(match.asDict() if hasattr(match, 'asDict') else match)
    
    all_records = [master_record] + match_records
    
    # Get attribute source mappings - convert if needed
    if isinstance(master_attribute_source_mapping, list) and len(master_attribute_source_mapping) > 0 and hasattr(master_attribute_source_mapping[0], 'asDict'):
        master_attr_mapping = [attr.asDict() for attr in master_attribute_source_mapping]
    else:
        master_attr_mapping = master_attribute_source_mapping or []
    
    all_attr_mappings = [master_attr_mapping]
    
    for match_attr_mapping in match_attribute_source_mappings:
        if isinstance(match_attr_mapping, list) and len(match_attr_mapping) > 0 and hasattr(match_attr_mapping[0], 'asDict'):
            converted_mapping = [attr.asDict() for attr in match_attr_mapping]
        else:
            converted_mapping = match_attr_mapping or []
        all_attr_mappings.append(converted_mapping)
    
    resultant_record = {}
    resultant_master_attribute_source_mapping = []
    
    resultant_record[id_key] = master_dict.get(id_key)
    
    for attribute in entity_attributes:
        if attribute == id_key:
            continue
        
        all_attr_infos = []
        for record, attr_mapping in zip(all_records, all_attr_mappings):
            attr_info = get_attribute_source_info(attr_mapping, attribute, record)
            all_attr_infos.append(attr_info)
        
        # Check for MANUAL_UPDATE priority
        manual_update_info = None
        for attr_info in all_attr_infos:
            if attr_info.get('source') == 'MANUAL_UPDATE':
                manual_update_info = attr_info
                break
        
        result_value = None
        result_source = None
        
        if manual_update_info:
            result_value = manual_update_info.get('value')
            result_source = 'MANUAL_UPDATE'
        else:
            rule = next((item for item in default_survivorship_rules 
                        if item.get('attribute') == attribute or item.get('entity_attribute') == attribute), None)
            
            ignore_null = False
            
            if rule:
                # Extract toggle properties
                ignore_null = rule.get('ignoreNull', False)
                
                if rule['strategy'] == 'Source System':
                    strategyRule = rule['strategyRule']
                    result_value, result_source = apply_source_system_strategy_multi_merge(
                        attribute, strategyRule, all_attr_infos, ignore_null
                    )
                elif rule['strategy'] == 'Aggregation':
                    aggregate_unique_only = rule.get('aggregateUniqueOnly', False)
                    result_value, result_source = apply_aggregation_strategy_multi_merge(
                        attribute, all_attr_infos, entity_attributes_datatype, ignore_null, aggregate_unique_only
                    )
                elif rule['strategy'] == 'Recency':
                    strategyRule = rule.get('strategyRule', [])
                    result_value, result_source = apply_recency_strategy_multi_merge(
                        attribute, strategyRule, all_attr_infos, all_records, ignore_null
                    )
            else:
                # Default: use master record value
                master_value = all_attr_infos[0].get('value')
                master_source = all_attr_infos[0].get('source')
                
                if ignore_null and (master_value is None or master_value == ""):
                    result_value = None
                    result_source = None
                else:
                    result_value = master_value
                    result_source = master_source
        
        resultant_record[attribute] = result_value
        
        resultant_master_attribute_source_mapping.append({
            'attribute_name': attribute,
            'attribute_value': str(result_value) if result_value is not None else '',
            'source': result_source or ''
        })
    
    return {
        'resultant_record': resultant_record,
        'resultant_master_attribute_source_mapping': resultant_master_attribute_source_mapping
    }

# COMMAND ----------

resultant_record_schema = mergable_records_df_grouped.select("master_record").schema["master_record"].dataType

attribute_source_mapping_schema = mergable_records_df_grouped.select("master_attribute_source_mapping").schema["master_attribute_source_mapping"].dataType

udf_output_schema = StructType([
    StructField("resultant_record", resultant_record_schema, True),
    StructField("resultant_master_attribute_source_mapping", attribute_source_mapping_schema, True)
])

# COMMAND ----------

process_survivorship_udf = udf(process_multi_master_merge_survivorship, udf_output_schema)

# COMMAND ----------

mergable_records_df_grouped_survivorship = (
    mergable_records_df_grouped
    .withColumn(
        'survivorship_result', 
        process_survivorship_udf(
            col('master_record'),
            col('match_records'),
            col('master_attribute_source_mapping'),
            col('match_attribute_source_mappings')
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
remaining_cols = [
    col_name for col_name in current_master_table.columns if col_name not in mergable_records_survivorship.columns
]

joined_df = mergable_records_survivorship.join(
    current_master_table, on=id_key, how='inner'
)

resultant_merged_df = joined_df.select(
    [mergable_records_survivorship[col] for col in mergable_records_survivorship.columns] +
    [current_master_table[col] for col in remaining_cols]
)


# COMMAND ----------

delta_table = DeltaTable.forName(spark, master_table)
delta_table.alias("target").merge(
    source=resultant_merged_df.alias("source"),
    condition=f"target.{id_key} = source.{id_key}"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

master_version = get_current_table_version(master_table)

# COMMAND ----------

resultant_master_attribute_source_mapping_survivorship = (
    mergable_records_df_grouped_survivorship
    .select(
        col(f'resultant_record.{id_key}').alias(id_key),
        lit(master_version).alias('version'),
        col('resultant_master_attribute_source_mapping').alias('attribute_source_mapping')
    )
)

# COMMAND ----------

delta_table_attr = DeltaTable.forName(spark, master_attribute_version_sources_table)
delta_table_attr.alias("target").merge(
    source=resultant_master_attribute_source_mapping_survivorship.alias("source"),
    condition=f"target.{id_key} = source.{id_key} and target.version = source.version"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

resultant_merge_activities_survivorship = (
    mergable_records_df_grouped_survivorship
    .select(
        col(f'master_{id_key}'),
        explode(col('match_records')).alias('match_record')
    )
    .select(
        col(f'master_{id_key}'),
        col(f'match_record.{id_key}').alias(id_key),
        lit(primary_table).alias('source'),
        lit(master_version).alias('version'),
        lit('JOB_MERGE').alias('action_type')
    )
)

# COMMAND ----------

delta_table_merge = DeltaTable.forName(spark, merge_activities_table)
delta_table_merge.alias("target").merge(
    source=resultant_merge_activities_survivorship.alias("source"),
    condition=f"target.{id_key} = source.{id_key} and target.version = source.version and target.master_{id_key} = source.master_{id_key} and target.action_type = source.action_type"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

match_ids_to_delete_df = spark.sql(f"""
    SELECT DISTINCT {id_key} as match_id
    FROM {merge_activities_table}
    WHERE version = {master_version}
    AND action_type = 'JOB_MERGE'
    AND {id_key} IS NOT NULL
""")

logger.info(f"Found {match_ids_to_delete_df.count()} match IDs to delete")

# COMMAND ----------

delta_table_master = DeltaTable.forName(spark, master_table)

delta_table_master.alias("target").merge(
    source=match_ids_to_delete_df.alias("source"),
    condition=f"target.{id_key} = source.match_id"
).whenMatchedDelete().execute()

logger.info("Deleted merged records from master table")

# COMMAND ----------

delta_table_processed_unified = DeltaTable.forName(spark, processed_unified_table)

delta_table_processed_unified.alias("target").merge(
    source=match_ids_to_delete_df.alias("source"),
    condition=f"target.master_{id_key} = source.match_id OR target.{id_key} = source.match_id"
).whenMatchedDelete().execute()

logger.info("Deleted processed unified records for merged IDs")
