# Databricks notebook source
from uuid import uuid4
import json
from delta.tables import DeltaTable
from pyspark.sql.functions import (
    col, explode, lit, collect_list, first, struct, count, 
    arrays_zip, least, greatest, udf, expr
)
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
dbutils.widgets.text("entity_attributes_datatype", "", "Entity Attributes Datatype")

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
entity_attributes_datatype = dbutils.widgets.get("entity_attributes_datatype")

# COMMAND ----------

entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
id_key = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="id_key", debugValue=id_key)
dataset_objects = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="dataset_objects", debugValue=dataset_objects)
default_survivorship_rules = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="default_survivorship_rules", debugValue=default_survivorship_rules)
entity_attributes = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes", debugValue=entity_attributes)
config_thresold = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "config_thresold", debugValue=config_thresold)
primary_table = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="primary_table", debugValue=primary_table)
entity_attributes_datatype = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", 
    "entity_attributes_datatype", 
    debugValue=entity_attributes_datatype
)

# COMMAND ----------

dataset_objects = json.loads(dataset_objects)
default_survivorship_rules = json.loads(default_survivorship_rules)
entity_attributes = json.loads(entity_attributes)
config_thresold = json.loads(config_thresold)
entity_attributes_datatype = json.loads(entity_attributes_datatype)

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

for rule in default_survivorship_rules:
    if rule.get('strategy') == 'Source System' and 'strategyRule' in rule:
        # Replace the strategyRule with dataset paths
        rule['strategyRule'] = [dataset_id_to_path.get(dataset_id) for dataset_id in rule['strategyRule']]

# COMMAND ----------

query = f"""
WITH RankedMasterAttributes AS (
    SELECT {id_key}, version, attribute_source_mapping,
        ROW_NUMBER() OVER (PARTITION BY {id_key} ORDER BY version DESC) AS rn
    FROM {master_attribute_version_sources_table}
    WHERE {id_key} IN ('{master_id}', '{match_record_id}')
),
LatestMasterAttributes AS (
    SELECT {id_key}, attribute_source_mapping
    FROM RankedMasterAttributes
    WHERE rn = 1
)
SELECT
    m.*,
    lma.attribute_source_mapping
FROM {master_table} m
INNER JOIN LatestMasterAttributes lma ON m.{id_key} = lma.{id_key}
WHERE m.{id_key} IN ('{master_id}', '{match_record_id}')
"""

# COMMAND ----------

master_records_df = spark.sql(query)

# Separate master and match records
master_record_df = master_records_df.filter(col(id_key) == master_id)
match_record_df = master_records_df.filter(col(id_key) == match_record_id)

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

def apply_source_system_strategy_master_merge(attribute, strategyRule, master_attr_info, match_attr_info, ignore_null=False):
    """
    Apply source system strategy for master merge
    Compare sources based on survivorship rule priority
    """
    master_source = master_attr_info.get('source')
    match_source = match_attr_info.get('source')
    master_value = master_attr_info.get('value')
    match_value = match_attr_info.get('value')
    
    # Apply ignore_null logic
    if ignore_null:
        if master_value is None or master_value == "":
            master_value = None
        if match_value is None or match_value == "":
            match_value = None
    
    # If both values are null after ignore_null filtering, return None
    if ignore_null and master_value is None and match_value is None:
        return None, None
    
    # If only one value is valid after ignore_null filtering
    if ignore_null:
        if master_value is None and match_value is not None:
            return match_value, match_source
        if match_value is None and master_value is not None:
            return master_value, master_source
    
    if master_source == match_source:
        return master_value, master_source
    
    master_priority = float('inf')
    match_priority = float('inf')
    
    if master_source in strategyRule:
        master_priority = strategyRule.index(master_source)
    if match_source in strategyRule:
        match_priority = strategyRule.index(match_source)
    
    if master_priority <= match_priority:
        return master_value, master_source
    else:
        return match_value, match_source

# COMMAND ----------

def apply_aggregation_strategy_master_merge(attribute, master_attr_info, match_attr_info, entity_attributes_datatype, ignore_null=False, aggregate_unique_only=False):
    """Apply aggregation strategy for master merge with datatype awareness"""
    attr_datatype = entity_attributes_datatype.get(attribute, 'string')
    
    master_value = master_attr_info.get('value')
    match_value = match_attr_info.get('value')
    master_source = master_attr_info.get('source')
    match_source = match_attr_info.get('source')
    
    # Apply ignore_null logic
    if ignore_null:
        if master_value is None or master_value == "":
            master_value = None
        if match_value is None or match_value == "":
            match_value = None
    
    # Handle case where both values are None or empty
    if master_value is None and match_value is None:
        return None, None
    
    # Handle case where one value is None or empty
    if master_value is None:
        return match_value, match_source
    if match_value is None:
        return master_value, master_source
    
    all_values = [master_value, match_value]
    
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
                result_value = all_values[0]
        except:
            result_value = all_values[0]
    
    elif is_string_type(attr_datatype):
        # For string types, concatenate with separator
        master_str = str(master_value) if master_value is not None else ''
        match_str = str(match_value) if match_value is not None else ''
        
        all_string_values = []
        if master_str:
            all_string_values.extend(master_str.split(aggregation_strategy_separator))
        if match_str:
            all_string_values.extend(match_str.split(aggregation_strategy_separator))
        
        # Clean values
        clean_values = [v.strip() for v in all_string_values if v.strip()]
        
        # Apply unique logic
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
    sources = [src for src in [master_source, match_source] if src]
    result_source = ','.join(list(dict.fromkeys(sources))) if sources else None
    
    return result_value, result_source

# COMMAND ----------

def apply_recency_strategy_master_merge(attribute, strategyRule, master_attr_info, match_attr_info, master_record, match_record, ignore_null=False):
    """
    Apply recency strategy for master merge using simplified structure
    strategyRule is now just the attribute name (string) to use for recency comparison
    """
    def get_master_fallback():
        master_value = master_attr_info.get('value')
        master_source = master_attr_info.get('source')
        
        # Apply ignore_null to fallback
        if ignore_null and (master_value is None or master_value == ""):
            return None, None
        
        return master_value, master_source
    
    if not strategyRule or strategyRule == "__none__":
        logger.info(f"No recency attribute specified for attribute '{attribute}', falling back to master")
        return get_master_fallback()
    
    date_attr = strategyRule
    
    master_dict = master_record.asDict() if hasattr(master_record, 'asDict') else master_record
    match_dict = match_record.asDict() if hasattr(match_record, 'asDict') else match_record
    
    # Check if the date attribute exists in both records
    if date_attr not in master_dict and date_attr not in match_dict:
        logger.info(f"Date attribute '{date_attr}' not found in either record, falling back to master")
        return get_master_fallback()
    
    master_date_value = master_dict.get(date_attr)
    match_date_value = match_dict.get(date_attr)
    
    # If both date values are None, fall back to master
    if master_date_value is None and match_date_value is None:
        logger.info(f"Both date values are None for attribute '{date_attr}', falling back to master")
        return get_master_fallback()
    
    try:
        import pandas as pd
        
        master_parsed_date = None
        match_parsed_date = None
        
        # Parse master date
        if master_date_value is not None:
            if isinstance(master_date_value, str):
                master_parsed_date = pd.to_datetime(master_date_value)
            else:
                master_parsed_date = master_date_value
        
        # Parse match date
        if match_date_value is not None:
            if isinstance(match_date_value, str):
                match_parsed_date = pd.to_datetime(match_date_value)
            else:
                match_parsed_date = match_date_value
        
        # Get values and apply ignore_null if needed
        master_value = master_attr_info.get('value')
        match_value = match_attr_info.get('value')
        
        if ignore_null:
            if master_value is None or master_value == "":
                master_value = None
            if match_value is None or match_value == "":
                match_value = None
        
        # Compare dates and return the value from the record with the more recent date
        if master_parsed_date is not None and match_parsed_date is not None:
            if match_parsed_date > master_parsed_date:
                logger.info(f"Match record has more recent date for '{date_attr}': {match_parsed_date} > {master_parsed_date}")
                return match_value, match_attr_info.get('source')
            else:
                logger.info(f"Master record has more recent or equal date for '{date_attr}': {master_parsed_date} >= {match_parsed_date}")
                return master_value, master_attr_info.get('source')
        elif match_parsed_date is not None:
            # Only match has a valid date
            logger.info(f"Only match record has valid date for '{date_attr}': {match_parsed_date}")
            return match_value, match_attr_info.get('source')
        elif master_parsed_date is not None:
            # Only master has a valid date
            logger.info(f"Only master record has valid date for '{date_attr}': {master_parsed_date}")
            return master_value, master_attr_info.get('source')
        else:
            # Neither has a valid date
            logger.info(f"Neither record has valid date for '{date_attr}', falling back to master")
            return get_master_fallback()
            
    except Exception as e:
        logger.error(f"Error parsing dates for recency strategy on attribute '{attribute}' with date attribute '{date_attr}': {e}")
        logger.error(f"Master date value: {master_date_value}, Match date value: {match_date_value}")
        logger.info("Falling back to master")
        return get_master_fallback()

# COMMAND ----------

def process_master_merge_survivorship(master_record, match_record, master_attr_mapping, match_attr_mapping):
    """
    Process survivorship rules for master-to-master merge
    """
    master_record = master_record.asDict()
    match_record = match_record.asDict()
    master_attr_mapping = [attr.asDict() for attr in master_attr_mapping] if master_attr_mapping else []
    match_attr_mapping = [attr.asDict() for attr in match_attr_mapping] if match_attr_mapping else []
    
    resultant_record = {}
    resultant_master_attribute_source_mapping = []
    
    resultant_record[id_key] = master_record.get(id_key)
    
    for attribute in entity_attributes:
        if attribute == id_key:
            continue
        
        master_attr_info = get_attribute_source_info(master_attr_mapping, attribute, master_record)
        match_attr_info = get_attribute_source_info(match_attr_mapping, attribute, match_record)
        
        master_source = master_attr_info.get('source')
        match_source = match_attr_info.get('source')
        
        result_value = None
        result_source = None
        
        # Case 1: One attribute source value is MANUAL_UPDATE
        if master_source == 'MANUAL_UPDATE' and match_source != 'MANUAL_UPDATE':
            result_value = master_attr_info.get('value')
            result_source = 'MANUAL_UPDATE'
        elif match_source == 'MANUAL_UPDATE' and master_source != 'MANUAL_UPDATE':
            result_value = match_attr_info.get('value')
            result_source = 'MANUAL_UPDATE'
        # Case 2: Both attributes source value is MANUAL_UPDATE
        elif master_source == 'MANUAL_UPDATE' and match_source == 'MANUAL_UPDATE':
            result_value = master_attr_info.get('value')
            result_source = 'MANUAL_UPDATE'
        # Case 3: Neither source is MANUAL_UPDATE
        else:
            rule = next((item for item in default_survivorship_rules 
                        if item.get('attribute') == attribute or item.get('entity_attribute') == attribute), None)
            
            if rule:
                # Extract toggle properties
                ignore_null = rule.get('ignoreNull', False)
                
                if rule['strategy'] == 'Source System':
                    # Apply source system strategy
                    strategyRule = rule['strategyRule']
                    result_value, result_source = apply_source_system_strategy_master_merge(
                        attribute, strategyRule, master_attr_info, match_attr_info, ignore_null
                    )
                elif rule['strategy'] == 'Aggregation':
                    # Apply aggregation strategy
                    aggregate_unique_only = rule.get('aggregateUniqueOnly', False)
                    result_value, result_source = apply_aggregation_strategy_master_merge(
                        attribute, master_attr_info, match_attr_info, entity_attributes_datatype, ignore_null, aggregate_unique_only
                    )
                elif rule['strategy'] == 'Recency':
                    # Apply recency strategy
                    strategyRule = rule.get('strategyRule', [])
                    result_value, result_source = apply_recency_strategy_master_merge(
                        attribute, strategyRule, master_attr_info, match_attr_info, 
                        master_record, match_record, ignore_null
                    )
            else:
                # No rule found, fall back to master
                result_value = master_attr_info.get('value')
                result_source = master_attr_info.get('source')
        
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

# Check if both master and match records exist before proceeding
master_record_count = master_record_df.count()
match_record_count = match_record_df.count()

logger.info(f"Master record count: {master_record_count}")
logger.info(f"Match record count: {match_record_count}")

# COMMAND ----------

if master_record_count == 0:
    logger.warning(f"Master record with ID '{master_id}' not found in master table")
    dbutils.notebook.exit("Master record not found")

if match_record_count == 0:
    logger.warning(f"Match record with ID '{match_record_id}' not found in master table")
    dbutils.notebook.exit("Match record not found")

# COMMAND ----------

master_data = master_record_df.collect()[0]
match_data = match_record_df.collect()[0]

# COMMAND ----------

attribute_source_mapping_schema = StructType([
    StructField("attribute_name", StringType(), True),
    StructField("attribute_value", StringType(), True),
    StructField("source", StringType(), True)
])

# COMMAND ----------

master_merge_grouped_schema = StructType([
    StructField(f"master_{id_key}", StringType(), True),
    StructField("master_record", master_record_df.schema, True),
    StructField("match_record", match_record_df.schema, True),
    StructField("master_attribute_source_mapping", ArrayType(attribute_source_mapping_schema), True),
    StructField("match_attribute_source_mapping", ArrayType(attribute_source_mapping_schema), True)
])

# COMMAND ----------

master_merge_grouped_data = [{
    f'master_{id_key}': master_id,
    'master_record': master_data,
    'match_record': match_data,
    'master_attribute_source_mapping': master_data['attribute_source_mapping'],
    'match_attribute_source_mapping': match_data['attribute_source_mapping']
}]

master_merge_grouped_df = spark.createDataFrame(master_merge_grouped_data,schema=master_merge_grouped_schema)

# COMMAND ----------

full_master_record_schema = master_merge_grouped_df.select("master_record").schema["master_record"].dataType

# Get the attributes that use aggregation strategy
aggregation_attributes = set()
for rule in default_survivorship_rules:
    if rule.get('strategy') == 'Aggregation':
        attr_name = rule.get('attribute') or rule.get('entity_attribute')
        if attr_name:
            aggregation_attributes.add(attr_name)

entity_attribute_fields = []
for field in full_master_record_schema.fields:
    if field.name == id_key:
        entity_attribute_fields.append(StructField(field.name, StringType(), field.nullable))
    elif field.name in entity_attributes:
        entity_attribute_fields.append(field)

resultant_record_schema = StructType(entity_attribute_fields)

attribute_source_mapping_schema = master_merge_grouped_df.select("master_attribute_source_mapping").schema["master_attribute_source_mapping"].dataType

udf_output_schema = StructType([
    StructField("resultant_record", resultant_record_schema, True),
    StructField("resultant_master_attribute_source_mapping", attribute_source_mapping_schema, True)
])


# COMMAND ----------

process_master_merge_survivorship_udf_registered = udf(process_master_merge_survivorship, udf_output_schema)

# COMMAND ----------

master_merge_grouped_survivorship = (
    master_merge_grouped_df
    .withColumn(
        'survivorship_result', 
        process_master_merge_survivorship_udf_registered(
            col('master_record'),
            col('match_record'),
            col('master_attribute_source_mapping'),
            col('match_attribute_source_mapping')
        )
    )
    .withColumn('resultant_record', col('survivorship_result.resultant_record'))
    .withColumn('resultant_master_attribute_source_mapping', col('survivorship_result.resultant_master_attribute_source_mapping'))
)

# COMMAND ----------

mergable_records_survivorship_one_col = (
    master_merge_grouped_survivorship
    .select('resultant_record')
)

nested_fields = mergable_records_survivorship_one_col.schema["resultant_record"].dataType.fields
columns_to_select = [col(f"resultant_record.{field.name}").alias(field.name) for field in nested_fields]
mergable_records_survivorship = mergable_records_survivorship_one_col.select(*columns_to_select)

# COMMAND ----------

current_master_table = spark.read.table(master_table)

remaining_cols = [
    col_name for col_name in current_master_table.columns 
    if col_name not in mergable_records_survivorship.columns
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
    master_merge_grouped_survivorship
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

merge_activities_df = spark.createDataFrame([{
    f'master_{id_key}': master_id,
    id_key: match_record_id,
    'source': primary_table,
    'version': master_version,
    'action_type': 'MANUAL_MERGE'
}])

# COMMAND ----------

# Update merge activities table
delta_table_merge = DeltaTable.forName(spark, merge_activities_table)

delta_table_merge.alias("target").merge(
    source=merge_activities_df.alias("source"),
    condition=f"target.{id_key} = source.{id_key} and target.version = source.version and target.master_{id_key} = source.master_{id_key} and target.action_type = source.action_type"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

delta_table_master = DeltaTable.forName(spark, master_table)
delta_table_master.delete(condition=expr(f"{id_key} = '{match_record_id}'"))

# COMMAND ----------

delta_table_processed_unified = DeltaTable.forName(spark, processed_unified_table)
processed_unified_condition = f"""
    master_{id_key} = '{match_record_id}' 
    OR ({id_key} = '{match_record_id}' AND master_{id_key} != '{master_id}')
"""

delta_table_processed_unified.delete(condition=expr(processed_unified_condition))

# COMMAND ----------

# MAGIC %run "/Lakefusion_Notebooks/match-maven-notebooks/src/maven-core-deduplication/Process Potential Match Table"

# COMMAND ----------

logger.info("Successfully Completed Merge")
