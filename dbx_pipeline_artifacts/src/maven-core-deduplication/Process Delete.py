# Databricks notebook source
from uuid import uuid4
import json
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from functools import reduce

# COMMAND ----------

dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("primary_table", "", "Primary Table")
dbutils.widgets.text("primary_key", "", "Source Primary Key")
dbutils.widgets.text("id_key", "", "Master Primary Key (lakefusion_id)")
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")
dbutils.widgets.text("attributes_mapping", "", "Attributes Mapping")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("meta_info_table", "", "Meta Info Table")
dbutils.widgets.text("dataset_objects", "", "Dataset Objects")
dbutils.widgets.text("default_survivorship_rules", "", "Default Survivorship Rules")

# COMMAND ----------

entity = dbutils.widgets.get("entity")
primary_table = dbutils.widgets.get("primary_table")
source_id_key = dbutils.widgets.get("primary_key")
id_key = dbutils.widgets.get("id_key")
entity_attributes = dbutils.widgets.get("entity_attributes")
attributes_mapping = dbutils.widgets.get("attributes_mapping")
experiment_id = dbutils.widgets.get("experiment_id")
catalog_name = dbutils.widgets.get("catalog_name")
meta_info_table = dbutils.widgets.get("meta_info_table")
dataset_objects = dbutils.widgets.get("dataset_objects")
default_survivorship_rules = dbutils.widgets.get("default_survivorship_rules")

# COMMAND ----------

# Get values from task values
entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
primary_table = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "primary_table", debugValue=primary_table)
source_id_key = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "primary_key", debugValue=source_id_key)
id_key = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "id_key", debugValue=id_key)
entity_attributes = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes", debugValue=entity_attributes)
attributes_mapping = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "attributes_mapping", debugValue=attributes_mapping)
meta_info_table = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "meta_info_table", debugValue=meta_info_table)
dataset_objects = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "dataset_objects", debugValue=dataset_objects)
default_survivorship_rules = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "default_survivorship_rules", debugValue=default_survivorship_rules)

# COMMAND ----------

# Parse JSON parameters
entity_attributes = json.loads(entity_attributes)
entity_attributes.append("lakefusion_id")
attributes_mapping = json.loads(attributes_mapping)
dataset_objects = json.loads(dataset_objects)
default_survivorship_rules = json.loads(default_survivorship_rules)

# Get primary table mapping
primary_table_mapping = None
for mapping in attributes_mapping:
    if primary_table in mapping:
        primary_table_mapping = mapping[primary_table]
        break

if not primary_table_mapping:
    raise Exception(f"No attribute mapping found for primary table: {primary_table}")

# Get source column name for source_id_key
source_column_for_id = None
for source_col, entity_col in primary_table_mapping.items():
    if entity_col == source_id_key:
        source_column_for_id = source_col
        break

if not source_column_for_id:
    raise Exception(f"No source column mapping found for source_id_key: {source_id_key}")

# Create dataset mapping
dataset_id_to_path = {dataset['id']: dataset['path'] for dataset in dataset_objects.values()}

# Update survivorship rules with dataset paths
for rule in default_survivorship_rules:
    if rule.get('strategy') == 'Source System' and 'strategyRule' in rule:
        rule['strategyRule'] = [dataset_id_to_path.get(dataset_id) for dataset_id in rule['strategyRule']]

logger.info(f"Source column for ID key: {source_column_for_id}")

# COMMAND ----------

# MAGIC %run ../utils/match_merge_utils

# COMMAND ----------

import logging
if 'logger' not in dir():
    logger = logging.getLogger(__name__)

# COMMAND ----------

# Define table names
unified_table = f"{catalog_name}.silver.{entity}_unified"
master_table = f"{catalog_name}.gold.{entity}_master"
if experiment_id:
    unified_table += f"_{experiment_id}"
    master_table += f"_{experiment_id}"

merge_activities_table = f"{master_table}_merge_activities"
master_attribute_version_sources_table = f"{master_table}_attribute_version_sources"

aggregation_strategy_separator = ' • '

logger.info(f"Master table: {master_table}")
logger.info(f"Merge activities table: {merge_activities_table}")
logger.info(f"Attribute version sources table: {master_attribute_version_sources_table}")

# COMMAND ----------

# Get last processed version for primary table
def get_last_processed_version(table_path, entity_name):
    try:
        result = spark.sql(f"""
            SELECT last_processed_version 
            FROM {meta_info_table} 
            WHERE table_name = '{table_path}' AND entity_name = '{entity_name}'
        """).collect()
        
        if result:
            return result[0]['last_processed_version']
        else:
            raise Exception(f"No version tracking found for table: {table_path}")
    except Exception as e:
        logger.error(f"Error getting last processed version: {str(e)}")
        raise e

last_processed_version = get_last_processed_version(primary_table, entity)
logger.info(f"Last processed version for {primary_table}: {last_processed_version}")

# COMMAND ----------

# Get current version of primary table
def get_current_table_version(table_name):
    try:
        return spark.sql(f"SELECT MAX(version) as version FROM (DESCRIBE HISTORY {table_name})").collect()[0]['version']
    except Exception as e:
        logger.error(f"Error getting current version for {table_name}: {str(e)}")
        return 0

current_version = get_current_table_version(primary_table)
logger.info(f"Current version for {primary_table}: {current_version}")

# COMMAND ----------

# Check if there are new versions to process
if current_version <= last_processed_version:
    logger.info(f"No new versions to process. Last processed: {last_processed_version}, Current: {current_version}")
    dbutils.notebook.exit("No new versions to process")

# Calculate the starting version (next version after last processed)
starting_version = last_processed_version + 1
logger.info(f"Processing versions from {starting_version} to {current_version}")

# COMMAND ----------

# Get CDF changes (deletes only) from primary table
def get_cdf_deletes(table_name, from_version, to_version):
    try:
        cdf_query = f"""
        SELECT *, _change_type, _commit_version, _commit_timestamp
        FROM table_changes('{table_name}', {from_version}, {to_version})
        WHERE _change_type = 'delete'
        ORDER BY {source_column_for_id}, _commit_version
        """
        return spark.sql(cdf_query)
    except Exception as e:
        logger.error(f"Error getting CDF deletes: {str(e)}")
        return spark.createDataFrame([], StructType([]))

cdf_deletes = get_cdf_deletes(primary_table, starting_version, current_version)
logger.info(f"CDF deletes count: {cdf_deletes.count()}")

if cdf_deletes.count() == 0:
    logger.info("No deletes found in primary table since last processing")
    dbutils.notebook.exit("No deletes to process")

# COMMAND ----------

cdf_deletes.display()

# COMMAND ----------

# Extract deleted source IDs
deleted_source_ids = [row[source_column_for_id] for row in cdf_deletes.collect()]
logger.info(f"Deleted source IDs: {deleted_source_ids}")

def format_sql_value(value):
    """Format a value for SQL IN clause based on its type"""
    if value is None:
        return "NULL"
    elif isinstance(value, str):
        escaped_value = str(value).replace("'", "''")
        return f"'{escaped_value}'"
    elif isinstance(value, (int, float)):
        return str(value)
    else:
        escaped_value = str(value).replace("'", "''")
        return f"'{escaped_value}'"

# Handle empty list case
if not deleted_source_ids:
    logger.info("No deleted source IDs found")
    dbutils.notebook.exit("No source IDs to process for deletion")

# Format deleted source IDs using the same function
formatted_deleted_source_ids = [format_sql_value(sid) for sid in deleted_source_ids]
deleted_source_ids_str = ",".join(formatted_deleted_source_ids)

logger.info(f"Formatted deleted source IDs string: {deleted_source_ids_str}")

# Find corresponding master records
master_records_query = f"""
SELECT * 
FROM {master_table}
WHERE {source_id_key} IN ({deleted_source_ids_str})
"""

master_records_to_delete = spark.sql(master_records_query)
logger.info(f"Master records to process for deletion: {master_records_to_delete.count()}")

# COMMAND ----------

if master_records_to_delete.count() == 0:
    logger.info("No corresponding master records found for deleted source records")
    dbutils.notebook.exit("No master records to process")

# COMMAND ----------

# Check which master records have merges
def check_merge_status(master_records):
    """
    Check which master records have merges (JOB_MERGE or MANUAL_MERGE)
    """
    lakefusion_ids = [row[id_key] for row in master_records.collect()]
    lakefusion_ids_str = "','".join(lakefusion_ids)
    
    merge_check_query = f"""
    WITH MergeActivitiesCast AS (
        SELECT *, 
               COALESCE(CAST(master_{id_key} AS STRING), '') AS master_id_str,
               COALESCE(CAST({id_key} AS STRING), '') AS id_str
        FROM {merge_activities_table}
    ),
    ValidMerges AS (
        SELECT DISTINCT master_id_str
        FROM MergeActivitiesCast
        WHERE master_id_str IN ('{lakefusion_ids_str}')
        AND action_type IN ('{ActionType.JOB_MERGE.value}', '{ActionType.MANUAL_MERGE.value}')
        AND NOT EXISTS (
            SELECT 1
            FROM MergeActivitiesCast unmerge
            WHERE unmerge.master_id_str = MergeActivitiesCast.master_id_str
            AND unmerge.id_str = MergeActivitiesCast.id_str
            AND unmerge.action_type = '{ActionType.MANUAL_UNMERGE.value}'
            AND unmerge.version > MergeActivitiesCast.version
        )
    )
    SELECT master_id_str as lakefusion_id
    FROM ValidMerges
    """
    
    merged_records_df = spark.sql(merge_check_query)
    merged_lakefusion_ids = [row['lakefusion_id'] for row in merged_records_df.collect()]
    
    return merged_lakefusion_ids

merged_lakefusion_ids = check_merge_status(master_records_to_delete)
logger.info(f"Master records with merges: {len(merged_lakefusion_ids)}")
logger.info(f"Merged lakefusion IDs: {merged_lakefusion_ids}")

# Separate records with and without merges
standalone_records = master_records_to_delete.filter(~col(id_key).isin(merged_lakefusion_ids))
merged_records = master_records_to_delete.filter(col(id_key).isin(merged_lakefusion_ids))

logger.info(f"Standalone records to delete: {standalone_records.count()}")
logger.info(f"Merged records to process: {merged_records.count()}")

# COMMAND ----------

# Process standalone records (simple deletion)
if standalone_records.count() > 0:
    logger.info("Processing standalone record deletions...")
    
    # Delete from master table
    standalone_ids = [row[id_key] for row in standalone_records.collect()]
    standalone_ids_str = "','".join(standalone_ids)
    
    delete_master_sql = f"""
    DELETE FROM {master_table}
    WHERE {id_key} IN ('{standalone_ids_str}')
    """
    
    spark.sql(delete_master_sql)
    logger.info(f"Deleted {len(standalone_ids)} standalone records from master table")
    
    # Get updated master version
    master_version = get_current_table_version(master_table)
    
    # Log deletions in merge activities
    delete_activities = []
    for lakefusion_id in standalone_ids:
        delete_activities.append(f"('{lakefusion_id}', '', '{primary_table}', {master_version}, '{ActionType.SOURCE_DELETE.value}')")
    
    delete_activities_sql = f"""
    INSERT INTO {merge_activities_table}
    (`master_{id_key}`, `{id_key}`, source, version, action_type)
    VALUES {', '.join(delete_activities)}
    """
    
    spark.sql(delete_activities_sql)
    logger.info("Logged standalone deletions in merge activities")

# COMMAND ----------

# Process merged records (survivorship recalculation)
if merged_records.count() > 0:
    logger.info("Processing merged record deletions with survivorship recalculation...")
    
    # For each merged record, get all merged records and recalculate survivorship
    merged_records_to_process = []
    
    for master_row in merged_records.collect():
        lakefusion_id = master_row[id_key]
        
        # Get all merged records for this master record
        merged_query = f"""
        WITH MergeActivitiesCast AS (
            SELECT *, 
                   COALESCE(CAST(master_{id_key} AS STRING), '') AS master_id_str,
                   COALESCE(CAST({id_key} AS STRING), '') AS id_str
            FROM {merge_activities_table}
        ),
        ValidMerges AS (
            SELECT *
            FROM MergeActivitiesCast
            WHERE master_id_str = '{lakefusion_id}'
            AND action_type IN ('{ActionType.JOB_MERGE.value}', '{ActionType.MANUAL_MERGE.value}')
            AND NOT EXISTS (
                SELECT 1
                FROM MergeActivitiesCast unmerge
                WHERE unmerge.master_id_str = '{lakefusion_id}'
                AND unmerge.id_str = MergeActivitiesCast.id_str
                AND unmerge.action_type = '{ActionType.MANUAL_UNMERGE.value}'
                AND unmerge.version > MergeActivitiesCast.version
            )
        )
        SELECT id_str as merged_id, source
        FROM ValidMerges
        WHERE id_str != ''
        """
        
        merged_details = spark.sql(merged_query).collect()
        
        # Separate primary table records from other sources
        primary_merged_records = []
        other_source_records = []
        
        for merge_detail in merged_details:
            if merge_detail['source'] == primary_table:
                primary_merged_records.append(merge_detail['merged_id'])
            else:
                other_source_records.append({
                    'merged_id': merge_detail['merged_id'],
                    'source': merge_detail['source']
                })
        
        merged_records_to_process.append({
            'lakefusion_id': lakefusion_id,
            'primary_merged_records': primary_merged_records,
            'other_source_records': other_source_records
        })
    
    logger.info(f"Merged records analysis: {len(merged_records_to_process)} master records to process")

# COMMAND ----------

# Helper functions for survivorship with toggle support
def apply_source_system_strategy(attribute, strategyRule, unified_records, master_record, unified_datasets, initial_values, initial_sources, ignore_null=False):
    result_value = initial_values.get(attribute)
    result_source = initial_sources.get(attribute)
    
    # Apply ignore_null to initial value
    if ignore_null and (result_value is None or result_value == ""):
        result_value = None
        result_source = None

    for priority_dataset in strategyRule:
        try:
            unified_dataset_index = unified_datasets.index(priority_dataset)
            unified_record_column_value = unified_records[unified_dataset_index].get(attribute)
            
            # Apply ignore_null logic
            if ignore_null and (unified_record_column_value is None or unified_record_column_value == ""):
                continue
                
            if unified_record_column_value is not None:
                result_value = unified_record_column_value
                result_source = priority_dataset
                return result_value, result_source
        except (ValueError, IndexError):
            pass
    
    for idx, record in enumerate(unified_records):
        if idx < len(unified_datasets):
            record_value = record.get(attribute)
            if record_value is not None:
                if not ignore_null or record_value != "":
                    result_value = record_value
                    result_source = unified_datasets[idx]
                    return result_value, result_source
    
    return result_value, result_source

def apply_aggregation_strategy(attribute, master_record, unified_records, initial_value=None, ignore_null=False, aggregate_unique_only=False):
    # Get unified values
    unified_values = []
    for record in unified_records:
        value = record.get(attribute)
        if value is not None:
            if not ignore_null or value != "":
                unified_values.append(value)
    
    all_values = unified_values[:]
    
    # Handle initial value based on ignore_null
    if initial_value is not None:
        if not ignore_null or initial_value != "":
            all_values.append(initial_value)
    
    # Flatten aggregated values (split by separator)
    flat_values = []
    for value in all_values:
        parts = str(value).split(aggregation_strategy_separator)
        flat_values.extend(parts)
    
    # Clean and filter values
    clean_values = [v.strip() for v in flat_values if v.strip()]
    
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
        # Original logic: use set for deduplication
        clean_values = list(set(clean_values))
    
    return aggregation_strategy_separator.join(clean_values) if clean_values else None

def apply_recency_strategy(attribute, unified_records, unified_datasets, initial_value=None, initial_source=None, ignore_null=False):
    result_value = initial_value
    result_source = initial_source
    
    # Apply ignore_null to initial value
    if ignore_null and (result_value is None or result_value == ""):
        result_value = None
        result_source = None
    
    for idx in range(len(unified_records) - 1, -1, -1):
        if idx < len(unified_datasets):
            record_value = unified_records[idx].get(attribute)
            if record_value is not None:
                if not ignore_null or record_value != "":
                    result_value = record_value
                    result_source = unified_datasets[idx]
                    break
    
    return result_value, result_source

# COMMAND ----------

# Process each merged record for survivorship recalculation
survivorship_updates = []

for record_info in merged_records_to_process:
    lakefusion_id = record_info['lakefusion_id']
    primary_merged_records = record_info['primary_merged_records']
    other_source_records = record_info['other_source_records']
    
    logger.info(f"Processing lakefusion_id: {lakefusion_id}")
    
    # Get initial record values
    initial_record_query = f"""
    WITH RankedVersions AS (
        SELECT {id_key}, version, attribute_source_mapping,
            ROW_NUMBER() OVER (PARTITION BY {id_key} ORDER BY version ASC) AS rn
        FROM {master_attribute_version_sources_table}
        WHERE {id_key} = '{lakefusion_id}'
    )
    SELECT {id_key}, version, attribute_source_mapping
    FROM RankedVersions
    WHERE rn = 1
    """
    
    initial_record_df = spark.sql(initial_record_query)
    
    if initial_record_df.count() == 0:
        logger.warning(f"No initial version found for master record {lakefusion_id}")
        continue
    
    initial_record = initial_record_df.first()
    initial_attribute_mapping = initial_record["attribute_source_mapping"]
    
    initial_values = {}
    initial_sources = {}
    
    for attr_map in initial_attribute_mapping:
        attr_name = attr_map.attribute_name
        attr_value = attr_map.attribute_value
        attr_source = attr_map.source
        
        if attr_name and attr_name != id_key:
            initial_values[attr_name] = attr_value
            initial_sources[attr_name] = attr_source
    
    # Collect remaining unified records (excluding deleted primary records)
    remaining_unified_records = []
    remaining_datasets = []
    
    # Add records from other sources (unified table)
    if other_source_records:
        other_ids = [rec['merged_id'] for rec in other_source_records]
        other_ids_str = "','".join(other_ids)
        
        unified_records_query = f"""
        SELECT *, dataset.path as dataset_path
        FROM {unified_table}
        WHERE {id_key} IN ('{other_ids_str}')
        """
        
        unified_records_df = spark.sql(unified_records_query)
        
        for row in unified_records_df.collect():
            record_dict = row.asDict()
            remaining_unified_records.append(record_dict)
            remaining_datasets.append(record_dict['dataset_path'])
    
    # Check if any records remain after deletion
    if not remaining_unified_records:
        # No remaining records, use initial values
        logger.info(f"No remaining records for {lakefusion_id}, reverting to initial values")
        
        resultant_record = {id_key: lakefusion_id}
        resultant_attr_mapping = []
        
        for attr in entity_attributes:
            if attr == id_key:
                continue
            
            attr_value = initial_values.get(attr)
            attr_source = initial_sources.get(attr)
            resultant_record[attr] = attr_value
            
            resultant_attr_mapping.append({
                'attribute_name': attr,
                'attribute_value': str(attr_value) if attr_value is not None else None,
                'source': attr_source
            })
        
        survivorship_updates.append({
            'lakefusion_id': lakefusion_id,
            'resultant_record': resultant_record,
            'resultant_attr_mapping': resultant_attr_mapping
        })
    
    elif len(remaining_unified_records) == 1:
        # Only one record remains, create new master record
        logger.info(f"One record remains for {lakefusion_id}, creating new master record")
        
        remaining_record = remaining_unified_records[0]
        resultant_record = {id_key: lakefusion_id}
        resultant_attr_mapping = []
        
        for attr in entity_attributes:
            if attr == id_key:
                continue
            
            attr_value = remaining_record.get(attr)
            attr_source = remaining_datasets[0]
            resultant_record[attr] = attr_value
            
            resultant_attr_mapping.append({
                'attribute_name': attr,
                'attribute_value': str(attr_value) if attr_value is not None else None,
                'source': attr_source
            })
        
        survivorship_updates.append({
            'lakefusion_id': lakefusion_id,
            'resultant_record': resultant_record,
            'resultant_attr_mapping': resultant_attr_mapping
        })
    
    else:
        # Multiple records remain, apply survivorship rules
        logger.info(f"Multiple records remain for {lakefusion_id}, applying survivorship rules")
        
        resultant_record = {id_key: lakefusion_id}
        resultant_attr_mapping = []
        
        for attribute in entity_attributes:
            if attribute == id_key:
                continue
            
            initial_value = initial_values.get(attribute)
            initial_source = initial_sources.get(attribute)
            
            rule = next(
                (item for item in default_survivorship_rules 
                 if item.get('attribute') == attribute or item.get('entity_attribute') == attribute), 
                None
            )
            
            result_value = initial_value
            result_source = initial_source
            
            if rule:
                # Extract toggle properties
                ignore_null = rule.get('ignoreNull', False)
                
                if rule['strategy'] == 'Source System':
                    strategyRule = rule['strategyRule']
                    value, source = apply_source_system_strategy(
                        attribute, strategyRule, remaining_unified_records, None, 
                        remaining_datasets, initial_values, initial_sources, ignore_null
                    )
                    result_value = value
                    result_source = source
                    
                elif rule['strategy'] == 'Aggregation':
                    aggregate_unique_only = rule.get('aggregateUniqueOnly', False)
                    aggregated_value = apply_aggregation_strategy(
                        attribute, None, remaining_unified_records, initial_value,
                        ignore_null, aggregate_unique_only
                    )
                    result_value = aggregated_value
                    
                    sources = [s for s in [initial_source] + remaining_datasets if s]
                    result_source = ','.join(sources)
                    
                elif rule['strategy'] == 'Recency':
                    value, source = apply_recency_strategy(
                        attribute, remaining_unified_records, remaining_datasets, 
                        initial_value, initial_source, ignore_null
                    )
                    result_value = value
                    result_source = source
            
            resultant_record[attribute] = result_value
            
            resultant_attr_mapping.append({
                'attribute_name': attribute,
                'attribute_value': str(result_value) if result_value is not None else None,
                'source': result_source
            })
        
        survivorship_updates.append({
            'lakefusion_id': lakefusion_id,
            'resultant_record': resultant_record,
            'resultant_attr_mapping': resultant_attr_mapping
        })

logger.info(f"Survivorship updates prepared: {len(survivorship_updates)}")

# COMMAND ----------

# Batch update master table with survivorship results
if survivorship_updates:
    # Create DataFrame for master updates
    master_updates_data = [update['resultant_record'] for update in survivorship_updates]
    
    # Define schema for master updates
    master_update_fields = [StructField(id_key, StringType(), False)]
    for attr in entity_attributes:
        if attr != id_key:
            master_update_fields.append(StructField(attr, StringType(), True))
    
    master_update_schema = StructType(master_update_fields)
    master_updates_df = spark.createDataFrame(master_updates_data, schema=master_update_schema)
    
    # Get additional columns from original master table
    master_columns = master_records_to_delete.columns
    updated_columns = master_updates_df.columns
    extra_columns = [c for c in master_columns if c not in updated_columns]
    
    for col_name in extra_columns:
        # Get a sample value for the extra column
        sample_value = master_records_to_delete.select(col_name).first()[0]
        master_updates_df = master_updates_df.withColumn(col_name, lit(sample_value))
    
    # Batch update master table
    master_delta_table = DeltaTable.forName(spark, master_table)
    
    master_delta_table.alias("target").merge(
        source=master_updates_df.alias("source"),
        condition=f"target.{id_key} = source.{id_key}"
    ).whenMatchedUpdate(set={
        attr: f"source.{attr}" 
        for attr in entity_attributes 
        if attr in master_updates_df.columns and attr != id_key
    }).execute()
    
    logger.info("Master table updated with survivorship results")
    
    # Get updated master version
    updated_master_version = get_current_table_version(master_table)
    
    # Log deletions in merge activities
    delete_activities = []
    for update in survivorship_updates:
        lakefusion_id = update['lakefusion_id']
        delete_activities.append(f"('{lakefusion_id}', '', '{primary_table}', {updated_master_version}, '{ActionType.SOURCE_DELETE.value}')")
    
    delete_activities_sql = f"""
    INSERT INTO {merge_activities_table}
    (`master_{id_key}`, `{id_key}`, source, version, action_type)
    VALUES {', '.join(delete_activities)}
    """
    
    spark.sql(delete_activities_sql)
    logger.info("Logged merged record deletions in merge activities")
    
    # Update attribute version sources
    for update in survivorship_updates:
        attr_structs = []
        for attr_map in update['resultant_attr_mapping']:
            attr_name = attr_map['attribute_name']
            
            if attr_map['attribute_value']:
                escaped_value = attr_map['attribute_value'].replace("'", "''")
                attr_value = f"'{escaped_value}'"
            else:
                attr_value = "NULL"
                
            if attr_map['source']:
                escaped_source = attr_map['source'].replace("'", "''")
                attr_source = f"'{escaped_source}'"
            else:
                attr_source = "NULL"
            
            attr_structs.append(f"named_struct('attribute_name', '{attr_name}', 'attribute_value', {attr_value}, 'source', {attr_source})")
        
        attr_array = f"array({', '.join(attr_structs)})"
        
        insert_sql = f"""
        INSERT INTO {master_attribute_version_sources_table}
        ({id_key}, version, attribute_source_mapping)
        VALUES ('{update['lakefusion_id']}', {updated_master_version}, {attr_array})
        """
        
        spark.sql(insert_sql)
    
    logger.info("Attribute version sources updated")

# COMMAND ----------

# Clear search_results for any records that reference the deleted lakefusion_ids
# This must be done in BOTH unified and unified_deduplicate tables
# Collect all deleted lakefusion_ids (both standalone and merged)
all_deleted_lakefusion_ids = []

if standalone_records.count() > 0:
    standalone_ids = [row[id_key] for row in standalone_records.collect()]
    all_deleted_lakefusion_ids.extend(standalone_ids)

if survivorship_updates:
    # Note: These are NOT deleted, but we still need to clear references to them
    # since their data changed due to survivorship recalculation
    pass  # We don't add these to the deleted list

# For primary table deletes, we actually delete the master records
# So we should clear references to those deleted master lakefusion_ids
if all_deleted_lakefusion_ids:
    logger.info("=" * 80)
    logger.info("CLEARING SEARCH RESULTS FOR RECORDS REFERENCING DELETED IDS")
    logger.info("=" * 80)
    
    # Define both table names
    unified_deduplicate_table = f"{catalog_name}.silver.{entity}_unified_deduplicate"
    if experiment_id:
        unified_deduplicate_table += f"_{experiment_id}"
    
    tables_to_clear = [
        (unified_table, "unified"),
        (unified_deduplicate_table, "unified_deduplicate")
    ]
    
    total_cleared_across_tables = 0
    
    for table_name, table_type in tables_to_clear:
        if not spark.catalog.tableExists(table_name):
            logger.warning(f"Table {table_name} does not exist, skipping...")
            continue
        
        logger.info("=" * 60)
        logger.info(f"Processing {table_type} table: {table_name}")
        logger.info("=" * 60)
        
        table_total_cleared = 0
        
        for lakefusion_id in all_deleted_lakefusion_ids:
            # Find all records that have this deleted lakefusion_id in their search_results
            try:
                records_with_references = spark.sql(f"""
                    SELECT {id_key}
                    FROM {table_name}
                    WHERE search_results LIKE '%{lakefusion_id}%'
                    AND {id_key} != '{lakefusion_id}'
                """).collect()
                
                if records_with_references:
                    referenced_ids = [row[id_key] for row in records_with_references]
                    referenced_ids_str = "','".join(referenced_ids)
                    
                    # Clear search_results and scoring_results for these records
                    spark.sql(f"""
                        UPDATE {table_name}
                        SET search_results = '',
                            scoring_results = ''
                        WHERE {id_key} IN ('{referenced_ids_str}')
                    """)
                    
                    table_total_cleared += len(referenced_ids)
                    logger.info(f"  Cleared search_results for {len(referenced_ids)} records that referenced {lakefusion_id}")
                    logger.info(f"    Referenced records: {referenced_ids[:5]}{'...' if len(referenced_ids) > 5 else ''}")
                    
            except Exception as e:
                logger.error(f"  Error processing {lakefusion_id} in {table_type}: {str(e)}")
                continue
        
        if table_total_cleared > 0:
            logger.info(f"  Total records cleared in {table_type}: {table_total_cleared}")
            total_cleared_across_tables += table_total_cleared
        else:
            logger.info(f"  No records needed clearing in {table_type}")
    
    logger.info("=" * 80)
    logger.info(f"TOTAL RECORDS CLEARED ACROSS ALL TABLES: {total_cleared_across_tables}")
    logger.info("=" * 80)

# COMMAND ----------

# Summary
logger.info("=== DELETE PROCESSING SUMMARY ===")
logger.info(f"Entity: {entity}")
logger.info(f"Primary table: {primary_table}")
logger.info(f"Processed version range: {last_processed_version} to {current_version}")
logger.info(f"Total deleted source records: {len(deleted_source_ids)}")
logger.info(f"Standalone records deleted: {standalone_records.count()}")
logger.info(f"Merged records processed with survivorship: {len(survivorship_updates)}")

if standalone_records.count() > 0:
    standalone_ids = [row[id_key] for row in standalone_records.collect()]
    logger.info(f"Standalone deleted lakefusion_ids: {standalone_ids}")

if survivorship_updates:
    for update in survivorship_updates:
        logger.info(f"Merged record processed: {update['lakefusion_id']}")

# COMMAND ----------

logger.info("Primary source deletes processing completed successfully")
