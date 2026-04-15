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
dbutils.widgets.text("id_key", "", "Master Primary Key (lakefusion_id)")
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("meta_info_table", "", "Meta Info Table")
dbutils.widgets.text("dataset_objects", "", "Dataset Objects")
dbutils.widgets.text("default_survivorship_rules", "", "Default Survivorship Rules")

# COMMAND ----------

entity = dbutils.widgets.get("entity")
id_key = dbutils.widgets.get("id_key")
entity_attributes = dbutils.widgets.get("entity_attributes")
experiment_id = dbutils.widgets.get("experiment_id")
catalog_name = dbutils.widgets.get("catalog_name")
meta_info_table = dbutils.widgets.get("meta_info_table")
dataset_objects = dbutils.widgets.get("dataset_objects")
default_survivorship_rules = dbutils.widgets.get("default_survivorship_rules")

# COMMAND ----------

# Get values from task values
entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
id_key = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "id_key", debugValue=id_key)
entity_attributes = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes", debugValue=entity_attributes)
meta_info_table = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "meta_info_table", debugValue=meta_info_table)
dataset_objects = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "dataset_objects", debugValue=dataset_objects)
default_survivorship_rules = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "default_survivorship_rules", debugValue=default_survivorship_rules)

# COMMAND ----------

# Parse JSON parameters
entity_attributes = json.loads(entity_attributes)
entity_attributes.append("lakefusion_id")
dataset_objects = json.loads(dataset_objects)
default_survivorship_rules = json.loads(default_survivorship_rules)

# Create dataset mapping
dataset_id_to_path = {dataset['id']: dataset['path'] for dataset in dataset_objects.values()}

# Update survivorship rules with dataset paths
for rule in default_survivorship_rules:
    if rule.get('strategy') == 'Source System' and 'strategyRule' in rule:
        rule['strategyRule'] = [dataset_id_to_path.get(dataset_id) for dataset_id in rule['strategyRule']]

# COMMAND ----------

# MAGIC %run ../utils/execute_utils

# COMMAND ----------

logger.info(f"Entity attributes: {entity_attributes}")
logger.info(f"ID key: {id_key}")

# COMMAND ----------

# MAGIC %run ../utils/match_merge_utils

# COMMAND ----------

# Define table names
unified_table = f"{catalog_name}.silver.{entity}_unified"
master_table = f"{catalog_name}.gold.{entity}_master"
processed_unified_table = f"{catalog_name}.silver.{entity}_processed_unified"
if experiment_id:
    unified_table += f"_{experiment_id}"
    master_table += f"_{experiment_id}"
    processed_unified_table += f"_{experiment_id}"

merge_activities_table = f"{master_table}_merge_activities"
master_attribute_version_sources_table = f"{master_table}_attribute_version_sources"

aggregation_strategy_separator = ' • '

logger.info(f"Unified table: {unified_table}")
logger.info(f"Master table: {master_table}")
logger.info(f"Processed unified table: {processed_unified_table}")
logger.info(f"Merge activities table: {merge_activities_table}")
logger.info(f"Attribute version sources table: {master_attribute_version_sources_table}")

# COMMAND ----------

# Get last processed version for unified table
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

last_processed_version = get_last_processed_version(unified_table, entity)
logger.info(f"Last processed version for {unified_table}: {last_processed_version}")

# COMMAND ----------

# Get current version of unified table
def get_current_table_version(table_name):
    try:
        return spark.sql(f"SELECT MAX(version) as version FROM (DESCRIBE HISTORY {table_name})").collect()[0]['version']
    except Exception as e:
        logger.error(f"Error getting current version for {table_name}: {str(e)}")
        return 0

current_version = get_current_table_version(unified_table)
logger.info(f"Current version for {unified_table}: {current_version}")

# COMMAND ----------

# Check if there are new versions to process
if current_version <= last_processed_version:
    logger.info(f"No new versions to process. Last processed: {last_processed_version}, Current: {current_version}")
    dbutils.notebook.exit("No new versions to process")

# Calculate the starting version (next version after last processed)
starting_version = last_processed_version + 1
logger.info(f"Processing versions from {starting_version} to {current_version}")

# COMMAND ----------

# Get CDF changes (deletes only) from unified table
def get_cdf_deletes(table_name, from_version, to_version):
    try:
        cdf_query = f"""
        SELECT *, _change_type, _commit_version, _commit_timestamp
        FROM table_changes('{table_name}', {from_version}, {to_version})
        WHERE _change_type = 'delete'
        ORDER BY {id_key}, _commit_version
        """
        return spark.sql(cdf_query)
    except Exception as e:
        logger.error(f"Error getting CDF deletes: {str(e)}")
        return spark.createDataFrame([], StructType([]))

cdf_deletes = get_cdf_deletes(unified_table, starting_version, current_version)
logger.info(f"CDF deletes count: {cdf_deletes.count()}")

if cdf_deletes.count() == 0:
    logger.info("No deletes found in unified table since last processing")
    dbutils.notebook.exit("No deletes to process")

# COMMAND ----------

deleted_records_info = []
for row in cdf_deletes.collect():
    lakefusion_id = row[id_key]
    # Get source table from the unified table structure
    source_table = row['table_name'] if 'table_name' in row else (row['dataset']['path'] if 'dataset' in row and isinstance(row['dataset'], dict) and 'path' in row['dataset'] else None)
    
    deleted_records_info.append({
        'lakefusion_id': lakefusion_id,
        'source_table': source_table
    })

deleted_lakefusion_ids = [rec['lakefusion_id'] for rec in deleted_records_info]
logger.info(f"Deleted lakefusion IDs: {deleted_lakefusion_ids}")

# Create a mapping of lakefusion_id to source_table
lakefusion_to_source = {rec['lakefusion_id']: rec['source_table'] for rec in deleted_records_info}

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
if not deleted_lakefusion_ids:
    logger.info("No deleted lakefusion IDs found")
    dbutils.notebook.exit("No lakefusion IDs to process for deletion")

# Format deleted lakefusion IDs
formatted_deleted_ids = [format_sql_value(lid) for lid in deleted_lakefusion_ids]
deleted_ids_str = ",".join(formatted_deleted_ids)

logger.info(f"Formatted deleted lakefusion IDs string: {deleted_ids_str}")
logger.info(f"Source table mapping: {lakefusion_to_source}")

# COMMAND ----------

# Check merge activities to determine how each record was handled
def analyze_merge_activities(lakefusion_ids):
    """
    Analyze merge activities to categorize deleted records:
    1. Job inserted records (JOB_INSERT) - simple deletion
    2. Records that were merged into other records - need to find their master records for survivorship
    """
    if not lakefusion_ids:
        return [], []
    
    ids_str = "','".join(lakefusion_ids)
    
    # Check for JOB_INSERT records (standalone records)
    job_insert_query = f"""
    SELECT DISTINCT master_{id_key} as lakefusion_id
    FROM {merge_activities_table}
    WHERE master_{id_key} IN ('{ids_str}')
    AND action_type = '{ActionType.JOB_INSERT.value}'
    AND NOT EXISTS (
        SELECT 1 FROM {merge_activities_table} m2
        WHERE m2.master_{id_key} = {merge_activities_table}.master_{id_key}
        AND m2.action_type IN ('{ActionType.JOB_MERGE.value}', '{ActionType.MANUAL_MERGE.value}')
    )
    """
    
    job_insert_df = spark.sql(job_insert_query)
    job_insert_ids = [row['lakefusion_id'] for row in job_insert_df.collect()]
    
    # Find master records that need survivorship recalculation
    # These are records where the deleted IDs were merged INTO other master records
    master_records_query = f"""
    WITH MergeActivitiesCast AS (
        SELECT *, 
               COALESCE(CAST(master_{id_key} AS STRING), '') AS master_id_str,
               COALESCE(CAST({id_key} AS STRING), '') AS id_str
        FROM {merge_activities_table}
    ),
    AffectedMasters AS (
        SELECT DISTINCT master_id_str as master_lakefusion_id
        FROM MergeActivitiesCast
        WHERE id_str IN ('{ids_str}')
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
    SELECT master_lakefusion_id
    FROM AffectedMasters
    WHERE master_lakefusion_id NOT IN ('{("','".join(job_insert_ids) if job_insert_ids else "")}')
    """
    
    master_records_df = spark.sql(master_records_query)
    affected_master_ids = [row['master_lakefusion_id'] for row in master_records_df.collect()]
    
    return job_insert_ids, affected_master_ids

job_insert_ids, affected_master_ids = analyze_merge_activities(deleted_lakefusion_ids)
logger.info(f"Job insert records (standalone): {len(job_insert_ids)}")
logger.info(f"Master records requiring survivorship recalculation: {len(affected_master_ids)}")
logger.info(f"Affected master IDs: {affected_master_ids}")

# COMMAND ----------

# Initialize tracking DataFrames for batch operations
all_merge_activities = []
all_attribute_versions = []

# Process standalone records (simple deletion)
if job_insert_ids:
    logger.info("Processing standalone record deletions...")
    
    # Get updated master version
    master_version = get_current_table_version(master_table)
    
    # Collect merge activities for batch insert
    for lakefusion_id in job_insert_ids:
        all_merge_activities.append({
            f'master_{id_key}': lakefusion_id,
            id_key: '',
            'source': source_table,
            'version': master_version + 1,  # Predict next version
            'action_type': ActionType.SOURCE_DELETE.value
        })
    
    logger.info(f"Prepared {len(job_insert_ids)} merge activity records for batch insert")

# COMMAND ----------

# Helper functions for survivorship recalculation
def apply_source_system_strategy(attribute, strategyRule, unified_records, master_record, unified_datasets, initial_values, initial_sources):
    result_value = initial_values.get(attribute)
    result_source = initial_sources.get(attribute)

    for priority_dataset in strategyRule:
        try:
            unified_dataset_index = unified_datasets.index(priority_dataset)
            unified_record_column_value = unified_records[unified_dataset_index].get(attribute)
            if unified_record_column_value is not None:
                result_value = unified_record_column_value
                result_source = priority_dataset
                return result_value, result_source
        except (ValueError, IndexError):
            pass
    
    for idx, record in enumerate(unified_records):
        if idx < len(unified_datasets) and record.get(attribute) is not None:
            result_value = record[attribute]
            result_source = unified_datasets[idx]
            return result_value, result_source
    
    return result_value, result_source

def apply_aggregation_strategy(attribute, master_record, unified_records, initial_value=None):
    unified_values = [record.get(attribute) for record in unified_records if record.get(attribute) is not None]
    
    all_values = unified_values[:]
    if initial_value is not None:
        all_values.append(initial_value)
    
    flat_values = []
    for value in all_values:
        parts = str(value).split(aggregation_strategy_separator)
        flat_values.extend(parts)
    
    unique_values = list(set([v.strip() for v in flat_values if v.strip()]))
    
    return aggregation_strategy_separator.join(unique_values) if unique_values else None

def apply_recency_strategy(attribute, unified_records, unified_datasets, initial_value=None, initial_source=None):
    result_value = initial_value
    result_source = initial_source
    
    for idx in range(len(unified_records) - 1, -1, -1):
        if idx < len(unified_datasets) and unified_records[idx].get(attribute) is not None:
            result_value = unified_records[idx][attribute]
            result_source = unified_datasets[idx]
            break
    
    return result_value, result_source

# COMMAND ----------

# Process master records affected by unified record deletions
survivorship_updates = []

if affected_master_ids:
    logger.info("Processing master records affected by unified record deletions...")
    
    for master_lakefusion_id in affected_master_ids:
        logger.info(f"Processing master record: {master_lakefusion_id}")
        
        # Get initial record values for this master record
        initial_record_query = f"""
        WITH RankedVersions AS (
            SELECT {id_key}, version, attribute_source_mapping,
                ROW_NUMBER() OVER (PARTITION BY {id_key} ORDER BY version ASC) AS rn
            FROM {master_attribute_version_sources_table}
            WHERE {id_key} = '{master_lakefusion_id}'
        )
        SELECT {id_key}, version, attribute_source_mapping
        FROM RankedVersions
        WHERE rn = 1
        """
        
        initial_record_df = spark.sql(initial_record_query)
        
        if initial_record_df.count() == 0:
            logger.warning(f"No initial version found for master record {master_lakefusion_id}")
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
        
        # Get remaining merged records for this master record (excluding deleted ones)
        remaining_merged_query = f"""
        WITH MergeActivitiesCast AS (
            SELECT *, 
                   COALESCE(CAST(master_{id_key} AS STRING), '') AS master_id_str,
                   COALESCE(CAST({id_key} AS STRING), '') AS id_str
            FROM {merge_activities_table}
        ),
        ValidMerges AS (
            SELECT *
            FROM MergeActivitiesCast
            WHERE master_id_str = '{master_lakefusion_id}'
            AND action_type IN ('{ActionType.JOB_MERGE.value}', '{ActionType.MANUAL_MERGE.value}')
            AND NOT EXISTS (
                SELECT 1
                FROM MergeActivitiesCast unmerge
                WHERE unmerge.master_id_str = '{master_lakefusion_id}'
                AND unmerge.id_str = MergeActivitiesCast.id_str
                AND unmerge.action_type = '{ActionType.MANUAL_UNMERGE.value}'
                AND unmerge.version > MergeActivitiesCast.version
            )
        )
        SELECT u.*, m.source as merge_source
        FROM {unified_table} u
        JOIN ValidMerges m ON COALESCE(CAST(u.{id_key} AS STRING), '') = m.id_str
        WHERE u.{id_key} NOT IN ({deleted_ids_str})
        """
        
        remaining_records_df = spark.sql(remaining_merged_query)
        remaining_count = remaining_records_df.count()
        
        logger.info(f"Remaining records for master {master_lakefusion_id}: {remaining_count}")
        
        if remaining_count == 0:
            # No remaining records, revert to initial values
            logger.info(f"No remaining records for {master_lakefusion_id}, reverting to initial values")
            
            resultant_record = {id_key: master_lakefusion_id}
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
                'lakefusion_id': master_lakefusion_id,
                'resultant_record': resultant_record,
                'resultant_attr_mapping': resultant_attr_mapping
            })
            
        elif remaining_count == 1:
            # Only one record remains, use its values
            logger.info(f"One record remains for {master_lakefusion_id}, using its values")
            
            remaining_record = remaining_records_df.first().asDict()
            dataset_path = remaining_record.get('dataset', {}).get('path', '') if remaining_record.get('dataset') else ''
            
            resultant_record = {id_key: master_lakefusion_id}
            resultant_attr_mapping = []
            
            for attr in entity_attributes:
                if attr == id_key:
                    continue
                
                attr_value = remaining_record.get(attr)
                attr_source = dataset_path
                resultant_record[attr] = attr_value
                
                resultant_attr_mapping.append({
                    'attribute_name': attr,
                    'attribute_value': str(attr_value) if attr_value is not None else None,
                    'source': attr_source
                })
            
            survivorship_updates.append({
                'lakefusion_id': master_lakefusion_id,
                'resultant_record': resultant_record,
                'resultant_attr_mapping': resultant_attr_mapping
            })
            
        else:
            # Multiple records remain, apply survivorship rules
            logger.info(f"Multiple records remain for {master_lakefusion_id}, applying survivorship rules")
            
            remaining_records = remaining_records_df.collect()
            remaining_unified_records = [record.asDict() for record in remaining_records]
            remaining_datasets = [record.asDict().get('dataset', {}).get('path', '') for record in remaining_records]
            
            resultant_record = {id_key: master_lakefusion_id}
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
                    if rule['strategy'] == 'Source System':
                        strategyRule = rule['strategyRule']
                        value, source = apply_source_system_strategy(
                            attribute, strategyRule, remaining_unified_records, None, 
                            remaining_datasets, initial_values, initial_sources
                        )
                        result_value = value
                        result_source = source
                        
                    elif rule['strategy'] == 'Aggregation':
                        aggregated_value = apply_aggregation_strategy(
                            attribute, None, remaining_unified_records, initial_value
                        )
                        result_value = aggregated_value
                        
                        sources = [s for s in [initial_source] + remaining_datasets if s]
                        result_source = ','.join(sources)
                        
                    elif rule['strategy'] == 'Recency':
                        value, source = apply_recency_strategy(
                            attribute, remaining_unified_records, remaining_datasets, initial_value, initial_source
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
                'lakefusion_id': master_lakefusion_id,
                'resultant_record': resultant_record,
                'resultant_attr_mapping': resultant_attr_mapping
            })

logger.info(f"Survivorship updates prepared: {len(survivorship_updates)}")

# COMMAND ----------

# Prepare master table updates
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
    master_df = spark.read.table(master_table)
    master_columns = master_df.columns
    updated_columns = master_updates_df.columns
    extra_columns = [c for c in master_columns if c not in updated_columns]
    
    # Add extra columns with sample values
    for col_name in extra_columns:
        sample_value = master_df.select(col_name).first()[0]
        master_updates_df = master_updates_df.withColumn(col_name, lit(sample_value))
    
    # Get updated master version (for tracking purposes)
    current_master_version = get_current_table_version(master_table)
    predicted_master_version = current_master_version + 1
    
    # Collect merge activities for batch insert
    for update in survivorship_updates:
        lakefusion_id = update['lakefusion_id']
        all_merge_activities.append({
            f'master_{id_key}': lakefusion_id,
            id_key: '',
            'source': source_table,
            'version': predicted_master_version,
            'action_type': ActionType.SOURCE_DELETE.value
        })
    
    # Collect attribute version sources for batch insert
    for update in survivorship_updates:
        all_attribute_versions.append({
            id_key: update['lakefusion_id'],
            'version': predicted_master_version,
            'attribute_source_mapping': update['resultant_attr_mapping']
        })
    
    logger.info(f"Prepared {len(survivorship_updates)} records for batch operations")
    logger.info(f"Master updates prepared: {master_updates_df.count()} records")

# COMMAND ----------

logger.info("PREVIEW OF ALL PENDING CHANGES")
logger.info("-" * 80)

logger.info(f"1. STANDALONE DELETIONS: {len(job_insert_ids) if job_insert_ids else 0}")
if job_insert_ids:
    logger.info("   Records to be deleted from master table:")
    for idx, record_id in enumerate(job_insert_ids):
        logger.info(f"   - {idx+1}. {record_id}")

logger.info(f"2. SURVIVORSHIP UPDATES: {len(survivorship_updates)}")
if survivorship_updates:
    logger.info("   Master records to be updated:")
    for idx, update in enumerate(survivorship_updates):
        logger.info(f"   - {idx+1}. {update['lakefusion_id']}")

logger.info(f"3. MERGE ACTIVITIES: {len(all_merge_activities)}")

logger.info(f"4. ATTRIBUTE VERSION SOURCES: {len(all_attribute_versions)}")

logger.info(f"5. PROCESSED UNIFIED TABLE CLEANUP: {len(deleted_lakefusion_ids)}")

# COMMAND ----------

if job_insert_ids:
    logger.info(f"Deleting {len(job_insert_ids)} standalone records from master table...")
    standalone_ids_str = "','".join(job_insert_ids)
    
    delete_master_sql = f"""
    DELETE FROM {master_table}
    WHERE {id_key} IN ('{standalone_ids_str}')
    """
    
    spark.sql(delete_master_sql)
    logger.info(f"   Deleted {len(job_insert_ids)} standalone records")
else:
    logger.info("No standalone records to delete")

# COMMAND ----------

if survivorship_updates:
    logger.info(f"Updating {len(survivorship_updates)} master records with survivorship results...")
    
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
    
    logger.info(f"   Updated {len(survivorship_updates)} master records")

    # Get final master version after all updates
    final_master_version = get_current_table_version(master_table)

    # Update version numbers in tracking arrays with actual version
    for activity in all_merge_activities:
        activity['version'] = final_master_version
        
    for attr_version in all_attribute_versions:
        attr_version['version'] = final_master_version

    logger.info(f"   Master table version updated to: {final_master_version}")
else:
    logger.info("No master table updates needed")

# COMMAND ----------

if all_merge_activities:
    logger.info(f"Batch inserting {len(all_merge_activities)} merge activity records...")
    
    # Define schema for merge activities
    merge_activities_schema = StructType([
        StructField(f'master_{id_key}', StringType(), False),
        StructField(id_key, StringType(), True),
        StructField('source', StringType(), False),
        StructField('version', IntegerType(), False),
        StructField('action_type', StringType(), False)
    ])
    
    # Create DataFrame for merge activities
    merge_activities_df = spark.createDataFrame(all_merge_activities, schema=merge_activities_schema)
    
    # Batch insert using Delta merge (upsert)
    merge_activities_delta_table = DeltaTable.forName(spark, merge_activities_table)
    
    merge_activities_delta_table.alias("target").merge(
        source=merge_activities_df.alias("source"),
        condition=f"""target.master_{id_key} = source.master_{id_key} 
                     AND target.{id_key} = source.{id_key} 
                     AND target.source = source.source 
                     AND target.version = source.version 
                     AND target.action_type = source.action_type"""
    ).whenNotMatchedInsertAll().execute()
    
    logger.info("   Merge activities batch insert completed")
else:
    logger.info("No merge activities to insert")

# COMMAND ----------

if all_attribute_versions:
    logger.info(f"Batch inserting {len(all_attribute_versions)} attribute version records...")
    
    # Define schema for attribute source mapping
    attr_mapping_struct = StructType([
        StructField("attribute_name", StringType(), True),
        StructField("attribute_value", StringType(), True),
        StructField("source", StringType(), True)
    ])
    
    # Define schema for attribute version sources
    attr_version_schema = StructType([
        StructField(id_key, StringType(), False),
        StructField('version', IntegerType(), False),
        StructField('attribute_source_mapping', ArrayType(attr_mapping_struct), True)
    ])
    
    # Create DataFrame for attribute version sources
    attr_version_df = spark.createDataFrame(all_attribute_versions, schema=attr_version_schema)
    
    # Batch insert using Delta merge
    attr_version_delta_table = DeltaTable.forName(spark, master_attribute_version_sources_table)
    
    attr_version_delta_table.alias("target").merge(
        source=attr_version_df.alias("source"),
        condition=f"target.{id_key} = source.{id_key} AND target.version = source.version"
    ).whenNotMatchedInsertAll().execute()
    
    logger.info("   Attribute version sources batch insert completed")
else:
    logger.info("No attribute version sources to insert")

# COMMAND ----------

# Clean up processed unified table
if deleted_lakefusion_ids:
    logger.info("Cleaning up processed unified table...")
    try:
        table_exists = spark.catalog.tableExists(processed_unified_table)

        if table_exists:
            processed_unified_delta = DeltaTable.forName(spark, processed_unified_table)

            processed_unified_delta.delete(col(id_key).isin(deleted_lakefusion_ids))
            logger.info(f"Deleted {len(deleted_lakefusion_ids)} records from processed unified table")

            processed_unified_delta.delete(col(f"master_{id_key}").isin(deleted_lakefusion_ids))
            logger.info("Cleaned up processed unified table references")
        else:
            logger.info(f"Processed unified table {processed_unified_table} does not exist. Skipping cleanup.")

    except Exception as e:
        logger.error(f"Error checking or cleaning up processed unified table: {e}")
        logger.info("Skipping processed unified table cleanup")

# COMMAND ----------

# Clear search_results for any records that reference the deleted lakefusion_ids
# This must be done in BOTH unified and unified_deduplicate tables
if deleted_lakefusion_ids:
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

        logger.info(f"{'='*60}")
        logger.info(f"Processing {table_type} table: {table_name}")
        logger.info(f"{'='*60}")
        
        table_total_cleared = 0
        
        for lakefusion_id in deleted_lakefusion_ids:
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

    logger.info(f"{'='*80}")
    logger.info(f"TOTAL RECORDS CLEARED ACROSS ALL TABLES: {total_cleared_across_tables}")
    logger.info(f"{'='*80}")
