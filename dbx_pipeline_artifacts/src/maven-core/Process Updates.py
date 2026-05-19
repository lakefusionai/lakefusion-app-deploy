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
dbutils.widgets.text("source_id_key", "", "Source Primary Key")
dbutils.widgets.text("id_key", "", "Master Primary Key (lakefusion_id)")
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")
dbutils.widgets.text("attributes_mapping", "", "Attributes Mapping")
dbutils.widgets.text("dataset_objects", "", "Dataset Objects")
dbutils.widgets.text("default_survivorship_rules", "", "Default Survivorship Rules")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("meta_info_table", "", "Meta Info Table")
dbutils.widgets.text("attributes", "", "Merged Description Attributes")

# COMMAND ----------

entity = dbutils.widgets.get("entity")
primary_table = dbutils.widgets.get("primary_table")
source_id_key = dbutils.widgets.get("source_id_key")
id_key = dbutils.widgets.get("id_key")
dataset_objects = dbutils.widgets.get("dataset_objects")
entity_attributes = dbutils.widgets.get("entity_attributes")
attributes_mapping = dbutils.widgets.get("attributes_mapping")
experiment_id = dbutils.widgets.get("experiment_id")
catalog_name = dbutils.widgets.get("catalog_name")
meta_info_table = dbutils.widgets.get("meta_info_table")
attributes = dbutils.widgets.get("attributes")

# COMMAND ----------

# Get values from task values
entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
dataset_objects = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "dataset_objects", debugValue=dataset_objects)
primary_table = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "primary_table", debugValue=primary_table)
source_id_key = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "primary_key", debugValue=source_id_key)
id_key = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "id_key", debugValue=id_key)
entity_attributes = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes", debugValue=entity_attributes)
attributes_mapping = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "attributes_mapping", debugValue=attributes_mapping)
meta_info_table = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "meta_info_table", debugValue=meta_info_table)
attributes = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="match_attributes", debugValue=attributes)

# COMMAND ----------

dataset_objects = json.loads(dataset_objects)
entity_attributes = json.loads(entity_attributes)
entity_attributes.append("lakefusion_id")
attributes = json.loads(attributes)

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

logger.info(f"Master table: {master_table}")
logger.info(f"Merge activities table: {merge_activities_table}")
logger.info(f"Attribute version sources table: {master_attribute_version_sources_table}")
logger.info(f"Unified table: {unified_table}")
logger.info(f"Processed unified table: {processed_unified_table}")

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

last_processed_version = get_last_processed_version(unified_table, entity)
logger.info(f"Last processed version for {unified_table}: {last_processed_version}")

# COMMAND ----------

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

# Get CDF changes (updates only) from unified table
def get_cdf_updates(table_name, from_version, to_version):
    try:
        cdf_query = f"""
        SELECT *, _change_type, _commit_version, _commit_timestamp
        FROM table_changes('{table_name}', {from_version}, {to_version})
        WHERE _change_type IN ('update_preimage', 'update_postimage')
        ORDER BY {id_key}, _commit_version, _change_type
        """
        return spark.sql(cdf_query)
    except Exception as e:
        logger.error(f"Error getting CDF changes: {str(e)}")
        return spark.createDataFrame([], StructType([]))

cdf_changes = get_cdf_updates(unified_table, starting_version, current_version)
logger.info(f"CDF changes count: {cdf_changes.count()}")

# COMMAND ----------

if cdf_changes.count() == 0:
    logger.info("No updates found in unified table since last processing")
    dbutils.notebook.exit("No updates to process")

# COMMAND ----------

# Separate pre and post images
pre_images = cdf_changes.filter(col("_change_type") == "update_preimage").drop("_change_type")
post_images = cdf_changes.filter(col("_change_type") == "update_postimage").drop("_change_type")

logger.info(f"Pre-images count: {pre_images.count()}")
logger.info(f"Post-images count: {post_images.count()}")

# COMMAND ----------

pre_columns = [col(f"pre.{c}").alias(f"pre_{c}") 
               for c in pre_images.columns 
               if c not in ["_commit_version", "_commit_timestamp"]]

post_columns = [col(f"post.{c}").alias(f"post_{c}") 
                for c in post_images.columns 
                if c not in ["_commit_version", "_commit_timestamp"]]

joined_changes = pre_images.alias("pre").join(
    post_images.alias("post"),
    on=[
        col(f"pre.{id_key}") == col(f"post.{id_key}"),
        col("pre.table_name") == col("post.table_name"),
        col("pre._commit_version") == col("post._commit_version"),
        col("pre._commit_timestamp") == col("post._commit_timestamp")
    ],
    how="inner"
).select(
    col(f"post.{id_key}").alias(id_key),
    col("post.table_name").alias("source_table"),
    col("pre._commit_version").alias("_commit_version"),
    col("pre._commit_timestamp").alias("_commit_timestamp"),
    *pre_columns,
    *post_columns
)

logger.info(f"Joined changes count: {joined_changes.count()}")

# COMMAND ----------

def identify_changed_attributes_unified(joined_df, entity_attributes):
    """
    Identify which attributes actually changed between pre and post images
    """
    changed_records = []
    
    # Get attributes to compare (exclude metadata and keys)
    comparable_attributes = [attr for attr in entity_attributes 
                            if attr != id_key]
    
    for row in joined_df.collect():
        lakefusion_id = getattr(row, id_key)
        source_table = getattr(row, "source_table")
        commit_version = getattr(row, "_commit_version")
        
        changed_attributes = {}
        
        # Compare each entity attribute
        for attr in comparable_attributes:
            pre_col = f"pre_{attr}"
            post_col = f"post_{attr}"
            
            if pre_col in joined_df.columns and post_col in joined_df.columns:
                pre_value = getattr(row, pre_col, None)
                post_value = getattr(row, post_col, None)
                
                # Check if values are different
                if pre_value != post_value:
                    changed_attributes[attr] = {
                        'old_value': pre_value,
                        'new_value': post_value
                    }
        
        if changed_attributes:
            changed_records.append({
                'lakefusion_id': lakefusion_id,
                'source_table': source_table,
                'commit_version': commit_version,
                'changed_attributes': changed_attributes
            })
    
    return changed_records

changed_records = identify_changed_attributes_unified(joined_changes, entity_attributes)
logger.info(f"Records with actual changes: {len(changed_records)}")

# COMMAND ----------

if not changed_records:
    dbutils.notebook.exit("No attribute changes to process")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

changed_records_data = []
for record in changed_records:
    for attr_name, attr_info in record['changed_attributes'].items():
        changed_records_data.append({
            'lakefusion_id': record['lakefusion_id'],
            'source_table': record['source_table'],
            'commit_version': record['commit_version'],
            'attribute_name': attr_name,
            'old_value': str(attr_info['old_value']) if attr_info['old_value'] is not None else None,
            'new_value': str(attr_info['new_value']) if attr_info['new_value'] is not None else None
        })

# Define explicit schema
changed_records_schema = StructType([
    StructField('lakefusion_id', StringType(), False),
    StructField('source_table', StringType(), False),
    StructField('commit_version', IntegerType(), False),
    StructField('attribute_name', StringType(), False),
    StructField('old_value', StringType(), True),
    StructField('new_value', StringType(), True)
])

# Create DataFrame with explicit schema
changed_records_df = spark.createDataFrame(changed_records_data, schema=changed_records_schema)
logger.info(f"Changed records DataFrame count: {changed_records_df.count()}")

# COMMAND ----------

all_changed_lakefusion_ids = list(set([record['lakefusion_id'] for record in changed_records]))
logger.info(f"Total unique records with changes: {len(all_changed_lakefusion_ids)}")

# COMMAND ----------

# Clear search and scoring results for ALL changed records in unified table
if all_changed_lakefusion_ids:
    if spark.catalog.tableExists(unified_table):
        lakefusion_ids_str = "','".join(all_changed_lakefusion_ids)
        
        # Clear search_results and scoring_results using SQL UPDATE
        spark.sql(f"""
            UPDATE {unified_table}
            SET search_results = '',
                scoring_results = ''
            WHERE {id_key} IN ('{lakefusion_ids_str}')
        """)
        
        logger.info(f"Cleared search_results and scoring_results for {len(all_changed_lakefusion_ids)} records in unified table")
        logger.info(f"  This includes records that may not be in master yet but need re-matching")
    else:
        logger.warning(f"{unified_table} does not exist")

# COMMAND ----------

# Remove ALL changed records from processed_unified table
if all_changed_lakefusion_ids:
    if spark.catalog.tableExists(processed_unified_table):
        lakefusion_ids_str = "','".join(all_changed_lakefusion_ids)
        
        # Delete records to force LLM re-processing
        delete_result = spark.sql(f"""
            DELETE FROM {processed_unified_table}
            WHERE {id_key} IN ('{lakefusion_ids_str}')
        """)
        
        logger.info(f"Removed {len(all_changed_lakefusion_ids)} records from processed_unified table")
        logger.info(f"  All changed records (including potential matches) will be re-processed by LLM matching")
    else:
        logger.info(f"Note: {processed_unified_table} does not exist yet")

# COMMAND ----------

def analyze_update_merge_status(lakefusion_ids):
    """
    Analyze whether updated records are:
    1. Standalone - JOB_INSERT with no merges
    2. Has merges - Other records merged INTO this one (appears as master_lakefusion_id)
    3. Got merged - This record was merged INTO another master (appears as lakefusion_id with different master)
    
    Returns:
    - standalone_ids: Records that were JOB_INSERT and have no merges
    - master_with_merges: Records that have other records merged into them
    - got_merged_mapping: Dict mapping updated lakefusion_id to master_lakefusion_id it was merged into
    """
    if not lakefusion_ids:
        return [], [], {}
    
    ids_str = "','".join(lakefusion_ids)
    
    # Check for standalone records (JOB_INSERT with no merges)
    standalone_query = f"""
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
    
    standalone_df = spark.sql(standalone_query)
    standalone_ids = [row['lakefusion_id'] for row in standalone_df.collect()]
    
    # Check for records with merges (other records merged INTO these)
    master_with_merges_query = f"""
    WITH MergeActivitiesCast AS (
        SELECT *, 
               COALESCE(CAST(master_{id_key} AS STRING), '') AS master_id_str,
               COALESCE(CAST({id_key} AS STRING), '') AS id_str
        FROM {merge_activities_table}
    ),
    RecordsWithMerges AS (
        SELECT DISTINCT master_id_str as lakefusion_id
        FROM MergeActivitiesCast
        WHERE master_id_str IN ('{ids_str}')
        AND action_type IN ('{ActionType.JOB_MERGE.value}', '{ActionType.MANUAL_MERGE.value}')
        AND id_str != ''
        AND NOT EXISTS (
            SELECT 1
            FROM MergeActivitiesCast unmerge
            WHERE unmerge.master_id_str = MergeActivitiesCast.master_id_str
            AND unmerge.id_str = MergeActivitiesCast.id_str
            AND unmerge.action_type = '{ActionType.MANUAL_UNMERGE.value}'
            AND unmerge.version > MergeActivitiesCast.version
        )
    )
    SELECT lakefusion_id
    FROM RecordsWithMerges
    WHERE lakefusion_id NOT IN ('{("','".join(standalone_ids) if standalone_ids else "")}')
    """
    
    master_with_merges_df = spark.sql(master_with_merges_query)
    master_with_merges = [row['lakefusion_id'] for row in master_with_merges_df.collect()]
    
    # Check for records that GOT merged INTO other masters
    got_merged_query = f"""
    WITH MergeActivitiesCast AS (
        SELECT *, 
               COALESCE(CAST(master_{id_key} AS STRING), '') AS master_id_str,
               COALESCE(CAST({id_key} AS STRING), '') AS id_str
        FROM {merge_activities_table}
    ),
    GotMergedRecords AS (
        SELECT DISTINCT 
            id_str as lakefusion_id,
            master_id_str as master_lakefusion_id
        FROM MergeActivitiesCast
        WHERE id_str IN ('{ids_str}')
        AND action_type IN ('{ActionType.JOB_MERGE.value}', '{ActionType.MANUAL_MERGE.value}')
        AND master_id_str != id_str
        AND NOT EXISTS (
            SELECT 1
            FROM MergeActivitiesCast unmerge
            WHERE unmerge.master_id_str = MergeActivitiesCast.master_id_str
            AND unmerge.id_str = MergeActivitiesCast.id_str
            AND unmerge.action_type = '{ActionType.MANUAL_UNMERGE.value}'
            AND unmerge.version > MergeActivitiesCast.version
        )
    )
    SELECT lakefusion_id, master_lakefusion_id
    FROM GotMergedRecords
    """
    
    got_merged_df = spark.sql(got_merged_query)
    got_merged_mapping = {row['lakefusion_id']: row['master_lakefusion_id'] for row in got_merged_df.collect()}
    
    return standalone_ids, master_with_merges, got_merged_mapping

lakefusion_ids = list(set([record['lakefusion_id'] for record in changed_records]))
standalone_ids, master_with_merges, got_merged_mapping = analyze_update_merge_status(lakefusion_ids)

logger.info(f"Standalone records to update: {len(standalone_ids)}")
logger.info(f"Master records with merges: {len(master_with_merges)}")
logger.info(f"Records that got merged into masters: {len(got_merged_mapping)}")
logger.info(f"Standalone IDs: {standalone_ids}")
logger.info(f"Master with merges IDs: {master_with_merges}")
logger.info(f"Got merged mapping: {got_merged_mapping}")

# COMMAND ----------

def generate_attributes_combined(record, attributes):
    """
    Generate the attributes_combined string by concatenating attribute values
    in the order specified by the attributes list.
    
    Args:
        record: Dictionary containing the record data
        attributes: List of attribute names to combine
    
    Returns:
        String with values separated by ' | '
    """
    values = []
    for attr in attributes:
        value = record.get(attr, '')
        # Convert None to empty string
        if value is None:
            value = ''
        values.append(str(value))
    
    return ' | '.join(values)

# COMMAND ----------

# Initialize tracking for batch operations
all_master_updates = []
all_merge_activities = []
all_attribute_versions = []

# CASE 1: Process standalone records - simple update
if standalone_ids:
    logger.info("CASE 1: Processing standalone record updates...")
    
    for lakefusion_id in standalone_ids:
        # Get changes for this record
        record_changes = [r for r in changed_records if r['lakefusion_id'] == lakefusion_id]
        
        if not record_changes:
            continue
        
        # Get current master record
        master_record_df = spark.sql(f"""
            SELECT * FROM {master_table}
            WHERE {id_key} = '{lakefusion_id}'
        """)
        
        if master_record_df.count() == 0:
            logger.warning(f"No master record found for {lakefusion_id}")
            continue
        
        master_record = master_record_df.first().asDict()
        
        # Get current attribute version mapping
        attr_version_df = spark.sql(f"""
            WITH RankedVersions AS (
                SELECT {id_key}, version, attribute_source_mapping,
                    ROW_NUMBER() OVER (PARTITION BY {id_key} ORDER BY version DESC) AS rn
                FROM {master_attribute_version_sources_table}
                WHERE {id_key} = '{lakefusion_id}'
            )
            SELECT {id_key}, version, attribute_source_mapping
            FROM RankedVersions
            WHERE rn = 1
        """)
        
        if attr_version_df.count() == 0:
            logger.warning(f"No attribute version found for {lakefusion_id}")
            continue
        
        current_attr_mapping = attr_version_df.first()['attribute_source_mapping']
        
        # Prepare update record with all attributes
        update_record = {id_key: lakefusion_id}
        updated_attr_mapping = []
        source_table = record_changes[0]['source_table']
        
        # Apply changes to all attributes
        for attr in entity_attributes:
            if attr == id_key:
                continue
            
            # Check if this attribute was changed
            attr_change = None
            for change in record_changes:
                if attr in change['changed_attributes']:
                    attr_change = change['changed_attributes'][attr]
                    break
            
            if attr_change:
                # Use new value
                update_record[attr] = attr_change['new_value']
                updated_attr_mapping.append({
                    'attribute_name': attr,
                    'attribute_value': str(attr_change['new_value']) if attr_change['new_value'] is not None else None,
                    'source': source_table
                })
            else:
                # Keep existing value and source
                update_record[attr] = master_record.get(attr)
                existing_attr = next((a for a in current_attr_mapping if a.attribute_name == attr), None)
                if existing_attr:
                    updated_attr_mapping.append({
                        'attribute_name': attr,
                        'attribute_value': existing_attr.attribute_value,
                        'source': existing_attr.source
                    })
        
        # Generate attributes_combined
        update_record['attributes_combined'] = generate_attributes_combined(update_record, attributes)
        
        # Add attributes_combined to attribute mapping
        updated_attr_mapping.append({
            'attribute_name': 'attributes_combined',
            'attribute_value': update_record['attributes_combined'],
            'source': source_table
        })
        
        all_master_updates.append(update_record)
        all_attribute_versions.append({
            id_key: lakefusion_id,
            'version': None,
            'attribute_source_mapping': updated_attr_mapping
        })
        all_merge_activities.append({
            f'master_{id_key}': lakefusion_id,
            id_key: '',
            'source': source_table,
            'version': None,
            'action_type': ActionType.SOURCE_UPDATE.value
        })
    
    logger.info(f"Prepared {len([u for u in all_master_updates if u[id_key] in standalone_ids])} standalone updates")

# COMMAND ----------

# CASE 2: Process records with merges - selective update
if master_with_merges:
    logger.info("CASE 2: Processing master records with merges...")
    
    for lakefusion_id in master_with_merges:
        # Get changes for this record
        record_changes = [r for r in changed_records if r['lakefusion_id'] == lakefusion_id]
        
        if not record_changes:
            continue
        
        source_table = record_changes[0]['source_table']
        
        # Get current master record
        master_record_df = spark.sql(f"""
            SELECT * FROM {master_table}
            WHERE {id_key} = '{lakefusion_id}'
        """)
        
        if master_record_df.count() == 0:
            logger.warning(f"No master record found for {lakefusion_id}")
            continue
        
        master_record = master_record_df.first().asDict()
        
        # Get current attribute version mapping
        attr_version_df = spark.sql(f"""
            WITH RankedVersions AS (
                SELECT {id_key}, version, attribute_source_mapping,
                    ROW_NUMBER() OVER (PARTITION BY {id_key} ORDER BY version DESC) AS rn
                FROM {master_attribute_version_sources_table}
                WHERE {id_key} = '{lakefusion_id}'
            )
            SELECT {id_key}, version, attribute_source_mapping
            FROM RankedVersions
            WHERE rn = 1
        """)
        
        if attr_version_df.count() == 0:
            logger.warning(f"No attribute version found for {lakefusion_id}")
            continue
        
        current_attr_mapping = attr_version_df.first()['attribute_source_mapping']
        
        # Prepare update record
        update_record = {id_key: lakefusion_id}
        updated_attr_mapping = []
        has_updates = False
        
        # Process each attribute - only update if from same source and value matches
        for attr in entity_attributes:
            if attr == id_key:
                continue
            
            # Check if this attribute was changed
            attr_change = None
            for change in record_changes:
                if attr in change['changed_attributes']:
                    attr_change = change['changed_attributes'][attr]
                    break
            
            # Get current attribute source
            existing_attr = next((a for a in current_attr_mapping if a.attribute_name == attr), None)
            
            if attr_change and existing_attr:
                # Check if this attribute is contributed by the updated source
                # AND if the current value matches the old value
                if existing_attr.source == source_table and str(master_record.get(attr)) == str(attr_change['old_value']):
                    # Update this attribute
                    update_record[attr] = attr_change['new_value']
                    updated_attr_mapping.append({
                        'attribute_name': attr,
                        'attribute_value': str(attr_change['new_value']) if attr_change['new_value'] is not None else None,
                        'source': source_table
                    })
                    has_updates = True
                else:
                    # Keep existing value
                    update_record[attr] = master_record.get(attr)
                    updated_attr_mapping.append({
                        'attribute_name': attr,
                        'attribute_value': existing_attr.attribute_value,
                        'source': existing_attr.source
                    })
            else:
                # No change for this attribute, keep existing
                update_record[attr] = master_record.get(attr)
                if existing_attr:
                    updated_attr_mapping.append({
                        'attribute_name': attr,
                        'attribute_value': existing_attr.attribute_value,
                        'source': existing_attr.source
                    })
        
        if has_updates:
            # Regenerate attributes_combined after updates
            update_record['attributes_combined'] = generate_attributes_combined(update_record, attributes)
            
            # Add attributes_combined to attribute mapping
            updated_attr_mapping.append({
                'attribute_name': 'attributes_combined',
                'attribute_value': update_record['attributes_combined'],
                'source': source_table
            })
            
            all_master_updates.append(update_record)
            all_attribute_versions.append({
                id_key: lakefusion_id,
                'version': None,
                'attribute_source_mapping': updated_attr_mapping
            })
            # For records with merges, only log master_lakefusion_id
            all_merge_activities.append({
                f'master_{id_key}': lakefusion_id,
                id_key: '',
                'source': source_table,
                'version': None,
                'action_type': ActionType.SOURCE_UPDATE.value
            })
    
    logger.info(f"Prepared updates for {len([u for u in all_master_updates if u[id_key] in master_with_merges])} master records with merges")

# COMMAND ----------

# CASE 3: Process records that got merged into other masters
if got_merged_mapping:
    logger.info("CASE 3: Processing records that got merged into other masters...")
    
    for updated_lakefusion_id, master_lakefusion_id in got_merged_mapping.items():
        # Get changes for this record
        record_changes = [r for r in changed_records if r['lakefusion_id'] == updated_lakefusion_id]
        
        if not record_changes:
            continue
        
        source_table = record_changes[0]['source_table']
        
        # Get current MASTER record (the one this was merged into)
        master_record_df = spark.sql(f"""
            SELECT * FROM {master_table}
            WHERE {id_key} = '{master_lakefusion_id}'
        """)
        
        if master_record_df.count() == 0:
            logger.warning(f"No master record found for {master_lakefusion_id}")
            continue
        
        master_record = master_record_df.first().asDict()
        
        # Get current attribute version mapping of MASTER
        attr_version_df = spark.sql(f"""
            WITH RankedVersions AS (
                SELECT {id_key}, version, attribute_source_mapping,
                    ROW_NUMBER() OVER (PARTITION BY {id_key} ORDER BY version DESC) AS rn
                FROM {master_attribute_version_sources_table}
                WHERE {id_key} = '{master_lakefusion_id}'
            )
            SELECT {id_key}, version, attribute_source_mapping
            FROM RankedVersions
            WHERE rn = 1
        """)
        
        if attr_version_df.count() == 0:
            logger.warning(f"No attribute version found for master {master_lakefusion_id}")
            continue
        
        current_attr_mapping = attr_version_df.first()['attribute_source_mapping']
        
        # Prepare update record for MASTER
        update_record = {id_key: master_lakefusion_id}
        updated_attr_mapping = []
        has_updates = False
        
        # Process each attribute - only update if from same source and value matches
        for attr in entity_attributes:
            if attr == id_key:
                continue
            
            # Check if this attribute was changed
            attr_change = None
            for change in record_changes:
                if attr in change['changed_attributes']:
                    attr_change = change['changed_attributes'][attr]
                    break
            
            # Get current attribute source from MASTER
            existing_attr = next((a for a in current_attr_mapping if a.attribute_name == attr), None)
            
            if attr_change and existing_attr:
                # Check if this attribute is contributed by the updated source
                # AND if the current value matches the old value
                if existing_attr.source == source_table and str(master_record.get(attr)) == str(attr_change['old_value']):
                    # Update this attribute in MASTER
                    update_record[attr] = attr_change['new_value']
                    updated_attr_mapping.append({
                        'attribute_name': attr,
                        'attribute_value': str(attr_change['new_value']) if attr_change['new_value'] is not None else None,
                        'source': source_table
                    })
                    has_updates = True
                else:
                    # Keep existing value in MASTER
                    update_record[attr] = master_record.get(attr)
                    updated_attr_mapping.append({
                        'attribute_name': attr,
                        'attribute_value': existing_attr.attribute_value,
                        'source': existing_attr.source
                    })
            else:
                # No change for this attribute, keep existing
                update_record[attr] = master_record.get(attr)
                if existing_attr:
                    updated_attr_mapping.append({
                        'attribute_name': attr,
                        'attribute_value': existing_attr.attribute_value,
                        'source': existing_attr.source
                    })
        
        if has_updates:
            # Regenerate attributes_combined after updates
            update_record['attributes_combined'] = generate_attributes_combined(update_record, attributes)
            
            # Add attributes_combined to attribute mapping
            updated_attr_mapping.append({
                'attribute_name': 'attributes_combined',
                'attribute_value': update_record['attributes_combined'],
                'source': source_table
            })
            
            all_master_updates.append(update_record)
            all_attribute_versions.append({
                id_key: master_lakefusion_id,
                'version': None,
                'attribute_source_mapping': updated_attr_mapping
            })
            # For got merged records, log both the master and the merged record ID
            all_merge_activities.append({
                f'master_{id_key}': master_lakefusion_id,
                id_key: updated_lakefusion_id,
                'source': source_table,
                'version': None,
                'action_type': ActionType.SOURCE_UPDATE.value
            })
    
    logger.info(f"Prepared updates for {len(got_merged_mapping)} records that got merged")

logger.info(f"Total master updates prepared: {len(all_master_updates)}")

# COMMAND ----------

if not all_master_updates:
    dbutils.notebook.exit("No updates to apply after merge analysis")

# COMMAND ----------

logger.info("PREVIEW OF ALL PENDING CHANGES")
logger.info("-" * 80)

logger.info(f"1. MASTER TABLE UPDATES: {len(all_master_updates)}")
if all_master_updates:
    master_update_fields = [StructField(id_key, StringType(), False)]
    for attr in entity_attributes:
        if attr != id_key:
            master_update_fields.append(StructField(attr, StringType(), True))
    
    master_update_schema = StructType(master_update_fields)
    master_updates_preview_df = spark.createDataFrame(all_master_updates, schema=master_update_schema)
    logger.info("   Master records to be updated:")
    master_updates_preview_df.display()

logger.info(f"2. MERGE ACTIVITIES: {len(all_merge_activities)}")
logger.info(f"3. ATTRIBUTE VERSION SOURCES: {len(all_attribute_versions)}")

# COMMAND ----------

if all_master_updates:
    logger.info(f"Updating {len(all_master_updates)} master records...")
    
    # Create DataFrame for master updates
    master_update_fields = [StructField(id_key, StringType(), False)]
    for attr in entity_attributes:
        if attr != id_key:
            master_update_fields.append(StructField(attr, StringType(), True))
    # NEW: Add attributes_combined field
    master_update_fields.append(StructField('attributes_combined', StringType(), True))
    
    master_update_schema = StructType(master_update_fields)
    master_updates_df = spark.createDataFrame(all_master_updates, schema=master_update_schema)
    
    # Batch update master table
    master_delta_table = DeltaTable.forName(spark, master_table)
    
    # Include attributes_combined in the update set
    update_columns = [attr for attr in entity_attributes if attr in master_updates_df.columns and attr != id_key]
    if 'attributes_combined' in master_updates_df.columns:
        update_columns.append('attributes_combined')
    
    master_delta_table.alias("target").merge(
        source=master_updates_df.alias("source"),
        condition=f"target.{id_key} = source.{id_key}"
    ).whenMatchedUpdate(set={
        attr: f"source.{attr}" 
        for attr in update_columns
    }).execute()
    
    logger.info(f"Updated {len(all_master_updates)} master records")
    
    # Get final master version
    final_master_version = get_current_table_version(master_table)
    
    # Update version numbers in tracking arrays
    for activity in all_merge_activities:
        activity['version'] = final_master_version
        
    for attr_version in all_attribute_versions:
        attr_version['version'] = final_master_version
    
    logger.info(f"Master table version updated to: {final_master_version}")

    # Clear search_results for any records in BOTH unified and unified_deduplicate tables
    # that reference these updated master records
    # This ensures vector search will be recomputed for records that matched with updated records
    
    # Define both table names
    unified_deduplicate_table = f"{catalog_name}.silver.{entity}_unified_deduplicate"
    if experiment_id:
        unified_deduplicate_table += f"_{experiment_id}"
    
    tables_to_clear = [
        (unified_table, "unified"),
        (unified_deduplicate_table, "unified_deduplicate")
    ]
    
    logger.info("=" * 80)
    logger.info("CLEARING SEARCH RESULTS IN BOTH TABLES FOR UPDATED MASTER RECORDS")
    logger.info("=" * 80)
    
    updated_master_lakefusion_ids = [record[id_key] for record in all_master_updates]
    total_cleared_across_tables = 0
    
    for table_name, table_type in tables_to_clear:
        if not spark.catalog.tableExists(table_name):
            logger.warning(f"Table {table_name} does not exist, skipping...")
            continue

        logger.info(f"{'='*60}")
        logger.info(f"Processing {table_type} table: {table_name}")
        logger.info(f"{'='*60}")
        
        table_total_cleared = 0
        
        for lakefusion_id in updated_master_lakefusion_ids:
            try:
                # Find all records that have this lakefusion_id in their search_results
                records_with_references = spark.sql(f"""
                    SELECT {id_key}
                    FROM {table_name}
                    WHERE search_results LIKE '%{lakefusion_id}%'
                    AND {id_key} != '{lakefusion_id}'
                """).collect()
                
                if records_with_references:
                    referenced_ids = [row[id_key] for row in records_with_references]
                    referenced_ids_str = "','".join(referenced_ids)
                    
                    # Clear search_results for these records
                    spark.sql(f"""
                        UPDATE {table_name}
                        SET search_results = '',
                            scoring_results = ''
                        WHERE {id_key} IN ('{referenced_ids_str}')
                    """)
                    
                    table_total_cleared += len(referenced_ids)
                    logger.info(f"  Cleared {len(referenced_ids)} records that referenced {lakefusion_id}")

            except Exception as e:
                logger.error(f"  Error processing {lakefusion_id} in {table_type}: {str(e)}")
                continue
        
        if table_total_cleared > 0:
            logger.info(f"  Total records cleared in {table_type}: {table_total_cleared}")
            total_cleared_across_tables += table_total_cleared
        else:
            logger.info(f"  No additional records needed clearing in {table_type}")

    logger.info(f"{'='*80}")
    logger.info(f"TOTAL RECORDS CLEARED ACROSS ALL TABLES: {total_cleared_across_tables}")
    logger.info(f"{'='*80}")
else:
    logger.info("No master table updates to apply")

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
    
    # Batch insert using Delta merge
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

logger.info("UNIFIED TABLE UPDATES PROCESSING COMPLETED SUCCESSFULLY")
logger.info("-" * 80)
logger.info(f"Total changed records analyzed: {len(changed_records)}")
logger.info(f"Standalone updates: {len(standalone_ids)}")
logger.info(f"Master records with merges updated: {len(master_with_merges)}")
logger.info(f"Total master records updated: {len(all_master_updates)}")
logger.info(f"Total merge activities logged: {len(all_merge_activities)}")
logger.info(f"Total attribute versions logged: {len(all_attribute_versions)}")
if 'final_master_version' in locals():
    logger.info(f"Final master table version: {final_master_version}")
