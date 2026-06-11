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
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("meta_info_table", "", "Meta Info Table")
dbutils.widgets.text("attributes", "", "Merged Description Attributes")

# COMMAND ----------

entity = dbutils.widgets.get("entity")
primary_table = dbutils.widgets.get("primary_table")
source_id_key = dbutils.widgets.get("source_id_key")
id_key = dbutils.widgets.get("id_key")
entity_attributes = dbutils.widgets.get("entity_attributes")
attributes_mapping = dbutils.widgets.get("attributes_mapping")
experiment_id = dbutils.widgets.get("experiment_id")
catalog_name = dbutils.widgets.get("catalog_name")
meta_info_table = dbutils.widgets.get("meta_info_table")
attributes = dbutils.widgets.get("attributes")

# COMMAND ----------

# Get values from task values
entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
primary_table = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "primary_table", debugValue=primary_table)
source_id_key = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "primary_key", debugValue=source_id_key)
id_key = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "id_key", debugValue=id_key)
entity_attributes = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes", debugValue=entity_attributes)
attributes_mapping = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "attributes_mapping", debugValue=attributes_mapping)
meta_info_table = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "meta_info_table", debugValue=meta_info_table)
attributes = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="match_attributes", debugValue=attributes)

# COMMAND ----------

# Parse JSON parameters
entity_attributes = json.loads(entity_attributes)
attributes_mapping = json.loads(attributes_mapping)
attributes = json.loads(attributes)

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

logger.info(f"Source column for ID key: {source_column_for_id}")
logger.info(f"Primary table mapping: {primary_table_mapping}")

# COMMAND ----------

# MAGIC %run ../utils/match_merge_utils

# COMMAND ----------

import logging
if 'logger' not in dir():
    logger = logging.getLogger(__name__)

# COMMAND ----------

# Define table names
master_table = f"{catalog_name}.gold.{entity}_master"
processed_unified_deduplicate_table = f"{catalog_name}.silver.{entity}_processed_unified_deduplicate"
unified_deduplicate_table = f"{catalog_name}.silver.{entity}_unified_deduplicate"
if experiment_id:
    master_table += f"_{experiment_id}"
    processed_unified_deduplicate_table += f"_{experiment_id}"
    unified_deduplicate_table += f"_{experiment_id}"

merge_activities_table = f"{master_table}_merge_activities"
master_attribute_version_sources_table = f"{master_table}_attribute_version_sources"

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

# Get CDF changes (updates only) from primary table
def get_cdf_updates(table_name, from_version, to_version):
    try:
        cdf_query = f"""
        SELECT *, _change_type, _commit_version, _commit_timestamp
        FROM table_changes('{table_name}', {from_version}, {to_version})
        WHERE _change_type IN ('update_preimage', 'update_postimage')
        ORDER BY {source_column_for_id}, _commit_version, _change_type
        """
        return spark.sql(cdf_query)
    except Exception as e:
        logger.error(f"Error getting CDF changes: {str(e)}")
        return spark.createDataFrame([], StructType([]))

cdf_changes = get_cdf_updates(primary_table, starting_version, current_version)
logger.info(f"CDF changes count: {cdf_changes.count()}")

if cdf_changes.count() == 0:
    logger.info("No updates found in primary table since last processing")
    dbutils.notebook.exit("No updates to process")

# COMMAND ----------

# Separate pre and post images
pre_images = cdf_changes.filter(col("_change_type") == "update_preimage").drop("_change_type")
post_images = cdf_changes.filter(col("_change_type") == "update_postimage").drop("_change_type")

logger.info(f"Pre-images count: {pre_images.count()}")
logger.info(f"Post-images count: {post_images.count()}")

# COMMAND ----------

pre_columns = [col(f"pre.{c}").alias(f"pre_{c}") for c in pre_images.columns if c not in ["_commit_version", "_commit_timestamp"]]
post_columns = [col(f"post.{c}").alias(f"post_{c}") for c in post_images.columns if c not in ["_commit_version", "_commit_timestamp"]]

joined_changes = pre_images.alias("pre").join(
    post_images.alias("post"),
    on=[source_column_for_id, "_commit_version", "_commit_timestamp"],
    how="inner"
).select(
    col(f"pre.{source_column_for_id}").alias(source_column_for_id),
    col("pre._commit_version").alias("_commit_version"),
    col("pre._commit_timestamp").alias("_commit_timestamp"),
    *pre_columns,
    *post_columns
)

logger.info(f"Joined changes count: {joined_changes.count()}")

# COMMAND ----------

def identify_changed_attributes_simple(pre_post_df, mapping):
    """
    Identify which attributes actually changed between pre and post images
    """
    changed_records = []
    
    for row in pre_post_df.collect():
        # Access aliased columns correctly
        source_id = getattr(row, f"{source_column_for_id}")  # This should work since we're selecting with alias
        commit_version = getattr(row, "_commit_version")
        
        changed_attributes = {}
        
        # Compare each mapped attribute
        for source_col, entity_col in mapping.items():
            try:
                # Get pre and post values using getattr for nested struct access
                pre_col_name = f"pre_{source_col}"
                post_col_name = f"post_{source_col}"
                
                if hasattr(row, pre_col_name) and hasattr(row, post_col_name):
                    pre_value = getattr(row, pre_col_name)
                    post_value = getattr(row, post_col_name)
                    
                    # Check if values are different (handling nulls)
                    if pre_value != post_value:
                        changed_attributes[entity_col] = {
                            'old_value': pre_value,
                            'new_value': post_value,
                            'source_column': source_col
                        }
            except AttributeError:
                continue
        
        if changed_attributes:
            changed_records.append({
                'source_id': source_id,
                'commit_version': commit_version,
                'changed_attributes': changed_attributes
            })
    
    return changed_records

# Use the simple approach with corrected join
changed_records = identify_changed_attributes_simple(joined_changes, primary_table_mapping)
logger.info(f"Records with actual changes: {len(changed_records)}")

if not changed_records:
    logger.info("No actual attribute changes detected")
    dbutils.notebook.exit("No attribute changes to process")

# COMMAND ----------

changed_records_data = []
for record in changed_records:
    for attr_name, attr_info in record['changed_attributes'].items():
        old_val = attr_info['old_value']
        new_val = attr_info['new_value']
        
        changed_records_data.append({
            'source_id': record['source_id'],
            'commit_version': record['commit_version'],
            'attribute_name': attr_name,
            'old_value': old_val,
            'new_value': new_val,
            'source_column': attr_info['source_column']
        })

if not changed_records_data:
    logger.info("No changed records data to process")
    dbutils.notebook.exit("No attribute changes to process")
logger.info(f"1324343 {changed_records_data}")
import pandas as pd
for row in changed_records_data:
    row["old_value"] = str(row["old_value"])
    row["new_value"] = str(row["new_value"])
pandas_df = pd.DataFrame(changed_records_data)

# Convert to Spark DataFrame
changed_records_df = spark.createDataFrame(pandas_df)
logger.info(f"Changed records DataFrame count: {changed_records_df.count()}")

# COMMAND ----------

# Get master records that need to be updated
source_ids = [record['source_id'] for record in changed_records]

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

formatted_source_ids = [format_sql_value(sid) for sid in source_ids]
source_ids_str = ",".join(formatted_source_ids)

master_records_query = f"""
SELECT * 
FROM {master_table}
WHERE {source_id_key} IN ({source_ids_str})
"""

master_records_df = spark.sql(master_records_query)
logger.info(f"Master records to update count: {master_records_df.count()}")

if master_records_df.count() == 0:
    logger.info("No corresponding master records found")
    dbutils.notebook.exit("No master records to update")

# COMMAND ----------

# Get current attribute version mapping for affected records
lakefusion_ids = [row[id_key] for row in master_records_df.collect()]
lakefusion_ids_str = "','".join(lakefusion_ids)

current_attr_version_query = f"""
WITH RankedVersions AS (
    SELECT {id_key}, version, attribute_source_mapping,
        ROW_NUMBER() OVER (PARTITION BY {id_key} ORDER BY version DESC) AS rn
    FROM {master_attribute_version_sources_table}
    WHERE {id_key} IN ('{lakefusion_ids_str}')
)
SELECT {id_key}, version, attribute_source_mapping
FROM RankedVersions
WHERE rn = 1
"""

current_attr_versions_df = spark.sql(current_attr_version_query)
logger.info(f"Current attribute versions count: {current_attr_versions_df.count()}")

# COMMAND ----------

# Process updates for each master record
def process_master_record_updates():
    """
    Process updates for master records by checking attribute sources and updating accordingly
    """
    updates_to_apply = []
    
    for master_row in master_records_df.collect():
        lakefusion_id = master_row[id_key]
        source_id = master_row[source_id_key]
        
        # Get current attribute version mapping for this record
        attr_version_row = current_attr_versions_df.filter(col(id_key) == lakefusion_id).collect()
        if not attr_version_row:
            logger.warning(f"No attribute version mapping found for lakefusion_id: {lakefusion_id}")
            continue
            
        current_attr_mapping = attr_version_row[0]['attribute_source_mapping']
        
        # Get changes for this record
        record_changes = [r for r in changed_records if r['source_id'] == source_id]
        
        if not record_changes:
            continue
            
        record_updates = {}
        updated_attributes = []
        
        for change in record_changes:
            for attr_name, attr_info in change['changed_attributes'].items():
                # Check if this attribute's source matches the primary table
                attr_source = None
                for attr_map in current_attr_mapping:
                    if attr_map.attribute_name == attr_name:
                        attr_source = attr_map.source
                        break
                
                # Update only if source matches primary table or if no source found (new attribute)
                if not attr_source or attr_source == primary_table:
                    record_updates[attr_name] = attr_info['new_value']
                    updated_attributes.append({
                        'attribute_name': attr_name,
                        'new_value': attr_info['new_value'],
                        'source': primary_table
                    })
        
        if record_updates:
            updates_to_apply.append({
                'lakefusion_id': lakefusion_id,
                'source_id': source_id,
                'updates': record_updates,
                'updated_attributes': updated_attributes,
                'current_attr_mapping': current_attr_mapping
            })
    
    return updates_to_apply

updates_to_apply = process_master_record_updates()
logger.info(f"Records to update: {len(updates_to_apply)}")

if not updates_to_apply:
    logger.info("No updates to apply to master records")
    dbutils.notebook.exit("No updates to apply")

# COMMAND ----------

updates_to_apply

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

# Create DataFrame for batch update of master table
master_updates_data = []
for update in updates_to_apply:
    update_record = {'lakefusion_id': update['lakefusion_id']}
    
    # Get current master record
    current_master = master_records_df.filter(col(id_key) == update['lakefusion_id']).collect()[0]
    
    # Start with current values
    for attr in entity_attributes:
        if hasattr(current_master, attr):
            update_record[attr] = getattr(current_master, attr)
    
    # Apply updates
    for attr_name, new_value in update['updates'].items():
        update_record[attr_name] = new_value
    
    # Generate attributes_combined from the updated record
    update_record['attributes_combined'] = generate_attributes_combined(update_record, attributes)
    
    master_updates_data.append(update_record)

# Create DataFrame with explicit schema matching master table
master_schema = master_records_df.schema
rows_data = []

for update_record in master_updates_data:
    row_data = []
    for field in master_schema.fields:
        field_name = field.name
        
        if field_name in update_record:
            value = update_record[field_name]
            
            # Type conversion based on schema
            if isinstance(field.dataType, StringType):
                row_data.append(str(value) if value is not None else None)
            elif isinstance(field.dataType, IntegerType):
                try:
                    row_data.append(int(float(value)) if value is not None else None)
                except (ValueError, TypeError):
                    row_data.append(None)
            elif isinstance(field.dataType, DoubleType):
                try:
                    row_data.append(float(value) if value is not None else None)
                except (ValueError, TypeError):
                    row_data.append(None)
            else:
                row_data.append(value)
        else:
            row_data.append(None)
    
    rows_data.append(tuple(row_data))

master_updates_df = spark.createDataFrame(rows_data, master_schema)
logger.info(f"Master updates DataFrame count: {master_updates_df.count()}")

# COMMAND ----------

# Clear search and scoring results for updated records in BOTH unified and unified_deduplicate tables
# Also clear search_results for any records that reference these updated master records
if updates_to_apply:
    # Define both table names
    unified_table = f"{catalog_name}.silver.{entity}_unified"
    if experiment_id:
        unified_table += f"_{experiment_id}"
    
    tables_to_clear = [
        (unified_table, "unified"),
        (unified_deduplicate_table, "unified_deduplicate")
    ]
    
    logger.info("=" * 80)
    logger.info("CLEARING SEARCH RESULTS IN BOTH TABLES")
    logger.info("=" * 80)
    
    # Get lakefusion_ids that were updated
    updated_lakefusion_ids = [update['lakefusion_id'] for update in updates_to_apply]
    
    total_cleared_across_tables = 0
    
    for table_name, table_type in tables_to_clear:
        if not spark.catalog.tableExists(table_name):
            logger.warning(f"Table {table_name} does not exist, skipping...")
            continue
        
        logger.info("=" * 60)
        logger.info(f"Processing {table_type} table: {table_name}")
        logger.info("=" * 60)
        
        lakefusion_ids_str = "','".join(updated_lakefusion_ids)
        
        # Step 1: Clear search_results and scoring_results for the updated records themselves
        spark.sql(f"""
            UPDATE {table_name}
            SET search_results = '',
                scoring_results = ''
            WHERE {id_key} IN ('{lakefusion_ids_str}')
        """)
        
        logger.info(f"  Cleared search_results for {len(updated_lakefusion_ids)} updated records")
        
        # Step 2: Clear search_results for any records that have these lakefusion_ids in their search_results
        # This ensures vector search will be recomputed for records that matched with updated records
        table_total_cleared = 0
        
        for lakefusion_id in updated_lakefusion_ids:
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
                    if len(referenced_ids) <= 5:
                        logger.info(f"    Referenced records: {referenced_ids}")
                    else:
                        logger.info(f"    Referenced records: {referenced_ids[:5]}... and {len(referenced_ids)-5} more")
                        
            except Exception as e:
                logger.error(f"  Error processing {lakefusion_id} in {table_type}: {str(e)}")
                continue
        
        if table_total_cleared > 0:
            logger.info(f"  Total records cleared in {table_type}: {table_total_cleared}")
            total_cleared_across_tables += table_total_cleared
        else:
            logger.info(f"  No additional records needed clearing in {table_type}")
    
    logger.info("=" * 80)
    logger.info(f"TOTAL RECORDS CLEARED ACROSS ALL TABLES: {total_cleared_across_tables}")
    logger.info("=" * 80)

# COMMAND ----------

# Remove updated records from processed_unified_deduplicate
if updates_to_apply:
    if spark.catalog.tableExists(processed_unified_deduplicate_table):
        updated_lakefusion_ids = [update['lakefusion_id'] for update in updates_to_apply]
        lakefusion_ids_str = "','".join(updated_lakefusion_ids)
        
        # Delete records to force LLM re-processing
        delete_count = spark.sql(f"""
            DELETE FROM {processed_unified_deduplicate_table}
            WHERE {id_key} IN ('{lakefusion_ids_str}')
        """)
        
        logger.info(f"Removed {len(updated_lakefusion_ids)} records from processed_unified_deduplicate table")
        logger.info(f"  These records will be re-processed by LLM matching")
    else:
        logger.info(f"Note: {processed_unified_deduplicate_table} does not exist yet")

# COMMAND ----------

# Batch update master table
master_delta_table = DeltaTable.forName(spark, master_table)

# Build update set including all entity attributes plus attributes_combined
update_columns = [attr for attr in entity_attributes if attr in master_updates_df.columns and attr != id_key]

# Add attributes_combined to the update set if it exists in the DataFrame
if 'attributes_combined' in master_updates_df.columns:
    update_columns.append('attributes_combined')

# Create the update set dictionary
update_set = {attr: f"source.{attr}" for attr in update_columns}

logger.info(f"Columns to update: {update_columns}")

# COMMAND ----------

master_delta_table.alias("target").merge(
    source=master_updates_df.alias("source"),
    condition=f"target.{id_key} = source.lakefusion_id"
).whenMatchedUpdate(
    set=update_set
).execute()

logger.info("Master table updated successfully")

# COMMAND ----------

# Get current master table version after updates
updated_master_version = get_current_table_version(master_table)
logger.info(f"Master table version after updates: {updated_master_version}")

# COMMAND ----------

# Prepare merge activities updates
merge_activities_data = []
for update in updates_to_apply:
    merge_activities_data.append({
        'master_lakefusion_id': update['lakefusion_id'],
        'lakefusion_id': '',  # Empty for primary source updates
        'source': primary_table,
        'version': updated_master_version,
        'action_type': ActionType.MANUAL_UPDATE.value
    })

# COMMAND ----------

merge_activities_data

# COMMAND ----------

if merge_activities_data:
    insert_values = []
    for record in merge_activities_data:
        insert_values.append(
            f"('{record['master_lakefusion_id']}', '{record['lakefusion_id']}', '{record['source']}', {record['version']}, '{record['action_type']}')"
        )
    
    insert_sql = f"""
    INSERT INTO {merge_activities_table} 
    (master_lakefusion_id, lakefusion_id, source, version, action_type)
    VALUES {', '.join(insert_values)}
    """
    
    spark.sql(insert_sql)
    logger.info("Merge activities updated successfully")

# COMMAND ----------

# Prepare attribute version sources updates
attr_version_data = []
for update in updates_to_apply:
    # Create new attribute source mapping
    new_attr_mapping = []
    
    # Get the corresponding update record to access attributes_combined
    corresponding_update = next((u for u in master_updates_data if u['lakefusion_id'] == update['lakefusion_id']), None)
    
    # Start with current mapping
    for attr_map in update['current_attr_mapping']:
        attr_name = attr_map.attribute_name
        
        # Skip attributes_combined as we'll add it at the end
        if attr_name == 'attributes_combined':
            continue
        
        # Check if this attribute was updated
        updated_attr = next((ua for ua in update['updated_attributes'] if ua['attribute_name'] == attr_name), None)
        
        if updated_attr:
            # Use new value and source
            new_attr_mapping.append({
                'attribute_name': attr_name,
                'attribute_value': str(updated_attr['new_value']) if updated_attr['new_value'] is not None else None,
                'source': updated_attr['source']
            })
        else:
            # Keep existing mapping
            new_attr_mapping.append({
                'attribute_name': attr_name,
                'attribute_value': attr_map.attribute_value,
                'source': attr_map.source
            })
    
    # Add attributes_combined to the mapping
    if corresponding_update and 'attributes_combined' in corresponding_update:
        new_attr_mapping.append({
            'attribute_name': 'attributes_combined',
            'attribute_value': corresponding_update['attributes_combined'],
            'source': primary_table
        })
    
    attr_version_data.append({
        'lakefusion_id': update['lakefusion_id'],
        'version': updated_master_version,
        'attribute_source_mapping': new_attr_mapping
    })

# COMMAND ----------

attr_version_data

# COMMAND ----------

# Batch insert into attribute version sources
if attr_version_data:
    for record in attr_version_data:
        # Build SQL for attribute source mapping
        attr_structs = []
        for attr_map in record['attribute_source_mapping']:
            attr_name = attr_map['attribute_name']
            
            if attr_map['attribute_value']:
                escaped_value = str(attr_map['attribute_value']).replace("'", "''")
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
        VALUES ('{record['lakefusion_id']}', {record['version']}, {attr_array})
        """
        
        spark.sql(insert_sql)
    
    logger.info("Attribute version sources updated successfully")

# COMMAND ----------

# Summary
logger.info("=== UPDATE PROCESSING SUMMARY ===")
logger.info(f"Entity: {entity}")
logger.info(f"Primary table: {primary_table}")
logger.info(f"Processed version range: {last_processed_version} to {current_version}")
logger.info(f"Records with changes: {len(changed_records)}")
logger.info(f"Master records updated: {len(updates_to_apply)}")
logger.info(f"New master table version: {updated_master_version}")

# COMMAND ----------

logger.info("Primary source updates processing completed successfully")
