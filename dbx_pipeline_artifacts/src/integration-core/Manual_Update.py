# Databricks notebook source
import json
from pyspark.sql.functions import (
    col, lit, current_timestamp, coalesce, max as spark_max,
    from_json, explode, get_json_object, array_contains, expr
)
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

# Widget parameters
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("entity_name", "", "Entity Name")
dbutils.widgets.text("lakefusion_id", "", "LakeFusion ID")
dbutils.widgets.text("update_attributes", "{}", "Update Attributes JSON")
dbutils.widgets.text("entity_attributes", "[]", "Entity Attributes JSON")

# COMMAND ----------

# Get parameters
catalog_name = dbutils.widgets.get("catalog_name")
entity_name = dbutils.widgets.get("entity_name")
lakefusion_id = dbutils.widgets.get("lakefusion_id")
update_attributes = json.loads(dbutils.widgets.get("update_attributes"))
entity_attributes = json.loads(dbutils.widgets.get("entity_attributes"))

# COMMAND ----------

# MAGIC %run ../utils/execute_utils

# COMMAND ----------

# Define table names
master_table = f"{catalog_name}.gold.{entity_name}_master_prod"
merge_activities_table = f"{catalog_name}.gold.{entity_name}_master_prod_merge_activities"
attr_version_sources_table = f"{catalog_name}.gold.{entity_name}_master_prod_attribute_version_sources"
unified_table = f"{catalog_name}.silver.{entity_name}_unified_prod"

# Source identifier for manual updates
MANUAL_UPDATE_SOURCE = "MANUAL_UPDATE"

logger.info("="*80)
logger.info("MANUAL UPDATE ENTITY PROFILE")
logger.info("="*80)
logger.info(f"Entity: {entity_name}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"LakeFusion ID: {lakefusion_id}")
logger.info(f"Attributes to update: {list(update_attributes.keys())}")
logger.info(f"Entity attributes: {entity_attributes}")
logger.info("")
logger.info(f"Master Table: {master_table}")
logger.info(f"Merge Activities Table: {merge_activities_table}")
logger.info(f"Attribute Version Sources Table: {attr_version_sources_table}")
logger.info(f"Unified Table: {unified_table}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 1: Getting current master record")
logger.info("="*80)

# Get current master record
current_master_df = spark.sql(f"""
    SELECT *
    FROM {master_table}
    WHERE lakefusion_id = '{lakefusion_id}'
""")

if current_master_df.count() == 0:
    error_msg = f"Master record not found for lakefusion_id: {lakefusion_id}"
    logger.error(f"ERROR: {error_msg}")
    dbutils.notebook.exit(json.dumps({
        "status": "error",
        "error": error_msg
    }))

current_master = current_master_df.collect()[0]
logger.info(f"Found master record for lakefusion_id: {lakefusion_id}")

# Store current attribute values for comparison
current_attr_values = {attr: current_master[attr] for attr in entity_attributes if attr in current_master.asDict()}
logger.info(f"\nCurrent attribute values:")
for attr, val in current_attr_values.items():
    if attr in update_attributes:
        logger.info(f"  - {attr}: '{val}' → '{update_attributes[attr]}' (UPDATING)")
    else:
        logger.info(f"  - {attr}: '{val}'")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 2: Getting current attribute source mapping")
logger.info("="*80)

# Get the latest attribute_source_mapping for this record
current_mapping_df = spark.sql(f"""
    SELECT attribute_source_mapping, version
    FROM {attr_version_sources_table}
    WHERE lakefusion_id = '{lakefusion_id}'
    ORDER BY version DESC
    LIMIT 1
""")

existing_attrs_dict = {}
if current_mapping_df.count() > 0:
    mapping_row = current_mapping_df.collect()[0]
    current_mapping = mapping_row['attribute_source_mapping']
    mapping_version = mapping_row['version']
    
    if current_mapping:
        # Handle both cases: already a list OR a JSON string
        if isinstance(current_mapping, str):
            existing_attrs = json.loads(current_mapping)
        elif isinstance(current_mapping, list):
            # Already a list (Spark returned native Python list)
            # Convert Row objects to dicts if needed
            existing_attrs = []
            for item in current_mapping:
                if hasattr(item, 'asDict'):
                    existing_attrs.append(item.asDict())
                elif isinstance(item, dict):
                    existing_attrs.append(item)
                else:
                    # Try to convert to dict
                    existing_attrs.append(dict(item) if item else {})
        else:
            existing_attrs = []
            logger.warning(f"Unexpected mapping type: {type(current_mapping)}")
        
        existing_attrs_dict = {attr['attribute_name']: attr for attr in existing_attrs if attr and 'attribute_name' in attr}
        logger.info(f"Found existing attribute source mapping (version {mapping_version})")
        logger.info(f"  Attributes tracked: {list(existing_attrs_dict.keys())}")
else:
    logger.info("No existing attribute source mapping found")
    logger.info("   Will create initial mapping from current master values")
    # Initialize from current master values
    for attr in entity_attributes:
        if attr in current_attr_values and current_attr_values[attr] is not None:
            existing_attrs_dict[attr] = {
                "attribute_name": attr,
                "attribute_value": str(current_attr_values[attr]),
                "source": "INITIAL_LOAD"
            }

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 3: Updating master table")
logger.info("="*80)

# Build the SET clause for the update
set_clauses = []
for attr_name, attr_value in update_attributes.items():
    if attr_value is None:
        set_clauses.append(f"{attr_name} = NULL")
    else:
        escaped_value = str(attr_value).replace("'", "''")
        set_clauses.append(f"{attr_name} = '{escaped_value}'")

if set_clauses:
    set_clause = ", ".join(set_clauses)
    
    update_master_query = f"""
        UPDATE {master_table}
        SET {set_clause}
        WHERE lakefusion_id = '{lakefusion_id}'
    """
    
    logger.info(f"Executing update query...")
    logger.info(f"  SET: {set_clause[:200]}..." if len(set_clause) > 200 else f"  SET: {set_clause}")
    spark.sql(update_master_query)
    logger.info(f"Master table updated successfully")
    
    # Update attributes_combined
    updated_record = spark.sql(f"""
        SELECT * FROM {master_table} WHERE lakefusion_id = '{lakefusion_id}'
    """).collect()
    
    if updated_record:
        attr_values = []
        for attr in entity_attributes:
            if attr in updated_record[0].asDict():
                val = updated_record[0][attr]
                attr_values.append(str(val) if val is not None else '')
        
        attributes_combined = " | ".join(attr_values)
        attributes_combined_escaped = attributes_combined.replace("'", "''")
        
        spark.sql(f"""
            UPDATE {master_table}
            SET attributes_combined = '{attributes_combined_escaped}'
            WHERE lakefusion_id = '{lakefusion_id}'
        """)
        logger.info(f"attributes_combined updated")
else:
    logger.warning("No attributes to update in master table")

# Now get the version to use for attr_version_sources and merge_activities
# This is max existing version + 1
version_df = spark.sql(f"""
    SELECT COALESCE(MAX(version), 0) + 1 as new_version
    FROM {attr_version_sources_table}
    WHERE lakefusion_id = '{lakefusion_id}'
""")
new_version = version_df.collect()[0]['new_version']
logger.info(f"\nNew version for attr_version_sources and merge_activities: {new_version}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 4: Building and inserting new attribute source mapping")
logger.info("="*80)

# Build new mapping - only update the changed attributes to MANUAL_UPDATE
new_attribute_mapping = []
updated_attrs = []

for attr_name in entity_attributes:
    if attr_name in update_attributes:
        new_value = update_attributes[attr_name]
        new_attribute_mapping.append({
            "attribute_name": attr_name,
            "attribute_value": str(new_value) if new_value is not None else None,
            "source": MANUAL_UPDATE_SOURCE
        })
        updated_attrs.append(attr_name)
        logger.info(f"  {attr_name}: Updated to '{new_value}' (source: {MANUAL_UPDATE_SOURCE})")
    elif attr_name in existing_attrs_dict:
        new_attribute_mapping.append(existing_attrs_dict[attr_name])
        logger.info(f"  - {attr_name}: Unchanged (source: {existing_attrs_dict[attr_name].get('source', 'unknown')})")
    elif attr_name in current_attr_values and current_attr_values[attr_name] is not None:
        new_attribute_mapping.append({
            "attribute_name": attr_name,
            "attribute_value": str(current_attr_values[attr_name]),
            "source": "INITIAL_LOAD"
        })
        logger.info(f"  + {attr_name}: Added from master (source: INITIAL_LOAD)")

# Convert to SQL ARRAY of STRUCTs (not JSON string)
def build_sql_array_of_structs(mapping_list):
    """Convert list of dicts to SQL array of named_struct"""
    if not mapping_list:
        return "array()"
    
    struct_parts = []
    for item in mapping_list:
        attr_name = str(item.get('attribute_name', '')).replace("'", "''")
        attr_value = item.get('attribute_value')
        if attr_value is None:
            attr_value_sql = "NULL"
        else:
            attr_value_sql = "'" + str(attr_value).replace("'", "''") + "'"
        source = str(item.get('source', '')).replace("'", "''")
        
        struct_parts.append(
            f"named_struct('attribute_name', '{attr_name}', 'attribute_value', {attr_value_sql}, 'source', '{source}')"
        )
    
    return f"array({', '.join(struct_parts)})"

mapping_sql = build_sql_array_of_structs(new_attribute_mapping)

insert_attr_version_query = f"""
    INSERT INTO {attr_version_sources_table}
    (lakefusion_id, version, attribute_source_mapping)
    VALUES (
        '{lakefusion_id}',
        {new_version},
        {mapping_sql}
    )
"""

spark.sql(insert_attr_version_query)
logger.info(f"\nAttribute version sources updated")
logger.info(f"  - lakefusion_id: {lakefusion_id}")
logger.info(f"  - version: {new_version}")
logger.info(f"  - Updated attributes: {updated_attrs}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 5: Inserting into merge activities")
logger.info("="*80)

insert_merge_activities_query = f"""
    INSERT INTO {merge_activities_table}
    (master_id, match_id, source, version, action_type, created_at)
    VALUES (
        '{lakefusion_id}',
        NULL,
        '{MANUAL_UPDATE_SOURCE}',
        {new_version},
        'MANUAL_UPDATE',
        current_timestamp()
    )
"""

spark.sql(insert_merge_activities_query)
logger.info(f"Merge activities updated")
logger.info(f"  - master_id: {lakefusion_id}")
logger.info(f"  - action_type: MANUAL_UPDATE")
logger.info(f"  - version: {new_version}")
logger.info(f"  - source: {MANUAL_UPDATE_SOURCE}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 6: Clearing stale search results in unified table")
logger.info("="*80)

# Find all ACTIVE records that have this lakefusion_id in their scoring_results
# The scoring_results is a JSON array string, we need to check if it contains this lakefusion_id

# First, let's see how many records are affected
affected_records_query = f"""
    SELECT surrogate_key, source_path, source_id, scoring_results
    FROM {unified_table}
    WHERE record_status = 'ACTIVE'
      AND scoring_results IS NOT NULL
      AND scoring_results != ''
      AND scoring_results != '[]'
      AND scoring_results LIKE '%{lakefusion_id}%'
"""

affected_df = spark.sql(affected_records_query)
affected_count = affected_df.count()

logger.info(f"Found {affected_count} ACTIVE records with lakefusion_id '{lakefusion_id}' in scoring_results")

if affected_count > 0:
    # Show affected records (limited)
    logger.info("\nAffected records (first 10):")
    affected_df.select("surrogate_key", "source_path", "source_id").show(10, truncate=False)
    
    # Update these records to clear search_results and scoring_results
    # This ensures they will be re-evaluated with the updated master data
    clear_results_query = f"""
        UPDATE {unified_table}
        SET search_results = NULL,
            scoring_results = NULL
        WHERE record_status = 'ACTIVE'
          AND scoring_results IS NOT NULL
          AND scoring_results != ''
          AND scoring_results != '[]'
          AND scoring_results LIKE '%{lakefusion_id}%'
    """
    
    spark.sql(clear_results_query)
    logger.info(f"\nCleared search_results and scoring_results for {affected_count} ACTIVE records")
    logger.info("  These records will be re-evaluated in the next entity matching run")
else:
    logger.info("No ACTIVE records found with this lakefusion_id in scoring_results")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("MANUAL UPDATE COMPLETED SUCCESSFULLY")
logger.info("="*80)

# Get the final state of the master record for verification
final_master = spark.sql(f"""
    SELECT * FROM {master_table} WHERE lakefusion_id = '{lakefusion_id}'
""").collect()[0]

logger.info(f"""
Summary:
--------
Entity: {entity_name}
LakeFusion ID: {lakefusion_id}

Updates performed:
1. Master table
   - Updated attributes: {list(update_attributes.keys())}

2. Attribute version sources
   - New version {new_version} created
   - Only updated attributes marked with source='{MANUAL_UPDATE_SOURCE}'
   - Other attributes retain their original sources

3. Merge activities
   - MANUAL_UPDATE action recorded at version {new_version}

4. Unified table
   - Cleared stale search/scoring results for {affected_count} ACTIVE records
   - These records will be re-matched in the next entity matching run

Updated values:
""")

for attr in update_attributes.keys():
    old_val = current_attr_values.get(attr, 'N/A')
    new_val = final_master[attr] if attr in final_master.asDict() else 'N/A'
    logger.info(f"  - {attr}: '{old_val}' → '{new_val}'")

# COMMAND ----------

logger_instance.shutdown()
