# Databricks notebook source
import json

# COMMAND ----------

dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")
dbutils.widgets.text("id_key", "", "lakefusion Primary Key")
dbutils.widgets.text("primary_key", "", "Primary Key")
dbutils.widgets.text("attributes_mapping_json", "{}", "Attributes Mapping (JSON)")
dbutils.widgets.text("dataset_objects", "{}", "Dataset Objects")
dbutils.widgets.text("primary_table", "", "Primary Table")

# COMMAND ----------

entity = dbutils.widgets.get("entity")
catalog_name = dbutils.widgets.get("catalog_name")
id_key = dbutils.widgets.get("id_key")
source_id_key = dbutils.widgets.get("primary_key")
entity_attributes = dbutils.widgets.get("entity_attributes")
primary_table = dbutils.widgets.get("primary_table")

# COMMAND ----------

entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
source_id_key = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "primary_key", debugValue=source_id_key)
id_key = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "id_key", debugValue=id_key)
entity_attributes = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes", debugValue=entity_attributes)
primary_table = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "primary_table", debugValue=primary_table)
experiment_id = "prod"

# COMMAND ----------

# MAGIC %run ../utils/execute_utils

# COMMAND ----------

# Parse entity attributes and additional parameters
if entity_attributes:
    entity_attributes = json.loads(entity_attributes)
else:
    logger.info("No entity attributes provided")

# Get additional parameters for mapping
attributes_mapping_json = dbutils.widgets.get("attributes_mapping_json")
dataset_objects = dbutils.widgets.get("dataset_objects")

# Get from task values if available
attributes_mapping_json = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "attributes_mapping", debugValue=attributes_mapping_json)
dataset_objects = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "dataset_objects", debugValue=dataset_objects)

# Parse the mapping and dataset information
if attributes_mapping_json:
    attributes_mapping = json.loads(attributes_mapping_json)
else:
    attributes_mapping = []

if dataset_objects:
    dataset_objects_dict = json.loads(dataset_objects)
else:
    dataset_objects_dict = {}

logger.info(f"Creating crosswalk view for entity: {entity}")

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

logger.info(f"Table Names:")
logger.info(f"Unified Table: {unified_table}")
logger.info(f"Master Table: {master_table}")
logger.info(f"Merge Activities Table: {merge_activities_table}")
logger.info(f"Attribute Version Sources Table: {master_attribute_version_sources_table}")
logger.info(f"Primary Table: {primary_table}")

# COMMAND ----------

# Function to check if unified table exists
def table_exists(table_name):
    """Check if a table exists"""
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}")
        return True
    except Exception:
        return False

# Check if unified table exists
unified_table_exists = table_exists(unified_table)
logger.info(f"Unified table exists: {unified_table_exists}")

# COMMAND ----------

# Function to create source mapping lookup with source column detection
def create_source_mapping_with_columns(attributes_mapping, dataset_objects_dict, source_id_key):
    """
    Create a mapping of source table paths to their metadata including source column names
    """
    source_mapping = {}
    
    for mapping_item in attributes_mapping:
        for source_table_path, field_mapping in mapping_item.items():
            dataset_info = dataset_objects_dict.get(source_table_path, {})
            dataset_name = dataset_info.get('name', source_table_path)

            source_id_column = None
            for source_col, master_col in field_mapping.items():
                if master_col == source_id_key:
                    source_id_column = source_col
                    break
            
            source_mapping[source_table_path] = {
                'dataset_name': dataset_name,
                'source_id_column': source_id_column or source_id_key,
                'table_path': source_table_path,
                'is_primary': source_table_path == primary_table
            }
    
    return source_mapping

source_mapping = create_source_mapping_with_columns(attributes_mapping, dataset_objects_dict, source_id_key)
logger.info(f"Source mapping created for {len(source_mapping)} tables:")
for table_path, mapping_info in source_mapping.items():
    primary_indicator = " (PRIMARY)" if mapping_info['is_primary'] else ""
    logger.info(f"  {table_path} -> {mapping_info['dataset_name']} (ID column: {mapping_info['source_id_column']}){primary_indicator}")


# COMMAND ----------

# Build unified_source_records CTE conditionally
if unified_table_exists:
    unified_source_records_cte = f"""
unified_source_records AS (
  SELECT DISTINCT
    lakefusion_id,
    {source_id_key} as source_record_id,
    table_name as source_table_path
  FROM {unified_table}
  WHERE table_name != '{primary_table}'
),"""
    
    unified_join_clause = """LEFT JOIN unified_source_records usr ON ma.lakefusion_id = usr.lakefusion_id 
    AND ma.source = usr.source_table_path"""
    
    unified_case_clause = """COALESCE(usr.source_record_id)"""
else:
    # For single source entities -> empty CTE
    unified_source_records_cte = f"""
unified_source_records AS (
  SELECT 
    CAST(NULL AS STRING) as lakefusion_id,
    CAST(NULL AS STRING) as source_record_id,
    CAST(NULL AS STRING) as source_table_path
  WHERE 1=0
),"""
    
    unified_join_clause = """-- No unified table join for single source entity"""
    
    unified_case_clause = """CAST(NULL AS STRING)"""

logger.info(f"Using unified table logic: {unified_table_exists}")

# COMMAND ----------

crosswalk_sql = f"""
CREATE OR REPLACE VIEW {catalog_name}.gold.{entity}_crosswalk AS
WITH source_table_mapping AS (
  SELECT 
    source_table_path,
    dataset_name,
    source_id_column,
    is_primary
  FROM VALUES 
    {', '.join([f"('{table_path}', '{info['dataset_name']}', '{info['source_id_column']}', {str(info['is_primary']).lower()})" for table_path, info in source_mapping.items()])}
  AS t(source_table_path, dataset_name, source_id_column, is_primary)
),

-- Get all merge relationships (latest per merged record)
all_merges AS (
    SELECT 
        lakefusion_id as merged_id,
        master_lakefusion_id as merged_into,
        ROW_NUMBER() OVER (PARTITION BY lakefusion_id ORDER BY version DESC) as rn
    FROM {merge_activities_table}
    WHERE action_type IN ('JOB_MERGE', 'MANUAL_MERGE')
    AND lakefusion_id IS NOT NULL
),

latest_merges AS (
    SELECT merged_id, merged_into
    FROM all_merges
    WHERE rn = 1
),

-- Resolve merge chains (up to 5 levels deep)
-- This handles A->B->C->D->E chains where A merged into B, B merged into C, etc.
resolved_masters AS (
    SELECT 
        m0.merged_id as original_id,
        COALESCE(m4.merged_into, m3.merged_into, m2.merged_into, m1.merged_into, m0.merged_into) as ultimate_master
    FROM latest_merges m0
    LEFT JOIN latest_merges m1 ON m0.merged_into = m1.merged_id
    LEFT JOIN latest_merges m2 ON m1.merged_into = m2.merged_id
    LEFT JOIN latest_merges m3 ON m2.merged_into = m3.merged_id
    LEFT JOIN latest_merges m4 ON m3.merged_into = m4.merged_id
),

-- Active masters that exist in master table
active_masters AS (
    SELECT lakefusion_id
    FROM {master_table}
),

-- For records that were merged, map to their ultimate master
-- For records not merged, keep as-is
master_id_resolver AS (
    SELECT 
        am.lakefusion_id as original_master_id,
        am.lakefusion_id as resolved_master_id
    FROM active_masters am
    
    UNION ALL
    
    -- Map merged masters to their ultimate master (only if ultimate master is active)
    SELECT 
        rm.original_id as original_master_id,
        rm.ultimate_master as resolved_master_id
    FROM resolved_masters rm
    INNER JOIN active_masters am ON rm.ultimate_master = am.lakefusion_id
),

excluded_records AS (
  SELECT DISTINCT 
    CASE 
      WHEN action_type = 'SOURCE_DELETE' THEN master_lakefusion_id
      WHEN action_type = 'MANUAL_UNMERGE' THEN lakefusion_id
    END as excluded_lakefusion_id,
    source,
    action_type
  FROM {merge_activities_table}
  WHERE action_type IN ('SOURCE_DELETE', 'MANUAL_UNMERGE')
    AND (
      (action_type = 'SOURCE_DELETE') OR
      (action_type = 'MANUAL_UNMERGE')
    )
),

latest_merge_activities AS (
  SELECT 
    master_lakefusion_id,
    lakefusion_id,
    source,
    action_type,
    version,
    ROW_NUMBER() OVER (
      PARTITION BY master_lakefusion_id, source, COALESCE(lakefusion_id, master_lakefusion_id)
      ORDER BY version DESC
    ) as rn
  FROM {merge_activities_table}
  WHERE action_type IN ('MANUAL_MERGE', 'JOB_MERGE', 'JOB_INSERT', 'INITIAL_LOAD')
),

attribute_source_ids AS (
  SELECT 
    avs.lakefusion_id,
    attr_map.attribute_value as source_record_id,
    attr_map.source as source_table
  FROM {master_attribute_version_sources_table} avs
  LATERAL VIEW explode(avs.attribute_source_mapping) exploded_table AS attr_map
  WHERE attr_map.attribute_name = '{source_id_key}'
),

{unified_source_records_cte}

master_records AS (
  SELECT 
    lakefusion_id as master_lakefusion_id,
    {source_id_key} as master_source_record_id
  FROM {master_table}
),

processed_activities AS (
  SELECT 
    -- Use resolved master ID instead of original
    COALESCE(mir.resolved_master_id, ma.master_lakefusion_id) as master_lakefusion_id,
    ma.lakefusion_id,
    ma.master_lakefusion_id as original_master_lakefusion_id,
    COALESCE(stm.dataset_name, ma.source) as source_system_name,
    COALESCE(stm.source_table_path, ma.source) as source_table_name,
    COALESCE(stm.source_id_column, '{source_id_key}') as source_record_id_column_name,
    ma.action_type as merge_activity_id,
    ma.version,
    
    CASE 
      WHEN ma.action_type IN ('INITIAL_LOAD', 'JOB_INSERT') THEN 
        CASE 
          WHEN ma.source = '{primary_table}' THEN 
            COALESCE(asi.source_record_id, mr.master_source_record_id)
          ELSE 
            COALESCE({unified_case_clause}, mr.master_source_record_id)
        END
      WHEN ma.action_type IN ('MANUAL_MERGE', 'JOB_MERGE') THEN
        CASE 
          WHEN ma.source = '{primary_table}' THEN 
            COALESCE(asi_lakefusion.source_record_id, asi.source_record_id, ma.lakefusion_id)
          ELSE 
            COALESCE({unified_case_clause})
        END
    END as source_record_id,
    
    ROW_NUMBER() OVER (
      PARTITION BY COALESCE(mir.resolved_master_id, ma.master_lakefusion_id), COALESCE(stm.source_table_path, ma.source), ma.action_type, COALESCE(ma.lakefusion_id, ma.master_lakefusion_id)
      ORDER BY ma.version DESC
    ) as rn
    
  FROM latest_merge_activities ma
  -- Join to resolve master IDs that were themselves merged
  LEFT JOIN master_id_resolver mir ON ma.master_lakefusion_id = mir.original_master_id
  LEFT JOIN source_table_mapping stm ON ma.source = stm.source_table_path
  LEFT JOIN master_records mr ON COALESCE(mir.resolved_master_id, ma.master_lakefusion_id) = mr.master_lakefusion_id
  LEFT JOIN attribute_source_ids asi ON COALESCE(mir.resolved_master_id, ma.master_lakefusion_id) = asi.lakefusion_id 
    AND ma.source = asi.source_table
  LEFT JOIN attribute_source_ids asi_lakefusion ON ma.lakefusion_id = asi_lakefusion.lakefusion_id 
    AND ma.source = asi_lakefusion.source_table
  {unified_join_clause}
  LEFT JOIN excluded_records er_delete ON COALESCE(mir.resolved_master_id, ma.master_lakefusion_id) = er_delete.excluded_lakefusion_id 
    AND ma.source = er_delete.source 
    AND er_delete.action_type = 'SOURCE_DELETE'
  LEFT JOIN excluded_records er_unmerge ON COALESCE(ma.lakefusion_id, ma.master_lakefusion_id) = er_unmerge.excluded_lakefusion_id 
    AND ma.source = er_unmerge.source 
    AND er_unmerge.action_type = 'MANUAL_UNMERGE'
  -- Only include records where the resolved master exists in master table
  INNER JOIN active_masters am ON COALESCE(mir.resolved_master_id, ma.master_lakefusion_id) = am.lakefusion_id
WHERE ma.rn = 1
  AND er_delete.excluded_lakefusion_id IS NULL
  AND er_unmerge.excluded_lakefusion_id IS NULL
)

SELECT 
  pa.master_lakefusion_id as lakefusion_id,
  pa.source_system_name,
  pa.source_table_name,
  pa.source_record_id_column_name,
  pa.source_record_id,
  pa.merge_activity_id,
  hst.timestamp as created_timestamp
FROM processed_activities pa
INNER JOIN (DESCRIBE HISTORY {master_table}) hst ON pa.version = hst.version
WHERE pa.rn = 1
ORDER BY pa.master_lakefusion_id, pa.version DESC
"""

spark.sql(crosswalk_sql)
