# Databricks notebook source
import json
from pyspark.sql.functions import col, lit

# COMMAND ----------

dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("dataset_objects", "", "Dataset Objects")

# COMMAND ----------

entity = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "entity",
    debugValue=dbutils.widgets.get("entity")
)
catalog_name = dbutils.widgets.get("catalog_name")
experiment_id = dbutils.widgets.get("experiment_id")
dataset_objects = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "dataset_objects",
    debugValue=dbutils.widgets.get("dataset_objects")
)

# Parse JSON parameters
dataset_objects = json.loads(dataset_objects)

# COMMAND ----------

# MAGIC %run ../utils/execute_utils

# COMMAND ----------

logger.info(f"Entity: {entity}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment ID: {experiment_id or 'None'}")

# COMMAND ----------

# Base table names
unified_table = f"{catalog_name}.silver.{entity}_unified"
master_table = f"{catalog_name}.gold.{entity}_master"
crosswalk_view = f"{catalog_name}.gold.{entity}_crosswalk"

# Add experiment suffix if exists
if experiment_id:
    unified_table += f"_{experiment_id}"
    master_table += f"_{experiment_id}"
    crosswalk_view += f"_{experiment_id}"

# Related tables
merge_activities_table = f"{master_table}_merge_activities"

logger.info(f"Unified Table: {unified_table}")
logger.info(f"Master Table: {master_table}")
logger.info(f"Merge Activities Table: {merge_activities_table}")
logger.info(f"Crosswalk View: {crosswalk_view}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("CREATING SOURCE SYSTEM MAPPING")
logger.info("="*80)

# Create mapping from source_path to friendly name and ID column
# Extract from dataset_objects
source_mapping = {}
for dataset_id, dataset_info in dataset_objects.items():
    source_path = dataset_info.get('path', '')
    source_name = dataset_info.get('name', source_path)
    id_column = dataset_info.get('id', 'id')  # Default to 'id' if not specified
    
    source_mapping[source_path] = {
        'name': source_name,
        'id_column': id_column,
        'path': source_path
    }

logger.info(f"Created mapping for {len(source_mapping)} source systems:")
for path, info in source_mapping.items():
    logger.info(f"  {path}")
    logger.info(f"    Name: {info['name']}")
    logger.info(f"    ID Column: {info['id_column']}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("BUILDING CROSSWALK QUERY")
logger.info("="*80)

# Build VALUES clause for source mapping
source_mapping_values = []
for path, info in source_mapping.items():
    # Escape single quotes in strings
    escaped_path = path.replace("'", "''")
    escaped_name = info['name'].replace("'", "''")
    escaped_id = str(info['id_column']).replace("'", "''")
    
    source_mapping_values.append(
        f"('{escaped_path}', '{escaped_name}', '{escaped_id}')"
    )

values_clause = ",\n    ".join(source_mapping_values)

# Build the crosswalk query
crosswalk_query = f"""
CREATE OR REPLACE VIEW {crosswalk_view} AS
WITH source_system_mapping AS (
    -- Mapping of source paths to friendly names and ID columns
    SELECT 
        source_path,
        source_name,
        id_column_name
    FROM VALUES 
        {values_clause}
    AS t(source_path, source_name, id_column_name)
),
master_merge_chain AS (
    -- Build mapping of merged masters to their survivors
    -- This helps trace contributors through master merges
    SELECT DISTINCT
        ma.match_id as old_master_id,
        ma.master_id as final_master_id,
        ma.action_type as merge_type,
        ma.version as merge_version,
        ma.created_at as merge_timestamp
    FROM {merge_activities_table} ma
    WHERE ma.action_type IN ('MASTER_JOB_MERGE', 'MASTER_MANUAL_MERGE','MANUAL_FORCE_MERGE')
),
unified_contributors AS (
    -- Get all MERGED records from unified table
    -- These are the actual source records that contribute to masters
    SELECT
        u.master_lakefusion_id as lakefusion_id,
        u.source_path,
        u.source_id,
        u.surrogate_key,
        u.record_status
    FROM {unified_table} u
    WHERE u.record_status = 'MERGED'
        AND u.master_lakefusion_id IS NOT NULL
),
merge_activity_direct AS (
    -- Get merge activity for direct contributors (normal merge/insert)
    SELECT
        ma.master_id as lakefusion_id,
        ma.match_id as surrogate_key,
        ma.source as source_path,
        ma.action_type as merge_activity_id,
        ma.version,
        ma.created_at,
        'DIRECT' as match_type
    FROM {merge_activities_table} ma
    WHERE ma.action_type IN ('INITIAL_LOAD', 'JOB_MERGE', 'JOB_INSERT', 'MANUAL_MERGE')
        AND ma.master_id IS NOT NULL
        AND ma.match_id IS NOT NULL
),
merge_activity_inherited AS (
    -- Get merge activity for contributors inherited through master merges
    -- These are contributors that belonged to a merged master
    SELECT
        mmc.final_master_id as lakefusion_id,
        ma.match_id as surrogate_key,
        ma.source as source_path,
        ma.action_type as merge_activity_id,
        ma.version,
        ma.created_at,
        'INHERITED' as match_type
    FROM {merge_activities_table} ma
    INNER JOIN master_merge_chain mmc
        ON ma.master_id = mmc.old_master_id
    WHERE ma.action_type IN ('INITIAL_LOAD', 'JOB_MERGE', 'JOB_INSERT', 'MANUAL_MERGE')
        AND ma.master_id IS NOT NULL
        AND ma.match_id IS NOT NULL
),
merge_activity_combined AS (
    -- Combine direct and inherited, prioritizing direct matches
    SELECT
        lakefusion_id,
        surrogate_key,
        source_path,
        merge_activity_id,
        version,
        created_at,
        match_type,
        ROW_NUMBER() OVER (
            PARTITION BY lakefusion_id, surrogate_key
            ORDER BY 
                CASE match_type 
                    WHEN 'DIRECT' THEN 1 
                    WHEN 'INHERITED' THEN 2 
                END,
                version DESC
        ) as rn
    FROM (
        SELECT * FROM merge_activity_direct
        UNION ALL
        SELECT * FROM merge_activity_inherited
    )
),
merge_activity_metadata AS (
    -- Get the best match for each contributor
    SELECT
        lakefusion_id,
        surrogate_key,
        source_path,
        merge_activity_id,
        version,
        created_at
    FROM merge_activity_combined
    WHERE rn = 1
),
master_history AS (
    -- Get timestamp for each master table version
    SELECT 
        version,
        timestamp as created_timestamp
    FROM (DESCRIBE HISTORY {master_table})
),
crosswalk_data AS (
    -- Join everything together
    SELECT
        uc.lakefusion_id,
        COALESCE(ssm.source_name, uc.source_path) as source_system_name,
        uc.source_path as source_table_name,
        uc.source_id as source_record_id,
        mam.merge_activity_id,
        COALESCE(mam.created_at, mh.created_timestamp) as created_timestamp,
        mam.version,
        ROW_NUMBER() OVER (
            PARTITION BY uc.lakefusion_id, uc.source_path, uc.source_id
            ORDER BY COALESCE(mam.version, 0) DESC
        ) as rn
    FROM unified_contributors uc
    LEFT JOIN source_system_mapping ssm
        ON uc.source_path = ssm.source_path
    LEFT JOIN merge_activity_metadata mam
        ON uc.lakefusion_id = mam.lakefusion_id
        AND uc.surrogate_key = mam.surrogate_key
    LEFT JOIN master_history mh
        ON mam.version = mh.version
)
SELECT
    lakefusion_id,
    source_system_name,
    source_table_name,
    source_record_id,
    merge_activity_id,
    created_timestamp
FROM crosswalk_data
WHERE rn = 1
ORDER BY lakefusion_id, created_timestamp DESC
"""

logger.info("Query built successfully")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("CREATING CROSSWALK VIEW")
logger.info("="*80)

# Execute the query to create the view
spark.sql(crosswalk_query)

logger.info(f"Crosswalk view created: {crosswalk_view}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("VALIDATING CROSSWALK")
logger.info("="*80)

# Get statistics
stats_query = f"""
SELECT
    COUNT(DISTINCT lakefusion_id) as unique_masters,
    COUNT(*) as total_contributions,
    AVG(contributions_per_master) as avg_contributions_per_master
FROM (
    SELECT 
        lakefusion_id,
        COUNT(*) as contributions_per_master
    FROM {crosswalk_view}
    GROUP BY lakefusion_id
)
"""

stats = spark.sql(stats_query).collect()[0]

# Get unique sources separately
unique_sources = spark.sql(f"""
    SELECT COUNT(DISTINCT source_table_name) as cnt 
    FROM {crosswalk_view}
""").collect()[0]['cnt']

# Get master table count
master_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {master_table}").collect()[0]['cnt']

logger.info(f"\nValidation Results:")
logger.info(f"  Master table records: {master_count}")
logger.info(f"  Unique masters in crosswalk: {stats['unique_masters']}")
logger.info(f"  Total source contributions: {stats['total_contributions']}")
logger.info(f"  Unique source systems: {unique_sources}")
logger.info(f"  Average contributions per master: {stats['avg_contributions_per_master']:.2f}")

# Validation check
if stats['unique_masters'] == master_count:
    logger.info(f"\nVALIDATION PASSED: All master records represented in crosswalk")
else:
    logger.warning(f"\nVALIDATION WARNING: Master count mismatch!")
    logger.warning(f"   Master table: {master_count}")
    logger.warning(f"   Crosswalk unique masters: {stats['unique_masters']}")
    logger.warning(f"   Difference: {abs(master_count - stats['unique_masters'])}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("SAMPLE CROSSWALK DATA")
logger.info("="*80)

# Show distribution by source system
logger.info("Contributions by source system:")
spark.sql(f"""
    SELECT 
        source_system_name,
        COUNT(*) as contribution_count,
        COUNT(DISTINCT lakefusion_id) as unique_masters
    FROM {crosswalk_view}
    GROUP BY source_system_name
    ORDER BY contribution_count DESC
""").show(truncate=False)

# Show distribution by merge activity type
logger.info("Contributions by merge activity type:")
spark.sql(f"""
    SELECT 
        merge_activity_id,
        COUNT(*) as contribution_count,
        COUNT(DISTINCT lakefusion_id) as unique_masters
    FROM {crosswalk_view}
    GROUP BY merge_activity_id
    ORDER BY contribution_count DESC
""").show(truncate=False)

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("CROSSWALK GENERATION COMPLETE")
logger.info("="*80)

summary = {
    "status": "success",
    "view_name": crosswalk_view,
    "unique_masters": int(stats['unique_masters']),
    "total_contributions": int(stats['total_contributions']),
    "unique_sources": int(unique_sources),
    "validation_passed": stats['unique_masters'] == master_count
}

logger.info(f"\nSummary:")
logger.info(f"  View created: {crosswalk_view}")
logger.info(f"  Unique masters: {summary['unique_masters']}")
logger.info(f"  Total contributions: {summary['total_contributions']}")
logger.info(f"  Unique sources: {summary['unique_sources']}")
logger.info(f"  Validation: {'PASSED' if summary['validation_passed'] else 'FAILED'}")

# Set task values
dbutils.jobs.taskValues.set("crosswalk_complete", True)
dbutils.jobs.taskValues.set("unique_masters", summary['unique_masters'])
dbutils.jobs.taskValues.set("total_contributions", summary['total_contributions'])

# COMMAND ----------

logger_instance.shutdown()
