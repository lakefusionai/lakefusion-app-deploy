# Databricks notebook source
"""
v4.0.0 Migration - Step 4: Create Crosswalk View

Creates the v4.0.0 crosswalk view after transform completes.
Crosswalk joins master + unified to provide a mapping of source records to master records.
"""

# COMMAND ----------

import json
from pyspark.sql.functions import col, lit

# COMMAND ----------

# DBTITLE 1,Widget Parameters
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("dataset_objects", "", "Dataset Objects (JSON)")

# COMMAND ----------

# DBTITLE 1,Parse Parameters
entity = dbutils.widgets.get("entity")
catalog_name = dbutils.widgets.get("catalog_name")
experiment_id = dbutils.widgets.get("experiment_id")
dataset_objects_json = dbutils.widgets.get("dataset_objects")

# Parse JSON parameters
dataset_objects = json.loads(dataset_objects_json) if dataset_objects_json else {}

print("="*80)
print("STEP 4: CREATE v4.0.0 CROSSWALK VIEW")
print("="*80)
print(f"Entity: {entity}")
print(f"Catalog: {catalog_name}")
print(f"Experiment: {experiment_id or 'prod'}")
print(f"")

# COMMAND ----------

# DBTITLE 1,Define Table Names
experiment_suffix = f"_{experiment_id}" if experiment_id else ""

# v4.0.0 table names
master_table = f"{catalog_name}.gold.{entity}_master{experiment_suffix}"
unified_table = f"{catalog_name}.silver.{entity}_unified{experiment_suffix}"
merge_activities_table = f"{master_table}_merge_activities"
crosswalk_view = f"{catalog_name}.gold.{entity}_crosswalk{experiment_suffix}"

print("Target Tables (v4.0.0):")
print(f"  Master: {master_table}")
print(f"  Unified: {unified_table}")
print(f"  Merge Activities: {merge_activities_table}")
print(f"  Crosswalk View: {crosswalk_view}")
print("")

# COMMAND ----------

# DBTITLE 1,Verify Required Tables Exist
print("Verifying required tables exist...")

required_tables = [
    master_table,
    unified_table,
    merge_activities_table
]

missing_tables = []
for table in required_tables:
    if not spark.catalog.tableExists(table):
        missing_tables.append(table)

if missing_tables:
    error_msg = f"❌ Missing required tables:\n" + "\n".join([f"  - {t}" for t in missing_tables])
    print(error_msg)
    raise Exception(error_msg)

print("✓ All required tables exist")
print("")

# COMMAND ----------

# DBTITLE 1,Build Source System Mapping
print("="*80)
print("Building Source System Mapping")
print("="*80)

# Create mapping from source_path to friendly name and ID column
# Extract from dataset_objects
source_mapping = {}
if dataset_objects:
    for dataset_id, dataset_info in dataset_objects.items():
        source_path = dataset_info.get('path', '')
        source_name = dataset_info.get('name', source_path)
        id_column = dataset_info.get('id', 'id')  # Default to 'id' if not specified
        
        source_mapping[source_path] = {
            'name': source_name,
            'id_column': id_column,
            'path': source_path
        }
    
    print(f"✓ Created mapping for {len(source_mapping)} source systems:")
    for path, info in source_mapping.items():
        print(f"  {path}")
        print(f"    Name: {info['name']}")
        print(f"    ID Column: {info['id_column']}")
else:
    print("⚠️  No dataset_objects provided, using source_path as-is for names")

print("")

# COMMAND ----------

# DBTITLE 1,Build Crosswalk Query
print("="*80)
print("Building Crosswalk Query")
print("="*80)

# Build VALUES clause for source mapping (if provided)
if source_mapping:
    source_mapping_values = []
    for path, info in source_mapping.items():
        # Escape single quotes in strings
        escaped_path = path.replace("'", "''")
        escaped_name = info['name'].replace("'", "''")
        escaped_id = str(info['id_column']).replace("'", "''")
        
        source_mapping_values.append(
            f"('{escaped_path}', '{escaped_name}', '{escaped_id}')"
        )
    
    values_clause = ",\n        ".join(source_mapping_values)
    
    # Query with source mapping CTE
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
    WHERE ma.action_type IN ('MASTER_JOB_MERGE', 'MASTER_MANUAL_MERGE')
        AND ma.match_id IS NOT NULL
        AND ma.match_id != ''
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
        AND u.master_lakefusion_id != ''
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
        AND ma.master_id != ''
        AND ma.match_id IS NOT NULL
        AND ma.match_id != ''
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
        AND ma.master_id != ''
        AND ma.match_id IS NOT NULL
        AND ma.match_id != ''
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
else:
    # Simple query without source mapping
    crosswalk_query = f"""
CREATE OR REPLACE VIEW {crosswalk_view} AS
WITH master_merge_chain AS (
    -- Build mapping of merged masters to their survivors
    SELECT DISTINCT
        ma.match_id as old_master_id,
        ma.master_id as final_master_id,
        ma.action_type as merge_type,
        ma.version as merge_version,
        ma.created_at as merge_timestamp
    FROM {merge_activities_table} ma
    WHERE ma.action_type IN ('MASTER_JOB_MERGE', 'MASTER_MANUAL_MERGE')
        AND ma.match_id IS NOT NULL
        AND ma.match_id != ''
),
unified_contributors AS (
    -- Get all MERGED records from unified table
    SELECT
        u.master_lakefusion_id as lakefusion_id,
        u.source_path,
        u.source_id,
        u.surrogate_key,
        u.record_status
    FROM {unified_table} u
    WHERE u.record_status = 'MERGED'
        AND u.master_lakefusion_id IS NOT NULL
        AND u.master_lakefusion_id != ''
),
merge_activity_direct AS (
    -- Get merge activity for direct contributors
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
        AND ma.master_id != ''
        AND ma.match_id IS NOT NULL
        AND ma.match_id != ''
),
merge_activity_inherited AS (
    -- Get merge activity for inherited contributors
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
        AND ma.master_id != ''
        AND ma.match_id IS NOT NULL
        AND ma.match_id != ''
),
merge_activity_combined AS (
    -- Combine direct and inherited
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
        uc.source_path as source_system_name,
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

print("✓ Query built successfully")
print("")

# COMMAND ----------

# DBTITLE 1,Create Crosswalk View
print("="*80)
print("Creating Crosswalk View")
print("="*80)

# Drop old view if exists
try:
    spark.sql(f"DROP VIEW IF EXISTS {crosswalk_view}")
    print("✓ Dropped existing view (if any)")
except Exception as e:
    print(f"⚠️  Could not drop existing view: {e}")

# Execute the query to create the view
try:
    spark.sql(crosswalk_query)
    print(f"✓ Crosswalk view created: {crosswalk_view}")
except Exception as e:
    print(f"❌ Failed to create crosswalk view: {e}")
    raise

print("")

# COMMAND ----------

# DBTITLE 1,Validate Crosswalk
print("="*80)
print("Validating Crosswalk")
print("="*80)

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

# Get unified MERGED count
unified_merged_count = spark.sql(f"""
    SELECT COUNT(*) as cnt 
    FROM {unified_table} 
    WHERE record_status = 'MERGED'
""").collect()[0]['cnt']

print(f"\n📊 Validation Results:")
print(f"  Master table records: {master_count}")
print(f"  Unified MERGED records: {unified_merged_count}")
print(f"  Unique masters in crosswalk: {stats['unique_masters']}")
print(f"  Total source contributions: {stats['total_contributions']}")
print(f"  Unique source systems: {unique_sources}")

# Validation checks
validation_passed = True

if stats['unique_masters'] == master_count:
    print(f"\n✓ CHECK 1 PASSED: All master records represented in crosswalk")
else:
    print(f"\n❌ CHECK 1 FAILED: Master count mismatch!")
    print(f"   Master table: {master_count}")
    print(f"   Crosswalk unique masters: {stats['unique_masters']}")
    print(f"   Difference: {abs(master_count - stats['unique_masters'])}")
    validation_passed = False

if stats['total_contributions'] == unified_merged_count:
    print(f"✓ CHECK 2 PASSED: All unified MERGED records in crosswalk")
else:
    print(f"⚠️  CHECK 2 INFO: Contribution count differs from unified MERGED")
    print(f"   Unified MERGED: {unified_merged_count}")
    print(f"   Crosswalk contributions: {stats['total_contributions']}")
    print(f"   Difference: {abs(unified_merged_count - stats['total_contributions'])}")
    print(f"   (This may be expected if master merges create inherited contributions)")

print("")

# COMMAND ----------

# DBTITLE 1,Sample Crosswalk Data
print("="*80)
print("Sample Crosswalk Data")
print("="*80)

# Show distribution by source system
print("\nContributions by source system:")
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
print("Contributions by merge activity type:")
spark.sql(f"""
    SELECT 
        merge_activity_id,
        COUNT(*) as contribution_count,
        COUNT(DISTINCT lakefusion_id) as unique_masters
    FROM {crosswalk_view}
    GROUP BY merge_activity_id
    ORDER BY contribution_count DESC
""").show(truncate=False)

# Show sample records
print("Sample crosswalk records:")
spark.sql(f"""
    SELECT *
    FROM {crosswalk_view}
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Summary
print("\n" + "="*80)
print("CROSSWALK GENERATION COMPLETE")
print("="*80)

summary = {
    "status": "success" if validation_passed else "warning",
    "view_name": crosswalk_view,
    "unique_masters": int(stats['unique_masters']),
    "total_contributions": int(stats['total_contributions']),
    "unique_sources": int(unique_sources),
    "validation_passed": validation_passed
}

print(f"\n📊 Summary:")
print(f"  ✓ View created: {crosswalk_view}")
print(f"  ✓ Unique masters: {summary['unique_masters']:,}")
print(f"  ✓ Total contributions: {summary['total_contributions']:,}")
print(f"  ✓ Unique sources: {summary['unique_sources']}")
print(f"  ✓ Validation: {'PASSED' if summary['validation_passed'] else 'WARNING'}")

if not validation_passed:
    print(f"\n⚠️  Note: Some validation checks did not pass")
    print(f"   Review the validation results above for details")

print("\n" + "="*80)
print("✅ Step 4 completed successfully!")
print("="*80)
