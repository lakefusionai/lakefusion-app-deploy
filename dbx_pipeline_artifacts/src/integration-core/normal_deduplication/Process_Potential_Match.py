# Databricks notebook source
import json
from pyspark.sql.functions import (
    col, lit, collect_set, struct, concat, coalesce, map_from_arrays, collect_list
)
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")

# COMMAND ----------

entity = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "entity",
    debugValue=dbutils.widgets.get("entity")
)
catalog_name = dbutils.widgets.get("catalog_name")
experiment_id = dbutils.widgets.get("experiment_id")
entity_attributes = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "entity_attributes",
    debugValue=dbutils.widgets.get("entity_attributes")
)

# Parse JSON parameters
entity_attributes = json.loads(entity_attributes)

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

# Base table names
unified_table = f"{catalog_name}.silver.{entity}_unified"
master_table = f"{catalog_name}.gold.{entity}_master"
processed_unified_table = f"{catalog_name}.silver.{entity}_processed_unified"
potential_match_table = f"{catalog_name}.gold.{entity}_master_potential_match"

# Add experiment suffix if exists
if experiment_id:
    unified_table += f"_{experiment_id}"
    master_table += f"_{experiment_id}"
    processed_unified_table += f"_{experiment_id}"
    potential_match_table += f"_{experiment_id}"

# Related tables
merge_activities_table = f"{master_table}_merge_activities"

# COMMAND ----------

id_key = 'lakefusion_id'
unified_id_key = 'surrogate_key'

# COMMAND ----------

# Check if processed_unified_table exists
processed_unified_exists = spark.catalog.tableExists(processed_unified_table)

logger.info(f"Entity: {entity}")
logger.info(f"Master ID Key: {id_key}")
logger.info(f"Unified ID Key: {unified_id_key}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment ID: {experiment_id or 'None'}")
logger.info(f"Unified Table: {unified_table}")
logger.info(f"Master Table: {master_table}")
logger.info(f"Processed Unified Table: {processed_unified_table}")
logger.info(f"  Exists: {processed_unified_exists}")
logger.info(f"Potential Match Table: {potential_match_table}")
logger.info(f"Merge Activities Table: {merge_activities_table}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("BUILDING POTENTIAL MATCH QUERY")
logger.info("="*80)

# Build select clause for master attributes
master_select_list = [f"m.{id_key} as {id_key}"] + [f"m.{attr} as {attr}" for attr in entity_attributes]
master_select = ", ".join(master_select_list)

# Build named_struct for potential matches
named_struct_parts = []

# Add all entity attributes from unified table
for attr in entity_attributes:
    named_struct_parts.append(f"'{attr}', u.{attr}")

# Add metadata fields
named_struct_parts.extend([
    "'__score__', match_info.score",
    "'__reason__', match_info.reason",
    "'__source_path__', u.source_path",
    "'__source_id__', u.source_id",
    f"'{unified_id_key}', u.{unified_id_key}",
    "'__mergestatus__', match_info.merge_status"
])

named_struct_expr = f"named_struct({', '.join(named_struct_parts)})"

# Build the pending matches CTEs only if processed_unified_table exists
if processed_unified_exists:
    pending_matches_ctes = f"""
pending_matches_raw AS (
    -- Get POTENTIAL_MATCH records from processed_unified
    SELECT
        pu.{id_key} as original_master_{id_key},
        pu.{unified_id_key},
        pu.exploded_result.score as score,
        pu.exploded_result.reason as reason
    FROM {processed_unified_table} pu
    WHERE pu.exploded_result.match = 'POTENTIAL_MATCH'
),
pending_matches_resolved AS (
    -- Resolve pending matches to their final master
    SELECT
        COALESCE(mmf.final_master_id, pm.original_master_{id_key}) as master_{id_key},
        pm.{unified_id_key},
        pm.score,
        pm.reason
    FROM pending_matches_raw pm
    LEFT JOIN master_merge_final mmf
        ON pm.original_master_{id_key} = mmf.old_master_id
),
pending_matches_from_processed AS (
    -- Join with unified table and filter
    SELECT
        pmr.master_{id_key},
        pmr.{unified_id_key},
        u.source_path,
        'POTENTIAL_MATCH' as action_type,
        pmr.score,
        pmr.reason,
        'PENDING' as merge_status
    FROM pending_matches_resolved pmr
    INNER JOIN {unified_table} u
        ON pmr.{unified_id_key} = u.{unified_id_key}
    LEFT JOIN not_a_match_pairs nam
        ON pmr.master_{id_key} = nam.master_{id_key}
        AND pmr.{unified_id_key} = nam.{unified_id_key}
    WHERE u.record_status = 'ACTIVE'
        AND nam.{unified_id_key} IS NULL
        AND EXISTS (
            SELECT 1 FROM {master_table} m
            WHERE m.{id_key} = pmr.master_{id_key}
        )
),"""
    
    all_matches_union = f"""
    UNION ALL
    
    SELECT 
        master_{id_key},
        {unified_id_key},
        source_path,
        score,
        reason,
        merge_status
    FROM pending_matches_from_processed"""
    
    logger.info("Including pending matches from processed_unified table")
else:
    pending_matches_ctes = ""
    all_matches_union = ""
    logger.info("Processed unified table does not exist - skipping pending matches")

# Build the complete query
query = f"""
WITH current_contributors AS (
    -- Get all records assigned to a master
    SELECT
        u.master_{id_key},
        u.{unified_id_key},
        u.source_path,
        u.source_id,
        u.record_status,
        u.scoring_results,
        {', '.join([f'u.{attr}' for attr in entity_attributes])}
    FROM {unified_table} u
    WHERE u.master_{id_key} IS NOT NULL
        AND u.record_status IN ('MERGED', 'ACTIVE')
),
master_merge_chain AS (
    -- Build mapping from old merged masters to their final survivors
    SELECT DISTINCT
        ma.match_id as old_master_id,
        ma.master_id as intermediate_master_id
    FROM {merge_activities_table} ma
    WHERE ma.action_type IN ('MASTER_JOB_MERGE', 'MASTER_MANUAL_MERGE', 'MASTER_FORCE_MERGE')
),
master_merge_final AS (
    -- Resolve to final master (handles one level of chaining)
    SELECT DISTINCT
        mmc1.old_master_id,
        COALESCE(mmc2.intermediate_master_id, mmc1.intermediate_master_id) as final_master_id
    FROM master_merge_chain mmc1
    LEFT JOIN master_merge_chain mmc2
        ON mmc1.intermediate_master_id = mmc2.old_master_id
),
not_a_match_pairs AS (
    -- Get all NOT_A_MATCH pairs to exclude from potential matches
    SELECT DISTINCT
        ma.master_id as master_{id_key},
        ma.match_id as {unified_id_key}
    FROM {merge_activities_table} ma
    WHERE ma.action_type = 'MANUAL_NOT_A_MATCH'
),
{pending_matches_ctes}
-- Get the ORIGINAL merge activity for each source record
-- This is the first time it was merged into ANY master
original_merge_activity AS (
    SELECT
        ma.match_id as {unified_id_key},
        ma.master_id as original_master_id,
        ma.action_type as original_action_type,
        ROW_NUMBER() OVER (
            PARTITION BY ma.match_id 
            ORDER BY ma.version DESC, ma.created_at DESC
        ) as rn
    FROM {merge_activities_table} ma
    WHERE ma.match_id IS NOT NULL 
      AND ma.match_id LIKE 'sk_%'  -- Only source records, not master merges
),
original_merge AS (
    SELECT {unified_id_key}, original_master_id, original_action_type
    FROM original_merge_activity
    WHERE rn = 1
),

unmerged_records AS (
    -- Identify records unmerged and promoted to their own master.
    -- Use ROW_NUMBER to keep only the LATEST unmerge event per surrogate_key,
    -- so that re-unmerge cycles don't produce duplicate rows.
    SELECT
        {unified_id_key},
        old_master_id,
        new_master_id
    FROM (
        SELECT
            unmerge.match_id     AS {unified_id_key},
            unmerge.master_id    AS old_master_id,
            insert_evt.master_id AS new_master_id,
            ROW_NUMBER() OVER (
                PARTITION BY unmerge.match_id
                ORDER BY unmerge.version DESC, unmerge.created_at DESC
            ) AS rn
        FROM {merge_activities_table} unmerge
        INNER JOIN {merge_activities_table} insert_evt
            ON  unmerge.match_id  = insert_evt.match_id
            AND unmerge.version   = insert_evt.version
            AND insert_evt.action_type = 'JOB_INSERT'
        WHERE unmerge.action_type = 'MANUAL_UNMERGE'
          AND unmerge.match_id IS NOT NULL
          AND unmerge.match_id LIKE 'sk_%'
    )
    WHERE rn = 1
),
-- Determine where each promoted master ended up after a subsequent merge
promoted_master_fate AS (
    -- For each unmerged record, check if its promoted new master was
    -- subsequently merged, and if so, into which final master.
    SELECT
        ur.{unified_id_key},
        ur.old_master_id,
        ur.new_master_id,
        mmf.final_master_id AS merged_into_master_id,
        ma_latest.action_type AS merge_into_action_type
    FROM unmerged_records ur
    LEFT JOIN master_merge_final mmf
        ON ur.new_master_id = mmf.old_master_id
    -- Get the actual action_type of the master-to-master merge
    -- so we can distinguish MASTER_FORCE_MERGE vs MASTER_MANUAL_MERGE
    LEFT JOIN (
        SELECT
            match_id,
            action_type,
            ROW_NUMBER() OVER (
                PARTITION BY match_id
                ORDER BY version DESC, created_at DESC
            ) AS rn
        FROM {merge_activities_table}
        WHERE action_type IN ('MASTER_FORCE_MERGE', 'MASTER_MANUAL_MERGE', 'MASTER_JOB_MERGE')
    ) ma_latest
        ON ur.new_master_id = ma_latest.match_id
        AND ma_latest.rn = 1
),
-- - If promoted master was merged back into this old master → MASTER_FORCE_MERGE
-- - If promoted master was merged into a DIFFERENT master → keep MANUAL_UNMERGE
-- - If promoted master was never merged → MANUAL_UNMERGE
unmerged_as_match AS (
    SELECT
        pmf.old_master_id                        AS master_{id_key},
        pmf.{unified_id_key},
        u.source_path,
        '1.0'                                    AS score,
        CASE
            WHEN pmf.merged_into_master_id = pmf.old_master_id
                AND pmf.merge_into_action_type = 'MASTER_MANUAL_MERGE'
                THEN 'Masters merged manually'
            WHEN pmf.merged_into_master_id = pmf.old_master_id
                AND pmf.merge_into_action_type = 'MASTER_JOB_MERGE'
                THEN 'Masters merged automatically'
            WHEN pmf.merged_into_master_id = pmf.old_master_id
                THEN 'Force merged'
            ELSE 'Record unmerged'
        END                                      AS reason,
        CASE
            WHEN pmf.merged_into_master_id = pmf.old_master_id
                AND pmf.merge_into_action_type = 'MASTER_MANUAL_MERGE'
                THEN 'MASTER_MANUAL_MERGE'
            WHEN pmf.merged_into_master_id = pmf.old_master_id
                AND pmf.merge_into_action_type = 'MASTER_JOB_MERGE'
                THEN 'MASTER_JOB_MERGE'
            WHEN pmf.merged_into_master_id = pmf.old_master_id
                THEN 'MASTER_FORCE_MERGE'
            ELSE 'MANUAL_UNMERGE'
        END                                      AS merge_status
    FROM promoted_master_fate pmf
    INNER JOIN {unified_table} u
        ON pmf.{unified_id_key} = u.{unified_id_key}
),
-- One row for the NEW promoted master or the DIFFERENT master
-- it was subsequently merged into.
-- - If still active (not merged away) → show under new_master_id as PROMOTED_TO_MASTER
-- - If merged into a DIFFERENT master → show under that master as MASTER_FORCE_MERGE
-- - If merged back into OLD master → handled by unmerged_as_match above, skip here
promoted_masters AS (
    SELECT
        CASE
            WHEN pmf.merged_into_master_id IS NOT NULL
                AND pmf.merged_into_master_id != pmf.old_master_id
                THEN pmf.merged_into_master_id
            ELSE pmf.new_master_id
        END                                      AS master_{id_key},
        pmf.{unified_id_key},
        u.source_path,
        '1.0'                                    AS score,
        CASE
            WHEN pmf.merged_into_master_id IS NOT NULL
                AND pmf.merged_into_master_id != pmf.old_master_id
                AND pmf.merge_into_action_type = 'MASTER_MANUAL_MERGE'
                THEN 'Masters merged manually'
            WHEN pmf.merged_into_master_id IS NOT NULL
                AND pmf.merged_into_master_id != pmf.old_master_id
                AND pmf.merge_into_action_type = 'MASTER_JOB_MERGE'
                THEN 'Masters merged automatically'
            WHEN pmf.merged_into_master_id IS NOT NULL
                AND pmf.merged_into_master_id != pmf.old_master_id
                THEN 'Force merged'
            ELSE 'Record unmerged and promoted to master'
        END                                      AS reason,
        CASE
            WHEN pmf.merged_into_master_id IS NOT NULL
                AND pmf.merged_into_master_id != pmf.old_master_id
                AND pmf.merge_into_action_type = 'MASTER_MANUAL_MERGE'
                THEN 'MASTER_MANUAL_MERGE'
            WHEN pmf.merged_into_master_id IS NOT NULL
                AND pmf.merged_into_master_id != pmf.old_master_id
                AND pmf.merge_into_action_type = 'MASTER_JOB_MERGE'
                THEN 'MASTER_JOB_MERGE'
            WHEN pmf.merged_into_master_id IS NOT NULL
                AND pmf.merged_into_master_id != pmf.old_master_id
                THEN 'MASTER_FORCE_MERGE'
            ELSE 'PROMOTED_TO_MASTER'
        END                                      AS merge_status
    FROM promoted_master_fate pmf
    INNER JOIN {unified_table} u
        ON pmf.{unified_id_key} = u.{unified_id_key}
    -- Skip when merged back into old master — already handled by unmerged_as_match
    WHERE pmf.merged_into_master_id IS NULL
       OR pmf.merged_into_master_id != pmf.old_master_id
),
-- Parse scoring_results JSON to extract score/reason for each target lakefusion_id
scoring_results_exploded AS (
    SELECT
        cc.{unified_id_key},
        EXPLODE(FROM_JSON(cc.scoring_results, 'ARRAY<STRUCT<id:STRING, match:STRING, score:DOUBLE, reason:STRING, lakefusion_id:STRING>>')) as sr
    FROM current_contributors cc
    WHERE cc.scoring_results IS NOT NULL 
      AND cc.scoring_results != '' 
      AND cc.scoring_results != '[]'
),
scoring_results_parsed AS (
    SELECT
        {unified_id_key},
        sr.lakefusion_id as target_lakefusion_id,
        CAST(sr.score AS STRING) as score,
        sr.reason as reason
    FROM scoring_results_exploded
),
-- Exclude unmerged records entirely from merged_contributors_enriched
-- (they are handled by unmerged_as_match / promoted_masters).
-- Detect force-merge chains via master_merge_final to override reason/status.
merged_contributors_enriched AS (
    SELECT
        cc.master_{id_key},
        cc.{unified_id_key},
        cc.source_path,
        CASE
            WHEN om.original_action_type IN ('INITIAL_LOAD', 'JOB_INSERT') THEN '1.0'
            WHEN om.original_action_type IN ('JOB_MERGE', 'MANUAL_MERGE')  THEN COALESCE(srp.score, '1.0')
            ELSE '1.0'
        END AS score,
        CASE
            WHEN force_merge_chain.final_master_id IS NOT NULL
                AND force_merge_chain.action_type = 'MASTER_MANUAL_MERGE'
                THEN 'Masters merged manually'
            WHEN force_merge_chain.final_master_id IS NOT NULL
                AND force_merge_chain.action_type = 'MASTER_JOB_MERGE'
                THEN 'Masters merged automatically'
            WHEN force_merge_chain.final_master_id IS NOT NULL
                THEN 'Force merged'
            WHEN om.original_action_type = 'INITIAL_LOAD'  THEN 'Record initially loaded'
            WHEN om.original_action_type = 'JOB_INSERT'    THEN 'Record inserted'
            WHEN om.original_action_type = 'JOB_MERGE'
                THEN COALESCE(CONCAT('Record merged automatically - ', srp.reason), 'Merged contributor')
            WHEN om.original_action_type = 'MANUAL_MERGE'  THEN 'Record merged manually'
            ELSE 'Unknown'
        END AS reason,
        CASE
            WHEN force_merge_chain.final_master_id IS NOT NULL
                AND force_merge_chain.action_type = 'MASTER_MANUAL_MERGE'
                THEN 'MASTER_MANUAL_MERGE'
            WHEN force_merge_chain.final_master_id IS NOT NULL
                AND force_merge_chain.action_type = 'MASTER_JOB_MERGE'
                THEN 'MASTER_JOB_MERGE'
            WHEN force_merge_chain.final_master_id IS NOT NULL
                THEN 'MASTER_FORCE_MERGE'
            ELSE COALESCE(om.original_action_type, 'MERGED')
        END AS merge_status
    FROM current_contributors cc
    -- Exclude records that have been unmerged; they are fully
    -- handled by unmerged_as_match and promoted_masters in all scenarios.
    LEFT JOIN unmerged_records ur_excl
        ON cc.{unified_id_key} = ur_excl.{unified_id_key}
    LEFT JOIN original_merge om
        ON cc.{unified_id_key} = om.{unified_id_key}
    LEFT JOIN scoring_results_parsed srp
        ON  cc.{unified_id_key}   = srp.{unified_id_key}
        AND om.original_master_id = srp.target_lakefusion_id
    -- Detect force-merge only for records NOT currently in an unmerged state.
    -- If the record is currently unmerged, unmerged_as_match owns it — we must
    -- not let the stale force_merge_chain join produce a duplicate row.
    LEFT JOIN (
        SELECT
            match_id       AS old_master_id,
            master_id      AS final_master_id,
            action_type
        FROM (
            SELECT
                match_id,
                master_id,
                action_type,
                ROW_NUMBER() OVER (
                    PARTITION BY match_id
                    ORDER BY version DESC, created_at DESC
                ) AS rn
            FROM {merge_activities_table}
            WHERE action_type IN ('MASTER_FORCE_MERGE', 'MASTER_MANUAL_MERGE', 'MASTER_JOB_MERGE')
        )
        WHERE rn = 1
    ) force_merge_chain
        ON  om.original_master_id = force_merge_chain.old_master_id
        AND cc.master_{id_key}    = force_merge_chain.final_master_id
        AND ur_excl.{unified_id_key} IS NULL   -- only apply when not currently unmerged
    WHERE cc.record_status = 'MERGED'
      AND ur_excl.{unified_id_key} IS NULL   -- FIX 1: drop unmerged records
),
all_matches AS (
    -- Combine MERGED contributors, PENDING matches, unmerged views,
    -- and promoted master views.
    SELECT master_{id_key}, {unified_id_key}, source_path, score, reason, merge_status
    FROM merged_contributors_enriched

    UNION ALL

    -- Old master sees the record as 'Record unmerged' / MANUAL_UNMERGE
    SELECT master_{id_key}, {unified_id_key}, source_path, score, reason, merge_status
    FROM unmerged_as_match

    UNION ALL

    -- New promoted master sees 'Record unmerged and promoted to master'
    SELECT master_{id_key}, {unified_id_key}, source_path, score, reason, merge_status
    FROM promoted_masters

    {all_matches_union}
),
-- Deduplicate when the same surrogate_key appears multiple times under
-- the same master (e.g. record matched both B and C as POTENTIAL_MATCH, then
-- B and C merged into A — record shows up twice with different scores).
-- Keep the row with the highest score.
all_matches_deduped AS (
    SELECT master_{id_key}, {unified_id_key}, source_path, score, reason, merge_status
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY master_{id_key}, {unified_id_key}
                ORDER BY CAST(score AS DOUBLE) DESC
            ) AS _dedup_rn
        FROM all_matches
    )
    WHERE _dedup_rn = 1
),
matches_with_details AS (
    -- Join with unified table to get all entity attributes
    SELECT
        am.master_{id_key},
        u.{unified_id_key},
        u.source_path,
        u.source_id,
        {', '.join([f'u.{attr}' for attr in entity_attributes])},
        STRUCT(
            am.score as score,
            am.reason as reason,
            am.merge_status as merge_status
        ) as match_info
    FROM all_matches_deduped am
    INNER JOIN {unified_table} u
        ON am.{unified_id_key} = u.{unified_id_key}
),
aggregated_matches AS (
    -- Aggregate all matches per master
    SELECT
        master_{id_key},
        COLLECT_SET({named_struct_expr}) as potential_matches
    FROM matches_with_details u
    GROUP BY master_{id_key}
)
SELECT
    {master_select},
    m.attributes_combined as attributes_combined,
    COALESCE(am.potential_matches, ARRAY()) as potential_matches
FROM {master_table} m
LEFT JOIN aggregated_matches am
    ON m.{id_key} = am.master_{id_key}
ORDER BY m.{id_key}
"""

logger.info("Query built successfully")
logger.info(f"Master columns: {id_key}, {', '.join(entity_attributes)}, attributes_combined, potential_matches")
logger.info(f"Score/reason sourced from scoring_results for JOB_MERGE/MANUAL_MERGE")
logger.info(f"INITIAL_LOAD/JOB_INSERT get fixed reasons")
logger.info(f"Handles master merge chains by looking at original merge activity")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("EXECUTING QUERY")
logger.info("="*80)

potential_match_df = spark.sql(query)
total_count = potential_match_df.count()

logger.info(f"Generated potential match table")
logger.info(f"  Total master records: {total_count}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("ADDING COLUMN TAGS")
logger.info("="*80)

try:
    # Extract schema and table name for tags query
    schema_name = master_table.split('.')[1]
    table_name = master_table.split('.')[2]
    
    # Query column tags from information schema
    tags_query = f"""
    SELECT
        column_name,
        map_from_arrays(collect_list(tag_name), collect_list(tag_value)) AS tags_map
    FROM {catalog_name}.information_schema.column_tags
    WHERE schema_name = '{schema_name}'
        AND table_name = '{table_name}'
    GROUP BY column_name
    """
    
    column_tags_df = spark.sql(tags_query)
    
    if column_tags_df.count() > 0:
        # Create tags column for each entity attribute
        tags_expr = []
        for attr in entity_attributes:
            tags_expr.append(
                f"'{attr}', (SELECT tags_map FROM column_tags WHERE column_name = '{attr}')"
            )
        
        map_clause = f"map({', '.join(tags_expr)}) AS tags"
        
        # Add tags as CTE
        potential_match_with_tags_df = spark.sql(f"""
            WITH column_tags AS (
                {tags_query}
            ),
            pot_match AS (
                {query}
            )
            SELECT
                pot_match.*,
                {map_clause}
            FROM pot_match
        """)
        
        logger.info("Column tags added successfully")
    else:
        # No tags found, add empty tags map
        logger.info("No column tags found, using empty tags map")
        
        from pyspark.sql.functions import create_map, array
        
        tag_columns = []
        for attr in entity_attributes:
            tag_columns.extend([lit(attr), lit(None).cast("map<string,string>")])
        
        potential_match_with_tags_df = potential_match_df.withColumn(
            "tags",
            create_map(*tag_columns)
        )
        
except Exception as e:
    logger.warning(f"Error adding tags: {e}")
    logger.warning("Proceeding with empty tags column")
    
    from pyspark.sql.functions import create_map, array
    
    tag_columns = []
    for attr in entity_attributes:
        tag_columns.extend([lit(attr), lit(None).cast("map<string,string>")])
    
    potential_match_with_tags_df = potential_match_df.withColumn(
        "tags",
        create_map(*tag_columns)
    )

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("WRITING TO DELTA TABLE")
logger.info("="*80)

# Check if table exists
table_exists = spark.catalog.tableExists(potential_match_table)

if not table_exists:
    logger.info(f"Creating new table: {potential_match_table}")
    
    potential_match_with_tags_df.write \
        .format("delta") \
        .option("mergeSchema", "true") \
        .mode("overwrite") \
        .saveAsTable(potential_match_table)
    
    logger.info("Table created successfully")
else:
    logger.info(f"Updating existing table: {potential_match_table}")
    
    delta_table = DeltaTable.forName(spark, potential_match_table)
    
    delta_table.alias("target").merge(
        source=potential_match_with_tags_df.alias("source"),
        condition=f"target.{id_key} = source.{id_key}"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .whenNotMatchedBySourceDelete() \
     .execute()
    
    logger.info("Table updated successfully")

# COMMAND ----------

logger_instance.shutdown()
