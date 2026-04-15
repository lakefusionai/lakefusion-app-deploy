# Databricks notebook source
import json
from pyspark.sql.functions import (
    col, lit, collect_set, struct, concat, coalesce, map_from_arrays, collect_list,
    explode, array, create_map, count, sum as spark_sum, avg, max as spark_max,
    size, filter as spark_filter, when, array_union, array_distinct, flatten,
    row_number, first, greatest
)
from pyspark.sql.types import *
from pyspark.sql import Window
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
unified_dedup_table = f"{catalog_name}.silver.{entity}_unified_deduplicate"
master_table = f"{catalog_name}.gold.{entity}_master"
processed_unified_dedup_table = f"{catalog_name}.silver.{entity}_processed_unified_deduplicate"
potential_match_dedup_table = f"{catalog_name}.gold.{entity}_master_potential_match_deduplicate"

# Add experiment suffix if exists
if experiment_id:
    unified_dedup_table += f"_{experiment_id}"
    master_table += f"_{experiment_id}"
    processed_unified_dedup_table += f"_{experiment_id}"
    potential_match_dedup_table += f"_{experiment_id}"

# Related tables
merge_activities_table = f"{master_table}_merge_activities"
attribute_version_sources_table = f"{master_table}_attribute_version_sources"

# COMMAND ----------

id_key = 'lakefusion_id'

# COMMAND ----------

logger.info("="*80)
logger.info("GOLDEN DEDUP - POTENTIAL MATCH TABLE")
logger.info("="*80)
logger.info(f"Entity: {entity}")
logger.info(f"Master ID Key: {id_key}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment ID: {experiment_id or 'None'}")
logger.info(f"Unified Dedup Table: {unified_dedup_table}")
logger.info(f"Master Table: {master_table}")
logger.info(f"Processed Unified Dedup Table: {processed_unified_dedup_table}")
logger.info(f"Potential Match Dedup Table: {potential_match_dedup_table}")
logger.info(f"Merge Activities Table: {merge_activities_table}")
logger.info(f"Attribute Version Sources Table: {attribute_version_sources_table}")
logger.info("="*80)

# COMMAND ----------

table_exists = spark.catalog.tableExists(processed_unified_dedup_table)
if not table_exists:
    dbutils.notebook.exit("Table not exists, Skipping below steps")
else:
    logger.info("Table exists, Continue below steps")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 1: BUILD TRANSITIVE MERGE CHAIN")
logger.info("="*80)

# Get active masters - need to collect for transitive resolution logic
df_active_masters = spark.sql(f"SELECT {id_key} FROM {master_table}")
active_masters_set = set([row[id_key] for row in df_active_masters.collect()])
logger.info(f"Active masters loaded: {len(active_masters_set)}")

# Get all master-to-master merges
df_all_merges = spark.sql(f"""
    SELECT 
        master_id as immediate_survivor,
        match_id as merged_record,
        action_type,
        created_at
    FROM {merge_activities_table}
    WHERE action_type IN ('MASTER_JOB_MERGE', 'MASTER_MANUAL_MERGE', 'MASTER_FORCE_MERGE')
        AND master_id IS NOT NULL
        AND match_id IS NOT NULL
""")

# Build merge lookup - need to collect for transitive resolution
merge_lookup = {}
merge_info = {}

for row in df_all_merges.collect():
    merged = row['merged_record']
    survivor = row['immediate_survivor']
    action_type = row['action_type']
    created_at = row['created_at']
    
    if merged not in merge_info or created_at > merge_info[merged][2]:
        merge_lookup[merged] = survivor
        merge_info[merged] = (survivor, action_type, created_at)

logger.info(f"Merge relationships loaded: {len(merge_lookup)}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 2: RESOLVE ULTIMATE SURVIVORS (TRANSITIVE CLOSURE)")
logger.info("="*80)

def find_ultimate_survivor(merged_id, merge_lookup, active_masters_set, max_depth=10):
    """Follow the merge chain to find the ultimate survivor."""
    current = merged_id
    path = [merged_id]
    depth = 0
    
    while current in merge_lookup and depth < max_depth:
        next_survivor = merge_lookup[current]
        path.append(next_survivor)
        
        if next_survivor in active_masters_set:
            return next_survivor, path, len(path) > 2
        
        current = next_survivor
        depth += 1
    
    if current in active_masters_set:
        return current, path, len(path) > 2
    
    return path[-1] if path else None, path, len(path) > 2

# Build ultimate survivor mapping
ultimate_survivor_map = {}
merge_path_map = {}
transitive_flag_map = {}

for merged_id in merge_lookup.keys():
    ultimate, path, is_transitive = find_ultimate_survivor(merged_id, merge_lookup, active_masters_set)
    ultimate_survivor_map[merged_id] = ultimate
    merge_path_map[merged_id] = path
    transitive_flag_map[merged_id] = is_transitive

logger.info(f"Transitive closure resolved")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 3: BUILD MERGED RECORDS WITH PROPER REASONS")
logger.info("="*80)

merged_records_data = []

for merged_id, (immediate_survivor, action_type, created_at) in merge_info.items():
    ultimate_survivor = ultimate_survivor_map.get(merged_id, immediate_survivor)
    is_transitive = transitive_flag_map.get(merged_id, False)
    path = merge_path_map.get(merged_id, [merged_id, immediate_survivor])
    
    if ultimate_survivor in active_masters_set:
        merged_records_data.append({
            'ultimate_survivor': ultimate_survivor,
            'merged_record': merged_id,
            'immediate_survivor': immediate_survivor,
            'action_type': action_type,
            'is_transitive': is_transitive,
            'merge_depth': len(path) - 1
        })

schema = StructType([
    StructField('ultimate_survivor', StringType(), True),
    StructField('merged_record', StringType(), True),
    StructField('immediate_survivor', StringType(), True),
    StructField('action_type', StringType(), True),
    StructField('is_transitive', BooleanType(), True),
    StructField('merge_depth', IntegerType(), True)
])

df_merged_records = spark.createDataFrame(merged_records_data, schema)
logger.info(f"Merged records DataFrame created: {len(merged_records_data)} records")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 4: LOOKUP BEST MATCH SCORES FOR MERGED RECORDS")
logger.info("="*80)

# For MASTER_JOB_MERGE: Find the best MATCH score involving the merged record
best_match_scores_query = f"""
WITH all_matches AS (
    SELECT 
        query_{id_key} as record_id,
        match_{id_key} as partner_id,
        exploded_result.score as score,
        exploded_result.reason as reason
    FROM {processed_unified_dedup_table}
    WHERE exploded_result.match = 'MATCH'
    
    UNION ALL
    
    SELECT 
        match_{id_key} as record_id,
        query_{id_key} as partner_id,
        exploded_result.score as score,
        exploded_result.reason as reason
    FROM {processed_unified_dedup_table}
    WHERE exploded_result.match = 'MATCH'
),
ranked_matches AS (
    SELECT 
        record_id,
        partner_id,
        score,
        reason,
        ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY score DESC) as rn
    FROM all_matches
)
SELECT 
    record_id as merged_record,
    partner_id as matched_with,
    score as best_match_score,
    reason as best_match_reason
FROM ranked_matches
WHERE rn = 1
"""

df_best_match_scores = spark.sql(best_match_scores_query)
logger.info("Best match scores query prepared")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 5: LOOKUP DIRECT RELATIONSHIP SCORES")
logger.info("="*80)

direct_scores_query = f"""
WITH direct_forward AS (
    SELECT 
        query_{id_key} as survivor_id,
        match_{id_key} as merged_id,
        exploded_result.score as score,
        exploded_result.reason as reason,
        exploded_result.match as match_status
    FROM {processed_unified_dedup_table}
),
direct_reverse AS (
    SELECT 
        match_{id_key} as survivor_id,
        query_{id_key} as merged_id,
        exploded_result.score as score,
        exploded_result.reason as reason,
        exploded_result.match as match_status
    FROM {processed_unified_dedup_table}
)
SELECT * FROM direct_forward
UNION ALL
SELECT * FROM direct_reverse
"""

df_direct_scores = spark.sql(f"""
    WITH raw AS (
        {direct_scores_query}
    )
    SELECT
        survivor_id,
        merged_id,
        score,
        reason,
        match_status
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY survivor_id, merged_id
                ORDER BY score DESC
            ) AS rn
        FROM raw
    )
    WHERE rn = 1
""")
df_direct_scores.createOrReplaceTempView("direct_scores_temp")
logger.info("Direct scores query prepared")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 6: BUILD FINAL MERGED RECORDS WITH SCORES AND REASONS")
logger.info("="*80)

df_merged_records.createOrReplaceTempView("merged_records_temp")
df_best_match_scores.createOrReplaceTempView("best_match_scores_temp")

final_merged_query = f"""
WITH merged_with_scores AS (
    SELECT 
        mr.ultimate_survivor,
        mr.merged_record,
        mr.immediate_survivor,
        mr.action_type,
        mr.is_transitive,
        mr.merge_depth,
        bms.best_match_score,
        bms.best_match_reason,
        bms.matched_with,
        ds.score as direct_score,
        ds.reason as direct_reason,
        ds.match_status as direct_match_status
    FROM merged_records_temp mr
    LEFT JOIN best_match_scores_temp bms
        ON mr.merged_record = bms.merged_record
    LEFT JOIN direct_scores_temp ds
        ON mr.ultimate_survivor = ds.survivor_id
        AND mr.merged_record = ds.merged_id
)
SELECT
    ultimate_survivor,
    merged_record,
    action_type,
    is_transitive,
    merge_depth,
    CASE
        WHEN action_type = 'MASTER_FORCE_MERGE' THEN 1.0
        WHEN action_type = 'MASTER_MANUAL_MERGE' THEN COALESCE(direct_score, best_match_score, 1.0)
        WHEN action_type = 'MASTER_JOB_MERGE' THEN COALESCE(best_match_score, direct_score, 1.0)
        ELSE COALESCE(best_match_score, direct_score, 1.0)
    END as display_score,
    CASE
        WHEN action_type = 'MASTER_FORCE_MERGE' THEN 
            'Force merged'
        WHEN action_type = 'MASTER_MANUAL_MERGE' AND direct_reason IS NOT NULL THEN
            CONCAT('Masters merged manually - ', direct_reason)
        WHEN action_type = 'MASTER_MANUAL_MERGE' THEN
            'Masters merged manually'
        WHEN action_type = 'MASTER_JOB_MERGE' AND is_transitive = true AND best_match_reason IS NOT NULL THEN
            CONCAT('Merged via connected records - ', best_match_reason)
        WHEN action_type = 'MASTER_JOB_MERGE' AND is_transitive = true THEN
            'Merged via connected component (transitive)'
        WHEN action_type = 'MASTER_JOB_MERGE' AND best_match_reason IS NOT NULL THEN
            CONCAT('Masters merged automatically - ', best_match_reason)
        WHEN action_type = 'MASTER_JOB_MERGE' AND direct_reason IS NOT NULL THEN
            CONCAT('Masters merged automatically - ', direct_reason)
        WHEN action_type = 'MASTER_JOB_MERGE' THEN
            'Masters merged automatically (high confidence match)'
        ELSE 'Merged'
    END as display_reason
FROM merged_with_scores
"""

df_final_merged = spark.sql(final_merged_query)
df_final_merged.createOrReplaceTempView("final_merged_temp")
logger.info("Final merged records query prepared")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 7: GET NOT_A_MATCH PAIRS TO EXCLUDE")
logger.info("="*80)

not_a_match_query = f"""
SELECT DISTINCT
    ma.master_id as master1_{id_key},
    ma.match_id as master2_{id_key}
FROM {merge_activities_table} ma
WHERE ma.action_type = 'MANUAL_NOT_A_MATCH'
    AND ma.master_id IS NOT NULL
    AND ma.match_id IS NOT NULL
"""

logger.info("NOT_A_MATCH query prepared")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 8: BUILD PENDING AND MERGED RELATIONSHIPS")
logger.info("="*80)

pending_matches_query = f"""
WITH not_a_match_bidirectional AS (
    SELECT master1_{id_key}, master2_{id_key} FROM ({not_a_match_query})
    UNION
    SELECT master2_{id_key} as master1_{id_key}, master1_{id_key} as master2_{id_key}
    FROM ({not_a_match_query})
),
active_masters AS (
    SELECT {id_key} FROM {master_table}
)
SELECT DISTINCT
    pu.query_{id_key} as master1_{id_key},
    pu.match_{id_key} as master2_{id_key},
    pu.exploded_result.score as score,
    pu.exploded_result.reason as reason,
    'PENDING' as merge_status
FROM {processed_unified_dedup_table} pu
-- Both query and match must be active masters
INNER JOIN active_masters am1 ON pu.query_{id_key} = am1.{id_key}
INNER JOIN active_masters am2 ON pu.match_{id_key} = am2.{id_key}
-- Exclude NOT_A_MATCH pairs
LEFT JOIN not_a_match_bidirectional nam
    ON pu.query_{id_key} = nam.master1_{id_key}
    AND pu.match_{id_key} = nam.master2_{id_key}
WHERE pu.exploded_result.match = 'POTENTIAL_MATCH'
    AND nam.master1_{id_key} IS NULL
"""

# Build merged matches from final_merged
merged_matches_query = f"""
SELECT
    ultimate_survivor as master1_{id_key},
    merged_record as master2_{id_key},
    display_score as score,
    display_reason as reason,
    CASE
        WHEN action_type IS NOT NULL THEN action_type
        ELSE 'MERGED'
    END as merge_status
    FROM final_merged_temp
"""

# Combine pending and merged
all_relationships_query = f"""
WITH pending AS (
    {pending_matches_query}
),
merged AS (
    {merged_matches_query}
)
SELECT * FROM pending
UNION ALL
SELECT * FROM merged
"""

logger.info("Relationships queries prepared")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 9: GET MASTER ATTRIBUTES FOR RELATED RECORDS")
logger.info("="*80)

# Get attribute values from attribute_version_sources for merged masters
merged_master_attributes_query = f"""
WITH merged_ids AS (
    SELECT DISTINCT merged_record as {id_key}
    FROM final_merged_temp
),
latest_versions AS (
    SELECT
        avs.{id_key},
        avs.version,
        avs.attribute_source_mapping,
        ROW_NUMBER() OVER (PARTITION BY avs.{id_key} ORDER BY avs.version DESC) as rn
    FROM {attribute_version_sources_table} avs
    INNER JOIN merged_ids mi ON avs.{id_key} = mi.{id_key}
)
SELECT
    {id_key},
    version,
    attribute_source_mapping
FROM latest_versions
WHERE rn = 1
"""

# Build pivot query for merged master attributes
pivot_cols = ", ".join([
    f"MAX(CASE WHEN attribute_name = '{attr}' THEN attribute_value END) as {attr}"
    for attr in entity_attributes
])

merged_attrs_flattened_query = f"""
WITH latest_merged_attrs AS (
    {merged_master_attributes_query}
)
SELECT
    {id_key},
    exploded.attribute_name,
    exploded.attribute_value
FROM latest_merged_attrs
LATERAL VIEW EXPLODE(attribute_source_mapping) AS exploded
"""

merged_masters_reconstructed_query = f"""
WITH flattened AS (
    {merged_attrs_flattened_query}
)
SELECT
    {id_key},
    {pivot_cols}
FROM flattened
GROUP BY {id_key}
"""

logger.info("Attribute queries prepared")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 10: JOIN RELATIONSHIPS WITH MASTER ATTRIBUTES")
logger.info("="*80)

active_master_cols = [f"CAST(m.{attr} AS STRING) as {attr}" for attr in entity_attributes]
active_master_select = ", ".join([f"m.{id_key}"] + active_master_cols + ["m.attributes_combined"])

join_query = f"""
WITH relationships AS (
    {all_relationships_query}
),
active_masters AS (
    SELECT
        {active_master_select}
    FROM {master_table} m
),
merged_masters_reconstructed AS (
    {merged_masters_reconstructed_query}
),
all_masters AS (
    SELECT * FROM active_masters
    UNION ALL
    SELECT
        mr.{id_key},
        {', '.join([f'mr.{attr}' for attr in entity_attributes])},
        NULL as attributes_combined
    FROM merged_masters_reconstructed mr
    WHERE mr.{id_key} NOT IN (SELECT {id_key} FROM active_masters)
)
SELECT
    r.master1_{id_key},
    r.master2_{id_key},
    r.score,
    r.reason,
    r.merge_status,
    md2.{id_key},
    {', '.join([f'md2.{attr}' for attr in entity_attributes])},
    COALESCE(md2.attributes_combined, '') as attributes_combined
FROM relationships r
INNER JOIN all_masters md2
    ON r.master2_{id_key} = md2.{id_key}
"""

logger.info("Join query prepared")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 11: BUILD POTENTIAL MATCHES STRUCT")
logger.info("="*80)

# Build named_struct for each match
named_struct_parts = []

for attr in entity_attributes:
    named_struct_parts.append(f"'{attr}', {attr}")

named_struct_parts.extend([
    "'__score__', CAST(score AS STRING)",
    "'__reason__', reason",
    f"'{id_key}', {id_key}",
    "'__mergestatus__', merge_status",
    "'__attributes_combined__', attributes_combined"
])

named_struct_expr = f"NAMED_STRUCT({', '.join(named_struct_parts)})"

# Aggregate matches per master
aggregate_query = f"""
WITH relationships_with_attrs AS (
    {join_query}
)
SELECT
    master1_{id_key} as {id_key},
    COLLECT_SET({named_struct_expr}) as potential_matches
FROM relationships_with_attrs
GROUP BY master1_{id_key}
"""

logger.info("Aggregate query prepared")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 12: BUILD FINAL POTENTIAL MATCH TABLE")
logger.info("="*80)

master_attr_list = [f"m.{attr}" for attr in entity_attributes]
master_select = ", ".join([f"m.{id_key}"] + master_attr_list + ["m.attributes_combined"])

final_query = f"""
WITH aggregated AS (
    {aggregate_query}
)
SELECT
    {master_select},
    COALESCE(a.potential_matches, ARRAY()) as potential_matches
FROM {master_table} m
LEFT JOIN aggregated a
    ON m.{id_key} = a.{id_key}
ORDER BY m.{id_key}
"""

df_potential_match = spark.sql(final_query)
logger.info("Final potential match DataFrame created")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 13: ADD COLUMN TAGS")
logger.info("="*80)

try:
    schema_name = master_table.split('.')[1]
    table_name = master_table.split('.')[2]
    
    tags_query = f"""
    SELECT
        column_name,
        MAP_FROM_ARRAYS(COLLECT_LIST(tag_name), COLLECT_LIST(tag_value)) AS tags_map
    FROM {catalog_name}.information_schema.column_tags
    WHERE schema_name = '{schema_name}'
        AND table_name = '{table_name}'
    GROUP BY column_name
    """
    
    column_tags_df = spark.sql(tags_query)
    
    if column_tags_df.take(1):  # More efficient than count() > 0
        tags_expr = []
        for attr in entity_attributes:
            tags_expr.append(
                f"'{attr}', (SELECT tags_map FROM column_tags WHERE column_name = '{attr}')"
            )
        
        map_clause = f"MAP({', '.join(tags_expr)}) AS tags"
        
        df_potential_match_with_tags = spark.sql(f"""
            WITH column_tags AS (
                {tags_query}
            ),
            pot_match AS (
                {final_query}
            )
            SELECT
                pot_match.*,
                {map_clause}
            FROM pot_match
        """)
        
        logger.info("Column tags added")
    else:
        logger.info("No column tags found, using empty tags map")
        
        tag_columns = []
        for attr in entity_attributes:
            tag_columns.extend([lit(attr), lit(None).cast("map<string,string>")])
        
        df_potential_match_with_tags = df_potential_match.withColumn(
            "tags",
            create_map(*tag_columns)
        )
        
except Exception as e:
    logger.warning(f"Error adding tags: {e}, proceeding with empty tags")
    
    tag_columns = []
    for attr in entity_attributes:
        tag_columns.extend([lit(attr), lit(None).cast("map<string,string>")])
    
    df_potential_match_with_tags = df_potential_match.withColumn(
        "tags",
        create_map(*tag_columns)
    )

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 14: WRITE TO DELTA TABLE")
logger.info("="*80)

table_exists = spark.catalog.tableExists(potential_match_dedup_table)

if not table_exists:
    logger.info(f"  Creating new table: {potential_match_dedup_table}")
    
    df_potential_match_with_tags.write \
        .format("delta") \
        .option("mergeSchema", "true") \
        .mode("overwrite") \
        .saveAsTable(potential_match_dedup_table)
    
    logger.info("Table created successfully")
else:
    logger.info(f"  Updating existing table: {potential_match_dedup_table}")
    
    delta_table = DeltaTable.forName(spark, potential_match_dedup_table)
    
    delta_table.alias("target").merge(
        source=df_potential_match_with_tags.alias("source"),
        condition=f"target.{id_key} = source.{id_key}"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .whenNotMatchedBySourceDelete() \
     .execute()
    
    logger.info("Table updated successfully")

# COMMAND ----------

# Set task values
dbutils.jobs.taskValues.set("potential_match_complete", True)

logger.info("\n" + "="*80)
logger.info("GOLDEN DEDUP POTENTIAL MATCH TABLE COMPLETED")
logger.info("="*80)
logger.info(f"Table: {potential_match_dedup_table}")
logger.info("="*80)

# COMMAND ----------

logger_instance.shutdown()
