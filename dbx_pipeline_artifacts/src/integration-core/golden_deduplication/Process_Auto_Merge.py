# Databricks notebook source
import json
from datetime import datetime
from pyspark.sql.functions import (
    col, explode, lit, collect_list, collect_set, first, struct, count, row_number,
    max as spark_max, min as spark_min, current_timestamp, arrays_zip, udf, 
    concat_ws, coalesce, size, array_distinct, flatten, when
)
from pyspark.sql.types import *
from pyspark.sql import Window
from delta.tables import DeltaTable
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, TimestampType
)

# COMMAND ----------

dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")
dbutils.widgets.text("entity_attributes_datatype", "", "Entity Attributes Datatype")
dbutils.widgets.text("default_survivorship_rules", "", "Survivorship Rules")
dbutils.widgets.text("dataset_objects", "", "Dataset Objects")
dbutils.widgets.text("match_attributes", "", "Match Attributes")

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
entity_attributes_datatype = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "entity_attributes_datatype",
    debugValue=dbutils.widgets.get("entity_attributes_datatype")
)
default_survivorship_rules = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "default_survivorship_rules",
    debugValue=dbutils.widgets.get("default_survivorship_rules")
)
dataset_objects = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "dataset_objects",
    debugValue=dbutils.widgets.get("dataset_objects")
)
match_attributes = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="match_attributes",
    debugValue=dbutils.widgets.get("match_attributes")
)

# COMMAND ----------

# Parse JSON parameters
entity_attributes = json.loads(entity_attributes)
entity_attributes_datatype = json.loads(entity_attributes_datatype)
default_survivorship_rules = json.loads(default_survivorship_rules)
dataset_objects = json.loads(dataset_objects)
match_attributes = json.loads(match_attributes)

# COMMAND ----------

id_key = 'lakefusion_id'

# COMMAND ----------

unified_id_key = 'surrogate_key'

# COMMAND ----------

# Base table names
unified_table = f"{catalog_name}.silver.{entity}_unified"
unified_dedup_table = f"{catalog_name}.silver.{entity}_unified_deduplicate"
processed_unified_dedup_table = f"{catalog_name}.silver.{entity}_processed_unified_deduplicate"
master_table = f"{catalog_name}.gold.{entity}_master"

# Add experiment suffix if exists
if experiment_id:
    unified_table += f"_{experiment_id}"
    unified_dedup_table += f"_{experiment_id}"
    processed_unified_dedup_table += f"_{experiment_id}"
    master_table += f"_{experiment_id}"

# Related tables
merge_activities_table = f"{master_table}_merge_activities"
attribute_version_sources_table = f"{master_table}_attribute_version_sources"

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

logger.info("="*80)
logger.info("GOLDEN DEDUP - AUTO MERGE PROCESSING")
logger.info("="*80)
logger.info(f"Entity: {entity}")
logger.info(f"Master ID Key: {id_key}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment ID: {experiment_id or 'None'}")
logger.info(f"Unified Table: {unified_table}")
logger.info(f"Unified Dedup Table: {unified_dedup_table}")
logger.info(f"Processed Unified Dedup Table: {processed_unified_dedup_table}")
logger.info(f"Master Table: {master_table}")
logger.info(f"Merge Activities Table: {merge_activities_table}")
logger.info(f"Attribute Version Sources Table: {attribute_version_sources_table}")
logger.info("="*80)

# COMMAND ----------

from lakefusion_core_engine.survivorship import SurvivorshipEngine, __version__

logger.info(f"Survivorship Engine Version: {__version__}")

# COMMAND ----------

# Create mapping from dataset ID to path
dataset_id_to_path = {dataset['id']: dataset['path'] for dataset in dataset_objects.values()}

# Convert Source System strategy rules from IDs to paths
for rule in default_survivorship_rules:
    if rule.get('strategy') == 'Source System' and 'strategyRule' in rule:
        rule['strategyRule'] = [
            dataset_id_to_path.get(dataset_id) 
            for dataset_id in rule['strategyRule']
        ]

logger.info("Survivorship rules updated with dataset paths")
logger.info(f"  Rules: {len(default_survivorship_rules)}")

# COMMAND ----------

engine = SurvivorshipEngine(
    survivorship_config=default_survivorship_rules,
    entity_attributes=entity_attributes,
    entity_attributes_datatype=entity_attributes_datatype,
    id_key=unified_id_key
)

logger.info("Survivorship Engine initialized")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 1: FILTER MATCH RECORDS FROM PROCESSED_UNIFIED_DEDUP")
logger.info("="*80)

# Get all MATCH records (auto-merge candidates) from processed unified dedup
df_all_matches = spark.sql(f"""
    SELECT 
        query_lakefusion_id,
        match_lakefusion_id,
        exploded_result.score as match_score,
        exploded_result.reason as match_reason
    FROM {processed_unified_dedup_table}
    WHERE exploded_result.match = 'MATCH'
""")

total_match_count = df_all_matches.count()
logger.info(f"  Total MATCH records in processed_unified_dedup: {total_match_count}")

# Get existing merge relationships from merge_activities
# We need to check both directions since A->B and B->A represent the same merge
df_existing_merges = spark.sql(f"""
    SELECT DISTINCT
        master_id,
        match_id
    FROM {merge_activities_table}
    WHERE action_type IN ('MASTER_JOB_MERGE', 'MASTER_MANUAL_MERGE', 'MASTER_FORCE_MERGE')
""")

# Commenting as not used in logic elsewhere
# existing_merge_count = df_existing_merges.count()
# print(f"  Existing merge relationships: {existing_merge_count}")

# Filter out pairs that already exist in merge_activities
# Check both directions: (query->match) and (match->query) since merges are bidirectional
df_matches = df_all_matches.alias("m").join(
    df_existing_merges.alias("e"),
    on=(
        # Check if this pair exists in either direction
        ((col("m.query_lakefusion_id") == col("e.master_id")) & (col("m.match_lakefusion_id") == col("e.match_id"))) |
        ((col("m.query_lakefusion_id") == col("e.match_id")) & (col("m.match_lakefusion_id") == col("e.master_id")))
    ),
    how="left_anti"  # Keep only records that DON'T have a match in existing merges
)

match_count = df_matches.count()
already_merged_count = total_match_count - match_count
logger.info(f"  Already merged pairs (skipped): {already_merged_count}")
logger.info(f"Found {match_count} NEW MATCH records for auto-merge")

if match_count == 0:
    logger.info("No NEW MATCH records to process - all pairs already merged")
    
    dbutils.jobs.taskValues.set("auto_merge_complete", True)
    dbutils.jobs.taskValues.set("records_merged", 0)
    dbutils.jobs.taskValues.set("already_merged_skipped", already_merged_count)
    
    dbutils.notebook.exit(json.dumps({
        "status": "skipped",
        "message": "No new MATCH records to process - all pairs already merged",
        "records_merged": 0,
        "already_merged_skipped": already_merged_count
    }))

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 2: BUILD UNDIRECTED GRAPH (HANDLE BIDIRECTIONAL MATCHES)")
logger.info("="*80)

# Create edges in both directions to treat graph as undirected
# This handles: A→B, B→A as the same relationship
df_edges_forward = df_matches.select(
    col("query_lakefusion_id").alias("node1"),
    col("match_lakefusion_id").alias("node2")
)

df_edges_backward = df_matches.select(
    col("match_lakefusion_id").alias("node1"),
    col("query_lakefusion_id").alias("node2")
)

# Union and deduplicate to create undirected edges
df_edges = df_edges_forward.union(df_edges_backward).distinct()

# Commenting as not used in logic elsewhere
# edge_count = df_edges.count()
# print(f"✓ Created {edge_count} undirected edges")
# print(f"  Original matches: {match_count}")
# print(f"  After deduplication: {edge_count}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 3: FIND CONNECTED COMPONENTS (GROUP TRANSITIVE MATCHES)")
logger.info("="*80)

# Use GraphFrames for connected components if available, otherwise optimized algorithm
try:
    from graphframes import GraphFrame
    
    # Create nodes DataFrame
    df_nodes = df_edges.select("node1").union(df_edges.select("node2")).distinct() \
        .withColumnRenamed("node1", "id")
    
    # Create graph
    graph = GraphFrame(df_nodes, df_edges.withColumnRenamed("node1", "src").withColumnRenamed("node2", "dst"))
    
    # Find connected components
    df_components = graph.connectedComponents()
    
    logger.info("Using GraphFrames for connected components")
    
except ImportError:
    logger.info("GraphFrames not available, using optimized broadcast algorithm")
    
    # ULTRA-EFFICIENT CONNECTED COMPONENTS FOR SMALL DATASETS
    # Collect edges to driver (fine for small datasets like 68 records)
    edges_list = [(row['node1'], row['node2']) for row in df_edges.collect()]
    
    logger.info(f"  Processing {len(edges_list)} edges in memory...")
    
    # Union-Find algorithm (in-memory, very fast)
    parent = {}
    
    def find(x):
        if x not in parent:
            parent[x] = x
        if parent[x] != x:
            parent[x] = find(parent[x])  # Path compression
        return parent[x]
    
    def union(x, y):
        px, py = find(x), find(y)
        if px != py:
            parent[px] = py
    
    # Build union-find structure
    for node1, node2 in edges_list:
        union(node1, node2)
    
    # Get final components
    components = {}
    for node in parent.keys():
        root = find(node)
        if root not in components:
            components[root] = []
        components[root].append(node)
    
    # Convert back to DataFrame
    component_data = []
    for root, members in components.items():
        for member in members:
            component_data.append((member, root))
    
    df_components = spark.createDataFrame(component_data, ["id", "component"])
    
    logger.info(f"Processed {len(parent)} nodes in {len(components)} components")

# Rename to lakefusion_id for clarity
df_components = df_components.withColumnRenamed("id", id_key)

# Count groups
num_components = df_components.select("component").distinct().count()
total_records = df_components.count()

logger.info(f"Found {num_components} connected components")
logger.info(f"Total records in groups: {total_records}")
logger.info(f"Average group size: {total_records / num_components if num_components > 0 else 0:.2f}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 4: SELECT SURVIVOR FOR EACH GROUP")
logger.info("="*80)

# Get first appearance timestamp for each lakefusion_id from merge_activities
df_first_appearance = spark.sql(f"""
    SELECT 
        master_id as {id_key},
        MIN(created_at) as first_seen
    FROM {merge_activities_table}
    GROUP BY master_id
""")

# Get current contributor count for each master
df_contributor_count = spark.sql(f"""
    SELECT 
        master_lakefusion_id as {id_key},
        COUNT(DISTINCT surrogate_key) as contributor_count
    FROM {unified_table}
    WHERE record_status != 'DELETED'
        AND master_lakefusion_id IS NOT NULL
    GROUP BY master_lakefusion_id
""")

# Join components with metadata
df_components_with_metadata = df_components \
    .join(df_first_appearance, id_key, "left") \
    .join(df_contributor_count, id_key, "left") \
    .fillna({"contributor_count": 0})

# Rank within each component
# Priority: 1) Oldest (earliest first_seen), 2) Most contributors, 3) Smallest lakefusion_id
window_survivor = Window.partitionBy("component").orderBy(
    col("first_seen").asc_nulls_last(),
    col("contributor_count").desc(),
    col(id_key).asc()
)

df_survivors = df_components_with_metadata \
    .withColumn("rank", row_number().over(window_survivor)) \
    .filter(col("rank") == 1) \
    .select("component", col(id_key).alias("survivor_id"), "first_seen", "contributor_count")

survivor_count = df_survivors.count()
logger.info(f"Selected {survivor_count} survivors (one per group)")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 5: IDENTIFY RECORDS TO BE MERGED (NON-SURVIVORS)")
logger.info("="*80)

# Get all members of each group with their survivor
df_merge_plan = df_components \
    .join(df_survivors, "component") \
    .filter(col(id_key) != col("survivor_id"))  # Exclude survivor itself

records_to_merge = df_merge_plan.count()
logger.info(f"Identified {records_to_merge} records to be merged into survivors")

if records_to_merge == 0:
    logger.info("All groups have only 1 member - nothing to merge")
    
    dbutils.jobs.taskValues.set("auto_merge_complete", True)
    dbutils.jobs.taskValues.set("records_merged", 0)
    
    dbutils.notebook.exit(json.dumps({
        "status": "completed",
        "message": "No records to merge (all singletons)",
        "records_merged": 0
    }))

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 6: GET ALL CONTRIBUTORS FOR EACH GROUP")
logger.info("="*80)

# For each group member (including survivor), get their current contributors from unified table
df_all_contributors = spark.sql(f"""
    SELECT 
        master_lakefusion_id as {id_key},
        surrogate_key,
        source_path,
        source_id
    FROM {unified_table}
    WHERE record_status != 'DELETED'
        AND master_lakefusion_id IS NOT NULL
""")

# Get survivor's current contributors
df_survivor_contributors = df_merge_plan.select("survivor_id", "component").distinct() \
    .join(df_all_contributors, col("survivor_id") == col(id_key)) \
    .select("component", "survivor_id", "surrogate_key", "source_path", "source_id")

# Get merging records' contributors
df_merging_contributors = df_merge_plan \
    .join(df_all_contributors, id_key) \
    .select("component", "survivor_id", "surrogate_key", "source_path", "source_id")

# Union all contributors per survivor
df_combined_contributors = df_survivor_contributors.union(df_merging_contributors).distinct()

contributor_count = df_combined_contributors.count()
logger.info(f"Retrieved {contributor_count} total contributors across all groups")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 7: PREPARE UNIFIED RECORDS FOR SURVIVORSHIP")
logger.info("="*80)

# Get full unified records for all contributors
df_unified_records = spark.table(unified_table).filter(col("record_status") != "DELETED")

# Create temp view for combined contributors
df_combined_contributors.createOrReplaceTempView("combined_contributors_temp")

# Simpler approach: use DataFrame operations with explicit column references
all_contributors_df = df_combined_contributors \
    .select(
        col("survivor_id").alias("survivor_lakefusion_id"),
        col("surrogate_key").alias("contributor_surrogate_key")
    ) \
    .join(
        df_unified_records,
        col("contributor_surrogate_key") == df_unified_records[unified_id_key]
    ) \
    .select(
        col("survivor_lakefusion_id").alias(id_key),
        col("contributor_surrogate_key").alias(unified_id_key),
        "source_path",
        *entity_attributes
    )

# Commenting as not used in logic elsewhere
# total_contributors = all_contributors_df.count()
# print(f"✓ Total contributors for survivorship: {total_contributors}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 8: GROUP CONTRIBUTORS BY SURVIVOR")
logger.info("="*80)

# Group all contributors by survivor lakefusion_id
grouped_contributors = (
    all_contributors_df
    .groupBy(id_key)
    .agg(
        collect_list(
            struct([col(c) for c in all_contributors_df.columns if c != id_key])
        ).alias("unified_records")
    )
)

# Commenting as not used in logic elsewhere
# grouped_count = grouped_contributors.count()
# print(f"✓ Grouped into {grouped_count} survivors")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 9: APPLY SURVIVORSHIP ENGINE")
logger.info("="*80)

# Define UDF wrapper
def apply_survivorship_wrapper(unified_records):
    """
    UDF wrapper for survivorship engine.
    Converts Spark Rows to dicts and applies survivorship.
    """
    # Convert Spark Rows to dictionaries
    records_list = [r.asDict() for r in unified_records]
    
    # Apply survivorship
    result = engine.apply_survivorship(unified_records=records_list)
    
    # Return as dict for Spark
    return result.to_dict()

# Define output schema
udf_output_schema = StructType([
    StructField("resultant_record", MapType(StringType(), StringType())),
    StructField("resultant_master_attribute_source_mapping", ArrayType(StructType([
        StructField("attribute_name", StringType()),
        StructField("attribute_value", StringType()),
        StructField("source", StringType())
    ])))
])

# Register UDF
survivorship_udf = udf(apply_survivorship_wrapper, udf_output_schema)

logger.info("UDF registered")

# Apply survivorship UDF
result_df = grouped_contributors.withColumn(
    "survivorship_result",
    survivorship_udf(col("unified_records"))
)

# Extract results
final_results_df = result_df.select(
    col(id_key),
    col("survivorship_result.resultant_record").alias("merged_record"),
    col("survivorship_result.resultant_master_attribute_source_mapping").alias("attribute_sources")
)

logger.info(f"Survivorship applied to {final_results_df.count()} survivors")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 10: UPDATE MASTER TABLE WITH MERGED RECORDS")
logger.info("="*80)

# Prepare master table updates with proper type casting
master_updates_cols = [col(id_key)]

for attr in entity_attributes:
    if attr == id_key:
        continue
    
    # Get the target data type for this attribute
    target_type = entity_attributes_datatype.get(attr, "string")
    
    # Cast the string value from merged_record to proper type
    if target_type.lower() in ["timestamp", "date", "datetime"]:
        master_updates_cols.append(
            col(f"merged_record.{attr}").cast("timestamp").alias(attr)
        )
    elif target_type.lower() in ["int", "integer", "long", "bigint", "smallint", "tinyint"]:
        # Fix: Cast to double first, then to long to handle decimal strings like "28.0"
        master_updates_cols.append(
            col(f"merged_record.{attr}").cast("double").cast("long").alias(attr)
        )
    elif target_type.lower() in ["float", "double", "decimal"]:
        master_updates_cols.append(
            col(f"merged_record.{attr}").cast("double").alias(attr)
        )
    elif target_type.lower() in ["boolean", "bool"]:
        master_updates_cols.append(
            col(f"merged_record.{attr}").cast("boolean").alias(attr)
        )
    else:
        # Default to string
        master_updates_cols.append(
            col(f"merged_record.{attr}").alias(attr)
        )

# Add attributes_combined generation using match_attributes
if match_attributes:
    # Build concat expression for match attributes
    concat_cols = []
    for attr in match_attributes:
        if attr in entity_attributes and attr != id_key:
            concat_cols.append(coalesce(col(f"merged_record.{attr}").cast("string"), lit("")))
    
    if concat_cols:
        master_updates_cols.append(concat_ws(" | ", *concat_cols).alias("attributes_combined"))
    else:
        master_updates_cols.append(lit("").alias("attributes_combined"))
else:
    master_updates_cols.append(lit("").alias("attributes_combined"))

# Apply selections
master_updates_df = final_results_df.select(*master_updates_cols)

# Commenting as not used in logic elsewhere
# updates_count = master_updates_df.count()
# print(f"  Prepared {updates_count} master updates")

# Update master table using Delta merge
master_delta = DeltaTable.forName(spark, master_table)

master_delta.alias("target").merge(
    source=master_updates_df.alias("source"),
    condition=f"target.{id_key} = source.{id_key}"
).whenMatchedUpdateAll().execute()

# print(f"✓ Updated {updates_count} survivor records in master table")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 11: DELETE MERGED RECORDS FROM MASTER TABLE")
logger.info("="*80)

# Get list of all records that were merged (non-survivors)
merged_ids = [row[id_key] for row in df_merge_plan.select(id_key).distinct().collect()]

logger.info(f"  Deleting {len(merged_ids)} merged records from master table")

# Delete from master table
if merged_ids:
    master_delta = DeltaTable.forName(spark, master_table)
    
    # Build delete condition with proper quote escaping
    quoted_ids = [f"'{id}'" for id in merged_ids]
    delete_condition = f"{id_key} IN ({','.join(quoted_ids)})"
    
    master_delta.delete(delete_condition)
    
    logger.info(f"Deleted {len(merged_ids)} merged records from master table")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 12: LOG MERGE ACTIVITIES")
logger.info("="*80)

from datetime import datetime

# Prepare merge activities logs
merge_activities = []
current_time = datetime.now()

# Get the current version of the master table
master_table_version = spark.sql(f"DESCRIBE HISTORY {master_table} LIMIT 1").collect()[0]["version"]

logger.info(f"Master table current version: {master_table_version}")

# Rewiring the code
# for row in df_merge_plan.collect():
#     merge_activities.append({
#         "master_id": row["survivor_id"],
#         "match_id": row[id_key],
#         "source": "",
#         "version": master_table_version,
#         "action_type": "MASTER_JOB_MERGE",
#         "created_at": current_time
#     })

# # Define schema explicitly to match merge_activities table schema
# merge_activities_schema = StructType([
#     StructField("master_id", StringType(), True),
#     StructField("match_id", StringType(), True),
#     StructField("source", StringType(), True),
#     StructField("version", IntegerType(), False),
#     StructField("action_type", StringType(), False),
#     StructField("created_at", TimestampType(), False)
# ])

# Create DataFrame with explicit schema
# df_merge_activities = spark.createDataFrame(merge_activities, schema=merge_activities_schema)


merge_activities_schema = StructType([
    StructField("master_id", StringType(), True),
    StructField("match_id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("version", IntegerType(), False),
    StructField("action_type", StringType(), False),
    StructField("created_at", TimestampType(), False)
])

merge_activities_df = (
    df_merge_plan
        .select(
            col("survivor_id").alias("master_id"),
            col(id_key).alias("match_id"),
            lit("").alias("source"),
            lit(master_table_version).alias("version"),
            lit("MASTER_JOB_MERGE").alias("action_type"),
            lit(current_time).alias("created_at")
        )
)

df_merge_activities = (
    merge_activities_df
        .select(
            col("master_id").cast(StringType()).alias("master_id"),
            col("match_id").cast(StringType()).alias("match_id"),
            col("source").cast(StringType()).alias("source"),
            col("version").cast(IntegerType()).alias("version"),
            col("action_type").cast(StringType()).alias("action_type"),
            col("created_at").cast(TimestampType()).alias("created_at")
        )
)




# Append to merge_activities table
df_merge_activities.write.mode("append").saveAsTable(merge_activities_table)

logger.info(f"Logged {len(merge_activities)} merge activities with version {master_table_version}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 13: UPDATE ATTRIBUTE VERSION SOURCES")
logger.info("="*80)

# The attribute_version_sources table has this schema:
# - lakefusion_id: string
# - version: integer (Delta table version of master table)
# - attribute_source_mapping: array<struct<attribute_name, attribute_value, source>>

# Prepare DataFrame with correct schema
attribute_sources_df = final_results_df.select(
    col(id_key).alias("lakefusion_id"),
    lit(master_table_version).cast("integer").alias("version"),  # Use master table version
    col("attribute_sources").alias("attribute_source_mapping")
)

version_sources_count = attribute_sources_df.count()

# Append to attribute_version_sources table
attribute_sources_df.write.mode("append").saveAsTable(attribute_version_sources_table)

logger.info(f"Logged {version_sources_count} attribute version source records with version {master_table_version}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 14: UPDATE UNIFIED TABLE - REASSIGN CONTRIBUTORS")
logger.info("="*80)

# For all contributors of merged records, update their master_lakefusion_id to survivor
# This includes:
# - Contributors of survivor (already have correct ID, but include for completeness)
# - Contributors of merged records (need to be updated to survivor ID)

# Build update mapping: old_master_id -> new_survivor_id
update_mapping = df_merge_plan.select(
    col(id_key).alias("old_master_id"),
    col("survivor_id").alias("new_master_id")
).distinct()

# Also include survivor -> survivor (no change, but include in mapping)
survivor_mapping = df_survivors.select(
    col("survivor_id").alias("old_master_id"),
    col("survivor_id").alias("new_master_id")
)

# Union all mappings
full_mapping = update_mapping.union(survivor_mapping).distinct()

# Get affected surrogate keys
df_affected_contributors = df_combined_contributors.select("surrogate_key", "survivor_id").distinct()

# Update unified table
unified_delta = DeltaTable.forName(spark, unified_table)

unified_delta.alias("target").merge(
    source=df_affected_contributors.alias("source"),
    condition="target.surrogate_key = source.surrogate_key"
).whenMatchedUpdate(
    set={"master_lakefusion_id": "source.survivor_id"}
).execute()

affected_count = df_affected_contributors.count()
logger.info(f"Updated master_lakefusion_id for {affected_count} contributors in unified table")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 15: VERIFY FINAL STATE")
logger.info("="*80)

# Count survivors in master
final_master_count = spark.table(master_table).count()

# Count merge activities
final_merge_count = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM {merge_activities_table}
    WHERE action_type = 'MASTER_JOB_MERGE'
""").collect()[0]["cnt"]

logger.info(f"  Final master table count: {final_master_count}")
logger.info(f"  Total MASTER_JOB_MERGE activities: {final_merge_count}")
logger.info(f"  Groups processed: {num_components}")
logger.info(f"  Records merged this run: {records_to_merge}")

# COMMAND ----------

# Set task values
dbutils.jobs.taskValues.set("auto_merge_complete", True)
dbutils.jobs.taskValues.set("records_merged", records_to_merge)
dbutils.jobs.taskValues.set("groups_processed", num_components)
dbutils.jobs.taskValues.set("survivors_count", survivor_count)

logger.info("\n" + "="*80)
logger.info("GOLDEN DEDUP AUTO MERGE COMPLETED SUCCESSFULLY")
logger.info("="*80)
logger.info(f"Groups processed: {num_components}")
logger.info(f"Records merged: {records_to_merge}")
logger.info(f"Survivors created: {survivor_count}")
logger.info("="*80)
