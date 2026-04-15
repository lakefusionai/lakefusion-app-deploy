# Databricks notebook source
import json
from datetime import datetime
from pyspark.sql.functions import (
    col, explode, lit, collect_list, first, struct, count, row_number,
    max as spark_max, current_timestamp, arrays_zip, udf, concat_ws, coalesce
)
from pyspark.sql.types import *
from pyspark.sql import Window
from delta.tables import DeltaTable


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

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

id_key = 'lakefusion_id'
unified_id_key = 'surrogate_key'

# COMMAND ----------

# Base table names
unified_table = f"{catalog_name}.silver.{entity}_unified"
master_table = f"{catalog_name}.gold.{entity}_master"
processed_unified_table = f"{catalog_name}.silver.{entity}_processed_unified"

# Add experiment suffix if exists
if experiment_id:
    unified_table += f"_{experiment_id}"
    master_table += f"_{experiment_id}"
    processed_unified_table += f"_{experiment_id}"

# Related tables
merge_activities_table = f"{master_table}_merge_activities"
attribute_version_sources_table = f"{master_table}_attribute_version_sources"

# COMMAND ----------

logger.info(f"Entity: {entity}")
logger.info(f"Master ID Key: {id_key}")
logger.info(f"Unified ID Key: {unified_id_key}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment ID: {experiment_id or 'None'}")
logger.info(f"Unified Table: {unified_table}")
logger.info(f"Master Table: {master_table}")
logger.info(f"Processed Unified Table: {processed_unified_table}")
logger.info(f"Merge Activities Table: {merge_activities_table}")
logger.info(f"Attribute Version Sources Table: {attribute_version_sources_table}")

# COMMAND ----------

setup_lakefusion_engine()

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
logger.info("STEP 1: EXTRACT MATCH RECORDS")
logger.info("="*80)

# Query to get MATCH records with scores
match_records_query = f"""
SELECT
    pu.{unified_id_key},
    pu.{id_key},
    pu.exploded_result.score as match_score,
    pu.exploded_result.match as match_status
FROM {processed_unified_table} pu
WHERE pu.exploded_result.match = 'MATCH'
"""

match_records_df = spark.sql(match_records_query)
total_match_count = match_records_df.count()

logger.info(f"Found {total_match_count} MATCH records")

if total_match_count == 0:
    logger.info("No MATCH records to process")
    dbutils.notebook.exit(json.dumps({
        "status": "success",
        "message": "No MATCH records to process",
        "records_processed": 0
    }))

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 2: HANDLE DUPLICATE MATCHES")
logger.info("="*80)

# Find records that match to multiple masters
window_spec = Window.partitionBy(unified_id_key).orderBy(col("match_score").desc())

# Add row number to identify duplicates
match_records_ranked = match_records_df.withColumn(
    "rank",
    row_number().over(window_spec)
)

# Check for duplicates
duplicates_count = match_records_ranked.filter(col("rank") > 1).count()
logger.info(f"  Records matching multiple masters: {duplicates_count}")

# Keep only highest scoring match per unified record
match_records_unique = match_records_ranked.filter(col("rank") == 1).drop("rank")

final_match_count = match_records_unique.count()
logger.info(f"  After keeping highest score: {final_match_count}")
logger.info(f"  Dropped: {total_match_count - final_match_count}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 3: GROUP RECORDS BY TARGET MASTER")
logger.info("="*80)

# Group by master_lakefusion_id to find which unified records merge to same master
grouped_by_master = (
    match_records_unique
    .groupBy(id_key)
    .agg(
        collect_list(unified_id_key).alias("unified_keys_to_merge"),
        count("*").alias("merge_count")
    )
)

# Get statistics
master_count = grouped_by_master.count()
max_merge_count = grouped_by_master.select(spark_max("merge_count")).collect()[0][0]

logger.info(f"Masters receiving merges: {master_count}")
logger.info(f"  Max records merging to single master: {max_merge_count}")

# Show distribution
logger.info("\nMerge distribution:")
grouped_by_master.groupBy("merge_count").count().orderBy("merge_count").show()

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 4: FETCH ALL CONTRIBUTORS FOR EACH MASTER")
logger.info("="*80)

# For each master, get:
# 1. Existing MERGED records (already contributors) - ONLY for masters being updated
# 2. New ACTIVE records being merged now

# Build attribute columns dynamically
attribute_cols = ", ".join([f"u.{attr}" for attr in entity_attributes])

# CRITICAL FIX: Create a temporary view of match_records_unique to use in SQL
match_records_unique.createOrReplaceTempView("match_records_temp")

all_contributors_query = f"""
WITH masters_to_update AS (
    -- Only get masters that are actually receiving new merges
    SELECT DISTINCT {id_key}
    FROM match_records_temp
),
existing_contributors AS (
    -- Get ONLY existing contributors for masters being updated
    SELECT
        u.{unified_id_key},
        u.source_path,
        u.master_{id_key},
        {attribute_cols}
    FROM {unified_table} u
    INNER JOIN masters_to_update m
        ON u.master_{id_key} = m.{id_key}
    WHERE u.record_status = 'MERGED'
),
new_contributors AS (
    -- Get new records being merged (from match_records_unique)
    SELECT
        u.{unified_id_key},
        u.source_path,
        mr.{id_key} as master_{id_key},
        {attribute_cols}
    FROM {unified_table} u
    INNER JOIN match_records_temp mr
        ON u.{unified_id_key} = mr.{unified_id_key}
    WHERE u.record_status = 'ACTIVE'
)
SELECT * FROM existing_contributors
UNION ALL
SELECT * FROM new_contributors
"""

all_contributors_df = spark.sql(all_contributors_query)
total_contributors = all_contributors_df.count()

logger.info(f"Total contributors (existing + new): {total_contributors}")

# Show breakdown by master
contributors_by_master = all_contributors_df.groupBy(f"master_{id_key}").count()
logger.info(f"Contributors grouped by master:")
contributors_by_master.orderBy(col("count").desc()).show(10, truncate=False)

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 5: GROUP CONTRIBUTORS BY MASTER")
logger.info("="*80)

# Group all contributors by master_lakefusion_id
grouped_contributors = (
    all_contributors_df
    .groupBy(f"master_{id_key}")
    .agg(
        collect_list(
            struct([col(c) for c in all_contributors_df.columns if c != f"master_{id_key}"])
        ).alias("unified_records")
    )
    .withColumnRenamed(f"master_{id_key}", id_key)
)

grouped_count = grouped_contributors.count()
logger.info(f"Grouped into {grouped_count} masters")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 6: APPLY SURVIVORSHIP ENGINE")
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

# COMMAND ----------

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

logger.info(f"Survivorship applied to {final_results_df.count()} masters")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 7: UPDATE MASTER TABLE")
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
    elif target_type.lower() in ["int", "integer", "long", "bigint"]:
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
            # Coalesce to handle nulls, convert to string
            concat_cols.append(coalesce(col(f"merged_record.{attr}").cast("string"), lit("")))
    
    if concat_cols:
        master_updates_cols.append(
            concat_ws(" | ", *concat_cols).alias("attributes_combined")
        )
    else:
        master_updates_cols.append(lit("").alias("attributes_combined"))
else:
    master_updates_cols.append(lit("").alias("attributes_combined"))

master_updates_df = final_results_df.select(*master_updates_cols)

logger.info(f"Prepared master updates with attributes_combined")
logger.info(f"  Match attributes used: {len([a for a in match_attributes if a in entity_attributes and a != id_key])}")

# Get current master table version before merge
current_master_version = spark.sql(f"DESCRIBE HISTORY {master_table} LIMIT 1").select("version").collect()[0][0]
logger.info(f"  Current master table version: {current_master_version}")

# Perform merge - only update attributes that exist in source
master_delta_table = DeltaTable.forName(spark, master_table)

# Build update dict for all attributes including attributes_combined
update_dict = {attr: f"source.{attr}" for attr in entity_attributes if attr != id_key}
update_dict["attributes_combined"] = "source.attributes_combined"

master_delta_table.alias("target").merge(
    master_updates_df.alias("source"),
    f"target.{id_key} = source.{id_key}"
).whenMatchedUpdate(set=update_dict).execute()

# Get new version
new_master_version = spark.sql(f"DESCRIBE HISTORY {master_table} LIMIT 1").select("version").collect()[0][0]

logger.info(f"Master table updated")
logger.info(f"  New version: {new_master_version}")
logger.info(f"  Records updated: {master_updates_df.count()}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 8: UPDATE ATTRIBUTE VERSION SOURCES")
logger.info("="*80)

# Prepare attribute version sources updates
attribute_sources_update_df = final_results_df.select(
    col(id_key),
    lit(new_master_version).alias("version"),
    col("attribute_sources").alias("attribute_source_mapping")
)

# Perform merge
attr_sources_delta_table = DeltaTable.forName(spark, attribute_version_sources_table)

attr_sources_delta_table.alias("target").merge(
    attribute_sources_update_df.alias("source"),
    f"target.{id_key} = source.{id_key} AND target.version = source.version"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

logger.info(f"Attribute version sources updated")
logger.info(f"  Records updated: {attribute_sources_update_df.count()}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 9: LOG MERGE ACTIVITIES")
logger.info("="*80)

# Prepare merge activities records
# ONLY log NEW merges (from match_records_unique), not existing contributors
# Get source_path directly from unified table to avoid duplicates

merge_activities_query = f"""
SELECT
    mr.{id_key} as master_id,
    mr.{unified_id_key} as match_id,
    u.source_path as source,
    {new_master_version} as version,
    'JOB_MERGE' as action_type,
    current_timestamp() as created_at
FROM match_records_temp mr
INNER JOIN {unified_table} u
    ON mr.{unified_id_key} = u.{unified_id_key}
WHERE u.record_status = 'ACTIVE'
"""

merge_activities_df = spark.sql(merge_activities_query)

merge_activities_count = merge_activities_df.count()

logger.info(f"  Prepared {merge_activities_count} merge activities")

# Show sample
logger.info("\n  Sample merge activities:")
merge_activities_df.show(5, truncate=False)

if merge_activities_count > 0:
    # Perform merge
    merge_activities_delta_table = DeltaTable.forName(spark, merge_activities_table)
    
    merge_activities_delta_table.alias("target").merge(
        merge_activities_df.alias("source"),
        f"""target.match_id = source.match_id 
            AND target.master_id = source.master_id 
            AND target.version = source.version
            AND target.action_type = source.action_type"""
    ).whenNotMatchedInsertAll().execute()
    
    logger.info(f"Merge activities logged")
    logger.info(f"  New merge activities: {merge_activities_count}")
else:
    logger.info("  No new merge activities to log")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 10: UPDATE UNIFIED TABLE")
logger.info("="*80)

# Update unified table for merged records:
# 1. Set master_lakefusion_id
# 2. Set record_status = 'MERGED'

# Get list of unified keys that were successfully merged
merged_unified_keys = (
    match_records_unique
    .select(
        col(unified_id_key),
        col(id_key).alias(f"master_{id_key}")
    )
)

# Create update dataframe
unified_updates_df = merged_unified_keys.select(
    col(unified_id_key),
    col(f"master_{id_key}"),
    lit("MERGED").alias("record_status")
)

# Perform merge
unified_delta_table = DeltaTable.forName(spark, unified_table)

unified_delta_table.alias("target").merge(
    unified_updates_df.alias("source"),
    f"target.{unified_id_key} = source.{unified_id_key}"
).whenMatchedUpdate(set={
    f"master_{id_key}": f"source.master_{id_key}",
    "record_status": "source.record_status"
}).execute()

logger.info(f"Unified table updated")
logger.info(f"  Records updated: {unified_updates_df.count()}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("MERGE PROCESSING COMPLETE")
logger.info("="*80)

summary = {
    "status": "success",
    "records_processed": final_match_count,
    "masters_updated": master_count,
    "merge_activities_logged": merge_activities_count,
    "master_version": int(new_master_version),
    "timestamp": datetime.now().isoformat()
}

logger.info(f"\nSummary:")
logger.info(f"  Records processed: {summary['records_processed']}")
logger.info(f"  Masters updated: {summary['masters_updated']}")
logger.info(f"  Merge activities logged: {summary['merge_activities_logged']}")
logger.info(f"  Master table version: {summary['master_version']}")

# Set task values
dbutils.jobs.taskValues.set("merge_complete", True)
dbutils.jobs.taskValues.set("records_merged", summary['records_processed'])
dbutils.jobs.taskValues.set("masters_updated", summary['masters_updated'])
