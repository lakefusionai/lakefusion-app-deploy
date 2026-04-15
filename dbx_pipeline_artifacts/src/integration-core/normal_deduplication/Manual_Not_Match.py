# Databricks notebook source
import json
from datetime import datetime
from pyspark.sql.functions import (
    col, explode, lit, collect_list, first, struct, count,
    current_timestamp, udf, size, concat_ws, coalesce
)
from pyspark.sql.types import *
from delta.tables import DeltaTable


# COMMAND ----------

dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")
dbutils.widgets.text("entity_attributes_datatype", "", "Entity Attributes Datatype")
dbutils.widgets.text("match_attributes", "", "Match Attributes")
dbutils.widgets.text("master_id", "", "Master Record ID")
dbutils.widgets.text("unified_dataset_ids", "", "Unified Record IDs (JSON)")

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
match_attributes = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "match_attributes",
    debugValue=dbutils.widgets.get("match_attributes")
)
master_id = dbutils.widgets.get("master_id")
unified_dataset_ids = dbutils.widgets.get("unified_dataset_ids")

# COMMAND ----------

entity_attributes = json.loads(entity_attributes)
entity_attributes_datatype = json.loads(entity_attributes_datatype)
match_attributes = json.loads(match_attributes)
unified_dataset_ids = json.loads(unified_dataset_ids)

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

# Extract surrogate keys and dataset IDs from input
not_match_surrogate_keys = []
not_match_dataset_ids = []
for item in unified_dataset_ids:
    not_match_surrogate_keys.append(item.get("id"))
    not_match_dataset_ids.append(item.get("dataset_id"))

logger.info(f"Records to mark as NOT A MATCH: {len(not_match_surrogate_keys)}")
logger.info(f"Surrogate keys: {not_match_surrogate_keys}")
logger.info(f"Dataset IDs: {not_match_dataset_ids}")

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

logger.info("="*80)
logger.info("MANUAL NOT A MATCH PROCESSING")
logger.info("="*80)
logger.info(f"Entity: {entity}")
logger.info(f"Master ID: {master_id}")
logger.info(f"Master ID Key: {id_key}")
logger.info(f"Unified ID Key: {unified_id_key}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment ID: {experiment_id or 'None'}")
logger.info(f"Unified Table: {unified_table}")
logger.info(f"Master Table: {master_table}")
logger.info(f"Processed Unified Table: {processed_unified_table}")
logger.info(f"Merge Activities Table: {merge_activities_table}")
logger.info(f"Attribute Version Sources Table: {attribute_version_sources_table}")
logger.info(f"Records to mark as NOT A MATCH: {len(not_match_surrogate_keys)}")
logger.info("="*80)

# COMMAND ----------

setup_lakefusion_engine()

# COMMAND ----------

from lakefusion_core_engine.identifiers import generate_lakefusion_id

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 1: VALIDATE MASTER RECORD EXISTS")
logger.info("="*80)

# Check if master record exists
master_exists_query = f"""
SELECT COUNT(*) as count
FROM {master_table}
WHERE {id_key} = '{master_id}'
"""

master_exists = spark.sql(master_exists_query).collect()[0]['count']

if master_exists == 0:
    error_msg = f"Master record with {id_key} = '{master_id}' not found"
    logger.error(error_msg)
    dbutils.notebook.exit(json.dumps({
        "status": "error",
        "message": error_msg
    }))

logger.info(f"Master record exists: {id_key} = {master_id}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 2: VALIDATE RECORDS EXIST AND GET SOURCE PATHS")
logger.info("="*80)

# Build filter for records
surrogate_keys_list = ", ".join([f"'{sk}'" for sk in not_match_surrogate_keys])

# Check if records exist in processed_unified
# Note: processed_unified has one row per match, so each surrogate_key will have multiple rows
check_processed_query = f"""
SELECT DISTINCT {unified_id_key}
FROM {processed_unified_table}
WHERE {unified_id_key} IN ({surrogate_keys_list})
"""

processed_records_df = spark.sql(check_processed_query)
processed_unique_count = processed_records_df.count()

logger.info(f"Unique surrogate keys found in processed_unified: {processed_unique_count}")

if processed_unique_count == 0:
    error_msg = "No records found in processed_unified table"
    logger.error(error_msg)
    dbutils.notebook.exit(json.dumps({
        "status": "error",
        "message": error_msg
    }))

if processed_unique_count != len(not_match_surrogate_keys):
    missing_count = len(not_match_surrogate_keys) - processed_unique_count
    logger.warning(f"{missing_count} requested records not found in processed_unified")

    # Show which records were found
    found_keys = [row[unified_id_key] for row in processed_records_df.select(unified_id_key).collect()]
    missing_keys = [sk for sk in not_match_surrogate_keys if sk not in found_keys]
    logger.warning(f"  Missing keys: {missing_keys}")

# Get detailed match count for information
match_count_query = f"""
SELECT 
    {unified_id_key},
    COUNT(*) as match_count
FROM {processed_unified_table}
WHERE {unified_id_key} IN ({surrogate_keys_list})
GROUP BY {unified_id_key}
"""

match_counts_df = spark.sql(match_count_query)
logger.info(f"\n  Match counts per record:")
for row in match_counts_df.collect():
    logger.info(f"    {row[unified_id_key]}: {row['match_count']} potential matches")

# Get source_path from unified table
get_source_paths_query = f"""
SELECT {unified_id_key}, source_path
FROM {unified_table}
WHERE {unified_id_key} IN ({surrogate_keys_list})
"""

existing_records_df = spark.sql(get_source_paths_query)
existing_count = existing_records_df.count()

logger.info(f"\nSource paths retrieved from unified table: {existing_count}")

if existing_count != len(not_match_surrogate_keys):
    logger.warning(f"Expected {len(not_match_surrogate_keys)} records in unified, found {existing_count}")

# Materialize for serverless
existing_records_data = existing_records_df.collect()
logger.info(f"Materialized {len(existing_records_data)} records")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 3: LOG MANUAL_NOT_A_MATCH ACTIVITIES")
logger.info("="*80)

# Recreate DataFrame from materialized data
existing_records_df = spark.createDataFrame(existing_records_data)

master_table_version = spark.sql(f"DESCRIBE HISTORY {master_table} LIMIT 1").collect()[0]["version"]

logger.info(f"Master table current version: {master_table_version}")

# Prepare NOT_A_MATCH activities
not_match_activities_df = existing_records_df.select(
    lit(master_id).alias("master_id"),
    col(unified_id_key).alias("match_id"),
    col("source_path").alias("source"),
    lit(master_table_version).alias("version"),
    lit("MANUAL_NOT_A_MATCH").alias("action_type"),
    current_timestamp().alias("created_at")
)

activities_count = not_match_activities_df.count()

logger.info(f"  Prepared {activities_count} MANUAL_NOT_A_MATCH activities")

# Perform merge
merge_activities_delta_table = DeltaTable.forName(spark, merge_activities_table)

merge_activities_delta_table.alias("target").merge(
    not_match_activities_df.alias("source"),
    f"""target.match_id = source.match_id 
        AND target.master_id = source.master_id 
        AND target.action_type = source.action_type"""
).whenNotMatchedInsertAll().execute()

logger.info(f"MANUAL_NOT_A_MATCH activities logged")
logger.info(f"  Records logged: {activities_count}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 4: CHECK FOR OTHER POTENTIAL MATCHES")
logger.info("="*80)

# For each record, check if it has other POTENTIAL matches in processed_unified
# The processed_unified table has matches in exploded_result column

# Recreate DataFrame
existing_records_df = spark.createDataFrame(existing_records_data)

# First, get all previously rejected masters for these surrogate keys (NOT_A_MATCH history)
not_a_match_history_query = f"""
SELECT DISTINCT
    ma.match_id as {unified_id_key},
    ma.master_id as rejected_master_id
FROM {merge_activities_table} ma
WHERE ma.action_type = 'MANUAL_NOT_A_MATCH'
    AND ma.match_id IN ({surrogate_keys_list})
"""

not_a_match_history_df = spark.sql(not_a_match_history_query)
not_a_match_count = not_a_match_history_df.count()

logger.info(f"  Found {not_a_match_count} previously rejected masters for these records")

if not_a_match_count > 0:
    logger.info(f"  Previously rejected masters:")
    for row in not_a_match_history_df.collect():
        logger.info(f"    {row[unified_id_key]} -> Rejected master: {row['rejected_master_id']}")

# Get records with their match results from processed_unified
# Note: processed_unified has one row per match (exploded)
check_matches_query = f"""
SELECT 
    pu.{unified_id_key},
    pu.exploded_result.lakefusion_id as match_lakefusion_id,
    pu.exploded_result.score as match_score,
    pu.exploded_result.match as match_type,
    pu.exploded_result.reason as match_reason
FROM {processed_unified_table} pu
WHERE pu.{unified_id_key} IN ({surrogate_keys_list})
"""

records_with_matches_df = spark.sql(check_matches_query)

logger.info(f"\n  All matches found in processed_unified:")
matches_data = records_with_matches_df.collect()
for row in matches_data:
    logger.info(f"    {row[unified_id_key]} -> Master: {row['match_lakefusion_id']}, Score: {row['match_score']}, Type: {row['match_type']}")

# Filter for OTHER matches (not current master, not previously rejected masters)
# AND only keep POTENTIAL_MATCH types
if not_a_match_count > 0:
    # Join to exclude previously rejected masters
    other_potential_matches_df = records_with_matches_df.join(
        not_a_match_history_df,
        on=[
            records_with_matches_df[unified_id_key] == not_a_match_history_df[unified_id_key],
            records_with_matches_df["match_lakefusion_id"] == not_a_match_history_df["rejected_master_id"]
        ],
        how="left_anti"  # Keep only records NOT in the rejection list
    ).filter(
        (col("match_lakefusion_id") != master_id) &  # Exclude current master
        (col("match_type") == "POTENTIAL_MATCH")     # Only POTENTIAL_MATCH types
    )
else:
    # No rejection history, just exclude current master and filter by type
    other_potential_matches_df = records_with_matches_df.filter(
        (col("match_lakefusion_id") != master_id) &  # Exclude current master
        (col("match_type") == "POTENTIAL_MATCH")     # Only POTENTIAL_MATCH types
    )

# Group by surrogate_key and collect all other POTENTIAL match IDs
from pyspark.sql.functions import collect_list

grouped_other_matches_df = other_potential_matches_df.groupBy(unified_id_key).agg(
    collect_list("match_lakefusion_id").alias("other_potential_match_ids"),
    collect_list("match_score").alias("other_match_scores"),
    collect_list("match_type").alias("other_match_types")
)

# Check if there are other POTENTIAL matches
def has_other_potential_matches_udf_func(match_ids_array):
    """Check if record has other POTENTIAL matches besides current and rejected masters."""
    if not match_ids_array:
        return False
    
    return len(match_ids_array) > 0

# Register UDF
has_other_potential_matches_udf = udf(has_other_potential_matches_udf_func, BooleanType())

# Apply UDF
records_with_other_matches_check = grouped_other_matches_df.withColumn(
    "has_other_potential_matches",
    has_other_potential_matches_udf(col("other_potential_match_ids"))
)

# Join back with source paths
records_with_other_matches_df_full = records_with_other_matches_check.join(
    existing_records_df,
    on=unified_id_key,
    how="inner"
)

# For records without grouped results (no other potential matches), add them explicitly
records_without_grouped = existing_records_df.join(
    grouped_other_matches_df,
    on=unified_id_key,
    how="left_anti"
).withColumn("has_other_potential_matches", lit(False))

# Combine
records_with_other_matches_df = records_with_other_matches_df_full.select(
    unified_id_key, "source_path", "has_other_potential_matches", 
    "other_potential_match_ids", "other_match_scores", "other_match_types"
).unionByName(
    records_without_grouped.select(
        unified_id_key, "source_path", "has_other_potential_matches",
        lit(None).cast("array<string>").alias("other_potential_match_ids"),
        lit(None).cast("array<string>").alias("other_match_scores"),
        lit(None).cast("array<string>").alias("other_match_types")
    ),
    allowMissingColumns=True
)

# Split into two groups based on POTENTIAL matches
records_with_other_matches = records_with_other_matches_df.filter(col("has_other_potential_matches") == True)
records_without_other_matches = records_with_other_matches_df.filter(col("has_other_potential_matches") == False)

with_other_count = records_with_other_matches.count()
without_other_count = records_without_other_matches.count()

logger.info(f"\nAnalysis complete:")
logger.info(f"  Records with other POTENTIAL matches: {with_other_count}")
if with_other_count > 0:
    logger.info(f"    Details:")
    for row in records_with_other_matches.collect():
        logger.info(f"      {row[unified_id_key]}: {len(row['other_potential_match_ids'])} other POTENTIAL matches")
        for i, (mid, score, mtype) in enumerate(zip(row['other_potential_match_ids'], row['other_match_scores'], row['other_match_types'])):
            logger.info(f"        -> Master {mid}: score={score}, type={mtype}")

logger.info(f"  Records without other POTENTIAL matches: {without_other_count}")
if without_other_count > 0:
    logger.info(f"    (These records have no other POTENTIAL matches or only NO_MATCH alternatives)")
    for row in records_without_other_matches.collect():
        logger.info(f"      {row[unified_id_key]}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 5: DECISION POINT")
logger.info("="*80)

if without_other_count == 0:
    # All records have other POTENTIAL matches - exit here
    summary = {
        "status": "success",
        "action": "not_a_match_logged_only",
        "master_id": master_id,
        "records_marked_not_match": activities_count,
        "records_with_other_matches": with_other_count,
        "records_without_other_matches": 0,
        "new_masters_created": 0,
        "message": "All records have other POTENTIAL matches. MANUAL_NOT_A_MATCH logged. No further action taken.",
        "timestamp": datetime.now().isoformat()
    }
    
    logger.info("All records have other potential matches")
    logger.info("MANUAL_NOT_A_MATCH logged successfully")
    logger.info("No further action needed - records will be re-evaluated in next matching cycle")
    logger.info("\nSummary:")
    logger.info(f"  Records marked as NOT A MATCH: {activities_count}")
    logger.info(f"  Records with other potential matches: {with_other_count}")
    
    # Set task values
    dbutils.jobs.taskValues.set("manual_not_match_complete", True)
    dbutils.jobs.taskValues.set("action", "not_a_match_logged_only")
    dbutils.jobs.taskValues.set("records_marked_not_match", activities_count)
    
    dbutils.notebook.exit(json.dumps(summary))
else:
    # Some records have NO other potential matches - proceed to insert as individual masters
    logger.info(f"Found {without_other_count} records with NO other potential matches")
    logger.info(f"Proceeding to insert as individual masters")
    
    # Materialize records without other matches
    records_without_other_matches_data = records_without_other_matches.select(
        col(unified_id_key),
        col("source_path")
    ).collect()
    
    logger.info(f"Materialized {len(records_without_other_matches_data)} records for insertion")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 6: FETCH FULL RECORDS FOR INSERTION")
logger.info("="*80)

# Get surrogate keys of records without other matches
no_match_surrogate_keys = [row[unified_id_key] for row in records_without_other_matches_data]
no_match_keys_list = ", ".join([f"'{sk}'" for sk in no_match_surrogate_keys])

logger.info(f"  Surrogate keys to insert: {no_match_surrogate_keys}")

# Build attribute columns dynamically
attribute_cols = ", ".join([f"u.{attr}" for attr in entity_attributes if attr != id_key])

# Fetch full records from unified table
fetch_full_records_query = f"""
SELECT
    u.{unified_id_key},
    u.source_path,
    {attribute_cols}
FROM {unified_table} u
WHERE u.{unified_id_key} IN ({no_match_keys_list})
"""

full_records_df = spark.sql(fetch_full_records_query)
full_records_count = full_records_df.count()

logger.info(f"Fetched {full_records_count} full records from unified table")

# Materialize for serverless
full_records_data = full_records_df.collect()
logger.info(f"Materialized {len(full_records_data)} full records")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 7: GENERATE NEW LAKEFUSION IDS")
logger.info("="*80)

# Generate new lakefusion_id for each record
new_lakefusion_ids = {}
for row in full_records_data:
    surrogate_key = row[unified_id_key]
    new_lf_id = generate_lakefusion_id()
    new_lakefusion_ids[surrogate_key] = new_lf_id
    logger.info(f"  {surrogate_key} -> {new_lf_id}")

logger.info(f"Generated {len(new_lakefusion_ids)} new lakefusion IDs")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 8: PREPARE RECORDS FOR MASTER INSERT")
logger.info("="*80)

# Recreate full_records_df from materialized data
full_records_df = spark.createDataFrame(full_records_data)

# Create a mapping DataFrame for new lakefusion_ids
new_ids_data = [{"surrogate_key": sk, "new_lakefusion_id": lf_id} 
                for sk, lf_id in new_lakefusion_ids.items()]
new_ids_df = spark.createDataFrame(new_ids_data)

# Join to add new lakefusion_ids to records
records_with_ids_df = full_records_df.join(
    new_ids_df,
    on=unified_id_key,
    how="inner"
)

# Prepare insert records with proper type casting
insert_cols = [col("new_lakefusion_id").alias(id_key)]

for attr in entity_attributes:
    if attr == id_key:
        continue
    
    # Get target data type
    target_type = entity_attributes_datatype.get(attr, "string")
    
    # Cast to proper type
    if target_type.lower() in ["timestamp", "date", "datetime"]:
        insert_cols.append(col(attr).cast("timestamp").alias(attr))
    elif target_type.lower() in ["int", "integer", "long", "bigint"]:
        insert_cols.append(col(attr).cast("long").alias(attr))
    elif target_type.lower() in ["float", "double", "decimal"]:
        insert_cols.append(col(attr).cast("double").alias(attr))
    elif target_type.lower() in ["boolean", "bool"]:
        insert_cols.append(col(attr).cast("boolean").alias(attr))
    else:
        insert_cols.append(col(attr).alias(attr))

# Add attributes_combined generation using match_attributes
if match_attributes:
    # Build concat expression for match attributes
    concat_cols = []
    for attr in match_attributes:
        if attr in entity_attributes:
            # Coalesce to handle nulls
            concat_cols.append(coalesce(col(attr).cast("string"), lit("")))
    
    if concat_cols:
        insert_cols.append(
            concat_ws(" ", *concat_cols).alias("attributes_combined")
        )
    else:
        insert_cols.append(lit("").alias("attributes_combined"))
else:
    insert_cols.append(lit("").alias("attributes_combined"))

master_insert_df = records_with_ids_df.select(*insert_cols)

logger.info(f"Prepared {master_insert_df.count()} records for master insert")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 9: INSERT INTO MASTER TABLE")
logger.info("="*80)

# Get current version before transaction
current_master_version = spark.sql(f"DESCRIBE HISTORY {master_table} LIMIT 1").select("version").collect()[0][0]
logger.info(f"  Current master version: {current_master_version}")

# Perform insert
master_delta_table = DeltaTable.forName(spark, master_table)

master_delta_table.alias("target").merge(
    master_insert_df.alias("source"),
    f"target.{id_key} = source.{id_key}"
).whenNotMatchedInsertAll().execute()

# Get new version
new_master_version = spark.sql(f"DESCRIBE HISTORY {master_table} LIMIT 1").select("version").collect()[0][0]

logger.info(f"Master table updated")
logger.info(f"  New version: {new_master_version}")
logger.info(f"  Inserted records: {master_insert_df.count()}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 10: PREPARE ATTRIBUTE VERSION SOURCES")
logger.info("="*80)

# Recreate records_with_ids_df for attribute sources
full_records_df = spark.createDataFrame(full_records_data)
records_with_ids_df = full_records_df.join(new_ids_df, on=unified_id_key, how="inner")

# For each record, create attribute source mapping
# Since it's a single contributor, source is its own source_path for all attributes
def create_single_contributor_sources(source_path, attributes, attribute_values_row):
    """Create attribute source mapping for single contributor."""
    # Convert Row to dict if needed
    if hasattr(attribute_values_row, 'asDict'):
        attribute_values = attribute_values_row.asDict()
    else:
        attribute_values = attribute_values_row
    
    sources = []
    for attr in attributes:
        if attr != 'lakefusion_id':  # Skip id_key
            value = attribute_values.get(attr)
            sources.append({
                "attribute_name": attr,
                "attribute_value": str(value) if value is not None else None,
                "source": source_path
            })
    return sources

# Create UDF for single contributor sources
single_contributor_udf = udf(
    lambda source_path, attr_row: create_single_contributor_sources(
        source_path, 
        entity_attributes, 
        attr_row
    ),
    ArrayType(StructType([
        StructField("attribute_name", StringType()),
        StructField("attribute_value", StringType()),
        StructField("source", StringType())
    ]))
)

# Build attribute dict for each record
attr_cols_struct = struct(*[col(attr).alias(attr) for attr in entity_attributes if attr != id_key])

attr_sources_df = records_with_ids_df.select(
    col("new_lakefusion_id").alias(id_key),
    lit(new_master_version).alias("version"),
    single_contributor_udf(col("source_path"), attr_cols_struct).alias("attribute_source_mapping")
)

logger.info(f"Prepared attribute version sources")
logger.info(f"  Records: {attr_sources_df.count()}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 11: UPDATE ATTRIBUTE VERSION SOURCES")
logger.info("="*80)

# Perform merge
attr_sources_delta_table = DeltaTable.forName(spark, attribute_version_sources_table)

attr_sources_delta_table.alias("target").merge(
    attr_sources_df.alias("source"),
    f"target.{id_key} = source.{id_key} AND target.version = source.version"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

logger.info(f"Attribute version sources updated")
logger.info(f"  Records updated/inserted: {attr_sources_df.count()}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 12: LOG JOB_INSERT ACTIVITIES")
logger.info("="*80)

# Recreate records_with_ids_df
full_records_df = spark.createDataFrame(full_records_data)
records_with_ids_df = full_records_df.join(new_ids_df, on=unified_id_key, how="inner")

# Prepare JOB_INSERT activities
insert_activities_df = records_with_ids_df.select(
    col("new_lakefusion_id").alias("master_id"),
    col(unified_id_key).alias("match_id"),
    col("source_path").alias("source"),
    lit(new_master_version).alias("version"),
    lit("JOB_INSERT").alias("action_type"),
    current_timestamp().alias("created_at")
)

insert_activities_count = insert_activities_df.count()

logger.info(f"  Prepared {insert_activities_count} JOB_INSERT activities")

# Perform merge
merge_activities_delta_table.alias("target").merge(
    insert_activities_df.alias("source"),
    f"""target.match_id = source.match_id 
        AND target.master_id = source.master_id 
        AND target.version = source.version
        AND target.action_type = source.action_type"""
).whenNotMatchedInsertAll().execute()

logger.info(f"JOB_INSERT activities logged")
logger.info(f"  Records logged: {insert_activities_count}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 13: UPDATE UNIFIED TABLE")
logger.info("="*80)

# Recreate records_with_ids_df
full_records_df = spark.createDataFrame(full_records_data)
records_with_ids_df = full_records_df.join(new_ids_df, on=unified_id_key, how="inner")

# Update unified table for inserted records:
# 1. Set master_lakefusion_id to new lakefusion_id
# 2. Set record_status = 'MERGED'

unified_updates_df = records_with_ids_df.select(
    col(unified_id_key),
    col("new_lakefusion_id").alias(f"master_{id_key}"),
    lit("MERGED").alias("record_status")
)

logger.info(f"  Prepared {unified_updates_df.count()} records for unified table update")

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
logger.info("MANUAL NOT A MATCH PROCESSING COMPLETE")
logger.info("="*80)

summary = {
    "status": "success",
    "action": "not_a_match_and_inserted",
    "master_id": master_id,
    "records_marked_not_match": activities_count,
    "records_with_other_matches": with_other_count,
    "records_without_other_matches": without_other_count,
    "new_masters_created": len(new_lakefusion_ids),
    "new_lakefusion_ids": list(new_lakefusion_ids.values()),
    "master_version": int(new_master_version),
    "timestamp": datetime.now().isoformat()
}

logger.info(f"\nSummary:")
logger.info(f"  Records marked as NOT A MATCH: {summary['records_marked_not_match']}")
logger.info(f"  Records with other potential matches: {summary['records_with_other_matches']}")
logger.info(f"  Records without other potential matches: {summary['records_without_other_matches']}")
logger.info(f"  New individual masters created: {summary['new_masters_created']}")
logger.info(f"  Master table version: {summary['master_version']}")
logger.info(f"\n  New lakefusion IDs created:")
for sk, lf_id in new_lakefusion_ids.items():
    logger.info(f"    {sk} -> {lf_id}")
