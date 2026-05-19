# Databricks notebook source
import json
from datetime import datetime
from pyspark.sql.functions import (
    col, lit, current_timestamp, regexp_extract_all, explode,
    array_distinct, concat_ws, when, coalesce
)
from pyspark.sql.types import StringType
from delta.tables import DeltaTable

# COMMAND ----------

dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("meta_info_table", "", "Metadata Table")

# COMMAND ----------

entity = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="entity",
    debugValue=dbutils.widgets.get("entity")
)

meta_info_table = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="meta_info_table",
    debugValue=dbutils.widgets.get("meta_info_table")
)

catalog_name = dbutils.widgets.get("catalog_name")
experiment_id = dbutils.widgets.get("experiment_id")

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

id_key = "lakefusion_id"

# COMMAND ----------

# Table names
master_table = f"{catalog_name}.gold.{entity}_master"
unified_dedup_table = f"{catalog_name}.silver.{entity}_unified_deduplicate"

if experiment_id:
    master_table += f"_{experiment_id}"
    unified_dedup_table += f"_{experiment_id}"

merge_activities_table = f"{master_table}_merge_activities"

# COMMAND ----------

logger.info("="*80)
logger.info("CREATE UNIFIED DEDUP TABLE FOR GOLDEN DEDUPLICATION")
logger.info("="*80)
logger.info(f"Entity: {entity}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment ID: {experiment_id or 'None'}")
logger.info(f"Master Table: {master_table}")
logger.info(f"Unified Dedup Table: {unified_dedup_table}")
logger.info(f"Merge Activities Table: {merge_activities_table}")
logger.info(f"Meta Info Table: {meta_info_table}")
logger.info(f"Master ID Key: {id_key}")
logger.info("="*80)

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 1: CHECK IF UNIFIED_DEDUP TABLE EXISTS")
logger.info("="*80)

try:
    spark.sql(f"DESCRIBE TABLE {unified_dedup_table}")
    dedup_table_exists = True
    logger.info(f"Unified dedup table exists: {unified_dedup_table}")
except Exception:
    dedup_table_exists = False
    logger.info(f"Unified dedup table does not exist: {unified_dedup_table}")

# COMMAND ----------

if not dedup_table_exists:
    logger.info("\n" + "="*80)
    logger.info("INITIAL LOAD - CREATING UNIFIED_DEDUP TABLE")
    logger.info("="*80)
    
    # Get current master table version
    master_version = spark.sql(f"DESCRIBE HISTORY {master_table} LIMIT 1").select("version").collect()[0][0]
    logger.info(f"  Current master version: {master_version}")
    
    # Clone master table and add search/scoring results columns
    logger.info(f"\n  Creating clone of master table...")
    
    # Read master table
    master_df = spark.table(master_table)
    
    # Add search_results and scoring_results columns (empty strings)
    unified_dedup_df = master_df.withColumn("search_results", lit("").cast(StringType())) \
                                 .withColumn("scoring_results", lit("").cast(StringType()))
    
    # Write to unified_dedup table
    unified_dedup_df.write.format("delta").mode("overwrite").saveAsTable(unified_dedup_table)
    
    logger.info(f"Created unified_dedup table (cloned from master)")
    logger.info(f"  Added columns: search_results, scoring_results")
    
    logger.info("\n" + "="*80)
    logger.info("INITIAL LOAD COMPLETE")
    logger.info("="*80)
    logger.info(f"Records cloned from master with empty search_results and scoring_results")
    logger.info(f"Ready for vector search processing")
    logger.info(f"Note: Metadata will be updated at end of pipeline")
    logger.info("="*80)
    
    # Exit notebook after initial load
    dbutils.notebook.exit(json.dumps({
        "status": "success",
        "mode": "initial_load",
        "master_version": int(master_version)
    }))

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("INCREMENTAL MODE - DETECTING CHANGES")
logger.info("="*80)

# Get last processed version from meta_info_table using master table name
last_processed_query = f"""
SELECT last_processed_version
FROM {meta_info_table}
WHERE table_name = '{master_table}' AND
entity_name = '{entity}'
"""

last_processed_result = spark.sql(last_processed_query).collect()

if len(last_processed_result) == 0:
    error_msg = f"ERROR: No metadata found for table '{master_table}' in {meta_info_table}"
    logger.info(error_msg)
    raise ValueError(error_msg)

last_processed_version = last_processed_result[0]['last_processed_version']
current_master_version = spark.sql(f"DESCRIBE HISTORY {master_table} LIMIT 1").select("version").collect()[0][0]

logger.info(f"  Last processed master version: {last_processed_version}")
logger.info(f"  Current master version: {current_master_version}")

if current_master_version == last_processed_version:
    logger.info("\nNo changes detected - master version unchanged")
    logger.info("\n" + "="*80)
    logger.info("EXITING - NO CHANGES TO PROCESS")
    logger.info("="*80)
    
    dbutils.notebook.exit(json.dumps({
        "status": "success",
        "mode": "incremental",
        "message": "No changes detected",
        "records_processed": 0,
        "master_version": int(current_master_version)
    }))

logger.info(f"  Detected changes: versions {last_processed_version} → {current_master_version}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 2: IDENTIFY CHANGED LAKEFUSION_IDS FROM MERGE ACTIVITIES")
logger.info("="*80)

# Get all lakefusion_ids that changed between last processed version and current version
# For normal dedup: Only master_id is a lakefusion_id
# For golden dedup (MASTER_MANUAL_MERGE, MASTER_JOB_MERGE): Both master_id and match_id are lakefusion_ids

# Query 1: Get changed master_ids (applies to all merge types)
master_ids_query = f"""
SELECT DISTINCT master_id as {id_key}
FROM {merge_activities_table}
WHERE version > {last_processed_version} 
  AND version <= {current_master_version}
"""

master_ids_df = spark.sql(master_ids_query)

# Query 2: Get changed match_ids for golden dedup merges
# These are also lakefusion_ids that participated in master-to-master merges
match_ids_query = f"""
SELECT DISTINCT match_id as {id_key}
FROM {merge_activities_table}
WHERE version > {last_processed_version}
  AND version <= {current_master_version}
  AND action_type IN ('MASTER_MANUAL_MERGE', 'MASTER_JOB_MERGE','MASTER_FORCE_MERGE')
"""

match_ids_df = spark.sql(match_ids_query)

# Combine both master_ids and match_ids, then get distinct
changed_ids_df = master_ids_df.union(match_ids_df).distinct()

# Empty-check without full count (head(1) short-circuits with LIMIT 1).
# Reuse the result for an observability boolean log line — keeps a signal
# in logs without triggering a second action or a full count.
_changed_first_row = changed_ids_df.head(1)
logger.info(f"  changed_ids non-empty: {bool(_changed_first_row)}")
if not _changed_first_row:
    logger.info("\nNo changed records found in merge_activities")
    logger.info("  This might indicate no merge activities occurred")
    logger.info("  Exiting without processing...")

    dbutils.notebook.exit(json.dumps({
        "status": "success",
        "mode": "incremental",
        "message": "No changed records in merge activities",
        "master_version": int(current_master_version)
    }))

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 3: IDENTIFY RECORDS THAT REFERENCE CHANGED IDS")
logger.info("="*80)


logger.info("  Finding records that reference changed masters in their search_results...")

indirect_update_df = (
    spark.table(unified_dedup_table)
        .where("search_results IS NOT NULL AND search_results != ''")
        .select(
            col(id_key).alias("__referrer_id"),
            explode(
                regexp_extract_all(col("search_results"), lit(r'"([0-9a-f]{32})"'), lit(1))
            ).alias(id_key)
        )
        .join(changed_ids_df, on=id_key, how="inner")
        .select(col("__referrer_id").alias(id_key))
        .distinct()
)

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 4: SYNC MASTER TO UNIFIED_DEDUP WITH DELTA MERGE")
logger.info("="*80)

# Read current master table
current_master_df = spark.table(master_table)

# Get Delta table and determine target schema
unified_dedup_delta = DeltaTable.forName(spark, unified_dedup_table)
target_cols = {f.name.strip().lower() for f in spark.table(unified_dedup_table).schema}

# Select only master columns that exist in the target unified_dedup table
master_cols_for_sync = [c for c in current_master_df.columns if c.strip().lower() in target_cols]

# Prepare master data with empty search/scoring results for changed records only.
# For unchanged records, leave search_results as NULL → MERGE preserves existing.

_changed_flag_df = changed_ids_df.withColumn("__changed", lit(True))

master_with_results_df = (
    current_master_df
    .select(*master_cols_for_sync)
    .join(_changed_flag_df, on=id_key, how="left")
    .withColumn(
        "search_results",
        when(col("__changed").isNotNull(), lit("")).otherwise(lit(None))
    )
    .withColumn(
        "scoring_results",
        when(col("__changed").isNotNull(), lit("")).otherwise(lit(None))
    )
    .drop("__changed")
)

logger.info(f"  Prepared master data with conditional result clearing")

# Build update set - only update columns that exist in both source and target
# This allows us to preserve search_results for unchanged records
update_cols = {col_name: f"source.{col_name}" for col_name in master_cols_for_sync}
update_cols["search_results"] = "COALESCE(source.search_results, target.search_results)"
update_cols["scoring_results"] = "COALESCE(source.scoring_results, target.scoring_results)"

# Perform single MERGE operation
logger.info("  Executing Delta MERGE operation...")
logger.info("    - INSERT: New master records")
logger.info("    - UPDATE: Changed master records (with cleared results)")
logger.info("    - UPDATE: Unchanged master records (preserve results)")
logger.info("    - DELETE: Records not in master anymore")

(
    unified_dedup_delta.alias("target")
    .merge(
        master_with_results_df.alias("source"),
        f"target.{id_key} = source.{id_key}"
    )
    .whenMatchedUpdate(set=update_cols)
    .whenNotMatchedInsertAll()
    .whenNotMatchedBySourceDelete()
    .execute()
)

logger.info(f"Delta MERGE completed successfully")
# Removed log-only full-table .count() calls on unified_dedup table —
# triggered redundant scans on large tables without affecting logic.

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 5: CLEAR SEARCH RESULTS FOR INDIRECT UPDATES")
logger.info("="*80)

# For records that reference changed masters but weren't directly updated,
# we need to clear their search_results and scoring_results
# This is separate from the main merge because these are records that exist
# in both source and target but need special handling based on their references

# Indirect-only = referrers minus direct-updates.
# Issue the MERGE unconditionally — Delta MERGE on an empty source is a no-op.
# A prior head(1) guard would have re-executed the upstream chain
# (unified_dedup scan + regexp_extract_all + explode + join + anti-join) a
# second time when the MERGE ran. Caching the DataFrame would avoid the
# re-execution, but cache/persist are not used (serverless constraint), so
# instead we just skip the gate.
logger.info("  Clearing search results for records that reference changed masters (MERGE; no-op if no rows)...")

indirect_only_df = indirect_update_df.join(changed_ids_df, on=id_key, how="left_anti")

indirect_clear_df = (
    indirect_only_df
    .select(col(id_key))
    .withColumn("search_results", lit("").cast(StringType()))
    .withColumn("scoring_results", lit("").cast(StringType()))
)

unified_dedup_delta.alias("target").merge(
    indirect_clear_df.alias("source"),
    f"target.{id_key} = source.{id_key}"
).whenMatchedUpdate(set={
    "search_results": "source.search_results",
    "scoring_results": "source.scoring_results"
}).execute()

logger.info("  Indirect-update MERGE complete")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 6: FINAL STATISTICS")
logger.info("="*80)
# Removed log-only full-table .count() calls (filter().count() on
# unified_dedup) — they scanned the entire table each run for log lines only.

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("INCREMENTAL PROCESSING COMPLETE")
logger.info("="*80)

summary = {
    "status": "success",
    "mode": "incremental",
    "master_version": int(current_master_version),
    "timestamp": datetime.now().isoformat()
}

logger.info(f"\nSummary:")
logger.info(f"Master version: {current_master_version}")
logger.info(f"Note: Metadata will be updated at end of pipeline")
logger.info("="*80)

# Set task values
dbutils.jobs.taskValues.set("unified_dedup_sync_complete", True)
dbutils.jobs.taskValues.set("master_version", int(current_master_version))

# COMMAND ----------

logger_instance.shutdown()
