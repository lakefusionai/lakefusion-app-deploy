# Databricks notebook source
"""
COMPREHENSIVE INCREMENTAL DELETE PROCESSING - NEW ARCHITECTURE WITH SURVIVORSHIP

PURPOSE:
1. Process DELETE incrementals from ALL source tables
2. Update unified table status to DELETE (no physical deletion)
3. Apply OLD architecture logic for master table deletes based on scenarios
4. Handle survivorship recalculation for multi-contributor deletes

FLOW:
1. Loop through source tables with deletes
2. Read CDF deletes from each source
3. Update unified table status to DELETE by surrogate_key
4. Identify lakefusion_ids affected via master_lakefusion_id in unified table
5. Categorize by scenarios (ACTIVE, STANDALONE, MULTI_CONTRIBUTOR) using contributor count
6. Apply appropriate delete logic based on scenario
7. For MULTI_CONTRIBUTOR: Recalculate survivorship with remaining contributors
"""

import json
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, ArrayType, TimestampType
from datetime import datetime



# COMMAND ----------

# MAGIC %run ../../utils/match_merge_utils

# COMMAND ----------

# WIDGETS
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("primary_table", "", "Primary Table")
dbutils.widgets.text("primary_key", "", "Primary Key Column")
dbutils.widgets.text("dataset_tables", "", "Dataset Tables (JSON)")
dbutils.widgets.text("dataset_objects", "", "Dataset Objects (JSON)")
dbutils.widgets.text("entity_attributes", "", "Entity Attributes (JSON)")
dbutils.widgets.text("entity_attributes_datatype", "", "Entity Attributes Datatype (JSON)")
dbutils.widgets.text("attributes_mapping_json", "{}", "Attributes Mapping (JSON)")
dbutils.widgets.text("attributes", "", "Match Attributes (JSON)")
dbutils.widgets.text("survivorship_config", "", "Survivorship Config (JSON)")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("catalog_name", "", "Catalog Name")

# COMMAND ----------

# READ PARAMETERS
entity = dbutils.widgets.get("entity")
primary_table = dbutils.widgets.get("primary_table")
primary_key = dbutils.widgets.get("primary_key")
dataset_tables = dbutils.widgets.get("dataset_tables")
dataset_objects = dbutils.widgets.get("dataset_objects")
entity_attributes = dbutils.widgets.get("entity_attributes")
entity_attributes_datatype = dbutils.widgets.get("entity_attributes_datatype")
attributes_mapping_json = dbutils.widgets.get("attributes_mapping_json")
attributes = dbutils.widgets.get("attributes")
survivorship_config = dbutils.widgets.get("survivorship_config")
experiment_id = dbutils.widgets.get("experiment_id")
catalog_name = dbutils.widgets.get("catalog_name")

# COMMAND ----------

# OVERRIDE FROM PARSE TASK
entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
primary_table = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "primary_table", debugValue=primary_table)
primary_key = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "primary_key", debugValue=primary_key)
dataset_tables = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "dataset_tables", debugValue=dataset_tables)
dataset_objects = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "dataset_objects", debugValue=dataset_objects)
entity_attributes = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes", debugValue=entity_attributes)
entity_attributes_datatype = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes_datatype", debugValue=entity_attributes_datatype)
attributes_mapping_json = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "attributes_mapping", debugValue=attributes_mapping_json)
attributes = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "match_attributes", debugValue=attributes)

# FIXED: Read from correct task value key set by Parse task
# Parse task sets: TaskValueKey.DEFAULT_SURVIVORSHIP_RULES.value
survivorship_config = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "default_survivorship_rules", debugValue=survivorship_config if survivorship_config else "[]")

# Get info from Check_Increments_Exists
has_deletes = dbutils.jobs.taskValues.get("Check_Increments_Exists", "has_deletes", debugValue=True)
tables_with_deletes = dbutils.jobs.taskValues.get("Check_Increments_Exists", "tables_with_deletes", debugValue="[]")
table_version_info = dbutils.jobs.taskValues.get("Check_Increments_Exists", "table_version_info", debugValue="{}")

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

from lakefusion_core_engine.survivorship import SurvivorshipEngine

# COMMAND ----------

from lakefusion_core_engine.identifiers import generate_surrogate_key
from lakefusion_core_engine.models import RecordStatus, ActionType

# COMMAND ----------

# PARSE JSON
dataset_tables = json.loads(dataset_tables)
dataset_objects = json.loads(dataset_objects)
entity_attributes = json.loads(entity_attributes)
entity_attributes_datatype = json.loads(entity_attributes_datatype)
attributes_mapping_json = json.loads(attributes_mapping_json)
attributes = json.loads(attributes)
survivorship_config = json.loads(survivorship_config) if survivorship_config else []
tables_with_deletes = json.loads(tables_with_deletes)
table_version_info = json.loads(table_version_info)

# Convert has_deletes to boolean
if isinstance(has_deletes, str):
    has_deletes = has_deletes.lower() == 'true'

# Remove experiment_id hyphens
if experiment_id:
    experiment_id = experiment_id.replace("-", "")

# COMMAND ----------

# TRANSFORM SURVIVORSHIP RULES: Dataset IDs → Source Paths
# Parse task returns rules with dataset IDs in strategyRule, but SurvivorshipEngine needs source paths
logger.info("Transforming survivorship rules: dataset IDs → source paths...")

if survivorship_config:
    # Build dataset ID to path mapping
    dataset_id_to_path = {}
    for path, dataset_info in dataset_objects.items():
        dataset_id = dataset_info.get('id')
        if dataset_id:
            dataset_id_to_path[dataset_id] = path
    
    logger.info(f"  Dataset ID to Path mapping: {dataset_id_to_path}")
    
    # Transform each rule
    transformed_rules = []
    for rule in survivorship_config:
        transformed_rule = rule.copy()
        
        # Convert strategyRule from [50, 51, 52] to ["internal_dev.bronze.customer_primary", ...]
        if rule.get('strategyRule') and isinstance(rule['strategyRule'], list):
            transformed_strategy = []
            for dataset_id in rule['strategyRule']:
                if dataset_id in dataset_id_to_path:
                    transformed_strategy.append(dataset_id_to_path[dataset_id])
                else:
                    logger.warning(f"   Dataset ID {dataset_id} not found in mapping for attribute {rule.get('attribute')}")
            transformed_rule['strategyRule'] = transformed_strategy
        
        transformed_rules.append(transformed_rule)
    
    survivorship_config = transformed_rules
    logger.info(f" Transformed {len(survivorship_config)} survivorship rules")
    logger.info(f"  Sample rule: {survivorship_config[0] if survivorship_config else 'None'}")
else:
    logger.info(" No survivorship rules to transform")

# COMMAND ----------

# TABLE NAMES
unified_table = f"{catalog_name}.silver.{entity}_unified"
master_table = f"{catalog_name}.gold.{entity}_master"
meta_info_table = f"{catalog_name}.silver.table_meta_info"

if experiment_id:
    unified_table += f"_{experiment_id}"
    master_table += f"_{experiment_id}"

merge_activities_table = f"{master_table}_merge_activities"
master_attribute_version_sources_table = f"{master_table}_attribute_version_sources"

# COMMAND ----------

# UTILITY FUNCTIONS
# Create UDF for surrogate key generation
generate_surrogate_key_udf = udf(
    lambda source_path, source_id: generate_surrogate_key(source_path, str(source_id)), 
    StringType()
)

def get_mapping_for_table(source_table: str):
    """Get attribute mapping for a specific source table"""
    for mapping in attributes_mapping_json:
        if source_table in mapping:
            return mapping[source_table]
    return {}

def generate_attributes_combined(record_dict, attributes_list):
    """Generate attributes_combined string from attribute values"""
    values = []
    for attr in attributes_list:
        val = record_dict.get(attr)
        if val is not None and str(val).strip():
            values.append(str(val).strip().lower())
    return '||'.join(values) if values else ''

# ==============================================================================
# CDF DEDUPLICATION FUNCTION FOR DELETES
# ==============================================================================

def read_and_deduplicate_cdf_for_deletes(source_table, start_version, end_version, source_pk_column):
    """
    Read ALL CDF change types and deduplicate per surrogate_key.
    Keep only records where the LATEST operation is DELETE.
    Returns list of surrogate_keys that should be marked as DELETED.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    
    logger.info(f"  Reading ALL CDF change types for deduplication...")
    
    # Read CDF for ALL change types
    df_cdf = spark.read.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", start_version) \
        .option("endingVersion", end_version) \
        .table(source_table)
    
    # Classify operations
    df_cdf = df_cdf.withColumn(
        "operation_type",
        F.when(F.col("_change_type") == "insert", F.lit("insert"))
         .when(F.col("_change_type") == "update_postimage", F.lit("update"))
         .when(F.col("_change_type") == "delete", F.lit("delete"))
         .otherwise(F.col("_change_type"))
    )
    
    # Generate surrogate_key
    df_cdf = df_cdf.withColumn(
        "_source_pk_value",
        F.col(source_pk_column).cast("string")
    )
    
    df_cdf = df_cdf.withColumn(
        "surrogate_key",
        generate_surrogate_key_udf(F.lit(source_table), F.col("_source_pk_value"))
    )
    
    # DEDUPLICATION: Keep LATEST operation per surrogate_key
    window_spec = Window.partitionBy("surrogate_key") \
                        .orderBy(F.desc("_commit_version"), F.desc("_commit_timestamp"))
    
    df_latest = df_cdf.withColumn(
        "_row_num", F.row_number().over(window_spec)
    ).filter(F.col("_row_num") == 1)
    
    # Count by operation type
    operation_counts = df_latest.groupBy("operation_type").count().collect()
    logger.info(f"  Latest operations per surrogate_key:")
    for row in operation_counts:
        logger.info(f"    - {row.operation_type}: {row['count']}")
    
    # Keep ONLY records where latest operation is DELETE
    df_deletes_only = df_latest.filter(F.col("operation_type") == "delete")
    
    # Cheap emptiness check instead of full count
    if not df_deletes_only.take(1):
        logger.info(f"  After deduplication: 0 records with DELETE as latest operation")
        return []
    
    logger.info(f"  After deduplication: DELETE records present")
    
    # Return list of surrogate_keys to mark as DELETED
    delete_surrogate_keys = [row.surrogate_key for row in df_deletes_only.select("surrogate_key").collect()]
    
    return delete_surrogate_keys

# ==============================================================================

# COMMAND ----------

# INITIALIZE SURVIVORSHIP ENGINE
logger.info("Initializing Survivorship Engine...")

survivorship_engine = None
if survivorship_config:
    try:
        survivorship_engine = SurvivorshipEngine(
            survivorship_config=survivorship_config,
            entity_attributes=entity_attributes,
            entity_attributes_datatype=entity_attributes_datatype,
            id_key="surrogate_key"
        )
        logger.info(f" Survivorship Engine initialized with {len(survivorship_config)} rules")
    except Exception as e:
        logger.error(f" Failed to initialize Survivorship Engine: {e}")
        logger.warning("   Survivorship recalculation will be skipped")
        survivorship_engine = None
else:
    logger.info(" No survivorship_config provided - survivorship recalculation will be skipped")

# COMMAND ----------

# EARLY EXIT CHECK
if not has_deletes:
    logger.info("=" * 80)
    logger.info("  SKIPPING: No DELETE incrementals found")
    logger.info("=" * 80)
    dbutils.notebook.exit("SKIPPED_NO_DELETES")

if not tables_with_deletes:
    logger.info("=" * 80)
    logger.info("  SKIPPING: No tables with deletes found")
    logger.info("=" * 80)
    dbutils.notebook.exit("SKIPPED_NO_TABLES_WITH_DELETES")

logger.info("\n" + "=" * 80)
logger.info(" STARTING INCREMENTAL DELETE PROCESSING")
logger.info("=" * 80)
logger.info(f"Entity: {entity}")
logger.info(f"Tables with deletes: {len(tables_with_deletes)}")
logger.info(f"Unified table: {unified_table}")
logger.info(f"Master table: {master_table}")
logger.info("=" * 80)

# COMMAND ----------

# ==============================================================================
# PHASE 1: UPDATE UNIFIED TABLE STATUS TO DELETE
# ==============================================================================
logger.info("\n" + "=" * 80)
logger.info(" PHASE 1: UPDATING UNIFIED TABLE STATUS TO DELETE")
logger.info("=" * 80)

# Track all records to mark as DELETE
records_to_mark_delete = []

for source_table in tables_with_deletes:
    logger.info(f"\n{'='*60}")
    logger.info(f" Processing: {source_table}")
    logger.info(f"{'='*60}")
    
    version_info = table_version_info.get(source_table)
    
    if not version_info or not version_info.get("has_deletes", False):
        logger.info(f" No deletes for {source_table}, skipping...")
        continue
    
    # FIXED: Use last_processed_version (consistent with UPDATE notebook)
    last_processed_version = version_info["last_processed_version"]
    current_version = version_info["current_version"]
    starting_version = last_processed_version + 1
    expected_delete_count = version_info.get("delete_count", 0)
    
    logger.info(f" Processing versions {starting_version} to {current_version} (last processed: {last_processed_version}), expecting {expected_delete_count} deletes")
    
    # Get dataset object for this source
    dataset_obj = None
    for ds_key, ds_val in dataset_objects.items():
        if ds_val['path'] == source_table:
            dataset_obj = ds_val
            break
    
    if not dataset_obj:
        logger.info(f" No dataset object found for {source_table}, skipping...")
        continue
    
    dataset_id = dataset_obj['id']
    source_path = dataset_obj['path']
    
    # Get attribute mapping for this source
    attr_mapping = get_mapping_for_table(source_table)
    if not attr_mapping:
        logger.info(f" No attribute mapping found for {source_table}, skipping...")
        continue
    
    # REPLACE LINES 300-334 WITH THIS:

    # Determine primary key column name in source
    source_primary_key = attr_mapping.get(primary_key, primary_key)
    
    # Get starting_version from table_version_info
    starting_version_from_info = version_info.get("starting_version")
    
    if starting_version_from_info is None:
        # Fallback: calculate from last_processed_version
        starting_version_from_info = starting_version
    
    # READ CDF WITH DEDUPLICATION
    try:
        deleted_sks = read_and_deduplicate_cdf_for_deletes(
            source_table,
            starting_version_from_info,
            current_version,
            source_primary_key
        )
        
        if len(deleted_sks) == 0:
            logger.info(f"   No deletes to process after deduplication")
            continue
        
        logger.info(f"   Generated {len(deleted_sks)} surrogate keys to mark as DELETE")
        records_to_mark_delete.extend(deleted_sks)
        
    except Exception as e:
        logger.error(f"Error reading CDF from {source_table}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        continue

logger.info(f"\nTotal records to mark as DELETE: {len(records_to_mark_delete)}")

# COMMAND ----------

# COLLECT RECORDS TO DELETE
if records_to_mark_delete:
    logger.info(f"\n Collected {len(records_to_mark_delete)} surrogate keys for deletion")
else:
    logger.info("  No records to delete")

# COMMAND ----------

# ==============================================================================
# PHASE 1.5: UPDATE UNIFIED TABLE STATUS TO DELETED
# ==============================================================================
logger.info("\n" + "=" * 80)
logger.info(" PHASE 1.5: Update Unified Table Status to DELETED")
logger.info("=" * 80)
logger.info("  CRITICAL: Unified must be updated BEFORE survivorship recalculation")
logger.info("   Survivorship will fetch contributors from unified and expects DELETED status")

if records_to_mark_delete:
    logger.info(f"\n Found {len(records_to_mark_delete)} records with DELETE as latest operation")

    # Create temp view for all surrogate keys to delete (avoids huge IN clause + batched loops)
    from pyspark.sql import functions as F
    sks_df = spark.createDataFrame([(sk,) for sk in records_to_mark_delete], ["surrogate_key"])
    sks_df.createOrReplaceTempView("_phase15_sks_to_delete")

    # Check which records actually exist in unified
    existing_records = spark.sql(f"""
        SELECT u.surrogate_key
        FROM {unified_table} u
        INNER JOIN _phase15_sks_to_delete d ON u.surrogate_key = d.surrogate_key
        WHERE u.record_status IN ('{RecordStatus.ACTIVE.value}', '{RecordStatus.MERGED.value}')
    """).collect()

    existing_sks = [row.surrogate_key for row in existing_records]
    skipped_count = len(records_to_mark_delete) - len(existing_sks)

    if len(existing_sks) > 0:
        logger.info(f" Updating {len(existing_sks)} records to DELETED status in unified table...")

        try:
            # Single UPDATE using temp view (1 Delta commit instead of N batched commits)
            spark.sql(f"""
                UPDATE {unified_table}
                SET record_status = '{RecordStatus.DELETED.value}'
                WHERE surrogate_key IN (SELECT surrogate_key FROM _phase15_sks_to_delete)
                AND record_status IN ('{RecordStatus.ACTIVE.value}', '{RecordStatus.MERGED.value}')
            """)

            total_updated = len(existing_sks)
            logger.info(f" Successfully updated {total_updated} records to DELETED status")

        except Exception as e:
            logger.error(f" Error updating unified table: {e}")
            raise
    else:
        logger.info(f"  0 records exist in unified table to update")

    if skipped_count > 0:
        logger.info(f"  Skipped {skipped_count} records (not found in unified table)")
        logger.info(f"   These records were never inserted due to deduplication")

    logger.info(f"\n Summary:")
    logger.info(f"   - Total DELETE operations found: {len(records_to_mark_delete)}")
    logger.info(f"   - Actually updated in unified: {len(existing_sks)}")
    logger.info(f"   - Skipped (not in unified): {skipped_count}")
    
else:
    logger.info("\n  No records to update in unified table")

# COMMAND ----------

# ==============================================================================
# PHASE 2: IDENTIFY AND CATEGORIZE AFFECTED MASTERS
# ==============================================================================
logger.info("\n" + "=" * 80)
logger.info(" PHASE 2: IDENTIFYING AFFECTED MASTER RECORDS")
logger.info("=" * 80)

# Get lakefusion_ids associated with deleted surrogate keys directly from unified table
deleted_by_surrogate = {}

if records_to_mark_delete:
    logger.info(f"Looking up master_lakefusion_id for {len(records_to_mark_delete)} deleted surrogate keys from unified table...")

    # Single query using temp view (reuse from Phase 1.5) instead of batched loop
    df_lookup = spark.sql(f"""
        SELECT u.surrogate_key, u.master_lakefusion_id
        FROM {unified_table} u
        INNER JOIN _phase15_sks_to_delete d ON u.surrogate_key = d.surrogate_key
        WHERE u.master_lakefusion_id IS NOT NULL
        AND u.master_lakefusion_id != ''
    """)

    for row in df_lookup.collect():
        deleted_by_surrogate[row.surrogate_key] = {
            'lakefusion_id': row.master_lakefusion_id,
            'surrogate_key': row.surrogate_key
        }

    logger.info(f" Found {len(deleted_by_surrogate)} surrogate keys mapped to master_lakefusion_ids")

# COMMAND ----------

# CATEGORIZE MASTER RECORDS
logger.info("\n Categorizing affected master records by scenario...")

# Get unique lakefusion_ids
deleted_lakefusion_ids = list(set([info['lakefusion_id'] for info in deleted_by_surrogate.values() if info['lakefusion_id']]))

# Create reverse mapping: lakefusion_id -> list of deleted surrogate_keys
lakefusion_to_deleted_sks = {}
for sk, info in deleted_by_surrogate.items():
    lfid = info['lakefusion_id']
    if lfid:
        if lfid not in lakefusion_to_deleted_sks:
            lakefusion_to_deleted_sks[lfid] = []
        lakefusion_to_deleted_sks[lfid].append(sk)

logger.info(f"Unique lakefusion_ids affected: {len(deleted_lakefusion_ids)}")

# Initialize variables for completion section
standalone_ids = []
multi_contributor_ids = []
records_processed_with_survivorship = []
master_ids_to_delete = []
all_master_updates = []
attr_version_ids_to_delete = []
all_attribute_versions = []
all_merge_activities = []

if not deleted_lakefusion_ids:
    logger.info(" No MERGED records to process in master - only ACTIVE records were marked as DELETE")
    logger.info("  Skipping master processing, proceeding to completion...")

# COMMAND ----------

# Build categorization query using unified table contributor count
if deleted_lakefusion_ids:
    # Create temp views for IDs (avoids huge IN clauses and VALUES lists at 50K+ scale)
    deleted_lfids_df = spark.createDataFrame(
        [(lid,) for lid in deleted_lakefusion_ids], ["lakefusion_id"]
    )
    deleted_lfids_df.createOrReplaceTempView("_categorize_deleted_ids")

    df_categorized = spark.sql(f"""
        WITH RemainingContributorCount AS (
            SELECT
                u.master_lakefusion_id,
                COUNT(*) as contributor_count
            FROM {unified_table} u
            INNER JOIN _categorize_deleted_ids d ON u.master_lakefusion_id = d.lakefusion_id
            WHERE u.record_status = '{RecordStatus.MERGED.value}'
            AND u.surrogate_key NOT IN (SELECT surrogate_key FROM _phase15_sks_to_delete)
            GROUP BY u.master_lakefusion_id
        )
        SELECT
            d.lakefusion_id,
            COALESCE(cc.contributor_count, 0) as remaining_contributor_count,
            CASE
                WHEN COALESCE(cc.contributor_count, 0) = 0 THEN 'STANDALONE'
                ELSE 'MULTI_CONTRIBUTOR'
            END as scenario
        FROM _categorize_deleted_ids d
        LEFT JOIN RemainingContributorCount cc ON d.lakefusion_id = cc.master_lakefusion_id
    """)
    
    # Debug: Show categorization details
    logger.info("   Categorization details:")
    for row in df_categorized.collect():
        logger.info(f"     {row.lakefusion_id}: remaining={row.remaining_contributor_count} → {row.scenario}")
    
    # Collect categorizations
    standalone_ids = [row.lakefusion_id for row in df_categorized.filter(col("scenario") == "STANDALONE").collect()]
    multi_contributor_ids = [row.lakefusion_id for row in df_categorized.filter(col("scenario") == "MULTI_CONTRIBUTOR").collect()]
    
    logger.info(f" Categorization Results:")
    logger.info(f"   STANDALONE (no remaining contributors): {len(standalone_ids)}")
    logger.info(f"   MULTI_CONTRIBUTOR (has remaining contributors): {len(multi_contributor_ids)}")
else:
    # No MERGED records to categorize - initialize empty lists
    standalone_ids = []
    multi_contributor_ids = []
    logger.info("  Skipping categorization - no MERGED records to process")

# COMMAND ----------

# ==============================================================================
# PHASE 3: BATCH EXECUTION - COLLECT ALL UPDATES
# ==============================================================================

# Initialize batch collection lists (always needed for completion section)
master_ids_to_delete = []
attr_version_ids_to_delete = []
all_merge_activities = []
all_master_updates = []
all_attribute_versions = []
records_requiring_survivorship = []
records_processed_with_survivorship = []

if deleted_lakefusion_ids:
    logger.info("\n" + "=" * 80)
    logger.info(" PHASE 3: COLLECTING MASTER TABLE UPDATES")
    logger.info("=" * 80)

# COMMAND ----------

if deleted_lakefusion_ids:
    # SCENARIO 1: STANDALONE DELETIONS  
    logger.info(f"\n SCENARIO 1: Processing STANDALONE deletions ({len(standalone_ids)} records)...")

    if standalone_ids:
        for lakefusion_id in standalone_ids:
            # Collect for deletion from master table only
            master_ids_to_delete.append(lakefusion_id)
            
            # Get deleted surrogate_keys for this lakefusion_id
            deleted_sks = lakefusion_to_deleted_sks.get(lakefusion_id, [])
            
            # Log merge activity for each deleted surrogate_key
            for deleted_sk in deleted_sks:
                all_merge_activities.append({
                    'master_id': lakefusion_id,
                    'match_id': deleted_sk,
                    'source': 'SYSTEM',
                    'version': None,  # Will be set during batch execution
                    'action_type': ActionType.SOURCE_DELETE.value
                })
        
        logger.info(f" Prepared {len(standalone_ids)} STANDALONE records for deletion")
    else:
        logger.info("No STANDALONE records to process")

# COMMAND ----------

if deleted_lakefusion_ids:
    # SCENARIO 2: MULTI_CONTRIBUTOR DELETIONS (SURVIVORSHIP RECALCULATION)
    logger.info("\n" + "=" * 60)
    logger.info(f" SCENARIO 2: Processing MULTI_CONTRIBUTOR deletions ({len(multi_contributor_ids)} records)...")
    logger.info("   These require survivorship recalculation with remaining contributors")

    if multi_contributor_ids:
        if not survivorship_engine:
            logger.warning(" Survivorship Engine not initialized - skipping MULTI_CONTRIBUTOR processing")
            logger.warning("   These records will NOT be processed correctly without survivorship engine!")
        else:
            # BATCH FETCH all remaining contributors for ALL multi-contributor masters in 1 query
            from collections import defaultdict

            multi_ids_df = spark.createDataFrame(
                [(lid,) for lid in multi_contributor_ids], ["lakefusion_id"]
            )
            multi_ids_df.createOrReplaceTempView("_scenario2_multi_ids")

            all_contributor_rows = spark.sql(f"""
                SELECT u.*
                FROM {unified_table} u
                INNER JOIN _scenario2_multi_ids m ON u.master_lakefusion_id = m.lakefusion_id
                WHERE u.record_status = '{RecordStatus.MERGED.value}'
                ORDER BY u.master_lakefusion_id, u.surrogate_key
            """).collect()

            # Group contributors by master on the driver
            contributors_by_master = defaultdict(list)
            for row in all_contributor_rows:
                contributors_by_master[row.master_lakefusion_id].append(row)

            logger.info(f" Batch fetched {len(all_contributor_rows)} contributor records for {len(contributors_by_master)} masters")

            for lakefusion_id in multi_contributor_ids:
                try:
                    # Get deleted surrogate keys for this master
                    deleted_sks_for_master = lakefusion_to_deleted_sks.get(lakefusion_id, [])

                    # Use pre-fetched contributors (no spark.sql() call)
                    contributor_records = contributors_by_master.get(lakefusion_id, [])

                    if not contributor_records:
                        logger.warning(f"    No remaining contributors found for {lakefusion_id} - will be deleted")

                        master_ids_to_delete.append(lakefusion_id)

                        for deleted_sk in deleted_sks_for_master:
                            all_merge_activities.append({
                                'master_id': lakefusion_id,
                                'match_id': deleted_sk,
                                'source': 'SYSTEM',
                                'version': None,
                                'action_type': ActionType.SOURCE_DELETE.value
                            })
                        continue

                    records_requiring_survivorship.append(lakefusion_id)

                    # Convert to list of dicts for survivorship engine
                    unified_records = []
                    for row in contributor_records:
                        record_dict = row.asDict()
                        if 'surrogate_key' not in record_dict or 'source_path' not in record_dict:
                            logger.warning(f"    Missing required fields in contributor record")
                            continue
                        unified_records.append(record_dict)

                    if not unified_records:
                        logger.warning(f"    No valid contributor records for survivorship")
                        continue

                    # APPLY SURVIVORSHIP WITH REMAINING CONTRIBUTORS
                    survivorship_result = survivorship_engine.apply_survivorship(
                        unified_records=unified_records
                    )

                    # Extract results
                    resultant_record = survivorship_result.resultant_record
                    attribute_source_mapping = survivorship_result.resultant_master_attribute_source_mapping

                    # Build update record for master
                    update_record = {'lakefusion_id': lakefusion_id}

                    for attr in entity_attributes:
                        if attr == 'lakefusion_id':
                            continue
                        attr_value = resultant_record.get(attr)
                        update_record[attr] = attr_value

                    # Add attributes_combined if present
                    if 'attributes_combined' in resultant_record:
                        update_record['attributes_combined'] = resultant_record.get('attributes_combined')
                    elif attributes:
                        update_record['attributes_combined'] = generate_attributes_combined(resultant_record, attributes)

                    # Convert attribute_source_mapping to expected format
                    updated_attr_mapping = []
                    for attr_map in attribute_source_mapping:
                        updated_attr_mapping.append({
                            'attribute_name': attr_map.attribute_name,
                            'attribute_value': attr_map.attribute_value,
                            'source': attr_map.source
                        })

                    # Add attributes_combined to mapping if not already present
                    if 'attributes_combined' not in [m['attribute_name'] for m in updated_attr_mapping]:
                        attributes_combined_source = updated_attr_mapping[0]['source'] if updated_attr_mapping else 'unknown'
                        updated_attr_mapping.append({
                            'attribute_name': 'attributes_combined',
                            'attribute_value': update_record.get('attributes_combined'),
                            'source': attributes_combined_source
                        })

                    # Collect into batch lists
                    all_master_updates.append(update_record)
                    all_attribute_versions.append({
                        'lakefusion_id': lakefusion_id,
                        'version': None,
                        'attribute_source_mapping': updated_attr_mapping
                    })

                    # Log merge activity
                    all_sources = []
                    for m in updated_attr_mapping:
                        source = m['source']
                        if ',' in source:
                            all_sources.extend([s.strip() for s in source.split(',')])
                        else:
                            all_sources.append(source)

                    unique_sources = ','.join(sorted(set(all_sources)))

                    for deleted_sk in deleted_sks_for_master:
                        all_merge_activities.append({
                            'master_id': lakefusion_id,
                            'match_id': deleted_sk,
                            'source': unique_sources,
                            'version': None,
                            'action_type': ActionType.SOURCE_DELETE.value
                        })

                    records_processed_with_survivorship.append(lakefusion_id)
                    logger.info(f"    Survivorship applied for {lakefusion_id}")

                except Exception as e:
                    logger.error(f" Error processing multi-contributor {lakefusion_id}: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    continue

            logger.info(f"\n Processed {len(records_processed_with_survivorship)} MULTI_CONTRIBUTOR records with survivorship")
            logger.info(f"   Total requiring survivorship: {len(records_requiring_survivorship)}")
            logger.info(f"   Successfully processed: {len(records_processed_with_survivorship)}")
    else:
        logger.info("No MULTI_CONTRIBUTOR records to process")

# COMMAND ----------

# ==============================================================================
# PHASE 4: EXECUTE BATCH OPERATIONS
# ==============================================================================
logger.info("\n" + "=" * 80)
logger.info(" PHASE 4: EXECUTING BATCH OPERATIONS")
logger.info("=" * 80)

# STEP 1: DELETE FROM MASTER TABLE
if master_ids_to_delete:
    unique_master_ids = list(set(master_ids_to_delete))
    logger.info(f"\n  STEP 1: Deleting {len(unique_master_ids)} records from master table...")

    # Single DELETE using temp view (1 Delta commit instead of N batched commits)
    master_del_df = spark.createDataFrame([(lid,) for lid in unique_master_ids], ["lakefusion_id"])
    master_del_df.createOrReplaceTempView("_step1_master_ids_to_delete")

    spark.sql(f"""
        DELETE FROM {master_table}
        WHERE lakefusion_id IN (SELECT lakefusion_id FROM _step1_master_ids_to_delete)
    """)

    logger.info(f" Successfully deleted {len(unique_master_ids)} records from master table")
else:
    logger.info("\n  STEP 1: No records to delete from master table")

# COMMAND ----------

# STEP 2: UPDATE MASTER TABLE (SURVIVORSHIP RESULTS)
if all_master_updates:
    logger.info(f"\n STEP 2: Updating {len(all_master_updates)} master records with survivorship results...")
    
    # Get current master version before updates
    current_master_version = spark.sql(f"DESCRIBE HISTORY {master_table} LIMIT 1").first()['version']
    
    # Create DataFrame for updates
    master_update_schema = StructType([StructField('lakefusion_id', StringType(), False)])
    for attr in entity_attributes:
        if attr != 'lakefusion_id':
            master_update_schema.add(StructField(attr, StringType(), True))
    
    # Add attributes_combined if needed
    if attributes:
        master_update_schema.add(StructField('attributes_combined', StringType(), True))
    
    df_master_updates = spark.createDataFrame(all_master_updates, schema=master_update_schema)
    
    # Perform merge
    master_delta = DeltaTable.forName(spark, master_table)
    
    master_delta.alias("target").merge(
        source=df_master_updates.alias("source"),
        condition="target.lakefusion_id = source.lakefusion_id"
    ).whenMatchedUpdateAll().execute()
    
    # Get new master version after updates
    new_master_version = spark.sql(f"DESCRIBE HISTORY {master_table} LIMIT 1").first()['version']
    
    logger.info(f" Updated {len(all_master_updates)} master records")
    logger.info(f"   Master version: {current_master_version} → {new_master_version}")
    
    # Update version in attribute_versions and merge_activities for survivorship records
    for attr_version in all_attribute_versions:
        if attr_version['version'] is None:
            attr_version['version'] = new_master_version
    
    for merge_activity in all_merge_activities:
        if merge_activity['version'] is None and merge_activity['master_id'] in records_processed_with_survivorship:
            merge_activity['version'] = new_master_version
else:
    logger.info("\n  STEP 2: No master updates (no survivorship recalculations)")
    new_master_version = None

# COMMAND ----------

# STEP 3: INSERT ATTRIBUTE VERSION SOURCES (SURVIVORSHIP RESULTS)
if all_attribute_versions:
    logger.info(f"\n STEP 3: Inserting {len(all_attribute_versions)} attribute version sources...")
    
    # Define schema for attribute versions
    attr_version_schema = StructType([
        StructField('lakefusion_id', StringType(), False),
        StructField('version', IntegerType(), False),
        StructField('attribute_source_mapping', ArrayType(StructType([
            StructField('attribute_name', StringType(), True),
            StructField('attribute_value', StringType(), True),
            StructField('source', StringType(), True)
        ])), False)
    ])
    
    # Create DataFrame
    df_attr_versions = spark.createDataFrame(all_attribute_versions, schema=attr_version_schema)
    
    # Batch insert using Delta merge
    attr_version_delta = DeltaTable.forName(spark, master_attribute_version_sources_table)
    
    attr_version_delta.alias("target").merge(
        source=df_attr_versions.alias("source"),
        condition="""target.lakefusion_id = source.lakefusion_id 
                     AND target.version = source.version"""
    ).whenNotMatchedInsertAll().execute()
    
    logger.info(f" Batch inserted {len(all_attribute_versions)} attribute version sources")
else:
    logger.info("\n  STEP 3: No attribute version sources to insert")

# COMMAND ----------

# STEP 4: LOG MERGE ACTIVITIES
# Set version for deletion merge activities if not already set
if new_master_version is None:
    # Get current master version for deletion activities
    current_master_version = spark.sql(f"DESCRIBE HISTORY {master_table} LIMIT 1").first()['version']
    new_master_version = current_master_version + 1

for merge_activity in all_merge_activities:
    if merge_activity['version'] is None:
        merge_activity['version'] = new_master_version

if all_merge_activities:
    logger.info(f"\n STEP 4: Logging {len(all_merge_activities)} merge activities...")
    
    # Define schema for merge activities, including created_at
    merge_activities_schema = StructType([
        StructField('master_id', StringType(), False),
        StructField('match_id', StringType(), True),
        StructField('source', StringType(), False),
        StructField('version', IntegerType(), False),
        StructField('action_type', StringType(), False),
        StructField('created_at', TimestampType(), False)
    ])
    
    # Add created_at to each record
    now = datetime.now()
    all_merge_activities_with_ts = [
        {**record, 'created_at': now} for record in all_merge_activities
    ]
    
    # Create DataFrame for merge activities
    merge_activities_df = spark.createDataFrame(all_merge_activities_with_ts, schema=merge_activities_schema)
    
    # Batch insert using Delta merge
    merge_activities_delta_table = DeltaTable.forName(spark, merge_activities_table)
    
    merge_activities_delta_table.alias("target").merge(
        source=merge_activities_df.alias("source"),
        condition="""target.master_id = source.master_id 
                     AND target.match_id = source.match_id 
                     AND target.source = source.source 
                     AND target.version = source.version 
                     AND target.action_type = source.action_type"""
    ).whenNotMatchedInsertAll().execute()
    
    logger.info(f" Batch inserted {len(all_merge_activities)} merge activities")
else:
    logger.info("\n  STEP 4: No merge activities to insert")

# COMMAND ----------

# STEP 5: CLEAN UP PROCESSED_UNIFIED TABLE
# Get all deleted lakefusion_ids
deleted_lakefusion_ids = list(set([info['lakefusion_id'] for info in deleted_by_surrogate.values() if info['lakefusion_id']]))

# COMMENTED OUT PER USER REQUEST - NOT REMOVING FROM PROCESSED_UNIFIED FOR RE-MATCHING
# if deleted_lakefusion_ids:
#     print(f"\n STEP 5: Cleaning up processed_unified table...")
#     
#     processed_unified_table = f"{catalog_name}.silver.{entity}_processed_unified"
#     if experiment_id:
#         processed_unified_table += f"_{experiment_id}"
#     
#     try:
#         table_exists = spark.catalog.tableExists(processed_unified_table)
#         
#         if table_exists:
#             processed_unified_delta = DeltaTable.forName(spark, processed_unified_table)
#             
#             # Delete by lakefusion_id column
#             processed_unified_delta.delete(col('lakefusion_id').isin(deleted_lakefusion_ids))
#             print(f" Deleted {len(deleted_lakefusion_ids)} records from processed_unified table")
#             
#             # Delete by master_lakefusion_id column if exists
#             try:
#                 processed_unified_delta.delete(col('master_lakefusion_id').isin(deleted_lakefusion_ids))
#                 print(" Cleaned up processed_unified table references")
#             except:
#                 pass  # Column might not exist
#         else:
#             print(f" Processed unified table {processed_unified_table} does not exist. Skipping cleanup.")
#     
#     except Exception as e:
#         print(f" Error cleaning up processed unified table: {e}")
#         print("Skipping processed unified table cleanup")
# else:
#     print("\n  STEP 5: No lakefusion_ids to clean from processed_unified")
logger.info("\n  STEP 5: Processed unified cleanup DISABLED per configuration")

# COMMAND ----------

# STEP 6: CLEAR SEARCH_RESULTS FOR RECORDS REFERENCING DELETED IDS (EXPLODE + EQUI-JOIN)
# Strategy: Extract lakefusion_ids from search_results via split+explode, then equi-join on deleted IDs.
# This replaces the per-ID loop of LIKE queries (2N Spark SQL calls) with 1 batch read + 1 conditional UPDATE.
if deleted_lakefusion_ids:
    logger.info("\n STEP 6: Clearing search_results for records referencing deleted lakefusion_ids...")
    logger.info("=" * 80)

    unified_deduplicate_table = f"{catalog_name}.silver.{entity}_unified_deduplicate"
    if experiment_id:
        unified_deduplicate_table += f"_{experiment_id}"

    total_cleared_across_tables = 0

    # Create a broadcast-friendly DataFrame of deleted IDs
    deleted_ids_df = spark.createDataFrame(
        [(lid,) for lid in deleted_lakefusion_ids],
        ["deleted_id"]
    )
    deleted_ids_df.createOrReplaceTempView("_step6_deleted_ids")

    # TABLE 1: Clear in UNIFIED table (uses surrogate_key)
    if spark.catalog.tableExists(unified_table):
        logger.info(f"\n Processing unified table: {unified_table}")

        df_matched_keys = spark.sql(f"""
            SELECT DISTINCT u.surrogate_key
            FROM {unified_table} u
            LATERAL VIEW explode(split(u.search_results, '[,\\\\s\\\\[\\\\]]+')) AS token
            WHERE u.record_status = 'ACTIVE'
            AND u.search_results IS NOT NULL
            AND u.search_results != ''
            AND token IN (SELECT deleted_id FROM _step6_deleted_ids)
        """)

        matched_count_unified = df_matched_keys.count()
        logger.info(f" Found {matched_count_unified} rows to clear in unified table")

        if matched_count_unified > 0:
            matched_keys = [row.surrogate_key for row in df_matched_keys.collect()]
            keys_str = "','".join(matched_keys)
            spark.sql(f"""
                UPDATE {unified_table}
                SET search_results = '',
                    scoring_results = ''
                WHERE surrogate_key IN ('{keys_str}')
            """)
            logger.info(f" Cleared search_results for {matched_count_unified} rows")
            total_cleared_across_tables += matched_count_unified
        else:
            logger.info(f" No records needed clearing in unified")
    else:
        logger.info(f" Table {unified_table} does not exist, skipping...")

    # TABLE 2: Clear in UNIFIED_DEDUPLICATE table (uses lakefusion_id)
    if spark.catalog.tableExists(unified_deduplicate_table):
        logger.info(f"\n Processing unified_deduplicate table: {unified_deduplicate_table}")

        df_matched_dedup_keys = spark.sql(f"""
            SELECT DISTINCT u.lakefusion_id
            FROM {unified_deduplicate_table} u
            LATERAL VIEW explode(split(u.search_results, '[,\\\\s\\\\[\\\\]]+')) AS token
            WHERE u.search_results IS NOT NULL
            AND u.search_results != ''
            AND token IN (SELECT deleted_id FROM _step6_deleted_ids)
            AND u.lakefusion_id NOT IN (SELECT deleted_id FROM _step6_deleted_ids)
        """)

        matched_count_dedup = df_matched_dedup_keys.count()
        logger.info(f" Found {matched_count_dedup} rows to clear in unified_deduplicate table")

        if matched_count_dedup > 0:
            matched_dedup_keys = [row.lakefusion_id for row in df_matched_dedup_keys.collect()]
            dedup_keys_str = "','".join(matched_dedup_keys)
            spark.sql(f"""
                UPDATE {unified_deduplicate_table}
                SET search_results = '',
                    scoring_results = ''
                WHERE lakefusion_id IN ('{dedup_keys_str}')
            """)
            logger.info(f" Cleared search_results for {matched_count_dedup} rows")
            total_cleared_across_tables += matched_count_dedup
        else:
            logger.info(f" No records needed clearing in unified_deduplicate")
    else:
        logger.info(f" Table {unified_deduplicate_table} does not exist, skipping...")

    logger.info(f"\n TOTAL RECORDS CLEARED ACROSS ALL TABLES: {total_cleared_across_tables}")
    logger.info("=" * 80)
else:
    logger.info("\n  STEP 6: No deleted lakefusion_ids to clear from search_results")

# COMMAND ----------

# STEP 7: CLEAR MASTER_LAKEFUSION_ID FOR DELETED RECORDS
logger.info("\n STEP 7: Clearing master_lakefusion_id for deleted records...")

# Only process records that had master_lakefusion_id (MERGED records)
merged_records_to_clear = list(deleted_by_surrogate.keys())

if merged_records_to_clear:
    try:
        # Single UPDATE using temp view (reuse _phase15_sks_to_delete which has all deleted SKs)
        # Filter to only merged records by using the merged_records_to_clear list
        merged_sks_df = spark.createDataFrame([(sk,) for sk in merged_records_to_clear], ["surrogate_key"])
        merged_sks_df.createOrReplaceTempView("_step7_merged_sks_to_clear")

        spark.sql(f"""
            UPDATE {unified_table}
            SET master_lakefusion_id = ''
            WHERE surrogate_key IN (SELECT surrogate_key FROM _step7_merged_sks_to_clear)
            AND record_status = '{RecordStatus.DELETED.value}'
        """)

        logger.info(f" Cleared master_lakefusion_id for {len(merged_records_to_clear)} deleted records")

    except Exception as e:
        logger.error(f" Error clearing master_lakefusion_id: {e}")
else:
    logger.info("  No MERGED records to clear master_lakefusion_id (only ACTIVE records were deleted)")

# COMMAND ----------

# COMPLETION
logger.info("\n" + "=" * 80)
logger.info(" Incremental Delete Processing Completed")
logger.info("=" * 80)
logger.info(f" Summary:")
logger.info(f"   Phase 1 - Unified Status Updates:")
logger.info(f"     - Sources processed: {len(tables_with_deletes)}")
logger.info(f"     - Records marked as DELETE: {len(records_to_mark_delete)}")
logger.info(f"   Phase 2 - Master Deletes (Batch Execution):")
logger.info(f"     - STANDALONE categorized: {len(standalone_ids)}")
logger.info(f"     - MULTI_CONTRIBUTOR categorized: {len(multi_contributor_ids)}")
logger.info(f"     - MULTI_CONTRIBUTOR with survivorship: {len(records_processed_with_survivorship)}")
logger.info(f"     - Master records deleted: {len(set(master_ids_to_delete))}")
logger.info(f"     - Master records updated (survivorship): {len(all_master_updates)}")
logger.info(f"     - Attribute versions inserted (survivorship): {len(all_attribute_versions)}")
logger.info(f"     - Merge activities logged: {len(all_merge_activities)}")
logger.info("=" * 80)

# Set task values
dbutils.jobs.taskValues.set(key="status", value="SUCCESS")
dbutils.jobs.taskValues.set(key="records_marked_delete_unified", value=len(records_to_mark_delete))
dbutils.jobs.taskValues.set(key="records_deleted_master", value=len(set(master_ids_to_delete)))
dbutils.jobs.taskValues.set(key="records_updated_master_survivorship", value=len(all_master_updates))
dbutils.jobs.taskValues.set(key="merge_activities_logged", value=len(all_merge_activities))

dbutils.notebook.exit("SUCCESS")
