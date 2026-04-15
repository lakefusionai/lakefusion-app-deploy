# Databricks notebook source
"""
COMPREHENSIVE INCREMENTAL UPDATE PROCESSING - NEW ARCHITECTURE WITH SURVIVORSHIP

PURPOSE: 
1. Process UPDATE incrementals from ALL source tables (primary + secondary)
2. Update unified table with new values
3. Apply OLD architecture logic for master table updates based on scenarios
4. Handle survivorship recalculation when needed
5. Track changes in merge_activities and attribute_version_sources tables

FLOW:
1. Loop through source tables with updates
2. Read CDF updates from each source (PRE and POST)
3. COMPARE PRE vs POST to identify changed attributes
4. Transform and collect updates (NO DB WRITES YET)
5. Identify changed lakefusion_ids in master via master_lakefusion_id in unified table
6. Categorize by scenarios (STANDALONE, MULTI_CONTRIBUTOR) using contributor count
7. BATCH EXECUTION: Apply all updates atomically including merge_activities and attribute_version_sources
8. Flag survivorship recalculation where needed
"""

import json
import hashlib
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, ArrayType, TimestampType
from pyspark.sql.window import Window
from datetime import datetime

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

setup_lakefusion_engine()

# COMMAND ----------

from lakefusion_core_engine.survivorship import SurvivorshipEngine

# COMMAND ----------

from lakefusion_core_engine.identifiers import generate_surrogate_key
from lakefusion_core_engine.models import RecordStatus, ActionType

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
has_updates = dbutils.jobs.taskValues.get("Check_Increments_Exists", "has_updates", debugValue=True)
tables_with_updates = dbutils.jobs.taskValues.get("Check_Increments_Exists", "tables_with_updates", debugValue="[]")
table_version_info = dbutils.jobs.taskValues.get("Check_Increments_Exists", "table_version_info", debugValue="{}")

# COMMAND ----------

logger.info(has_updates)
logger.info(tables_with_updates)
logger.info(table_version_info) 

# COMMAND ----------

# PARSE JSON
dataset_tables = json.loads(dataset_tables)
dataset_objects = json.loads(dataset_objects)
entity_attributes = json.loads(entity_attributes)
entity_attributes_datatype = json.loads(entity_attributes_datatype)
attributes_mapping_json = json.loads(attributes_mapping_json)
attributes = json.loads(attributes)
survivorship_config = json.loads(survivorship_config) if survivorship_config else []
tables_with_updates = json.loads(tables_with_updates)
table_version_info = json.loads(table_version_info)

# Convert has_updates to boolean
if isinstance(has_updates, str):
    has_updates = has_updates.lower() == 'true'

# Remove experiment_id hyphens
if experiment_id:
    experiment_id = experiment_id.replace("-", "")

# COMMAND ----------

# TRANSFORM SURVIVORSHIP RULES: Convert dataset IDs to source paths
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
processed_unified_table = f"{catalog_name}.silver.{entity}_processed_unified"

if experiment_id:
    processed_unified_table += f"_{experiment_id}"

# COMMAND ----------

# UTILITY FUNCTIONS
# Create UDF for surrogate key generation
generate_surrogate_key_udf = udf(
    lambda source_path, source_id: generate_surrogate_key(source_path, str(source_id)), 
    StringType()
)

def get_mapping_for_table(source_table: str):
    """
    Get attribute mapping for a specific source table.
    Returns mapping in source_column -> entity_column format.
    """
    for mapping in attributes_mapping_json:
        if source_table in mapping:
            # CRITICAL FIX: The mapping from parse_attributes_mapping_json() is entity->source
            # but we need source->entity for the UPDATE logic to work
            # Example input:  {"client_id": "customer_id", "email": "email_address"}
            # Example output: {"customer_id": "client_id", "email_address": "email"}
            reversed_mapping = {v: k for k, v in mapping[source_table].items()}
            return reversed_mapping
    return {}

def generate_attributes_combined(record_dict, attributes_list):
    """Generate attributes_combined string from attribute values"""
    values = []
    for attr in attributes_list:
        value = record_dict.get(attr, '')
        if value is None:
            value = ''
        values.append(str(value))
    return ' | '.join(values)

# ==============================================================================
# CDF DEDUPLICATION FUNCTION FOR UPDATES
# ==============================================================================

def read_and_deduplicate_cdf_for_updates(source_table, start_version, end_version, source_pk_column, mapping_dict):
    """
    Read ALL CDF change types and deduplicate per surrogate_key.
    Keep only records where the LATEST operation is UPDATE.
    Returns both preimage and postimage DataFrames for the deduplicated updates.
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
    
    # Classify operations (use postimage for updates, ignore preimage in classification)
    df_cdf = df_cdf.withColumn(
        "operation_type",
        F.when(F.col("_change_type") == "insert", F.lit("insert"))
         .when(F.col("_change_type") == "update_postimage", F.lit("update"))
         .when(F.col("_change_type") == "delete", F.lit("delete"))
         .when(F.col("_change_type") == "update_preimage", F.lit("update_pre"))
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
    
    # DEDUPLICATION: For each surrogate_key, keep LATEST operation
    # Only consider postimages and other operations for determining latest
    df_for_dedup = df_cdf.filter(F.col("operation_type") != "update_pre")
    
    window_spec = Window.partitionBy("surrogate_key") \
                        .orderBy(F.desc("_commit_version"), F.desc("_commit_timestamp"))
    
    df_latest = df_for_dedup.withColumn(
        "_row_num", F.row_number().over(window_spec)
    ).filter(F.col("_row_num") == 1)
    
    logger.info("  Latest operations per surrogate_key:")
    df_latest.groupBy("operation_type").count().show(truncate=False)
    
    # Keep ONLY records where latest operation is UPDATE
    df_updates_only = df_latest.filter(F.col("operation_type") == "update")
    
    # Cheap emptiness check instead of count + collect
    if not df_updates_only.take(1):
        logger.info(f"  After deduplication: 0 records with UPDATE as latest operation")
        return spark.createDataFrame([], df_cdf.schema), spark.createDataFrame([], df_cdf.schema)

    logger.info("  After deduplication: UPDATE records present")

    # Keep surrogate keys as a Spark DataFrame (NOT a Python list) and avoiding duplicate surrogate keys
    update_keys_df = df_updates_only.select("surrogate_key").distinct()

    # Now get BOTH preimage and postimage using joins (not isin(list))

    df_preimage = (
        df_cdf
        .filter(F.col("_change_type") == "update_preimage")
        .join(update_keys_df, on="surrogate_key", how="inner")
    )

    df_postimage = (
        df_cdf
        .filter(F.col("_change_type") == "update_postimage")
        .join(update_keys_df, on="surrogate_key", how="inner")
    )
    
    # For each surrogate_key, keep only the LATEST preimage and postimage by version
    window_latest = Window.partitionBy("surrogate_key", "_change_type") \
                          .orderBy(F.desc("_commit_version"), F.desc("_commit_timestamp"))
    
    df_preimage = df_preimage.withColumn("_rn", F.row_number().over(window_latest)) \
                             .filter(F.col("_rn") == 1).drop("_rn")
    
    df_postimage = df_postimage.withColumn("_rn", F.row_number().over(window_latest)) \
                               .filter(F.col("_rn") == 1).drop("_rn")
    
    return df_preimage, df_postimage

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
        logger.error("   Survivorship recalculation will be skipped")
        survivorship_engine = None
else:
    logger.info(" No survivorship_config provided - survivorship recalculation will be skipped")

# COMMAND ----------

# EARLY EXIT CHECK
if not has_updates:
    logger.info("=" * 80)
    logger.info("  SKIPPING: No UPDATE incrementals found")
    logger.info("=" * 80)
    dbutils.notebook.exit("SKIPPED_NO_UPDATES")

if len(tables_with_updates) == 0:
    logger.info("=" * 80)
    logger.info("  SKIPPING: tables_with_updates is empty")
    logger.info("=" * 80)
    dbutils.notebook.exit("SKIPPED_NO_UPDATES")

# COMMAND ----------

# ==============================================================================
# PHASE 1: PROCESS SOURCE TABLES AND COLLECT UPDATES (NO DB WRITES)
# ==============================================================================

logger.info("=" * 80)
logger.info(f" PHASE 1: Process Source Table Updates (Collection Phase)")
logger.info(f" Tables to process: {len(tables_with_updates)}")
logger.info("=" * 80)

# Track all surrogate keys that were updated and changed attributes
all_updated_surrogate_keys = []
all_transformed_updates = []
changed_attributes_by_surrogate = {}  # Maps surrogate_key -> list of changed attribute names

for source_table in tables_with_updates:
    logger.info(f"\n{'='*60}")
    logger.info(f" Processing: {source_table}")
    logger.info(f"{'='*60}")
    
    version_info = table_version_info.get(source_table)
    
    if not version_info or not version_info.get("has_updates", False):
        logger.info(f" No updates for {source_table}, skipping...")
        continue
    
    # FIXED: Use last_processed_version (consistent with INSERT notebook)
    last_processed_version = version_info["last_processed_version"]
    current_version = version_info["current_version"]
    starting_version = last_processed_version + 1
    expected_update_count = version_info.get("update_count", 0)
    
    logger.info(f" Processing versions {starting_version} to {current_version} (last processed: {last_processed_version}), expecting {expected_update_count} updates")
    
    # REPLACE LINES 296-324 WITH THIS:

    # Get starting_version from table_version_info
    starting_version = version_info.get("starting_version")
    
    if starting_version is None:
        # Fallback: calculate from last_processed_version
        starting_version = last_processed_version + 1
    
    logger.info(f" Processing versions {starting_version} to {current_version}, expecting {expected_update_count} updates")
    
    # Get attribute mapping FIRST (needed for deduplication)
    mapping_dict = get_mapping_for_table(source_table)
    
    if not mapping_dict:
        logger.warning(f" No mapping found, skipping...")
        continue
    
    # Find source primary key
    source_pk_column = None
    for src_col, entity_col in mapping_dict.items():
        if entity_col == primary_key:
            source_pk_column = src_col
            break
    
    if not source_pk_column:
        logger.warning(f" Primary key not found in mapping or data")
        continue
    
    # READ CDF WITH DEDUPLICATION
    try:
        df_preimage, df_postimage = read_and_deduplicate_cdf_for_updates(
            source_table,
            starting_version,
            current_version,
            source_pk_column,
            mapping_dict
        )
        
        if not df_postimage.take(1):
            logger.info(f" No updates found after deduplication")
            continue
        
        # Clean CDF metadata
        df_preimage_clean = df_preimage.drop("_change_type", "_commit_version", "_commit_timestamp", "operation_type", "_source_pk_value")
        df_postimage_clean = df_postimage.drop("_change_type", "_commit_version", "_commit_timestamp", "operation_type", "_source_pk_value")
        
    except Exception as e:
        logger.error(f" Error reading CDF: {e}")
        import traceback
        logger.error(traceback.format_exc())
        continue
    
    # TRANSFORM TO UNIFIED SCHEMA
    mapping_dict = get_mapping_for_table(source_table)
    
    if not mapping_dict:
        logger.warning(f" No mapping found, skipping...")
        continue
    
    # Find source primary key
    # mapping_dict is now: {source_col: entity_col} after reversal
    # e.g., {"customer_id": "client_id", "email_address": "email"}
    source_pk_column = None
    for src_col, entity_col in mapping_dict.items():
        if entity_col == primary_key:
            source_pk_column = src_col
            break
    
    if not source_pk_column or source_pk_column not in df_postimage_clean.columns:
        logger.warning(f" Primary key not found in mapping or data")
        continue
    
    logger.info(f" Transforming BOTH preimage and postimage for comparison...")
    
    # Store source PK before transformation for BOTH
    df_postimage_clean = df_postimage_clean.withColumn("_source_pk_value", col(source_pk_column).cast("string"))
    df_preimage_clean = df_preimage_clean.withColumn("_source_pk_value", col(source_pk_column).cast("string"))
    
    # Transform POSTIMAGE to entity schema
    # mapping_dict is now: {source_col: entity_col} after reversal
    try:
        df_post_transformed = df_postimage_clean.selectExpr(
            *[f"`{source_col}` as `{entity_col}`" for source_col, entity_col in mapping_dict.items()],
            "_source_pk_value"
        )
    except Exception as e:
        logger.error(f" Error applying mappings to postimage: {e}")
        continue
    
    # Transform PREIMAGE to entity schema
    try:
        df_pre_transformed = df_preimage_clean.selectExpr(
            *[f"`{source_col}` as `{entity_col}`" for source_col, entity_col in mapping_dict.items()],
            "_source_pk_value"
        )
    except Exception as e:
        logger.error(f" Error applying mappings to preimage: {e}")
        continue
    
    # Add missing entity attributes to BOTH
    existing_columns = df_post_transformed.columns
    missing_columns = [col_name for col_name in entity_attributes if col_name not in existing_columns] 
    
    for col_name in missing_columns:
        sql_dtype = entity_attributes_datatype.get(col_name, "string")
        spark_dtype = sql_dtype.lower().strip()
        df_post_transformed = df_post_transformed.withColumn(col_name, lit(None).cast(spark_dtype))
        df_pre_transformed = df_pre_transformed.withColumn(col_name, lit(None).cast(spark_dtype))
    
    # Select in entity_attributes order for BOTH
    final_columns = entity_attributes
    df_post_transformed = df_post_transformed.select(*final_columns, "_source_pk_value")
    df_pre_transformed = df_pre_transformed.select(*final_columns, "_source_pk_value")
    
    # Generate surrogate_key for BOTH
    df_post_transformed = df_post_transformed.withColumn(
        "surrogate_key",
        generate_surrogate_key_udf(lit(source_table), col("_source_pk_value"))
    )
    df_pre_transformed = df_pre_transformed.withColumn(
        "surrogate_key",
        generate_surrogate_key_udf(lit(source_table), col("_source_pk_value"))
    )
    
    logger.info(f" Transformed both preimage and postimage to unified schema")
    
    # ============================================================================
    # COMPARE PRE vs POST TO IDENTIFY CHANGED ATTRIBUTES
    # ============================================================================
    logger.info(f" Comparing pre/post to identify changed attributes...")

    # Join pre and post on surrogate_key with proper aliases
    df_pre_aliased = df_pre_transformed.select(
        col("surrogate_key"),
        *[col(c).alias(f"pre_{c}") for c in entity_attributes]
    )

    df_post_aliased = df_post_transformed.select(
        col("surrogate_key"),
        *[col(c).alias(f"post_{c}") for c in entity_attributes]
    )

    df_comparison = df_pre_aliased.join(df_post_aliased, on="surrogate_key", how="inner")

    # For each record, identify which attributes changed
    comparison_records = df_comparison.collect()

    for row in comparison_records:
        surrogate_key = row.surrogate_key
        changed_attrs = []
    
        # Compare each entity attribute
        for attr in entity_attributes:
            try:
                pre_value = row[f"pre_{attr}"]
                post_value = row[f"post_{attr}"]
            
                if str(pre_value) != str(post_value):
                    changed_attrs.append(attr)
            except KeyError:
                continue
    
        if changed_attrs:
            changed_attributes_by_surrogate[surrogate_key] = changed_attrs

    logger.info(f" Identified changed attributes for {len(changed_attributes_by_surrogate)} records from {source_table}")
    
    # Continue with POSTIMAGE for unified table update
    df_transformed = df_post_transformed
    
    # Add source metadata
    df_transformed = df_transformed.withColumn("source_path", lit(source_table))
    df_transformed = df_transformed.withColumn("source_id", col("_source_pk_value"))
    df_transformed = df_transformed.drop("_source_pk_value")
    
    # DEBUG: Log what we have before generating attributes_combined
    logger.info(f" DEBUG: match_attributes = {attributes}")
    logger.info(f" DEBUG: df_transformed.columns = {df_transformed.columns}")
    logger.info(f" DEBUG: Sample row:")
    sample = df_transformed.limit(1).collect()
    if sample:
        for attr in attributes:
            try:
                value = getattr(sample[0], attr, "NOT_FOUND")
                logger.info(f"   {attr} = {value}")
            except:
                logger.info(f"   {attr} = ERROR_ACCESSING")
    
    # Generate attributes_combined
    # CRITICAL: Ensure all match attributes are included
    # Create a case-insensitive mapping of columns
    df_columns_lower = {c.lower().strip('`'): c for c in df_transformed.columns}
    
    available_attributes = []
    for attr in attributes:
        attr_lower = attr.lower()
        if attr in df_transformed.columns:
            # Exact match
            available_attributes.append(attr)
        elif attr_lower in df_columns_lower:
            # Case-insensitive match
            actual_col_name = df_columns_lower[attr_lower]
            logger.info(f" WARNING: Using '{actual_col_name}' for match attribute '{attr}' (case mismatch)")
            available_attributes.append(actual_col_name)
        else:
            logger.info(f" ERROR: Match attribute '{attr}' not found in DataFrame!")
            logger.error(f"   Available columns: {sorted(df_transformed.columns)}")
    
    if available_attributes:
        df_transformed = df_transformed.withColumn(
            "attributes_combined",
            concat_ws(" | ", *[regexp_replace(trim(col(c)), r'\s+', ' ') for c in available_attributes])
        )
        logger.info(f" Generated attributes_combined with {len(available_attributes)} attributes: {available_attributes}")
    else:
        logger.info(f" ERROR: No match attributes available for attributes_combined generation!")
        df_transformed = df_transformed.withColumn("attributes_combined", lit(""))
    
    # Add search/scoring columns
    df_transformed = df_transformed.withColumn("search_results", lit(""))
    df_transformed = df_transformed.withColumn("scoring_results", lit(""))
    
    # Add master_lakefusion_id (prevent NULL/"null" issue for upsert scenarios)
    df_transformed = df_transformed.withColumn("master_lakefusion_id", lit(""))
    
    # Add status column - initially set to ACTIVE, will be preserved during merge
    df_transformed = df_transformed.withColumn("record_status", lit(RecordStatus.ACTIVE.value))
    
    logger.info(f" Prepared data for unified update")
    
    # Collect transformed data and surrogate keys
    all_transformed_updates.append(df_transformed)
    
    # Track surrogate keys for this batch
    surrogate_keys = [row.surrogate_key for row in df_transformed.select("surrogate_key").collect()]
    all_updated_surrogate_keys.extend(surrogate_keys)
    
    logger.info(f" Completed {source_table}")

logger.info(f"\n Total changed attributes tracked: {len(changed_attributes_by_surrogate)} records")

# COMMAND ----------

# UNION ALL UPDATES
if len(all_transformed_updates) == 0:
    logger.info(" No update data to process")
    dbutils.notebook.exit("NO_UPDATES_PROCESSED")

logger.info("\n" + "=" * 80)
logger.info(f" Combining updates from {len(all_transformed_updates)} source(s)")

df_all_updates = all_transformed_updates[0]
for df in all_transformed_updates[1:]:
    df_all_updates = df_all_updates.unionByName(df, allowMissingColumns=True)

total_update_count = df_all_updates.count()
logger.info(f" Combined total: {total_update_count} UPDATE records")

# COMMAND ----------

# DEDUPLICATE BY SURROGATE KEY
window_spec = Window.partitionBy("surrogate_key").orderBy(lit(1))
df_all_updates_deduped = df_all_updates \
    .withColumn("_rn", row_number().over(window_spec)) \
    .filter(col("_rn") == 1) \
    .drop("_rn")

deduped_count = df_all_updates_deduped.count()
if deduped_count < total_update_count:
    logger.info(f" Removed {total_update_count - deduped_count} duplicate updates")

logger.info(f" Phase 1 complete - all updates collected in memory")

# COMMAND ----------

# ==============================================================================
# PHASE 1.5: UPDATE UNIFIED TABLE (BEFORE SURVIVORSHIP)
# ==============================================================================

logger.info("\n" + "=" * 80)
logger.info(" PHASE 1.5: Update Unified Table with New Values")
logger.info("=" * 80)
logger.info("  CRITICAL: Unified must be updated BEFORE survivorship recalculation")
logger.info("   Survivorship will fetch contributors from unified and expects NEW values")

# Initialize matched_count for batch execution summary (will be set in STEP 3)
matched_count = 0

# Create unique list of all updated surrogate keys
all_unique_updated_surrogate_keys = list(set(all_updated_surrogate_keys))
logger.info(f"\n Total surrogate keys to update: {len(all_unique_updated_surrogate_keys)}")

# COMMAND ----------

# STEP 1: CLEAR SEARCH/SCORING RESULTS IN UNIFIED
logger.info("\n STEP 1: Clearing search/scoring results...")

if spark.catalog.tableExists(unified_table) and all_unique_updated_surrogate_keys:
    surrogate_keys_str = "','".join(all_unique_updated_surrogate_keys)
    spark.sql(f"""
        UPDATE {unified_table}
        SET search_results = '',
            scoring_results = ''
        WHERE surrogate_key IN ('{surrogate_keys_str}')
    """)
    logger.info(f" Cleared for {len(all_unique_updated_surrogate_keys)} records")
else:
    logger.info("  No records to clear")

# COMMAND ----------

# DBTITLE 1,Not Needed for v4.0.0
# # STEP 2: DELETE FROM PROCESSED_UNIFIED
# print("\n🗑️  STEP 2: Removing from processed_unified for re-matching...")

# if spark.catalog.tableExists(processed_unified_table) and all_unique_updated_surrogate_keys:
#     surrogate_keys_str = "','".join(all_unique_updated_surrogate_keys)
#     spark.sql(f"""
#         DELETE FROM {processed_unified_table}
#         WHERE surrogate_key IN ('{surrogate_keys_str}')
#     """)
#     print(f"✅ Removed {len(all_unique_updated_surrogate_keys)} records")
# else:
#     print("⏭️  No records to remove")

# COMMAND ----------

# REPLACE LINES 598-640 WITH THIS UPSERT LOGIC:

# STEP 3: UPSERT INTO UNIFIED TABLE (Update if exists, Insert if doesn't exist)
logger.info("\n📝 STEP 3: UPSERT into unified table...")

# Check which records exist in unified
df_unified_existing = spark.read.table(unified_table).select(
    "surrogate_key",
    "record_status",
    "master_lakefusion_id"
)

# LEFT JOIN to identify existing vs new records
df_with_check = df_all_updates_deduped.alias("upd").join(
    df_unified_existing.alias("ext"),
    on="surrogate_key",
    how="left"
)

# Records that EXIST → UPDATE (preserve their record_status AND master_lakefusion_id)
df_existing_updates = df_with_check.filter(col("ext.record_status").isNotNull()).select(
    *[col(f"upd.{c}") for c in df_all_updates_deduped.columns if c not in ["record_status", "master_lakefusion_id"]],
    col("ext.record_status").alias("record_status"),
    col("ext.master_lakefusion_id").alias("master_lakefusion_id")
)

# Records that DON'T EXIST → INSERT as ACTIVE
# These are records where latest CDF operation was UPDATE but record not in unified yet
# (Due to deduplication: INSERT→UPDATE→UPDATE, latest=UPDATE but never inserted)
df_new_records = df_with_check.filter(col("ext.record_status").isNull()).select(
    *[col(f"upd.{c}") for c in df_all_updates_deduped.columns]
).withColumn("record_status", lit(RecordStatus.ACTIVE.value))

if not df_existing_updates.take(1) and not df_new_records.take(1):
    logger.info("No records to process!")
    dbutils.notebook.exit("No records")

existing_count = df_existing_updates.count()
new_count = df_new_records.count()

logger.info(f"  Existing records to UPDATE: {existing_count}")
logger.info(f"  New records to INSERT as ACTIVE: {new_count}")

# UPDATE existing records
if existing_count > 0:
    delta_unified = DeltaTable.forName(spark, unified_table)
    
    update_expressions = {}
    for col_name in df_existing_updates.columns:
        if col_name != "surrogate_key":
            update_expressions[col_name] = f"source.{col_name}"
    
    delta_unified.alias("target").merge(
        df_existing_updates.alias("source"),
        "target.surrogate_key = source.surrogate_key"
    ).whenMatchedUpdate(
        set=update_expressions
    ).execute()
    
    logger.info(f"✅ Updated {existing_count} existing records")

# INSERT new records as ACTIVE using MERGE (consistent with INSERT notebook)
if new_count > 0:
    delta_unified = DeltaTable.forName(spark, unified_table)
    
    delta_unified.alias("target").merge(
        df_new_records.alias("source"),
        "target.surrogate_key = source.surrogate_key"
    ).whenNotMatchedInsertAll().execute()
    
    logger.info(f"✅ Inserted {new_count} new records as ACTIVE (upsert via MERGE)")
    logger.info(f"⚠️  These records came from UPDATE CDF but didn't exist in unified")

logger.info(f"✅ Unified table now contains NEW values for survivorship")
logger.info(f"   Total records processed: {existing_count + new_count}")

# Set matched_count for batch execution summary
matched_count = existing_count + new_count

# COMMAND ----------

# ==============================================================================
# PHASE 2: IDENTIFY AFFECTED MASTER RECORDS VIA MERGE_ACTIVITIES
# ==============================================================================


logger.info("\n" + "=" * 80)
logger.info(f" PHASE 2: Identify Affected Master Records")
logger.info("=" * 80)

# Get merged surrogate keys
df_unified_existing = spark.read.table(unified_table).select("surrogate_key", "record_status")

df_updates_with_status = df_all_updates_deduped.alias("upd").join(
    df_unified_existing.alias("ext"),
    on="surrogate_key",
    how="inner"
).select(
    col("upd.surrogate_key"),
    col("ext.record_status")
)

merged_surrogate_keys = [row.surrogate_key for row in df_updates_with_status.filter(
    col("record_status") == RecordStatus.MERGED.value
).collect()]

merged_update_count = len(merged_surrogate_keys)
logger.info(f" Records with MERGED status in unified: {merged_update_count} surrogate keys")

if merged_update_count == 0:
    logger.info(" No MERGED records to process in master - only ACTIVE records were updated")
    updated_lakefusion_ids = []
    surrogate_to_lakefusion = {}
    changed_attributes_by_lakefusion = {}
else:
    # Lookup lakefusion_ids directly from unified table using master_lakefusion_id
    surrogate_keys_str = "','".join(merged_surrogate_keys)
    
    logger.info(" Looking up master_lakefusion_id for MERGED surrogate keys from unified table...")
    
    df_lakefusion_mapping = spark.sql(f"""
        SELECT surrogate_key, master_lakefusion_id as lakefusion_id
        FROM {unified_table}
        WHERE surrogate_key IN ('{surrogate_keys_str}')
        AND master_lakefusion_id IS NOT NULL 
        AND master_lakefusion_id != ''
    """)
    
    surrogate_to_lakefusion = {row.surrogate_key: row.lakefusion_id for row in df_lakefusion_mapping.collect()}
    updated_lakefusion_ids = list(set(surrogate_to_lakefusion.values()))
    
    logger.info(f" Found {len(surrogate_to_lakefusion)} surrogate_key to master_lakefusion_id mappings")
    logger.info(f" Unique MERGED lakefusion_ids to process: {len(updated_lakefusion_ids)}")
    
    if updated_lakefusion_ids:
        # Map changed attributes to lakefusion_ids
        logger.info("\n" + "=" * 60)
        logger.info(" Mapping changed attributes to lakefusion_ids...")
        
        changed_attributes_by_lakefusion = {}
        updated_lakefusion_ids_set = set(updated_lakefusion_ids)
        
        for surrogate_key, changed_attrs in changed_attributes_by_surrogate.items():
            lakefusion_id = surrogate_to_lakefusion.get(surrogate_key)
            
            if lakefusion_id and lakefusion_id in updated_lakefusion_ids_set:
                if lakefusion_id not in changed_attributes_by_lakefusion:
                    changed_attributes_by_lakefusion[lakefusion_id] = set()
                changed_attributes_by_lakefusion[lakefusion_id].update(changed_attrs)
        
        changed_attributes_by_lakefusion = {k: list(v) for k, v in changed_attributes_by_lakefusion.items()}
        logger.info(f" Mapped changed attributes for {len(changed_attributes_by_lakefusion)} lakefusion_ids")
    else:
        changed_attributes_by_lakefusion = {}



# COMMAND ----------

logger.info("\n" + "=" * 60)
logger.info(" Categorizing by scenario using contributor count from unified table...")

# Optimization:
# Replaced SQL "IN (...)" query + driver collect pattern with a distributed JOIN-based approach.

standalone_ids = []
multi_contributor_ids = []

if updated_lakefusion_ids:

    # Convert IDs to Spark DF (safe, broadcastable)
    ids_df = spark.createDataFrame(
        [(x,) for x in updated_lakefusion_ids],
        ["lakefusion_id"]
    ).dropDuplicates()

    df_categorized = (
        spark.read.table(unified_table)
        .filter(col("record_status") == RecordStatus.MERGED.value)
        .join(ids_df, col("master_lakefusion_id") == col("lakefusion_id"), "inner")
        .groupBy(col("master_lakefusion_id").alias("lakefusion_id"))
        .agg(count("*").alias("contributor_count"))
        .withColumn(
            "scenario",
            when(col("contributor_count") == 1, lit("STANDALONE"))
             .when(col("contributor_count") > 1, lit("MULTI_CONTRIBUTOR"))
             .otherwise(lit("UNKNOWN"))
        )
    )

    logger.info("Categorization results:")
    df_categorized.groupBy("scenario").count().show()

    standalone_ids = [row.lakefusion_id for row in df_categorized.filter(col("scenario") == "STANDALONE").collect()]
    multi_contributor_ids = [row.lakefusion_id for row in df_categorized.filter(col("scenario") == "MULTI_CONTRIBUTOR").collect()]

    logger.info(f" STANDALONE: {len(standalone_ids)}")
    logger.info(f" MULTI_CONTRIBUTOR: {len(multi_contributor_ids)}")

else:
    logger.info(" No lakefusion_ids to categorize")
    df_categorized = None

# COMMAND ----------

# ==============================================================================
# INITIALIZE BATCH COLLECTION LISTS
# ==============================================================================
logger.info("\n" + "=" * 80)
logger.info(" Initializing batch collection for master table updates...")
logger.info("=" * 80)

all_master_updates = []
all_merge_activities = []
all_attribute_versions = []

# COMMAND ----------

# SHARED PRE-COMPUTATION: Batch fetch data used by both STANDALONE and MULTI_CONTRIBUTOR
# This eliminates N individual spark.sql() calls by pre-fetching all data in 1-2 batch queries

logger.info("\n" + "=" * 60)
logger.info(" Pre-fetching shared data for batch processing...")

lakefusion_to_surrogate = {}
attr_sources_map = {}

if updated_lakefusion_ids:
    # Build reverse mapping: lakefusion_id -> list of surrogate_keys (O(1) lookup instead of O(N) list comprehension)
    for sk, lfid in surrogate_to_lakefusion.items():
        lakefusion_to_surrogate.setdefault(lfid, []).append(sk)

    # Batch fetch ALL attribute sources in 1 query (replaces N individual spark.sql() calls)
    affected_ids_str = "','".join(updated_lakefusion_ids)
    attr_sources_rows = spark.sql(f"""
        SELECT lakefusion_id, attr_map.attribute_name, attr_map.source as winning_source
        FROM {master_attribute_version_sources_table}
        LATERAL VIEW explode(attribute_source_mapping) AS attr_map
        WHERE lakefusion_id IN ('{affected_ids_str}')
    """).collect()

    for row in attr_sources_rows:
        attr_sources_map.setdefault(row.lakefusion_id, {})[row.attribute_name] = row.winning_source

    logger.info(f" Pre-fetched attribute sources for {len(attr_sources_map)} lakefusion_ids")
    logger.info(f" Pre-built reverse mapping for {len(lakefusion_to_surrogate)} lakefusion_ids")
else:
    logger.info(" No lakefusion_ids to pre-fetch")

# COMMAND ----------

# SCENARIO 1: STANDALONE UPDATES (BATCH OPTIMIZED)
# Instead of 2 spark.sql() calls per ID, batch-fetch all unified records and attr sources in 2 queries total
logger.info("\n" + "=" * 60)
logger.info(f" Processing STANDALONE updates ({len(standalone_ids)} records)...")

if standalone_ids:
    try:
        standalone_ids_set = set(standalone_ids)

        # Batch fetch ALL unified records for standalone surrogate keys (1 query instead of N)
        standalone_sks = [sk for lfid in standalone_ids for sk in lakefusion_to_surrogate.get(lfid, [])]

        if standalone_sks:
            standalone_sks_str = "','".join(standalone_sks)
            unified_rows = spark.sql(f"""
                SELECT * FROM {unified_table}
                WHERE surrogate_key IN ('{standalone_sks_str}')
            """).collect()

            # Index by lakefusion_id for O(1) lookups
            unified_records_map = {}
            for row in unified_rows:
                lfid = surrogate_to_lakefusion.get(row.surrogate_key)
                if lfid and lfid in standalone_ids_set:
                    unified_records_map[lfid] = row

            logger.info(f" Batch fetched {len(unified_records_map)} unified records")

            # Process each standalone ID using pre-fetched data (no Spark SQL calls in loop)
            for lakefusion_id in standalone_ids:
                try:
                    sks_for_id = lakefusion_to_surrogate.get(lakefusion_id, [])
                    if not sks_for_id:
                        logger.warning(f" Could not find surrogate key for standalone master {lakefusion_id}")
                        continue

                    unified_record = unified_records_map.get(lakefusion_id)
                    if not unified_record:
                        logger.warning(f" No unified record found for {lakefusion_id}")
                        continue

                    # Use pre-fetched attribute sources (no spark.sql() call)
                    current_sources = attr_sources_map.get(lakefusion_id, {})
                    changed_attrs = changed_attributes_by_lakefusion.get(lakefusion_id, [])
                    unified_source_path = unified_record.source_path

                    # Build update record with ALL attributes
                    update_record = {'lakefusion_id': lakefusion_id}
                    updated_attr_mapping = []
                    has_changes = False

                    for attr in entity_attributes:
                        if attr == 'lakefusion_id':
                            continue

                        attr_value = getattr(unified_record, attr, None)
                        attr_value_str = str(attr_value) if attr_value is not None else None

                        update_record[attr] = attr_value_str
                        updated_attr_mapping.append({
                            'attribute_name': attr,
                            'attribute_value': attr_value_str,
                            'source': unified_source_path
                        })

                        if attr in changed_attrs and current_sources.get(attr) == unified_source_path:
                            has_changes = True

                    # Add attributes_combined
                    attributes_combined_value = getattr(unified_record, 'attributes_combined', None)
                    if attributes_combined_value:
                        update_record['attributes_combined'] = str(attributes_combined_value)
                        updated_attr_mapping.append({
                            'attribute_name': 'attributes_combined',
                            'attribute_value': str(attributes_combined_value),
                            'source': unified_source_path
                        })

                    if has_changes:
                        all_master_updates.append(update_record)
                        all_attribute_versions.append({
                            'lakefusion_id': lakefusion_id,
                            'version': None,  # Will be set after master update
                            'attribute_source_mapping': updated_attr_mapping
                        })
                        all_merge_activities.append({
                            'master_id': lakefusion_id,
                            'match_id': sks_for_id[0],
                            'source': unified_source_path,
                            'version': None,  # Will be set after master update
                            'action_type': 'SOURCE_UPDATE'
                        })

                except Exception as e:
                    logger.error(f" Error processing standalone {lakefusion_id}: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    continue

        standalone_update_count = len([u for u in all_master_updates if u['lakefusion_id'] in standalone_ids_set])
        logger.info(f" Prepared {standalone_update_count} STANDALONE updates for batch execution")

    except Exception as e:
        logger.error(f" Error in batch STANDALONE processing: {e}")
        import traceback
        logger.error(traceback.format_exc())
else:
    logger.info("No STANDALONE records to process")

# COMMAND ----------

# SCENARIO 2: MULTI_CONTRIBUTOR UPDATES (SURVIVORSHIP RECALCULATION) - BATCH OPTIMIZED
# Instead of nested loops with individual spark.sql() calls per ID, batch-fetch data and process on driver
logger.info("\n" + "=" * 60)
logger.info(f" Processing MULTI_CONTRIBUTOR updates ({len(multi_contributor_ids)} records)...")

records_requiring_survivorship = []
records_processed_with_survivorship = []

if multi_contributor_ids and df_categorized:
    if not survivorship_engine:
        logger.info(" Survivorship Engine not initialized - skipping MULTI_CONTRIBUTOR processing")
    else:
        try:
            multi_contributor_ids_set = set(multi_contributor_ids)

            # Batch fetch source_paths for all updated surrogate keys of multi-contributor masters (1 query)
            multi_updated_sks = [sk for sk in
                                 [s for s, lfid in surrogate_to_lakefusion.items() if lfid in multi_contributor_ids_set]
                                 if sk in changed_attributes_by_surrogate]

            sk_source_map = {}
            if multi_updated_sks:
                multi_sks_str = "','".join(multi_updated_sks)
                sk_source_rows = spark.sql(f"""
                    SELECT surrogate_key, source_path
                    FROM {unified_table}
                    WHERE surrogate_key IN ('{multi_sks_str}')
                """).collect()
                sk_source_map = {row.surrogate_key: row.source_path for row in sk_source_rows}

            logger.info(f" Batch fetched source_paths for {len(sk_source_map)} updated surrogate keys")

            # Determine which masters need survivorship (pure Python — no Spark calls in loop)
            for lakefusion_id in multi_contributor_ids:
                changed_attrs = changed_attributes_by_lakefusion.get(lakefusion_id, [])
                if not changed_attrs:
                    continue

                current_sources = attr_sources_map.get(lakefusion_id, {})
                updated_surrogate_keys_for_master = [sk for sk in lakefusion_to_surrogate.get(lakefusion_id, [])
                                                     if sk in changed_attributes_by_surrogate]

                is_survivorship_required = False
                for attr in changed_attrs:
                    winning_source_path = current_sources.get(attr)
                    for updated_sk in updated_surrogate_keys_for_master:
                        if sk_source_map.get(updated_sk) == winning_source_path:
                            is_survivorship_required = True
                            break
                    if is_survivorship_required:
                        break

                if is_survivorship_required:
                    records_requiring_survivorship.append(lakefusion_id)

            logger.info(f" {len(records_requiring_survivorship)} masters require survivorship recalculation")

            # Batch fetch ALL contributors for ALL masters needing survivorship (1 query instead of M)
            if records_requiring_survivorship:
                from collections import defaultdict

                masters_str = "','".join(records_requiring_survivorship)
                all_contributor_rows = spark.sql(f"""
                    SELECT *
                    FROM {unified_table}
                    WHERE master_lakefusion_id IN ('{masters_str}')
                    AND record_status = '{RecordStatus.MERGED.value}'
                    ORDER BY master_lakefusion_id, surrogate_key
                """).collect()

                # Group contributors by master on the driver
                contributors_by_master = defaultdict(list)
                for row in all_contributor_rows:
                    contributors_by_master[row.master_lakefusion_id].append(row)

                logger.info(f" Batch fetched {len(all_contributor_rows)} contributor records for {len(contributors_by_master)} masters")

                # Apply survivorship per master (sequential on driver — fast for typical counts)
                for lakefusion_id in records_requiring_survivorship:
                    try:
                        contributor_rows = contributors_by_master.get(lakefusion_id, [])
                        if not contributor_rows:
                            logger.info(f"    No contributors found for {lakefusion_id}")
                            continue

                        logger.info(f"   Applying survivorship for {lakefusion_id} ({len(contributor_rows)} contributors)...")

                        # Convert to list of dicts for survivorship engine
                        unified_records = []
                        for row in contributor_rows:
                            record_dict = row.asDict()
                            if 'surrogate_key' not in record_dict or 'source_path' not in record_dict:
                                logger.info(f"    Missing required fields in contributor record")
                                continue
                            unified_records.append(record_dict)

                        if not unified_records:
                            logger.info(f"    No valid contributor records for survivorship")
                            continue

                        # Apply survivorship
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
                            'version': None,  # Will be set after master update
                            'attribute_source_mapping': updated_attr_mapping
                        })

                        # Log merge activity for each updated surrogate_key
                        all_sources = []
                        for m in updated_attr_mapping:
                            source = m['source']
                            if ',' in source:
                                all_sources.extend([s.strip() for s in source.split(',')])
                            else:
                                all_sources.append(source)

                        unique_sources = ','.join(sorted(set(all_sources)))

                        updated_surrogate_keys_for_master = [sk for sk in lakefusion_to_surrogate.get(lakefusion_id, [])
                                                             if sk in changed_attributes_by_surrogate]

                        for updated_sk in updated_surrogate_keys_for_master:
                            all_merge_activities.append({
                                'master_id': lakefusion_id,
                                'match_id': updated_sk,
                                'source': unique_sources,
                                'version': None,  # Will be set after master update
                                'action_type': ActionType.SOURCE_UPDATE.value
                            })

                        records_processed_with_survivorship.append(lakefusion_id)
                        logger.info(f"    Survivorship applied for {lakefusion_id}")

                    except Exception as e:
                        logger.error(f" Error processing multi-contributor {lakefusion_id}: {e}")
                        import traceback
                        logger.error(traceback.format_exc())
                        continue

            logger.info(f" Processed {len(records_processed_with_survivorship)} MULTI_CONTRIBUTOR records with survivorship")
            logger.info(f"   Total requiring survivorship: {len(records_requiring_survivorship)}")
            logger.info(f"   Successfully processed: {len(records_processed_with_survivorship)}")

        except Exception as e:
            logger.error(f" Error in batch MULTI_CONTRIBUTOR processing: {e}")
            import traceback
            logger.error(traceback.format_exc())
else:
    logger.info("No MULTI_CONTRIBUTOR records to process")

# COMMAND ----------

# ==============================================================================
# BATCH EXECUTION: UPDATE MASTER TABLE AND LOG CHANGES
# ==============================================================================

logger.info("\n" + "=" * 80)
logger.info(" BATCH EXECUTION: Updating Master Table and Logging Changes")
logger.info("=" * 80)
logger.info("   Note: Unified table was already updated in Phase 1.5")

logger.info(f"\n Summary of collected operations:")
logger.info(f"   - Unified records updated (Phase 1.5): {matched_count}")
logger.info(f"   - Master table updates to apply: {len(all_master_updates)}")
logger.info(f"   - Merge activities to log: {len(all_merge_activities)}")
logger.info(f"   - Attribute versions to log: {len(all_attribute_versions)}")
logger.info(f"   - Records requiring survivorship: {len(records_requiring_survivorship)}")

# COMMAND ----------

# STEP 1: BATCH UPDATE MASTER TABLE
if all_master_updates:
    logger.info(f"\n STEP 1: Batch updating {len(all_master_updates)} master records...")
    
    # Create DataFrame for master updates
    master_update_fields = [StructField('lakefusion_id', StringType(), False)]
    for attr in entity_attributes:
        if attr != 'lakefusion_id':
            master_update_fields.append(StructField(attr, StringType(), True))
    master_update_fields.append(StructField('attributes_combined', StringType(), True))
    
    master_update_schema = StructType(master_update_fields)
    master_updates_df = spark.createDataFrame(all_master_updates, schema=master_update_schema)
    
    # Batch update master table using MERGE
    master_delta_table = DeltaTable.forName(spark, master_table)
    
    update_columns = [attr for attr in entity_attributes if attr != 'lakefusion_id']
    update_columns.append('attributes_combined')
    
    master_delta_table.alias("target").merge(
        source=master_updates_df.alias("source"),
        condition="target.lakefusion_id = source.lakefusion_id"
    ).whenMatchedUpdate(set={
        attr: f"source.{attr}" 
        for attr in update_columns
    }).execute()
    
    logger.info(f" Batch updated {len(all_master_updates)} master records")
    
    # Get new master version
    master_version = spark.sql(f"""
        SELECT MAX(version) as version 
        FROM (DESCRIBE HISTORY {master_table})
    """).first()['version']
    
    logger.info(f" Master table version: {master_version}")
    
    # Update version numbers in tracking arrays
    for activity in all_merge_activities:
        activity['version'] = master_version
        
    for attr_version in all_attribute_versions:
        attr_version['version'] = master_version
    
    logger.info(f" Updated version numbers in tracking arrays")
else:
    logger.info("  No master table updates to apply")

# COMMAND ----------

# STEP 2: CLEAR SEARCH RESULTS FOR RECORDS REFERENCING UPDATED MASTERS (EXPLODE + EQUI-JOIN)
# Strategy: Extract lakefusion_ids from search_results via split+explode, then equi-join on master IDs.
# This is O(tokens) with hash join vs O(rows × IDs) with LIKE string scanning.
# Find matching keys with a read-only query first, only run UPDATE if there are actual matches.
if all_master_updates:
    from pyspark.sql import functions as F
    logger.info("\n STEP 2: Clearing search_results for records referencing updated masters...")
    logger.info("=" * 60)

    updated_master_lakefusion_ids = [record['lakefusion_id'] for record in all_master_updates]
    logger.info(f" Number of updated master IDs: {len(updated_master_lakefusion_ids)}")

    # Create a small broadcast-friendly DataFrame of master IDs
    ids_df = spark.createDataFrame(
        [(lid,) for lid in updated_master_lakefusion_ids],
        ["master_id"]
    )
    ids_df.createOrReplaceTempView("_step2_updated_master_ids")

    # TABLE 1: UNIFIED — explode search_results tokens, equi-join on master IDs
    if spark.catalog.tableExists(unified_table):
        logger.info(f"\n Processing unified table: {unified_table}")

        df_matched_keys = spark.sql(f"""
            SELECT DISTINCT u.surrogate_key
            FROM {unified_table} u
            LATERAL VIEW explode(split(u.search_results, '[,\\\\s\\\\[\\\\]]+')) AS token
            WHERE u.record_status = 'ACTIVE'
            AND u.search_results IS NOT NULL
            AND u.search_results != ''
            AND token IN (SELECT master_id FROM _step2_updated_master_ids)
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
        else:
            logger.info(" No rows to clear, skipping UPDATE")

    # TABLE 2: UNIFIED_DEDUPLICATE — same explode + equi-join approach
    unified_deduplicate_table = f"{catalog_name}.silver.{entity}_unified_deduplicate"
    if experiment_id:
        unified_deduplicate_table += f"_{experiment_id}"

    if spark.catalog.tableExists(unified_deduplicate_table):
        logger.info(f"\n Processing unified_deduplicate table: {unified_deduplicate_table}")

        df_matched_dedup_keys = spark.sql(f"""
            SELECT DISTINCT u.lakefusion_id
            FROM {unified_deduplicate_table} u
            LATERAL VIEW explode(split(u.search_results, '[,\\\\s\\\\[\\\\]]+')) AS token
            WHERE u.search_results IS NOT NULL
            AND u.search_results != ''
            AND token IN (SELECT master_id FROM _step2_updated_master_ids)
            AND u.lakefusion_id NOT IN (SELECT master_id FROM _step2_updated_master_ids)
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
        else:
            logger.info(" No rows to clear, skipping UPDATE")

    logger.info(f"\n Batch search_results clearing complete for {len(updated_master_lakefusion_ids)} master IDs")
    logger.info("=" * 60)
else:
    logger.info("  No master updates, skipping search results clearing")

# COMMAND ----------

# STEP 3: BATCH INSERT MERGE ACTIVITIES
if all_merge_activities:
    logger.info(f"\n STEP 3: Batch inserting {len(all_merge_activities)} merge activity records...")

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
    merge_activities_df = spark.createDataFrame(
        all_merge_activities_with_ts,
        schema=merge_activities_schema
    )

    # Batch insert using Delta merge
    merge_activities_delta_table = DeltaTable.forName(
        spark,
        merge_activities_table
    )

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
    logger.info("  No merge activities to insert")

# COMMAND ----------

# STEP 4: BATCH INSERT ATTRIBUTE VERSION SOURCES
if all_attribute_versions:
    logger.info(f"\n STEP 4: Batch inserting {len(all_attribute_versions)} attribute version records...")
    
    # Define schema for attribute source mapping
    attr_mapping_struct = StructType([
        StructField("attribute_name", StringType(), True),
        StructField("attribute_value", StringType(), True),
        StructField("source", StringType(), True)
    ])
    
    # Define schema for attribute version sources
    attr_version_schema = StructType([
        StructField('lakefusion_id', StringType(), False),
        StructField('version', IntegerType(), False),
        StructField('attribute_source_mapping', ArrayType(attr_mapping_struct), True)
    ])
    
    # Create DataFrame for attribute version sources
    attr_version_df = spark.createDataFrame(all_attribute_versions, schema=attr_version_schema)
    
    # Batch insert using Delta merge
    attr_version_delta_table = DeltaTable.forName(spark, master_attribute_version_sources_table)
    
    attr_version_delta_table.alias("target").merge(
        source=attr_version_df.alias("source"),
        condition="target.lakefusion_id = source.lakefusion_id AND target.version = source.version"
    ).whenNotMatchedInsertAll().execute()
    
    logger.info(f" Batch inserted {len(all_attribute_versions)} attribute versions")
else:
    logger.info("  No attribute versions to insert")

# COMMAND ----------

# COMPLETION
logger.info("\n" + "=" * 80)
logger.info(" Incremental Update Processing Completed")
logger.info("=" * 80)
logger.info(f" Summary:")
logger.info(f"   Phase 1 - Unified Updates:")
logger.info(f"     - Sources processed: {len(tables_with_updates)}")
logger.info(f"     - Records updated in unified: {matched_count}")
logger.info(f"     - Changed attributes tracked: {len(changed_attributes_by_surrogate)} records")
logger.info(f"   Phase 2 - Master Updates (Batch Execution):")
logger.info(f"     - STANDALONE categorized: {len(standalone_ids)}")
logger.info(f"     - MULTI_CONTRIBUTOR categorized: {len(multi_contributor_ids)}")
logger.info(f"     - Master records updated: {len(all_master_updates)}")
logger.info(f"     - Merge activities logged: {len(all_merge_activities)}")
logger.info(f"     - Attribute versions logged: {len(all_attribute_versions)}")
logger.info(f"     - Records requiring survivorship: {len(records_requiring_survivorship)}")
logger.info("=" * 80)

# COMMAND ----------

# Set task values
dbutils.jobs.taskValues.set(key="status", value="SUCCESS")
dbutils.jobs.taskValues.set(key="records_updated_unified", value=matched_count)
dbutils.jobs.taskValues.set(key="records_updated_master", value=len(all_master_updates))
dbutils.jobs.taskValues.set(key="merge_activities_logged", value=len(all_merge_activities))
dbutils.jobs.taskValues.set(key="attribute_versions_logged", value=len(all_attribute_versions))
dbutils.jobs.taskValues.set(key="records_requiring_survivorship", value=len(records_requiring_survivorship))
