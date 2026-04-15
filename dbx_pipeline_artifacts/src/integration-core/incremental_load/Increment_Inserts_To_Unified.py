# Databricks notebook source
"""
PURPOSE: Process INSERT incrementals from ALL sources (primary + secondary)
         Transform to unified schema and merge into unified table
         
LOGIC FLOW:
1. Read CDF inserts from each source table
2. Transform using attributes_mapping (same as Prepare Tables)
3. Generate surrogate_key (hash of source_path + source_primary_key)
4. Add metadata columns (status, timestamps, source info)
5. Union all sources
6. Merge into unified table
7. Update meta_info_table versions

"""

import json
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType, LongType, DoubleType, FloatType, BooleanType, DateType, TimestampType, ShortType, ByteType
from pyspark.sql.window import Window
from datetime import datetime


def get_spark_data_type(dtype_str):
    """
    Convert string data type to Spark DataType.
    Handles both old lowercase types and new uppercase types.
    """
    dtype_map = {
        # New uppercase types (primary)
        'BIGINT': LongType(),
        'BOOLEAN': BooleanType(),
        'DATE': DateType(),
        'DOUBLE': DoubleType(),
        'FLOAT': FloatType(),
        'INT': IntegerType(),
        'SMALLINT': ShortType(),
        'STRING': StringType(),
        'TINYINT': ByteType(),
        'TIMESTAMP': TimestampType(),

        # Legacy lowercase types (backward compatibility)
        'bigint': LongType(),
        'boolean': BooleanType(),
        'char': StringType(),
        'varchar': StringType(),
        'date': DateType(),
        'double precision': DoubleType(),
        'double': DoubleType(),
        'integer': IntegerType(),
        'int': IntegerType(),
        'long': LongType(),
        'numeric': FloatType(),
        'real': FloatType(),
        'smallint': ShortType(),
        'text': StringType(),
        'string': StringType(),
        'timestamp': TimestampType(),
        'float': FloatType(),
        'decimal': DoubleType(),
    }

    # Try exact match first, then lowercase fallback
    return dtype_map.get(dtype_str, dtype_map.get(dtype_str.lower(), StringType()))

# COMMAND ----------

# MAGIC %run ../../utils/parse_utils

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
attributes = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="match_attributes", debugValue=attributes)

# Get info from previous task (Check_Increments_Exists)
has_inserts = dbutils.jobs.taskValues.get("Check_Increments_Exists", "has_inserts", debugValue=True)
tables_with_inserts = dbutils.jobs.taskValues.get("Check_Increments_Exists", "tables_with_inserts", debugValue="[]")
table_version_info = dbutils.jobs.taskValues.get("Check_Increments_Exists", "table_version_info", debugValue="{}")

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

from lakefusion_core_engine.identifiers import generate_surrogate_key
from lakefusion_core_engine.models import RecordStatus

# COMMAND ----------

# PARSE JSON
dataset_tables = json.loads(dataset_tables)
dataset_objects = json.loads(dataset_objects)
entity_attributes = json.loads(entity_attributes)
entity_attributes_datatype = json.loads(entity_attributes_datatype)
attributes_mapping_json = json.loads(attributes_mapping_json)
attributes = json.loads(attributes)
tables_with_inserts = json.loads(tables_with_inserts)
table_version_info = json.loads(table_version_info)

# Convert has_inserts to boolean if it's a string
if isinstance(has_inserts, str):
    has_inserts = has_inserts.lower() == 'true'

# Remove experiment_id hyphens
if experiment_id:
    experiment_id = experiment_id.replace("-", "")

# Add lakefusion_id to entity attributes if not present
if "lakefusion_id" not in entity_attributes:
    entity_attributes.append("lakefusion_id")

# COMMAND ----------

# TABLE NAMES
unified_table = f"{catalog_name}.silver.{entity}_unified"
master_table = f"{catalog_name}.gold.{entity}_master"
meta_info_table = f"{catalog_name}.silver.table_meta_info"

if experiment_id:
    unified_table += f"_{experiment_id}"
    master_table += f"_{experiment_id}"

merged_activities_table = f"{master_table}_merge_activities"

# COMMAND ----------

# UTILITY FUNCTIONS
# Create UDF for surrogate key generation using lakefusion_core_engine
generate_surrogate_key_udf = udf(
    lambda source_path, source_id: generate_surrogate_key(source_path, str(source_id)), 
    StringType()
)

def get_mapping_for_table(source_table: str):
    """
    Get attribute mapping for a specific source table.
    Uses same logic as get_primary_attr_mapping() from parse_utils
    but works for ANY source table, not just primary.
    
    Returns mapping in source_column -> entity_column format.
    """
    for attr_map in attributes_mapping_json:
        for dataset_path, mapping in attr_map.items():
            if source_table.startswith(dataset_path):
                # CRITICAL FIX: The mapping from parse_attributes_mapping() is entity->source
                # but we need source->entity for the INSERT logic to work
                # Example input:  {"client_id": "customer_id", "email": "email_address"}
                # Example output: {"customer_id": "client_id", "email_address": "email"}
                reversed_mapping = {v: k for k, v in mapping.items()}
                return reversed_mapping
    return {}

# COMMAND ----------

# ==============================================================================
# CDF DEDUPLICATION FUNCTION
# ==============================================================================

def read_and_deduplicate_cdf_for_inserts(source_table, start_version, end_version, source_id_cols):
    """
    Read ALL CDF change types and deduplicate per surrogate_key.
    Keep only records where the LATEST operation is INSERT.
    
    This handles scenarios where same record has multiple operations:
    - v2: INSERT rec1
    - v3: UPDATE rec1  
    - v4: UPDATE rec1
    
    Result: rec1 NOT returned (latest operation is UPDATE, not INSERT)
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    
    logger.info(f"  Reading ALL CDF change types for deduplication...")
    
    # Read CDF for ALL change types (not just inserts)
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
    
    # Find source primary key column for surrogate key generation
    # source_id_cols should be list of primary key columns
    if isinstance(source_id_cols, str):
        source_id_cols = [source_id_cols]
    
    # Concatenate PK columns to create source_id
    df_cdf = df_cdf.withColumn(
        "_source_id_concat",
        F.concat_ws("||", *[F.col(c).cast("string") for c in source_id_cols])
    )
    
    # Generate surrogate_key using lakefusion_core_engine
    df_cdf = df_cdf.withColumn(
        "surrogate_key",
        generate_surrogate_key_udf(F.lit(source_table), F.col("_source_id_concat"))
    )
    
    # DEDUPLICATION: Keep LATEST operation per surrogate_key
    # Order by: version DESC, timestamp DESC (latest first)
    window_spec = Window.partitionBy("surrogate_key") \
                        .orderBy(F.desc("_commit_version"), F.desc("_commit_timestamp"))
    
    df_latest = df_cdf.withColumn(
        "_row_num", F.row_number().over(window_spec)
    ).filter(F.col("_row_num") == 1).drop("_row_num")
    
    # Count by operation type before filtering
    logger.info(f"  Latest operations per surrogate_key:")
    df_latest.groupBy("operation_type").count().show(truncate=False)
    
    # Keep ONLY records where latest operation is INSERT
    df_inserts_only = df_latest.filter(F.col("operation_type") == "insert")

    # Cheap emptiness check instead of full count
    if not df_inserts_only.take(1):
        logger.info(f"  After deduplication: 0 records with INSERT as latest operation")
    
    # Clean up temporary columns
    df_inserts_only = df_inserts_only.drop("operation_type", "_source_id_concat")
    
    return df_inserts_only

def filter_existing_records_idempotency(df_inserts, unified_table):
    """
    IDEMPOTENCY CHECK: Remove records that already exist in unified table.
    This prevents duplicate inserts if notebook is re-run.
    """
    from pyspark.sql import functions as F
    
    logger.info(f"  Checking for existing records in unified table (idempotency)...")
    
    # Anti-join: keep only records NOT in unified
    df_new = df_inserts.alias("ins").join(
        spark.table(unified_table).select("surrogate_key").alias("uni"),
        F.col("ins.surrogate_key") == F.col("uni.surrogate_key"),
        "left_anti"
    )
    
    # Cheap presence checks instead of full counts
    has_original = bool(df_inserts.take(1))
    has_new = bool(df_new.take(1))
    
    if not has_original:
        logger.info("  No INSERT candidates found after deduplication")
        return df_new
    
    if not has_new:
        logger.info("  All INSERT candidates already exist in unified (idempotent skip)")
        return df_new
    
    logger.info("  New INSERT records present after idempotency filtering")
    
    return df_new

# ==============================================================================

# COMMAND ----------

# EARLY EXIT CHECK
if not has_inserts:
    logger.info("=" * 80)
    logger.info("  SKIPPING: No INSERT incrementals found (per Check_Increments_Exists)")
    logger.info("=" * 80)
    
    dbutils.jobs.taskValues.set(key="status", value="SKIPPED")
    dbutils.jobs.taskValues.set(key="records_inserted", value=0)
    dbutils.notebook.exit("SKIPPED_NO_INSERTS")

if len(tables_with_inserts) == 0:
    logger.info("=" * 80)
    logger.info("  SKIPPING: tables_with_inserts is empty")
    logger.info("=" * 80)
    
    dbutils.jobs.taskValues.set(key="status", value="SKIPPED")
    dbutils.jobs.taskValues.set(key="records_inserted", value=0)
    dbutils.notebook.exit("SKIPPED_NO_INSERTS")

# COMMAND ----------

# DEBUG: Log table_version_info structure
logger.info("\n" + "=" * 80)
logger.info(" Debug: table_version_info structure")
logger.info(f"   Type: {type(table_version_info)}")
logger.info(f"   Content: {table_version_info}")
logger.info("=" * 80)

# COMMAND ----------

# PROCESS EACH SOURCE TABLE
logger.info("\n" + "=" * 80)
logger.info(f" Processing {len(tables_with_inserts)} source table(s)")
logger.info("=" * 80)

all_transformed_dfs = []

for source_table in tables_with_inserts:
    logger.info("\n" + "=" * 80)
    logger.info(f" Processing: {source_table}")
    logger.info("=" * 80)
    
    # Get version info with error handling
    if source_table not in table_version_info:
        logger.warning(f" Version info not found for {source_table}")
        logger.warning(f"   Available tables in version_info: {list(table_version_info.keys())}")
        continue
    
    version_info = table_version_info[source_table]
    
    # Validate version_info structure
    if not isinstance(version_info, dict):
        logger.warning(f" Invalid version_info structure for {source_table}")
        logger.warning(f"   Expected dict, got: {type(version_info)}")
        logger.warning(f"   Content: {version_info}")
        continue
    
    # Get last_processed_version (may be None if first run)
    last_version = version_info.get("last_processed_version")
    current_version = version_info.get("current_version")
    
    if current_version is None:
        logger.warning(f" current_version not found in version_info for {source_table}")
        logger.warning(f"   version_info content: {version_info}")
        continue
    
    # If last_version is None, start from version 0
    if last_version is None:
        logger.info(f"  No last_processed_version found for {source_table}, starting from version 0")
        last_version = 0
    
    logger.info(f" Processing versions {last_version + 1} to {current_version} ({current_version - last_version} version(s))")
    
    # REPLACE LINES 230-256 WITH THIS:

    # Get starting_version from table_version_info
    starting_version = version_info.get("starting_version")
    
    if starting_version is None:
        # Fallback: calculate from last_processed_version
        starting_version = last_version + 1
    
    logger.info(f" Processing versions {starting_version} to {current_version}")
    
    # CRITICAL: Find source primary key column(s) for this table
    # Need this for surrogate key generation in deduplication
    mapping_dict = get_mapping_for_table(source_table)
    
    if not mapping_dict:
        logger.warning(f" No mapping found for {source_table}, skipping...")
        continue
    
    # Find source PK column from mapping
    source_pk_column = None
    for src_col, entity_col in mapping_dict.items():
        if entity_col == primary_key:
            source_pk_column = src_col
            break
    
    if not source_pk_column:
        logger.warning(f" Primary key not found in mapping for {source_table}, skipping...")
        continue
    
    # READ CDF WITH DEDUPLICATION
    try:
        df_inserts = read_and_deduplicate_cdf_for_inserts(
            source_table,
            starting_version,
            current_version,
            source_pk_column  # Pass PK column for surrogate key generation
        )
        
        if not df_inserts.take(1):
            logger.info(f"  No inserts to process for {source_table} after deduplication")
            continue
            
    except Exception as e:
        logger.error(f" Error reading CDF from {source_table}: {e}")
        logger.error(f"   Attempted to read versions {starting_version} to {current_version}")
        import traceback
        logger.error(traceback.format_exc())
        continue
    
    # Drop CDF metadata columns
    df_clean = df_inserts.drop("_change_type", "_commit_version", "_commit_timestamp")
    
    # Get attribute mapping for this source
    mapping_dict = get_mapping_for_table(source_table)
    
    if not mapping_dict:
        logger.warning(f" No mapping found for {source_table}")
        continue
    
    # CRITICAL: Find source primary key column name BEFORE transformation
    # The mapping tells us: source_pk_column -> entity_pk_column
    source_pk_column = None
    for src_col, entity_col in mapping_dict.items():
        if entity_col == primary_key:  # primary_key is the entity-level PK name
            source_pk_column = src_col
            break
    
    if not source_pk_column:
        logger.warning(f" Primary key '{primary_key}' not found in mapping for {source_table}")
        logger.warning(f"   Mapping: {mapping_dict}")
        continue
    
    # Verify source PK column exists in source data
    if source_pk_column not in df_clean.columns:
        logger.warning(f" Source PK column '{source_pk_column}' not found in {source_table}")
        logger.warning(f"   Available columns: {df_clean.columns}")
        continue
    
    # Store original source PK value BEFORE transformation
    df_clean = df_clean.withColumn("_source_pk_value", col(source_pk_column).cast("string"))
    
    logger.info(f" Applying mapping: {len(mapping_dict)} columns")
    logger.info(f"   Source PK column: {source_pk_column} -> Entity PK: {primary_key}")
    
    # Apply column mappings (source_col -> entity_col) WITH TYPE CASTING
    # Cast each column to its entity model type to prevent schema mismatches
    try:
        select_exprs = []
        for source_col, entity_col in mapping_dict.items():
            target_dtype_str = entity_attributes_datatype.get(entity_col, 'string')
            target_spark_dtype = get_spark_data_type(target_dtype_str)
            select_exprs.append(col(source_col).cast(target_spark_dtype).alias(entity_col))
        select_exprs.append(col("_source_pk_value"))
        df_transformed = df_clean.select(*select_exprs)
    except Exception as e:
        logger.error(f" Error applying column mappings: {e}")
        logger.error(f"   Mapping: {mapping_dict}")
        logger.error(f"   Source columns: {df_clean.columns}")
        continue

    # Add missing entity attributes as NULL columns with correct types
    existing_columns = df_transformed.columns
    missing_columns = [col_name for col_name in entity_attributes if col_name not in existing_columns and col_name != "lakefusion_id"]

    for col_name in missing_columns:
        sql_dtype = entity_attributes_datatype.get(col_name, "string")
        spark_dtype = get_spark_data_type(sql_dtype)
        df_transformed = df_transformed.withColumn(col_name, lit(None).cast(spark_dtype))
    
    # Select columns in entity_attributes order (excluding lakefusion_id for now)
    final_columns = [col_name for col_name in entity_attributes if col_name != "lakefusion_id"]
    df_transformed = df_transformed.select(*final_columns, "_source_pk_value")
    
    logger.info(f" Transformed to unified schema with {len(final_columns)} columns")
    
    # ADD METADATA COLUMNS
    
    # Generate surrogate_key: Using lakefusion_core_engine
    # source_path = table path, source_id = primary key value
    df_transformed = df_transformed.withColumn(
        "surrogate_key",
        generate_surrogate_key_udf(lit(source_table), col("_source_pk_value"))
    )
    
    # Add source metadata (matching unified schema)
    df_transformed = df_transformed.withColumn("source_path", lit(source_table))
    df_transformed = df_transformed.withColumn("source_id", col("_source_pk_value"))
    df_transformed = df_transformed.withColumn("master_lakefusion_id", lit(""))
    
    # Drop the temporary _source_pk_value column now that we've used it
    df_transformed = df_transformed.drop("_source_pk_value")
    
    # Add record status: ACTIVE (new inserts are always ACTIVE, not yet merged)
    df_transformed = df_transformed.withColumn("record_status", lit(RecordStatus.ACTIVE.value))
    
    # Add timestamps
    df_transformed = df_transformed.withColumn("created_at", current_timestamp())
    df_transformed = df_transformed.withColumn("updated_at", lit(None).cast("timestamp"))
    
    # Add table_name for compatibility with existing code
    df_transformed = df_transformed.withColumn("table_name", lit(source_table))
    
    # Generate attributes_combined column (for deduplication matching)
    # Only include attributes that exist in the dataframe
    available_attributes = [attr for attr in attributes if attr in df_transformed.columns]
    if available_attributes:
        df_transformed = df_transformed.withColumn(
            "attributes_combined",
            concat_ws(" | ", *[regexp_replace(trim(col(c)), r'\s+', ' ') for c in available_attributes])
        )
    else:
        df_transformed = df_transformed.withColumn("attributes_combined", lit(""))
    
    # Add search/scoring results columns (per unified schema)
    df_transformed = df_transformed.withColumn("search_results", lit(""))
    df_transformed = df_transformed.withColumn("scoring_results", lit(""))
    
    logger.info(f" Added metadata columns (surrogate_key, status, timestamps)")
    
    # Collect for union
    all_transformed_dfs.append(df_transformed)
    
    logger.info(f" Completed processing {source_table}")

# COMMAND ----------

# UNION ALL SOURCES
if len(all_transformed_dfs) == 0:
    logger.info(" No data to process from any source table")
    dbutils.jobs.taskValues.set(key="status", value="NO_DATA_PROCESSED")
    dbutils.notebook.exit("NO_DATA_PROCESSED")

logger.info("\n" + "=" * 80)
logger.info(f" Combining data from {len(all_transformed_dfs)} source(s)")

df_all_inserts = all_transformed_dfs[0]
for df in all_transformed_dfs[1:]:
    df_all_inserts = df_all_inserts.unionByName(df, allowMissingColumns=True)

total_insert_count = df_all_inserts.count()
logger.info(f" Combined total: {total_insert_count} INSERT records")

# ==============================================================================
# IDEMPOTENCY CHECK: Filter out records already in unified
# ==============================================================================
logger.info("\n" + "=" * 80)
logger.info(" IDEMPOTENCY CHECK: Filtering existing records")
logger.info("=" * 80)

df_all_inserts_new = filter_existing_records_idempotency(df_all_inserts, unified_table)

idempotent_count = df_all_inserts_new.count()

if idempotent_count < total_insert_count:
    logger.info(f" Removed {total_insert_count - idempotent_count} duplicate records (already in unified)")
    logger.info(f" Proceeding with {idempotent_count} new records")
else:
    logger.info(f" All {idempotent_count} records are new")

# Update df_all_inserts to use the idempotent version
df_all_inserts = df_all_inserts_new
total_insert_count = idempotent_count

# ==============================================================================

# COMMAND ----------

# DEDUPLICATE BY SURROGATE KEY (in case same record appears multiple times)
# Use deterministic ordering: keep most recent by created_at
window_spec = Window.partitionBy("surrogate_key").orderBy(desc("created_at"))
df_all_inserts_deduped = df_all_inserts \
    .withColumn("_rn", row_number().over(window_spec)) \
    .filter(col("_rn") == 1) \
    .drop("_rn")

deduped_count = df_all_inserts_deduped.count()

if deduped_count < total_insert_count:
    logger.info(f" Removed {total_insert_count - deduped_count} duplicate surrogate_keys")

# COMMAND ----------

# MERGE INTO UNIFIED TABLE
logger.info("\n" + "=" * 80)
logger.info(f" Merging into unified table: {unified_table}")

if not spark.catalog.tableExists(unified_table):
    logger.error(" Unified table doesn't exist!")
    logger.error(" Unified table must be created by Prepare Tables notebook first")
    logger.error(f" Missing table: {unified_table}")
    logger.error("=" * 80)
    
    dbutils.jobs.taskValues.set(key="status", value="ERROR")
    dbutils.jobs.taskValues.set(key="error_message", value=f"Unified table {unified_table} does not exist")
    dbutils.notebook.exit(f"ERROR: Unified table {unified_table} does not exist. Run Prepare Tables notebook first.")

# Merge logic: Insert only if surrogate_key doesn't exist
delta_unified = DeltaTable.forName(spark, unified_table)

delta_unified.alias("target").merge(
    df_all_inserts_deduped.alias("source"),
    "target.surrogate_key = source.surrogate_key"
).whenNotMatchedInsertAll().execute()

logger.info(f" Merged {deduped_count} new records into unified table")

# COMMAND ----------

# COMPLETION
logger.info("\n" + "=" * 80)
logger.info(" Incremental Insert Processing Completed Successfully")
logger.info(f" Summary:")
logger.info(f"   - Sources processed: {len(tables_with_inserts)}")
logger.info(f"   - Total inserts: {deduped_count}")
logger.info(f"   - Unified table: {unified_table}")
logger.info("=" * 80)

# Set task values for next step (deduplication)
dbutils.jobs.taskValues.set(key="status", value="SUCCESS")
dbutils.jobs.taskValues.set(key="records_inserted", value=deduped_count)
dbutils.jobs.taskValues.set(key="unified_table", value=unified_table)
dbutils.jobs.taskValues.set(key="master_table", value=master_table)

dbutils.notebook.exit("SUCCESS")
