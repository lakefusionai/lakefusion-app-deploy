# Databricks notebook source
# MAGIC %pip install ../../wheel/lakefusion_core_engine-1.0.0-py3-none-any.whl

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# DBTITLE 1,Imports
import json
from datetime import datetime
from pyspark.sql.functions import (
    col, lit, udf, concat_ws, coalesce, struct, collect_list, 
    current_timestamp, row_number, md5, concat, regexp_replace,
    when, count, countDistinct, sum as spark_sum, expr, to_json,
    broadcast, monotonically_increasing_id, explode
)
from pyspark.sql.types import (
    StringType, IntegerType, LongType, DoubleType, FloatType, 
    BooleanType, DateType, TimestampType, DecimalType, MapType, 
    ArrayType, StructType, StructField
)
from pyspark.sql import Window
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Widget Parameters
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("primary_key", "", "Primary Key")
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")
dbutils.widgets.text("entity_attributes_datatype", "", "Entity Attributes Datatype")
dbutils.widgets.text("attributes_mapping", "", "Attributes Mapping")
dbutils.widgets.text("primary_table", "", "Primary Table")
dbutils.widgets.text("dataset_tables", "", "Dataset Tables")
dbutils.widgets.text("dataset_objects", "", "Dataset Objects")
dbutils.widgets.text("match_attributes", "", "Match Attributes")
dbutils.widgets.text("default_survivorship_rules", "", "Survivorship Rules")
dbutils.widgets.text("config_thresholds", "", "Config Thresholds")

# COMMAND ----------

# DBTITLE 1,Parse Parameters
entity = dbutils.widgets.get("entity")
catalog_name = dbutils.widgets.get("catalog_name")
experiment_id = dbutils.widgets.get("experiment_id")
primary_key = dbutils.widgets.get("primary_key")
entity_attributes = json.loads(dbutils.widgets.get("entity_attributes"))
entity_attributes_datatype = json.loads(dbutils.widgets.get("entity_attributes_datatype"))
attributes_mapping = json.loads(dbutils.widgets.get("attributes_mapping"))
primary_table = dbutils.widgets.get("primary_table")
dataset_tables = json.loads(dbutils.widgets.get("dataset_tables"))
dataset_objects = json.loads(dbutils.widgets.get("dataset_objects"))
match_attributes = json.loads(dbutils.widgets.get("match_attributes"))
default_survivorship_rules = json.loads(dbutils.widgets.get("default_survivorship_rules"))
config_thresholds = json.loads(dbutils.widgets.get("config_thresholds")) 

# Parse thresholds
merge_thresholds = config_thresholds.get('merge', [0.9, 1.0])
matches_thresholds = config_thresholds.get('matches', [0.7, 0.89])
not_match_thresholds = config_thresholds.get('not_match', [0.0, 0.69])

# COMMAND ----------

# DBTITLE 1,Import LakeFusion Core Engine
from lakefusion_core_engine.identifiers import generate_lakefusion_id, generate_surrogate_key
from lakefusion_core_engine.models import RecordStatus, ActionType
from lakefusion_core_engine.survivorship import SurvivorshipEngine

# COMMAND ----------

# DBTITLE 1,Define Table Names
experiment_suffix = f"_{experiment_id}" if experiment_id else ""

# v4.0.0 table names (target)
master_table = f"{catalog_name}.gold.{entity}_master{experiment_suffix}"
unified_table = f"{catalog_name}.silver.{entity}_unified{experiment_suffix}"
merge_activities_table = f"{master_table}_merge_activities"
attribute_version_sources_table = f"{master_table}_attribute_version_sources"

# v3.x backup table names (source)
backup_master_table = f"{catalog_name}.gold.{entity}_master{experiment_suffix}_backup_v3"
backup_unified_table = f"{catalog_name}.silver.{entity}_unified{experiment_suffix}_backup_v3"
backup_merge_activities_table = f"{backup_master_table}_merge_activities"
backup_attribute_version_sources_table = f"{backup_master_table}_attribute_version_sources"

print("="*80)
print("MIGRATION: v3.x → v4.0.0 - TRANSFORM AND LOAD DATA")
print("="*80)
print(f"Entity: {entity}")
print(f"Catalog: {catalog_name}")
print(f"Experiment: {experiment_id if experiment_id else 'prod'}")
print(f"Primary Table: {primary_table}")
print(f"Primary Key: {primary_key}")
print(f"")
print("Target Tables (v4.0.0):")
print(f"  Master: {master_table}")
print(f"  Unified: {unified_table}")
print(f"  Merge Activities: {merge_activities_table}")
print(f"  Attribute Version Sources: {attribute_version_sources_table}")
print(f"")
print("Source Tables (v3.x backups):")
print(f"  Master Backup: {backup_master_table}")
print(f"  Unified Backup: {backup_unified_table}")
print(f"  Merge Activities Backup: {backup_merge_activities_table}")
print(f"  Attribute Version Sources Backup: {backup_attribute_version_sources_table}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Utility Functions
def get_spark_data_type(dtype_str):
    """Convert string data type to Spark DataType."""
    dtype_map = {
        'bigint': LongType(),
        'boolean': BooleanType(),
        'char': StringType(),
        'varchar': StringType(),
        'date': DateType(),
        'double precision': DoubleType(),
        'integer': IntegerType(),
        'numeric': FloatType(),
        'real': FloatType(),
        'smallint': IntegerType(),
        'text': StringType(),
        'timestamp': TimestampType(),
        'string': StringType(),
        'int': IntegerType(),
        'long': LongType(),
        'double': DoubleType(),
        'float': FloatType(),
        'decimal': DecimalType(10, 2),
    }
    return dtype_map.get(dtype_str.lower(), StringType())

# Create UDFs for ID generation
generate_surrogate_key_udf = udf(
    lambda source_path, source_id: generate_surrogate_key(source_path, str(source_id)), 
    StringType()
)

print("✓ Utility functions and UDFs registered")

# COMMAND ----------

# DBTITLE 1,Prepare Survivorship Engine
print("\n" + "="*80)
print("PREPARING SURVIVORSHIP ENGINE")
print("="*80)

# Create mapping from dataset ID to path
dataset_id_to_path = {dataset['id']: dataset['path'] for dataset in dataset_objects.values()}

# Convert Source System strategy rules from IDs to paths
for rule in default_survivorship_rules:
    if rule.get('strategy') == 'Source System' and 'strategyRule' in rule:
        rule['strategyRule'] = [
            dataset_id_to_path.get(dataset_id) 
            for dataset_id in rule['strategyRule']
        ]

print(f"✓ Survivorship rules updated with dataset paths")
print(f"  Rules: {len(default_survivorship_rules)}")

# Initialize survivorship engine
engine = SurvivorshipEngine(
    survivorship_config=default_survivorship_rules,
    entity_attributes=entity_attributes,
    entity_attributes_datatype=entity_attributes_datatype,
    id_key='surrogate_key'
)

print("✓ Survivorship Engine initialized")
print("="*80)

# COMMAND ----------

print("\n" + "="*80)
print("STEP 1: BUILD MAPPINGS FROM BACKUP TABLES")
print("="*80)
print("Building all mappings directly from backup tables (no crosswalk view)")
print("-"*80)

# COMMAND ----------

# DBTITLE 1,Step 1.1: Read Old Unified Table
print("\n[1.1] Reading old unified table...")

old_unified_df = spark.table(backup_unified_table)
old_unified_count = old_unified_df.count()

print(f"✓ Read {old_unified_count} records from backup unified")
print(f"  Columns: {old_unified_df.columns}")
print(f"\nSample old unified records:")
old_unified_df.show(5, truncate=False)

# Identify the source column name (could be 'table_name', 'source', or 'source_path')
source_col_name = None
for possible_col in ['table_name', 'source', 'source_path']:
    if possible_col in old_unified_df.columns:
        source_col_name = possible_col
        break

if source_col_name:
    print(f"✓ Source column identified: {source_col_name}")
else:
    raise Exception("Could not identify source column in old unified table!")

# COMMAND ----------

# DBTITLE 1,Step 1.2: Build Old Unified to Surrogate Key Mapping
print("\n[1.2] Building mapping from old unified records to surrogate keys...")

# The old unified table has:
# - lakefusion_id: the old record's ID  
# - table_name/source: the source table path
# - primary_key attribute: the source record ID

# Build mapping: old_lakefusion_id → surrogate_key
old_lf_to_sk_df = old_unified_df.select(
    col("lakefusion_id").alias("old_lakefusion_id"),
    col(source_col_name).alias("old_source_path"),
    col(primary_key).cast("string").alias("old_source_id")
).withColumn(
    "old_surrogate_key",
    generate_surrogate_key_udf(col("old_source_path"), col("old_source_id"))
)

# Materialize to temp table (serverless doesn't support cache or DBFS writes)
temp_old_lf_mapping_table = f"{catalog_name}.silver.{entity}_temp_old_lf_mapping{experiment_suffix}"
spark.sql(f"DROP TABLE IF EXISTS {temp_old_lf_mapping_table}")
old_lf_to_sk_df.write.format("delta").mode("overwrite").saveAsTable(temp_old_lf_mapping_table)
old_lf_to_sk_df = spark.table(temp_old_lf_mapping_table)

old_lf_mapping_count = old_lf_to_sk_df.count()
print(f"✓ Built mapping for {old_lf_mapping_count} old unified records")
print(f"✓ Materialized to temp table: {temp_old_lf_mapping_table}")
print(f"\nSample mapping:")
old_lf_to_sk_df.show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Step 1.3: Read Backup Merge Activities
print("\n[1.3] Reading backup merge activities...")

backup_ma_df = spark.table(backup_merge_activities_table)
backup_ma_count = backup_ma_df.count()

print(f"✓ Read {backup_ma_count} merge activities from backup")
print(f"  Columns: {backup_ma_df.columns}")
print(f"\nSample merge activities:")
backup_ma_df.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Step 1.3.1: Deduplicate INITIAL_LOAD/JOB_INSERT Records in Merge Activities
print("\n[1.3.1] Deduplicating INITIAL_LOAD/JOB_INSERT records in merge activities...")
print("-"*80)

initial_insert_records = backup_ma_df.filter(
    (col("action_type").isin(["INITIAL_LOAD", "JOB_INSERT"])) &
    (col("lakefusion_id").isNull() | (col("lakefusion_id") == ""))
)

other_records = backup_ma_df.filter(
    ~(
        (col("action_type").isin(["INITIAL_LOAD", "JOB_INSERT"])) &
        (col("lakefusion_id").isNull() | (col("lakefusion_id") == ""))
    )
)

dedup_window = Window.partitionBy("master_lakefusion_id", "source").orderBy(
    when(col("action_type") == "INITIAL_LOAD", lit(0)).otherwise(lit(1)).asc(),
    col("version").asc()
)

deduplicated_initial_insert = initial_insert_records.withColumn(
    "dedup_rank", row_number().over(dedup_window)
).filter(col("dedup_rank") == 1).drop("dedup_rank")

# Combine deduplicated INITIAL_LOAD/JOB_INSERT with other records
backup_ma_df = deduplicated_initial_insert.unionByName(other_records)

print("✓ Deduplication logic applied (will materialize on next action)")

# COMMAND ----------

# DBTITLE 1,Step 1.4: Build Master to Contributors Mapping
print("\n[1.4] Building master to contributors mapping...")

# From old unified, we can determine which records belong to which master
# Records with the same lakefusion_id in old unified are contributors to the same master

# First, check if old unified has a reference to master
# In v3.x, unified records that were merged share the same lakefusion_id as the master

# Get all unique lakefusion_ids from old unified
old_unified_lf_ids = old_unified_df.select("lakefusion_id").distinct()

# Cross-reference with master table to find which are masters
backup_master_df = spark.table(backup_master_table)
master_lf_ids = backup_master_df.select(
    col("lakefusion_id").alias("master_lakefusion_id")
)

print(f"  Old unified lakefusion_ids: {old_unified_lf_ids.count()}")
print(f"  Master lakefusion_ids: {master_lf_ids.count()}")

# In v3.x architecture:
# - Each old unified record has its OWN lakefusion_id
# - Merge activities track: master_lakefusion_id, lakefusion_id (contributor), source
# - To find contributors for each master, we need to use merge_activities

# Build mapping from merge_activities: which old unified records belong to which master
contributor_mapping_df = backup_ma_df.filter(
    col("action_type").isin(["INITIAL_LOAD", "JOB_INSERT", "JOB_MERGE", "MANUAL_MERGE"])
).select(
    col("master_lakefusion_id"),
    col("lakefusion_id").alias("contributor_lakefusion_id"),
    col("source").alias("contributor_source"),
    col("action_type")
)

# For INITIAL_LOAD and JOB_INSERT, lakefusion_id is NULL (the master itself is the contributor)
# For JOB_MERGE and MANUAL_MERGE, lakefusion_id is the old unified record that was merged

print(f"\nContributor mapping from merge activities:")
contributor_mapping_df.show(10, truncate=False)

# Count by action type
print(f"\nAction type distribution:")
contributor_mapping_df.groupBy("action_type").count().show()

# COMMAND ----------

# DBTITLE 1,Step 1.5: Build Complete Source Record to Master Mapping
print("\n[1.5] Building complete source record to master mapping...")

# We need to map each source record (identified by source_path + source_id) to its master_lakefusion_id

# Strategy:
# 1. For INITIAL_LOAD/JOB_INSERT: Use merge_activities.source + master.custid
# 2. For JOB_MERGE/MANUAL_MERGE: Use old_unified records

# Step 1: Get merged records mapping (JOB_MERGE/MANUAL_MERGE)
merged_records = contributor_mapping_df.filter(
    (col("action_type").isin(["JOB_MERGE", "MANUAL_MERGE"])) &
    (col("contributor_lakefusion_id").isNotNull())
).join(
    old_lf_to_sk_df,
    col("contributor_lakefusion_id") == col("old_lakefusion_id"),
    "inner"
).select(
    col("master_lakefusion_id"),
    col("old_source_path").alias("source_path"),
    col("old_source_id").alias("source_id"),
    col("old_surrogate_key").alias("surrogate_key"),
    col("action_type")
)

merged_count = merged_records.count()
print(f"✓ Merged records (JOB_MERGE/MANUAL_MERGE): {merged_count}")

# Step 2: Get initial records mapping (INITIAL_LOAD/JOB_INSERT)
# Use merge_activities to get the correct source, then join with master to get custid

# Get INITIAL_LOAD/JOB_INSERT activities with their source
initial_activities = contributor_mapping_df.filter(
    col("action_type").isin(["INITIAL_LOAD", "JOB_INSERT"])
).select(
    col("master_lakefusion_id").alias("init_master_id"),
    col("contributor_source").alias("init_source")
).distinct()

initial_activities_count = initial_activities.count()
print(f"\n✓ Found {initial_activities_count} INITIAL_LOAD/JOB_INSERT activities")

# Join with master table to get the custid for each master
backup_master_df = spark.table(backup_master_table)

initial_records_with_custid = initial_activities.join(
    backup_master_df.select(
        col("lakefusion_id").alias("master_lf_id"),
        col(primary_key).cast("string").alias("custid_value")
    ),
    col("init_master_id") == col("master_lf_id"),
    "inner"
).select(
    col("init_master_id").alias("master_lakefusion_id"),
    col("init_source").alias("source_path"),
    col("custid_value").alias("source_id")
).withColumn(
    "surrogate_key",
    generate_surrogate_key_udf(col("source_path"), col("source_id"))
).withColumn(
    "action_type", lit("INITIAL_LOAD")
).distinct()

initial_count = initial_records_with_custid.count()
print(f"✓ Initial records from merge_activities + master: {initial_count}")

print(f"\nSample initial records mapping:")
initial_records_with_custid.show(10, truncate=False)

# Combine merged and initial records
all_source_to_master_df = merged_records.unionByName(initial_records_with_custid, allowMissingColumns=True)

# Materialize to temp table
temp_source_master_table = f"{catalog_name}.silver.{entity}_temp_source_master{experiment_suffix}"
spark.sql(f"DROP TABLE IF EXISTS {temp_source_master_table}")
all_source_to_master_df.write.format("delta").mode("overwrite").saveAsTable(temp_source_master_table)
all_source_to_master_df = spark.table(temp_source_master_table)

total_mapping_count = all_source_to_master_df.count()
print(f"\n✓ Total source to master mappings: {total_mapping_count}")
print(f"✓ Materialized to temp table: {temp_source_master_table}")
print(f"\nSample mappings:")
all_source_to_master_df.show(10, truncate=False)

# Verify we have all masters covered
masters_with_mapping = all_source_to_master_df.select("master_lakefusion_id").distinct().count()
total_masters = backup_master_df.count()
print(f"\n✓ Masters with mapping: {masters_with_mapping}/{total_masters}")
if masters_with_mapping < total_masters:
    print(f"⚠️ WARNING: {total_masters - masters_with_mapping} masters missing from mapping!")

# COMMAND ----------

print("\n[1.6] Validating source to master mapping...")

# Check for masters without mappings
backup_master_df = spark.table(backup_master_table)
all_master_ids = backup_master_df.select(
    col("lakefusion_id").alias("check_master_id")
)

masters_without_mapping = all_master_ids.join(
    all_source_to_master_df.select("master_lakefusion_id").distinct(),
    col("check_master_id") == col("master_lakefusion_id"),
    "left_anti"
)

missing_count = masters_without_mapping.count()

if missing_count > 0:
    print(f"⚠️ WARNING: {missing_count} masters have no source mapping!")
    print(f"\nMissing masters:")
    masters_without_mapping.show(truncate=False)
    
    # Show their merge activities to debug
    missing_master_ids = [row.check_master_id for row in masters_without_mapping.collect()]
    print(f"\nMerge activities for missing masters:")
    backup_ma_df.filter(col("master_lakefusion_id").isin(missing_master_ids)).show(truncate=False)
    
    raise Exception(f"❌ Migration cannot proceed - {missing_count} masters missing from source mapping")
else:
    print(f"✓ All {all_master_ids.count()} masters have source mappings")

# Validate no duplicate surrogate_keys
duplicate_sks = all_source_to_master_df.groupBy("surrogate_key", "master_lakefusion_id").count().filter(col("count") > 1)
dup_count = duplicate_sks.count()

if dup_count > 0:
    print(f"⚠️ WARNING: {dup_count} duplicate (surrogate_key, master) pairs found!")
    duplicate_sks.show(truncate=False)
else:
    print(f"✓ No duplicate mappings found")

print(f"\n✓ Source to master mapping validation complete")

# COMMAND ----------

print("\n" + "="*80)
print("STEP 2: ENRICH MERGE ACTIVITIES WITH TIMESTAMPS")
print("="*80)
print("Adding created_at timestamps from table history")
print("-"*80)

# COMMAND ----------

# DBTITLE 1,Step 2.1: Get Master Table History
print("\n[2.1] Retrieving master table history...")

try:
    # Use BACKUP master table history (original v3.x history)
    master_history_df = spark.sql(f"DESCRIBE HISTORY {backup_master_table}").select(
        col("version").alias("master_version"),
        col("timestamp").alias("version_timestamp")
    )
    
    history_count = master_history_df.count()
    print(f"✓ Retrieved {history_count} versions from backup master table history")
    
    print(f"\nSample history:")
    master_history_df.orderBy("master_version").show(10, truncate=False)
    
except Exception as e:
    print(f"⚠️ WARNING: Could not retrieve master table history: {e}")
    print("   Using current timestamp as fallback")
    master_history_df = spark.createDataFrame(
        [(0, datetime.now())],
        ["master_version", "version_timestamp"]
    )

# COMMAND ----------

# DBTITLE 1,Step 2.2: Enrich Merge Activities with Timestamps
print("\n[2.2] Enriching merge activities with timestamps...")

# Join merge activities with history to get timestamps
enriched_ma_df = backup_ma_df.join(
    broadcast(master_history_df),
    backup_ma_df["version"] == master_history_df["master_version"],
    "left"
).select(
    backup_ma_df["master_lakefusion_id"],
    backup_ma_df["lakefusion_id"],
    backup_ma_df["source"],
    backup_ma_df["version"],
    backup_ma_df["action_type"],
    coalesce(col("version_timestamp"), current_timestamp()).alias("created_at")
)

enriched_count = enriched_ma_df.count()
with_timestamps = enriched_ma_df.filter(col("created_at").isNotNull()).count()

print(f"✓ Enriched {enriched_count} merge activities")
print(f"  Records with timestamps: {with_timestamps}")
print(f"\nSample enriched merge activities:")
enriched_ma_df.show(5, truncate=False)

# COMMAND ----------

print("\n" + "="*80)
print("STEP 3: BUILD NEW UNIFIED TABLE")
print("="*80)
print("Creating unified table with all source records")
print("-"*80)

# COMMAND ----------

# DBTITLE 1,Step 3.1: Read All Source Records
print("\n[3.1] Reading all source records from dataset tables...")

all_source_records = []

for source_table in dataset_tables:
    print(f"\n  Processing: {source_table}")
    
    # Find attribute mapping for this source
    source_mapping = None
    for mapping_entry in attributes_mapping:
        if source_table in mapping_entry:
            source_mapping = mapping_entry[source_table]
            break
    
    if not source_mapping:
        print(f"    ⚠️ No mapping found - skipping")
        continue
    
    # Read source table
    source_df = spark.table(source_table)
    source_count = source_df.count()
    print(f"    ✓ Read {source_count} records")
    
    # Find source primary key column
    source_pk_col = None
    if primary_key in source_mapping:
        source_pk_col = source_mapping[primary_key]
    
    if not source_pk_col or source_pk_col not in source_df.columns:
        print(f"    ⚠️ Primary key column '{source_pk_col}' not found - skipping")
        continue
    
    print(f"    ✓ Source PK column: {source_pk_col} → {primary_key}")
    
    # Build select expressions with type casting
    # Mapping format: {entity_attr: dataset_attr}
    select_exprs = []
    for entity_attr, dataset_attr in source_mapping.items():
        if dataset_attr in source_df.columns:
            target_dtype = get_spark_data_type(entity_attributes_datatype.get(entity_attr, 'string'))
            select_exprs.append(col(dataset_attr).cast(target_dtype).alias(entity_attr))
    
    # Add missing entity attributes as NULL
    mapped_entity_attrs = set(source_mapping.keys())
    for attr in entity_attributes:
        if attr not in mapped_entity_attrs and attr != "lakefusion_id":
            target_dtype = get_spark_data_type(entity_attributes_datatype.get(attr, 'string'))
            select_exprs.append(lit(None).cast(target_dtype).alias(attr))
    
    # Apply mapping
    mapped_df = source_df.select(*select_exprs)
    
    # Add source metadata
    mapped_df = mapped_df \
        .withColumn("source_path", lit(source_table)) \
        .withColumn("source_id", col(primary_key).cast("string"))
    
    # Generate surrogate_key
    mapped_df = mapped_df.withColumn(
        "surrogate_key",
        generate_surrogate_key_udf(col("source_path"), col("source_id"))
    )
    
    all_source_records.append(mapped_df)
    print(f"    ✓ Processed {source_count} records")

# Union all source records
if not all_source_records:
    raise Exception("❌ No source records found!")

all_sources_df = all_source_records[0]
for df in all_source_records[1:]:
    all_sources_df = all_sources_df.unionByName(df, allowMissingColumns=True)

total_source_count = all_sources_df.count()
print(f"\n✓ Total source records: {total_source_count}")

# COMMAND ----------

# DBTITLE 1,Step 3.2: Determine Record Status (MERGED vs ACTIVE)
print("\n[3.2] Determining record status (MERGED vs ACTIVE)...")

# Join all_sources_df with all_source_to_master_df to determine status
# If surrogate_key is in mapping → MERGED (has master_lakefusion_id)
# If not → ACTIVE (standalone record)

print(f"\nDEBUG - Source records surrogate_keys:")
all_sources_df.select("surrogate_key", "source_path", "source_id").show(10, truncate=False)

print(f"\nDEBUG - Source to master mapping:")
all_source_to_master_df.select("surrogate_key", "source_path", "source_id", "master_lakefusion_id").show(10, truncate=False)

# Check matches
matches = all_sources_df.alias("src").join(
    all_source_to_master_df.alias("map"),
    col("src.surrogate_key") == col("map.surrogate_key"),
    "inner"
).count()
print(f"\nDEBUG - Matching records: {matches}")

# Join to get master_lakefusion_id
unified_with_status_df = all_sources_df.alias("src").join(
    all_source_to_master_df.select(
        col("surrogate_key").alias("map_sk"),
        col("master_lakefusion_id")
    ).alias("map"),
    col("src.surrogate_key") == col("map.map_sk"),
    "left"
).select(
    col("src.surrogate_key"),
    col("src.source_path"),
    col("src.source_id"),
    coalesce(col("map.master_lakefusion_id"), lit("")).alias("master_lakefusion_id"),
    when(col("map.master_lakefusion_id").isNotNull(), lit(RecordStatus.MERGED.value))
     .otherwise(lit(RecordStatus.ACTIVE.value)).alias("record_status"),
    *[col(f"src.{attr}") for attr in entity_attributes if attr != "lakefusion_id"]
)

# Count by status
status_counts = unified_with_status_df.groupBy("record_status").count()
print("\n✓ Record status determined")
print("\nStatus distribution:")
status_counts.show(truncate=False)

merged_count = unified_with_status_df.filter(col("record_status") == RecordStatus.MERGED.value).count()
active_count = unified_with_status_df.filter(col("record_status") == RecordStatus.ACTIVE.value).count()

print(f"\n  MERGED records: {merged_count}")
print(f"  ACTIVE records: {active_count}")
print(f"  Total: {total_source_count}")

# COMMAND ----------

# DBTITLE 1,Step 3.3: Preserve Search Results from Old Unified
print("\n[3.3] Preserving search_results from old unified table...")

# Get search_results from old unified
old_search_df = old_unified_df.select(
    col("lakefusion_id").alias("old_lf_id"),
    col("search_results")
).filter(
    (col("search_results").isNotNull()) & (col("search_results") != "")
)

# Map to surrogate_key using old_lf_to_sk_df
search_with_sk = old_search_df.join(
    old_lf_to_sk_df,
    col("old_lf_id") == col("old_lakefusion_id"),
    "inner"
).select(
    col("old_surrogate_key").alias("search_sk"),
    col("search_results")
)

search_mapped_count = search_with_sk.count()
print(f"✓ Mapped {search_mapped_count} search_results to surrogate_keys")

# Join with unified
unified_with_search = unified_with_status_df.alias("u").join(
    search_with_sk.alias("s"),
    col("u.surrogate_key") == col("s.search_sk"),
    "left"
).select(
    col("u.*"),
    coalesce(col("s.search_results"), lit("")).alias("search_results")
)

# COMMAND ----------

# DBTITLE 1,Step 3.4: Build Scoring Results
print("\n[3.4] Building scoring_results from old processed_unified...")

# Define UDF to transform old match labels to new convention based on thresholds
def transform_scoring_results(scoring_results_json, merge_thresh, matches_thresh):
    """
    Transform old MATCH/NOT_A_MATCH labels to new MATCH/POTENTIAL_MATCH/NO_MATCH
    based on score thresholds.
    
    - Score within merge_thresholds → "MATCH"
    - Score within matches_thresholds → "POTENTIAL_MATCH"  
    - Score below → "NO_MATCH"
    """
    if not scoring_results_json:
        return scoring_results_json
    
    try:
        import json
        results = json.loads(scoring_results_json)
        
        if not isinstance(results, list):
            return scoring_results_json
        
        transformed = []
        for result in results:
            if isinstance(result, dict) and 'score' in result:
                score = float(result.get('score', 0))
                
                # Determine new match label based on thresholds
                if merge_thresh[0] <= score <= merge_thresh[1]:
                    new_match = "MATCH"
                elif matches_thresh[0] <= score <= matches_thresh[1]:
                    new_match = "POTENTIAL_MATCH"
                else:
                    new_match = "NO_MATCH"
                
                result['match'] = new_match
            transformed.append(result)
        
        return json.dumps(transformed)
    except:
        return scoring_results_json

# Register UDF with threshold values captured in closure
transform_scoring_udf = udf(
    lambda x: transform_scoring_results(x, merge_thresholds, matches_thresholds),
    StringType()
)

# Check if processed_unified backup exists
backup_processed_unified = f"{catalog_name}.silver.{entity}_processed_unified{experiment_suffix}_backup_v3"

try:
    if spark.catalog.tableExists(backup_processed_unified):
        processed_df = spark.table(backup_processed_unified)
        
        # Show columns to understand structure
        print(f"  Processed unified columns: {processed_df.columns}")
        
        # Identify the result column (could be 'result', 'exploded_result', or other)
        result_col_name = None
        for possible_col in ['exploded_result', 'result', 'scoring_result']:
            if possible_col in processed_df.columns:
                result_col_name = possible_col
                break
        
        if result_col_name is None:
            print(f"  ⚠️ No result column found in processed_unified")
            scoring_results_df = spark.createDataFrame([], schema="proc_sk string, scoring_results string")
        else:
            print(f"  Using result column: {result_col_name}")
            
            # Map to surrogate_key using old_lf_to_sk_df
            processed_with_sk = processed_df.join(
                old_lf_to_sk_df,
                processed_df["lakefusion_id"] == old_lf_to_sk_df["old_lakefusion_id"],
                "inner"
            ).select(
                col("old_surrogate_key").alias("proc_sk"),
                col(result_col_name).alias("result_value")
            )
            
            # Group by surrogate_key and collect results
            scoring_results_raw = processed_with_sk.groupBy("proc_sk").agg(
                to_json(collect_list(col("result_value"))).alias("scoring_results_raw")
            )
            
            # Transform match labels based on thresholds
            scoring_results_df = scoring_results_raw.withColumn(
                "scoring_results",
                transform_scoring_udf(col("scoring_results_raw"))
            ).select("proc_sk", "scoring_results")
            
            print(f"✓ Built and transformed scoring_results")
            print(f"  Thresholds applied:")
            print(f"    MATCH: {merge_thresholds}")
            print(f"    POTENTIAL_MATCH: {matches_thresholds}")
            print(f"    NO_MATCH: below {matches_thresholds[0]}")
    else:
        print("⚠️ Processed unified backup not found - scoring_results will be empty")
        scoring_results_df = spark.createDataFrame([], schema="proc_sk string, scoring_results string")
except Exception as e:
    print(f"⚠️ Error building scoring_results: {e}")
    scoring_results_df = spark.createDataFrame([], schema="proc_sk string, scoring_results string")

# Join with unified
unified_with_scoring = unified_with_search.alias("u2").join(
    scoring_results_df.alias("sc"),
    col("u2.surrogate_key") == col("sc.proc_sk"),
    "left"
).select(
    col("u2.*"),
    coalesce(col("sc.scoring_results"), lit("")).alias("scoring_results")
)

# COMMAND ----------

# DBTITLE 1,Step 3.5: Create Attributes Combined
print("\n[3.5] Creating attributes_combined column...")

if match_attributes:
    concat_cols = []
    for attr in match_attributes:
        if attr in entity_attributes and attr != "lakefusion_id":
            concat_cols.append(coalesce(col(attr).cast("string"), lit("")))
    
    if concat_cols:
        new_unified_df = unified_with_scoring.withColumn(
            "attributes_combined",
            concat_ws(" | ", *concat_cols)
        )
    else:
        new_unified_df = unified_with_scoring.withColumn(
            "attributes_combined", lit("")
        )
else:
    new_unified_df = unified_with_scoring.withColumn(
        "attributes_combined", lit("")
    )

print(f"✓ Created attributes_combined from {len(match_attributes)} attributes")

# Reorder columns
final_column_order = [
    "surrogate_key",
    "source_path",
    "source_id",
    "master_lakefusion_id",
    "record_status"
] + [attr for attr in entity_attributes if attr != "lakefusion_id"] + [
    "attributes_combined",
    "search_results",
    "scoring_results"
]

new_unified_df = new_unified_df.select(*final_column_order)

print(f"✓ Final unified DataFrame prepared")
print(f"  Total records: {new_unified_df.count()}")
print(f"\nSample unified data:")
new_unified_df.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Step 3.5.1: Deduplicate Records Merged into Multiple Masters
print("\n[3.5.1] Checking for duplicate merges (same source record merged into multiple masters)...")

# Find surrogate_keys that have multiple master_lakefusion_ids (the bug from old architecture)
from pyspark.sql.functions import get_json_object, from_json, schema_of_json, size, array_max, transform

# Count how many masters each surrogate_key is merged into
merged_records_df = new_unified_df.filter(col("record_status") == RecordStatus.MERGED.value)

duplicate_check = merged_records_df.groupBy("surrogate_key").agg(
    countDistinct("master_lakefusion_id").alias("master_count"),
    collect_list("master_lakefusion_id").alias("masters")
).filter(col("master_count") > 1)

duplicate_count = duplicate_check.count()
print(f"  Found {duplicate_count} surrogate_keys merged into multiple masters")

if duplicate_count > 0:
    print("\n  Duplicate merge details:")
    duplicate_check.show(10, truncate=False)
    
    # Get the list of duplicate surrogate_keys
    duplicate_sks = [row.surrogate_key for row in duplicate_check.collect()]
    print(f"  Duplicate surrogate_keys: {duplicate_sks}")
    
    # For each duplicate, we need to find the best master based on scoring_results
    # The scoring_results contains JSON array with scores for each potential match
    # Format: [{"id":"...", "match":"MATCH", "score":0.95, "lakefusion_id":"master_id"}, ...]
    
    # Define UDF to extract score for a specific master from scoring_results
    def get_score_for_master(scoring_results_str, master_id):
        """Extract the score for a specific master from scoring_results JSON."""
        if not scoring_results_str or not master_id:
            return 0.0
        try:
            import json
            results = json.loads(scoring_results_str)
            for result in results:
                if result.get("lakefusion_id") == master_id:
                    return float(result.get("score", 0.0))
            return 0.0
        except:
            return 0.0
    
    get_score_udf = udf(get_score_for_master, DoubleType())
    
    # Add score column for each record's master
    unified_with_score = new_unified_df.withColumn(
        "master_score",
        when(
            col("record_status") == RecordStatus.MERGED.value,
            get_score_udf(col("scoring_results"), col("master_lakefusion_id"))
        ).otherwise(lit(1.0))  # ACTIVE records keep score 1.0
    )
    
    # For duplicates, keep only the record with the highest score
    # Use window function to rank by score within each surrogate_key
    dedup_window = Window.partitionBy("surrogate_key").orderBy(col("master_score").desc())
    
    unified_ranked = unified_with_score.withColumn(
        "score_rank", row_number().over(dedup_window)
    )
    
    # Keep only rank 1 (highest score) for each surrogate_key
    new_unified_df = unified_ranked.filter(col("score_rank") == 1).drop("master_score", "score_rank")
    
    # Track which (surrogate_key, master_id) pairs were removed for cleaning merge_activities later
    removed_merges_df = unified_ranked.filter(col("score_rank") > 1).select(
        col("surrogate_key").alias("removed_sk"),
        col("master_lakefusion_id").alias("removed_master_id")
    )
    
    removed_count = removed_merges_df.count()
    print(f"\n  ✓ Removed {removed_count} duplicate merge records")
    print(f"  Removed mappings:")
    removed_merges_df.show(truncate=False)
    
    # Store removed merges for later cleanup of merge_activities
    # We'll use this in Step 5 to filter out these from merge_activities
else:
    print("  ✓ No duplicate merges found - data is clean")
    removed_merges_df = None

# Verify deduplication
final_merged_count = new_unified_df.filter(col("record_status") == RecordStatus.MERGED.value).count()
unique_sk_count = new_unified_df.select("surrogate_key").distinct().count()
total_count = new_unified_df.count()

print(f"\n  After deduplication:")
print(f"    Total records: {total_count}")
print(f"    Unique surrogate_keys: {unique_sk_count}")
print(f"    MERGED records: {final_merged_count}")

# Verify no duplicates remain
remaining_dups = new_unified_df.groupBy("surrogate_key").count().filter(col("count") > 1).count()
if remaining_dups > 0:
    print(f"  ⚠️ WARNING: {remaining_dups} surrogate_keys still have duplicates!")
else:
    print(f"  ✓ All surrogate_keys are now unique")

# COMMAND ----------

# DBTITLE 1,Step 3.6: Materialize Unified DataFrame
print("\n[3.6] Materializing unified DataFrame...")

temp_unified_table = f"{catalog_name}.silver.{entity}_temp_unified_migration{experiment_suffix}"
spark.sql(f"DROP TABLE IF EXISTS {temp_unified_table}")
new_unified_df.write.format("delta").mode("overwrite").saveAsTable(temp_unified_table)
new_unified_df = spark.table(temp_unified_table)

final_unified_count = new_unified_df.count()
print(f"✓ Materialized to temp table: {temp_unified_table}")
print(f"✓ Final unified count: {final_unified_count}")

# COMMAND ----------

print("\n" + "="*80)
print("STEP 4: REPROCESS SURVIVORSHIP FOR MASTER")
print("="*80)
print("Applying survivorship rules to determine master record values")
print("-"*80)

# COMMAND ----------

# DBTITLE 1,Step 4.1: Group Contributors by Master
print("\n[4.1] Grouping contributors by master_lakefusion_id...")

# Filter to only MERGED records
merged_unified = new_unified_df.filter(col("record_status") == RecordStatus.MERGED.value)

contributor_count = merged_unified.count()
master_count = merged_unified.select("master_lakefusion_id").distinct().count()

print(f"✓ Found {contributor_count} contributors to {master_count} master records")

if contributor_count == 0:
    print("⚠️ No merged records found - skipping survivorship")
    survivorship_results_df = None
else:
    # Group contributors
    grouped_contributors = merged_unified.groupBy("master_lakefusion_id").agg(
        collect_list(
            struct(
                col("surrogate_key"),
                col("source_path"),
                *[col(attr) for attr in entity_attributes if attr != "lakefusion_id"]
            )
        ).alias("contributors")
    )
    
    print(f"✓ Grouped into {grouped_contributors.count()} master records")

# COMMAND ----------

# DBTITLE 1,Step 4.2: Apply Survivorship Rules
if contributor_count > 0:
    print("\n[4.2] Applying survivorship rules...")
    
    # Define survivorship UDF wrapper
    def apply_survivorship_wrapper(contributors):
        """
        UDF wrapper for survivorship engine.
        Converts Spark Rows to dicts and applies survivorship.
        """
        if not contributors:
            return None
        
        # Convert Spark Rows to dictionaries
        contributor_dicts = [c.asDict() for c in contributors]
        
        # Apply survivorship using correct method name
        result = engine.apply_survivorship(unified_records=contributor_dicts)
        
        # Return as dict for Spark (result has .to_dict() method)
        result_dict = result.to_dict()
        
        return {
            "resultant_record": result_dict.get("resultant_record", {}),
            "attribute_sources": result_dict.get("resultant_master_attribute_source_mapping", [])
        }
    
    survivorship_schema = StructType([
        StructField("resultant_record", MapType(StringType(), StringType())),
        StructField("attribute_sources", ArrayType(
            StructType([
                StructField("attribute_name", StringType()),
                StructField("attribute_value", StringType()),
                StructField("source", StringType())
            ])
        ))
    ])
    
    survivorship_udf = udf(apply_survivorship_wrapper, survivorship_schema)
    
    # Apply survivorship
    survivorship_results_df = grouped_contributors.withColumn(
        "survivorship_result",
        survivorship_udf(col("contributors"))
    ).select(
        col("master_lakefusion_id").alias("lakefusion_id"),
        col("survivorship_result.resultant_record").alias("merged_record"),
        col("survivorship_result.attribute_sources").alias("attribute_sources")
    )
    
    survivorship_count = survivorship_results_df.count()
    print(f"✓ Applied survivorship to {survivorship_count} master records")
else:
    survivorship_results_df = None

# COMMAND ----------

# DBTITLE 1,Step 4.3: Prepare Master Updates
if survivorship_results_df is not None:
    print("\n[4.3] Preparing master updates...")
    
    # Build column expressions
    master_update_cols = [col("lakefusion_id")]
    
    for attr in entity_attributes:
        if attr != "lakefusion_id":
            target_dtype = get_spark_data_type(entity_attributes_datatype.get(attr, 'string'))
            master_update_cols.append(
                col(f"merged_record.{attr}").cast(target_dtype).alias(attr)
            )
    
    # Add attributes_combined
    if match_attributes:
        concat_cols = []
        for attr in match_attributes:
            if attr in entity_attributes and attr != "lakefusion_id":
                concat_cols.append(coalesce(col(f"merged_record.{attr}").cast("string"), lit("")))
        
        if concat_cols:
            master_update_cols.append(concat_ws(" | ", *concat_cols).alias("attributes_combined"))
        else:
            master_update_cols.append(lit("").alias("attributes_combined"))
    else:
        master_update_cols.append(lit("").alias("attributes_combined"))
    
    master_updates_df = survivorship_results_df.select(*master_update_cols)
    
    print(f"✓ Prepared {master_updates_df.count()} master updates")
    print(f"\nSample master updates:")
    master_updates_df.show(5, truncate=False)
    
    # Prepare attribute version sources
    attribute_sources_df = survivorship_results_df.select(
        col("lakefusion_id"),
        col("attribute_sources").alias("attribute_source_mapping")
    )
else:
    master_updates_df = None
    attribute_sources_df = None

# COMMAND ----------

print("\n" + "="*80)
print("STEP 5: TRANSFORM MERGE ACTIVITIES")
print("="*80)
print("Converting merge activities to new schema")
print("-"*80)

# COMMAND ----------

# DBTITLE 1,Step 5.1: Rename and Map Merge Activities
print("\n[5.1] Transforming merge activities to new schema...")

# Rename columns: master_lakefusion_id → master_id
# Map lakefusion_id → match_id (surrogate_key)

# Build lookup from old lakefusion_id to surrogate_key
lf_to_sk_lookup = old_lf_to_sk_df.select(
    col("old_lakefusion_id").alias("lookup_lf_id"),
    col("old_surrogate_key").alias("lookup_sk")
)

# Transform merge activities
transformed_ma_df = enriched_ma_df.join(
    broadcast(lf_to_sk_lookup),
    enriched_ma_df["lakefusion_id"] == col("lookup_lf_id"),
    "left"
).select(
    col("master_lakefusion_id").alias("master_id"),
    when(
        (col("lakefusion_id").isNotNull()) & (col("lakefusion_id") != ""),
        col("lookup_sk")
    ).otherwise(lit(None)).alias("match_id"),
    col("source"),
    col("version"),
    col("action_type"),
    col("created_at")
)

# For INITIAL_LOAD/JOB_INSERT, match_id should be the surrogate_key of the source record
# We need to fill in NULL match_ids using all_source_to_master_df

# Get surrogate_keys for master records
master_sk_lookup = all_source_to_master_df.select(
    col("master_lakefusion_id").alias("msk_master_id"),
    col("source_path").alias("msk_source"),
    col("surrogate_key").alias("msk_sk")
).dropDuplicates(["msk_master_id", "msk_source"])

# Fill NULL match_ids
new_merge_activities_df = transformed_ma_df.join(
    broadcast(master_sk_lookup),
    (transformed_ma_df["master_id"] == col("msk_master_id")) &
    (transformed_ma_df["source"] == col("msk_source")) &
    (transformed_ma_df["match_id"].isNull()),
    "left"
).select(
    col("master_id"),
    when(col("match_id").isNotNull(), col("match_id"))
        .otherwise(col("msk_sk")).alias("match_id"),
    col("source"),
    col("version"),
    col("action_type"),
    col("created_at")
)

# Step 5.1.1: Remove duplicate merge activities (same source merged into multiple masters)
# Filter out the merge activities that were removed during unified deduplication
if removed_merges_df is not None and removed_merges_df.count() > 0:
    print("\n  Removing duplicate merge activities...")
    
    ma_before_dedup = new_merge_activities_df.count()
    
    # Left anti join to remove records where (match_id, master_id) matches removed pairs
    new_merge_activities_df = new_merge_activities_df.join(
        removed_merges_df,
        (new_merge_activities_df["match_id"] == removed_merges_df["removed_sk"]) &
        (new_merge_activities_df["master_id"] == removed_merges_df["removed_master_id"]),
        "left_anti"
    )
    
    ma_after_dedup = new_merge_activities_df.count()
    removed_ma_count = ma_before_dedup - ma_after_dedup
    
    print(f"  ✓ Removed {removed_ma_count} duplicate merge activities")
    print(f"    Before: {ma_before_dedup}")
    print(f"    After: {ma_after_dedup}")

# Verify
total_ma = new_merge_activities_df.count()
with_match_id = new_merge_activities_df.filter(col("match_id").isNotNull()).count()
null_match_id = total_ma - with_match_id

print(f"✓ Transformed {total_ma} merge activities")
print(f"  With match_id: {with_match_id}")
print(f"  NULL match_id: {null_match_id}")

print(f"\nSample transformed merge activities:")
new_merge_activities_df.show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Step 5.2: Materialize Merge Activities
print("\n[5.2] Materializing merge activities...")

temp_ma_table = f"{catalog_name}.silver.{entity}_temp_ma_migration{experiment_suffix}"
spark.sql(f"DROP TABLE IF EXISTS {temp_ma_table}")
new_merge_activities_df.write.format("delta").mode("overwrite").saveAsTable(temp_ma_table)
new_merge_activities_df = spark.table(temp_ma_table)

final_ma_count = new_merge_activities_df.count()
print(f"✓ Materialized to temp table: {temp_ma_table}")
print(f"✓ Final merge activities count: {final_ma_count}")

# COMMAND ----------

print("\n" + "="*80)
print("STEP 6: EXECUTE TABLE UPDATES")
print("="*80)
print("Writing transformed data to target tables")
print("-"*80)

# COMMAND ----------

# DBTITLE 1,Step 6.1: Update Unified Table
print("\n[6.1] Updating Unified table...")

# Drop old table and create new
spark.sql(f"DROP TABLE IF EXISTS {unified_table}")
print(f"  ✓ Dropped old table: {unified_table}")

new_unified_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(unified_table)

unified_final_count = spark.table(unified_table).count()
print(f"  ✓ Created new table: {unified_table}")
print(f"  ✓ Records: {unified_final_count}")

# COMMAND ----------

# DBTITLE 1,Step 6.2: Update Master Table
print("\n[6.2] Updating Master table...")

if master_updates_df is not None and master_updates_df.count() > 0:
    # Use MERGE to update master records
    master_delta = DeltaTable.forName(spark, master_table)
    
    # Build update expression
    update_exprs = {attr: f"source.{attr}" for attr in entity_attributes if attr != "lakefusion_id"}
    update_exprs["attributes_combined"] = "source.attributes_combined"
    
    master_delta.alias("target").merge(
        master_updates_df.alias("source"),
        "target.lakefusion_id = source.lakefusion_id"
    ).whenMatchedUpdate(set=update_exprs).execute()
    
    # Get new version
    new_master_version = spark.sql(f"DESCRIBE HISTORY {master_table}").select("version").first()[0]
    print(f"  ✓ Master table updated (version {new_master_version})")
else:
    print("  ⚠️ No master updates to apply")
    new_master_version = spark.sql(f"DESCRIBE HISTORY {master_table}").select("version").first()[0]

master_final_count = spark.table(master_table).count()
print(f"  ✓ Master records: {master_final_count}")

# COMMAND ----------

# DBTITLE 1,Step 6.3: Update Merge Activities Table
print("\n[6.3] Updating Merge Activities table...")

# Drop and recreate
spark.sql(f"DROP TABLE IF EXISTS {merge_activities_table}")
print(f"  ✓ Dropped old table")

new_merge_activities_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(merge_activities_table)

ma_final_count = spark.table(merge_activities_table).count()
print(f"  ✓ Created new table: {merge_activities_table}")
print(f"  ✓ Records: {ma_final_count}")

# COMMAND ----------

# DBTITLE 1,Step 6.4: Update Attribute Version Sources
print("\n[6.4] Updating Attribute Version Sources table...")

if attribute_sources_df is not None and attribute_sources_df.count() > 0:
    # Append new records with current version
    new_avs_df = attribute_sources_df.withColumn(
        "version",
        lit(new_master_version).cast("integer")
    )
    
    new_avs_df.write.format("delta").mode("append").saveAsTable(attribute_version_sources_table)
    
    new_avs_count = new_avs_df.count()
    print(f"  ✓ Appended {new_avs_count} new records")
else:
    print("  ⚠️ No attribute sources to append")

avs_final_count = spark.table(attribute_version_sources_table).count()
print(f"  ✓ Total records: {avs_final_count}")

# COMMAND ----------

# DBTITLE 1,Step 6.5: Cleanup Temp Tables
print("\n[6.5] Cleaning up temporary tables...")

temp_tables = [
    temp_old_lf_mapping_table,
    temp_source_master_table,
    temp_unified_table,
    temp_ma_table
]

for table in temp_tables:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        print(f"  ✓ Dropped: {table}")
    except Exception as e:
        print(f"  ⚠️ Could not drop {table}: {e}")

print("✓ Cleanup completed")

# COMMAND ----------

print("\n" + "="*80)
print("VALIDATION CHECKS")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Validation 1: Record Counts
print("\n[Validation 1] Checking record counts...")

unified_count = spark.table(unified_table).count()
master_count = spark.table(master_table).count()
ma_count = spark.table(merge_activities_table).count()

print(f"  Unified records: {unified_count}")
print(f"  Master records: {master_count}")
print(f"  Merge activities: {ma_count}")

if unified_count == total_source_count:
    print(f"  ✓ PASS: Unified equals source count ({total_source_count})")
else:
    print(f"  ⚠️ WARN: Unified ({unified_count}) != source ({total_source_count})")

# COMMAND ----------

# DBTITLE 1,Validation 2: Status Distribution
print("\n[Validation 2] Checking record status distribution...")

status_dist = spark.sql(f"""
    SELECT record_status, COUNT(*) as count
    FROM {unified_table}
    GROUP BY record_status
""")
status_dist.show()

merged = spark.sql(f"SELECT COUNT(*) FROM {unified_table} WHERE record_status = 'MERGED'").first()[0]
active = spark.sql(f"SELECT COUNT(*) FROM {unified_table} WHERE record_status = 'ACTIVE'").first()[0]

print(f"  MERGED: {merged}")
print(f"  ACTIVE: {active}")

# COMMAND ----------

# DBTITLE 1,Validation 3: Match ID Coverage
print("\n[Validation 3] Checking match_id coverage in merge activities...")

total_ma = spark.table(merge_activities_table).count()
with_match_id = spark.sql(f"""
    SELECT COUNT(*) FROM {merge_activities_table}
    WHERE match_id IS NOT NULL
""").first()[0]

print(f"  Total merge activities: {total_ma}")
print(f"  With match_id: {with_match_id}")
print(f"  Without match_id: {total_ma - with_match_id}")

# COMMAND ----------

print("\nDropping tables that will be created in pipeline...")
print("="*80)

# Tables to drop (no longer needed in v4.0.0)
tables_to_drop = [
    f"{catalog_name}.silver.{entity}_unified_deduplicate{experiment_suffix}",
    f"{catalog_name}.silver.{entity}_processed_unified{experiment_suffix}",
    f"{catalog_name}.silver.{entity}_processed_unified_deterministic{experiment_suffix}",
    f"{catalog_name}.silver.{entity}_processed_unified_deduplicate{experiment_suffix}",
    f"{catalog_name}.silver.{entity}_processed_unified_deduplicate_deterministic{experiment_suffix}",
    f"{catalog_name}.gold.{entity}_master_potential_match{experiment_suffix}",
    f"{catalog_name}.gold.{entity}_master_potential_match_deduplicate{experiment_suffix}",
    f"{catalog_name}.gold.{entity}_crosswalk"
]

dropped_count = 0
for table_to_drop in tables_to_drop:
    try:
        if spark.catalog.tableExists(table_to_drop):
            spark.sql(f"DROP TABLE {table_to_drop}")
            print(f"✓ Dropped: {table_to_drop}")
            dropped_count += 1
        else:
            print(f"⊘ Not found: {table_to_drop}")
    except Exception as e:
        print(f"⚠️ Failed to drop {table_to_drop}: {e}")

print(f"\n✓ Dropped {dropped_count} tables")
print("="*80)

# COMMAND ----------

print("\n" + "="*80)
print("✅ MIGRATION COMPLETE")
print("="*80)

print(f"\nFinal Statistics:")
print(f"  Master records: {master_count}")
print(f"  Unified records: {unified_count}")
print(f"    - MERGED: {merged}")
print(f"    - ACTIVE: {active}")
print(f"  Merge activities: {ma_count}")
print(f"  Attribute version sources: {avs_final_count}")

print("\n" + "="*80)
print("Migration completed successfully!")
print("="*80)

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")
