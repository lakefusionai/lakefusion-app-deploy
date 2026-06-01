# Databricks notebook source
import json
from datetime import datetime
from pyspark.sql.functions import (
    col, explode, lit, collect_list, first, struct, count,
    current_timestamp, udf, coalesce, concat_ws
)
from pyspark.sql.types import *
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
dbutils.widgets.text("master_id", "", "Master Record ID to unmerge from")
dbutils.widgets.text("unified_dataset_ids", "", "Unified Record IDs to unmerge (JSON)")

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
    "Parse_Entity_Model_JSON", "match_attributes",
    debugValue=dbutils.widgets.get("match_attributes")
)
reference_attribute_config = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="reference_attribute_config",
    debugValue="{}"
)
master_id = dbutils.widgets.get("master_id")
unmerge_unified_dataset_ids = dbutils.widgets.get("unified_dataset_ids")

# COMMAND ----------

entity_attributes = json.loads(entity_attributes)
entity_attributes_datatype = json.loads(entity_attributes_datatype)
default_survivorship_rules = json.loads(default_survivorship_rules)
dataset_objects = json.loads(dataset_objects)
match_attributes = json.loads(match_attributes)
unmerge_unified_dataset_ids = json.loads(unmerge_unified_dataset_ids)
reference_attribute_config = (
    json.loads(reference_attribute_config)
    if isinstance(reference_attribute_config, str)
    else (reference_attribute_config or {})
)

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

# MAGIC %run ../../utils/rdm_resolver

# COMMAND ----------

# Extract surrogate keys and dataset IDs from input
unmerge_surrogate_keys = []
unmerge_dataset_ids = []
for item in unmerge_unified_dataset_ids:
    unmerge_surrogate_keys.append(item.get("id"))
    unmerge_dataset_ids.append(item.get("dataset_id"))

logger.info(f"Records to unmerge: {len(unmerge_surrogate_keys)}")
logger.info(f"Surrogate keys: {unmerge_surrogate_keys}")
logger.info(f"Dataset IDs: {unmerge_dataset_ids}")

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
logger.info("MANUAL UNMERGE PROCESSING")
logger.info("="*80)
logger.info(f"Entity: {entity}")
logger.info(f"Master ID: {master_id}")
logger.info(f"Master ID Key: {id_key}")
logger.info(f"Unified ID Key: {unified_id_key}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment ID: {experiment_id or 'None'}")
logger.info(f"Unified Table: {unified_table}")
logger.info(f"Master Table: {master_table}")
logger.info(f"Merge Activities Table: {merge_activities_table}")
logger.info(f"Attribute Version Sources Table: {attribute_version_sources_table}")
logger.info(f"Records to unmerge: {len(unmerge_surrogate_keys)}")
logger.info("="*80)

# COMMAND ----------

setup_lakefusion_engine()

# COMMAND ----------

# Capture record snapshots BEFORE any modifications for steward decisions
try:
    attr_cols = ", ".join(entity_attributes)
    _sd_master_row = spark.sql(f"SELECT {attr_cols} FROM {master_table} WHERE {id_key} = '{master_id}'").collect()
    _sd_master_attrs = _sd_master_row[0].asDict() if _sd_master_row else {}
    _sd_match_id = unmerge_surrogate_keys[0] if unmerge_surrogate_keys else None
    if _sd_match_id:
        # Check if match_id is a surrogate_key (sk_ prefix) or a lakefusion_id
        if str(_sd_match_id).startswith("sk_"):
            _sd_match_row = spark.sql(f"SELECT {attr_cols}, source_path FROM {unified_table} WHERE {unified_id_key} = '{_sd_match_id}'").collect()
        else:
            _sd_match_row = spark.sql(f"SELECT {attr_cols} FROM {master_table} WHERE {id_key} = '{_sd_match_id}'").collect()
        _sd_match_attrs = _sd_match_row[0].asDict() if _sd_match_row else {}
    else:
        _sd_match_attrs = {}
    logger.info(f"Steward decision snapshots captured for master={master_id}, match={_sd_match_id}")
except Exception as e:
    logger.warning(f"Could not capture steward decision snapshots: {e}")
    _sd_master_attrs = {}
    _sd_match_attrs = {}
    _sd_match_id = unmerge_surrogate_keys[0] if unmerge_surrogate_keys else ""

# COMMAND ----------

from lakefusion_core_engine.survivorship import SurvivorshipEngine, __version__
from lakefusion_core_engine.identifiers import generate_lakefusion_id
from lakefusion_core_engine.utils import merged_record_column

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
logger.info("STEP 2: FETCH ALL CONTRIBUTORS FOR THE MASTER")
logger.info("="*80)

# Build attribute columns dynamically. Includes the survivorship audit
# timestamp `__lf_modified_at` (added by migration c4d2e8f7a1b9) which the
# survivorship engine uses as a deterministic tie-breaker.
attribute_cols = ", ".join(["u.__lf_modified_at"] + [f"u.{attr}" for attr in entity_attributes])

# Get all MERGED contributors for this master
all_contributors_query = f"""
SELECT
    u.{unified_id_key},
    u.source_path,
    {attribute_cols}
FROM {unified_table} u
WHERE u.master_{id_key} = '{master_id}'
    AND u.record_status = 'MERGED'
"""

all_contributors_df = spark.sql(all_contributors_query)
total_contributors = all_contributors_df.count()

logger.info(f"Total contributors found: {total_contributors}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 3: VALIDATE UNMERGE OPERATION")
logger.info("="*80)

# Check if records to unmerge exist in contributors
unmerge_contributors_df = all_contributors_df.filter(
    col(unified_id_key).isin(unmerge_surrogate_keys)
)
unmerge_count = unmerge_contributors_df.count()

logger.info(f"  Records to unmerge found in contributors: {unmerge_count}")

if unmerge_count == 0:
    error_msg = "None of the specified records are contributors to this master"
    logger.error(error_msg)
    dbutils.notebook.exit(json.dumps({
        "status": "error",
        "message": error_msg
    }))

if unmerge_count != len(unmerge_surrogate_keys):
    missing_count = len(unmerge_surrogate_keys) - unmerge_count
    logger.warning(f"{missing_count} requested records are not contributors to this master")

# Check if trying to unmerge the only contributor
remaining_contributors_count = total_contributors - unmerge_count

if remaining_contributors_count == 0:
    error_msg = "Cannot unmerge - this is the only contributor to the master. Master would be left empty."
    logger.error(error_msg)
    dbutils.notebook.exit(json.dumps({
        "status": "error",
        "message": error_msg,
        "total_contributors": total_contributors,
        "attempting_to_unmerge": unmerge_count
    }))

logger.info(f"Validation passed")
logger.info(f"  Total contributors: {total_contributors}")
logger.info(f"  Unmerging: {unmerge_count}")
logger.info(f"  Remaining: {remaining_contributors_count}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 4: SEPARATE REMAINING AND UNMERGE CONTRIBUTORS")
logger.info("="*80)

# Get remaining contributors (exclude unmerge records)
remaining_contributors_df = all_contributors_df.filter(
    ~col(unified_id_key).isin(unmerge_surrogate_keys)
)

remaining_count = remaining_contributors_df.count()

logger.info(f"Remaining contributors: {remaining_count}")
logger.info(f"  Unmerge contributors: {unmerge_count}")

# Capture schemas BEFORE materialization
remaining_schema = remaining_contributors_df.schema
unmerge_schema = unmerge_contributors_df.schema

# Materialize both DataFrames to avoid invalidation in serverless
remaining_contributors_data = remaining_contributors_df.collect()
unmerge_contributors_data = unmerge_contributors_df.collect()

logger.info(f"Materialized {len(remaining_contributors_data)} remaining contributors")
logger.info(f"Materialized {len(unmerge_contributors_data)} unmerge contributors")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 5: RECALCULATE MASTER WITH REMAINING CONTRIBUTORS")
logger.info("="*80)

# Recreate remaining_contributors_df from materialized data WITH EXPLICIT SCHEMA
remaining_contributors_df = spark.createDataFrame(remaining_contributors_data, schema=remaining_schema)

# Group remaining contributors for survivorship
grouped_remaining = (
    remaining_contributors_df
    .withColumn(id_key, lit(master_id))
    .groupBy(id_key)
    .agg(
        collect_list(
            struct(
                col(unified_id_key),
                col("source_path"),
                *[col(attr) for attr in entity_attributes]
            )
        ).alias("unified_records")
    )
)

logger.info(f"Grouped remaining contributors for survivorship")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 6: APPLY SURVIVORSHIP FOR UPDATED MASTER")
logger.info("="*80)

# Define UDF wrapper
def apply_survivorship_wrapper(unified_records):
    """UDF wrapper for survivorship engine."""
    records_list = [r.asDict() for r in unified_records]
    result = engine.apply_survivorship(unified_records=records_list)
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

# Apply survivorship
result_df = grouped_remaining.withColumn(
    "survivorship_result",
    survivorship_udf(col("unified_records"))
)

# Extract results
updated_master_results = result_df.select(
    col(id_key),
    col("survivorship_result.resultant_record").alias("merged_record"),
    col("survivorship_result.resultant_master_attribute_source_mapping").alias("attribute_sources")
)

logger.info(f"Survivorship applied for updated master")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 7: GENERATE NEW LAKEFUSION IDS FOR UNMERGE RECORDS")
logger.info("="*80)

# Generate new lakefusion_id for each unmerge record
new_lakefusion_ids = {}
for item in unmerge_unified_dataset_ids:
    surrogate_key = item.get("id")
    new_lf_id = generate_lakefusion_id()
    new_lakefusion_ids[surrogate_key] = new_lf_id
    logger.info(f"  {surrogate_key} -> {new_lf_id}")

logger.info(f"Generated {len(new_lakefusion_ids)} new lakefusion IDs")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 8: PREPARE UNMERGE RECORDS FOR MASTER INSERT")
logger.info("="*80)

# Recreate unmerge_contributors_df from materialized data WITH EXPLICIT SCHEMA
unmerge_contributors_df = spark.createDataFrame(unmerge_contributors_data, schema=unmerge_schema)

# Create a mapping DataFrame for new lakefusion_ids
new_ids_data = [{"surrogate_key": sk, "new_lakefusion_id": lf_id} 
                for sk, lf_id in new_lakefusion_ids.items()]
new_ids_df = spark.createDataFrame(new_ids_data)

# Join to add new lakefusion_ids to unmerge records
unmerge_with_ids_df = unmerge_contributors_df.join(
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

# attributes_combined: resolve REFERENCE_ENTITY attrs through ref tables.
# These rows hold a single ref_id per attribute (single contributor, no
# concat aggregation in this path), but the helper handles both cases.
if match_attributes:
    insert_cols.append(
        build_attributes_combined_column(
            spark=spark,
            match_attributes=match_attributes,
            entity_attributes=entity_attributes,
            id_key=None,  # don't skip any attr here
            reference_attribute_config=reference_attribute_config,
            source_prefix=None,
        ).alias("attributes_combined")
    )
else:
    insert_cols.append(lit("").alias("attributes_combined"))

unmerge_insert_df = unmerge_with_ids_df.select(*insert_cols)

logger.info(f"Prepared {unmerge_insert_df.count()} records for master insert")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 9: UPDATE MASTER TABLE (ATOMIC TRANSACTION)")
logger.info("="*80)

logger.info(f"  Match attributes: {match_attributes}")

# Prepare master table updates with proper type casting
master_update_cols = [col(id_key)]

for attr in entity_attributes:
    if attr == id_key:
        continue
    
    target_type = entity_attributes_datatype.get(attr, "string")
    master_update_cols.append(merged_record_column(attr, target_type))

if match_attributes:
    # Master rebuild after unmerge — survivorship may have produced a
    # concat-aggregated multi-id value; helper splits and resolves each id.
    master_update_cols.append(
        build_attributes_combined_column(
            spark=spark,
            match_attributes=match_attributes,
            entity_attributes=entity_attributes,
            id_key=None,
            reference_attribute_config=reference_attribute_config,
            source_prefix="merged_record",
        ).alias("attributes_combined")
    )
    logger.info(f"  Generated attributes_combined from: {match_attributes}")
    else:
        master_update_cols.append(lit("").alias("attributes_combined"))
        logger.warning("No valid match attributes found, using empty string")
else:
    master_update_cols.append(lit("").alias("attributes_combined"))
    logger.warning("No match attributes defined, using empty string")

master_updates_df = updated_master_results.select(*master_update_cols)

# Get current version before transaction
current_master_version = spark.sql(f"DESCRIBE HISTORY {master_table} LIMIT 1").select("version").collect()[0][0]
logger.info(f"  Current master version: {current_master_version}")

# Perform atomic transaction: UPDATE existing master + INSERT unmerged records
master_delta_table = DeltaTable.forName(spark, master_table)

# Build update dict
update_dict = {attr: f"source.{attr}" for attr in entity_attributes if attr != id_key}
update_dict["attributes_combined"] = "source.attributes_combined"
# Invalidate precomputed embedding so it recomputes on next pipeline run
master_cols_lower = {f.name.strip().lower() for f in spark.table(master_table).schema}
if "attributes_combined_embedding" in master_cols_lower:
    update_dict["attributes_combined_embedding"] = "NULL"

# Combine update and insert sources
combined_source = master_updates_df.union(unmerge_insert_df)

# Add attributes_combined_embedding as NULL if the target table has it
if "attributes_combined_embedding" in master_cols_lower:
    combined_source = combined_source.withColumn(
        "attributes_combined_embedding", lit(None).cast("array<float>")
    )

# Execute merge: UPDATE for existing master_id, INSERT for new lakefusion_ids
master_delta_table.alias("target").merge(
    combined_source.alias("source"),
    f"target.{id_key} = source.{id_key}"
).whenMatchedUpdate(
    set=update_dict
).whenNotMatchedInsertAll().execute()

# Get new version
new_master_version = spark.sql(f"DESCRIBE HISTORY {master_table} LIMIT 1").select("version").collect()[0][0]

logger.info(f"Master table updated (atomic transaction)")
logger.info(f"  New version: {new_master_version}")
logger.info(f"  Updated existing master: 1")
logger.info(f"  Inserted unmerged records: {unmerge_count}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 10: PREPARE ATTRIBUTE VERSION SOURCES")
logger.info("="*80)

# 1. Attribute sources for UPDATED master (remaining contributors)
updated_master_attr_sources = updated_master_results.select(
    col(id_key),
    lit(new_master_version).alias("version"),
    col("attribute_sources").alias("attribute_source_mapping")
)

# 2. Attribute sources for INSERTED unmerged records (single contributor each)
# Recreate unmerge_with_ids_df WITH EXPLICIT SCHEMA
unmerge_contributors_df = spark.createDataFrame(unmerge_contributors_data, schema=unmerge_schema)
unmerge_with_ids_df = unmerge_contributors_df.join(new_ids_df, on=unified_id_key, how="inner")

# For each unmerged record, create attribute source mapping
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

unmerged_attr_sources = unmerge_with_ids_df.select(
    col("new_lakefusion_id").alias(id_key),
    lit(new_master_version).alias("version"),
    single_contributor_udf(col("source_path"), attr_cols_struct).alias("attribute_source_mapping")
)

# Combine both attribute sources
all_attr_sources = updated_master_attr_sources.union(unmerged_attr_sources)

logger.info(f"Prepared attribute version sources")
logger.info(f"  Updated master: 1")
logger.info(f"  Unmerged records: {unmerge_count}")
logger.info(f"  Total: {all_attr_sources.count()}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 11: UPDATE ATTRIBUTE VERSION SOURCES")
logger.info("="*80)

# Perform merge
attr_sources_delta_table = DeltaTable.forName(spark, attribute_version_sources_table)

attr_sources_delta_table.alias("target").merge(
    all_attr_sources.alias("source"),
    f"target.{id_key} = source.{id_key} AND target.version = source.version"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

logger.info(f"Attribute version sources updated")
logger.info(f"  Records updated/inserted: {all_attr_sources.count()}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 12: LOG MERGE ACTIVITIES (ATOMIC TRANSACTION)")
logger.info("="*80)

# 1. MANUAL_UNMERGE activities (from old master)
unmerge_activities = unmerge_with_ids_df.select(
    lit(master_id).alias("master_id"),
    col(unified_id_key).alias("match_id"),
    col("source_path").alias("source"),
    lit(new_master_version).alias("version"),
    lit("MANUAL_UNMERGE").alias("action_type"),
    current_timestamp().alias("created_at")
)

# 2. JOB_INSERT activities (to new masters)
insert_activities = unmerge_with_ids_df.select(
    col("new_lakefusion_id").alias("master_id"),
    col(unified_id_key).alias("match_id"),
    col("source_path").alias("source"),
    lit(new_master_version).alias("version"),
    lit("JOB_INSERT").alias("action_type"),
    current_timestamp().alias("created_at")
)

# Combine both activities
all_activities = unmerge_activities.union(insert_activities)

activities_count = all_activities.count()

logger.info(f"  Prepared merge activities:")
logger.info(f"    MANUAL_UNMERGE: {unmerge_count}")
logger.info(f"    JOB_INSERT: {unmerge_count}")
logger.info(f"    Total: {activities_count}")

# Perform atomic insert of all activities
if activities_count > 0:
    merge_activities_delta_table = DeltaTable.forName(spark, merge_activities_table)
    
    merge_activities_delta_table.alias("target").merge(
        all_activities.alias("source"),
        f"""target.match_id = source.match_id 
            AND target.master_id = source.master_id 
            AND target.version = source.version
            AND target.action_type = source.action_type"""
    ).whenNotMatchedInsertAll().execute()
    
    logger.info(f"Merge activities logged (atomic transaction)")
else:
    logger.info("  No merge activities to log")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 13: UPDATE UNIFIED TABLE")
logger.info("="*80)

# Recreate unmerge_with_ids_df for unified table update WITH EXPLICIT SCHEMA
unmerge_contributors_df = spark.createDataFrame(unmerge_contributors_data, schema=unmerge_schema)
unmerge_with_ids_df = unmerge_contributors_df.join(new_ids_df, on=unified_id_key, how="inner")

# Update unified table for unmerged records:
# 1. Update master_lakefusion_id to new lakefusion_id
# 2. Keep record_status as 'MERGED' (still merged, just to different master)
# Note: We don't change status to ACTIVE because they're still merged records,
# just merged to their own individual masters now

unified_updates_df = unmerge_with_ids_df.select(
    col(unified_id_key),
    col("new_lakefusion_id").alias(f"master_{id_key}")
)

logger.info(f"  Prepared {unified_updates_df.count()} records for unified table update")

# Perform merge
unified_delta_table = DeltaTable.forName(spark, unified_table)

unified_delta_table.alias("target").merge(
    unified_updates_df.alias("source"),
    f"target.{unified_id_key} = source.{unified_id_key}"
).whenMatchedUpdate(set={
    f"master_{id_key}": f"source.master_{id_key}"
}).execute()

logger.info(f"Unified table updated")
logger.info(f"  Records updated: {unified_updates_df.count()}")

# COMMAND ----------

# Write steward decision with frozen snapshots to Delta table
try:
    import uuid as _uuid
    _sd_table = f"{catalog_name}.silver.{entity}_steward_decisions"
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {_sd_table} (
        decision_id STRING, entity_id STRING, action_type STRING, master_id STRING, match_id STRING,
        reason STRING, reason_category STRING, master_record_attributes STRING, match_record_attributes STRING,
        master_record_source STRING, match_record_source STRING, steward STRING, decided_at TIMESTAMP, aml_status STRING
    ) USING DELTA""")
    _sd_match_source = _sd_match_attrs.get("source_path", "") if _sd_match_attrs else ""
    _sd_to_str = lambda d: {k: str(v) if v is not None else None for k, v in d.items()}
    _sql_str = lambda v: "'" + str(v).replace("'", "''") + "'" if v is not None else 'NULL'
    _sd_m_json = json.dumps(_sd_to_str(_sd_master_attrs)) if _sd_master_attrs else None
    _sd_r_json = json.dumps(_sd_to_str(_sd_match_attrs)) if _sd_match_attrs else None
    spark.sql(f"""INSERT INTO {_sd_table} VALUES (
        '{str(_uuid.uuid4()).replace("-","")}', '', 'MANUAL_UNMERGE', '{master_id}', '{_sd_match_id or ""}',
        NULL, NULL, {_sql_str(_sd_m_json) if _sd_m_json else 'NULL'}, {_sql_str(_sd_r_json) if _sd_r_json else 'NULL'},
        NULL, {_sql_str(_sd_match_source) if _sd_match_source else 'NULL'}, '', current_timestamp(), 'PENDING'
    )""")
    logger.info(f"Steward decision written for MANUAL_UNMERGE master={master_id}")
except Exception as e:
    logger.warning(f"Could not write steward decision: {e}")

# COMMAND ----------

# --- Update Golden Dedup Health ---
logger.info("\n" + "="*80)
logger.info("STEP: UPDATE GOLDEN DEDUP HEALTH")
logger.info("="*80)

try:
    _gd_master_table = f"{catalog_name}.gold.{entity}_master"
    _gd_unified_table = f"{catalog_name}.silver.{entity}_unified"
    _gd_pm_dedup_table = f"{catalog_name}.gold.{entity}_master_potential_match_deduplicate"
    if experiment_id:
        _gd_master_table += f"_{experiment_id}"
        _gd_unified_table += f"_{experiment_id}"
        _gd_pm_dedup_table += f"_{experiment_id}"
    _gd_ma_table = f"{_gd_master_table}_merge_activities"

    if spark.catalog.tableExists(_gd_pm_dedup_table):
        _gd_sk_list_sql = ", ".join(f"'{sk}'" for sk in unmerge_surrogate_keys)
        _gd_affected = spark.sql(f"""
            SELECT DISTINCT ma.master_id AS original_master
            FROM {_gd_ma_table} ma
            WHERE ma.match_id IN ({_gd_sk_list_sql})
              AND ma.action_type IN ('JOB_INSERT','JOB_MERGE','MANUAL_MERGE','INITIAL_LOAD')
              AND ma.master_id != '{master_id}'
              AND ma.master_id NOT IN (SELECT lakefusion_id FROM {_gd_master_table})
        """).collect()
        _gd_affected_masters = [r['original_master'] for r in _gd_affected]

        if not _gd_affected_masters:
            logger.info("  No affected merged masters found in golden dedup, skipping")
        else:
            logger.info(f"  Affected merged masters: {_gd_affected_masters}")

            for _gd_merged_id in _gd_affected_masters:
                _gd_health = spark.sql(f"""
                    WITH source_records AS (
                        SELECT DISTINCT ma.match_id AS source_sk
                        FROM {_gd_ma_table} ma
                        WHERE ma.master_id = '{_gd_merged_id}'
                          AND ma.action_type IN ('JOB_INSERT','JOB_MERGE','MANUAL_MERGE','INITIAL_LOAD')
                          AND ma.match_id LIKE 'sk_%'
                    )
                    SELECT
                        COUNT(DISTINCT sr.source_sk) AS total,
                        COUNT(DISTINCT CASE
                            WHEN u.master_lakefusion_id = '{master_id}' AND u.record_status != 'DELETED'
                            THEN sr.source_sk END) AS remaining
                    FROM source_records sr
                    LEFT JOIN {_gd_unified_table} u ON u.{unified_id_key} = sr.source_sk
                """).collect()[0]
                _gd_remaining = _gd_health['remaining']
                _gd_total = _gd_health['total']
                logger.info(f"  Master {_gd_merged_id}: remaining={_gd_remaining}, total={_gd_total}")

                if _gd_total == 0:
                    logger.info(f"  No source records found for {_gd_merged_id}, skipping")
                    continue
                elif _gd_remaining == 0:
                    logger.info(f"  Fully unmerged — removing {_gd_merged_id}")
                    spark.sql(f"""
                        UPDATE {_gd_pm_dedup_table}
                        SET potential_matches = FILTER(potential_matches, x -> x.lakefusion_id != '{_gd_merged_id}')
                        WHERE lakefusion_id = '{master_id}'
                    """)
                elif _gd_remaining < _gd_total:
                    logger.info(f"  Partially merged — updating {_gd_merged_id} to MASTER_PARTIAL_MERGE ({_gd_remaining}/{_gd_total})")
                    _gd_struct_parts = []
                    for _gd_attr in entity_attributes:
                        _gd_struct_parts.append(f"'{_gd_attr}', x.{_gd_attr}")
                    _gd_struct_parts.extend([
                        "'__score__', x.__score__",
                        "'__reason__', x.__reason__",
                        "'lakefusion_id', x.lakefusion_id",
                        "'__mergestatus__', 'MASTER_PARTIAL_MERGE'",
                        f"'__merge_remaining__', CAST({_gd_remaining} AS STRING)",
                        f"'__merge_total__', CAST({_gd_total} AS STRING)",
                        "'__attributes_combined__', x.__attributes_combined__",
                    ])
                    _gd_ns = f"named_struct({', '.join(_gd_struct_parts)})"
                    spark.sql(f"""
                        UPDATE {_gd_pm_dedup_table}
                        SET potential_matches = TRANSFORM(potential_matches, x ->
                            IF(x.lakefusion_id = '{_gd_merged_id}', {_gd_ns}, x))
                        WHERE lakefusion_id = '{master_id}'
                    """)
                else:
                    logger.info(f"  Still fully merged ({_gd_remaining}/{_gd_total}), no update needed")

            logger.info("  Golden dedup health update complete")
    else:
        logger.info("  Golden dedup table does not exist, skipping")
except Exception as e:
    logger.warning(f"Golden dedup health update failed (non-fatal): {e}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("MANUAL UNMERGE PROCESSING COMPLETE")
logger.info("="*80)

summary = {
    "status": "success",
    "original_master_id": master_id,
    "records_unmerged": unmerge_count,
    "remaining_contributors": remaining_count,
    "new_masters_created": unmerge_count,
    "new_lakefusion_ids": list(new_lakefusion_ids.values()),
    "master_version": int(new_master_version),
    "timestamp": datetime.now().isoformat()
}

logger.info(f"\nSummary:")
logger.info(f"  Original master ID: {summary['original_master_id']}")
logger.info(f"  Records unmerged: {summary['records_unmerged']}")
logger.info(f"  Remaining contributors to original master: {summary['remaining_contributors']}")
logger.info(f"  New individual masters created: {summary['new_masters_created']}")
logger.info(f"  Master table version: {summary['master_version']}")
logger.info(f"\n  New lakefusion IDs created:")
for sk, lf_id in new_lakefusion_ids.items():
    logger.info(f"    {sk} -> {lf_id}")
