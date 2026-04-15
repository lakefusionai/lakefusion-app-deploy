# Databricks notebook source
import json
from datetime import datetime
from pyspark.sql.functions import (
    col, explode, lit, collect_list, first, struct, count,
    current_timestamp, udf, concat_ws, coalesce
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
dbutils.widgets.text("master_id", "", "Master Record ID to merge into")
dbutils.widgets.text("unified_dataset_ids", "", "Unified Record IDs with Dataset IDs (JSON)")
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
master_id = dbutils.widgets.get("master_id")
unified_dataset_ids = dbutils.widgets.get("unified_dataset_ids")

# COMMAND ----------

entity_attributes = json.loads(entity_attributes)
entity_attributes_datatype = json.loads(entity_attributes_datatype)
default_survivorship_rules = json.loads(default_survivorship_rules)
dataset_objects = json.loads(dataset_objects)
unified_dataset_ids = json.loads(unified_dataset_ids)
match_attributes = json.loads(match_attributes)

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

unified_record_ids = []
dataset_ids = []
for unified_dataset_id in unified_dataset_ids:
    unified_record_ids.append(unified_dataset_id.get("id"))
    dataset_ids.append(unified_dataset_id.get("dataset_id"))

logger.info(f"Records to merge: {len(unified_record_ids)}")
logger.info(f"Surrogate keys: {unified_record_ids}")
logger.info(f"Dataset IDs: {dataset_ids}")

# COMMAND ----------

id_key = 'lakefusion_id'
unified_id_key = 'surrogate_key'

# COMMAND ----------

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
logger.info("MANUAL MERGE PROCESSING")
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
logger.info(f"Records to merge: {len(unified_record_ids)}")
logger.info("="*80)

# COMMAND ----------

from lakefusion_core_engine.survivorship import SurvivorshipEngine, __version__

logger.info(f"Survivorship Engine Version: {__version__}")

# COMMAND ----------

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
logger.info("STEP 2: FETCH EXISTING CONTRIBUTORS")
logger.info("="*80)

# Build attribute columns dynamically
attribute_cols = ", ".join([f"u.{attr}" for attr in entity_attributes])

# Get existing MERGED contributors for this master
existing_contributors_query = f"""
SELECT
    u.{unified_id_key},
    u.source_path,
    {attribute_cols}
FROM {unified_table} u
WHERE u.master_{id_key} = '{master_id}'
    AND u.record_status = 'MERGED'
"""

existing_contributors_df = spark.sql(existing_contributors_query)
existing_count = existing_contributors_df.count()

logger.info(f"Existing contributors found: {existing_count}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 3: FETCH NEW RECORDS TO MERGE")
logger.info("="*80)

# Build filter for new records using surrogate keys
surrogate_keys_list = ", ".join([f"'{sk}'" for sk in unified_record_ids])

new_contributors_query = f"""
SELECT
    u.{unified_id_key},
    u.source_path,
    {attribute_cols}
FROM {unified_table} u
WHERE u.{unified_id_key} IN ({surrogate_keys_list})
    AND u.record_status = 'ACTIVE'
"""

new_contributors_df = spark.sql(new_contributors_query)
new_count = new_contributors_df.count()

logger.info(f"New records to merge: {new_count}")

# Validate all requested records were found
if new_count != len(unified_record_ids):
    missing_count = len(unified_record_ids) - new_count
    logger.warning(f"{missing_count} requested records not found or not in ACTIVE status")

    # Show which records were found
    found_keys = [row[unified_id_key] for row in new_contributors_df.select(unified_id_key).collect()]
    missing_keys = [sk for sk in unified_record_ids if sk not in found_keys]
    logger.warning(f"  Missing keys: {missing_keys}")

if new_count == 0:
    error_msg = "No ACTIVE records found to merge"
    logger.error(error_msg)
    dbutils.notebook.exit(json.dumps({
        "status": "error",
        "message": error_msg
    }))

# Collect the data to Python and recreate the DataFrame
new_contributors_schema = new_contributors_df.schema
new_contributors_data = new_contributors_df.collect()
logger.info(f"Materialized {len(new_contributors_data)} new contributor records")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 4: COMBINE ALL CONTRIBUTORS")
logger.info("="*80)

# Combine existing and new contributors
all_contributors_df = existing_contributors_df.union(new_contributors_df)
total_count = all_contributors_df.count()

logger.info(f"Total contributors: {total_count}")
logger.info(f"  Existing: {existing_count}")
logger.info(f"  New: {new_count}")

# Show sample
logger.info("\nUnified Records:")
all_contributors_df.select(unified_id_key, "source_path").show(10, truncate=False)

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 5: PREPARE RECORDS FOR SURVIVORSHIP")
logger.info("="*80)

# Group all contributors into a single array for the master
grouped_contributors = (
    all_contributors_df
    .withColumn(id_key, lit(master_id))  # Add master_id for grouping
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

logger.info(f"Records grouped for survivorship")
logger.info(f"  Total records in group: {total_count}")

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

logger.info(f"Survivorship applied")
logger.info(f"  Master records processed: {final_results_df.count()}")

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
logger.info("STEP 9: LOG MANUAL MERGE ACTIVITIES")
logger.info("="*80)

# Recreate new_contributors_df from materialized data to avoid invalidation
new_contributors_df = spark.createDataFrame(new_contributors_data, schema=new_contributors_schema)

# Prepare merge activities for NEW merges only
# Join new contributors with their source paths
merge_activities_df = (
    new_contributors_df
    .select(
        lit(master_id).alias("master_id"),
        col(unified_id_key).alias("match_id"),
        col("source_path").alias("source"),
        lit(new_master_version).alias("version"),
        lit("MANUAL_MERGE").alias("action_type"),
        current_timestamp().alias("created_at")
    )
)

merge_activities_count = merge_activities_df.count()

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
    logger.info(f"  New MANUAL_MERGE activities: {merge_activities_count}")
else:
    logger.info("  No new merge activities to log")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 10: UPDATE UNIFIED TABLE FOR MERGED RECORDS")
logger.info("="*80)

# Recreate new_contributors_df from materialized data to avoid invalidation
new_contributors_df = spark.createDataFrame(new_contributors_data, schema=new_contributors_schema)

# Update unified table for newly merged records:
# 1. Set master_lakefusion_id to the master_id
# 2. Set record_status = 'MERGED'

# Create update dataframe for new merges
unified_updates_df = (
    new_contributors_df
    .select(
        col(unified_id_key),
        lit(master_id).alias(f"master_{id_key}"),
        lit("MERGED").alias("record_status")
    )
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
logger.info("MANUAL MERGE PROCESSING COMPLETE")
logger.info("="*80)

summary = {
    "status": "success",
    "master_id": master_id,
    "records_merged": new_count,
    "existing_contributors": existing_count,
    "total_contributors": total_count,
    "merge_activities_logged": merge_activities_count,
    "master_version": int(new_master_version),
    "timestamp": datetime.now().isoformat()
}

logger.info(f"\nSummary:")
logger.info(f"  Master ID: {summary['master_id']}")
logger.info(f"  New records merged: {summary['records_merged']}")
logger.info(f"  Existing contributors: {summary['existing_contributors']}")
logger.info(f"  Total contributors: {summary['total_contributors']}")
logger.info(f"  Merge activities logged: {summary['merge_activities_logged']}")
logger.info(f"  Master table version: {summary['master_version']}")

# Set task values
dbutils.jobs.taskValues.set("manual_merge_complete", True)
dbutils.jobs.taskValues.set("records_merged", summary['records_merged'])
dbutils.jobs.taskValues.set("master_id", summary['master_id'])
dbutils.jobs.taskValues.set("master_version", summary['master_version'])
