# Databricks notebook source
import json
from datetime import datetime
from pyspark.sql.functions import (
    col, explode, lit, collect_list, first, struct, count,
    current_timestamp, udf, concat_ws, coalesce, collect_set
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
dbutils.widgets.text("master_id", "", "Master Record ID")
dbutils.widgets.text("match_record_id", "", "Match Record ID to merge into master")
dbutils.widgets.text("match_attributes", "", "Match Attributes")
dbutils.widgets.text("operation_type", "", "Action Type")

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
match_record_id = dbutils.widgets.get("match_record_id")
action_type = dbutils.widgets.get("operation_type")

# COMMAND ----------

entity_attributes = json.loads(entity_attributes)
entity_attributes_datatype = json.loads(entity_attributes_datatype)
default_survivorship_rules = json.loads(default_survivorship_rules)
dataset_objects = json.loads(dataset_objects)
match_attributes = json.loads(match_attributes)

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

logger.info(f"Master to keep: {master_id}")
logger.info(f"Match record to merge: {match_record_id}")

# COMMAND ----------

id_key = 'lakefusion_id'
unified_id_key = 'surrogate_key'

# COMMAND ----------

unified_table = f"{catalog_name}.silver.{entity}_unified"
master_table = f"{catalog_name}.gold.{entity}_master"
unified_dedup_table = f"{catalog_name}.silver.{entity}_unified_deduplicate"

# Add experiment suffix if exists
if experiment_id:
    unified_table += f"_{experiment_id}"
    master_table += f"_{experiment_id}"
    unified_dedup_table += f"_{experiment_id}"

# Related tables
merge_activities_table = f"{master_table}_merge_activities"
attribute_version_sources_table = f"{master_table}_attribute_version_sources"

# COMMAND ----------

logger.info("="*80)
logger.info("GOLDEN DEDUP MANUAL MERGE")
logger.info("="*80)
logger.info(f"Entity: {entity}")
logger.info(f"Master ID Key: {id_key}")
logger.info(f"Unified ID Key: {unified_id_key}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment ID: {experiment_id or 'None'}")
logger.info(f"Unified Table: {unified_table}")
logger.info(f"Master Table: {master_table}")
logger.info(f"Unified Dedup Table: {unified_dedup_table}")
logger.info(f"Merge Activities Table: {merge_activities_table}")
logger.info(f"Attribute Version Sources Table: {attribute_version_sources_table}")
logger.info(f"\nMerge Operation:")
logger.info(f"  Keeping master: {master_id}")
logger.info(f"  Merging match: {match_record_id}")
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
logger.info("STEP 1: VALIDATE INPUT MASTERS")
logger.info("="*80)

# Validate that both master_id and match_record_id exist in master table
validation_query = f"""
    SELECT {id_key}, COUNT(*) as cnt
    FROM {master_table}
    WHERE {id_key} IN ('{master_id}', '{match_record_id}')
    GROUP BY {id_key}
"""

validation_result = spark.sql(validation_query).collect()

if len(validation_result) != 2:
    existing_ids = [row[id_key] for row in validation_result]
    if master_id not in existing_ids:
        raise ValueError(f"Master ID {master_id} does not exist in master table")
    if match_record_id not in existing_ids:
        raise ValueError(f"Match record ID {match_record_id} does not exist in master table")

logger.info(f"Master {master_id} exists")
logger.info(f"Match record {match_record_id} exists")
logger.info(f"Both records validated for merge")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 2: GET ALL CONTRIBUTORS FOR MASTER AND MATCH")
logger.info("="*80)

# Get all contributors for BOTH the master we're keeping AND the match record
all_contributors_query = f"""
SELECT
    u.master_{id_key},
    u.{unified_id_key},
    u.source_path,
    {', '.join([f'u.{attr}' for attr in entity_attributes])}
FROM {unified_table} u
WHERE u.master_{id_key} IN ('{master_id}', '{match_record_id}')
    AND u.record_status != 'DELETED'
ORDER BY u.master_{id_key}, u.{unified_id_key}
"""

df_all_contributors = spark.sql(all_contributors_query)
total_contributors = df_all_contributors.count()

logger.info(f"Found {total_contributors} total contributors")

# Show breakdown by master
contributors_by_master = df_all_contributors.groupBy(f"master_{id_key}").count()
logger.info(f"\nContributors by master:")
contributors_by_master.orderBy(col("count").desc()).show(truncate=False)

# Get individual counts
master_contrib_count = df_all_contributors.filter(col(f"master_{id_key}") == master_id).count()
match_contrib_count = df_all_contributors.filter(col(f"master_{id_key}") == match_record_id).count()

logger.info(f"\n  Master {master_id}: {master_contrib_count} contributors")
logger.info(f"  Match {match_record_id}: {match_contrib_count} contributors")
logger.info(f"  Total to process: {total_contributors} contributors")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 3: PREPARE UNIFIED RECORDS FOR SURVIVORSHIP")
logger.info("="*80)

# Group all contributors together (they will all contribute to the final master)
grouped_contributors = (
    df_all_contributors
    .groupBy(lit(master_id).alias(id_key))  # All go to the master we're keeping
    .agg(
        collect_list(
            struct([col(c) for c in df_all_contributors.columns if c != f"master_{id_key}"])
        ).alias("unified_records")
    )
)

grouped_count = grouped_contributors.count()
logger.info(f"Grouped {total_contributors} contributors into 1 master record")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 4: APPLY SURVIVORSHIP ENGINE")
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

logger.info(f"Survivorship applied to master {master_id}")

# COMMAND ----------

final_results_df.display()

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 5: UPDATE MASTER TABLE")
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

logger.info(f"  Prepared update for master {master_id}")

# Update master table using Delta merge
master_delta = DeltaTable.forName(spark, master_table)

master_delta.alias("target").merge(
    source=master_updates_df.alias("source"),
    condition=f"target.{id_key} = source.{id_key}"
).whenMatchedUpdateAll().execute()

logger.info(f"Updated master {master_id} with merged attributes")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 6: DELETE MATCH RECORD FROM MASTER TABLE")
logger.info("="*80)

# Delete the match record (keep only the master_id)
master_delta = DeltaTable.forName(spark, master_table)

master_delta.delete(f"{id_key} = '{match_record_id}'")

logger.info(f"Deleted match record {match_record_id} from master table")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 7: LOG MERGE ACTIVITY")
logger.info("="*80)

# Get the current version of the master table (after update)
master_table_version = spark.sql(f"DESCRIBE HISTORY {master_table} LIMIT 1").collect()[0]["version"]

logger.info(f"Master table current version: {master_table_version}")

# Prepare merge activity log
merge_activities = [{
    "master_id": master_id,        # The survivor
    "match_id": match_record_id,   # The merged match
    "source": "",                  # Empty string for master-to-master merge
    "version": master_table_version,
    "action_type": action_type,
    "created_at": datetime.now()
}]

# Define schema explicitly to match merge_activities table schema
merge_activities_schema = StructType([
    StructField("master_id", StringType(), True),
    StructField("match_id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("version", IntegerType(), False),
    StructField("action_type", StringType(), False),
    StructField("created_at", TimestampType(), False)
])

# Create DataFrame with explicit schema
df_merge_activities = spark.createDataFrame(merge_activities, schema=merge_activities_schema)

# Append to merge_activities table
df_merge_activities.write.mode("append").saveAsTable(merge_activities_table)

logger.info(f"Logged merge activity with version {master_table_version}")
logger.info(f"  Master: {master_id}")
logger.info(f"  Match: {match_record_id}")
logger.info(f"  Action type: MASTER_MANUAL_MERGE")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 8: UPDATE ATTRIBUTE VERSION SOURCES")
logger.info("="*80)

# Use the same master table version
logger.info(f"Using master table version: {master_table_version}")

# Prepare DataFrame with correct schema
attribute_sources_df = final_results_df.select(
    col(id_key).alias("lakefusion_id"),
    lit(master_table_version).cast("integer").alias("version"),
    col("attribute_sources").alias("attribute_source_mapping")
)

# Append to attribute_version_sources table
attribute_sources_df.write.mode("append").saveAsTable(attribute_version_sources_table)

logger.info(f"Logged attribute version sources for master {master_id} with version {master_table_version}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 9: UPDATE UNIFIED TABLE - REASSIGN ALL CONTRIBUTORS")
logger.info("="*80)

# Update ALL contributors from merged masters to point to the survivor master
# This includes contributors from both the keeping master and merged masters

# Build the update DataFrame
unified_updates_df = df_all_contributors.select(
    col(unified_id_key),
    lit(master_id).alias("new_master_id")
)

updates_count = unified_updates_df.count()

logger.info(f"  Reassigning {updates_count} contributors to master {master_id}")

# Update unified table using Delta merge
unified_delta = DeltaTable.forName(spark, unified_table)

unified_delta.alias("target").merge(
    source=unified_updates_df.alias("source"),
    condition=f"target.{unified_id_key} = source.{unified_id_key}"
).whenMatchedUpdate(
    set={f"master_{id_key}": "source.new_master_id"}
).execute()

logger.info(f"Updated {updates_count} unified records to point to master {master_id}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 10: VERIFY FINAL STATE")
logger.info("="*80)

# Verify master still exists
final_master_check = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM {master_table}
    WHERE {id_key} = '{master_id}'
""").collect()[0]["cnt"]

logger.info(f"  Master {master_id} exists: {final_master_check == 1}")

# Verify match record is deleted
match_deleted_check = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM {master_table}
    WHERE {id_key} = '{match_record_id}'
""").collect()[0]["cnt"]

logger.info(f"  Match {match_record_id} deleted: {match_deleted_check == 0}")

# Verify all contributors point to master
final_contributors = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM {unified_table}
    WHERE master_{id_key} = '{master_id}'
        AND record_status != 'DELETED'
""").collect()[0]["cnt"]

logger.info(f"  Total contributors for master {master_id}: {final_contributors}")

# Verify merge activity logged
final_merge_activity = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM {merge_activities_table}
    WHERE action_type = '{action_type}'
        AND master_id = '{master_id}'
        AND match_id = '{match_record_id}'
        AND version = {master_table_version}
""").collect()[0]["cnt"]

logger.info(f"  Merge activity logged: {final_merge_activity == 1}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("GOLDEN DEDUP MANUAL MERGE COMPLETED SUCCESSFULLY")
logger.info("="*80)
logger.info(f"Master kept: {master_id}")
logger.info(f"Match merged: {match_record_id}")
logger.info(f"Total contributors: {final_contributors}")
logger.info(f"Version: {master_table_version}")
