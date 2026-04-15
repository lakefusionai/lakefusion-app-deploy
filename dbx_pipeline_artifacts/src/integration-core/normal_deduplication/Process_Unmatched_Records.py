# Databricks notebook source
import json
from datetime import datetime
from pyspark.sql.functions import (
    col, lit, current_timestamp, udf, array, struct
)
from pyspark.sql.types import StringType
from delta.tables import DeltaTable


# COMMAND ----------

dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")
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
match_attributes = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="match_attributes",
    debugValue=dbutils.widgets.get("match_attributes")
)

# COMMAND ----------

# Parse JSON parameters
entity_attributes = json.loads(entity_attributes)
match_attributes = json.loads(match_attributes)

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

id_key = 'lakefusion_id'
unified_id_key = 'surrogate_key'

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

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

from lakefusion_core_engine.identifiers import generate_lakefusion_id
from lakefusion_core_engine.models import ActionType

logger.info("✓ Lakefusion core engine loaded")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 1: FIND UNMATCHED RECORDS")
logger.info("="*80)

#   - INNER JOIN to processed_unified so we ONLY consider records that were scored
#   - Among those, find records where NONE of their scoring results were MATCH or POTENTIAL_MATCH

unmatched_query = f"""
SELECT u.*
FROM {unified_table} u
INNER JOIN (
    SELECT {unified_id_key}
    FROM {processed_unified_table}
    GROUP BY {unified_id_key}
    HAVING MAX(CASE WHEN exploded_result.match IN ('MATCH', 'POTENTIAL_MATCH') THEN 1 ELSE 0 END) = 0
) scored_no_match
    ON u.{unified_id_key} = scored_no_match.{unified_id_key}
WHERE u.record_status = 'ACTIVE'
"""

unmatched_df = spark.sql(unmatched_query)

if unmatched_df.isEmpty():
    logger.info("No unmatched scored records to process")
    dbutils.notebook.exit(json.dumps({
        "status": "success",
        "message": "No unmatched scored records to process",
        "records_inserted": 0
    }))

logger.info("Found unmatched ACTIVE records that were scored but had no MATCH or POTENTIAL_MATCH")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 2: GENERATE LAKEFUSION IDS")
logger.info("="*80)

# Create UDF for lakefusion_id generation using core engine function
# This generates deterministic IDs based on source_path and source_id
generate_lakefusion_id_udf = udf(
    lambda source_path, source_id: generate_lakefusion_id(source_path, str(source_id)),
    StringType()
)

# Add lakefusion_id column using source_path and source_id from the record
unmatched_with_id_df = unmatched_df.withColumn(
    id_key,
    generate_lakefusion_id_udf(col("source_path"), col("source_id"))
)

logger.info("Generated lakefusion_ids for unmatched records")
logger.info("IDs are deterministic - same source_path + source_id = same lakefusion_id")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 3: PREPARE MASTER RECORDS")
logger.info("="*80)

from pyspark.sql.functions import concat_ws

# Select entity attributes plus lakefusion_id
master_columns = [id_key] + [attr for attr in entity_attributes if attr != id_key]

# Create attributes_combined by concatenating all attributes (except lakefusion_id)
combine_attrs = [attr for attr in match_attributes]

new_masters_df = unmatched_with_id_df.select(*master_columns) \
    .withColumn(
        "attributes_combined",
        concat_ws(" | ", *[col(attr) for attr in combine_attrs])
    )

logger.info("Prepared new master records")
logger.info(f"  Columns: {', '.join(master_columns + ['attributes_combined'])}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 4: INSERT INTO MASTER TABLE")
logger.info("="*80)

# Get current master table version before insert
current_master_version = spark.sql(
    f"DESCRIBE HISTORY {master_table} LIMIT 1"
).select("version").collect()[0][0]

logger.info(f"  Current master table version: {current_master_version}")

# Check for existing lakefusion_ids to avoid duplicates
existing_ids = spark.sql(f"""
    SELECT {id_key}
    FROM {master_table}
""").select(id_key)

# Filter out records that already exist
new_masters_to_insert = new_masters_df.join(
    existing_ids,
    on=id_key,
    how="left_anti"
)

if new_masters_to_insert.isEmpty():
    logger.info("All unmatched records already exist in master table — skipping insert")
    dbutils.notebook.exit(json.dumps({
        "status": "success",
        "message": "No new records to insert - all already exist",
        "records_inserted": 0
    }))

# Insert only new masters
new_masters_to_insert.write.mode("append").saveAsTable(master_table)

# Get new version
new_master_version = spark.sql(
    f"DESCRIBE HISTORY {master_table} LIMIT 1"
).select("version").collect()[0][0]

logger.info(f"Inserted new master records")
logger.info(f"  New master table version: {new_master_version}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 5: COLLECT DATA BEFORE UNIFIED UPDATE")
logger.info("="*80)

# CRITICAL FOR SERVERLESS: Collect data into memory BEFORE updating unified table
# This prevents DataFrame invalidation when unified table changes

id_mapping_df_select = unmatched_with_id_df.select(
    col(id_key),
    col(unified_id_key),
    col("source_path"),
    col("source_id"),
    *[col(attr) for attr in entity_attributes if attr != id_key]
)

# Collect the data into Python list (materializes in driver memory)
id_mapping_data = id_mapping_df_select.collect()
id_mapping_schema = id_mapping_df_select.schema

logger.info(f"Collected {len(id_mapping_data)} records into memory")
logger.info("Schema preserved for recreating DataFrames")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 6: UPDATE UNIFIED TABLE")
logger.info("="*80)

# Recreate DataFrame from collected data
id_mapping_df_for_unified = spark.createDataFrame(id_mapping_data, id_mapping_schema)

# Prepare updates for unified table
unified_updates_df = id_mapping_df_for_unified.select(
    col(unified_id_key),
    col(id_key).alias(f"master_{id_key}"),
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

logger.info("Updated unified table — set master_lakefusion_id and record_status = MERGED")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 7: PREPARE ATTRIBUTE VERSION SOURCES")
logger.info("="*80)

# Recreate DataFrame from collected data
id_mapping_df_for_attrs = spark.createDataFrame(id_mapping_data, id_mapping_schema)

# Build attribute source mapping
attribute_mappings = []

for attr in entity_attributes:
    if attr != id_key:
        attribute_mappings.append(
            struct(
                lit(attr).alias("attribute_name"),
                col(attr).cast("string").alias("attribute_value"),
                col("source_path").alias("source")
            )
        )

# Create attribute_source_mapping array
attr_sources_df = id_mapping_df_for_attrs.select(
    col(id_key),
    lit(new_master_version).alias("version"),
    array(*attribute_mappings).alias("attribute_source_mapping")
)

logger.info("Prepared attribute version sources")
logger.info(f"  Attributes per record: {len(entity_attributes) - 1}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 8: INSERT INTO ATTRIBUTE VERSION SOURCES")
logger.info("="*80)

# Insert attribute version sources
attr_sources_df.write.mode("append").saveAsTable(attribute_version_sources_table)

logger.info("Inserted attribute version sources")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 9: LOG MERGE ACTIVITIES")
logger.info("="*80)

# Recreate DataFrame from collected data
id_mapping_df_for_activities = spark.createDataFrame(id_mapping_data, id_mapping_schema)

# Prepare merge activities records
merge_activities_df = id_mapping_df_for_activities.select(
    col(id_key).alias("master_id"),
    col(unified_id_key).alias("match_id"),
    col("source_path").alias("source"),
    lit(new_master_version).alias("version"),
    lit("JOB_INSERT").alias("action_type"),
    current_timestamp().alias("created_at")
)

# Insert merge activities
merge_activities_df.write.mode("append").saveAsTable(merge_activities_table)

logger.info("Logged merge activities — action_type: JOB_INSERT")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("UNMATCHED RECORDS PROCESSING COMPLETE")
logger.info("="*80)

records_processed = len(id_mapping_data)

summary = {
    "status": "success",
    "records_processed": records_processed,
    "master_version": int(new_master_version),
    "timestamp": datetime.now().isoformat()
}

logger.info(f"  Records processed: {summary['records_processed']}")
logger.info(f"  Master table version: {summary['master_version']}")
logger.info(f"  Unified table updated")
logger.info(f"  Attribute version sources logged")
logger.info(f"  Merge activities logged (JOB_INSERT)")

# Set task values
dbutils.jobs.taskValues.set("unmatched_complete", True)
dbutils.jobs.taskValues.set("records_processed", summary['records_processed'])
