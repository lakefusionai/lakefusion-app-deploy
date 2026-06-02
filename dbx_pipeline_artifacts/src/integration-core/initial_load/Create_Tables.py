# Databricks notebook source
# DBTITLE 1,Import required libraries
import json
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, 
    DoubleType, FloatType, BooleanType, DateType, TimestampType,
    ShortType, ByteType, ArrayType
)
from pyspark.sql.functions import col, lit, current_timestamp
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Get parameters
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("id_key", "", "ID Key")
dbutils.widgets.text("primary_key", "", "Primary Key")
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")
dbutils.widgets.text("entity_attributes_datatype", "", "Entity Attributes Datatype")
dbutils.widgets.text("dataset_tables", "", "Dataset Tables")

# COMMAND ----------

# DBTITLE 1,Get task values from Parse_Entity_Model_JSON
entity = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="entity",
    debugValue=dbutils.widgets.get("entity")
)

catalog_name = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="catalog_name",
    debugValue=dbutils.widgets.get("catalog_name")
)

id_key = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="id_key",
    debugValue=dbutils.widgets.get("id_key")
)

primary_key = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="primary_key",
    debugValue=dbutils.widgets.get("primary_key")
)

entity_attributes = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="entity_attributes",
    debugValue=dbutils.widgets.get("entity_attributes")
)

entity_attributes_datatype = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="entity_attributes_datatype",
    debugValue=dbutils.widgets.get("entity_attributes_datatype")
)

# Full attribute records (name + type + is_array + struct_definition) — used
# to build nested Spark schemas for STRUCT / ARRAY columns.
# Empty list when running against a legacy task that doesn't export the
# records; create_schema_fields then falls back to the scalar path.
entity_attribute_records = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="entity_attribute_records",
    debugValue="[]",
)

dataset_tables = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="dataset_tables",
    debugValue=dbutils.widgets.get("dataset_tables")
)

experiment_id = dbutils.widgets.get("experiment_id")

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

# DBTITLE 1,Parse JSON strings
entity_attributes = json.loads(entity_attributes)
entity_attributes_datatype = json.loads(entity_attributes_datatype)
entity_attribute_records = json.loads(entity_attribute_records) if entity_attribute_records else []
dataset_tables = json.loads(dataset_tables)

# COMMAND ----------

logger.info("="*60)
logger.info("CREATE TABLES")
logger.info("="*60)
logger.info(f"Entity: {entity}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment: {experiment_id if experiment_id else 'prod'}")
logger.info(f"ID Key: {id_key}")
logger.info(f"Primary Key: {primary_key}")
logger.info(f"Attributes: {len(entity_attributes)}")
logger.info(f"Entity Attributes Datatypes:")
for attr, dtype in entity_attributes_datatype.items():
    logger.info(f"  {attr}: {dtype}")
logger.info("="*60)

# COMMAND ----------

# DBTITLE 1,Construct table names with experiment suffix
experiment_suffix = f"_{experiment_id}" if experiment_id else ""

master_table = f"{catalog_name}.gold.{entity}_master{experiment_suffix}"
unified_table = f"{catalog_name}.silver.{entity}_unified{experiment_suffix}"
merge_activities_table = f"{master_table}_merge_activities"
attribute_version_sources_table = f"{master_table}_attribute_version_sources"

logger.info(f"\nTable Names:")
logger.info(f"  Master: {master_table}")
logger.info(f"  Unified: {unified_table}")
logger.info(f"  Merge Activities: {merge_activities_table}")
logger.info(f"  Attribute Version Sources: {attribute_version_sources_table}")

# COMMAND ----------

# Centralized type resolution — supports scalar, STRUCT, and
# ARRAY types. Includes legacy lowercase + uppercase variants for backward
# compatibility with older entity exports.
#
# Locate dbx_pipeline_artifacts/src so `utils.spark_types` resolves when the
# notebook is executed without a wheel install (Databricks repos + local dev).
import os as _os
import sys as _sys
_pp = _os.getcwd().split(_os.sep)
for _i in range(len(_pp) - 1, -1, -1):
    if _pp[_i] == "src":
        _src = _os.sep.join(_pp[: _i + 1])
        if _src not in _sys.path:
            _sys.path.insert(0, _src)
        break

from utils.spark_types import (
    create_schema_fields as _build_schema_fields,
    get_spark_data_type,
)


def create_schema_fields(attributes_list, attributes_datatype_dict, include_lakefusion_id=True):
    """Build StructFields for an entity table.

    Thin wrapper that feeds the centralized util the entity-level
    `entity_attribute_records` so STRUCT and ARRAY columns get resolved to
    proper nested Spark types. Legacy scalar callers see no behavior change.
    """
    return _build_schema_fields(
        attributes_list,
        attributes_datatype_dict,
        include_lakefusion_id=include_lakefusion_id,
        id_key=id_key,
        attributes=entity_attribute_records or None,
    )

def enable_cdf_on_table(table_name):
    """
    Enable Change Data Feed on a table (idempotent).

    Skips the ALTER when CDF is already enabled — every ALTER bumps the
    Delta table version and pollutes history with no-op commits.

    Args:
        table_name: Fully qualified table name
    """
    try:
        props = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()}
        if str(props.get("delta.enableChangeDataFeed", "false")).lower() == "true":
            logger.info(f"  CDF already enabled on {table_name} (skipping)")
            return True
        spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
        logger.info(f"  CDF enabled on {table_name}")
        return True
    except Exception as e:
        logger.warning(f"  Could not enable CDF on {table_name}: {str(e)}")
        return False

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("1. CREATING MASTER TABLE")
logger.info("="*60)

# Drop existing table if exists (fail-safe)
spark.sql(f"DROP TABLE IF EXISTS {master_table}")
logger.info(f"  Dropped existing table (if any)")

# Create schema with EXACT attribute names
master_fields = create_schema_fields(entity_attributes, entity_attributes_datatype, include_lakefusion_id=True)

# Add system columns
master_fields.extend([
    StructField("attributes_combined", StringType(), True),
    StructField("attributes_combined_embedding", ArrayType(FloatType()), True),
])

master_schema = StructType(master_fields)

# Create empty DataFrame with schema
master_df = spark.createDataFrame([], master_schema)

# Write as Delta table with CDF enabled
master_df.write \
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("overwrite") \
    .saveAsTable(master_table)

logger.info(f"  Created {master_table}")
logger.info(f"  Attributes: {len(entity_attributes)}")
logger.info(f"  CDF: Enabled")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("2. CREATING UNIFIED TABLE")
logger.info("="*60)

# Drop existing table if exists (fail-safe)
spark.sql(f"DROP TABLE IF EXISTS {unified_table}")
logger.info(f"  Dropped existing table (if any)")

# Create schema
unified_fields = [
    StructField("surrogate_key", StringType(), True),
    StructField("source_path", StringType(), True),
    StructField("source_id", StringType(), True),
    StructField("master_lakefusion_id", StringType(), True),  # NEW COLUMN
    StructField("record_status", StringType(), True),
    StructField("attributes_combined", StringType(), True),
    StructField("search_results", StringType(), True),
    StructField("scoring_results", StringType(), True),
    # Reserved system audit columns. Double-underscore prefix avoids
    # collision with user-defined entity attributes named `created_at` /
    # `modified_at`. Used by the survivorship engine as a deterministic
    # tie-breaker (latest record wins).
    StructField("__lf_created_at", TimestampType(), True),
    StructField("__lf_modified_at", TimestampType(), True),
]

# Add all entity attributes (NO lakefusion_id in unified). Delegate to the
# centralized helper so STRUCT and ARRAY columns get nested Spark types
# instead of the StringType fallback. include_lakefusion_id=False because
# unified rows are keyed on surrogate_key + master_lakefusion_id, not lakefusion_id.
unified_attr_fields = create_schema_fields(
    entity_attributes,
    entity_attributes_datatype if isinstance(entity_attributes_datatype, dict) else {},
    include_lakefusion_id=False,
)
unified_fields.extend(unified_attr_fields)


unified_schema = StructType(unified_fields)

# Create empty DataFrame with schema
unified_df = spark.createDataFrame([], unified_schema)

# Write as Delta table with CDF enabled
unified_df.write \
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("overwrite") \
    .saveAsTable(unified_table)

logger.info(f"  Created {unified_table}")
logger.info(f"  Attributes: {len([a for a in entity_attributes if a != id_key])}")
logger.info(f"  Added master_lakefusion_id column")
logger.info(f"  CDF: Enabled")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("3. CREATING MERGE ACTIVITIES TABLE")
logger.info("="*60)

# Drop existing table if exists (fail-safe)
spark.sql(f"DROP TABLE IF EXISTS {merge_activities_table}")
logger.info(f"  Dropped existing table (if any)")

# Create schema - log table with single timestamp
merge_activities_schema = StructType([
    StructField("master_id", StringType(), True),
    StructField("match_id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("version", IntegerType(), True),
    StructField("action_type", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

# Create empty DataFrame
merge_activities_df = spark.createDataFrame([], merge_activities_schema)

# Write as Delta table (NO CDF - it's a log table)
merge_activities_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(merge_activities_table)

logger.info(f"  Created {merge_activities_table}")
logger.info(f"  Log table: Insert-only (no CDF)")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("4. CREATING ATTRIBUTE VERSION SOURCES TABLE")
logger.info("="*60)

# Drop existing table if exists (fail-safe)
spark.sql(f"DROP TABLE IF EXISTS {attribute_version_sources_table}")
logger.info(f"  Dropped existing table (if any)")

# Create schema - log table, no timestamps needed
attribute_version_sources_schema = StructType([
    StructField(id_key, StringType(), True),
    StructField("version", IntegerType(), True),
    StructField("attribute_source_mapping", ArrayType(StructType([
        StructField("attribute_name", StringType(), True),
        StructField("attribute_value", StringType(), True),
        StructField("source", StringType(), True)
    ])), True)
])

# Create empty DataFrame
attr_version_df = spark.createDataFrame([], attribute_version_sources_schema)

# Write as Delta table (NO CDF - version tracking table)
attr_version_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(attribute_version_sources_table)

logger.info(f"  Created {attribute_version_sources_table}")
logger.info(f"  Version tracking table (no timestamps)")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("5. ENABLING CDF ON SOURCE TABLES")
logger.info("="*60)

cdf_enabled_count = 0
cdf_failed_count = 0

for source_table in dataset_tables:
    # Check if table exists before trying to enable CDF
    if spark.catalog.tableExists(source_table):
        if enable_cdf_on_table(source_table):
            cdf_enabled_count += 1
        else:
            cdf_failed_count += 1
    else:
        logger.warning(f"  Table does not exist: {source_table}")
        cdf_failed_count += 1

logger.info(f"\nCDF enabled on {cdf_enabled_count}/{len(dataset_tables)} source tables")
if cdf_failed_count > 0:
    logger.warning(f"{cdf_failed_count} tables failed or don't exist")
    

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("TABLE CREATION VERIFICATION")
logger.info("="*60)

tables_to_verify = [
    ("Master", master_table),
    ("Unified", unified_table),
    ("Merge Activities", merge_activities_table),
    ("Attribute Version Sources", attribute_version_sources_table)
]

all_created = True
for name, table in tables_to_verify:
    exists = spark.catalog.tableExists(table)
    if exists:
        logger.info(f"{name}: {table}")
    else:
        logger.error(f"{name}: {table}")
    if not exists:
        all_created = False

if not all_created:
    raise Exception("Not all tables were created successfully!")

logger.info("\nAll 4 tables created successfully!")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("TABLE SCHEMAS")
logger.info("="*60)

tables_to_show = [
    ("Master", master_table),
    ("Unified", unified_table),
    ("Merge Activities", merge_activities_table),
    ("Attribute Version Sources", attribute_version_sources_table)
]

for name, table in tables_to_show:
    if spark.catalog.tableExists(table):
        logger.info(f"\n{name} Table: {table}")
        logger.info("-" * 60)
        schema = spark.table(table).schema
        for field in schema:
            logger.info(f"  {field.name:30s} : {field.dataType}")
    else:
        logger.info(f"\n{name} Table: {table}")
        logger.info("-" * 60)
        logger.warning(f"  Table does not exist")

logger.info("\n" + "="*60)

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("CREATE TABLES - COMPLETE")
logger.info("="*60)
logger.info(f"Entity: {entity}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment: {experiment_id if experiment_id else 'prod'}")
logger.info(f"Tables Created: 4")
logger.info(f"Attributes: {len(entity_attributes)}")
logger.info("="*60)
logger.info("\nNext: Load Primary Source")
logger.info("="*60)

# COMMAND ----------

logger_instance.shutdown()
