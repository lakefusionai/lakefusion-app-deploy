# Databricks notebook source
# DBTITLE 1,Import required libraries
import json
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, FloatType, BooleanType, DateType, TimestampType,
    ShortType, ByteType
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
dbutils.widgets.text("is_single_source", "", "Is Single Source")

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

dataset_tables = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="dataset_tables",
    debugValue=dbutils.widgets.get("dataset_tables")
)

is_single_source = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="is_single_source",
    debugValue=dbutils.widgets.get("is_single_source")
)

experiment_id = dbutils.widgets.get("experiment_id")

# COMMAND ----------

# DBTITLE 1,Parse JSON strings
entity_attributes = json.loads(entity_attributes)
entity_attributes_datatype = json.loads(entity_attributes_datatype)
dataset_tables = json.loads(dataset_tables)

# Convert is_single_source to boolean
if isinstance(is_single_source, str):
    is_single_source = is_single_source.lower() == "true"

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

logger.info("="*60)
logger.info("CREATE TABLES - MATCH MAVEN EXPERIMENT")
logger.info("="*60)
logger.info(f"Entity: {entity}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment ID: {experiment_id}")
logger.info(f"ID Key: {id_key}")
logger.info(f"Primary Key: {primary_key}")
logger.info(f"Is Single Source: {is_single_source}")
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
potential_match_table = f"{master_table}_potential_match"

# For single source (golden dedup), we also need unified_deduplicate
unified_dedup_table = f"{catalog_name}.silver.{entity}_unified_deduplicate{experiment_suffix}"

logger.info(f"\nTable Names:")
logger.info(f"  Master: {master_table}")
logger.info(f"  Unified: {unified_table}")
logger.info(f"  Potential Match: {potential_match_table}")
if is_single_source:
    logger.info(f"  Unified Dedup (Golden): {unified_dedup_table}")

# COMMAND ----------

# DBTITLE 1,Helper functions
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

def create_schema_fields(attributes_list, attributes_datatype_dict, include_lakefusion_id=True):
    """
    Create StructFields for a schema.
    
    Args:
        attributes_list: List of attribute names (EXACT names from entity)
        attributes_datatype_dict: Dict mapping attribute name to data type
        include_lakefusion_id: Whether to include lakefusion_id field
        
    Returns:
        List of StructField objects
    """
    fields = []
    
    # Add lakefusion_id if requested (only for master table)
    if include_lakefusion_id:
        fields.append(StructField(id_key, StringType(), True))
    
    # Add all entity attributes with EXACT names
    for attr_name in attributes_list:
        if attr_name == id_key:  # Skip if it's lakefusion_id
            continue
        
        dtype_str = attributes_datatype_dict.get(attr_name, 'string')
        spark_dtype = get_spark_data_type(dtype_str)
        fields.append(StructField(attr_name, spark_dtype, True))
    
    return fields

# COMMAND ----------

# DBTITLE 1,Create Master Table
logger.info("\n" + "="*60)
logger.info("1. CREATING MASTER TABLE")
logger.info("="*60)

# Drop existing table if exists (for experiment - fresh start)
spark.sql(f"DROP TABLE IF EXISTS {master_table}")
logger.info(f"  Dropped existing table (if any)")

# Create schema with EXACT attribute names
master_fields = create_schema_fields(entity_attributes, entity_attributes_datatype, include_lakefusion_id=True)

# Add system columns
master_fields.extend([
    StructField("attributes_combined", StringType(), True)
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

# DBTITLE 1,Create Unified Table (for multi-source only)
if not is_single_source:
    logger.info("\n" + "="*60)
    logger.info("2. CREATING UNIFIED TABLE (Multi-Source)")
    logger.info("="*60)

    # Drop existing table if exists
    spark.sql(f"DROP TABLE IF EXISTS {unified_table}")
    logger.info(f"  Dropped existing table (if any)")
    
    # Create schema - SIMPLIFIED for experiments (no master_lakefusion_id)
    unified_fields = [
        StructField("surrogate_key", StringType(), True),
        StructField("source_path", StringType(), True),
        StructField("source_id", StringType(), True),
        StructField("record_status", StringType(), True)
    ]
    
    # Add all entity attributes (NO lakefusion_id in unified)
    for attr_name in entity_attributes:
        if attr_name == id_key:  # Skip lakefusion_id
            continue
        
        dtype_str = entity_attributes_datatype.get(attr_name, 'string') if isinstance(entity_attributes_datatype, dict) else 'string'
        spark_dtype = get_spark_data_type(dtype_str)
        unified_fields.append(StructField(attr_name, spark_dtype, True))
    
    # Add system columns
    unified_fields.extend([
        StructField("attributes_combined", StringType(), True),
        StructField("search_results", StringType(), True),
        StructField("scoring_results", StringType(), True)
    ])
    
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
    logger.info(f"  SIMPLIFIED: No master_lakefusion_id column (experiment mode)")
    logger.info(f"  CDF: Enabled")
else:
    logger.info("\n" + "="*60)
    logger.info("2. SKIPPING UNIFIED TABLE (Single-Source)")
    logger.info("="*60)
    logger.info(f"  Single-source entity uses unified_deduplicate instead")

# COMMAND ----------

# DBTITLE 1,Create Unified Dedup Table (for single-source golden dedup only)
if is_single_source:
    logger.info("\n" + "="*60)
    logger.info("3. CREATING UNIFIED DEDUP TABLE (Single-Source Golden Dedup)")
    logger.info("="*60)

    # Drop existing table if exists
    spark.sql(f"DROP TABLE IF EXISTS {unified_dedup_table}")
    logger.info(f"  Dropped existing table (if any)")
    
    # Create schema - matches master but with search/scoring results
    unified_dedup_fields = create_schema_fields(entity_attributes, entity_attributes_datatype, include_lakefusion_id=True)
    
    # Add system columns
    unified_dedup_fields.extend([
        StructField("attributes_combined", StringType(), True),
        StructField("search_results", StringType(), True),
        StructField("scoring_results", StringType(), True)
    ])
    
    unified_dedup_schema = StructType(unified_dedup_fields)
    
    # Create empty DataFrame with schema
    unified_dedup_df = spark.createDataFrame([], unified_dedup_schema)
    
    # Write as Delta table
    unified_dedup_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(unified_dedup_table)
    
    logger.info(f"  Created {unified_dedup_table}")
    logger.info(f"  Schema matches master table + search/scoring columns")
    logger.info(f"  Will be populated by cloning master table")
else:
    logger.info("\n" + "="*60)
    logger.info("3. SKIPPING UNIFIED DEDUP TABLE (Multi-Source)")
    logger.info("="*60)
    logger.info(f"  Multi-source entity uses unified table instead")

# COMMAND ----------

# DBTITLE 1,Verification
logger.info("\n" + "="*60)
logger.info("TABLE CREATION VERIFICATION")
logger.info("="*60)

tables_to_verify = [("Master", master_table)]

if is_single_source:
    tables_to_verify.append(("Unified Dedup", unified_dedup_table))
else:
    tables_to_verify.append(("Unified", unified_table))

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

logger.info(f"\nAll required tables created successfully!")
logger.info(f"  Mode: {'Single-Source (Golden Dedup)' if is_single_source else 'Multi-Source'}")

# COMMAND ----------

# DBTITLE 1,Display Table Schemas
logger.info("\n" + "="*60)
logger.info("TABLE SCHEMAS")
logger.info("="*60)

tables_to_show = [("Master", master_table)]

if is_single_source:
    tables_to_show.append(("Unified Dedup", unified_dedup_table))
else:
    tables_to_show.append(("Unified", unified_table))

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

# DBTITLE 1,Set Task Values
dbutils.jobs.taskValues.set("tables_created", True)
dbutils.jobs.taskValues.set("master_table", master_table)
dbutils.jobs.taskValues.set("unified_table", unified_table if not is_single_source else "")
dbutils.jobs.taskValues.set("unified_dedup_table", unified_dedup_table if is_single_source else "")
dbutils.jobs.taskValues.set("potential_match_table", potential_match_table)

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("CREATE TABLES EXPERIMENT - COMPLETE")
logger.info("="*60)
logger.info(f"Entity: {entity}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment ID: {experiment_id}")
logger.info(f"Mode: {'Single-Source (Golden Dedup)' if is_single_source else 'Multi-Source'}")
logger.info(f"Tables Created:")
logger.info(f"  - Master: {master_table}")
if is_single_source:
    logger.info(f"  - Unified Dedup: {unified_dedup_table}")
else:
    logger.info(f"  - Unified: {unified_table}")
logger.info(f"\nNOTE: No merge_activities or attribute_version_sources tables")
logger.info(f"      (Experiment mode - no merging)")
logger.info("="*60)
logger.info("\nNext: Load Experiment Data")
logger.info("="*60)

# COMMAND ----------

logger_instance.shutdown()
