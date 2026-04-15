# Databricks notebook source
# DBTITLE 1,Import required libraries
import json
from pyspark.sql.functions import col, lit, udf, concat_ws, current_timestamp, struct
from pyspark.sql.types import StringType, IntegerType, LongType, DoubleType, FloatType, BooleanType, DateType, TimestampType, ArrayType, StructType, StructField, ShortType, ByteType
from delta.tables import DeltaTable



# COMMAND ----------

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

# DBTITLE 1,Get parameters
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("primary_table", "", "Primary Table")
dbutils.widgets.text("primary_key", "", "Primary Key")
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")
dbutils.widgets.text("entity_attributes_datatype", "", "Entity Attributes Datatype")
dbutils.widgets.text("match_attributes", "", "Match Attributes")
dbutils.widgets.text("attributes_mapping", "", "Attributes Mapping")

# COMMAND ----------

# DBTITLE 1,Get task values
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

primary_table = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="primary_table",
    debugValue=dbutils.widgets.get("primary_table")
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

match_attributes = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="match_attributes",
    debugValue=dbutils.widgets.get("match_attributes")
)

attributes_mapping = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="attributes_mapping",
    debugValue=dbutils.widgets.get("attributes_mapping")
)

experiment_id = dbutils.widgets.get("experiment_id")

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

# DBTITLE 1,Import core engine components
from lakefusion_core_engine.identifiers import generate_lakefusion_id, generate_surrogate_key
from lakefusion_core_engine.models import RecordStatus, ActionType

# COMMAND ----------

# DBTITLE 1,Parse JSON strings
entity_attributes = json.loads(entity_attributes)
entity_attributes_datatype = json.loads(entity_attributes_datatype)
match_attributes = json.loads(match_attributes)
attributes_mapping = json.loads(attributes_mapping)

# COMMAND ----------

# DBTITLE 1,Construct table names
experiment_suffix = f"_{experiment_id}" if experiment_id else ""

master_table = f"{catalog_name}.gold.{entity}_master{experiment_suffix}"
unified_table = f"{catalog_name}.silver.{entity}_unified{experiment_suffix}"
merge_activities_table = f"{master_table}_merge_activities"
attribute_version_sources_table = f"{master_table}_attribute_version_sources"

# COMMAND ----------

logger.info("="*60)
logger.info("LOAD PRIMARY SOURCE")
logger.info("="*60)
logger.info(f"Entity: {entity}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment: {experiment_id if experiment_id else 'prod'}")
logger.info(f"Primary Table: {primary_table}")
logger.info(f"Primary Key: {primary_key}")
logger.info(f"Entity Attributes: {len(entity_attributes)}")
logger.info("\nConstructed Table Names:")
logger.info(f"  Master: {master_table}")
logger.info(f"  Unified: {unified_table}")
logger.info(f"  Merge Activities: {merge_activities_table}")
logger.info(f"  Attribute Version Sources: {attribute_version_sources_table}")
logger.info("="*60)

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 1: READ PRIMARY SOURCE DATA")
logger.info("="*60)

# Read primary source table
primary_df = spark.read.table(primary_table)

logger.info(f"Read {primary_df.count()} records from {primary_table}")
logger.info(f"Columns: {len(primary_df.columns)}")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 2: APPLY ATTRIBUTE MAPPING WITH TYPE CASTING")
logger.info("="*60)

# Find the mapping for this primary table
primary_table_mapping = None
for mapping_entry in attributes_mapping:
    if primary_table in mapping_entry:
        primary_table_mapping = mapping_entry[primary_table]
        break

if not primary_table_mapping:
    raise ValueError(f"No attribute mapping found for primary table: {primary_table}")

logger.info(f"Found attribute mapping for {primary_table}")
logger.info(f"Mapping: {len(primary_table_mapping)} attributes")

# Build select expressions for column mapping WITH TYPE CASTING
# Mapping format: {entity_attr: dataset_attr}
# We need to select dataset_attr, CAST it to entity model type, and alias as entity_attr
select_exprs = []
mapped_columns = set()

for entity_attr, dataset_attr in primary_table_mapping.items():
    if dataset_attr in primary_df.columns:
        # Get the target data type from entity model
        target_dtype_str = entity_attributes_datatype.get(entity_attr, 'string')
        target_spark_dtype = get_spark_data_type(target_dtype_str)
        
        # Get source data type
        source_field = [f for f in primary_df.schema.fields if f.name == dataset_attr][0]
        source_dtype = source_field.dataType
        
        # Cast to target type to match master table schema
        select_exprs.append(col(dataset_attr).cast(target_spark_dtype).alias(entity_attr))
        mapped_columns.add(entity_attr)
        
        # Log the mapping with type info
        if str(source_dtype) != str(target_spark_dtype):
            logger.info(f"  Mapped: {dataset_attr} ({source_dtype}) -> {entity_attr} ({target_spark_dtype}) [CAST]")
        else:
            logger.info(f"  Mapped: {dataset_attr} -> {entity_attr} ({target_spark_dtype})")
    else:
        logger.warning(f"Dataset column '{dataset_attr}' not found in source table")

logger.info(f"\nMapped {len(select_exprs)} columns")

# Add any missing entity attributes as NULL with CORRECT data type from entity model
missing_attrs = 0
for attr in entity_attributes:
    if attr not in mapped_columns and attr != "lakefusion_id":
        # Get the correct data type from entity model
        dtype_str = entity_attributes_datatype.get(attr, 'string')
        spark_dtype = get_spark_data_type(dtype_str)
        select_exprs.append(lit(None).cast(spark_dtype).alias(attr))
        missing_attrs += 1

if missing_attrs > 0:
    logger.info(f"Added {missing_attrs} missing attributes as NULL (with correct types)")

# Apply all mappings in a single select operation
mapped_df = primary_df.select(*select_exprs)

logger.info(f"\nMapped DataFrame has {mapped_df.count()} records")
logger.info(f"Resulting columns: {mapped_df.columns}")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 3: GENERATE IDS")
logger.info("="*60)

# Create UDF for surrogate key generation
generate_lakefusion_id_udf = udf(
    lambda source_path, source_id: generate_lakefusion_id(source_path, str(source_id)), 
    StringType()
)
generate_surrogate_key_udf = udf(
    lambda source_path, source_id: generate_surrogate_key(source_path, str(source_id)), 
    StringType()
)

# Add source metadata
id_df = mapped_df \
    .withColumn("source_path", lit(primary_table)) \
    .withColumn("source_id", col(primary_key).cast("string"))

# Generate both IDs deterministically based on source
id_df = id_df \
    .withColumn("lakefusion_id", generate_lakefusion_id_udf(col("source_path"), col("source_id"))) \
    .withColumn("surrogate_key", generate_surrogate_key_udf(col("source_path"), col("source_id")))

record_count = id_df.count()

logger.info(f"Generated lakefusion_id for {record_count} records")
logger.info(f"Generated surrogate_key for {record_count} records")
logger.info(f"IDs are deterministic - same input produces same ID")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 4: CREATE ATTRIBUTES_COMBINED")
logger.info("="*60)

# Get list of attributes to combine
combine_attrs = [attr for attr in match_attributes]

# Create attributes_combined by concatenating all attributes
id_df = id_df.withColumn(
    "attributes_combined",
    concat_ws(" | ", *[col(attr) for attr in combine_attrs])
)

logger.info(f"Created attributes_combined from {len(combine_attrs)} attributes")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 5: INSERT INTO MASTER TABLE")
logger.info("="*60)

# Prepare master insert DataFrame
master_columns = ["lakefusion_id"] + [attr for attr in entity_attributes if attr != "lakefusion_id"] + ["attributes_combined"]
master_insert_df = id_df.select(*master_columns)

# Get count before insert
before_count = spark.read.table(master_table).count()

# Insert into master table
master_insert_df.write.format("delta").mode("append").saveAsTable(master_table)

optimise_res = spark.sql(f"OPTIMIZE {master_table} ZORDER BY (lakefusion_id)")
spark.sql(f"""
    ALTER TABLE {unified_table}
    CLUSTER BY (surrogate_key, search_results)
""")
optimise_res = spark.sql(f"OPTIMIZE {unified_table}")


# Verify
after_count = spark.read.table(master_table).count()
inserted_count = after_count - before_count

logger.info(f"Inserted {inserted_count} records into Master table")
logger.info(f"  Before: {before_count}, After: {after_count}")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 6: INSERT INTO UNIFIED TABLE")
logger.info("="*60)

# Build select list for unified table schema
unified_select_cols = [
    "surrogate_key",
    "source_path",
    "source_id",
    col("lakefusion_id").alias("master_lakefusion_id"),  # NEW: Link to master
    lit(RecordStatus.MERGED.value).alias("record_status")
] + [col(attr) for attr in entity_attributes if attr != "lakefusion_id"] + [
    "attributes_combined",
    lit("").alias("search_results"),
    lit("").alias("scoring_results")
]

# Create unified DataFrame in one select operation
unified_insert_df = id_df.select(*unified_select_cols)

# Get count before insert
before_count = spark.read.table(unified_table).count()

# Insert into unified table
unified_insert_df.write.format("delta").mode("append").saveAsTable(unified_table)

# Verify
after_count = spark.read.table(unified_table).count()
inserted_count = after_count - before_count

logger.info(f"Inserted {inserted_count} records into Unified table")
logger.info(f"  Status: {RecordStatus.MERGED.value} (primary records are pre-merged)")
logger.info(f"  master_lakefusion_id: Linked to Master table")
logger.info(f"  Before: {before_count}, After: {after_count}")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 7: LOG TO MERGE ACTIVITIES")
logger.info("="*60)

# Get current version of master table (should be 0 or 1 for initial load)
master_version = spark.sql(f"DESCRIBE HISTORY {master_table}").selectExpr("max(version)").first()[0]

logger.info(f"Master table version: {master_version}")

# Create merge activities records
# Use master_id and match_id column names to match table schema
merge_activities_df = id_df.select(
    col("lakefusion_id").alias("master_id"),
    col("surrogate_key").alias("match_id"),
    col("source_path").alias("source"),
    lit(master_version).alias("version"),
    lit(ActionType.INITIAL_LOAD.value).alias("action_type"),
    current_timestamp().alias("created_at")
)

# Get count before insert
before_count = spark.read.table(merge_activities_table).count()

# Insert into merge activities
merge_activities_df.write.format("delta").mode("append").saveAsTable(merge_activities_table)

# Verify
after_count = spark.read.table(merge_activities_table).count()
inserted_count = after_count - before_count

logger.info(f"Logged {inserted_count} activities to Merge Activities table")
logger.info(f"  Action Type: {ActionType.INITIAL_LOAD.value}")
logger.info(f"  master_id = lakefusion_id, match_id = surrogate_key")
logger.info(f"  Before: {before_count}, After: {after_count}")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 8: LOG TO ATTRIBUTE VERSION SOURCES")
logger.info("="*60)

# Create attribute source mapping for each record
# This tracks which source contributed which attribute value

def create_attribute_mapping(row):
    """
    Create attribute source mapping for a single record.
    
    Returns list of dicts: [{attribute_name, attribute_value, source}, ...]
    """
    mapping = []
    source_path = row["source_path"]
    
    for attr in entity_attributes:
        if attr == "lakefusion_id":
            continue
        
        attr_value = row[attr]
        if attr_value is not None:
            mapping.append({
                "attribute_name": attr,
                "attribute_value": str(attr_value),
                "source": source_path
            })
    
    return mapping

# Register UDF for creating attribute mapping
create_mapping_udf = udf(
    create_attribute_mapping,
    ArrayType(StructType([
        StructField("attribute_name", StringType()),
        StructField("attribute_value", StringType()),
        StructField("source", StringType())
    ]))
)

# Create attribute version sources records DIRECTLY from id_df
# No need for complex joins - apply UDF and select needed columns in one go
attr_version_df = id_df.select(
    col("lakefusion_id"),
    lit(master_version).alias("version"),
    create_mapping_udf(struct(*id_df.columns)).alias("attribute_source_mapping")
)

# Get count before insert
before_count = spark.read.table(attribute_version_sources_table).count()

# Insert into attribute version sources
attr_version_df.write.format("delta").mode("append").saveAsTable(attribute_version_sources_table)

# Verify
after_count = spark.read.table(attribute_version_sources_table).count()
inserted_count = after_count - before_count

logger.info(f"Logged {inserted_count} attribute version records")
logger.info(f"  Version: {master_version}")
logger.info(f"  Before: {before_count}, After: {after_count}")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("VERIFICATION")
logger.info("="*60)

# Count records in each table
master_count = spark.read.table(master_table).count()
unified_count = spark.read.table(unified_table).count()
merge_activities_count = spark.read.table(merge_activities_table).count()
attr_version_count = spark.read.table(attribute_version_sources_table).count()

logger.info(f"Master Table: {master_count} records")
logger.info(f"Unified Table: {unified_count} records (status=MERGED)")
logger.info(f"Merge Activities: {merge_activities_count} records (action=INITIAL_LOAD)")
logger.info(f"Attribute Version Sources: {attr_version_count} records")

# Verify counts match
if master_count == unified_count == merge_activities_count == attr_version_count:
    logger.info(f"\nAll counts match: {master_count} records")
else:
    logger.warning(f"Counts don't match!")
    logger.warning(f"   Master: {master_count}")
    logger.warning(f"   Unified: {unified_count}")
    logger.warning(f"   Merge Activities: {merge_activities_count}")
    logger.warning(f"   Attr Version Sources: {attr_version_count}")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("LAKEFUSION_ID CONSISTENCY CHECK")
logger.info("="*60)

# Get lakefusion_ids from each table
master_ids = spark.read.table(master_table).select("lakefusion_id").distinct()
merge_master_ids = spark.read.table(merge_activities_table).select(col("master_id").alias("lakefusion_id")).distinct()
attr_version_ids = spark.read.table(attribute_version_sources_table).select("lakefusion_id").distinct()

master_id_count = master_ids.count()
merge_id_count = merge_master_ids.count()
attr_id_count = attr_version_ids.count()

logger.info(f"Unique lakefusion_ids in Master: {master_id_count}")
logger.info(f"Unique master_ids in Merge Activities: {merge_id_count}")
logger.info(f"Unique lakefusion_ids in Attr Version Sources: {attr_id_count}")

# Check if they match
if master_id_count == merge_id_count == attr_id_count:
    # Now check if the actual IDs are the same
    master_ids_set = set([row.lakefusion_id for row in master_ids.collect()])
    merge_ids_set = set([row.lakefusion_id for row in merge_master_ids.collect()])
    attr_ids_set = set([row.lakefusion_id for row in attr_version_ids.collect()])
    
    if master_ids_set == merge_ids_set == attr_ids_set:
        logger.info("All lakefusion_ids are consistent across tables!")
    else:
        logger.error("ERROR: lakefusion_ids don't match across tables!")
        logger.error(f"   IDs only in Master: {master_ids_set - merge_ids_set - attr_ids_set}")
        logger.error(f"   IDs only in Merge Activities: {merge_ids_set - master_ids_set - attr_ids_set}")
        logger.error(f"   IDs only in Attr Version Sources: {attr_ids_set - master_ids_set - merge_ids_set}")
else:
    logger.error("ERROR: Different number of lakefusion_ids across tables!")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("LOAD PRIMARY SOURCE - COMPLETE")
logger.info("="*60)
logger.info(f"Entity: {entity}")
logger.info(f"Source: {primary_table}")
logger.info(f"Records Loaded: {master_count}")
logger.info(f"Master Version: {master_version}")
logger.info("="*60)
logger.info("Next: Load Secondary Sources")
logger.info("="*60)
