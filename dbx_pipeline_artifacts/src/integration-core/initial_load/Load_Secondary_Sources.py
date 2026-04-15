# Databricks notebook source
import json
from pyspark.sql.functions import col, lit, concat_ws, struct
from pyspark.sql.types import StringType, IntegerType, LongType, DoubleType, FloatType, BooleanType, DateType, TimestampType, ShortType, ByteType
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

dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("dataset_tables", "", "Dataset Tables")
dbutils.widgets.text("dataset_objects", "", "Dataset Objects")
dbutils.widgets.text("primary_table", "", "Primary Table")
dbutils.widgets.text("primary_key", "", "Primary Key")
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")
dbutils.widgets.text("entity_attributes_datatype", "", "Entity Attributes Datatype")
dbutils.widgets.text("attributes_mapping", "", "Attributes Mapping")
dbutils.widgets.text("match_attributes", "", "Match Attributes")

# COMMAND ----------

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

dataset_tables = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="dataset_tables",
    debugValue=dbutils.widgets.get("dataset_tables")
)

dataset_objects = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="dataset_objects",
    debugValue=dbutils.widgets.get("dataset_objects")
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

attributes_mapping = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="attributes_mapping",
    debugValue=dbutils.widgets.get("attributes_mapping")
)

match_attributes = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="match_attributes",
    debugValue=dbutils.widgets.get("match_attributes")
)

experiment_id = dbutils.widgets.get("experiment_id")

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

from lakefusion_core_engine.identifiers import generate_surrogate_key
from lakefusion_core_engine.models import RecordStatus

# COMMAND ----------

entity_attributes = json.loads(entity_attributes)
entity_attributes_datatype = json.loads(entity_attributes_datatype)
attributes_mapping = json.loads(attributes_mapping)
dataset_tables = json.loads(dataset_tables)
dataset_objects = json.loads(dataset_objects)
match_attributes = json.loads(match_attributes)

# COMMAND ----------

experiment_suffix = f"_{experiment_id}" if experiment_id else ""
unified_table = f"{catalog_name}.silver.{entity}_unified{experiment_suffix}"

# COMMAND ----------

logger.info("="*60)
logger.info("LOAD SECONDARY SOURCES")
logger.info("="*60)
logger.info(f"Entity: {entity}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment: {experiment_id if experiment_id else 'prod'}")
logger.info(f"Unified Table: {unified_table}")
logger.info(f"Primary Table: {primary_table}")
logger.info(f"Primary Key (Entity): {primary_key}")
logger.info(f"Total Dataset Tables: {len(dataset_tables)}")
logger.info("="*60)

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 1: DERIVE SECONDARY SOURCES")
logger.info("="*60)

# Derive secondary tables by excluding primary table from dataset_tables
secondary_tables = [table for table in dataset_tables if table != primary_table]

logger.info(f"Dataset Tables: {len(dataset_tables)}")
logger.info(f"Primary Table: {primary_table}")
logger.info(f"Secondary Tables: {len(secondary_tables)}")

# Check if secondary sources exist
if not secondary_tables or len(secondary_tables) == 0:
    logger.info("No secondary sources found for this entity")
    logger.info("This is a single-source entity")
    logger.info("\n" + "="*60)
    logger.info("EXITING - NO SECONDARY SOURCES TO LOAD")
    logger.info("="*60)
    
    # Set task value indicating no secondary sources
    dbutils.jobs.taskValues.set("secondary_load_complete", True)
    dbutils.jobs.taskValues.set("secondary_records_loaded", 0)
    dbutils.jobs.taskValues.set("secondary_sources_count", 0)
    
    dbutils.notebook.exit(json.dumps({
        "status": "skipped",
        "message": "No secondary sources to load",
        "records_loaded": 0
    }))

logger.info(f"\nFound {len(secondary_tables)} secondary source(s):")
for i, sec_table in enumerate(secondary_tables, 1):
    table_info = dataset_objects.get(sec_table, {})
    table_name = table_info.get("name", sec_table)
    logger.info(f"  {i}. {sec_table}")
    logger.info(f"     Name: {table_name}")
    logger.info(f"     Active: {table_info.get('is_active', 'Unknown')}")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 2: PROCESS SECONDARY SOURCES")
logger.info("="*60)

# Create UDF for surrogate key generation
generate_surrogate_key_udf = udf(
    lambda source_path, source_id: generate_surrogate_key(source_path, str(source_id)), 
    StringType()
)

# Track total records loaded
total_records_loaded = 0
successful_sources = 0
failed_sources = []

# Get unified table count before processing
unified_before_count = spark.read.table(unified_table).count()
logger.info(f"Unified table count before: {unified_before_count}")

# Process each secondary source
for idx, secondary_table in enumerate(secondary_tables, 1):
    try:
        logger.info("\n" + "-"*60)
        logger.info(f"Processing Secondary Source {idx}/{len(secondary_tables)}")
        logger.info(f"  Table: {secondary_table}")
        logger.info("-"*60)
        
        # Step 2a: Find attribute mapping for this secondary table
        logger.info(f"\n[{idx}.a] Finding attribute mapping...")
        
        secondary_mapping = None
        for mapping_entry in attributes_mapping:
            if secondary_table in mapping_entry:
                secondary_mapping = mapping_entry[secondary_table]
                break
        
        if not secondary_mapping:
            logger.warning(f"No attribute mapping found for {secondary_table}")
            logger.warning(f"  Skipping this source")
            failed_sources.append({"table": secondary_table, "reason": "No attribute mapping"})
            continue
        
        logger.info(f"  Found mapping with {len(secondary_mapping)} attributes")
        
        # Step 2b: Find the source column that maps to the entity's primary key
        logger.info(f"\n[{idx}.b] Finding primary key column...")
        
        # The mapping is {entity_attr: dataset_attr}
        # We need to find the dataset_attr that corresponds to the entity's primary_key
        source_primary_key = secondary_mapping.get(primary_key)
        
        if not source_primary_key:
            logger.warning(f"   Warning: Entity primary key '{primary_key}' not found in mapping")
            logger.info(f"  Available entity attributes in mapping: {list(secondary_mapping.keys())}")
            logger.info(f"  Skipping this source")
            failed_sources.append({"table": secondary_table, "reason": f"Entity primary key '{primary_key}' not in mapping"})
            continue
        
        logger.info(f"  Source primary key column: {source_primary_key}")
        logger.info(f"  Entity attribute '{primary_key}' maps from dataset column '{source_primary_key}'")
        
        # Step 2c: Read secondary source data
        logger.info(f"\n[{idx}.c] Reading data from {secondary_table}...")
        secondary_df = spark.read.table(secondary_table)
        source_record_count = secondary_df.count()
        logger.info(f"  Read {source_record_count} records")
        
        if source_record_count == 0:
            logger.warning(f"   Skipping - no records in source table")
            continue
        
        # Verify source_primary_key column exists
        if source_primary_key not in secondary_df.columns:
            logger.warning(f"   Error: Primary key column '{source_primary_key}' not found in source table")
            logger.info(f"  Available columns: {secondary_df.columns}")
            failed_sources.append({"table": secondary_table, "reason": f"Primary key column '{source_primary_key}' not found"})
            continue
        
        # Step 2d: Apply attribute mapping WITH TYPE CASTING
        logger.info(f"\n[{idx}.d] Applying attribute mapping with type casting...")
        
        # Build the select expressions for column mapping WITH TYPE CASTING
        # Mapping format: {entity_attr: dataset_attr}
        select_exprs = []
        mapped_columns = set()
        
        for entity_attr, dataset_attr in secondary_mapping.items():
            if dataset_attr in secondary_df.columns:
                # Get the target data type from entity model
                target_dtype_str = entity_attributes_datatype.get(entity_attr, 'string')
                target_spark_dtype = get_spark_data_type(target_dtype_str)
                
                # Get source data type
                source_field = [f for f in secondary_df.schema.fields if f.name == dataset_attr][0]
                source_dtype = source_field.dataType
                
                # Cast to target type to match unified/master table schema
                select_exprs.append(col(dataset_attr).cast(target_spark_dtype).alias(entity_attr))
                mapped_columns.add(entity_attr)
                
                # Log the mapping with type info
                if str(source_dtype) != str(target_spark_dtype):
                    logger.info(f"    Mapped: {dataset_attr} ({source_dtype}) → {entity_attr} ({target_spark_dtype}) [CAST]")
                else:
                    logger.info(f"    Mapped: {dataset_attr} → {entity_attr} ({target_spark_dtype})")
            else:
                logger.warning(f"     Warning: Dataset column '{dataset_attr}' not found in source table")
        
        logger.info(f"  Mapped {len(select_exprs)} columns")
        
        # Add missing entity attributes as NULL with CORRECT data type from entity model
        missing_attrs = 0
        for attr in entity_attributes:
            if attr not in mapped_columns and attr != "lakefusion_id":
                # Get the correct data type from entity model
                dtype_str = entity_attributes_datatype.get(attr, 'string')
                spark_dtype = get_spark_data_type(dtype_str)
                select_exprs.append(lit(None).cast(spark_dtype).alias(attr))
                missing_attrs += 1
        
        if missing_attrs > 0:
            logger.info(f"  Added {missing_attrs} missing attributes as NULL (with correct types)")
        
        # Apply all mappings in a single select operation - this avoids ambiguous references
        mapped_df = secondary_df.select(*select_exprs)
        
        logger.info(f"  Resulting columns: {mapped_df.columns}")
        logger.info(f"  All columns now match unified table schema types")
        
        # Step 2e: Generate surrogate_key
        logger.info(f"\n[{idx}.e] Generating surrogate keys...")
        
        # Add source metadata
        # Use the entity primary key (already mapped) as source_id
        mapped_df = mapped_df \
            .withColumn("source_path", lit(secondary_table)) \
            .withColumn("source_id", col(primary_key).cast("string"))
        
        # Generate surrogate_key deterministically
        mapped_df = mapped_df.withColumn(
            "surrogate_key", 
            generate_surrogate_key_udf(col("source_path"), col("source_id"))
        )
        
        logger.info(f"  Generated surrogate keys for {source_record_count} records")
        logger.info(f"  Using '{primary_key}' as source_id")
        
        # Step 2f: Create attributes_combined
        logger.info(f"\n[{idx}.f] Creating attributes_combined column...")
        
        combine_attrs = [attr for attr in match_attributes]
        mapped_df = mapped_df.withColumn(
            "attributes_combined",
            concat_ws(" | ", *[col(attr) for attr in combine_attrs])
        )
        
        logger.info(f"  Created attributes_combined from {len(combine_attrs)} attributes")
        
        # Step 2g: Prepare for unified table insert
        logger.info(f"\n[{idx}.g] Preparing data for Unified table insert...")
        
        # Build select list for unified table schema
        unified_select_cols = [
            "surrogate_key",
            "source_path",
            "source_id",
            lit('').cast(StringType()).alias("master_lakefusion_id"),  # NULL until matched/merged
            lit(RecordStatus.ACTIVE.value).alias("record_status")
        ] + [col(attr) for attr in entity_attributes if attr != "lakefusion_id"] + [
            "attributes_combined",
            lit("").alias("search_results"),
            lit("").alias("scoring_results")
        ]
        
        # Create unified insert DataFrame with proper column order in one select
        unified_insert_df = mapped_df.select(*unified_select_cols)
        
        records_to_insert = unified_insert_df.count()
        logger.info(f"  Prepared {records_to_insert} records for insert")
        logger.info(f"  Status: {RecordStatus.ACTIVE.value}")
        logger.info(f"  master_lakefusion_id: NULL (will be set after matching/merging)")
        logger.info(f"  Schema matches unified table (all types cast to entity model types)")
        
        # Step 2h: Insert into unified table
        logger.info(f"\n[{idx}.h] Inserting into Unified table...")
        
        before_insert = spark.read.table(unified_table).count()
        unified_insert_df.write.format("delta").mode("append").saveAsTable(unified_table)
        after_insert = spark.read.table(unified_table).count()
        inserted = after_insert - before_insert
        
        logger.info(f"  Inserted {inserted} records")
        logger.info(f"  Before: {before_insert}, After: {after_insert}")
        
        total_records_loaded += inserted
        successful_sources += 1
        
        logger.info(f"\nSuccessfully processed {secondary_table}")
        
    except Exception as e:
        import traceback
        logger.error(f"\nError processing {secondary_table}:")
        logger.info(f"   {str(e)}")
        logger.info(f"\n   Stack trace:")
        traceback.print_exc()
        failed_sources.append({"table": secondary_table, "reason": str(e)})
        # Continue with next source instead of failing the entire job
        continue

# COMMAND ----------

spark.sql(f"""
    ALTER TABLE {unified_table}
    CLUSTER BY (surrogate_key, search_results)
""")
optimise_res = spark.sql(f"OPTIMIZE {unified_table}")

logger.info("\n" + "="*60)
logger.info("VERIFICATION")
logger.info("="*60)

# Get final unified table count
unified_after_count = spark.read.table(unified_table).count()

logger.info(f"Unified Table:")
logger.info(f"  Before secondary load: {unified_before_count}")
logger.info(f"  After secondary load: {unified_after_count}")
logger.info(f"  Records added: {unified_after_count - unified_before_count}")

logger.info(f"\nSecondary Sources Processing Summary:")
logger.info(f"  Total secondary sources: {len(secondary_tables)}")
logger.info(f"  Successfully processed: {successful_sources}")
logger.info(f"  Failed: {len(failed_sources)}")
logger.info(f"  Total records loaded: {total_records_loaded}")

if failed_sources:
    logger.warning(f"\n Failed Sources:")
    for failed in failed_sources:
        logger.info(f"    - {failed['table']}")
        logger.info(f"      Reason: {failed['reason']}")

# Verify expected vs actual records loaded
expected_records = unified_after_count - unified_before_count
if expected_records == total_records_loaded:
    logger.info(f"\nRecord count verification: PASSED")
else:
    logger.warning(f"\n Record count mismatch!")
    logger.info(f"   Expected: {expected_records}")
    logger.info(f"   Actual: {total_records_loaded}")

# Count records by status
logger.info(f"\n" + "-"*60)
logger.info("Unified Table Record Status Breakdown:")
logger.info("-"*60)
# status_counts = spark.read.table(unified_table) \
#     .groupBy("record_status") \
#     .count() \
#     .orderBy("record_status")
# status_counts.show(truncate=False)

# Count records by source
logger.info(f"\n" + "-"*60)
logger.info("Unified Table Records by Source:")
logger.info("-"*60)
# source_counts = spark.read.table(unified_table) \
#     .groupBy("source_path") \
#     .count() \
#     .orderBy(col("count").desc())
# source_counts.show(truncate=False)

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("LOAD SECONDARY SOURCES - COMPLETE")
logger.info("="*60)
logger.info(f"Entity: {entity}")
logger.info(f"Primary Table: {primary_table}")
logger.info(f"Secondary Sources: {len(secondary_tables)}")
logger.info(f"  - Processed: {successful_sources}")
logger.info(f"  - Failed: {len(failed_sources)}")
logger.info(f"Records Loaded: {total_records_loaded}")
logger.info(f"Status: {'SUCCESS' if len(failed_sources) == 0 else 'PARTIAL SUCCESS'}")
logger.info("="*60)
if failed_sources:
    logger.warning(f"\n {len(failed_sources)} source(s) failed - check logs above")
logger.info("Next: Entity Matching - Vector Search")
logger.info("="*60)
