# Databricks notebook source
# DBTITLE 1,Import required libraries
import json
from math import ceil as math_ceil  # For integer operations
from pyspark.sql.functions import (
    col, lit, udf, concat_ws, struct, row_number,
    monotonically_increasing_id, when
)
from pyspark.sql.types import (
    StringType, IntegerType, LongType, DoubleType, FloatType, 
    BooleanType, DateType, TimestampType, ShortType, ByteType
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable





# COMMAND ----------

# DBTITLE 1,Helper function for data type conversion
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
dbutils.widgets.text("processed_records", "", "Processed Records Range")
dbutils.widgets.text("primary_table", "", "Primary Table")
dbutils.widgets.text("primary_key", "", "Primary Key")
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")
dbutils.widgets.text("entity_attributes_datatype", "", "Entity Attributes Datatype")
dbutils.widgets.text("match_attributes", "", "Match Attributes")
dbutils.widgets.text("attributes_mapping", "", "Attributes Mapping")
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

# Get processed_records from job parameter
processed_records = dbutils.widgets.get("processed_records")

experiment_id = dbutils.widgets.get("experiment_id")

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

# DBTITLE 1,Import core engine components
from lakefusion_core_engine.identifiers import generate_lakefusion_id, generate_surrogate_key
from lakefusion_core_engine.models import RecordStatus

# COMMAND ----------

# DBTITLE 1,Parse JSON strings
entity_attributes = json.loads(entity_attributes)
entity_attributes_datatype = json.loads(entity_attributes_datatype)
match_attributes = json.loads(match_attributes)
attributes_mapping = json.loads(attributes_mapping)
dataset_tables = json.loads(dataset_tables)

# Parse processed_records - format: "[start, end]" or "[0, 2000]"
processed_records = json.loads(processed_records) if processed_records else [0, 2000]
record_start = processed_records[0]
record_end = processed_records[1]
record_limit = record_end - record_start

# Convert is_single_source to boolean
if isinstance(is_single_source, str):
    is_single_source = is_single_source.lower() == "true"

# COMMAND ----------

# DBTITLE 1,Construct table names
experiment_suffix = f"_{experiment_id}" if experiment_id else ""

master_table = f"{catalog_name}.gold.{entity}_master{experiment_suffix}"
unified_table = f"{catalog_name}.silver.{entity}_unified{experiment_suffix}"
unified_dedup_table = f"{catalog_name}.silver.{entity}_unified_deduplicate{experiment_suffix}"

# Derive secondary tables
secondary_tables = [table for table in dataset_tables if table != primary_table]

# COMMAND ----------

logger.info("="*60)
logger.info("LOAD EXPERIMENT DATA")
logger.info("="*60)
logger.info(f"Entity: {entity}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment ID: {experiment_id}")
logger.info(f"Is Single Source: {is_single_source}")
logger.info(f"Processed Records Range: [{record_start}, {record_end}]")
logger.info(f"Record Limit: {record_limit}")
logger.info(f"Primary Table: {primary_table}")
logger.info(f"Secondary Tables: {len(secondary_tables)}")
logger.info(f"\nTable Names:")
logger.info(f"  Master: {master_table}")
if is_single_source:
    logger.info(f"  Unified Dedup: {unified_dedup_table}")
else:
    logger.info(f"  Unified: {unified_table}")
logger.info("="*60)

# COMMAND ----------

# DBTITLE 1,Determine if this is initial or incremental load
#master_count_before = spark.table(master_table).count()
#master_count_before = spark.table(master_table).isEmpty()
is_initial_load = (record_start == 0)

logger.info(f"\nLoad Type Determination:")
#logger.info(f"  Master records before: {master_count_before}")
logger.info(f"  Record start: {record_start}")
logger.info(f"  Is Initial Load: {is_initial_load}")

# COMMAND ----------

# DBTITLE 1,Create UDFs for ID generation
generate_lakefusion_id_udf = udf(lambda: generate_lakefusion_id(), StringType())
generate_surrogate_key_udf = udf(
    lambda source_path, source_id: generate_surrogate_key(source_path, str(source_id)), 
    StringType()
)

# COMMAND ----------

# DBTITLE 1,Helper function to apply attribute mapping
def apply_attribute_mapping(df, table_name, mapping_list, entity_attrs, entity_attr_dtypes):
    """
    Apply attribute mapping with type casting to a DataFrame.
    
    Args:
        df: Source DataFrame
        table_name: Name of the source table
        mapping_list: List of mapping dictionaries
        entity_attrs: List of entity attribute names
        entity_attr_dtypes: Dict of attribute data types
        
    Returns:
        DataFrame with mapped and typed columns
    """
    # Find the mapping for this table
    table_mapping = None
    for mapping_entry in mapping_list:
        if table_name in mapping_entry:
            table_mapping = mapping_entry[table_name]
            break
    
    if not table_mapping:
        raise ValueError(f"No attribute mapping found for table: {table_name}")
    
    # Build select expressions
    select_exprs = []
    mapped_columns = set()
    
    for entity_attr, dataset_attr in table_mapping.items():
        if dataset_attr in df.columns:
            target_dtype_str = entity_attr_dtypes.get(entity_attr, 'string')
            target_spark_dtype = get_spark_data_type(target_dtype_str)
            select_exprs.append(col(dataset_attr).cast(target_spark_dtype).alias(entity_attr))
            mapped_columns.add(entity_attr)
    
    # Add missing attributes as NULL
    for attr in entity_attrs:
        if attr not in mapped_columns and attr != "lakefusion_id":
            dtype_str = entity_attr_dtypes.get(attr, 'string')
            spark_dtype = get_spark_data_type(dtype_str)
            select_exprs.append(lit(None).cast(spark_dtype).alias(attr))
    
    return df.select(*select_exprs)

# COMMAND ----------

# ==============================================================================
# SINGLE SOURCE LOGIC (Golden Dedup)
# ==============================================================================

if is_single_source:
    logger.info("\n" + "="*60)
    logger.info("SINGLE SOURCE MODE - Loading Primary with Record Limit")
    logger.info("="*60)
    
    # Read primary source
    primary_df = spark.read.table(primary_table)
    
    # Apply attribute mapping
    mapped_df = apply_attribute_mapping(
        primary_df, primary_table, attributes_mapping, 
        entity_attributes, entity_attributes_datatype
    )
    
    # Add row numbers for limiting
    window_spec = Window.orderBy(col(primary_key))
    mapped_df = mapped_df.withColumn("_row_num", row_number().over(window_spec))
    
    # Filter to the specified range (1-indexed row_number)
    filtered_df = mapped_df.filter(
        (col("_row_num") > record_start) & (col("_row_num") <= record_end)
    ).drop("_row_num")
    
    records_to_load = filtered_df.count()
    logger.info(f"  Records in range [{record_start+1}, {record_end}]: {records_to_load}")
    
    if records_to_load == 0:
        logger.warning("No records to load in specified range")
        dbutils.jobs.taskValues.set("records_loaded", 0)
        dbutils.notebook.exit(json.dumps({
            "status": "skipped",
            "message": "No records in specified range",
            "records_loaded": 0
        }))
    
    # Generate lakefusion_id
    id_df = filtered_df.withColumn("lakefusion_id", generate_lakefusion_id_udf())
    
    # Add source metadata (for tracking)
    id_df = id_df.withColumn("source_path", lit(primary_table)) \
                 .withColumn("source_id", col(primary_key).cast("string"))
    
    # Generate surrogate_key (for consistency)
    id_df = id_df.withColumn(
        "surrogate_key",
        generate_surrogate_key_udf(col("source_path"), col("source_id"))
    )
    
    # Create attributes_combined
    combine_attrs = [attr for attr in match_attributes if attr in id_df.columns]
    id_df = id_df.withColumn(
        "attributes_combined",
        concat_ws(" | ", *[col(attr) for attr in combine_attrs])
    )
    
    # Prepare master insert
    master_columns = ["lakefusion_id"] + [attr for attr in entity_attributes if attr != "lakefusion_id"] + ["attributes_combined"]
    master_insert_df = id_df.select(*master_columns)
    
    # Insert into master table
    master_insert_df.write.format("delta").mode("append").saveAsTable(master_table)
    
    # Now clone master to unified_dedup for self-comparison
    logger.info("\n" + "-"*60)
    logger.info("Cloning Master to Unified Dedup for self-comparison...")
    logger.info("-"*60)
    
    master_df = spark.table(master_table)
    unified_dedup_df = master_df.withColumn("search_results", lit("").cast(StringType())) \
                                 .withColumn("scoring_results", lit("").cast(StringType()))
    
    # Overwrite unified_dedup (full clone each time)
    unified_dedup_df.write.format("delta").mode("overwrite").saveAsTable(unified_dedup_table)
    
    logger.info(f"  All records have empty search_results and scoring_results")

# COMMAND ----------

# ==============================================================================
# MULTI SOURCE LOGIC (Normal Dedup)
# ==============================================================================

if not is_single_source:
    logger.info("\n" + "="*60)
    logger.info("MULTI SOURCE MODE")
    logger.info("="*60)
    
    total_records_loaded = 0
    
    # -------------------------------------------------------------------------
    # STEP 1: Load ALL Primary Records to Master (no limit for primary)
    # -------------------------------------------------------------------------
    if is_initial_load:
        logger.info("\n" + "-"*60)
        logger.info("STEP 1: Loading ALL Primary Records to Master")
        logger.info("-"*60)
        
        primary_df = spark.read.table(primary_table)
        
        # Apply attribute mapping
        mapped_df = apply_attribute_mapping(
            primary_df, primary_table, attributes_mapping,
            entity_attributes, entity_attributes_datatype
        )
        
        # Generate lakefusion_id
        id_df = mapped_df.withColumn("lakefusion_id", generate_lakefusion_id_udf())
        
        # Add source metadata
        id_df = id_df.withColumn("source_path", lit(primary_table)) \
                     .withColumn("source_id", col(primary_key).cast("string"))
        
        # Generate surrogate_key
        id_df = id_df.withColumn(
            "surrogate_key",
            generate_surrogate_key_udf(col("source_path"), col("source_id"))
        )
        
        # Create attributes_combined
        combine_attrs = [attr for attr in match_attributes if attr in id_df.columns]
        id_df = id_df.withColumn(
            "attributes_combined",
            concat_ws(" | ", *[col(attr) for attr in combine_attrs])
        )
        
        # Prepare and insert into master
        master_columns = ["lakefusion_id"] + [attr for attr in entity_attributes if attr != "lakefusion_id"] + ["attributes_combined"]
        master_insert_df = id_df.select(*master_columns)
        
        master_insert_df.write.format("delta").mode("append").saveAsTable(master_table)
        
        # NOTE: In the experiment pipeline, primary records do NOT go into Unified.
        # - Multi-source: Only secondary records go to Unified (with ACTIVE status)
        # - Single-source: Master is cloned to Unified_Deduplicate by Process_Unified_Dedup_Table
        
    else:
        logger.info("\n" + "-"*60)
        logger.info("STEP 1: SKIPPING Primary Load (Incremental - Primary already loaded)")
        logger.info("-"*60)
    
    # -------------------------------------------------------------------------
    # STEP 2: Load Secondary Records with Record Limit (Proportional Split)
    # -------------------------------------------------------------------------
    logger.info("\n" + "-"*60)
    logger.info("STEP 2: Loading Secondary Records with Limit")
    logger.info("-"*60)
    
    if len(secondary_tables) == 0:
        logger.info("  No secondary tables to process")
    else:
        # Calculate proportional limit per source
        num_secondary = len(secondary_tables)
        records_per_source = math_ceil(record_limit / num_secondary)
        
        logger.info(f"  Secondary tables: {num_secondary}")
        logger.info(f"  Total record limit: {record_limit}")
        logger.info(f"  Records per source (proportional): {records_per_source}")
        
        # Track how many records we've loaded so far in this batch
        batch_records_loaded = 0
        remaining_limit = record_limit
        
        for idx, secondary_table in enumerate(secondary_tables, 1):
            logger.info(f"\n[{idx}/{num_secondary}] Processing: {secondary_table}")
            
            if remaining_limit <= 0:
                logger.warning("Record limit reached, skipping")
                continue
            
            # Read secondary source
            secondary_df = spark.read.table(secondary_table)
            
            # Find the mapping for this table
            secondary_mapping = None
            source_primary_key = None
            for mapping_entry in attributes_mapping:
                if secondary_table in mapping_entry:
                    secondary_mapping = mapping_entry[secondary_table]
                    # Find primary key mapping
                    for entity_attr, dataset_attr in secondary_mapping.items():
                        if entity_attr == primary_key:
                            source_primary_key = dataset_attr
                            break
                    break
            
            if not secondary_mapping:
                raise Exception(f"No attribute mapping found for table: {secondary_table}")
            
            if not source_primary_key:
                source_primary_key = primary_key
            
            # Apply attribute mapping
            mapped_df = apply_attribute_mapping(
                secondary_df, secondary_table, attributes_mapping,
                entity_attributes, entity_attributes_datatype
            )
            
            # Add row numbers for range-based filtering
            window_spec = Window.orderBy(col(primary_key))
            mapped_df = mapped_df.withColumn("_row_num", row_number().over(window_spec))
            
            # Calculate the range for this source
            # For proportional split: each source contributes records_per_source
            # But we also need to account for the overall range [record_start, record_end]
            source_start = int(record_start / num_secondary) if idx > 1 else record_start
            source_limit = min(records_per_source, remaining_limit)
            
            # Filter by row number range
            filtered_df = mapped_df.filter(
                (col("_row_num") > source_start) & 
                (col("_row_num") <= source_start + source_limit)
            ).drop("_row_num")
            
            records_to_load = filtered_df.count()
            logger.info(f"  Records to load (range [{source_start+1}, {source_start + source_limit}]): {records_to_load}")
            
            if records_to_load == 0:
                logger.info("  No records in range, skipping")
                continue
            
            # Add source metadata
            mapped_df = filtered_df.withColumn("source_path", lit(secondary_table)) \
                                    .withColumn("source_id", col(primary_key).cast("string"))
            
            # Generate surrogate_key
            mapped_df = mapped_df.withColumn(
                "surrogate_key",
                generate_surrogate_key_udf(col("source_path"), col("source_id"))
            )
            
            # Create attributes_combined
            combine_attrs = [attr for attr in match_attributes if attr in mapped_df.columns]
            mapped_df = mapped_df.withColumn(
                "attributes_combined",
                concat_ws(" | ", *[col(attr) for attr in combine_attrs])
            )
            
            # Prepare for unified insert (SIMPLIFIED - no master_lakefusion_id)
            unified_select_cols = [
                "surrogate_key",
                "source_path",
                "source_id",
                lit(RecordStatus.ACTIVE.value).alias("record_status")
            ] + [col(attr) for attr in entity_attributes if attr != "lakefusion_id"] + [
                "attributes_combined",
                lit("").alias("search_results"),
                lit("").alias("scoring_results")
            ]
            
            unified_insert_df = mapped_df.select(*unified_select_cols)
            
            # Insert into unified
            before_unified = spark.table(unified_table).count()
            unified_insert_df.write.format("delta").mode("append").saveAsTable(unified_table)
            after_unified = spark.table(unified_table).count()
            inserted = after_unified - before_unified
            
            #print(f"  ✓ Inserted {inserted} records into Unified (status=ACTIVE)")
            
            batch_records_loaded += inserted
            remaining_limit -= inserted
            total_records_loaded += inserted
        
        logger.info(f"\nTotal secondary records loaded in this batch: {batch_records_loaded}")

# COMMAND ----------

# DBTITLE 1,Summary
logger.info("\n" + "="*60)
logger.info("LOAD EXPERIMENT DATA - COMPLETE")
logger.info("="*60)
logger.info(f"Entity: {entity}")
logger.info(f"Experiment ID: {experiment_id}")
logger.info(f"Mode: {'Single-Source (Golden Dedup)' if is_single_source else 'Multi-Source'}")
#logger.info(f"Processed Records Range: [{record_start}, {record_end}]")

logger.info("="*60)
logger.info("\nNext: Endpoints Mapping / Entity Matching")
logger.info("="*60)
