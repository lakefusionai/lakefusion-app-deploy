# Databricks notebook source
import json
import ast
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,lit
import uuid

# COMMAND ----------

import json
import ast
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,lit
import uuid

# COMMAND ----------

dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("primary_table", "", "Primary Table")
dbutils.widgets.text("dataset_tables", "", "Dataset Tables")
dbutils.widgets.text("dataset_objects", "", "Dataset Objects")
dbutils.widgets.text("id_key", "", "lakefusion Primary Key")
dbutils.widgets.text("primary_key", "", "Primary Key")
dbutils.widgets.text("attributes_mapping_json", "{}", "Attributes Mapping (JSON)")
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")
dbutils.widgets.text("experiment_id", "", "Match Maven Experiment ID")
dbutils.widgets.text("attributes", "", "Merged Description Attributes")
dbutils.widgets.text('processed_records', '', 'No of records to be proceesed')
dbutils.widgets.text("is_integration_hub", "", "Integration Hub Pipeline")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("entity_attributes_datatype","","entity_attributes_datatype")
#dbutils.widgets.text('validation_functions', '', 'Validation Functions')


# COMMAND ----------

entity = dbutils.widgets.get("entity")
primary_table = dbutils.widgets.get("primary_table")
dataset_tables = dbutils.widgets.get("dataset_tables")
dataset_objects = dbutils.widgets.get("dataset_objects")
id_key = dbutils.widgets.get("id_key")
primary_key = dbutils.widgets.get("primary_key")
attributes_mapping_json = dbutils.widgets.get("attributes_mapping_json")
entity_attributes = dbutils.widgets.get("entity_attributes")
entity_attributes_datatype=dbutils.widgets.get("entity_attributes_datatype")
attributes = dbutils.widgets.get("attributes")
if(dbutils.widgets.get("processed_records")!=""):
    process_records=ast.literal_eval(dbutils.widgets.get("processed_records"))
else:
    process_records=dbutils.widgets.get("processed_records")
#below inputs are to be passed as notebook params OR add them to entity_json itself
experiment_id = dbutils.widgets.get("experiment_id") # cannot contain "-" for table name
experiment_id = experiment_id.replace("-", "") #remove "-" which is invalid for table name
incremental_load = False
merged_desc_column = "attributes_combined"
catalog_name=dbutils.widgets.get("catalog_name")

# COMMAND ----------

entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
primary_table = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "primary_table", debugValue=primary_table
)
dataset_tables = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "dataset_tables", debugValue=dataset_tables)
dataset_objects = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "dataset_objects", debugValue=dataset_objects)
id_key = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "id_key", debugValue=id_key
)
primary_key = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "primary_key", debugValue=primary_key
)
entity_attributes = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes", debugValue=entity_attributes)
attributes_mapping_json = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "attributes_mapping", debugValue=attributes_mapping_json
)
# validation_functions_json=dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "validation_functions", debugValue=validation_functions)
attributes = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="match_attributes", debugValue=attributes)
entity_attributes_datatype = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes_datatype", debugValue=entity_attributes_datatype)

# COMMAND ----------

dataset_tables = json.loads(dataset_tables)
dataset_objects = json.loads(dataset_objects)
entity_attributes = json.loads(entity_attributes); 
entity_attributes.append("lakefusion_id")
entity_attributes_datatype = json.loads(entity_attributes_datatype)
attributes_mapping_json = json.loads(attributes_mapping_json)
attributes = json.loads(attributes)
# validation_function_json = json.loads(validation_functions_json)

# COMMAND ----------

# MAGIC %run ../utils/execute_utils

# COMMAND ----------

logger.info("==== Parameters ====")
logger.info(f"Entity: {entity}")
logger.info(f"Primary Table: {primary_table}")
logger.info(f"Dataset Tables: {dataset_tables}")
logger.info(f"Dataset Objects: {dataset_objects}")
logger.info(f"ID Key: {id_key}")
logger.info(f"Primary Key: {primary_key}")
logger.info(f"Entity Attributes: {entity_attributes}")
logger.info(f"Entity Attributes Datatype: {entity_attributes_datatype}")
logger.info(f"Attributes Mapping JSON: {attributes_mapping_json}")
logger.info(f"Attributes: {attributes}")
logger.info(f"Experiment ID: {experiment_id}")
logger.info(f"Process Records: {process_records}")
logger.info(f"Incremental Load: {incremental_load}")
logger.info(f"Merged Desc Column: {merged_desc_column}")
logger.info(f"Catalog Name: {catalog_name}")


# COMMAND ----------

# MAGIC %run ../utils/parse_utils

# COMMAND ----------

# MAGIC %run ../utils/match_merge_utils

# COMMAND ----------

unified_table = f"{catalog_name}.silver.{entity}_unified"
master_table = f"{catalog_name}.gold.{entity}_master"
processed_unified_table = f"{catalog_name}.silver.{entity}_processed_unified"
validation_error_unified_table = f"{catalog_name}.silver.{entity}_unified_validation_error"
if experiment_id:
  unified_table += f"_{experiment_id}"
  master_table += f"_{experiment_id}"
  validation_error_unified_table+=f"_{experiment_id}"
  processed_unified_table += f"_{experiment_id}"

merge_activities_table = f"{master_table}_merge_activities"
master_attribute_version_sources_table = f"{master_table}_attribute_version_sources"

master_table_list = master_table.split(".")
master_catalog, master_schema, master_table_name = master_table_list[0], master_table_list[1], master_table_list[2]

# COMMAND ----------

uuid_udf = udf(lambda: uuid.uuid4().hex.lower(), StringType())

# COMMAND ----------

from pyspark.sql.functions import col, lit, when, concat_ws, current_timestamp
from delta.tables import DeltaTable

def get_last_processed_version(table_meta_info, entity, table_name):
    """
    Get the last processed version of the table from metadata table.
    """
    if spark.catalog.tableExists(table_meta_info):
        meta_df = spark.read.table(table_meta_info).filter(col("table_name") == table_name).filter(col("entity_name")==entity)
        if meta_df.count() > 0:
            return meta_df.select("last_processed_version").first()[0]
    return None

def update_last_processed_version(table_meta_info, entity, table_name, version):
    """
    Update or insert last processed version into metadata table.
    """
    if not spark.catalog.tableExists(table_meta_info):
        # Create the meta info table
        schema = "table_name STRING,entity_name string,last_processed_version LONG, last_processed_timestamp TIMESTAMP"
        spark.sql(f"CREATE TABLE {table_meta_info} ({schema}) USING DELTA")
    
    # Create DataFrame to upsert
    new_row = spark.createDataFrame([(table_name, entity, version, )], ["table_name","entity_name","last_processed_version"])
    new_row = new_row.withColumn("last_processed_timestamp", current_timestamp())
    
    delta_meta = DeltaTable.forName(spark, table_meta_info)
    
    # Merge
    delta_meta.alias("target").merge(
        new_row.alias("source"),
        condition=f"target.table_name = source.table_name and target.entity_name = source.entity_name"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

def check_and_enable_cdf(table_name):
    try:
        # Check if CDF is already enabled
        table_properties = spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()
        cdf_enabled = False
        for prop in table_properties:
            if prop['key'] == 'delta.enableChangeDataFeed' and prop['value'].lower() == 'true':
                cdf_enabled = True
                break
        if not cdf_enabled:
            spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
            logger.info(f"CDF enabled on {table_name}")
        else:
            logger.info(f"CDF already enabled on {table_name}")
    except Exception as e:
        logger.error(f"Failed to check/enable CDF on {table_name}: {str(e)}")

# COMMAND ----------

master_table_exists = spark.catalog.tableExists(master_table)
master_table_created = False
meta_info_table = f"{catalog_name}.silver.table_meta_info"

if not master_table_exists:
    logger.info(f"Master table for entity: {entity}, doesn't exist. Proceeding with initial load. Creating {master_table}..")
    
    if primary_table and spark.catalog.tableExists(primary_table):
        logger.info("Loading primary dataset records into Master Table")
        df_master = spark.read.table(primary_table)
        primary_table_attr_mapping = get_primary_attr_mapping(attributes_mapping_json)
        df_master = df_master.selectExpr(
            *[f"{k} as {v}" for k, v in primary_table_attr_mapping.items()]
        )
        
        for key in entity_attributes:
            if key not in primary_table_attr_mapping.values():
                primary_table_attr_mapping[key] = None
        
        for new_col, value in primary_table_attr_mapping.items():
            if value is None:
                dtype = entity_attributes_datatype.get(new_col, "string")
                if dtype == 'date':
                    df_master = df_master.withColumn(new_col, lit(None).try_cast("date"))
                else:
                    df_master = df_master.withColumn(new_col, lit(None).cast("string"))
        
        df_master = df_master.select([column for column in df_master.columns if column in entity_attributes])
       
        df_master = df_master.withColumn(merged_desc_column, concat_ws(" | ", *[regexp_replace(trim(col(c)), r'\s+', ' ') for c in attributes]))
        df_master.withColumn(
            "lakefusion_id",
            when(col("lakefusion_id").isNull(), uuid_udf()).otherwise(col("lakefusion_id"))
        ).write.format("delta").saveAsTable(master_table)
        
        master_table_created = True
    else:
        raise ValueError(f"No master table- {master_table} or primary dataset table - {primary_table} found!")
else:
    df_master = spark.read.table(master_table)
    logger.info(f"Master table- {master_table} found for entity- {entity}. Proceeding with incremental load")
    incremental_load = True

logger.info(f"Original dataset_tables: {dataset_tables}")
if primary_table and primary_table in dataset_tables:
    dataset_tables.remove(primary_table)
    logger.info(f"Removed primary table {primary_table} from dataset_tables")

# Remove primary table mapping from attributes_mapping_json
attributes_mapping_json = remove_primary_from_attr_mapping(attributes_mapping_json)
logger.info(f"Processing {len(dataset_tables)} secondary tables: {dataset_tables}")

# COMMAND ----------

if experiment_id == "prod":
    check_and_enable_cdf(master_table)
    check_and_enable_cdf(primary_table)

# COMMAND ----------

if master_table_created and experiment_id == 'prod':
    logger.info(
        f"Creating or syncing master attribute version mapping and merge activities tables"
    )
    df_master=spark.read.table(master_table)
    master_version = get_current_table_version(master_table)

    # Get the list of columns to include in the mapping (excluding ID Key)
    columns_to_map = [col_name for col_name in df_master.columns if col_name != id_key]
    source = primary_table

    try:
        # Dynamically create the attribute_source_mapping using array of structs
        attribute_source_mapping = array(
            *[
                struct(
                    lit(col_name).alias("attribute_name"),
                    col(col_name).cast("string").alias("attribute_value"),  # Ensure attr_value is string
                    lit(source).alias("source")
                )
                for col_name in columns_to_map
            ]
        )

        df_master_attribute_version_mapping = df_master.select(
            col(id_key),
            lit(master_version).alias("version"),
            attribute_source_mapping.alias("attribute_source_mapping")
        )

        master_attribute_version_sources_table_exists = spark.catalog.tableExists(master_attribute_version_sources_table)
        
        if not master_attribute_version_sources_table_exists:
            df_master_attribute_version_mapping.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(master_attribute_version_sources_table)
        else:
            # Define the Delta table
            delta_table = DeltaTable.forName(spark, master_attribute_version_sources_table)

            # Perform merge operation
            delta_table.alias("target").merge(
                source=df_master_attribute_version_mapping.alias("source"),
                condition=f"target.{id_key} = source.{id_key} and target.version = source.version"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    except Exception as e:
        logger.error(
            f"Master attribute version mapping failed. Reason - {str(e)}"
        )

    try:
        df_master_merge_activities = df_master.select(
            col(id_key).alias(f'master_{id_key}'),
            lit("").alias(id_key),
            lit(source).alias("source"),
            lit(master_version).alias("version"),
            lit(ActionType.INITIAL_LOAD.value).alias("action_type")
        )

        merge_activities_table_exists = spark.catalog.tableExists(merge_activities_table)
        if not merge_activities_table_exists:
            df_master_merge_activities.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(merge_activities_table)
        else:
            # Define the Delta table
            delta_table = DeltaTable.forName(spark, merge_activities_table)

            # Perform merge operation
            delta_table.alias("target").merge(
                source=df_master_merge_activities.alias("source"),
                condition=f"target.{id_key} = source.{id_key} and target.master_{id_key} = source.master_{id_key} and target.version = source.version and target.source = source.source"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    except Exception as e:
        logger.error(
            f"Master merge activities failed. Reason - {str(e)}"
        )


# COMMAND ----------

from pyspark.sql.functions import lit

datasets_df_dict = {}

# Process only secondary tables (primary removed above)
for dataset in dataset_tables:
    # Find the mapping for this specific dataset
    mapping_dict = None
    for mapping in attributes_mapping_json:
        if dataset in mapping:
            mapping_dict = mapping[dataset]
            break
    
    if mapping_dict is None:
        logger.warning(f"No mapping found for {dataset}, skipping...")
        continue
    
    table_name = dataset.split(".").pop()
    logger.info(f"Processing secondary table: {dataset}")
    
    df = spark.read.table(dataset)

    if experiment_id == 'prod':
        check_and_enable_cdf(dataset)
    
    # Apply column mappings from source to entity attributes
    df = df.selectExpr(*[f"{source_col} as {entity_col}" for source_col, entity_col in mapping_dict.items()])
    
    # Add any missing entity attributes as NULL columns
    existing_columns = df.columns
    missing_columns = [col for col in entity_attributes if col not in existing_columns and col != "lakefusion_id"]
    
    for col_name in missing_columns:
        dtype = entity_attributes_datatype.get(col_name, "string")
        if dtype == 'date':
            df = df.withColumn(col_name, lit(None).cast("date"))
        else:
            df = df.withColumn(col_name, lit(None).cast("string"))
    
    # Select columns in entity_attributes order (excluding lakefusion_id - will be added later)
    final_columns = [col for col in entity_attributes if col != "lakefusion_id"]
    df = df.select(*final_columns).withColumn('table_name', lit(dataset))
    
    datasets_df_dict[table_name] = df
    logger.info(f"  - Added {df.count()} records from {table_name}")

logger.info(f"Total secondary tables processed: {len(datasets_df_dict)}")

# COMMAND ----------

# DBTITLE 1,Create unified dataset from datasets df
from pyspark.sql.functions import col, lit, row_number, spark_partition_id
from pyspark.sql.window import Window

# Initialize an empty DataFrame for df_unified
df_unified = spark.createDataFrame([], schema=datasets_df_dict[next(iter(datasets_df_dict))].schema)
# Append rows from each DataFrame in datasets_df_dict into df_unified
for df_name, df in datasets_df_dict.items():
    if process_records != "":
        # Add partitioning to avoid the warning
        #window_spec = Window.partitionBy(spark_partition_id()).orderBy(lit(1))
        window_spec = Window.orderBy(lit('1'))
        df = df.withColumn("row_number_id", row_number().over(window_spec))
        df = df.filter(col("row_number_id").between(int(process_records[0]), int(process_records[1])))
        df = df.drop("row_number_id")
       
    df_unified = df_unified.unionByName(df, allowMissingColumns=True)

df_unified = df_unified.withColumn(merged_desc_column, lit("")).withColumn("search_results", lit("")).withColumn("scoring_results", lit(""))

# COMMAND ----------

dataset_objects_df = spark.createDataFrame(list(dataset_objects.values()))

# COMMAND ----------

from pyspark.sql.functions import col, struct,concat_ws

joined_unified_df = (
    df_unified.join(dataset_objects_df, df_unified.table_name == dataset_objects_df.path)  # Fix: Remove []
    .select(df_unified['*'], struct(*[col(c) for c in dataset_objects_df.columns]).alias("dataset"))
)


# COMMAND ----------

# Generate merged decription
joined_unified_df = joined_unified_df.withColumn(merged_desc_column, concat_ws(" | ", *[col(c) for c in attributes]))

# COMMAND ----------

processed_unified_table_exists = spark.catalog.tableExists(processed_unified_table)
unified_table_exists = spark.catalog.tableExists(unified_table)


# COMMAND ----------

if unified_table_exists:
    from pyspark.sql.functions import col, coalesce, when, lit
    
    logger.info("Unified table exists - preserving lakefusion_ids for existing records")
    unified_table_df = spark.read.table(unified_table)
    
    left_df = joined_unified_df.alias("new_data")
    right_df = unified_table_df.alias("existing_data")
    
    join_condition = [
        col("new_data." + primary_key).eqNullSafe(col("existing_data." + primary_key)),
        col("new_data.table_name").eqNullSafe(col("existing_data.table_name"))
    ]
    
    joined_with_unified = left_df.join(right_df, on=join_condition, how="left")
    
    # Build select expressions
    select_expressions = []
    for column_name in left_df.columns:
        if column_name in ["search_results", "scoring_results"]:
            # Preserve from existing
            select_expressions.append(
                coalesce(
                    col("existing_data." + column_name),
                    col("new_data." + column_name)
                ).alias(column_name)
            )
        else:
            # Use new data
            select_expressions.append(col("new_data." + column_name))
    
    # Add lakefusion_id from existing data (new data doesn't have this column)
    select_expressions.append(
        col("existing_data.lakefusion_id").alias("lakefusion_id")
    )
    
    joined_unified_df_processable = joined_with_unified.select(*select_expressions)
    
    logger.info(f"Records to process: {joined_unified_df_processable.count()}")

else:
    logger.info("Initial load - creating unified table")
    joined_unified_df_processable = joined_unified_df.select(joined_unified_df['*'])
    logger.info(f"Records to process: {joined_unified_df_processable.count()}")

# COMMAND ----------

joined_unified_df_processable_deduplicated = joined_unified_df_processable.dropDuplicates([primary_key, "table_name"])

# Generate lakefusion_id based on whether column exists
if "lakefusion_id" in joined_unified_df_processable_deduplicated.columns:
    joined_unified_df_processable_deduplicated = joined_unified_df_processable_deduplicated.withColumn(
        "lakefusion_id",
        when(col("lakefusion_id").isNull(), uuid_udf()).otherwise(col("lakefusion_id"))
    )
else:
    joined_unified_df_processable_deduplicated = joined_unified_df_processable_deduplicated.withColumn(
        "lakefusion_id", 
        uuid_udf()
    )

logger.info(f"Final deduplicated records: {joined_unified_df_processable_deduplicated.count()}")

# COMMAND ----------

if not unified_table_exists:
    logger.info("Writing initial load to unified table")
    joined_unified_df_processable_deduplicated.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(unified_table)
    
    if experiment_id == "prod":
        check_and_enable_cdf(unified_table)
        
        latest_version = spark.sql(f"DESCRIBE HISTORY {master_table}").selectExpr("max(version)").first()[0]
        update_last_processed_version(meta_info_table, entity, master_table, latest_version)
                
        latest_version = spark.sql(f"DESCRIBE HISTORY {primary_table}").selectExpr("max(version)").first()[0]
        update_last_processed_version(meta_info_table, entity, primary_table, latest_version)
        
        for dataset in dataset_tables:
            latest_version = spark.sql(f"DESCRIBE HISTORY {dataset}").selectExpr("max(version)").first()[0]
            update_last_processed_version(meta_info_table, entity, dataset, latest_version)
        
        latest_version = spark.sql(f"DESCRIBE HISTORY {unified_table}").selectExpr("max(version)").first()[0]
        update_last_processed_version(meta_info_table, entity, unified_table, latest_version)

else:
    logger.info("Merging incremental changes to unified table")
    delta_table = DeltaTable.forName(spark, unified_table)
    
    delta_table.alias("target").merge(
        source=joined_unified_df_processable_deduplicated.alias("source"),
        condition=f"target.{primary_key} = source.{primary_key} AND target.table_name = source.table_name"
    ).whenMatchedUpdate(
        set={
            col: f"source.{col}" 
            for col in joined_unified_df_processable_deduplicated.columns 
            if col not in ["search_results", "scoring_results"]
        }
    ).whenNotMatchedInsertAll().whenNotMatchedBySourceDelete().execute()
    
    logger.info("Merge completed successfully")

# COMMAND ----------

dbutils.jobs.taskValues.set("master_table", master_table)
dbutils.jobs.taskValues.set("unified_table", unified_table)
dbutils.jobs.taskValues.set("entity", entity)
dbutils.jobs.taskValues.set("experiment_id", experiment_id)
dbutils.jobs.taskValues.set("id_key", id_key)
dbutils.jobs.taskValues.set("incremental_load", incremental_load)
dbutils.jobs.taskValues.set("meta_info_table", meta_info_table)

# COMMAND ----------

joined_unified_df=spark.read.table(unified_table)
num_cols = len(joined_unified_df.columns)+1
spark.sql(f"ALTER TABLE {unified_table} SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '{num_cols}')")
optimise_res = spark.sql(f"OPTIMIZE {unified_table} ZORDER BY ({merged_desc_column})")


num_cols = len(df_master.columns)+1
spark.sql(f"ALTER TABLE {master_table} SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '{num_cols}')")
optimise_res = spark.sql(f"OPTIMIZE {master_table} ZORDER BY ({id_key})")
