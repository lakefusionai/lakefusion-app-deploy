# Databricks notebook source
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
dbutils.widgets.text("id_key", "", "Primary Key")
dbutils.widgets.text("primary_key", "", "Primary Key")
dbutils.widgets.text("attributes_mapping_json", "{}", "Attributes Mapping (JSON)")
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")
dbutils.widgets.text("experiment_id", "", "Match Maven Experiment ID")
dbutils.widgets.text("attributes", "", "Merged Description Attributes")
dbutils.widgets.text('processed_records', '', 'No of records to be proceesed')
dbutils.widgets.text("is_integration_hub", "", "Integration Hub Pipeline")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("entity_attributes_datatype","","entity_attributes_datatype")
dbutils.widgets.text("is_golden_deduplication","","is_golden_deduplication")
#dbutils.widgets.text('validation_functions', '', 'Validation Functions')


# COMMAND ----------

entity = dbutils.widgets.get("entity")
primary_table = dbutils.widgets.get("primary_table")
dataset_tables = dbutils.widgets.get("dataset_tables")
dataset_objects = dbutils.widgets.get("dataset_objects")
id_key = dbutils.widgets.get("id_key")
primary_key = dbutils.widgets.get("primary_key")
attributes_mapping_json = dbutils.widgets.get("attributes_mapping_json")
attributes_mapping_json_deduplicate = dbutils.widgets.get("attributes_mapping_json")
entity_attributes = dbutils.widgets.get("entity_attributes")
entity_attributes_datatype=dbutils.widgets.get("entity_attributes_datatype")
attributes = dbutils.widgets.get("attributes")
if(dbutils.widgets.get("processed_records")!=""):
    process_records=ast.literal_eval(dbutils.widgets.get("processed_records"))
else:
    process_records=dbutils.widgets.get("processed_records")
logger.info(type(process_records))
#below inputs are to be passed as notebook params OR add them to entity_json itself
experiment_id = dbutils.widgets.get("experiment_id") # cannot contain "-" for table name
experiment_id = experiment_id.replace("-", "") #remove "-" which is invalid for table name
incremental_load = False
merged_desc_column = "attributes_combined"
catalog_name=dbutils.widgets.get("catalog_name")
is_golden_deduplication=dbutils.widgets.get("is_golden_deduplication")

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
attributes_mapping_json_deduplicate = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "attributes_mapping", debugValue=attributes_mapping_json_deduplicate
)
# validation_functions_json=dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "validation_functions", debugValue=validation_functions)
attributes = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="match_attributes", debugValue=attributes)
entity_attributes_datatype = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes_datatype", debugValue=entity_attributes_datatype)
is_golden_deduplication = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "is_golden_deduplication", debugValue=is_golden_deduplication)

# COMMAND ----------

dataset_tables = json.loads(dataset_tables)
dataset_objects = json.loads(dataset_objects)
entity_attributes = json.loads(entity_attributes)
entity_attributes.append("lakefusion_id")
attributes_mapping_json = json.loads(attributes_mapping_json)
entity_attributes_datatype = json.loads(entity_attributes_datatype)
attributes_mapping_json_deduplicate = json.loads(attributes_mapping_json_deduplicate)
attributes = json.loads(attributes)
# validation_function_json = json.loads(validation_functions_json)

# COMMAND ----------

# MAGIC %run ../utils/parse_utils

# COMMAND ----------

# MAGIC %run ../utils/match_merge_utils

# COMMAND ----------

import logging
if 'logger' not in dir():
    logger = logging.getLogger(__name__)

# COMMAND ----------

unified_table = f"{catalog_name}.silver.{entity}_unified_deduplicate"
master_table = f"{catalog_name}.gold.{entity}_master"
processed_unified_table = f"{catalog_name}.silver.{entity}_processed_unified_deduplicate"
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

def get_last_processed_version(table_meta_info,entity, table_name):
    """
    Get the last processed version of the table from metadata table.
    """
    if spark.catalog.tableExists(table_meta_info):
        meta_df = spark.read.table(table_meta_info).filter(col("table_name") == table_name).filter(col("entity_name")==entity)
        if meta_df.count() > 0:
            return meta_df.select("last_processed_version").first()[0]
    return None

def update_last_processed_version(table_meta_info,entity,table_name, version):
    """
    Update or insert last processed version into metadata table.
    """
    if not spark.catalog.tableExists(table_meta_info):
        # Create the meta info table
        schema = "table_name STRING,entity_name string,last_processed_version LONG, last_processed_timestamp TIMESTAMP"
        spark.sql(f"CREATE TABLE {table_meta_info} ({schema}) USING DELTA")
    
    # Create DataFrame to upsert
    new_row = spark.createDataFrame([(table_name,entity,version, )], ["table_name","entity_name","last_processed_version"])
    new_row = new_row.withColumn("last_processed_timestamp", current_timestamp())
    
    delta_meta = DeltaTable.forName(spark, table_meta_info)
    display(delta_meta)
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

# ---------- Main logic ----------
master_table_exists = spark.catalog.tableExists(master_table)
master_table_created = False
meta_info_table = f"{catalog_name}.silver.table_meta_info"
logger.info(f"meta_info_table {meta_info_table}")

if not master_table_exists:
    logger.info(
        f"Master table for entity: {entity}, doesn't exist. Proceeding with initial load. Creating {master_table}.."
    )
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
        for new_col,value in primary_table_attr_mapping.items():
            if(value is None):
                dtype = entity_attributes_datatype.get(new_col, "string")
                if dtype == 'date':
                   df_master = df_master.withColumn(new_col, lit(None).try_cast("date"))
                else:
                   df_master = df_master.withColumn(new_col, lit(None).cast("string"))
        
                
        df_master = df_master.select([column for column in df_master.columns if column in entity_attributes])
        df_master = df_master.withColumn(merged_desc_column, concat_ws(" | ", *[col(c) for c in attributes]))
       
        if(experiment_id=='prod'):
            df_master.withColumn("lakefusion_id",when(col("lakefusion_id").isNull(), uuid_udf()).otherwise(col("lakefusion_id"))).write.format("delta").option("delta.enableChangeDataFeed", "true").saveAsTable(master_table)
            
        elif(is_golden_deduplication==1):
            df_master.withColumn("lakefusion_id",when(col("lakefusion_id").isNull(), uuid_udf()).otherwise(col("lakefusion_id"))).write.format("delta").option("delta.enableChangeDataFeed", "true").saveAsTable(master_table)
        else:
            df_master.write.format("delta").saveAsTable(master_table)
        #df_master.dropDuplicates([id_key]).write.format("delta").saveAsTable(master_table)
        # # Removing primary table from attributes mapping json and dataset_tables list to avoid adding it to unified_table
        attributes_mapping_json = remove_primary_from_attr_mapping(
            attributes_mapping_json
        )
        #dataset_tables.remove(primary_table)
        master_table_created = True

    else:
        raise ValueError(
            f"No master table- {master_table} or primary dataset table - {primary_table} found!"
        )
else:
    df_master = spark.read.table(master_table)
    logger.info(
        f"Master table- {master_table} found for entity- {entity}. Proceeding with incremental load"
    )
    incremental_load = True
    logger.info(f"Master table- {master_table} found for entity- {entity}. Proceeding with incremental load")

   

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
                    col(col_name).cast("string").alias("attribute_value"),  # Ensure attr_value  is string
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


if experiment_id=='prod':
    check_and_enable_cdf(master_table)
    check_and_enable_cdf(primary_table)

# COMMAND ----------

processed_unified_table_exists = spark.catalog.tableExists(processed_unified_table)
unified_table_exists = spark.catalog.tableExists(unified_table)

# COMMAND ----------

if not unified_table_exists:
    latest_version = spark.sql(f"DESCRIBE HISTORY {primary_table}").selectExpr("max(version)").first()[0]
    update_last_processed_version(meta_info_table,entity,primary_table, latest_version)
    latest_version = spark.sql(f"DESCRIBE HISTORY {master_table}").selectExpr("max(version)").first()[0]
    update_last_processed_version(meta_info_table,entity,master_table, latest_version)

# COMMAND ----------

if(experiment_id=='prod'):
    dbutils.jobs.taskValues.set("master_table", master_table)
    dbutils.jobs.taskValues.set("unified_table", unified_table)
    dbutils.jobs.taskValues.set("entity", entity)
    dbutils.jobs.taskValues.set("experiment_id", experiment_id)
    dbutils.jobs.taskValues.set("id_key", id_key)
    dbutils.jobs.taskValues.set("incremental_load", incremental_load)
    dbutils.notebook.exit(0)

# COMMAND ----------

from pyspark.sql.functions import lit

datasets_df_dict = {}
for mapping_dict, dataset in zip(attributes_mapping_json_deduplicate, dataset_tables):
    if(dataset==primary_table):
        table_name = dataset.split(".").pop()
        mapping_dict[dataset]['lakefusion_id'] = 'lakefusion_id'
        # Read table into DataFrame
       
        # Apply column mappings
        df = df_master.selectExpr(*[f"{v} as {k}" for k, v in mapping_dict[dataset].items()]+["attributes_combined"])
        logger.info(df)
        # Ensure all columns from entity_attributes exist, maintaining order
        existing_columns = df.columns
        new_columns = [col for col in entity_attributes if col not in existing_columns] 
        # Add missing columns in order
        for col in new_columns:
            df = df.withColumn(col, lit(None).cast("string"))  # Add new column with NULL values
        
        # Reorder DataFrame to match entity_attributes order
        final_columns = [col for col in entity_attributes if col in df.columns]
        df = df.select(*final_columns).withColumn('table_name', lit(dataset))
        # Store DataFrame in dictionary
        datasets_df_dict[table_name] = df
        


# COMMAND ----------

dataset_objects_df = spark.createDataFrame(list(dataset_objects.values()))

# COMMAND ----------

from pyspark.sql.functions import col, lit, monotonically_increasing_id

df = spark.read.table(master_table)
if process_records != "":
    df_unified = (
        df.withColumn("row_number_id", monotonically_increasing_id())
        .filter(col("row_number_id").between(int(process_records[0]), int(process_records[1])))
        .drop("row_number_id")
    )
else:
    df_unified = df

df_unified = (
    df_unified.withColumn("table_name", lit(primary_table))
    .withColumn(merged_desc_column, lit(""))
    .withColumn("search_results", lit(""))
    .withColumn("scoring_results", lit(""))
)

# COMMAND ----------

from pyspark.sql.functions import col, struct

joined_unified_df = (
    df_unified.join(dataset_objects_df, df_unified.table_name == dataset_objects_df.path)  # Fix: Remove []
    .select(df_unified['*'], struct(*[col(c) for c in dataset_objects_df.columns]).alias("dataset"))
)


# COMMAND ----------

# Generate merged decription
joined_unified_df = joined_unified_df.withColumn(merged_desc_column, concat_ws(" | ", *[col(c) for c in attributes]))

# COMMAND ----------

if processed_unified_table_exists:
    # Fixed typo: unfied_table_df -> unified_table_df
    from pyspark.sql.functions import col, coalesce

    # Read unified table
    unified_table_df = spark.read.table(unified_table)
    
    # Build list of join keys excluding lakefusion_id
    join_keys = [
        i for i in unified_table_df.columns
        if i in entity_attributes and i != "lakefusion_id"] + ["table_name"]
    
    # Remove duplicates, if any
    join_keys = list(dict.fromkeys(join_keys))
    
    # Aliases for clarity
    left_df = joined_unified_df_processable = (
        joined_unified_df
        .join(spark.read.table(processed_unified_table), [id_key], 'left_anti')
        .join(df_master, [id_key], 'left_anti')
    ).alias("joined_unified_df_processable")
    
    right_df = unified_table_df.alias("unified_table_df")
    
    # Build null-safe join condition
    join_condition = [
        col(f"joined_unified_df_processable.{col_name}")
        .eqNullSafe(col(f"unified_table_df.{col_name}"))
        for col_name in join_keys
    ]
    
    # Perform left join to keep all records from joined_unified_df_processable
    joined_with_unified = left_df.join(
        right_df,
        on=join_condition,
        how="left"
    )

    # Optionally handle overlapping columns using coalesce
    left_cols = set(left_df.columns)
    right_cols = set(right_df.columns)
    all_cols = left_cols.union(right_cols)
    
    # Create select expressions using coalesce for overlapping columns
    select_expressions = []
    for column_name in all_cols:
        if column_name in left_cols and column_name in right_cols:
            select_expressions.append(
                coalesce(
                    right_df[column_name],
                    left_df[column_name]
                ).alias(column_name)
        )
        elif column_name in right_cols:
            select_expressions.append(right_df[column_name])
        else:
            select_expressions.append(left_df[column_name])

    # Final unified DataFrame
    joined_unified_df_processable = joined_with_unified.select(*select_expressions)

    
else:
    joined_unified_df_processable = (
        joined_unified_df
        .join(df_master, [id_key], 'left')
        .select(joined_unified_df['*'])  # Removed extra comma
    )


# COMMAND ----------

if processed_unified_table_exists:
  joined_unified_df_processable = (
    joined_unified_df
    .join(spark.read.table(processed_unified_table), [id_key], 'left')
    .join(df_master, [id_key], 'left')
    .select(joined_unified_df['*'], )
  )
else:
  joined_unified_df_processable = (
    joined_unified_df
    .join(df_master, [id_key], 'left')
    .select(joined_unified_df['*'], )
  )

# COMMAND ----------

joined_unified_df_processable_deduplicated = (
    joined_unified_df_processable
    .dropDuplicates([id_key])
)

# COMMAND ----------

if not unified_table_exists:
  joined_unified_df_processable_deduplicated.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(unified_table)
else:
  # Define the Delta table
  delta_table = DeltaTable.forName(spark, unified_table)

  # Perform merge operation
  delta_table.alias("target").merge(
      source=joined_unified_df_processable_deduplicated.alias("source"),
      condition=f"target.{id_key} = source.{id_key}"
  ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

dbutils.jobs.taskValues.set("master_table", master_table)
dbutils.jobs.taskValues.set("unified_table", unified_table)
dbutils.jobs.taskValues.set("entity", entity)
dbutils.jobs.taskValues.set("experiment_id", experiment_id)
dbutils.jobs.taskValues.set("id_key", id_key)
dbutils.jobs.taskValues.set("incremental_load", incremental_load)

# COMMAND ----------

num_cols = len(joined_unified_df_processable_deduplicated.columns)+1
spark.sql(f"ALTER TABLE {unified_table} SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '{num_cols}')")
optimise_res = spark.sql(f"OPTIMIZE {unified_table} ZORDER BY ({merged_desc_column})")
num_cols = len(df_master.columns)+1
spark.sql(f"ALTER TABLE {master_table} SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '{num_cols}')")
optimise_res = spark.sql(f"OPTIMIZE {master_table} ZORDER BY ({id_key})")
