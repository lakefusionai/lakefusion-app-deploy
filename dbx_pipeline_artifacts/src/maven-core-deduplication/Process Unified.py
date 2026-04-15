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

from pyspark.sql.functions import lit, current_timestamp
from delta.tables import DeltaTable

meta_info_table = f"{catalog_name}.silver.table_meta_info"

def get_last_processed_version(table_meta_info,entity_name, table_name):
    if spark.catalog.tableExists(table_meta_info):
        meta_df = spark.read.table(table_meta_info).filter(col("table_name") == table_name).filter(col("entity_name")==entity_name)
        if meta_df.count() > 0:
            return meta_df.select("last_processed_version").first()[0]
    return None

def update_last_processed_version(table_meta_info,entity_name, table_name, version):
    if not spark.catalog.tableExists(table_meta_info):
        schema = "entity_name string, table_name STRING, last_processed_version LONG, last_processed_timestamp TIMESTAMP"
        spark.sql(f"CREATE TABLE {table_meta_info} ({schema}) USING DELTA")

    new_row = spark.createDataFrame([(entity_name,table_name, version)], ["entity_name","table_name", "last_processed_version"])
    new_row = new_row.withColumn("last_processed_timestamp", current_timestamp())

    delta_meta = DeltaTable.forName(spark, table_meta_info)

    delta_meta.alias("target").merge(
        new_row.alias("source"),
        condition=f"target.table_name = source.table_name and target.entity_name = source.entity_name"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

datasets_df_dict = {}
df_master=spark.read.table(master_table)
for mapping_dict, dataset in zip(attributes_mapping_json_deduplicate, dataset_tables):
    if dataset == primary_table:
        table_name = dataset.split(".").pop()
        mapping_dict[dataset]['lakefusion_id'] = 'lakefusion_id'
        # Determine last processed version
        df = spark.read.table(master_table)
       
        logger.info(mapping_dict[dataset])
        # Apply column mappings
        df = df.selectExpr(*[f"{v} as {k}" for k, v in mapping_dict[dataset].items()] + ["attributes_combined"])
        

        # Ensure all columns from entity_attributes exist
        existing_columns = df.columns
        new_columns = [col for col in entity_attributes if col not in existing_columns]
        for col_name in new_columns:
            df = df.withColumn(col_name, lit(None).cast("string"))

        # Reorder columns to match entity_attributes
        final_columns = [col for col in entity_attributes if col in df.columns]
        df = df.select(*final_columns).withColumn("table_name", lit(dataset))

        # Store DataFrame
        datasets_df_dict[table_name] = df



# COMMAND ----------

dataset_objects_df = spark.createDataFrame(list(dataset_objects.values()))

# COMMAND ----------

from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.window import Window
df = spark.read.table(master_table)
if process_records != "":
    df_unified = (
        df.withColumn("row_number_id", row_number().over(Window.orderBy(lit(1))))
        .filter(col("row_number_id").between(int(process_records[0]), int(process_records[1])))
        .drop("row_number_id")
    )
    
else:
   df_unified=df
df_unified = (
    df_unified.withColumn("table_name",lit(primary_table)).withColumn(merged_desc_column, lit(""))
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

processed_unified_table_exists = spark.catalog.tableExists(processed_unified_table)
unified_table_exists = spark.catalog.tableExists(unified_table)


# COMMAND ----------

from pyspark.sql.functions import col, coalesce

if processed_unified_table_exists:
    # Read unified table
    unified_table_df = spark.read.table(unified_table)
    
    # Build list of join keys excluding lakefusion_id
    join_keys = [
        i for i in unified_table_df.columns
        if i in entity_attributes and i != "lakefusion_id"
    ] + ["table_name"]
    join_keys = list(dict.fromkeys(join_keys))  # remove duplicates
    
    # Prepare processed_unified_table and df_master with suffixes to avoid column clashes
    processed_df = spark.read.table(processed_unified_table).select(
        *[col(c).alias(f"{c}_processed") if c != id_key else col(c) for c in spark.read.table(processed_unified_table).columns]
    )
    
    df_master_renamed = df_master.select(
        *[col(c).alias(f"{c}_master") if c != id_key else col(c) for c in df_master.columns]
    )
    
    # Left DataFrame: join without duplicating column names
    left_df = (
        joined_unified_df
        .join(processed_df, [id_key], 'left')
        .join(df_master_renamed, [id_key], 'left')
        .alias("joined_unified_df_processable")
    )
    
    right_df = unified_table_df.alias("unified_table_df")
    
    # Build null-safe join condition
    join_condition = [
        col(f"joined_unified_df_processable.{col_name}")
        .eqNullSafe(col(f"unified_table_df.{col_name}"))
        for col_name in join_keys
        if col_name in left_df.columns and col_name in right_df.columns
    ]
    
    # Perform left join to keep all records from left_df
    joined_with_unified = left_df.join(
        right_df,
        on=join_condition,
        how="left"
    )
    
    # Handle overlapping columns using coalesce
    left_cols = set(left_df.columns)
    right_cols = set(right_df.columns)
    all_cols = left_cols.union(right_cols)
    
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
        .select(joined_unified_df['*'])
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
  ).whenMatchedUpdateAll().whenNotMatchedInsertAll().whenNotMatchedBySourceDelete().execute()

# COMMAND ----------

dbutils.jobs.taskValues.set("master_table", master_table)
dbutils.jobs.taskValues.set("unified_table", unified_table)
dbutils.jobs.taskValues.set("entity", entity)
dbutils.jobs.taskValues.set("experiment_id", experiment_id)
dbutils.jobs.taskValues.set("id_key", id_key)
dbutils.jobs.taskValues.set("incremental_load", incremental_load)

# COMMAND ----------

try:
    optimise_res = spark.sql(f"OPTIMIZE {unified_table} ZORDER BY ({id_key}, {merged_desc_column})")
    logger.info(f"Successfully optimized table {unified_table}")
except Exception as e:
    logger.error(f"Failed to optimize table {unified_table}: {str(e)}")
    optimise_res = None

# COMMAND ----------

try:
    optimise_res = spark.sql(f"OPTIMIZE {master_table} ZORDER BY ({id_key})")
    logger.info(f"Successfully optimized table {unified_table}")
except Exception as e:
    logger.error(f"Failed to optimize table {unified_table}: {str(e)}")
    optimise_res = None
