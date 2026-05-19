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

dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("id_key", "", "lakefusion Primary Key")
dbutils.widgets.text("primary_key", "", "Primary Key")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text('validation_functions', '', 'Validation Functions')

# COMMAND ----------

entity = dbutils.widgets.get("entity")
id_key = dbutils.widgets.get("id_key")
primary_key = dbutils.widgets.get("primary_key")
catalog_name=dbutils.widgets.get("catalog_name")
validation_functions=dbutils.widgets.get("validation_functions")

# COMMAND ----------

validation_functions=dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "validation_functions", debugValue=validation_functions)
entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)

# COMMAND ----------

# MAGIC %run ../utils/validation_functions_constant

# COMMAND ----------

import json

# If validation_functions is a string, convert it to list of dicts
if isinstance(validation_functions, str):
    validation_functions = json.loads(validation_functions)

validation_function_warning = []
# Process each validation function
for item in validation_functions:
    if item["validation_level"] == "warning":
        func_def = item["function_definition"]
        validation_id=item["validation_id"]
        
        # Fix formatting: restore newlines + strip spaces
        func_def_cleaned = func_def.replace("     ", "\n    ").replace('"""     ', '"""\n    ').strip()
        
        validation_function_warning.append({
            "attribute_name": item["attribute_name"],
            "function_name": item["function_name"],
            "validation_id":item["validation_id"],
            "function_definition": func_def_cleaned,
            "validation_message": item.get("validation_message", None)
        })


# COMMAND ----------

if(len(validation_function_warning)==0):
    dbutils.notebook.exit('0')

# COMMAND ----------

id_key='lakefusion_id'

# COMMAND ----------

from pyspark.sql.functions import *
master_table=f'{catalog_name}.gold.{entity}_master_prod'
master_warning_table=f'{catalog_name}.gold.{entity}_master_warning_prod'
df_master = spark.read.table(master_table)
df_master_validation_warning = df_master.select(
    col(id_key), 
    *[i['attribute_name'] for i in validation_function_warning]
)

# COMMAND ----------

# MAGIC %run ../utils/parse_utils

# COMMAND ----------

df_master_validation_warning = validation_function_exec(
        validation_function_warning, 
        df_master_validation_warning
    )

# COMMAND ----------

from pyspark.sql import functions as F
# build array of structs (one per validation column)
validation_exprs = []
for v in validation_function_warning:
    attr = v["attribute_name"]
    if attr == "lakefusion_id":
        continue
    flag_col = f"is_valid_{attr}" 
    validation_exprs.append(
        F.when(F.col(flag_col) == True,
               F.struct(
                   F.col("lakefusion_id").alias("lakefusion_id"),
                   F.lit(attr).alias("attribute_name"),
                   F.lit(v["validation_id"]).alias("validation_function_id"),
                   F.lit(v.get("status", "REVIEW")).alias("status")
                  
               )
        )
    )
# create an array column of all possible failures
df_with_array = df_master_validation_warning.withColumn(
    "validation_failures",
    F.array(*validation_exprs)
)

# explode array into rows, filter out nulls (valid cases)
df_master_validation_warning_filtered = (
    df_with_array
    .withColumn("failure", F.explode("validation_failures"))
    .select("failure.*")
    .filter(F.col("attribute_name").isNotNull())
)

# COMMAND ----------

master_warning_table_exists = spark.catalog.tableExists(master_warning_table)

# COMMAND ----------

if not master_warning_table_exists:
   df_master_validation_warning_filtered.write.mode('overwrite').saveAsTable(f'{master_warning_table}')
else:
    delta_table = DeltaTable.forName(spark, master_warning_table)
    id_cols = ["lakefusion_id", "attribute_name", "validation_function_id","status"]

    merge_condition = " AND ".join([f"target.{c} = source.{c}" for c in id_cols])

    (
        delta_table.alias("target")
        .merge(
            source=df_master_validation_warning_filtered.alias("source"),
            condition=merge_condition
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceUpdate(set={"status": "'DONE'"})
        .execute()
    )
