# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

transactional_db_json_path = 'dbfs:/dbfs/lakefusion_ai/transactional_db.json'

# COMMAND ----------

df = spark.read.json(transactional_db_json_path)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType

def flatten_column(df, column_name):
    """
    Flatten the given column of the DataFrame. If the column is an array of structs, 
    explode the array and flatten the struct fields inside it.
    """
    # Get the column data type
    column_type = df.schema[column_name].dataType
    
    if isinstance(column_type, ArrayType) and isinstance(column_type.elementType, StructType):
        # If it's an array of struct, explode the array and flatten the struct fields inside it
        df_exploded = df.select(F.explode(F.col(column_name)).alias(f"{column_name}_exploded"))
        
        # Flatten the struct inside the exploded array
        struct_columns = [F.col(f"{column_name}_exploded.{field.name}").alias(f"{field.name}")
                          for field in column_type.elementType.fields]
        
        return df_exploded.select(*struct_columns)
    
    else:
        # If it's a non-complex column, just return it
        return df.select(column_name)

def split_and_flatten_columns(df):
    """
    Split and flatten each column of the DataFrame into separate DataFrames.
    Returns a dictionary of flattened DataFrames.
    """
    flattened_dfs = {}
    
    for col in df.columns:
        # Flatten the column and create a DataFrame for it
        flattened_df = flatten_column(df, col)
        
        # Store the flattened DataFrame in the dictionary with a dynamic name
        flattened_dfs[col] = flattened_df
    
    return flattened_dfs

# Example usage: Assume `df` is your DataFrame with complex types
flattened_dfs = split_and_flatten_columns(df)

# COMMAND ----------

flattened_dfs.keys()

# COMMAND ----------

# Push the flattened dataFrames to the tables individually
schema = 'lakefusion_ai.transactional_db'
for table_name, df_ in flattened_dfs.items():
    print(f"Pushing {table_name}:")
    table_name_delta = f'{schema}.{table_name}'
    df_.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(table_name_delta)
