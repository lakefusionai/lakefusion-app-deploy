# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql.functions import col

def get_current_table_version(table_name: str) -> int:
    """
    Fetches the current version of a Delta table using the Unity Catalog table name.

    Parameters:
    - spark (SparkSession): The active Spark session.
    - table_name (str): The Unity Catalog table name in 'catalog.schema.table' format.

    Returns:
    - int: The current version of the Delta table.
    """
    try:
        # Initialize the DeltaTable object using the table name
        delta_table = DeltaTable.forName(spark, table_name)
        
        # Fetch the latest history and get the current version
        latest_version = (
            delta_table.history()
            .orderBy(col("version").desc())
            .limit(1)
            .collect()[0]["version"]
        )
        
        return latest_version
    except Exception as e:
        print(f"Error fetching table version for {table_name}: {e}")
        return -1


# COMMAND ----------

from enum import Enum

class ActionType(Enum):
    MANUAL_MERGE = "MANUAL_MERGE"
    MANUAL_UNMERGE = "MANUAL_UNMERGE"
    MANUAL_NOT_A_MATCH = "MANUAL_NOT_A_MATCH"
    JOB_MERGE = "JOB_MERGE"
    JOB_NOT_A_MATCH = "JOB_NOT_A_MATCH"
    JOB_INSERT = "JOB_INSERT"
    INITIAL_LOAD = "INITIAL_LOAD"
    MANUAL_EDIT = "MANUAL_EDIT"
    MANUAL_UPDATE = "MANUAL_UPDATE"
    SOURCE_DELETE = "SOURCE_DELETE"
    SOURCE_UPDATE = "SOURCE_UPDATE"
    MASTER_JOB_MERGE = "MASTER_JOB_MERGE"
    MASTER_MANUAL_MERGE = "MASTER_MANUAL_MERGE"
    MASTER_FORCE_MERGE="MASTER_FORCE_MERGE"

# COMMAND ----------

def is_numeric_type(datatype_str):
    """Check if datatype string is numeric"""
    numeric_types = [
        'int', 'integer', 'bigint', 'smallint', 'tinyint',
        'float', 'double', 'decimal', 'numeric', 'real'
    ]
    return any(datatype_str.lower().startswith(t) for t in numeric_types)

def is_timestamp_type(datatype_str):
    """Check if datatype string is timestamp or date"""
    datetime_types = ['timestamp', 'date', 'datetime']
    return any(datatype_str.lower().startswith(t) for t in datetime_types)

def is_string_type(datatype_str):
    """Check if datatype string is string/varchar/char"""
    string_types = ['varchar', 'char', 'string', 'text']
    return any(datatype_str.lower().startswith(t) for t in string_types)
