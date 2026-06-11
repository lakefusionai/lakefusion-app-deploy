# Databricks notebook source
dbutils.widgets.dropdown("function", "upgrade", ["upgrade", "downgrade"], "Function")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("entity_attributes_datatype", "", "Entity Attributes Datatype JSON")
dbutils.widgets.text("experiment_id", "", "Experiment ID")

# COMMAND ----------

function = dbutils.widgets.get("function")
catalog_name = dbutils.widgets.get("catalog_name")
entity = dbutils.widgets.get("entity")
entity_attributes_datatype = dbutils.widgets.get("entity_attributes_datatype")
experiment_id = dbutils.widgets.get("experiment_id")

# COMMAND ----------

"""
Migration: Ensure master, unified, and index tables have datatypes 
matching entity_attributes_datatype

Method: Read table -> Cast columns to expected types -> Overwrite table
"""

import json
from pyspark.sql.functions import col

# COMMAND ----------

try:
    attr_datatypes = json.loads(entity_attributes_datatype) if entity_attributes_datatype else {}
except:
    attr_datatypes = {}

print("="*70)
print("EXPECTED DATATYPES FROM ENTITY DEFINITION")
print("="*70)
for attr, dtype in attr_datatypes.items():
    print(f"  {attr}: {dtype}")

# COMMAND ----------

# Type mapping: entity_attributes_datatype value -> Spark SQL cast type
SPARK_CAST_TYPE = {
    # Uppercase (new format)
    'BIGINT': 'BIGINT',
    'BOOLEAN': 'BOOLEAN',
    'DATE': 'DATE',
    'DOUBLE': 'DOUBLE',
    'FLOAT': 'FLOAT',
    'INT': 'INT',
    'SMALLINT': 'SMALLINT',
    'STRING': 'STRING',
    'TINYINT': 'TINYINT',
    'TIMESTAMP': 'TIMESTAMP',
    # Lowercase (legacy, just in case)
    'bigint': 'BIGINT',
    'boolean': 'BOOLEAN',
    'date': 'DATE',
    'double': 'DOUBLE',
    'float': 'FLOAT',
    'int': 'INT',
    'integer': 'INT',
    'long': 'BIGINT',
    'smallint': 'SMALLINT',
    'string': 'STRING',
    'tinyint': 'TINYINT',
    'timestamp': 'TIMESTAMP',
    'varchar': 'STRING',
    'text': 'STRING',
}

SPARK_CLASS_TO_SQL = {
    'LongType': 'BIGINT',
    'BooleanType': 'BOOLEAN',
    'DateType': 'DATE',
    'DoubleType': 'DOUBLE',
    'FloatType': 'FLOAT',
    'IntegerType': 'INT',
    'ShortType': 'SMALLINT',
    'StringType': 'STRING',
    'ByteType': 'TINYINT',
    'TimestampType': 'TIMESTAMP',
    'TimestampNTZType': 'TIMESTAMP',
}

def get_cast_type(dtype_str):
    """Convert entity attribute type to Spark SQL cast type."""
    if not dtype_str:
        return 'STRING'
    return SPARK_CAST_TYPE.get(dtype_str, SPARK_CAST_TYPE.get(dtype_str.upper(), 'STRING'))

def get_current_type(spark_type):
    """Get SQL type name from Spark DataType."""
    return SPARK_CLASS_TO_SQL.get(type(spark_type).__name__, 'STRING')

# COMMAND ----------

def migrate_table(table_name, attr_datatypes):
    """
    Ensure table columns match entity_attributes_datatype.
    
    Returns: (exists, columns_fixed, error_message)
    """
    # Check if table exists
    if not spark.catalog.tableExists(table_name):
        return (False, 0, None)
    
    df = spark.table(table_name)
    schema = df.schema
    
    # Build a map of current column types (lowercase name -> (actual_name, current_type))
    current_columns = {}
    for field in schema.fields:
        current_columns[field.name.lower()] = (field.name, get_current_type(field.dataType))
    
    # Find columns that need conversion
    columns_to_fix = {}
    for attr_name, expected_dtype in attr_datatypes.items():
        if attr_name.lower() not in current_columns:
            continue  # Column doesn't exist in this table, skip
        
        actual_name, current_type = current_columns[attr_name.lower()]
        target_type = get_cast_type(expected_dtype)
        
        if current_type.upper() != target_type.upper():
            columns_to_fix[actual_name] = (current_type, target_type)
    
    # If no columns need fixing, we're done
    if not columns_to_fix:
        return (True, 0, None)
    
    print(f"  Columns to fix:")
    for col_name, (current, target) in columns_to_fix.items():
        print(f"    {col_name}: {current} -> {target}")
    
    try:
        # Build select expressions: cast columns that need fixing, keep others as-is
        select_exprs = []
        for field in schema.fields:
            if field.name in columns_to_fix:
                _, target_type = columns_to_fix[field.name]
                select_exprs.append(col(field.name).cast(target_type).alias(field.name))
            else:
                select_exprs.append(col(field.name))
        
        # Apply casts
        df_fixed = df.select(*select_exprs)
        
        # Check if CDF is enabled (to preserve it)
        cdf_enabled = False
        try:
            props = spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()
            cdf_enabled = any(
                row['key'] == 'delta.enableChangeDataFeed' and row['value'].lower() == 'true'
                for row in props
            )
        except:
            pass
        
        # Overwrite table with fixed schema
        writer = df_fixed.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        
        if cdf_enabled:
            writer = writer.option("delta.enableChangeDataFeed", "true")
        
        writer.saveAsTable(table_name)
        
        return (True, len(columns_to_fix), None)
        
    except Exception as e:
        return (True, 0, str(e))

# COMMAND ----------

def upgrade():
    print("\n" + "="*70)
    print(f"MIGRATION: ALIGN TABLE SCHEMAS TO ENTITY DEFINITION")
    print("="*70)
    print(f"Entity: {entity}")
    print(f"Catalog: {catalog_name}")
    print(f"Experiment: {experiment_id if experiment_id else 'prod'}")
    print("="*70)
    
    if not entity or not catalog_name or not attr_datatypes:
        print("\n⚠️  Missing required parameters, skipping migration")
        return
    
    # Build table names
    experiment_suffix = f"_{experiment_id}" if experiment_id else ""
    
    tables = [
        f"{catalog_name}.gold.{entity}_master{experiment_suffix}",
        f"{catalog_name}.silver.{entity}_unified{experiment_suffix}",
        f"{catalog_name}.silver.{entity}_unified{experiment_suffix}_deduplicate",
        f"{catalog_name}.gold.{entity}_master{experiment_suffix}_potential_match",
        f"{catalog_name}.gold.{entity}_master{experiment_suffix}_potential_match_deduplicate",
    ]
    
    total_fixed = 0
    errors = []
    
    for table in tables:
        table_short = table.split('.')[-1]
        print(f"\n[{table_short}]")
        
        exists, fixed, error = migrate_table(table, attr_datatypes)
        
        if not exists:
            print(f"  ⏭️  Table does not exist, skipping")
            continue
        
        if error:
            print(f"  ❌ Error: {error}")
            errors.append(f"{table_short}: {error}")
            continue
        
        if fixed > 0:
            print(f"  ✅ Fixed {fixed} column(s)")
            total_fixed += fixed
        else:
            print(f"  ✅ Schema already matches entity definition")
    
    # Summary
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    
    if errors:
        print(f"❌ Migration completed with {len(errors)} error(s):")
        for err in errors:
            print(f"   - {err}")
        raise Exception(f"Migration failed with {len(errors)} error(s)")
    else:
        print(f"✅ Migration completed successfully")
        print(f"   Total columns fixed: {total_fixed}")


def downgrade():
    print("Downgrade: No action (would require original type information)")

# COMMAND ----------

if function == "downgrade":
    downgrade()
else:
    upgrade()
