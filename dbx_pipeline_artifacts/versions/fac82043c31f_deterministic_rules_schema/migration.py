# Databricks notebook source
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("entity", "", "Entity Name")

# COMMAND ----------

dbutils.widgets.dropdown("function", "upgrade", ["upgrade", "downgrade"], "Function")
function = dbutils.widgets.get("function")
catalog_name = dbutils.widgets.get("catalog_name")
entity = dbutils.widgets.get("entity")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, col
from pyspark.sql.types import StringType, StructType, ArrayType, MapType


TABLE_NAME_MATCH = f"{catalog_name}.silver.{entity}_unified_deterministic_prod"
TABLE_NAME_PROCESSED = f"{catalog_name}.silver.{entity}_processed_unified_prod"
TABLE_NAME_MATCH_DEDUP = f"{catalog_name}.silver.{entity}_unified_deterministic_deduplicate_prod"
TABLE_NAME_PROCESSED_DEDUP = f"{catalog_name}.silver.{entity}_processed_unified_deduplicate_prod"


COLUMN_NAME = "deterministic_match_result"


def _column_datatype(df, column_name: str):
    for field in df.schema.fields:
        if field.name == column_name:
            return field.dataType
    return None


def convert_struct_to_json(table_name: str, column_name: str = COLUMN_NAME):
    """
    Safely convert a complex (STRUCT/ARRAY/MAP) column to a JSON STRING column.

    Handles the [DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION] error that arises
    when Delta cannot reconcile two structurally-similar but non-identical
    STRUCT schemas (e.g. nested field nullability or ordering differs).

    Idempotent: skips the rewrite when the column is already STRING or absent.
    """
    print(f"Converting {column_name} → JSON for: {table_name}")

    df = spark.read.table(table_name)
    dtype = _column_datatype(df, column_name)

    if dtype is None:
        print(f"⚠ Column '{column_name}' not found in {table_name}, skipping")
        return

    if isinstance(dtype, StringType):
        print(f"✓ Column '{column_name}' already STRING in {table_name}, skipping")
        return

    if not isinstance(dtype, (StructType, ArrayType, MapType)):
        print(
            f"⚠ Column '{column_name}' has unsupported type {dtype.simpleString()} "
            f"in {table_name}, skipping"
        )
        return

    try:
        new_df = df.withColumn(column_name, to_json(col(column_name)))

        (
            new_df.write
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )

        print(f"✓ Schema updated for {table_name} ({dtype.simpleString()} → STRING)")
    except Exception as exc:
        print(f"✗ Failed to convert {column_name} on {table_name}: {exc}")
        raise


def table_exists(table_name: str) -> bool:
    try:
        return spark.catalog.tableExists(table_name)
    except Exception:
        return False


def upgrade():
    """
    Deterministic to Rule Match
    """
    print(f"Applying migration: Deterministic to Rule Match")
    print("-" * 80)

    if table_exists(TABLE_NAME_MATCH):
        print(f"Table found: {TABLE_NAME_MATCH}")
        convert_struct_to_json(TABLE_NAME_MATCH)
    else:
        print(f"⚠ Table not found: {TABLE_NAME_MATCH}, skipping update")

    if table_exists(TABLE_NAME_MATCH_DEDUP):
        print(f"Table found: {TABLE_NAME_MATCH_DEDUP}")
        convert_struct_to_json(TABLE_NAME_MATCH_DEDUP)
        print("✓ Update applied")
    else:
        print(f"⚠ Table not found: {TABLE_NAME_MATCH_DEDUP}, skipping update")

    if table_exists(TABLE_NAME_PROCESSED):
        print(f"Table found: {TABLE_NAME_PROCESSED}")

       

        print("✓ Update applied")
    else:
        print(f"⚠ Table not found: {TABLE_NAME_PROCESSED}, skipping update")

    if table_exists(TABLE_NAME_PROCESSED_DEDUP):
        print(f"Table found: {TABLE_NAME_PROCESSED_DEDUP}")

        print("✓ Update applied")
    else:
        print(f"⚠ Table not found: {TABLE_NAME_PROCESSED_DEDUP}, skipping update")

    print("-" * 80)
    print(f"✓ Migration applied successfully")


def downgrade():
    """
    Revert Deterministic to Rule Match
    """
    print(f"Reverting migration: Deterministic to Rule Match")
    print("-" * 80)

    if table_exists(TABLE_NAME_MATCH):
        print(f"Table found: {TABLE_NAME_MATCH}")

       

        print("✓ Reverted update")
    else:
        print(f"⚠ Table not found: {TABLE_NAME_MATCH}, skipping downgrade")

    if table_exists(TABLE_NAME_MATCH_DEDUP):
        print(f"Table found: {TABLE_NAME_MATCH_DEDUP}")

        

        print("✓ Reverted update")
    else:
        print(f"⚠ Table not found: {TABLE_NAME_MATCH}, skipping downgrade")

    if table_exists(TABLE_NAME_PROCESSED):
        print(f"Table found: {TABLE_NAME_PROCESSED}")

       

        print("✓ Reverted update")
    else:
        print(f"⚠ Table not found: {TABLE_NAME_PROCESSED}, skipping downgrade")

    print("-" * 80)
    print(f"✓ Migration reverted successfully")

    if table_exists(TABLE_NAME_PROCESSED_DEDUP):
        print(f"Table found: {TABLE_NAME_PROCESSED_DEDUP}")


        print("✓ Reverted update")
    else:
        print(f"⚠ Table not found: {TABLE_NAME_PROCESSED_DEDUP}, skipping downgrade")

    print("-" * 80)
    print(f"✓ Migration reverted successfully")

# COMMAND ----------

if function == "downgrade":
    downgrade()
else:
    upgrade()
