# Databricks notebook source
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.dropdown("function", "upgrade", ["upgrade", "downgrade"], "Function")

# COMMAND ----------

function = dbutils.widgets.get("function")
catalog_name = dbutils.widgets.get("catalog_name")
entity = dbutils.widgets.get("entity")
experiment_id = dbutils.widgets.get("experiment_id")

# COMMAND ----------

"""
Migration: Add attributes_combined_embedding column
Revision: e89a756ad581_add_embedding_column

Adds ARRAY<FLOAT> column to master, unified, and unified_deduplicate tables
for precomputed embedding mode. Column starts NULL — Compute_Embeddings.py
populates it on first precomputed-mode pipeline run.

Forward-only: Delta column mapping is not enabled on these tables,
so DROP COLUMN is not supported for downgrade.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

experiment_suffix = f"_{experiment_id}" if experiment_id else ""

master_table = f"{catalog_name}.gold.{entity}_master{experiment_suffix}"
unified_table = f"{catalog_name}.silver.{entity}_unified{experiment_suffix}"
unified_dedup_table = f"{catalog_name}.silver.{entity}_unified_deduplicate{experiment_suffix}"


def table_exists(table_name: str) -> bool:
    try:
        return spark.catalog.tableExists(table_name)
    except Exception:
        return False


def has_column(table_name: str, column_name: str) -> bool:
    """Check if column exists (case-insensitive, stripped)."""
    cols_lower = {f.name.strip().lower() for f in spark.table(table_name).schema}
    return column_name.lower() in cols_lower


def add_embedding_column(table_name: str):
    """Add attributes_combined_embedding ARRAY<FLOAT> if it doesn't already exist."""
    if not table_exists(table_name):
        print(f"  Table not found: {table_name}, skipping")
        return

    if has_column(table_name, "attributes_combined_embedding"):
        print(f"  Column already exists in {table_name}, skipping")
        return

    spark.sql(f"""
        ALTER TABLE {table_name}
        ADD COLUMNS (attributes_combined_embedding ARRAY<FLOAT>)
    """)
    print(f"  Added attributes_combined_embedding to {table_name}")


def upgrade():
    """
    Add attributes_combined_embedding ARRAY<FLOAT> to master and unified tables.
    Idempotent: skips if column already exists (e.g., added by Compute_Embeddings.py).
    """
    print("=" * 80)
    print("MIGRATION: Add attributes_combined_embedding column")
    print("=" * 80)
    print(f"Revision: e89a756ad581_add_embedding_column")
    print(f"Entity: {entity}")
    print(f"Catalog: {catalog_name}")
    print(f"Experiment suffix: '{experiment_suffix}'")
    print(f"Master table: {master_table}")
    print(f"Unified table: {unified_table}")
    print(f"Unified dedup table: {unified_dedup_table}")
    print("-" * 80)

    add_embedding_column(master_table)
    add_embedding_column(unified_table)
    add_embedding_column(unified_dedup_table)

    print("-" * 80)
    print("Migration applied successfully")


def downgrade():
    """
    Not supported — Delta column mapping is not enabled on master/unified tables,
    so DROP COLUMN would fail. This migration is forward-only.
    """
    raise NotImplementedError(
        "Downgrade not supported for e89a756ad581_add_embedding_column. "
        "Delta column mapping mode is not enabled on master/unified tables, "
        "so DROP COLUMN is not available. The column is harmless when unused — "
        "managed-mode entities ignore it entirely."
    )


# COMMAND ----------

if function == "downgrade":
    downgrade()
else:
    upgrade()
