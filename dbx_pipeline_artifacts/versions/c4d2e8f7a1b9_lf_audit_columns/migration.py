# Databricks notebook source
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID (optional)")

# COMMAND ----------

dbutils.widgets.dropdown("function", "upgrade", ["upgrade", "downgrade"], "Function")
function = dbutils.widgets.get("function")
catalog_name = dbutils.widgets.get("catalog_name")
entity = dbutils.widgets.get("entity")
experiment_id = dbutils.widgets.get("experiment_id")

# COMMAND ----------

"""
Migration: Add __lf_created_at and __lf_modified_at audit columns to unified table
Revision: c4d2e8f7a1b9_lf_audit_columns

These columns are reserved system columns (double-underscore prefix avoids
collision with user-defined entity attributes like `created_at` / `modified_at`).

The survivorship engine reads `__lf_modified_at` as a deterministic tie-breaker
when multiple records share the highest priority slot (e.g. Source System
strategy with two records from the same primary source). The latest record
wins.
"""

from pyspark.sql.functions import col, current_timestamp, coalesce, lit, max as spark_max, min as spark_min, when

UNIFIED_TABLE = f"{catalog_name}.silver.{entity}_unified"
if experiment_id:
    UNIFIED_TABLE = f"{UNIFIED_TABLE}_{experiment_id}"

LF_CREATED = "__lf_created_at"
LF_MODIFIED = "__lf_modified_at"

# COMMAND ----------

def table_exists(table_name: str) -> bool:
    try:
        return spark.catalog.tableExists(table_name)
    except Exception:
        return False


def column_exists(table_name: str, column_name: str) -> bool:
    schema = spark.table(table_name).schema
    return any(f.name == column_name for f in schema.fields)


def cdf_enabled(table_name: str) -> bool:
    try:
        props = spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()
        for r in props:
            if r["key"] == "delta.enableChangeDataFeed" and str(r["value"]).lower() == "true":
                return True
    except Exception:
        pass
    return False


def add_columns_if_missing(table_name: str) -> None:
    needed = []
    if not column_exists(table_name, LF_CREATED):
        needed.append(f"{LF_CREATED} TIMESTAMP")
    if not column_exists(table_name, LF_MODIFIED):
        needed.append(f"{LF_MODIFIED} TIMESTAMP")
    if not needed:
        print(f"  Columns already present on {table_name}")
        return
    add_clause = ", ".join(needed)
    print(f"  ALTER TABLE {table_name} ADD COLUMNS ({add_clause})")
    spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({add_clause})")


def backfill_from_existing_columns(table_name: str) -> None:
    """First-pass backfill: copy from the legacy `created_at`/`updated_at`
    columns if they exist. Anything still null after this is filled with
    ``current_timestamp()`` so the columns are NOT NULL going forward.
    """
    fields = {f.name for f in spark.table(table_name).schema.fields}
    has_created = "created_at" in fields
    has_updated = "updated_at" in fields

    if has_created and has_updated:
        spark.sql(f"""
            UPDATE {table_name}
            SET {LF_CREATED} = COALESCE({LF_CREATED}, created_at, current_timestamp()),
                {LF_MODIFIED} = COALESCE({LF_MODIFIED}, updated_at, created_at, current_timestamp())
            WHERE {LF_CREATED} IS NULL OR {LF_MODIFIED} IS NULL
        """)
    elif has_created:
        spark.sql(f"""
            UPDATE {table_name}
            SET {LF_CREATED} = COALESCE({LF_CREATED}, created_at, current_timestamp()),
                {LF_MODIFIED} = COALESCE({LF_MODIFIED}, created_at, current_timestamp())
            WHERE {LF_CREATED} IS NULL OR {LF_MODIFIED} IS NULL
        """)
    else:
        spark.sql(f"""
            UPDATE {table_name}
            SET {LF_CREATED} = COALESCE({LF_CREATED}, current_timestamp()),
                {LF_MODIFIED} = COALESCE({LF_MODIFIED}, current_timestamp())
            WHERE {LF_CREATED} IS NULL OR {LF_MODIFIED} IS NULL
        """)
    print(f"  ✓ Backfill from legacy columns complete on {table_name}")


def refine_from_cdf(table_name: str) -> None:
    """Best-effort refinement: if Change Data Feed is enabled on the table,
    use it to refine `__lf_created_at` to the earliest insert commit time
    and `__lf_modified_at` to the latest write commit time per surrogate_key.

    Skipped silently if CDF is disabled or unavailable.
    """
    if not cdf_enabled(table_name):
        print(f"  CDF not enabled on {table_name} - skipping history refinement")
        return

    try:
        cdf_df = (
            spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", 0)
            .table(table_name)
        )
        cdf_agg = (
            cdf_df.filter(col("_change_type").isin("insert", "update_postimage"))
            .groupBy("surrogate_key")
            .agg(
                spark_min(when(col("_change_type") == "insert", col("_commit_timestamp"))).alias("cdf_created_at"),
                spark_max(col("_commit_timestamp")).alias("cdf_modified_at"),
            )
        )
        cdf_agg.createOrReplaceTempView("__lf_cdf_agg")

        spark.sql(f"""
            MERGE INTO {table_name} AS t
            USING __lf_cdf_agg AS s
            ON t.surrogate_key = s.surrogate_key
            WHEN MATCHED THEN UPDATE SET
                t.{LF_CREATED} = COALESCE(s.cdf_created_at, t.{LF_CREATED}),
                t.{LF_MODIFIED} = COALESCE(s.cdf_modified_at, t.{LF_MODIFIED})
        """)
        print(f"  ✓ CDF refinement applied on {table_name}")
    except Exception as e:
        print(f"  ⚠ CDF refinement failed on {table_name}: {e}")


def upgrade():
    print(f"Applying migration: Add LF audit columns to {UNIFIED_TABLE}")
    print("-" * 80)

    if not table_exists(UNIFIED_TABLE):
        print(f"⚠ Table not found: {UNIFIED_TABLE}, skipping")
        return

    add_columns_if_missing(UNIFIED_TABLE)
    backfill_from_existing_columns(UNIFIED_TABLE)
    refine_from_cdf(UNIFIED_TABLE)

    null_check = spark.sql(f"""
        SELECT
            SUM(CASE WHEN {LF_CREATED} IS NULL THEN 1 ELSE 0 END) AS null_created,
            SUM(CASE WHEN {LF_MODIFIED} IS NULL THEN 1 ELSE 0 END) AS null_modified,
            COUNT(*) AS total
        FROM {UNIFIED_TABLE}
    """).collect()[0]
    print(f"  Post-migration: total={null_check['total']}, "
          f"null_created={null_check['null_created']}, "
          f"null_modified={null_check['null_modified']}")

    print("-" * 80)
    print("✓ Migration applied successfully")


def downgrade():
    print(f"Reverting migration: Drop LF audit columns from {UNIFIED_TABLE}")
    print("-" * 80)

    if not table_exists(UNIFIED_TABLE):
        print(f"⚠ Table not found: {UNIFIED_TABLE}, skipping")
        return

    for col_name in (LF_CREATED, LF_MODIFIED):
        if column_exists(UNIFIED_TABLE, col_name):
            print(f"  ALTER TABLE {UNIFIED_TABLE} DROP COLUMN {col_name}")
            spark.sql(f"ALTER TABLE {UNIFIED_TABLE} DROP COLUMN {col_name}")
        else:
            print(f"  Column {col_name} not present, skipping drop")

    print("-" * 80)
    print("✓ Migration reverted successfully")


# COMMAND ----------

if function == "downgrade":
    downgrade()
else:
    upgrade()
