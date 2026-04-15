# Databricks notebook source
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("entity", "", "Entity Name")

# COMMAND ----------

dbutils.widgets.dropdown("function", "upgrade", ["upgrade", "downgrade"], "Function")
function = dbutils.widgets.get("function")
catalog_name = dbutils.widgets.get("catalog_name")
entity = dbutils.widgets.get("entity")

# COMMAND ----------

"""
Migration: Deterministic to Rule Match
Revision: 364310613380_deterministic_rules
"""

from pyspark.sql import SparkSession


TABLE_NAME_MATCH = f"{catalog_name}.silver.{entity}_unified_deterministic_prod"
TABLE_NAME_PROCESSED = f"{catalog_name}.silver.{entity}_processed_unified_prod"
TABLE_NAME_MATCH_DEDUP = f"{catalog_name}.silver.{entity}_unified_deterministic_deduplicate_prod"
TABLE_NAME_PROCESSED_DEDUP = f"{catalog_name}.silver.{entity}_processed_unified_deduplicate_prod"


from pyspark.sql.functions import to_json, col

def convert_struct_to_json(table_name: str):
    print(f"Converting struct → JSON for: {table_name}")

    df = spark.read.table(table_name)

    if "deterministic_match_result" in df.columns:
        df = df.withColumn(
            "deterministic_match_result",
            to_json(col("deterministic_match_result"))
        )

        df.write \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(table_name)

        print("✓ Schema updated (STRUCT → STRING)")
    else:
        print("⚠ Column not found, skipping")

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

        spark.sql(
            f"""
           UPDATE {TABLE_NAME_MATCH}
           SET exploded_result.reason = regexp_replace(
    exploded_result.reason,
    '^Due to Deterministic Match of Rule: (.*)',
    'Due to Match Rule: \\1'
      ) WHERE exploded_result.reason LIKE 'Due to Deterministic Match of Rule:%' """
        )

        print("✓ Update applied")
        convert_struct_to_json(TABLE_NAME_MATCH)
    else:
        print(f"⚠ Table not found: {TABLE_NAME_MATCH}, skipping update")

    if table_exists(TABLE_NAME_MATCH_DEDUP):
        print(f"Table found: {TABLE_NAME_MATCH_DEDUP}")

        spark.sql(
            f"""
           UPDATE {TABLE_NAME_MATCH_DEDUP}
           SET exploded_result.reason = regexp_replace(
    exploded_result.reason,
    '^Due to Deterministic Match of Rule: (.*)',
    'Due to Match Rule: \\1'
      ) WHERE exploded_result.reason LIKE 'Due to Deterministic Match of Rule:%' """
        )
        convert_struct_to_json(TABLE_NAME_MATCH_DEDUP)
        print("✓ Update applied")
    else:
        print(f"⚠ Table not found: {TABLE_NAME_MATCH_DEDUP}, skipping update")

    if table_exists(TABLE_NAME_PROCESSED):
        print(f"Table found: {TABLE_NAME_PROCESSED}")

        spark.sql(
            f"""
           UPDATE {TABLE_NAME_PROCESSED}
           SET exploded_result.reason = regexp_replace(
    exploded_result.reason,
    '^Due to Deterministic Match of Rule: (.*)',
    'Due to Match Rule: \\1'
      ) WHERE exploded_result.reason LIKE 'Due to Deterministic Match of Rule:%' """
        )

        print("✓ Update applied")
    else:
        print(f"⚠ Table not found: {TABLE_NAME_PROCESSED}, skipping update")

    if table_exists(TABLE_NAME_PROCESSED_DEDUP):
        print(f"Table found: {TABLE_NAME_PROCESSED_DEDUP}")

        spark.sql(
            f"""
           UPDATE {TABLE_NAME_PROCESSED_DEDUP}
           SET exploded_result.reason = regexp_replace(
    exploded_result.reason,
    '^Due to Deterministic Match of Rule: (.*)',
    'Due to Match Rule: \\1'
      ) WHERE exploded_result.reason LIKE 'Due to Deterministic Match of Rule:%' """
        )

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

        spark.sql(
            f"""
           UPDATE {TABLE_NAME_MATCH}
     SET exploded_result.reason = regexp_replace(
    exploded_result.reason,
    '^Due to Match Rule: (.*)',
    'Due to Deterministic Match of Rule: \\1'
)
WHERE exploded_result.reason LIKE 'Due to Match Rule:%'
        """
        )

        print("✓ Reverted update")
    else:
        print(f"⚠ Table not found: {TABLE_NAME_MATCH}, skipping downgrade")

    if table_exists(TABLE_NAME_MATCH_DEDUP):
        print(f"Table found: {TABLE_NAME_MATCH_DEDUP}")

        spark.sql(
            f"""
           UPDATE {TABLE_NAME_MATCH_DEDUP}
     SET exploded_result.reason = regexp_replace(
    exploded_result.reason,
    '^Due to Match Rule: (.*)',
    'Due to Deterministic Match of Rule: \\1'
)
WHERE exploded_result.reason LIKE 'Due to Match Rule:%'
        """
        )

        print("✓ Reverted update")
    else:
        print(f"⚠ Table not found: {TABLE_NAME_MATCH}, skipping downgrade")

    if table_exists(TABLE_NAME_PROCESSED):
        print(f"Table found: {TABLE_NAME_PROCESSED}")

        spark.sql(
            f"""
           UPDATE {TABLE_NAME_PROCESSED}
     SET exploded_result.reason = regexp_replace(
    exploded_result.reason,
    '^Due to Match Rule: (.*)',
    'Due to Deterministic Match of Rule: \\1'
)
WHERE exploded_result.reason LIKE 'Due to Match Rule:%'
        """
        )

        print("✓ Reverted update")
    else:
        print(f"⚠ Table not found: {TABLE_NAME_PROCESSED}, skipping downgrade")

    print("-" * 80)
    print(f"✓ Migration reverted successfully")

    if table_exists(TABLE_NAME_PROCESSED_DEDUP):
        print(f"Table found: {TABLE_NAME_PROCESSED_DEDUP}")

        spark.sql(
            f"""
           UPDATE {TABLE_NAME_PROCESSED_DEDUP}
     SET exploded_result.reason = regexp_replace(
    exploded_result.reason,
    '^Due to Match Rule: (.*)',
    'Due to Deterministic Match of Rule: \\1'
)
WHERE exploded_result.reason LIKE 'Due to Match Rule:%'
        """
        )

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
