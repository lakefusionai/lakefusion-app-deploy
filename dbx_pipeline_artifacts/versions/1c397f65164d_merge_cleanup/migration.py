# Databricks notebook source
dbutils.widgets.dropdown("function", "upgrade", ["upgrade", "downgrade"], "Function")

# COMMAND ----------

function = dbutils.widgets.get("function")

# COMMAND ----------

from pyspark.sql.functions import col
from delta.tables import DeltaTable

# COMMAND ----------

"""
Migration: Cleanup the Merged records in the master that doesn't have the source record in the soources/unified
Revision: 1c397f65164d_merge_cleanup
"""

def upgrade():
    """
    Cleanup orphaned records:
    - Remove merge activities where match_id doesn't exist in unified table
    - Remove master records that have no valid merge activities remaining
    - Remove attribute version sources for orphaned masters
    """
    
    # Get parameters
    entity = dbutils.widgets.get("entity")
    catalog_name = dbutils.widgets.get("catalog_name")
    experiment_id = dbutils.widgets.get("experiment_id")
    
    experiment_suffix = f"_{experiment_id}" if experiment_id else ""
    
    # Define table names
    master_table = f"{catalog_name}.gold.{entity}_master{experiment_suffix}"
    unified_table = f"{catalog_name}.silver.{entity}_unified{experiment_suffix}"
    merge_activities_table = f"{master_table}_merge_activities"
    attribute_version_sources_table = f"{master_table}_attribute_version_sources"
    
    print("="*80)
    print("MIGRATION: Cleanup Orphaned Records")
    print("="*80)
    print(f"Revision: 1c397f65164d_merge_cleanup")
    print(f"Entity: {entity}")
    print(f"Catalog: {catalog_name}")
    print(f"Experiment: {experiment_id if experiment_id else 'prod'}")
    print("-"*80)
    
    # Read tables
    unified_df = spark.table(unified_table)
    merge_activities_df = spark.table(merge_activities_table)
    
    # Get all valid surrogate_keys from unified table
    valid_surrogate_keys = unified_df.select(
        col("surrogate_key").alias("valid_sk")
    ).distinct()
    
    # Find orphan merge activities where match_id doesn't exist in unified
    orphan_ma_df = merge_activities_df.filter(
        col("match_id").isNotNull()
    ).join(
        valid_surrogate_keys,
        col("match_id") == col("valid_sk"),
        "left_anti"
    )
    
    # Get master_ids that have orphan merge activities
    orphan_master_ids = orphan_ma_df.select(col("master_id").alias("affected_master_id")).distinct()
    
    # Find masters that have at least one VALID merge activity
    masters_with_valid_ma = merge_activities_df.filter(
        col("match_id").isNotNull()
    ).join(
        valid_surrogate_keys,
        col("match_id") == col("valid_sk"),
        "inner"
    ).select(col("master_id").alias("valid_master_id")).distinct()
    
    # Completely orphaned masters = affected masters - masters with valid activities
    completely_orphaned_masters = orphan_master_ids.join(
        masters_with_valid_ma,
        col("affected_master_id") == col("valid_master_id"),
        "left_anti"
    ).select(col("affected_master_id").alias("orphan_master_id"))
    
    # Collect orphan master IDs
    orphan_ids = [row.orphan_master_id for row in completely_orphaned_masters.collect()]
    
    if not orphan_ids:
        print("\n✓ No orphaned masters found. Nothing to clean up.")
        print("="*80)
        return
    
    print(f"\n⚠️ Found {len(orphan_ids)} orphaned master(s) to remove")
    
    # Delete orphan merge activities
    ma_delta = DeltaTable.forName(spark, merge_activities_table)
    ma_delta.delete(col("master_id").isin(orphan_ids))
    print(f"  ✓ Removed merge activities for orphaned masters")
    
    # Delete orphan attribute version sources
    if spark.catalog.tableExists(attribute_version_sources_table):
        avs_delta = DeltaTable.forName(spark, attribute_version_sources_table)
        avs_delta.delete(col("lakefusion_id").isin(orphan_ids))
        print(f"  ✓ Removed attribute version sources for orphaned masters")
    
    # Delete orphan masters
    master_delta = DeltaTable.forName(spark, master_table)
    master_delta.delete(col("lakefusion_id").isin(orphan_ids))
    print(f"  ✓ Removed {len(orphan_ids)} orphaned master records")
    
    # Clean up individual orphan merge activities from non-orphaned masters
    remaining_ma_df = spark.table(merge_activities_table)
    
    orphan_match_ids = [
        row.match_id for row in remaining_ma_df.filter(
            col("match_id").isNotNull()
        ).join(
            valid_surrogate_keys,
            col("match_id") == col("valid_sk"),
            "left_anti"
        ).select("match_id").distinct().collect()
    ]
    
    if orphan_match_ids:
        ma_delta = DeltaTable.forName(spark, merge_activities_table)
        ma_delta.delete(col("match_id").isin(orphan_match_ids))
        print(f"  ✓ Removed {len(orphan_match_ids)} individual orphan merge activities")
    
    print("\n" + "="*80)
    print("✓ Migration applied successfully")
    print("="*80)


def downgrade():
    """
    Revert: No automatic rollback available
    """
    print("="*80)
    print("DOWNGRADE: Not supported")
    print("="*80)
    print("This migration removes orphaned records that have no source data.")
    print("Rollback is not supported as the orphan data cannot be reconstructed.")
    print("="*80)

# COMMAND ----------

if function == "downgrade":
    downgrade()
else:
    upgrade()
