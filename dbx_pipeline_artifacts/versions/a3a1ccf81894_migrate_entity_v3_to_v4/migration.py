# Databricks notebook source
dbutils.widgets.dropdown("function", "upgrade", ["upgrade", "downgrade"], "Function")

# COMMAND ----------

function = dbutils.widgets.get("function")

# COMMAND ----------

# Get parameters - these come from MigrationRunner
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("primary_key", "", "Primary Key")
dbutils.widgets.text("id_key", "", "ID Key")
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")
dbutils.widgets.text("entity_attributes_datatype", "", "Entity Attributes Datatype")
dbutils.widgets.text("attributes_mapping", "", "Attributes Mapping")
dbutils.widgets.text("primary_table", "", "Primary Table")
dbutils.widgets.text("dataset_tables", "", "Dataset Tables")
dbutils.widgets.text("dataset_objects", "", "Dataset Objects")
dbutils.widgets.text("match_attributes", "", "Match Attributes")
dbutils.widgets.text("default_survivorship_rules", "", "Survivorship Rules")
dbutils.widgets.text("config_thresholds", "", "Config Thresholds")

# COMMAND ----------

# MAGIC %md
# MAGIC Migration: v3.0.0 → v4.0.0
# MAGIC Revision: a3a1ccf81894_migrate_entity_v3_to_v4
# MAGIC
# MAGIC Transforms entity tables from v3.0.0 to v4.0.0 schema:
# MAGIC - Master table: No changes to schema
# MAGIC - Unified table: Adds master_lakefusion_id column
# MAGIC - Merge Activities: Changes column names and adds created_at

# COMMAND ----------

def upgrade():
    """Migrating Entity from v3.0.0 to v4.0.0"""
    
    # Get entity info
    entity = dbutils.widgets.get("entity")
    catalog_name = dbutils.widgets.get("catalog_name")
    experiment_id = dbutils.widgets.get("experiment_id")
    
    # Construct experiment suffix
    experiment_suffix = f"_{experiment_id}" if experiment_id else ""
    
    print("="*80)
    print("MIGRATION: v3.0.0 → v4.0.0")
    print("="*80)
    print(f"Revision: a3a1ccf81894_migrate_entity_v3_to_v4")
    print(f"Entity: {entity}")
    print(f"Catalog: {catalog_name}")
    print(f"Experiment: {experiment_id if experiment_id else 'prod'}")
    print("="*80)
    
    # Collect all widget values to pass to phase notebooks
    params = {}
    for key in ["entity", "catalog_name", "experiment_id", "primary_key", "id_key", 
                "entity_attributes", "entity_attributes_datatype", "attributes_mapping", 
                "primary_table", "dataset_tables", "dataset_objects", "match_attributes", 
                "default_survivorship_rules", "config_thresholds"]:
        try:
            params[key] = dbutils.widgets.get(key)
        except:
            params[key] = ""
            
    # ========================================================================
    # PHASE 0: Pre-Migration Validation
    # ========================================================================
    print("\n" + "="*80)
    print("PHASE 0: PRE-MIGRATION VALIDATION")
    print("="*80)
    
    result = dbutils.notebook.run(
        "./00_Pre_Migration_Validation",
        timeout_seconds=600,
        arguments=params
    )
    print(f"✓ Phase 0 Complete: {result}")

    # ========================================================================
    # PHASE 1: Backup Original Tables (DEEP CLONE)
    # ========================================================================
    print("\n" + "="*80)
    print("PHASE 1: BACKUP ORIGINAL TABLES (DEEP CLONE)")
    print("="*80)
    
    result = dbutils.notebook.run(
        "./01_Backup_Original_Tables",
        timeout_seconds=1800,
        arguments=params
    )
    print(f"✓ Phase 1 Complete: {result}")
    
    # ========================================================================
    # PHASE 2: Transform and Load Data (CREATE OR REPLACE)
    # ========================================================================
    print("\n" + "="*80)
    print("PHASE 2: TRANSFORM AND LOAD DATA (CREATE OR REPLACE)")
    print("="*80)
    
    result = dbutils.notebook.run(
        "./02_Transform",
        timeout_seconds=3600,
        arguments=params
    )
    print(f"✓ Phase 2 Complete: {result}")

    # ========================================================================
    # PHASE 3: Process Cross Walk 
    # ========================================================================
    print("\n" + "="*80)
    print("PHASE 3: CREATE CROSS WALK VIEW")
    print("="*80)
    
    result = dbutils.notebook.run(
        "./03_Create_Crosswalk_View",
        timeout_seconds=2400,
        arguments=params
    )
    print(f"✓ Phase 3 Complete: {result}")

    # ========================================================================
    # PHASE 4: Validate Post Migration
    # ========================================================================
    print("\n" + "="*80)
    print("PHASE 4: POST-MIGRATION VALIDATION")
    print("="*80)
    
    result = dbutils.notebook.run(
        "./04_Post_Migration_Validation",
        timeout_seconds=600,
        arguments=params
    )
    print(f"✓ Phase 4 Complete: {result}")
    
    # ========================================================================
    # MIGRATION COMPLETE
    # ========================================================================
    master_table = f"{catalog_name}.gold.{entity}_master{experiment_suffix}"
    unified_table = f"{catalog_name}.silver.{entity}_unified{experiment_suffix}"
    ma_table = f"{master_table}_merge_activities"
    
    backup_master = f"{catalog_name}.gold.{entity}_master{experiment_suffix}_backup_v3"
    backup_unified = f"{catalog_name}.silver.{entity}_unified{experiment_suffix}_backup_v3"
    backup_ma = f"{backup_master}_merge_activities"
    
    print("\n" + "="*80)
    print("✅ MIGRATION SUCCESSFUL - v4.0.0")
    print("="*80)
    print(f"")
    print("v4.0.0 Tables Created:")
    print(f"  - {master_table}")
    print(f"  - {unified_table}")
    print(f"  - {ma_table}")
    print(f"")
    print("Backup Tables Preserved (for rollback):")
    print(f"  - {backup_master}")
    print(f"  - {backup_unified}")
    print(f"  - {backup_ma}")
    print(f"")
    print("✓ Migration complete! Tables are live in v4.0.0 schema.")
    print("="*80)


def downgrade():
    """
    Downgrade: v4.0.0 → v3.0.0
    Restores v3.0.0 state from backup tables using Restore_Backup notebook
    """
    
    # Get parameters
    entity = dbutils.widgets.get("entity")
    catalog_name = dbutils.widgets.get("catalog_name")
    experiment_id = dbutils.widgets.get("experiment_id")
    
    # Construct experiment suffix
    experiment_suffix = f"_{experiment_id}" if experiment_id else ""
    
    print("="*80)
    print("DOWNGRADE: v4.0.0 → v3.0.0")
    print("="*80)
    print(f"Entity: {entity}")
    print(f"Catalog: {catalog_name}")
    print(f"Experiment: {experiment_id if experiment_id else 'prod'}")
    print("")
    print("⚠️  WARNING: This will restore v3.0.0 from backup!")
    print("="*80)
    
    # Prepare parameters for Restore_Backup notebook
    restore_params = {
        "entity": entity,
        "catalog_name": catalog_name,
        "experiment_id": experiment_id
    }
    
    # Execute Restore_Backup notebook
    print("\nExecuting Restore_Backup notebook...")
    
    try:
        result = dbutils.notebook.run(
            "./Restore_Backup_Preserve_Failure_State",
            timeout_seconds=3600,  # 1 hour timeout for large tables
            arguments=restore_params
        )
        print(f"✓ Restore Complete: {result}")
        
    except Exception as e:
        print(f"❌ Restore Failed: {str(e)}")
        raise
    
    # Define restored table names for summary
    master_table = f"{catalog_name}.gold.{entity}_master{experiment_suffix}"
    unified_table = f"{catalog_name}.silver.{entity}_unified{experiment_suffix}"
    ma_table = f"{master_table}_merge_activities"
    avs_table = f"{master_table}_attribute_version_sources"
    
    backup_master = f"{catalog_name}.gold.{entity}_master{experiment_suffix}_backup_v3"
    backup_unified = f"{catalog_name}.silver.{entity}_unified{experiment_suffix}_backup_v3"
    backup_ma = f"{backup_master}_merge_activities"
    backup_avs = f"{backup_master}_attribute_version_sources"
    
    migration_fail_master = f"{catalog_name}.gold.{entity}_master{experiment_suffix}_migration_fail_v4"
    migration_fail_unified = f"{catalog_name}.silver.{entity}_unified{experiment_suffix}_migration_fail_v4"
    migration_fail_ma = f"{migration_fail_master}_merge_activities"
    migration_fail_avs = f"{migration_fail_master}_attribute_version_sources"
    
    print("\n" + "="*80)
    print("✅ DOWNGRADE SUCCESSFUL - v3.0.0 RESTORED")
    print("="*80)
    print("")
    print("Restored Tables (now v3.0.0):")
    print(f"  - {master_table}")
    print(f"  - {unified_table}")
    print(f"  - {ma_table}")
    print(f"  - {avs_table}")
    print("")
    print("V4 Migration Failure Backups (for analysis):")
    print(f"  - {migration_fail_master}")
    print(f"  - {migration_fail_unified}")
    print(f"  - {migration_fail_ma}")
    print(f"  - {migration_fail_avs}")
    print("")
    print("V3 Original Backups (still preserved):")
    print(f"  - {backup_master}")
    print(f"  - {backup_unified}")
    print(f"  - {backup_ma}")
    print(f"  - {backup_avs}")
    print("")
    print("✓ System restored to v3.0.0 state")
    print("✓ V4 migration failure state preserved for analysis")
    print("="*80)
    raise Exception("System restored to v3.0.0 state and V4 migration failure state preserved for analysis")


# COMMAND ----------

if function == "downgrade":
    downgrade()
else:
    upgrade()
