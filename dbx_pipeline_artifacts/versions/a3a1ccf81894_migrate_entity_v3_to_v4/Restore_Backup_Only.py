# Databricks notebook source
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.dropdown("confirm_restore", "NO", ["NO", "YES"], "Confirm Restore")

# COMMAND ----------

entity = dbutils.widgets.get("entity")
catalog_name = dbutils.widgets.get("catalog_name")
experiment_id = dbutils.widgets.get("experiment_id")
confirm_restore = dbutils.widgets.get("confirm_restore")

# COMMAND ----------

if confirm_restore != "YES":
    print("="*80)
    print("⚠️  RESTORE NOT CONFIRMED")
    print("="*80)
    print("")
    print("This notebook will OVERWRITE current tables with backup data.")
    print("This action cannot be undone!")
    print("")
    print("To proceed, set 'confirm_restore' parameter to 'YES'")
    print("="*80)
    dbutils.notebook.exit("CANCELLED - Confirmation required")

# COMMAND ----------

# Construct table names
experiment_suffix = f"_{experiment_id}" if experiment_id else ""

# Original table names (restore targets)
original_master_table = f"{catalog_name}.gold.{entity}_master{experiment_suffix}"
original_unified_table = f"{catalog_name}.silver.{entity}_unified{experiment_suffix}"
original_merge_activities_table = f"{original_master_table}_merge_activities"
original_attribute_version_sources = f"{original_master_table}_attribute_version_sources"

# Additional tables
original_unified_dedupe = f"{catalog_name}.silver.{entity}_unified_deduplicate{experiment_suffix}"
original_processed_unified = f"{catalog_name}.silver.{entity}_processed_unified{experiment_suffix}"
original_processed_unified_dedupe = f"{catalog_name}.silver.{entity}_processed_unified_deduplicate{experiment_suffix}"
original_unified_validation_error = f"{catalog_name}.silver.{entity}_unified_validation_error{experiment_suffix}"
original_potential_match = f"{catalog_name}.gold.{entity}_master_potential_match{experiment_suffix}"
original_potential_match_dedupe = f"{catalog_name}.gold.{entity}_master_potential_match_deduplicate{experiment_suffix}"
crosswalk_view = f"{catalog_name}.gold.{entity}_crosswalk"

# Backup table names (restore sources)
backup_master_table = f"{catalog_name}.gold.{entity}_master{experiment_suffix}_backup_v3"
backup_unified_table = f"{catalog_name}.silver.{entity}_unified{experiment_suffix}_backup_v3"
backup_merge_activities_table = f"{backup_master_table}_merge_activities"
backup_attribute_version_sources = f"{backup_master_table}_attribute_version_sources"

# Additional backup names
backup_unified_dedupe = f"{catalog_name}.silver.{entity}_unified_deduplicate{experiment_suffix}_backup_v3"
backup_processed_unified = f"{catalog_name}.silver.{entity}_processed_unified{experiment_suffix}_backup_v3"
backup_processed_unified_dedupe = f"{catalog_name}.silver.{entity}_processed_unified_deduplicate{experiment_suffix}_backup_v3"
backup_unified_validation_error = f"{catalog_name}.silver.{entity}_unified_validation_error{experiment_suffix}_backup_v3"
backup_potential_match = f"{catalog_name}.gold.{entity}_master_potential_match{experiment_suffix}_backup_v3"
backup_potential_match_dedupe = f"{catalog_name}.gold.{entity}_master_potential_match_deduplicate{experiment_suffix}_backup_v3"
backup_crosswalk_view = f"{catalog_name}.gold.{entity}_crosswalk_backup_v3"

# Meta info tables
meta_info_table = f"{catalog_name}.silver.table_meta_info"
backup_meta_info_table = f"{catalog_name}.silver.table_meta_info_backup_v3"

# COMMAND ----------

print("="*80)
print("🔄 RESTORE FROM BACKUP (v3.0.0)")
print("="*80)
print(f"Entity: {entity}")
print(f"Catalog: {catalog_name}")
print(f"Experiment: {experiment_id if experiment_id else 'prod'}")
print("")
print("⚠️  WARNING: This will OVERWRITE current tables with backup data!")
print("")
print("Method: DEEP CLONE from backup tables")
print("="*80)

# COMMAND ----------

# Verify backup tables exist
print("\n" + "="*80)
print("VERIFYING BACKUP TABLES")
print("="*80)

required_backups = [
    ("1. Master Backup", backup_master_table),
    ("2. Unified Backup", backup_unified_table),
    ("3. Merge Activities Backup", backup_merge_activities_table),
    ("4. Attribute Version Sources Backup", backup_attribute_version_sources)
]

all_backups_exist = True
for name, table in required_backups:
    exists = spark.catalog.tableExists(table)
    status = "✓" if exists else "✗"
    print(f"{status} {name}: {table}")
    if not exists:
        all_backups_exist = False

if not all_backups_exist:
    raise Exception("❌ Cannot proceed: Required backup tables missing!")

print("\n✓ All required backup tables found")

# COMMAND ----------

# Get backup row counts
print("\n" + "="*80)
print("BACKUP TABLE ROW COUNTS")
print("="*80)

backup_master_count = spark.table(backup_master_table).count()
backup_unified_count = spark.table(backup_unified_table).count()
backup_ma_count = spark.table(backup_merge_activities_table).count()
backup_avs_count = spark.table(backup_attribute_version_sources).count()

print(f"1. Master Backup:                    {backup_master_count:,} rows")
print(f"2. Unified Backup:                   {backup_unified_count:,} rows")
print(f"3. Merge Activities Backup:          {backup_ma_count:,} rows")
print(f"4. Attribute Version Sources Backup: {backup_avs_count:,} rows")
print("="*80)

# COMMAND ----------

# Start restore process
print("\n" + "="*80)
print("RESTORING TABLES FROM BACKUP")
print("="*80)

print("\n1. Restoring Master table...")
spark.sql(f"CREATE OR REPLACE TABLE {original_master_table} DEEP CLONE {backup_master_table}")
restored_master_count = spark.table(original_master_table).count()
print(f"   ✓ Restored: {original_master_table}")
print(f"   ✓ Row count: {restored_master_count:,}")

print("\n2. Restoring Unified table...")
spark.sql(f"CREATE OR REPLACE TABLE {original_unified_table} DEEP CLONE {backup_unified_table}")
restored_unified_count = spark.table(original_unified_table).count()
print(f"   ✓ Restored: {original_unified_table}")
print(f"   ✓ Row count: {restored_unified_count:,}")

print("\n3. Restoring Merge Activities table...")
spark.sql(f"CREATE OR REPLACE TABLE {original_merge_activities_table} DEEP CLONE {backup_merge_activities_table}")
restored_ma_count = spark.table(original_merge_activities_table).count()
print(f"   ✓ Restored: {original_merge_activities_table}")
print(f"   ✓ Row count: {restored_ma_count:,}")

print("\n4. Restoring Attribute Version Sources table...")
spark.sql(f"CREATE OR REPLACE TABLE {original_attribute_version_sources} DEEP CLONE {backup_attribute_version_sources}")
restored_avs_count = spark.table(original_attribute_version_sources).count()
print(f"   ✓ Restored: {original_attribute_version_sources}")
print(f"   ✓ Row count: {restored_avs_count:,}")

# COMMAND ----------

# Restore meta_info if backup exists
print("\n5. Restoring CDF version tracking (table_meta_info)...")

if spark.catalog.tableExists(backup_meta_info_table):
    # Get this entity's backup entries
    entity_backup_count = spark.sql(f"""
        SELECT COUNT(*) as cnt FROM {backup_meta_info_table}
        WHERE entity_name = '{entity}'
    """).first()['cnt']
    
    if entity_backup_count > 0:
        # Delete current entity entries and insert backup
        spark.sql(f"""
            DELETE FROM {meta_info_table}
            WHERE entity_name = '{entity}'
        """)
        
        spark.sql(f"""
            INSERT INTO {meta_info_table}
            SELECT * FROM {backup_meta_info_table}
            WHERE entity_name = '{entity}'
        """)
        
        print(f"   ✓ Restored {entity_backup_count} CDF version entries for '{entity}'")
    else:
        print(f"   ⊘ No backup entries found for '{entity}' - skipping")
else:
    print(f"   ⊘ Backup table_meta_info not found - skipping")

# COMMAND ----------

# Restore crosswalk view if backup exists
print("\n6. Restoring Crosswalk view...")

if spark.catalog.tableExists(backup_crosswalk_view):
    # Get backup view DDL
    view_ddl = spark.sql(f"SHOW CREATE TABLE {backup_crosswalk_view}").collect()[0][0]
    
    # Split view names into parts
    backup_catalog, backup_schema, backup_view = backup_crosswalk_view.split('.')
    orig_catalog, orig_schema, orig_view = crosswalk_view.split('.')
    
    # Try multiple patterns for replacement
    search_patterns = [
        f"CREATE VIEW {backup_schema}.{backup_view}",
        f"CREATE VIEW `{backup_catalog}`.`{backup_schema}`.`{backup_view}`",
        f"CREATE VIEW {backup_catalog}.{backup_schema}.{backup_view}"
    ]
    
    replace_pattern = f"CREATE OR REPLACE VIEW {orig_catalog}.{orig_schema}.{orig_view}"
    
    restored_ddl = None
    for pattern in search_patterns:
        if pattern in view_ddl:
            restored_ddl = view_ddl.replace(pattern, replace_pattern)
            break
    
    if restored_ddl:
        spark.sql(restored_ddl)
        print(f"   ✓ Restored: {crosswalk_view}")
    else:
        print(f"   ⚠️ Could not parse backup view DDL - manual restoration may be needed")
else:
    print(f"   ⊘ Backup crosswalk view not found - skipping")

# COMMAND ----------

# Restore additional tables if backups exist
print("\n7-12. Restoring additional tables (if backups exist)...")

additional_restore_pairs = [
    (backup_unified_dedupe, original_unified_dedupe, "Unified Deduplicate"),
    (backup_processed_unified, original_processed_unified, "Processed Unified"),
    (backup_processed_unified_dedupe, original_processed_unified_dedupe, "Processed Unified Deduplicate"),
    (backup_unified_validation_error, original_unified_validation_error, "Unified Validation Error"),
    (backup_potential_match, original_potential_match, "Potential Match"),
    (backup_potential_match_dedupe, original_potential_match_dedupe, "Potential Match Deduplicate")
]

table_num = 7
for backup_table, original_table, name in additional_restore_pairs:
    if spark.catalog.tableExists(backup_table):
        spark.sql(f"CREATE OR REPLACE TABLE {original_table} DEEP CLONE {backup_table}")
        count = spark.table(original_table).count()
        print(f"   {table_num}. ✓ Restored {name}: {count:,} rows")
    else:
        print(f"   {table_num}. ⊘ {name} backup not found - skipping")
    table_num += 1

# COMMAND ----------

# Verify restoration
print("\n" + "="*80)
print("RESTORE VERIFICATION")
print("="*80)

master_match = backup_master_count == restored_master_count
unified_match = backup_unified_count == restored_unified_count
ma_match = backup_ma_count == restored_ma_count
avs_match = backup_avs_count == restored_avs_count

print(f"1. Master:                    {restored_master_count:,} rows {'✓' if master_match else '✗'}")
print(f"2. Unified:                   {restored_unified_count:,} rows {'✓' if unified_match else '✗'}")
print(f"3. Merge Activities:          {restored_ma_count:,} rows {'✓' if ma_match else '✗'}")
print(f"4. Attribute Version Sources: {restored_avs_count:,} rows {'✓' if avs_match else '✗'}")

if not (master_match and unified_match and ma_match and avs_match):
    raise Exception("❌ Restore verification failed: Row counts don't match!")

print("\n" + "="*80)
print("✅ RESTORE COMPLETED SUCCESSFULLY")
print("="*80)
print("")
print("All tables have been restored to their v3.0.0 state.")
print("")
print("Next steps:")
print("  1. Verify your application works with restored data")
print("  2. Investigate why migration failed")
print("  3. Fix issues and retry migration")
print("")
print("Note: Backup tables are still available for reference:")
print(f"  - {backup_master_table}")
print(f"  - {backup_unified_table}")
print(f"  - {backup_merge_activities_table}")
print(f"  - {backup_attribute_version_sources}")
print("="*80)
