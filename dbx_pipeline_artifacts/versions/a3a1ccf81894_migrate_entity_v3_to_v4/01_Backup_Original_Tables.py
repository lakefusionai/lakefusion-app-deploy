# Databricks notebook source
"""
v4.0.0 Migration - Backup Original Tables

Creates deep clone backups of v3.0.0 tables before migration.
Follows standard integration pipeline patterns.
"""

# COMMAND ----------

# Get parameters
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")

# COMMAND ----------

# Get values from widgets (passed as arguments)
entity = dbutils.widgets.get("entity")
catalog_name = dbutils.widgets.get("catalog_name")
experiment_id = dbutils.widgets.get("experiment_id")

# COMMAND ----------

# Construct table names WITH experiment suffix
experiment_suffix = f"_{experiment_id}" if experiment_id else ""

# Original table names (these will be backed up)
original_master_table = f"{catalog_name}.gold.{entity}_master{experiment_suffix}"
original_unified_table = f"{catalog_name}.silver.{entity}_unified{experiment_suffix}"
original_merge_activities_table = f"{original_master_table}_merge_activities"
original_attribute_version_sources = f"{original_master_table}_attribute_version_sources"

# Additional original tables (optional)
original_unified_dedupe = f"{catalog_name}.silver.{entity}_unified_deduplicate{experiment_suffix}"
original_processed_unified = f"{catalog_name}.silver.{entity}_processed_unified{experiment_suffix}"
original_processed_unified_dedupe = f"{catalog_name}.silver.{entity}_processed_unified_deduplicate{experiment_suffix}"
original_unified_validation_error = f"{catalog_name}.silver.{entity}_unified_validation_error{experiment_suffix}"
original_potential_match = f"{catalog_name}.gold.{entity}_master_potential_match{experiment_suffix}"
original_potential_match_dedupe = f"{catalog_name}.gold.{entity}_master_potential_match_deduplicate{experiment_suffix}"
crosswalk_view = f"{catalog_name}.gold.{entity}_crosswalk"

# Backup table names (with _backup_v3 suffix)
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
backup_meta_info_table = f"{catalog_name}.silver.table_meta_info_backup_v3"  # Shared across all entities

# COMMAND ----------

print("="*80)
print("BACKUP ORIGINAL TABLES (v3.0.0)")
print("="*80)
print(f"Entity: {entity}")
print(f"Catalog: {catalog_name}")
print(f"")
print("Method: DEEP CLONE with CREATE OR REPLACE (Idempotent)")
print(f"")
print("Tables to Backup:")
print(f"  1. Master")
print(f"  2. Unified")
print(f"  3. Merge Activities")
print(f"  4. Attribute Version Sources")
print(f"  5. Unified Deduplicate")
print(f"  6. Processed Unified")
print(f"  7. Processed Unified Deduplicate")
print(f"  8. Unified Validation Error")
print(f"  9. Potential Match")
print(f"  10. Potential Match Deduplicate")
print(f"  11. Crosswalk View")
print(f"")
print("Note: Tables 1-4 are required. Tables 5-11 will be backed up if found.")
print(f"")
print(f"Naming: All backups use '_backup_v3' suffix")
print(f"  Example: {entity}_master{experiment_suffix}_backup_v3")
print(f"")
print("Meta Info: MERGE into shared table_meta_info_backup_v3 (all entities)")
print("="*80)

# COMMAND ----------

# Verify original tables exist
print("\n" + "="*80)
print("VERIFYING TABLES")
print("="*80)

# Tables 1-4: Must exist
print("\nTables 1-4 (Required - must exist):")
required_tables = [
    ("1. Master", original_master_table),
    ("2. Unified", original_unified_table),
    ("3. Merge Activities", original_merge_activities_table),
    ("4. Attribute Version Sources", original_attribute_version_sources)
]

all_required_exist = True
for name, table in required_tables:
    exists = spark.catalog.tableExists(table)
    status = "✓" if exists else "✗"
    print(f"{status} {name}: {table}")
    if not exists:
        all_required_exist = False

if not all_required_exist:
    raise Exception("❌ Cannot proceed: Required tables (1-4) missing!")

# Tables 5-10: Back up if found, skip if not
print("\nTables 5-10 (Will backup if found):")
additional_tables = [
    ("5. Unified Deduplicate", original_unified_dedupe),
    ("6. Processed Unified", original_processed_unified),
    ("7. Processed Unified Deduplicate", original_processed_unified_dedupe),
    ("8. Unified Validation Error", original_unified_validation_error),
    ("9. Potential Match", original_potential_match),
    ("10. Potential Match Deduplicate", original_potential_match_dedupe)
]

for name, table in additional_tables:
    exists = spark.catalog.tableExists(table)
    status = "✓ Found" if exists else "⊘ Not found (will skip)"
    print(f"{status} {name}: {table}")

# Table 11: Crosswalk view
print("\nTable 11 (View - will backup if found):")
view_exists = spark.catalog.tableExists(crosswalk_view)
status = "✓ Found" if view_exists else "⊘ Not found (will skip)"
print(f"{status} 11. Crosswalk View: {crosswalk_view}")

print("\n✓ Verification complete")

# COMMAND ----------

# Get row counts before backup
print("\n" + "="*80)
print("ORIGINAL TABLE ROW COUNTS (Required Tables)")
print("="*80)

original_master_count = spark.table(original_master_table).count()
original_unified_count = spark.table(original_unified_table).count()
original_ma_count = spark.table(original_merge_activities_table).count()
original_avs_count = spark.table(original_attribute_version_sources).count()

print(f"1. Master:                    {original_master_count:,} rows")
print(f"2. Unified:                   {original_unified_count:,} rows")
print(f"3. Merge Activities:          {original_ma_count:,} rows")
print(f"4. Attribute Version Sources: {original_avs_count:,} rows")
print("="*80)

# COMMAND ----------

# Start backup process - naturally idempotent with CREATE OR REPLACE
print("\n" + "="*80)
print("CREATING BACKUP TABLES (DEEP CLONE - Idempotent)")
print("="*80)

print("\n1. Backing up Master table...")
spark.sql(f"CREATE OR REPLACE TABLE {backup_master_table} DEEP CLONE {original_master_table}")
backup_master_count = spark.table(backup_master_table).count()
print(f"  ✓ Created: {backup_master_table}")
print(f"  ✓ Row count: {backup_master_count:,}")

print("\n2. Backing up Unified table...")
spark.sql(f"CREATE OR REPLACE TABLE {backup_unified_table} DEEP CLONE {original_unified_table}")
backup_unified_count = spark.table(backup_unified_table).count()
print(f"  ✓ Created: {backup_unified_table}")
print(f"  ✓ Row count: {backup_unified_count:,}")

print("\n3. Backing up Merge Activities table...")
spark.sql(f"CREATE OR REPLACE TABLE {backup_merge_activities_table} DEEP CLONE {original_merge_activities_table}")
backup_ma_count = spark.table(backup_merge_activities_table).count()
print(f"  ✓ Created: {backup_merge_activities_table}")
print(f"  ✓ Row count: {backup_ma_count:,}")

print("\n4. Backing up Attribute Version Sources table...")
spark.sql(f"CREATE OR REPLACE TABLE {backup_attribute_version_sources} DEEP CLONE {original_attribute_version_sources}")
backup_avs_count = spark.table(backup_attribute_version_sources).count()
print(f"  ✓ Created: {backup_attribute_version_sources}")
print(f"  ✓ Row count: {backup_avs_count:,}")

print("\n5. Backing up CDF version tracking (table_meta_info)...")
# Check if meta_info table exists
if spark.catalog.tableExists(meta_info_table):
    # Create shared backup table if it doesn't exist (first entity migration)
    if not spark.catalog.tableExists(backup_meta_info_table):
        print(f"  → Creating shared backup table: {backup_meta_info_table}")
        spark.sql(f"CREATE TABLE {backup_meta_info_table} LIKE {meta_info_table}")
    
    # MERGE this entity's records into shared backup table
    spark.sql(f"""
        MERGE INTO {backup_meta_info_table} AS target
        USING (
            SELECT * FROM {meta_info_table}
            WHERE entity_name = '{entity}'
        ) AS source
        ON target.entity_name = source.entity_name 
           AND target.table_name = source.table_name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    
    # Count this entity's entries
    backup_meta_count = spark.sql(f"""
        SELECT COUNT(*) as cnt FROM {backup_meta_info_table}
        WHERE entity_name = '{entity}'
    """).collect()[0]['cnt']
    
    print(f"  ✓ Merged {backup_meta_count:,} CDF version entries for '{entity}'")
    print(f"  ✓ Stored in shared backup table: {backup_meta_info_table}")
else:
    print(f"  ⊘ table_meta_info not found - skipping")

print("\n6. Backing up Crosswalk view definition (v3.0.0)...")

if spark.catalog.tableExists(crosswalk_view):
    # Get original view DDL
    view_ddl = spark.sql(f"SHOW CREATE TABLE {crosswalk_view}").collect()[0][0]
    
    # Debug: Show first line of original DDL
    first_line = view_ddl.split('\n')[0]
    print(f"  Original DDL first line: {first_line}")
    
    # Split view name into parts
    catalog, schema, view = crosswalk_view.split('.')
    backup_catalog, backup_schema, backup_view = backup_crosswalk_view.split('.')
    
    # Try pattern 1: Just schema.view (most common in current catalog context)
    # NOTE: Even though source DDL omits catalog, we MUST include it in backup to ensure correct location
    search_pattern_1 = f"CREATE VIEW {schema}.{view}"
    replace_pattern_1 = f"CREATE OR REPLACE VIEW {backup_catalog}.{backup_schema}.{backup_view}"
    
    # Try pattern 2: With backticks
    search_pattern_2 = f"CREATE VIEW `{catalog}`.`{schema}`.`{view}`"
    replace_pattern_2 = f"CREATE OR REPLACE VIEW `{backup_catalog}`.`{backup_schema}`.`{backup_view}`"
    
    # Try pattern 3: Without backticks, full name
    search_pattern_3 = f"CREATE VIEW {catalog}.{schema}.{view}"
    replace_pattern_3 = f"CREATE OR REPLACE VIEW {backup_catalog}.{backup_schema}.{backup_view}"
    
    # Try each pattern
    matched = False
    if search_pattern_1 in view_ddl:
        print(f"  ✓ Matched pattern 1: {search_pattern_1}")
        backup_ddl = view_ddl.replace(search_pattern_1, replace_pattern_1)
        matched = True
    elif search_pattern_2 in view_ddl:
        print(f"  ✓ Matched pattern 2: {search_pattern_2}")
        backup_ddl = view_ddl.replace(search_pattern_2, replace_pattern_2)
        matched = True
    elif search_pattern_3 in view_ddl:
        print(f"  ✓ Matched pattern 3: {search_pattern_3}")
        backup_ddl = view_ddl.replace(search_pattern_3, replace_pattern_3)
        matched = True
    else:
        print(f"  ✗ No pattern matched!")
        print(f"    Tried:")
        print(f"      1. {search_pattern_1}")
        print(f"      2. {search_pattern_2}")
        print(f"      3. {search_pattern_3}")
        raise Exception(f"Could not find view name in DDL. First line: {first_line}")
    
    # Verify replacement worked
    backup_first_line = backup_ddl.split('\n')[0]
    print(f"  Backup DDL first line: {backup_first_line}")
    
    if backup_view not in backup_first_line:
        raise Exception(f"Replacement failed! Backup view name '{backup_view}' not found in: {backup_first_line}")
    
    # Create backup view (idempotent - CREATE OR REPLACE)
    spark.sql(backup_ddl)
    print(f"  ✓ Backed up: {crosswalk_view}")
    print(f"          → {backup_crosswalk_view}")
    print(f"     (v3.0.0 view definition preserved)")
    print(f"     (Original view remains available for queries)")
else:
    print(f"  ⊘ View not found - skipping")

# COMMAND ----------

# Tables 7-10: Back up if found, skip if not
table_number = 7
backup_pairs = [
    (original_unified_dedupe, backup_unified_dedupe, "Unified Deduplicate"),
    (original_processed_unified, backup_processed_unified, "Processed Unified"),
    (original_processed_unified_dedupe, backup_processed_unified_dedupe, "Processed Unified Deduplicate"),
    (original_unified_validation_error, backup_unified_validation_error, "Unified Validation Error"),
    (original_potential_match, backup_potential_match, "Potential Match"),
    (original_potential_match_dedupe, backup_potential_match_dedupe, "Potential Match Deduplicate")
]

for original_table, backup_table, name in backup_pairs:
    if spark.catalog.tableExists(original_table):
        print(f"\n{table_number}. Backing up {name}...")
        spark.sql(f"CREATE OR REPLACE TABLE {backup_table} DEEP CLONE {original_table}")
        count = spark.table(backup_table).count()
        print(f"  ✓ Created: {backup_table}")
        print(f"  ✓ Row count: {count:,}")
    else:
        print(f"\n{table_number}. {name} not found - skipping")
    table_number += 1

# COMMAND ----------

# Verify all backups succeeded
print("\n" + "="*80)
print("BACKUP VERIFICATION")
print("="*80)

# Check row counts match for required tables
master_match = original_master_count == backup_master_count
unified_match = original_unified_count == backup_unified_count
ma_match = original_ma_count == backup_ma_count
avs_match = original_avs_count == backup_avs_count

print(f"1. Master:                    {backup_master_count:,} rows {'✓' if master_match else '✗'}")
print(f"2. Unified:                   {backup_unified_count:,} rows {'✓' if unified_match else '✗'}")
print(f"3. Merge Activities:          {backup_ma_count:,} rows {'✓' if ma_match else '✗'}")
print(f"4. Attribute Version Sources: {backup_avs_count:,} rows {'✓' if avs_match else '✗'}")

if not (master_match and unified_match and ma_match and avs_match):
    raise Exception("❌ Backup verification failed: Row counts don't match!")

print("\n✅ All required tables backed up and verified!")
print("✅ Backup tables created via DEEP CLONE (idempotent)")
print(f"✅ Additional tables and crosswalk view backed up if found")
print("="*80)

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")
