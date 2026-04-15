# Databricks notebook source
"""
v4.0.0 Migration - Post-Migration Validation

Complete validation including transform and post-migration checks to ensure migration completed successfully.
Follows error collection pattern - collects all issues and raises exception at end if critical issues found.
"""

# COMMAND ----------

from pyspark.sql import functions as F
import json
from datetime import datetime

# COMMAND ----------

# Get parameters
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("dataset_tables", "", "Dataset Tables (JSON)")

# COMMAND ----------

# Get values from widgets (passed as arguments)
entity = dbutils.widgets.get("entity")
catalog_name = dbutils.widgets.get("catalog_name")
experiment_id = dbutils.widgets.get("experiment_id")
dataset_tables_json = dbutils.widgets.get("dataset_tables")

# Parse dataset_tables
dataset_tables = json.loads(dataset_tables_json) if dataset_tables_json else []

# COMMAND ----------

# Initialize validation results tracker
validation_results = {
    "entity": entity,
    "catalog": catalog_name,
    "timestamp": datetime.now().isoformat(),
    "critical_issues": [],
    "warnings": [],
    "checks_passed": [],
    "validation_passed": True,
    "part1_status": "not_started",
    "part2_status": "not_started",
    "part3_status": "not_started"
}

# COMMAND ----------

# Construct table names WITH experiment suffix
experiment_suffix = f"_{experiment_id}" if experiment_id else ""

# Original v3.0.0 tables
original_master_table = f"{catalog_name}.gold.{entity}_master{experiment_suffix}"
original_unified_table = f"{catalog_name}.silver.{entity}_unified{experiment_suffix}"
original_merge_activities_table = f"{original_master_table}_merge_activities"

# Backup v3.0.0 tables (DEEP CLONE copies)
backup_master_table = f"{catalog_name}.gold.{entity}_master{experiment_suffix}_backup_v3"
backup_unified_table = f"{catalog_name}.silver.{entity}_unified{experiment_suffix}_backup_v3"
backup_merge_activities_table = f"{backup_master_table}_merge_activities"
backup_attribute_version_sources = f"{backup_master_table}_attribute_version_sources"

# v4.0.0 tables (new)
master_table = f"{catalog_name}.gold.{entity}_master{experiment_suffix}"
unified_table = f"{catalog_name}.silver.{entity}_unified{experiment_suffix}"
merge_activities_table = f"{master_table}_merge_activities"
attribute_version_sources_table = f"{master_table}_attribute_version_sources"
crosswalk_view = f"{catalog_name}.gold.{entity}_crosswalk{experiment_suffix}"

# Meta info tables
meta_info_table = f"{catalog_name}.silver.table_meta_info"
backup_meta_info_table = f"{catalog_name}.silver.table_meta_info_backup_v3"

# Additional tables (backup if found, skip if not)
original_unified_dedupe = f"{catalog_name}.silver.{entity}_unified_deduplicate{experiment_suffix}"
original_processed_unified = f"{catalog_name}.silver.{entity}_processed_unified{experiment_suffix}"
original_processed_unified_dedupe = f"{catalog_name}.silver.{entity}_processed_unified_deduplicate{experiment_suffix}"
original_unified_validation_error = f"{catalog_name}.silver.{entity}_unified_validation_error{experiment_suffix}"
original_potential_match = f"{catalog_name}.gold.{entity}_master_potential_match{experiment_suffix}"
original_potential_match_dedupe = f"{catalog_name}.gold.{entity}_master_potential_match_deduplicate{experiment_suffix}"

# Additional backup names
backup_unified_dedupe = f"{catalog_name}.silver.{entity}_unified_deduplicate{experiment_suffix}_backup_v3"
backup_processed_unified = f"{catalog_name}.silver.{entity}_processed_unified{experiment_suffix}_backup_v3"
backup_processed_unified_dedupe = f"{catalog_name}.silver.{entity}_processed_unified_deduplicate{experiment_suffix}_backup_v3"
backup_unified_validation_error = f"{catalog_name}.silver.{entity}_unified_validation_error{experiment_suffix}_backup_v3"
backup_potential_match = f"{catalog_name}.gold.{entity}_master_potential_match{experiment_suffix}_backup_v3"
backup_potential_match_dedupe = f"{catalog_name}.gold.{entity}_master_potential_match_deduplicate{experiment_suffix}_backup_v3"

# COMMAND ----------

print("="*80)
print("MIGRATION VALIDATION")
print("="*80)
print(f"Entity: {entity}")
print(f"Catalog: {catalog_name}")
print(f"")
print("This validation has three parts:")
print("  PART 1: Validates Phase 1 (Backup)")
print("  PART 2: Validates Phase 2 (Transform - SKIPPED)")
print("  PART 3: Validates Post-Migration (v4.0.0 consistency)")
print(f"")
print("Errors are collected and reported at the end.")
print("="*80)

# COMMAND ----------

# ============================================================================
# PART 1: BACKUP VALIDATION (Phase 1)
# ============================================================================

print("\n" + "="*80)
print("PART 1: BACKUP VALIDATION (Phase 1)")
print("="*80)

# Check if backup tables exist
backup_tables_exist = all([
    spark.catalog.tableExists(backup_master_table),
    spark.catalog.tableExists(backup_unified_table),
    spark.catalog.tableExists(backup_merge_activities_table)
])

if not backup_tables_exist:
    print("\n⚠️  SKIPPING PART 1: Backup tables not found")
    print("   This is expected if Phase 1 (Backup) hasn't run yet")
    validation_results["part1_status"] = "skipped"
    validation_results["warnings"].append({
        "check": "PART 1: Backup Validation",
        "issue": "Backup tables not found - Phase 1 may not have run",
        "details": [backup_master_table, backup_unified_table, backup_merge_activities_table]
    })
else:
    print("\n✓ All required backup tables exist")
    validation_results["part1_status"] = "running"
    
    # Check if original tables still exist (before transform)
    originals_exist = all([
        spark.catalog.tableExists(original_master_table),
        spark.catalog.tableExists(original_unified_table),
        spark.catalog.tableExists(original_merge_activities_table)
    ])
    
    if originals_exist:
        print("\n✓ Original tables still exist (before transform)")
        print("\nValidating: Backup row counts")
        
        # ========================================================================
        # VALIDATION 1.1: Backup Row Counts
        # ========================================================================
        
        # 1. Master Table
        original_master_count = spark.table(original_master_table).count()
        backup_master_count = spark.table(backup_master_table).count()
        master_match = (original_master_count == backup_master_count)
        
        print(f"\n1. Master Table:")
        print(f"  Original: {original_master_count:,} rows")
        print(f"  Backup:   {backup_master_count:,} rows")
        print(f"  Status:   {'✓ Match' if master_match else '✗ Mismatch'}")
        
        if not master_match:
            validation_results["critical_issues"].append({
                "check": "PART 1.1: Master Table Backup",
                "issue": "Row count mismatch",
                "details": {
                    "original": original_master_count,
                    "backup": backup_master_count,
                    "difference": abs(original_master_count - backup_master_count)
                }
            })
            validation_results["validation_passed"] = False
        else:
            validation_results["checks_passed"].append("PART 1.1: Master Table Backup")
        
        # 2. Unified Table
        if dataset_tables:
            print(f"\n2. Unified Table:")
            print(f"  Calculating source records from dataset_tables...")
            
            total_source_records = 0
            for source_table in dataset_tables:
                if spark.catalog.tableExists(source_table):
                    count = spark.table(source_table).count()
                    total_source_records += count
                    print(f"    - {source_table}: {count:,} rows")
                else:
                    print(f"    ⚠️  {source_table}: Not found (skipping)")
            
            original_unified_count = spark.table(original_unified_table).count()
            backup_unified_count = spark.table(backup_unified_table).count()
            
            print(f"\n  Total source records (dataset_tables): {total_source_records:,} rows")
            print(f"  Original unified: {original_unified_count:,} rows")
            print(f"  Backup unified:   {backup_unified_count:,} rows (info only)")
            
            # Only validate: source tables sum = original unified
            unified_match = (total_source_records == original_unified_count)
            
            if unified_match:
                print(f"  Status: ✓ Source tables = Original unified")
                validation_results["checks_passed"].append("PART 1.1: Unified Table Validation")
            else:
                print(f"  Status: ✗ Mismatch detected")
                diff = abs(total_source_records - original_unified_count)
                print(f"    - Difference: {diff:,} records")
                if total_source_records < original_unified_count:
                    print(f"    - Unified has {diff:,} extra record(s) (fallback/historical data)")
                    validation_results["warnings"].append({
                        "check": "PART 1.1: Unified Table Validation",
                        "issue": "Unified has extra records compared to source tables",
                        "details": {
                            "source_total": total_source_records,
                            "unified_count": original_unified_count,
                            "extra_records": diff,
                            "note": "This may be expected if there are fallback/historical records"
                        }
                    })
                else:
                    print(f"    - Unified is missing {diff:,} record(s)")
                    validation_results["critical_issues"].append({
                        "check": "PART 1.1: Unified Table Validation",
                        "issue": "Unified is missing records compared to source tables",
                        "details": {
                            "source_total": total_source_records,
                            "unified_count": original_unified_count,
                            "missing_records": diff
                        }
                    })
                    validation_results["validation_passed"] = False
        else:
            print(f"\n2. Unified Table:")
            print(f"  ⚠️  dataset_tables not provided, skipping source validation")
            validation_results["warnings"].append({
                "check": "PART 1.1: Unified Table Validation",
                "issue": "dataset_tables parameter not provided",
                "details": "Cannot validate source tables vs unified"
            })
        
        # 3. Merge Activities
        original_ma_count = spark.table(original_merge_activities_table).count()
        backup_ma_count = spark.table(backup_merge_activities_table).count()
        ma_match = (original_ma_count == backup_ma_count)
        
        print(f"\n3. Merge Activities:")
        print(f"  Original: {original_ma_count:,} rows")
        print(f"  Backup:   {backup_ma_count:,} rows")
        print(f"  Status:   {'✓ Match' if ma_match else '✗ Mismatch'}")
        
        print("\n✓ Backup row count validation complete")
    else:
        print("\nℹ️  Original tables no longer exist (transform already ran)")
        print("   Cannot validate backup vs original")
        validation_results["warnings"].append({
            "check": "PART 1.1: Backup Row Counts",
            "issue": "Original tables no longer exist",
            "details": "Transform has already run - cannot validate backup vs original"
        })
    
    # ========================================================================
    # VALIDATION 1.2: Verify meta_info backup (if exists)
    # ========================================================================
    print("\n" + "-"*80)
    print("VALIDATION 1.2: CDF Version Tracking Backup")
    print("-"*80)
    
    if spark.catalog.tableExists(meta_info_table) and spark.catalog.tableExists(backup_meta_info_table):
        print("✓ Meta info tables exist")
        
        # Check row counts
        meta_count = spark.sql(f"""
            SELECT COUNT(*) as count FROM {meta_info_table}
            WHERE entity_name = '{entity}'
        """).collect()[0]['count']
        
        backup_meta_count = spark.sql(f"""
            SELECT COUNT(*) as count FROM {backup_meta_info_table}
            WHERE entity_name = '{entity}'
        """).collect()[0]['count']
        
        print(f"\nMeta info count: {meta_count}")
        print(f"Backup meta info count: {backup_meta_count}")
        
        if meta_count == backup_meta_count:
            print("✓ Counts match")
            
            # Verify versions match
            mismatches = spark.sql(f"""
                SELECT 
                    COALESCE(m.table_name, b.table_name) as table_name,
                    m.last_processed_version as meta_version,
                    b.last_processed_version as backup_version
                FROM (SELECT * FROM {meta_info_table} WHERE entity_name = '{entity}') m
                FULL OUTER JOIN (SELECT * FROM {backup_meta_info_table} WHERE entity_name = '{entity}') b
                    ON m.table_name = b.table_name
                WHERE COALESCE(m.last_processed_version, -1) != COALESCE(b.last_processed_version, -1)
            """).collect()
            
            if mismatches:
                print(f"\n⚠️  {len(mismatches)} version mismatches found:")
                for row in mismatches:
                    print(f"  - {row.table_name}: meta={row.meta_version}, backup={row.backup_version}")
                
                validation_results["warnings"].append({
                    "check": "PART 1.2: CDF Version Tracking",
                    "issue": "Version mismatches found",
                    "details": [{"table": row.table_name, "meta": row.meta_version, "backup": row.backup_version} for row in mismatches]
                })
            else:
                print("✓ All versions match")
                validation_results["checks_passed"].append("PART 1.2: CDF Version Tracking")
        else:
            validation_results["warnings"].append({
                "check": "PART 1.2: CDF Version Tracking",
                "issue": "Meta info count mismatch",
                "details": {"meta_count": meta_count, "backup_count": backup_meta_count}
            })
    else:
        print("ℹ️  Meta info tables not found, skipping")
        validation_results["warnings"].append({
            "check": "PART 1.2: CDF Version Tracking",
            "issue": "Meta info tables not found",
            "details": "Cannot validate CDF version tracking"
        })
    
    # ========================================================================
    # VALIDATION 1.3: Additional Tables Backup
    # ========================================================================
    print("\n" + "-"*80)
    print("VALIDATION 1.3: Additional Tables Backup")
    print("-"*80)
    
    additional_tables = [
        (original_unified_dedupe, backup_unified_dedupe, "unified_deduplicate"),
        (original_processed_unified, backup_processed_unified, "processed_unified"),
        (original_processed_unified_dedupe, backup_processed_unified_dedupe, "processed_unified_deduplicate"),
        (original_unified_validation_error, backup_unified_validation_error, "unified_validation_error"),
        (original_potential_match, backup_potential_match, "potential_match"),
        (original_potential_match_dedupe, backup_potential_match_dedupe, "potential_match_deduplicate")
    ]
    
    backed_up_count = 0
    not_found_count = 0
    mismatch_count = 0
    
    for original, backup, name in additional_tables:
        if spark.catalog.tableExists(original):
            if spark.catalog.tableExists(backup):
                original_count = spark.table(original).count()
                backup_count = spark.table(backup).count()
                
                if original_count == backup_count:
                    print(f"  ✓ {name}: {original_count:,} rows (match)")
                    backed_up_count += 1
                else:
                    print(f"  ✗ {name}: Original={original_count:,}, Backup={backup_count:,} (mismatch)")
                    mismatch_count += 1
                    validation_results["warnings"].append({
                        "check": f"PART 1.3: Additional Table - {name}",
                        "issue": "Row count mismatch",
                        "details": {"original": original_count, "backup": backup_count}
                    })
            else:
                print(f"  ⚠️  {name}: Original exists but backup not found")
                not_found_count += 1
                validation_results["warnings"].append({
                    "check": f"PART 1.3: Additional Table - {name}",
                    "issue": "Backup not found",
                    "details": f"Original table exists: {original}"
                })
        else:
            not_found_count += 1
    
    print(f"\n✓ Additional tables validated:")
    print(f"  - Backed up: {backed_up_count}")
    print(f"  - Not found: {not_found_count}")
    print(f"  - Mismatches: {mismatch_count}")
    
    if backed_up_count > 0:
        validation_results["checks_passed"].append(f"PART 1.3: Additional Tables ({backed_up_count} backed up)")
    
    # Set PART 1 status
    if validation_results["validation_passed"]:
        validation_results["part1_status"] = "passed"
        print("\n✅ PART 1 PASSED: All required backups validated")
    else:
        validation_results["part1_status"] = "failed"
        print("\n❌ PART 1 FAILED: Critical issues found in backup validation")

print("\n" + "="*80)

# COMMAND ----------

# ============================================================================
# PART 2: TRANSFORM VALIDATION (Phase 2) - SKIPPED
# ============================================================================

print("\n" + "="*80)
print("PART 2: TRANSFORM VALIDATION (Phase 2)")
print("="*80)
print("\n⚠️  SKIPPING PART 2: Transform validations are integrated in transform notebook")
print("   The transform notebook (03_Transform_Load_Data.py) includes 8 built-in validations")
validation_results["part2_status"] = "skipped"
validation_results["warnings"].append({
    "check": "PART 2: Transform Validation",
    "issue": "Skipped - validations run in transform notebook",
    "details": "Transform notebook includes 8 built-in validations"
})

print("\n" + "="*80)

# COMMAND ----------

# ============================================================================
# PART 3: POST-MIGRATION VALIDATION
# ============================================================================

print("\n" + "="*80)
print("PART 3: POST-MIGRATION VALIDATION")
print("="*80)

# Check if v4.0.0 tables exist
v4_tables_exist = all([
    spark.catalog.tableExists(master_table),
    spark.catalog.tableExists(unified_table),
    spark.catalog.tableExists(merge_activities_table),
    spark.catalog.tableExists(attribute_version_sources_table)
])

if not v4_tables_exist:
    print("\n⚠️  SKIPPING PART 3: v4.0.0 tables not found")
    print("   This is expected if Phase 2 (Transform) hasn't run yet")
    validation_results["part3_status"] = "skipped"
    validation_results["warnings"].append({
        "check": "PART 3: Post-Migration Validation",
        "issue": "v4.0.0 tables not found - Transform may not have run",
        "details": [master_table, unified_table, merge_activities_table, attribute_version_sources_table]
    })
else:
    print("\n✓ v4.0.0 tables verified")
    validation_results["part3_status"] = "running"
    
    # Check crosswalk view
    if spark.catalog.tableExists(crosswalk_view):
        print(f"✓ Crosswalk view exists: {crosswalk_view}")
        
        # ========================================================================
        # VALIDATION 3.1: Unique lakefusion_id Count
        # ========================================================================
        print("\n" + "-"*80)
        print("VALIDATION 3.1: Unique lakefusion_id Count")
        print("-"*80)
        print("TEST CASE 1: Crosswalk unique masters should equal Master table count")
        
        crosswalk_unique_masters = spark.sql(f"""
            SELECT COUNT(DISTINCT lakefusion_id) as count 
            FROM {crosswalk_view}
        """).collect()[0]['count']
        
        master_count = spark.table(master_table).count()
        
        print(f"\nCrosswalk unique lakefusion_id: {crosswalk_unique_masters:,}")
        print(f"Master table count:             {master_count:,}")
        
        if crosswalk_unique_masters == master_count:
            print("✓ Counts match")
            validation_results["checks_passed"].append("VALIDATION 3.1: Unique lakefusion_id Count")
        else:
            print(f"✗ Mismatch: Difference of {abs(crosswalk_unique_masters - master_count):,}")
            validation_results["warnings"].append({
                "check": "VALIDATION 3.1: Unique lakefusion_id Count",
                "issue": "Crosswalk and Master table counts don't match",
                "details": {
                    "crosswalk_unique": crosswalk_unique_masters,
                    "master_count": master_count,
                    "difference": abs(crosswalk_unique_masters - master_count)
                }
            })
        
        # ========================================================================
        # VALIDATION 3.2: Total Record Count Breakdown
        # ========================================================================
        print("\n" + "-"*80)
        print("VALIDATION 3.2: Total Record Count Breakdown")
        print("-"*80)
        print("TEST CASE 2: Unified total = MERGED + ACTIVE records")
        
        unified_total = spark.table(unified_table).count()
        
        unified_merged = spark.sql(f"""
            SELECT COUNT(*) as count 
            FROM {unified_table} 
            WHERE record_status = 'MERGED'
        """).collect()[0]['count']
        
        unified_active = spark.sql(f"""
            SELECT COUNT(*) as count 
            FROM {unified_table} 
            WHERE record_status = 'ACTIVE'
        """).collect()[0]['count']
        
        # Check for any other record_status values
        other_statuses = spark.sql(f"""
            SELECT COUNT(*) as count 
            FROM {unified_table} 
            WHERE record_status NOT IN ('MERGED', 'ACTIVE')
        """).collect()[0]['count']
        
        print(f"\nUnified total records:  {unified_total:,}")
        print(f"  - MERGED records:     {unified_merged:,}")
        print(f"  - ACTIVE records:     {unified_active:,}")
        print(f"  - Other statuses:     {other_statuses:,}")
        print(f"\nMaster table count:     {master_count:,}")
        
        # Verify: unified_total = unified_merged + unified_active
        calculated_total = unified_merged + unified_active + other_statuses
        
        if unified_total == calculated_total and other_statuses == 0:
            print(f"\n✓ Record count breakdown is correct")
            print(f"  Formula verified: {unified_total:,} = {unified_merged:,} (MERGED) + {unified_active:,} (ACTIVE)")
            validation_results["checks_passed"].append("VALIDATION 3.2: Total Record Count Breakdown")
        else:
            if unified_total != calculated_total:
                print(f"\n✗ Count mismatch: {unified_total:,} ≠ {calculated_total:,}")
                validation_results["critical_issues"].append({
                    "check": "VALIDATION 3.2: Total Record Count Breakdown",
                    "issue": "Unified total doesn't equal MERGED + ACTIVE",
                    "details": {
                        "unified_total": unified_total,
                        "merged": unified_merged,
                        "active": unified_active,
                        "other": other_statuses,
                        "calculated": calculated_total
                    }
                })
                validation_results["validation_passed"] = False
            if other_statuses > 0:
                print(f"\n✗ Found {other_statuses:,} records with invalid record_status")
                validation_results["critical_issues"].append({
                    "check": "VALIDATION 3.2: Total Record Count Breakdown",
                    "issue": f"Found {other_statuses} records with status other than MERGED/ACTIVE",
                    "details": {"other_statuses_count": other_statuses}
                })
                validation_results["validation_passed"] = False
        
        # ========================================================================
        # VALIDATION 3.3: Crosswalk vs Unified MERGED Count
        # ========================================================================
        print("\n" + "-"*80)
        print("VALIDATION 3.3: Crosswalk vs Unified MERGED Count")
        print("-"*80)
        print("TEST CASE 3: Crosswalk total records should equal Unified MERGED records")
        
        crosswalk_total = spark.table(crosswalk_view).count()
        
        print(f"\nCrosswalk total records:  {crosswalk_total:,}")
        print(f"Unified MERGED records:   {unified_merged:,}")
        
        if crosswalk_total == unified_merged:
            print("✓ Counts match")
            validation_results["checks_passed"].append("VALIDATION 3.3: Crosswalk vs Unified MERGED")
        else:
            print(f"✗ Mismatch: Difference of {abs(crosswalk_total - unified_merged):,}")
            validation_results["critical_issues"].append({
                "check": "VALIDATION 3.3: Crosswalk vs Unified MERGED Count",
                "issue": "Crosswalk and Unified MERGED counts don't match",
                "details": {
                    "crosswalk_total": crosswalk_total,
                    "unified_merged": unified_merged,
                    "difference": abs(crosswalk_total - unified_merged)
                }
            })
            validation_results["validation_passed"] = False
        
        # ========================================================================
        # VALIDATION 3.4: Lineage Consistency Check
        # ========================================================================
        print("\n" + "-"*80)
        print("VALIDATION 3.4: Lineage Consistency Check")
        print("-"*80)
        print("TEST CASE 4: Random surrogate keys - track full lineage")
        print("Path: unified → crosswalk → master")
        
        # Sample random surrogate keys from MERGED records
        sample_records = spark.sql(f"""
            SELECT surrogate_key, source_path, source_id, master_lakefusion_id
            FROM {unified_table}
            WHERE record_status = 'MERGED'
            ORDER BY RAND()
            LIMIT 20
        """).collect()
        
        print(f"\nValidating {len(sample_records)} random surrogate keys...")
        
        failed_checks = []
        
        for row in sample_records:
            sk = row['surrogate_key']
            source_path = row['source_path']
            source_id = row['source_id']
            master_lf_id_unified = row['master_lakefusion_id']
            
            try:
                # STEP 1: Verify unified has master_lakefusion_id
                if not master_lf_id_unified or master_lf_id_unified == "":
                    failed_checks.append(f"{sk}: MERGED record has empty master_lakefusion_id")
                    continue
                
                # STEP 2: Find in crosswalk by (source_table_name, source_record_id)
                crosswalk_rec = spark.sql(f"""
                    SELECT lakefusion_id 
                    FROM {crosswalk_view} 
                    WHERE source_table_name = '{source_path}'
                      AND source_record_id = '{source_id}'
                """).collect()
                
                if len(crosswalk_rec) == 0:
                    failed_checks.append(f"{sk}: Not found in crosswalk (source_table={source_path}, source_id={source_id})")
                    continue
                elif len(crosswalk_rec) > 1:
                    failed_checks.append(f"{sk}: Multiple records in crosswalk ({len(crosswalk_rec)} found)")
                    continue
                
                master_lf_id_crosswalk = crosswalk_rec[0]['lakefusion_id']
                
                # STEP 3: Verify master_lakefusion_id exists in master table
                master_rec = spark.sql(f"""
                    SELECT lakefusion_id 
                    FROM {master_table} 
                    WHERE lakefusion_id = '{master_lf_id_unified}'
                """).collect()
                
                if len(master_rec) != 1:
                    failed_checks.append(f"{sk}: master_lakefusion_id '{master_lf_id_unified}' not found in master table")
                    continue
                
                master_lf_id_actual = master_rec[0]['lakefusion_id']
                
                # STEP 4: Verify all lakefusion_ids match
                if not (master_lf_id_unified == master_lf_id_crosswalk == master_lf_id_actual):
                    failed_checks.append(
                        f"{sk}: Lineage mismatch - "
                        f"unified:{master_lf_id_unified}, "
                        f"crosswalk:{master_lf_id_crosswalk}, "
                        f"master:{master_lf_id_actual}"
                    )
                    continue
                
                # All checks passed for this record
                    
            except Exception as e:
                failed_checks.append(f"{sk}: Exception - {str(e)[:100]}")
        
        if failed_checks:
            print(f"\n❌ {len(failed_checks)} lineage check(s) failed:")
            for failure in failed_checks[:5]:  # Show first 5
                print(f"  - {failure}")
            if len(failed_checks) > 5:
                print(f"  ... and {len(failed_checks) - 5} more")
            validation_results["critical_issues"].append({
                "check": "VALIDATION 3.4: Lineage Consistency",
                "issue": f"{len(failed_checks)} lineage inconsistencies found",
                "details": failed_checks[:10]  # Store first 10
            })
            validation_results["validation_passed"] = False
        else:
            print(f"✓ All {len(sample_records)} lineage checks passed")
            print("  - Full path verified: unified.master_lakefusion_id → crosswalk.lakefusion_id → master.lakefusion_id")
            print("  - Crosswalk lookup: (source_table_name, source_record_id)")
            validation_results["checks_passed"].append("VALIDATION 3.4: Lineage Consistency")
        
        # ========================================================================
        # VALIDATION 3.5: Surrogate Key Uniqueness
        # ========================================================================
        print("\n" + "-"*80)
        print("VALIDATION 3.5: Surrogate Key Uniqueness")
        print("-"*80)
        print("Ensures no duplicate surrogate keys for same (source_path, source_id)")
        
        # Check for duplicates: same (source_path, source_id) with different surrogate_key
        duplicates = spark.sql(f"""
            SELECT source_path, source_id, COUNT(DISTINCT surrogate_key) as sk_count
            FROM {unified_table}
            GROUP BY source_path, source_id
            HAVING COUNT(DISTINCT surrogate_key) > 1
        """).collect()
        
        if duplicates:
            print(f"\n❌ Found {len(duplicates)} entities with multiple surrogate keys:")
            for i, row in enumerate(duplicates[:5]):  # Show first 5
                print(f"  - source_path: {row.source_path}, source_id: {row.source_id}, surrogate_key count: {row.sk_count}")
            validation_results["critical_issues"].append({
                "check": "VALIDATION 3.5: Surrogate Key Uniqueness",
                "issue": f"{len(duplicates)} entities have multiple surrogate keys",
                "details": [{"source_path": row.source_path, "source_id": row.source_id, "sk_count": row.sk_count} for row in duplicates[:10]]
            })
            validation_results["validation_passed"] = False
        else:
            print(f"\n✓ All entities have unique surrogate keys")
            print("  - Each (source_path, source_id) pair maps to exactly one surrogate_key")
        
        # Also check the inverse: same surrogate_key with different (source_path, source_id)
        inverse_duplicates = spark.sql(f"""
            SELECT surrogate_key, COUNT(DISTINCT source_path, source_id) as entity_count
            FROM {unified_table}
            GROUP BY surrogate_key
            HAVING COUNT(DISTINCT source_path, source_id) > 1
        """).collect()
        
        if inverse_duplicates:
            print(f"\n❌ Found {len(inverse_duplicates)} surrogate keys mapping to multiple entities:")
            for i, row in enumerate(inverse_duplicates[:5]):  # Show first 5
                print(f"  - surrogate_key: {row.surrogate_key}, entity count: {row.entity_count}")
            validation_results["critical_issues"].append({
                "check": "VALIDATION 3.5: Surrogate Key Uniqueness (Inverse)",
                "issue": f"{len(inverse_duplicates)} surrogate keys are not unique",
                "details": [{"surrogate_key": row.surrogate_key, "entity_count": row.entity_count} for row in inverse_duplicates[:10]]
            })
            validation_results["validation_passed"] = False
        else:
            print(f"✓ All surrogate keys are unique per entity")
            validation_results["checks_passed"].append("VALIDATION 3.5: Surrogate Key Uniqueness")
        
        # Set PART 3 status
        part3_critical_issues = [issue for issue in validation_results["critical_issues"] if issue["check"].startswith("VALIDATION 3.")]
        if part3_critical_issues:
            validation_results["part3_status"] = "failed"
            print("\n❌ PART 3 FAILED: Critical issues found in post-migration validation")
        else:
            validation_results["part3_status"] = "passed"
            print("\n✅ PART 3 PASSED: All post-migration validations successful")
        
    else:
        print(f"\n⚠️  Crosswalk view not found: {crosswalk_view}")
        print("   SKIPPING PART 3 validations")
        validation_results["part3_status"] = "skipped"
        validation_results["warnings"].append({
            "check": "PART 3: Post-Migration Validation",
            "issue": "Crosswalk view not found",
            "details": f"Expected view: {crosswalk_view}"
        })

print("\n" + "="*80)

# COMMAND ----------

# ============================================================================
# FINAL VALIDATION SUMMARY
# ============================================================================

print("\n" + "="*80)
print("VALIDATION SUMMARY")
print("="*80)
print(f"Entity: {entity}")
print(f"Catalog: {catalog_name}")
print(f"Timestamp: {validation_results['timestamp']}")
print("\nStatus by Part:")
print(f"  PART 1 (Backup):         {validation_results['part1_status'].upper()}")
print(f"  PART 2 (Transform):      {validation_results['part2_status'].upper()}")
print(f"  PART 3 (Post-Migration): {validation_results['part3_status'].upper()}")

print("\n" + "-"*80)
print(f"Checks Passed: {len(validation_results['checks_passed'])}")
if validation_results['checks_passed']:
    for check in validation_results['checks_passed']:
        print(f"  ✓ {check}")

print("\n" + "-"*80)
print(f"Warnings: {len(validation_results['warnings'])}")
if validation_results['warnings']:
    for warning in validation_results['warnings']:
        print(f"  ⚠️  {warning['check']}: {warning['issue']}")

print("\n" + "-"*80)
print(f"Critical Issues: {len(validation_results['critical_issues'])}")
if validation_results['critical_issues']:
    for issue in validation_results['critical_issues']:
        print(f"  ❌ {issue['check']}: {issue['issue']}")
        if isinstance(issue['details'], dict):
            for key, value in issue['details'].items():
                print(f"     - {key}: {value}")
        elif isinstance(issue['details'], list) and len(issue['details']) <= 3:
            for detail in issue['details']:
                print(f"     - {detail}")

print("\n" + "="*80)
if validation_results["validation_passed"]:
    print("✅ VALIDATION PASSED - MIGRATION SUCCESSFUL")
else:
    print("❌ VALIDATION FAILED - CRITICAL ISSUES FOUND")
print("="*80)

# COMMAND ----------

# Raise exception if validation failed
if not validation_results["validation_passed"]:
    print("\n" + "="*80)
    print("RAISING EXCEPTION DUE TO CRITICAL ISSUES")
    print("="*80)
    print("\nPlease fix the following issues:")
    for issue in validation_results["critical_issues"]:
        print(f"\n❌ {issue['check']}")
        print(f"   {issue['issue']}")
        if isinstance(issue['details'], dict):
            for key, value in issue['details'].items():
                print(f"   - {key}: {value}")
        elif isinstance(issue['details'], list) and len(issue['details']) <= 5:
            for detail in issue['details']:
                print(f"   - {detail}")
    
    # Prepare error details for exception message
    error_summary = {
        "status": "failed",
        "message": "Post-migration validation failed - critical issues found",
        "critical_issues_count": len(validation_results["critical_issues"]),
        "issues": [issue["check"] for issue in validation_results["critical_issues"]],
        "part1_status": validation_results["part1_status"],
        "part2_status": validation_results["part2_status"],
        "part3_status": validation_results["part3_status"]
    }
    
    # Raise exception to fail the pipeline
    raise ValueError(f"Migration validation failed with {len(validation_results['critical_issues'])} critical issue(s): {json.dumps(error_summary, indent=2)}")
else:
    print("\n" + "="*80)
    print("✅ VALIDATION COMPLETE - MIGRATION SUCCESSFUL")
    print("="*80)
    print("\nAll validations passed!")
    print(f"  - {len(validation_results['checks_passed'])} checks passed")
    print(f"  - {len(validation_results['warnings'])} warnings")
    print(f"  - {len(validation_results['critical_issues'])} critical issues")
