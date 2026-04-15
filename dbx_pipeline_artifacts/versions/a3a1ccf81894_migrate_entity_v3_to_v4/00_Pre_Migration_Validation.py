# Databricks notebook source
import json
from pyspark.sql.functions import col, count, countDistinct
from datetime import datetime

# COMMAND ----------

dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("dataset_tables", "", "Dataset Tables")
dbutils.widgets.text("attributes_mapping", "", "Attributes Mapping")
dbutils.widgets.text("primary_table", "", "Primary Table")
dbutils.widgets.text("primary_key", "", "Primary Key")

# COMMAND ----------

# Get values from widgets (passed as arguments)
entity = dbutils.widgets.get("entity")
catalog_name = dbutils.widgets.get("catalog_name")
dataset_tables = dbutils.widgets.get("dataset_tables")
attributes_mapping = dbutils.widgets.get("attributes_mapping")
primary_table = dbutils.widgets.get("primary_table")
primary_key = dbutils.widgets.get("primary_key")

# COMMAND ----------

dataset_tables = json.loads(dataset_tables)
attributes_mapping = json.loads(attributes_mapping)

# COMMAND ----------

validation_results = {
    "entity": entity,
    "timestamp": datetime.now().isoformat(),
    "critical_issues": [],
    "warnings": [],
    "checks_passed": [],
    "validation_passed": True
}

# COMMAND ----------

print("="*60)
print("ENTITY CONFIGURATION VALIDATION")
print("="*60)
print(f"Entity: {entity}")
print(f"Catalog: {catalog_name}")
print(f"Primary Table: {primary_table}")
print(f"Primary Key: {primary_key}")
print(f"Total Tables: {len(dataset_tables)}")
print("="*60)

# COMMAND ----------

print("\n" + "="*60)
print("VALIDATION 1: SOURCE TABLE EXISTENCE")
print("="*60)

missing_tables = []

for table in dataset_tables:
    try:
        # Try to describe the table to check if it exists
        spark.sql(f"DESCRIBE TABLE {table}")
        print(f"  ✓ Table exists: {table}")
    except Exception as e:
        missing_tables.append(table)
        print(f"  ❌ Table NOT found: {table}")

if missing_tables:
    validation_results["critical_issues"].append({
        "check": "Source Table Existence",
        "issue": "Missing tables",
        "details": missing_tables
    })
    validation_results["validation_passed"] = False
    print(f"\n❌ CRITICAL: {len(missing_tables)} table(s) do not exist")
else:
    validation_results["checks_passed"].append("Source Table Existence")
    print(f"\n✅ All {len(dataset_tables)} tables exist")

# COMMAND ----------

print("\n" + "="*60)
print("VALIDATION 2: ATTRIBUTE MAPPING COMPLETENESS")
print("="*60)

# Get list of tables that have mappings
mapped_tables = [list(mapping.keys())[0] for mapping in attributes_mapping]

tables_without_mapping = []

for table in dataset_tables:
    if table in mapped_tables:
        print(f"  ✓ Mapping exists: {table}")
    else:
        tables_without_mapping.append(table)
        print(f"  ❌ Mapping NOT found: {table}")

if tables_without_mapping:
    validation_results["critical_issues"].append({
        "check": "Attribute Mapping Completeness",
        "issue": "Tables without attribute mappings",
        "details": tables_without_mapping
    })
    validation_results["validation_passed"] = False
    print(f"\n❌ CRITICAL: {len(tables_without_mapping)} table(s) missing attribute mappings")
else:
    validation_results["checks_passed"].append("Attribute Mapping Completeness")
    print(f"\n✅ All {len(dataset_tables)} tables have attribute mappings")

# COMMAND ----------

print("\n" + "="*60)
print("VALIDATION 3: PRIMARY KEY MAPPING EXISTENCE")
print("="*60)

tables_missing_pk_mapping = []

for table in dataset_tables:
    # Find the mapping for this table
    table_mapping = None
    for mapping_entry in attributes_mapping:
        if table in mapping_entry:
            table_mapping = mapping_entry[table]
            break
    
    if not table_mapping:
        # Already caught in validation 2, skip
        continue
    
    # Check if primary_key is in the mapping KEYS
    # Mapping format: {entity_attr: dataset_attr}
    if primary_key in table_mapping:
        source_pk_column = table_mapping[primary_key]
        print(f"  ✓ Primary key mapped: {table}")
        print(f"    Entity attr '{primary_key}' ← Dataset column '{source_pk_column}'")
    else:
        tables_missing_pk_mapping.append(table)
        print(f"  ❌ Primary key NOT mapped: {table}")
        print(f"    Entity primary key '{primary_key}' not found in mapping keys")

if tables_missing_pk_mapping:
    validation_results["critical_issues"].append({
        "check": "Primary Key Mapping Existence",
        "issue": "Tables missing primary key mapping",
        "details": tables_missing_pk_mapping
    })
    validation_results["validation_passed"] = False
    print(f"\n❌ CRITICAL: {len(tables_missing_pk_mapping)} table(s) missing primary key mapping")
else:
    validation_results["checks_passed"].append("Primary Key Mapping Existence")
    print(f"\n✅ All tables have primary key mappings")

# COMMAND ----------

print("\n" + "="*60)
print("VALIDATION 4: PRIMARY KEY COLUMN EXISTENCE")
print("="*60)

tables_missing_pk_column = []

for table in dataset_tables:
    # Skip if table doesn't exist or has no mapping
    if table in missing_tables or table in tables_without_mapping:
        continue
    
    # Find the mapping for this table
    table_mapping = None
    for mapping_entry in attributes_mapping:
        if table in mapping_entry:
            table_mapping = mapping_entry[table]
            break
    
    # Skip if primary key not mapped
    if primary_key not in table_mapping:
        continue
    
    source_pk_column = table_mapping[primary_key]
    
    try:
        # Read table schema
        table_df = spark.read.table(table)
        table_columns = table_df.columns
        
        if source_pk_column in table_columns:
            print(f"  ✓ Column exists: {table}.{source_pk_column}")
        else:
            tables_missing_pk_column.append({
                "table": table,
                "expected_column": source_pk_column,
                "available_columns": table_columns
            })
            print(f"  ❌ Column NOT found: {table}.{source_pk_column}")
            print(f"    Available columns: {', '.join(table_columns[:5])}...")
    except Exception as e:
        print(f"  ⚠️  Could not read table: {table} - {str(e)}")

if tables_missing_pk_column:
    validation_results["critical_issues"].append({
        "check": "Primary Key Column Existence",
        "issue": "Primary key columns not found in source tables",
        "details": tables_missing_pk_column
    })
    validation_results["validation_passed"] = False
    print(f"\n❌ CRITICAL: {len(tables_missing_pk_column)} table(s) missing primary key column")
else:
    validation_results["checks_passed"].append("Primary Key Column Existence")
    print(f"\n✅ All primary key columns exist in source tables")

# COMMAND ----------

print("\n" + "="*60)
print("VALIDATION 5: PRIMARY KEY NULL CHECK")
print("="*60)

tables_with_nulls = []

for table in dataset_tables:
    # Skip if table has issues from previous validations
    if table in missing_tables or table in tables_without_mapping:
        continue
    
    # Find the mapping for this table
    table_mapping = None
    for mapping_entry in attributes_mapping:
        if table in mapping_entry:
            table_mapping = mapping_entry[table]
            break
    
    # Skip if primary key not mapped
    if primary_key not in table_mapping:
        continue
    
    source_pk_column = table_mapping[primary_key]
    
    # Skip if column doesn't exist
    column_missing = any(item["table"] == table for item in tables_missing_pk_column)
    if column_missing:
        continue
    
    try:
        # Read table and check for NULLs
        table_df = spark.read.table(table)
        total_count = table_df.count()
        null_count = table_df.filter(col(source_pk_column).isNull()).count()
        
        if null_count > 0:
            tables_with_nulls.append({
                "table": table,
                "column": source_pk_column,
                "total_records": total_count,
                "null_count": null_count,
                "null_percentage": round((null_count / total_count) * 100, 2)
            })
            print(f"  ❌ NULLs found: {table}.{source_pk_column}")
            print(f"    Total records: {total_count}, NULL count: {null_count} ({round((null_count / total_count) * 100, 2)}%)")
        else:
            print(f"  ✓ No NULLs: {table}.{source_pk_column} ({total_count} records)")
    except Exception as e:
        print(f"  ⚠️  Could not check NULLs: {table}.{source_pk_column} - {str(e)}")

if tables_with_nulls:
    validation_results["critical_issues"].append({
        "check": "Primary Key NULL Check",
        "issue": "NULL values found in primary key columns",
        "details": tables_with_nulls
    })
    validation_results["validation_passed"] = False
    print(f"\n❌ CRITICAL: {len(tables_with_nulls)} table(s) have NULL values in primary key")
else:
    validation_results["checks_passed"].append("Primary Key NULL Check")
    print(f"\n✅ No NULL values in primary key columns")

# COMMAND ----------

print("\n" + "="*60)
print("VALIDATION 6: PRIMARY KEY UNIQUENESS CHECK")
print("="*60)

tables_with_duplicates = []

for table in dataset_tables:
    # Skip if table has issues from previous validations
    if table in missing_tables or table in tables_without_mapping:
        continue
    
    # Find the mapping for this table
    table_mapping = None
    for mapping_entry in attributes_mapping:
        if table in mapping_entry:
            table_mapping = mapping_entry[table]
            break
    
    # Skip if primary key not mapped
    if primary_key not in table_mapping:
        continue
    
    source_pk_column = table_mapping[primary_key]
    
    # Skip if column doesn't exist
    column_missing = any(item["table"] == table for item in tables_missing_pk_column)
    if column_missing:
        continue
    
    try:
        # Read table and check for duplicates
        table_df = spark.read.table(table)
        total_count = table_df.count()
        distinct_count = table_df.select(source_pk_column).distinct().count()
        duplicate_count = total_count - distinct_count
        
        if duplicate_count > 0:
            tables_with_duplicates.append({
                "table": table,
                "column": source_pk_column,
                "total_records": total_count,
                "distinct_values": distinct_count,
                "duplicate_count": duplicate_count
            })
            print(f"  ❌ Duplicates found: {table}.{source_pk_column}")
            print(f"    Total: {total_count}, Distinct: {distinct_count}, Duplicates: {duplicate_count}")
        else:
            print(f"  ✓ All unique: {table}.{source_pk_column} ({total_count} records)")
    except Exception as e:
        print(f"  ⚠️  Could not check uniqueness: {table}.{source_pk_column} - {str(e)}")

if tables_with_duplicates:
    validation_results["critical_issues"].append({
        "check": "Primary Key Uniqueness Check",
        "issue": "Duplicate values found in primary key columns",
        "details": tables_with_duplicates
    })
    validation_results["validation_passed"] = False
    print(f"\n❌ CRITICAL: {len(tables_with_duplicates)} table(s) have duplicate primary keys")
else:
    validation_results["checks_passed"].append("Primary Key Uniqueness Check")
    print(f"\n✅ All primary key values are unique")

# COMMAND ----------

print("\n" + "="*60)
print("VALIDATION 7: MAPPED COLUMN EXISTENCE")
print("="*60)

missing_mapped_columns = []
special_char_columns = []

for table in dataset_tables:
    # Skip if table doesn't exist or has no mapping
    if table in missing_tables or table in tables_without_mapping:
        continue
    
    # Find the mapping for this table
    table_mapping = None
    for mapping_entry in attributes_mapping:
        if table in mapping_entry:
            table_mapping = mapping_entry[table]
            break
    
    try:
        # Read table schema
        table_df = spark.read.table(table)
        table_columns = table_df.columns
        
        print(f"\n  Table: {table}")
        
        # Check each mapped column
        # Mapping format: {entity_attr: dataset_attr}
        for entity_attr, dataset_attr in table_mapping.items():
            if dataset_attr in table_columns:
                print(f"    ✓ {dataset_attr} → {entity_attr}")
                
                # Check for special characters
                special_chars = [char for char in dataset_attr if not (char.isalnum() or char == '_')]
                if special_chars:
                    special_char_columns.append({
                        "table": table,
                        "column": dataset_attr,
                        "special_chars": list(set(special_chars))
                    })
                    print(f"      ⚠️  Contains special characters: {list(set(special_chars))}")
            else:
                missing_mapped_columns.append({
                    "table": table,
                    "entity_attr": entity_attr,
                    "dataset_attr": dataset_attr,
                    "available_columns": table_columns
                })
                print(f"    ❌ {dataset_attr} → {entity_attr} (column not found)")
    except Exception as e:
        print(f"  ⚠️  Could not read table: {table} - {str(e)}")

if missing_mapped_columns:
    validation_results["warnings"].append({
        "check": "Mapped Column Existence",
        "issue": "Mapped columns not found in source tables",
        "details": missing_mapped_columns
    })
    print(f"\n⚠️  WARNING: {len(missing_mapped_columns)} mapped column(s) not found in source tables")
    print("   Pipeline will continue but these attributes will be NULL")
else:
    validation_results["checks_passed"].append("Mapped Column Existence")
    print(f"\n✅ All mapped columns exist in source tables")

if special_char_columns:
    validation_results["warnings"].append({
        "check": "Special Characters in Column Names",
        "issue": "Columns contain special characters",
        "details": special_char_columns
    })
    print(f"\n⚠️  WARNING: {len(special_char_columns)} column(s) contain special characters")
    print("   This may cause issues in some SQL contexts")

# COMMAND ----------

print("\n" + "="*60)
print("VALIDATION SUMMARY")
print("="*60)
print(f"Entity: {entity}")
print(f"Timestamp: {validation_results['timestamp']}")
print(f"Total Tables: {len(dataset_tables)}")
print("\n" + "-"*60)
print("CHECKS PASSED:")
print("-"*60)
for check in validation_results["checks_passed"]:
    print(f"  ✅ {check}")

if validation_results["warnings"]:
    print("\n" + "-"*60)
    print("WARNINGS:")
    print("-"*60)
    for warning in validation_results["warnings"]:
        print(f"  ⚠️  {warning['check']}: {warning['issue']}")
        if isinstance(warning['details'], list):
            print(f"     Count: {len(warning['details'])}")

if validation_results["critical_issues"]:
    print("\n" + "-"*60)
    print("CRITICAL ISSUES:")
    print("-"*60)
    for issue in validation_results["critical_issues"]:
        print(f"  ❌ {issue['check']}: {issue['issue']}")
        if isinstance(issue['details'], list):
            print(f"     Count: {len(issue['details'])}")
            # Show first few items
            for item in issue['details'][:3]:
                if isinstance(item, dict):
                    print(f"     - {item}")
                else:
                    print(f"     - {item}")

print("\n" + "="*60)
if validation_results["validation_passed"]:
    print("✅ VALIDATION PASSED - PIPELINE CAN PROCEED")
else:
    print("❌ VALIDATION FAILED - PIPELINE CANNOT PROCEED")
print("="*60)

# COMMAND ----------

if not validation_results["validation_passed"]:
    print("\n" + "="*60)
    print("EXITING PIPELINE DUE TO CRITICAL ISSUES")
    print("="*60)
    print("\nPlease fix the following issues:")
    for issue in validation_results["critical_issues"]:
        print(f"\n❌ {issue['check']}")
        print(f"   {issue['issue']}")
        if isinstance(issue['details'], list) and len(issue['details']) <= 5:
            for detail in issue['details']:
                print(f"   - {detail}")
    
    # Prepare error details for exception message
    error_summary = {
        "status": "failed",
        "message": "Validation failed - critical issues found",
        "critical_issues_count": len(validation_results["critical_issues"]),
        "issues": [issue["check"] for issue in validation_results["critical_issues"]]
    }
    
    # Raise exception to fail the entire pipeline
    raise ValueError(f"Pipeline validation failed with {len(validation_results['critical_issues'])} critical issue(s): {json.dumps(error_summary, indent=2)}")
else:
    print("\n" + "="*60)
    print("✅ VALIDATION COMPLETE - PROCEEDING TO CREATE TABLES")
    print("="*60)
