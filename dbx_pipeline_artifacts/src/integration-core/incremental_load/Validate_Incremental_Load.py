# Databricks notebook source
# MAGIC %md
# MAGIC # Validate Incremental Load
# MAGIC
# MAGIC Validates CDF (Change Data Feed) data quality before processing incremental
# MAGIC changes. Performs 6 validation checks to ensure data integrity before
# MAGIC proceeding with insert/update/delete processing.
# MAGIC
# MAGIC **Validations:**
# MAGIC 1. CDF Data Readability
# MAGIC 2. Schema Drift Detection
# MAGIC 3. PK Duplicates in Source Tables
# MAGIC 4. Record Count Anomalies
# MAGIC 5. CDF Version Continuity
# MAGIC 6. Update/Delete Orphan Detection

# COMMAND ----------

import json
from datetime import datetime

from pyspark.sql.functions import col, lit, udf, count, when, desc, row_number
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("primary_table", "", "Primary Table")
dbutils.widgets.text("primary_key", "", "Primary Key Column")
dbutils.widgets.text("dataset_tables", "[]", "Dataset Tables (JSON)")
dbutils.widgets.text("attributes_mapping_json", "[]", "Attributes Mapping (JSON)")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("has_inserts", "false", "Has Inserts")
dbutils.widgets.text("has_updates", "false", "Has Updates")
dbutils.widgets.text("has_deletes", "false", "Has Deletes")
dbutils.widgets.text("tables_with_inserts", "[]", "Tables With Inserts (JSON)")
dbutils.widgets.text("tables_with_updates", "[]", "Tables With Updates (JSON)")
dbutils.widgets.text("tables_with_deletes", "[]", "Tables With Deletes (JSON)")
dbutils.widgets.text("table_version_info", "{}", "Table Version Info (JSON)")

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
entity = dbutils.widgets.get("entity")
primary_table = dbutils.widgets.get("primary_table")
primary_key = dbutils.widgets.get("primary_key")
dataset_tables = dbutils.widgets.get("dataset_tables")
attributes_mapping_json = dbutils.widgets.get("attributes_mapping_json")
experiment_id = dbutils.widgets.get("experiment_id")
has_inserts = dbutils.widgets.get("has_inserts")
has_updates = dbutils.widgets.get("has_updates")
has_deletes = dbutils.widgets.get("has_deletes")
tables_with_inserts = dbutils.widgets.get("tables_with_inserts")
tables_with_updates = dbutils.widgets.get("tables_with_updates")
tables_with_deletes = dbutils.widgets.get("tables_with_deletes")
table_version_info = dbutils.widgets.get("table_version_info")

# COMMAND ----------

# Get values from Parse_Entity_Model_JSON
entity = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="entity", debugValue=entity)
primary_table = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="primary_table", debugValue=primary_table)
primary_key = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="primary_key", debugValue=primary_key)
dataset_tables = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="dataset_tables", debugValue=dataset_tables)
attributes_mapping_json = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="attributes_mapping", debugValue=attributes_mapping_json)
catalog_name = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="catalog_name", debugValue=catalog_name)
 
# Get values from Check_Increments_Exists
has_inserts = dbutils.jobs.taskValues.get(taskKey="Check_Increments_Exists", key="has_inserts", debugValue=has_inserts)
has_updates = dbutils.jobs.taskValues.get(taskKey="Check_Increments_Exists", key="has_updates", debugValue=has_updates)
has_deletes = dbutils.jobs.taskValues.get(taskKey="Check_Increments_Exists", key="has_deletes", debugValue=has_deletes)
tables_with_inserts = dbutils.jobs.taskValues.get(taskKey="Check_Increments_Exists", key="tables_with_inserts", debugValue=tables_with_inserts)
tables_with_updates = dbutils.jobs.taskValues.get(taskKey="Check_Increments_Exists", key="tables_with_updates", debugValue=tables_with_updates)
tables_with_deletes = dbutils.jobs.taskValues.get(taskKey="Check_Increments_Exists", key="tables_with_deletes", debugValue=tables_with_deletes)
table_version_info = dbutils.jobs.taskValues.get(taskKey="Check_Increments_Exists", key="table_version_info", debugValue=table_version_info)

# COMMAND ----------

# Parse JSON parameters
dataset_tables = json.loads(dataset_tables) if isinstance(dataset_tables, str) and dataset_tables else []
attributes_mapping_json = json.loads(attributes_mapping_json) if isinstance(attributes_mapping_json, str) else attributes_mapping_json
tables_with_inserts = json.loads(tables_with_inserts) if isinstance(tables_with_inserts, str) else tables_with_inserts
tables_with_updates = json.loads(tables_with_updates) if isinstance(tables_with_updates, str) else tables_with_updates
tables_with_deletes = json.loads(tables_with_deletes) if isinstance(tables_with_deletes, str) else tables_with_deletes
table_version_info = json.loads(table_version_info) if isinstance(table_version_info, str) else table_version_info

# Convert boolean flags
if isinstance(has_inserts, str):
    has_inserts = has_inserts.lower() == 'true'
if isinstance(has_updates, str):
    has_updates = has_updates.lower() == 'true'
if isinstance(has_deletes, str):
    has_deletes = has_deletes.lower() == 'true'

# Remove experiment_id hyphens
if experiment_id:
    experiment_id = experiment_id.replace("-", "")

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

logger.info("=" * 60)
logger.info("INCREMENTAL LOAD VALIDATION")
logger.info("=" * 60)
logger.info(f"Entity: {entity}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment ID: {experiment_id or 'None'}")
logger.info(f"Has Inserts: {has_inserts}")
logger.info(f"Has Updates: {has_updates}")
logger.info(f"Has Deletes: {has_deletes}")
logger.info(f"Tables with Inserts: {len(tables_with_inserts)}")
logger.info(f"Tables with Updates: {len(tables_with_updates)}")
logger.info(f"Tables with Deletes: {len(tables_with_deletes)}")
logger.info("=" * 60)

# COMMAND ----------

# Build table names
experiment_suffix = f"_{experiment_id}" if experiment_id else ""
unified_table = f"{catalog_name}.silver.{entity}_unified{experiment_suffix}"
meta_info_table = f"{catalog_name}.silver.table_meta_info"

# All tables that have any changes
all_changed_tables = list(set(
    tables_with_inserts +
    tables_with_updates +
    tables_with_deletes
))

logger.info(f"Primary Key: {primary_key}")
logger.info(f"Unified Table: {unified_table}")
logger.info(f"Total tables with changes: {len(all_changed_tables)}")

# COMMAND ----------

from lakefusion_core_engine.identifiers import generate_surrogate_key

generate_surrogate_key_udf = udf(
    lambda source_path, source_id: generate_surrogate_key(source_path, str(source_id)),
    StringType()
)

# COMMAND ----------

# Initialize validation tracking
validation_results = {
    "entity": entity,
    "timestamp": datetime.now().isoformat(),
    "critical_issues": [],
    "warnings": [],
    "checks_passed": [],
    "validation_passed": True
}

unreadable_tables = []
schema_drift_issues = []
pk_column_missing_tables = []
tables_with_pk_issues = []
record_count_anomalies = []
version_gaps = []
orphan_updates = []
orphan_deletes = []

# CDF DataFrame cache
cdf_cache = {}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def get_mapping_for_table(source_table):
    """Get attribute mapping for a source table. Returns {entity_attr: dataset_attr}."""
    # Pass 1: exact match
    for mapping_entry in attributes_mapping_json:
        for table_path, mapping in mapping_entry.items():
            if table_path == source_table:
                return mapping
    # Pass 2: _cleaned suffix match (for DQ-enabled tables)
    for mapping_entry in attributes_mapping_json:
        for table_path, mapping in mapping_entry.items():
            if source_table == f"{table_path}_cleaned":
                return mapping
    return None


def get_source_pk_column(table_mapping):
    """Get the source column name mapped to the primary key."""
    return table_mapping.get(primary_key)


def get_cdf_dataframe(source_table):
    """Get cached CDF DataFrame for a table, reading it if not cached."""
    if source_table in cdf_cache:
        return cdf_cache[source_table]

    version_info = table_version_info.get(source_table)
    if not version_info:
        return None

    start_version = version_info.get("starting_version", 0)
    end_version = version_info.get("current_version", 0)

    try:
        df_cdf = spark.read.format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingVersion", start_version) \
            .option("endingVersion", end_version) \
            .table(source_table)
        cdf_cache[source_table] = df_cdf
        return df_cdf
    except Exception:
        return None


def get_update_delete_records(df_cdf, source_pk_column, source_table, change_type):
    """Filter CDF for UPDATE or DELETE records with deduplication."""
    target_type = "update" if change_type == "update" else "delete"

    df_classified = df_cdf.withColumn(
        "operation_type",
        when(col("_change_type") == "insert", lit("insert"))
        .when(col("_change_type") == "update_postimage", lit("update"))
        .when(col("_change_type") == "delete", lit("delete"))
        .otherwise(col("_change_type"))
    )

    df_classified = df_classified.withColumn(
        "_source_id_str", col(source_pk_column).cast("string")
    ).withColumn(
        "surrogate_key",
        generate_surrogate_key_udf(lit(source_table), col("_source_id_str"))
    )

    window_spec = Window.partitionBy("surrogate_key") \
        .orderBy(desc("_commit_version"), desc("_commit_timestamp"))

    df_latest = df_classified.withColumn(
        "_rn", row_number().over(window_spec)
    ).filter(col("_rn") == 1).drop("_rn")

    return df_latest.filter(col("operation_type") == target_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation 1: CDF Data Readability

# COMMAND ----------

logger.info("\n" + "=" * 60)
logger.info("VALIDATION 1: CDF DATA READABILITY")
logger.info("=" * 60)

for source_table in all_changed_tables:
    version_info = table_version_info.get(source_table)
    if not version_info:
        unreadable_tables.append({
            "table": source_table,
            "reason": "No version info available"
        })
        logger.info(f"  [FAIL] No version info: {source_table}")
        continue

    start_version = version_info.get("starting_version", 0)
    end_version = version_info.get("current_version", 0)

    try:
        df_cdf = spark.read.format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingVersion", start_version) \
            .option("endingVersion", end_version) \
            .table(source_table)

        record_count = df_cdf.count()
        cdf_cache[source_table] = df_cdf
        logger.info(f"  [OK] CDF readable: {source_table} (versions {start_version}-{end_version}, {record_count} records)")
    except Exception as e:
        unreadable_tables.append({
            "table": source_table,
            "reason": str(e),
            "version_range": f"{start_version}-{end_version}"
        })
        logger.info(f"  [FAIL] CDF unreadable: {source_table} - {str(e)}")

if unreadable_tables:
    validation_results["critical_issues"].append({
        "check": "CDF Data Readability",
        "issue": "Cannot read CDF for some tables",
        "details": unreadable_tables
    })
    validation_results["validation_passed"] = False
    logger.info(f"\n[CRITICAL] {len(unreadable_tables)} table(s) have unreadable CDF")
else:
    validation_results["checks_passed"].append("CDF Data Readability")
    logger.info(f"\n[PASSED] CDF readable for all {len(all_changed_tables)} table(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation 2: Schema Drift Detection

# COMMAND ----------

logger.info("\n" + "=" * 60)
logger.info("VALIDATION 2: SCHEMA DRIFT DETECTION")
logger.info("=" * 60)

has_pk_drift = False

for source_table in all_changed_tables:
    # Skip if CDF unreadable
    if any(t["table"] == source_table for t in unreadable_tables):
        continue

    table_mapping = get_mapping_for_table(source_table)
    if not table_mapping:
        continue

    df_cdf = get_cdf_dataframe(source_table)
    if df_cdf is None:
        continue

    # CDF columns (exclude internal _change_type, _commit_version, _commit_timestamp)
    cdf_columns = [c for c in df_cdf.columns if not c.startswith("_")]

    source_pk_column = get_source_pk_column(table_mapping)

    for entity_attr, dataset_attr in table_mapping.items():
        if dataset_attr not in cdf_columns:
            is_pk = (dataset_attr == source_pk_column)
            schema_drift_issues.append({
                "table": source_table,
                "entity_attr": entity_attr,
                "dataset_attr": dataset_attr,
                "is_primary_key": is_pk
            })
            if is_pk:
                has_pk_drift = True
                pk_column_missing_tables.append(source_table)
                logger.info(f"  [FAIL] PK column missing: {source_table}.{dataset_attr}")
            else:
                logger.info(f"  [WARN] Column missing: {source_table}.{dataset_attr} -> {entity_attr}")

    if not any(d["table"] == source_table for d in schema_drift_issues):
        logger.info(f"  [OK] No schema drift: {source_table}")

if has_pk_drift:
    validation_results["critical_issues"].append({
        "check": "Schema Drift Detection",
        "issue": "Primary key column missing from source tables",
        "details": [d for d in schema_drift_issues if d["is_primary_key"]]
    })
    validation_results["validation_passed"] = False
    logger.info(f"\n[CRITICAL] Primary key column missing in {len(pk_column_missing_tables)} table(s)")

non_pk_drift = [d for d in schema_drift_issues if not d["is_primary_key"]]
if non_pk_drift:
    validation_results["warnings"].append({
        "check": "Schema Drift Detection",
        "issue": "Non-PK mapped columns missing from source tables",
        "details": non_pk_drift
    })
    logger.info(f"\n[WARNING] {len(non_pk_drift)} non-PK mapped column(s) missing (will be NULL)")

if not schema_drift_issues:
    validation_results["checks_passed"].append("Schema Drift Detection")
    logger.info(f"\n[PASSED] No schema drift detected")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation 3: Primary Key Duplicates in Source Tables
# MAGIC
# MAGIC Checks every source table (that has changes) for duplicate primary keys in its
# MAGIC **current snapshot**. This replaces the old batch-only and unified-conflict checks
# MAGIC with a single, definitive validation: if the source table itself has duplicate PKs,
# MAGIC the pipeline cannot safely process it.
# MAGIC
# MAGIC **Efficiency:** Uses `groupBy().agg(count > 1)` with `limit(1)` to short-circuit
# MAGIC as soon as the first duplicate is found. No `collect()` or full `count()` unless
# MAGIC duplicates are detected and we need details for the error message.

# COMMAND ----------

logger.info("\n" + "=" * 60)
logger.info("VALIDATION 3: PRIMARY KEY DUPLICATES IN SOURCE TABLES")
logger.info("=" * 60)

tables_with_pk_issues = []

for source_table in all_changed_tables:
    # Skip if CDF unreadable or PK column missing
    if any(t["table"] == source_table for t in unreadable_tables):
        continue
    if source_table in pk_column_missing_tables:
        continue

    table_mapping = get_mapping_for_table(source_table)
    if not table_mapping:
        continue

    source_pk_column = get_source_pk_column(table_mapping)
    if not source_pk_column:
        continue

    try:
        # Read the CURRENT snapshot of the source table (not CDF)
        df_source = spark.table(source_table)

        # Check for NULL PKs first — fast isEmpty check
        df_null_pks = df_source.filter(col(source_pk_column).isNull())
        has_null_pks = not df_null_pks.isEmpty()

        if has_null_pks:
            tables_with_pk_issues.append({
                "table": source_table,
                "column": source_pk_column,
                "issue": "NULL primary keys found in source table"
            })
            logger.info(f"  [FAIL] NULL PKs found: {source_table}.{source_pk_column}")
            continue

        # Check for duplicate PKs using groupBy + having count > 1
        # limit(1) short-circuits: stops as soon as first duplicate is found
        df_duplicates = df_source.groupBy(source_pk_column) \
            .agg(count("*").alias("pk_count")) \
            .filter(col("pk_count") > 1) \
            .limit(1)

        has_duplicates = not df_duplicates.isEmpty()

        if has_duplicates:
            tables_with_pk_issues.append({
                "table": source_table,
                "column": source_pk_column,
                "issue": "Duplicate primary keys found in source table"
            })
            logger.info(f"  [FAIL] Duplicate PKs found: {source_table}.{source_pk_column}")
        else:
            logger.info(f"  [OK] All PKs unique: {source_table}.{source_pk_column}")

    except Exception as e:
        logger.info(f"  [WARN] Could not check PKs: {source_table} - {str(e)}")

if tables_with_pk_issues:
    validation_results["critical_issues"].append({
        "check": "PK Duplicates in Source Tables",
        "issue": "Source tables have NULL or duplicate primary keys",
        "details": tables_with_pk_issues
    })
    validation_results["validation_passed"] = False
    logger.info(f"\n[CRITICAL] {len(tables_with_pk_issues)} table(s) have PK issues")
else:
    validation_results["checks_passed"].append("PK Duplicates in Source Tables")
    logger.info(f"\n[PASSED] All source tables have unique, non-null primary keys")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation 4: Record Count Anomalies

# COMMAND ----------

logger.info("\n" + "=" * 60)
logger.info("VALIDATION 4: RECORD COUNT ANOMALIES")
logger.info("=" * 60)

ANOMALY_THRESHOLD_PCT = 50  # inserts > 50% of existing unified count for source

if not spark.catalog.tableExists(unified_table):
    validation_results["checks_passed"].append("Record Count Anomalies")
    logger.info(f"  [OK] Unified table does not exist yet, skipping anomaly check")
else:
    for source_table in tables_with_inserts:
        if any(t["table"] == source_table for t in unreadable_tables):
            continue

        version_info_entry = table_version_info.get(source_table, {})
        insert_count = version_info_entry.get("insert_count", 0)

        if insert_count == 0:
            continue

        try:
            # Count existing unified records from this source
            existing_count = spark.table(unified_table) \
                .filter(col("source_path") == source_table) \
                .count()

            if existing_count == 0:
                logger.info(f"  [OK] First load for source: {source_table} ({insert_count} inserts)")
                continue

            ratio = round((insert_count / existing_count) * 100, 2)

            if ratio > ANOMALY_THRESHOLD_PCT:
                record_count_anomalies.append({
                    "table": source_table,
                    "insert_count": insert_count,
                    "existing_unified_count": existing_count,
                    "ratio_percentage": ratio
                })
                logger.info(f"  [WARN] Anomaly: {source_table}")
                logger.info(f"    Inserts: {insert_count}, Existing: {existing_count}, Ratio: {ratio}%")
            else:
                logger.info(f"  [OK] Normal: {source_table} ({insert_count} inserts vs {existing_count} existing, {ratio}%)")
        except Exception as e:
            logger.info(f"  [WARN] Could not check anomaly: {source_table} - {str(e)}")

    if record_count_anomalies:
        validation_results["warnings"].append({
            "check": "Record Count Anomalies",
            "issue": f"Insert batch sizes exceed {ANOMALY_THRESHOLD_PCT}% of existing records (possible full reload)",
            "details": record_count_anomalies
        })
        logger.info(f"\n[WARNING] {len(record_count_anomalies)} table(s) have unusually large INSERT batches")
    else:
        validation_results["checks_passed"].append("Record Count Anomalies")
        logger.info(f"\n[PASSED] No record count anomalies detected")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation 5: CDF Version Continuity

# COMMAND ----------

logger.info("\n" + "=" * 60)
logger.info("VALIDATION 5: CDF VERSION CONTINUITY")
logger.info("=" * 60)

for source_table in all_changed_tables:
    version_info_entry = table_version_info.get(source_table, {})
    last_processed = version_info_entry.get("last_processed_version")
    starting_version = version_info_entry.get("starting_version", 0)

    if last_processed is None:
        logger.info(f"  [OK] First increment: {source_table} (starting at version {starting_version})")
        continue

    expected_start = last_processed + 1

    if starting_version != expected_start:
        gap_size = starting_version - expected_start
        version_gaps.append({
            "table": source_table,
            "last_processed_version": last_processed,
            "expected_starting_version": expected_start,
            "actual_starting_version": starting_version,
            "gap_size": gap_size
        })
        logger.info(f"  [WARN] Version gap: {source_table}")
        logger.info(f"    Last processed: {last_processed}, Expected start: {expected_start}, Actual start: {starting_version}")
    else:
        logger.info(f"  [OK] Continuous: {source_table} (version {last_processed} -> {starting_version})")

if version_gaps:
    validation_results["warnings"].append({
        "check": "CDF Version Continuity",
        "issue": "Version gaps detected between last processed and current versions",
        "details": version_gaps
    })
    logger.info(f"\n[WARNING] {len(version_gaps)} table(s) have version gaps")
else:
    validation_results["checks_passed"].append("CDF Version Continuity")
    logger.info(f"\n[PASSED] CDF versions are continuous for all tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation 6: Update/Delete Orphan Detection

# COMMAND ----------

logger.info("\n" + "=" * 60)
logger.info("VALIDATION 6: UPDATE/DELETE ORPHAN DETECTION")
logger.info("=" * 60)

if not spark.catalog.tableExists(unified_table):
    validation_results["checks_passed"].append("Update/Delete Orphan Detection")
    logger.info(f"  [OK] Unified table does not exist, skipping orphan check")
else:
    df_unified_keys = spark.table(unified_table).select("surrogate_key")

    # Check UPDATE orphans
    for source_table in tables_with_updates:
        if any(t["table"] == source_table for t in unreadable_tables):
            continue
        if source_table in pk_column_missing_tables:
            continue

        table_mapping = get_mapping_for_table(source_table)
        if not table_mapping:
            continue

        source_pk_column = get_source_pk_column(table_mapping)
        if not source_pk_column:
            continue

        df_cdf = get_cdf_dataframe(source_table)
        if df_cdf is None:
            continue

        try:
            df_updates = get_update_delete_records(
                df_cdf, source_pk_column, source_table, "update"
            )
            update_count = df_updates.count()

            if update_count == 0:
                continue

            df_orphans = df_updates.alias("upd").join(
                df_unified_keys.alias("uni"),
                col("upd.surrogate_key") == col("uni.surrogate_key"),
                "left_anti"
            )
            orphan_count = df_orphans.count()

            if orphan_count > 0:
                orphan_updates.append({
                    "table": source_table,
                    "total_update_records": update_count,
                    "orphan_count": orphan_count
                })
                logger.info(f"  [WARN] Update orphans: {source_table} ({orphan_count}/{update_count} not in unified)")
            else:
                logger.info(f"  [OK] No update orphans: {source_table} ({update_count} updates)")
        except Exception as e:
            logger.info(f"  [WARN] Could not check update orphans: {source_table} - {str(e)}")

    # Check DELETE orphans
    for source_table in tables_with_deletes:
        if any(t["table"] == source_table for t in unreadable_tables):
            continue
        if source_table in pk_column_missing_tables:
            continue

        table_mapping = get_mapping_for_table(source_table)
        if not table_mapping:
            continue

        source_pk_column = get_source_pk_column(table_mapping)
        if not source_pk_column:
            continue

        df_cdf = get_cdf_dataframe(source_table)
        if df_cdf is None:
            continue

        try:
            df_deletes = get_update_delete_records(
                df_cdf, source_pk_column, source_table, "delete"
            )
            delete_count = df_deletes.count()

            if delete_count == 0:
                continue

            df_orphans = df_deletes.alias("del").join(
                df_unified_keys.alias("uni"),
                col("del.surrogate_key") == col("uni.surrogate_key"),
                "left_anti"
            )
            orphan_count = df_orphans.count()

            if orphan_count > 0:
                orphan_deletes.append({
                    "table": source_table,
                    "total_delete_records": delete_count,
                    "orphan_count": orphan_count
                })
                logger.info(f"  [WARN] Delete orphans: {source_table} ({orphan_count}/{delete_count} not in unified)")
            else:
                logger.info(f"  [OK] No delete orphans: {source_table} ({delete_count} deletes)")
        except Exception as e:
            logger.info(f"  [WARN] Could not check delete orphans: {source_table} - {str(e)}")

    has_orphans = len(orphan_updates) > 0 or len(orphan_deletes) > 0

    if has_orphans:
        orphan_details = []
        if orphan_updates:
            orphan_details.extend([{**o, "type": "UPDATE"} for o in orphan_updates])
        if orphan_deletes:
            orphan_details.extend([{**o, "type": "DELETE"} for o in orphan_deletes])

        validation_results["warnings"].append({
            "check": "Update/Delete Orphan Detection",
            "issue": "UPDATE/DELETE records reference PKs not found in unified table",
            "details": orphan_details
        })
        logger.info(f"\n[WARNING] Found orphan records: {len(orphan_updates)} update table(s), {len(orphan_deletes)} delete table(s)")
    else:
        validation_results["checks_passed"].append("Update/Delete Orphan Detection")
        logger.info(f"\n[PASSED] No orphan UPDATE/DELETE records detected")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Summary

# COMMAND ----------

logger.info("\n" + "=" * 60)
logger.info("INCREMENTAL VALIDATION SUMMARY")
logger.info("=" * 60)
logger.info(f"Entity: {entity}")
logger.info(f"Timestamp: {validation_results['timestamp']}")
logger.info(f"Tables with changes: {len(all_changed_tables)}")

logger.info("\n" + "-" * 60)
logger.info("CHECKS PASSED:")
logger.info("-" * 60)
for check in validation_results["checks_passed"]:
    logger.info(f"  [PASSED] {check}")

if validation_results["warnings"]:
    logger.info("\n" + "-" * 60)
    logger.info("WARNINGS:")
    logger.info("-" * 60)
    for warning in validation_results["warnings"]:
        logger.info(f"  [WARN] {warning['check']}: {warning['issue']}")
        if isinstance(warning['details'], list):
            logger.info(f"     Count: {len(warning['details'])}")

if validation_results["critical_issues"]:
    logger.info("\n" + "-" * 60)
    logger.info("CRITICAL ISSUES:")
    logger.info("-" * 60)
    for issue in validation_results["critical_issues"]:
        logger.info(f"  [FAIL] {issue['check']}: {issue['issue']}")
        if isinstance(issue['details'], list):
            logger.info(f"     Count: {len(issue['details'])}")
            for item in issue['details'][:3]:
                if isinstance(item, dict):
                    logger.info(f"     - {item}")
                else:
                    logger.info(f"     - {item}")

logger.info("\n" + "=" * 60)
if validation_results["validation_passed"]:
    logger.info("[SUCCESS] INCREMENTAL VALIDATION PASSED - PIPELINE CAN PROCEED")
else:
    logger.info("[FAILED] INCREMENTAL VALIDATION FAILED - PIPELINE CANNOT PROCEED")
logger.info("=" * 60)

# COMMAND ----------

# Set task values
dbutils.jobs.taskValues.set("validation_passed", validation_results["validation_passed"])
dbutils.jobs.taskValues.set("validation_results", json.dumps(validation_results))

# COMMAND ----------

# Exit pipeline if critical issues found
if not validation_results["validation_passed"]:
    logger.info("\n" + "=" * 60)
    logger.info("EXITING PIPELINE DUE TO CRITICAL ISSUES")
    logger.info("=" * 60)
    logger.info("\nPlease fix the following issues:")
    for issue in validation_results["critical_issues"]:
        logger.info(f"\n[FAIL] {issue['check']}")
        logger.info(f"   {issue['issue']}")
        if isinstance(issue['details'], list) and len(issue['details']) <= 5:
            for detail in issue['details']:
                logger.info(f"   - {detail}")

    error_summary = {
        "status": "failed",
        "message": "Incremental validation failed - critical issues found",
        "critical_issues_count": len(validation_results["critical_issues"]),
        "issues": [issue["check"] for issue in validation_results["critical_issues"]]
    }
    raise ValueError(
        f"Incremental validation failed with {len(validation_results['critical_issues'])} critical issue(s): {json.dumps(error_summary, indent=2)}"
    )
