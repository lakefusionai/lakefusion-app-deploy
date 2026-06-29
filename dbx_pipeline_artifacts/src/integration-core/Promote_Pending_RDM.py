# Databricks notebook source
# MAGIC %pip install rapidfuzz --quiet

# COMMAND ----------

"""
PROMOTE PENDING RDM — re-ingest rows previously errored at the RDM stage.

When a steward updates the mapping table (e.g. flips a NO_MATCH row to
AUTO_APPROVED, or adds a missing ref_lakefusion_id), the rows already parked
in the unified error log don't move automatically. This notebook closes that
gap:

  1. Reads surrogate_keys from {entity}_unified_error[_<exp>] WHERE error_stage='RDM'
  2. Re-reads the corresponding source rows from each source table (matching
     by deterministic surrogate_key)
  3. Re-runs the RDM resolver — this time picking up the steward's updates
  4. Promotes rows now resolving to an approved status (AUTO_APPROVED / KEEP_RDM
     / APPROVED / MANUALLY_ADDED) into unified
     (and into master, if they came from the primary source)
  5. Deletes from the error log ONLY the entries whose mapping is now approved
     (fully resolved). Still-pending rows are left untouched so they stay
     flagged for the steward and are re-evaluated on the next incremental run.

Schema-compatible with both normal_dedup ({entity}_unified) and golden_dedup
({entity}_unified_deduplicate). Driven by the is_single_source task value.
"""

import json
import uuid
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run ../utils/execute_utils

# COMMAND ----------

# MAGIC %run ../utils/rdm_resolver

# COMMAND ----------

setup_lakefusion_engine()

# COMMAND ----------

from lakefusion_core_engine.identifiers import generate_surrogate_key, generate_lakefusion_id
from lakefusion_core_engine.models import RecordStatus

# COMMAND ----------

# DBTITLE 1, Widgets

dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("is_single_source", "false", "Is Single Source (golden dedup)")

# COMMAND ----------

# DBTITLE 1, Read task values (override widgets)

entity = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "entity",
    debugValue=dbutils.widgets.get("entity"),
)
catalog_name = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "catalog_name",
    debugValue=dbutils.widgets.get("catalog_name"),
)
experiment_id = dbutils.widgets.get("experiment_id")
is_single_source = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "is_single_source",
    debugValue=dbutils.widgets.get("is_single_source"),
)

primary_table = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "primary_table", debugValue="",
)
primary_key = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "primary_key", debugValue="",
)
dataset_tables = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "dataset_tables", debugValue="[]",
)
dataset_objects = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "dataset_objects", debugValue="{}",
)
attributes_mapping_raw = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "attributes_mapping", debugValue="[]",
)
entity_attributes = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "entity_attributes", debugValue="[]",
)
entity_attributes_datatype = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "entity_attributes_datatype", debugValue="{}",
)
match_attributes = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "match_attributes", debugValue="[]",
)
rdm_configs = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "rdm_configs", debugValue="[]",
)
entity_attribute_records_raw = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "entity_attribute_records", debugValue="[]",
)

# COMMAND ----------

# DBTITLE 1, Parse JSON

dataset_tables = json.loads(dataset_tables) if isinstance(dataset_tables, str) else dataset_tables
dataset_objects = json.loads(dataset_objects) if isinstance(dataset_objects, str) else dataset_objects
attributes_mapping = json.loads(attributes_mapping_raw) if isinstance(attributes_mapping_raw, str) else attributes_mapping_raw
entity_attributes = json.loads(entity_attributes) if isinstance(entity_attributes, str) else entity_attributes
entity_attributes_datatype = json.loads(entity_attributes_datatype) if isinstance(entity_attributes_datatype, str) else entity_attributes_datatype
match_attributes = json.loads(match_attributes) if isinstance(match_attributes, str) else match_attributes
rdm_configs = json.loads(rdm_configs) if isinstance(rdm_configs, str) else (rdm_configs or [])
try:
    entity_attribute_records = json.loads(entity_attribute_records_raw) if entity_attribute_records_raw else []
except Exception:
    entity_attribute_records = []
_records_by_name = {
    r["name"]: r for r in entity_attribute_records
    if isinstance(r, dict) and r.get("name")
}

# Make utils importable for build_attributes_combined_column.
import os as _pp_os, sys as _pp_sys
_pp_parts = _pp_os.getcwd().split(_pp_os.sep)
for _i in range(len(_pp_parts) - 1, -1, -1):
    if _pp_parts[_i] == "src":
        _src_path = _pp_os.sep.join(_pp_parts[: _i + 1])
        if _src_path not in _pp_sys.path:
            _pp_sys.path.insert(0, _src_path)
        break

if isinstance(is_single_source, str):
    is_single_source = is_single_source.lower() == "true"
else:
    is_single_source = bool(is_single_source)

# COMMAND ----------

# DBTITLE 1, Derive table names

experiment_suffix = f"_{experiment_id}" if experiment_id else ""

if is_single_source:
    unified_table = f"{catalog_name}.silver.{entity}_unified_deduplicate{experiment_suffix}"
else:
    unified_table = f"{catalog_name}.silver.{entity}_unified{experiment_suffix}"

master_table = f"{catalog_name}.gold.{entity}_master{experiment_suffix}"


def _derive_error_table(unified_tbl):
    """Mirrors UnifiedErrorHandler's table-name derivation."""
    prefix, name = unified_tbl.rsplit(".", 1)
    last_us = name.rfind("_")
    if last_us > 0:
        err_name = name[:last_us] + "_error" + name[last_us:]
    else:
        err_name = name + "_error"
    return f"{prefix}.{err_name}"


error_table = _derive_error_table(unified_table)

logger.info("=" * 60)
logger.info("PROMOTE PENDING RDM")
logger.info("=" * 60)
logger.info(f"Entity:        {entity}")
logger.info(f"Unified table: {unified_table}")
logger.info(f"Master table:  {master_table}")
logger.info(f"Error table:   {error_table}")
logger.info(f"Single source: {is_single_source}")
logger.info("=" * 60)

# COMMAND ----------

# DBTITLE 1, Early exits

if not spark.catalog.tableExists(error_table):
    logger.info(f"Error table {error_table} does not exist — nothing to promote")
    dbutils.notebook.exit("no_error_table")

try:
    run_id = (
        dbutils.notebook.entry_point.getDbutils().notebook()
        .getContext().currentRunId().toString()
    )
except Exception:
    run_id = str(uuid.uuid4())

# COMMAND ----------

# DBTITLE 1, Read errored surrogate_keys at stage=RDM

errored_keys_df = (
    spark.table(error_table)
    .filter(F.col("error_stage") == "RDM")
    .select("surrogate_key")
    .distinct()
)
errored_count = errored_keys_df.count()
logger.info(f"Found {errored_count} errored surrogate_keys at stage=RDM")

if errored_count == 0:
    logger.info("Nothing to promote")
    dbutils.notebook.exit("nothing_to_promote")

# Cache for repeated joins
errored_keys_df = errored_keys_df

# COMMAND ----------

# DBTITLE 1, Helpers

# Map: source_table -> {source_attr_name: entity_attr_name}
mapping_by_source = {}
for entry in attributes_mapping:
    for src_tbl, attr_map in entry.items():
        mapping_by_source[src_tbl] = attr_map

generate_surrogate_key_udf = F.udf(
    lambda source_path, source_id: generate_surrogate_key(source_path, str(source_id)),
    StringType(),
)
generate_lakefusion_id_udf = F.udf(
    lambda source_path, source_id: generate_lakefusion_id(source_path, str(source_id)),
    StringType(),
)

# COMMAND ----------
# MAGIC %run ../utils/attributes_combined

# COMMAND ----------
def _add_attributes_combined(approved_df):
    """Build attributes_combined with complex-type awareness. STRUCT folds
    to space-joined sub-fields, ARRAY / ARRAY<STRUCT> JSON-serialize. Falls
    back to scalar string for resolver `__display` aliases."""
    available = [a for a in match_attributes if a in approved_df.columns]
    if not available:
        return approved_df.withColumn("attributes_combined", F.lit(""))

   

    effective_names = []
    for c in available:
        disp = f"{c}__display"
        effective_names.append(disp if disp in approved_df.columns else c)

    effective_records = []
    for original_attr, eff_name in zip(available, effective_names):
        if eff_name == original_attr:
            rec = _records_by_name.get(original_attr, {})
            if rec:
                rec = {**rec, "name": original_attr}
            effective_records.append(rec)
        else:
            effective_records.append({"name": eff_name})

    df = approved_df.withColumn(
        "attributes_combined",
        build_attributes_combined_column(approved_df, effective_names, effective_records),
    )
    for c in available:
        disp = f"{c}__display"
        if disp in df.columns:
            df = df.drop(disp)
    return df


def _project_to_table_schema(df, target_table):
    """Project df to match the target Delta table's schema, filling missing
    columns with NULL of the right type."""
    target_schema = spark.table(target_table).schema
    projected = []
    for f in target_schema.fields:
        if f.name in df.columns:
            projected.append(F.col(f.name).cast(f.dataType).alias(f.name))
        else:
            projected.append(F.lit(None).cast(f.dataType).alias(f.name))
    return df.select(*projected)


# COMMAND ----------

# DBTITLE 1, Re-resolve each source's errored rows

promoted_unified_dfs = []      # ready-to-append to unified
promoted_master_dfs = []       # ready-to-append to master (primary source only)
resolved_keys_dfs = []         # surrogate_keys whose mapping is now APPROVED
                               # (fully resolved) — the ONLY keys cleared from
                               # the error log

for source_table in dataset_tables:
    logger.info(f"\n--- Processing source: {source_table} ---")

    if not spark.catalog.tableExists(source_table):
        logger.warning(f"  Source table missing, skipping")
        continue

    src_attr_map = mapping_by_source.get(source_table, {})  # {src_attr: entity_attr}
    if not src_attr_map:
        logger.warning(f"  No attribute mapping for this source, skipping")
        continue

    is_primary_source = (source_table == primary_table)
    src_df = spark.read.table(source_table)

    # Project to entity-named columns
    select_exprs = []
    seen_cols = set()
    for source_attr, entity_attr in src_attr_map.items():
        if source_attr in src_df.columns and entity_attr not in seen_cols:
            select_exprs.append(F.col(source_attr).alias(entity_attr))
            seen_cols.add(entity_attr)

    # Keep primary_key around for source_id derivation if not already mapped
    if primary_key in src_df.columns and primary_key not in seen_cols:
        select_exprs.append(F.col(primary_key))
        seen_cols.add(primary_key)

    if not select_exprs:
        logger.warning(f"  Source has no usable columns for mapping, skipping")
        continue

    mapped_df = src_df.select(*select_exprs)

    # Add ingestion metadata + deterministic surrogate_key
    mapped_df = (
        mapped_df
        .withColumn("source_path", F.lit(source_table))
        .withColumn("source_id", F.col(primary_key).cast("string"))
        .withColumn(
            "surrogate_key",
            generate_surrogate_key_udf(F.col("source_path"), F.col("source_id")),
        )
    )

    if is_primary_source:
        mapped_df = mapped_df.withColumn(
            "lakefusion_id",
            generate_lakefusion_id_udf(F.col("source_path"), F.col("source_id")),
        )

    # Keep only the rows currently parked in the error log
    candidates_df = mapped_df.join(errored_keys_df, on="surrogate_key", how="inner")
    candidate_count = candidates_df.count()
    if candidate_count == 0:
        logger.info(f"  No errored rows from this source")
        continue
    logger.info(f"  Re-resolving {candidate_count} candidate rows")

    # Re-run RDM resolver against the (now-updated) mapping table.
    # Derive source_id the SAME way rdm_configs is keyed (by source_table) so
    # the resolver's `cfg["source_id"] == source_id` filter matches its configs.
    # dataset_objects[...]["id"] can differ from rdm_configs' source_id, making
    # the resolver a no-op — which would return ALL candidates as "approved"
    # (empty pending) and wrongly clear every RDM error entry.
    source_id_for_resolver = next(
        (cfg["source_id"] for cfg in rdm_configs if cfg.get("source_table") == source_table),
        None,
    )
    approved_df, still_pending_df = resolve_reference_attributes(
        spark, candidates_df, rdm_configs, source_id=source_id_for_resolver,
    )

    approved_count = 0 if approved_df is None else approved_df.count()
    pending_count = 0 if still_pending_df is None else still_pending_df.count()
    logger.info(f"  Approved on retry: {approved_count} | still pending: {pending_count}")

    if approved_count > 0:
        approved_df = _add_attributes_combined(approved_df)

        if is_primary_source:
            approved_df = (
                approved_df
                .withColumn("master_lakefusion_id", F.col("lakefusion_id"))
                .withColumn("record_status", F.lit(RecordStatus.MERGED.value))
                .withColumn("search_results", F.lit(""))
                .withColumn("scoring_results", F.lit(""))
            )
            promoted_master_dfs.append(approved_df)
        else:
            approved_df = (
                approved_df
                .withColumn("master_lakefusion_id", F.lit(""))
                .withColumn("record_status", F.lit(RecordStatus.ACTIVE.value))
                .withColumn("search_results", F.lit(""))
                .withColumn("scoring_results", F.lit(""))
            )

        promoted_unified_dfs.append(approved_df)

        # Clear from the error log ONLY rows that are now FULLY resolved.
        # Under the keep_null action, approved_df also retains rows whose ref
        # attr is still unresolved (kept in master, attr nulled) — those rows
        # also appear in still_pending_df and MUST stay flagged. Subtract them
        # so a still-pending record's error entry is never deleted.
        resolved_keys_df = approved_df.select("surrogate_key")
        if still_pending_df is not None and pending_count > 0:
            resolved_keys_df = resolved_keys_df.join(
                still_pending_df.select("surrogate_key"),
                on="surrogate_key",
                how="left_anti",
            )
        resolved_keys_dfs.append(resolved_keys_df)

# COMMAND ----------

# DBTITLE 1, Write promoted records to master + unified (MERGE-keyed)
# Using Delta MERGE with whenNotMatchedInsertAll guarantees idempotency: if
# this notebook re-runs, or a half-failed prior run already inserted some
# surrogate_keys, those rows are silently skipped on the next run instead of
# producing duplicates.

total_master_candidates = 0
total_master_inserted = 0
total_unified_candidates = 0
total_unified_inserted = 0


def _read_delta_metrics(table_name):
    """Read the most recent MERGE's numTargetRowsInserted from Delta history."""
    try:
        h = (
            DeltaTable.forName(spark, table_name)
            .history(1)
            .select("operationMetrics")
            .collect()
        )
        if h and h[0]["operationMetrics"]:
            return int(h[0]["operationMetrics"].get("numTargetRowsInserted", 0))
    except Exception:
        pass
    return 0


# ── Master writes (primary-source rows only) ──────────────────────────────
if promoted_master_dfs:
    master_delta = DeltaTable.forName(spark, master_table)
    for df in promoted_master_dfs:
        out = _project_to_table_schema(df, master_table)
        candidates = out.count()
        if candidates == 0:
            continue
        (
            master_delta.alias("tgt")
            .merge(out.alias("src"), "tgt.lakefusion_id = src.lakefusion_id")
            .whenNotMatchedInsertAll()
            .execute()
        )
        total_master_candidates += candidates
        total_master_inserted += _read_delta_metrics(master_table)

# ── Unified writes (primary + secondary) ──────────────────────────────────
if promoted_unified_dfs:
    unified_delta = DeltaTable.forName(spark, unified_table)
    for df in promoted_unified_dfs:
        out = _project_to_table_schema(df, unified_table)
        candidates = out.count()
        if candidates == 0:
            continue
        (
            unified_delta.alias("tgt")
            .merge(out.alias("src"), "tgt.surrogate_key = src.surrogate_key")
            .whenNotMatchedInsertAll()
            .execute()
        )
        total_unified_candidates += candidates
        total_unified_inserted += _read_delta_metrics(unified_table)

logger.info(
    f"\nUnified: {total_unified_inserted} inserted "
    f"({total_unified_candidates - total_unified_inserted} skipped — already present)"
)
logger.info(
    f"Master:  {total_master_inserted} inserted "
    f"({total_master_candidates - total_master_inserted} skipped — already present)"
)

# COMMAND ----------

# DBTITLE 1, Delete ONLY now-approved (fully-resolved) entries from the error log

if resolved_keys_dfs:
    resolved_keys_df = resolved_keys_dfs[0]
    for d in resolved_keys_dfs[1:]:
        resolved_keys_df = resolved_keys_df.unionByName(d)
    resolved_keys_df = resolved_keys_df.distinct()

    delta_error = DeltaTable.forName(spark, error_table)
    (
        delta_error.alias("e")
        .merge(
            resolved_keys_df.alias("p"),
            "e.surrogate_key = p.surrogate_key AND e.error_stage = 'RDM'",
        )
        .whenMatchedDelete()
        .execute()
    )
    logger.info(f"Cleared {resolved_keys_df.count()} now-approved entries from error log")
else:
    logger.info("No approved entries to clear from error log")

# COMMAND ----------

# DBTITLE 1, Still-pending rows are left in place
# Still-pending RDM rows are intentionally LEFT untouched in the error log:
# their entries were never deleted above, so they stay flagged for the steward
# and are re-evaluated on the next incremental run. (Previously they were
# deleted alongside the approved rows and then re-logged — which churned
# run_id/timestamp, created duplicate entries, and risked permanent data loss
# if the re-log failed after the delete had already committed.)

# COMMAND ----------

logger.info("=" * 60)
logger.info("PROMOTE PENDING RDM — DONE")
logger.info("=" * 60)
logger_instance.shutdown()
