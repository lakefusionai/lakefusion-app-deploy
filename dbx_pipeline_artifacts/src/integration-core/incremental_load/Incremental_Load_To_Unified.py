# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental Load To Unified
# MAGIC
# MAGIC Single-notebook redesign of the incremental load process. Replaces the
# MAGIC Validate / Inserts / Updates / Deletes notebook chain with one notebook that
# MAGIC performs:
# MAGIC
# MAGIC 1. Per-source CDF read (once each), validation, and transform to unified schema.
# MAGIC 2. CDF collapse to last-change-wins so multi-step transactions to a single
# MAGIC    record (insert+update, delete+insert, etc.) are handled correctly.
# MAGIC 3. A single MERGE into the unified table covering inserts, updates, and
# MAGIC    deletes.
# MAGIC 4. A targeted read of the unified table's own CDF at the merge version to
# MAGIC    identify which masters need survivorship recalculated.
# MAGIC 5. Survivorship for every changed MERGED contributor (no per-attribute
# MAGIC    shortcut) and a batched MERGE into the master table.
# MAGIC 6. Audit table writes (`merge_activities`, `attribute_version_sources`)
# MAGIC    using `whenNotMatchedInsertAll` for idempotency on retry.
# MAGIC 7. A single advance of `last_processed_version` per source — written last
# MAGIC    so a mid-pipeline failure leaves the version pointers untouched and the
# MAGIC    next run re-processes the same window safely.
# MAGIC
# MAGIC See `lakefusion-universe/incremental_redesign.md` for the design rationale.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 1: Setup

# COMMAND ----------

import json
import re
from collections import defaultdict
from datetime import datetime
from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

# MAGIC %run ../../utils/spark_types

# COMMAND ----------

# MAGIC %run ../../utils/complex_type_mapping

# COMMAND ----------

# MAGIC %run ../../utils/rdm_resolver

# COMMAND ----------

# MAGIC %run ../../utils/attributes_combined

# COMMAND ----------

# NOTE: attributes_combined MUST be %run AFTER rdm_resolver. Both define a
# build_attributes_combined_column (different signatures), and the later %run
# wins in the shared namespace. add_attributes_combined (from attributes_combined)
# calls its own build_attributes_combined_column with selected_sub_fields_by_attr,
# which rdm_resolver's version doesn't accept — so attributes_combined must load
# last. We only need resolve_reference_attributes from rdm_resolver, which does
# not use build_attributes_combined_column.

setup_lakefusion_engine()

# COMMAND ----------

from lakefusion_core_engine.survivorship import SurvivorshipEngine
from lakefusion_core_engine.services.unified_error_handler import UnifiedErrorHandler

# COMMAND ----------

# Parameters arrive as Databricks task values set by Parse_Entity_Model_JSON.
entity              = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity")
primary_key         = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "primary_key")
entity_attributes   = json.loads(dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes"))
entity_attrs_dtype  = json.loads(dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes_datatype"))
attributes_mapping  = json.loads(dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "attributes_mapping"))
match_attributes    = json.loads(dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "match_attributes"))
dataset_tables      = json.loads(dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "dataset_tables"))
dataset_objects     = json.loads(dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "dataset_objects"))
survivorship_config = json.loads(
    dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "default_survivorship_rules")
)
catalog_name        = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "catalog_name")
experiment_id       = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "experiment_id", default="")

# complex-type metadata. Default to empty / legacy when not set.
try:
    entity_attribute_records = json.loads(
        dbutils.jobs.taskValues.get(
            "Parse_Entity_Model_JSON", "entity_attribute_records", default="[]"
        )
        or "[]"
    )
except Exception:
    entity_attribute_records = []

try:
    attributes_mapping_full = json.loads(
        dbutils.jobs.taskValues.get(
            "Parse_Entity_Model_JSON", "attributes_mapping_full", default="[]"
        )
        or "[]"
    )
except Exception:
    attributes_mapping_full = []

# REFERENCE_ENTITY config — resolve ref_lakefusion_id -> display value into
# attributes_combined (raw {attr} stays a ref id). ref_lookup feeds the Python
# master builder; the Spark unified builder joins the ref table directly.
try:
    reference_attribute_config = json.loads(
        dbutils.jobs.taskValues.get(
            "Parse_Entity_Model_JSON", "reference_attribute_config", default="{}"
        )
        or "{}"
    )
except Exception:
    reference_attribute_config = {}

# RDM configs drive the source-value -> ref_lakefusion_id resolution + the
# mapping-table MERGE that queues PENDING / NO_MATCH rows for steward review.
# Empty list → resolve_reference_attributes short-circuits (no ref attrs).
# Parse_Entity_Model_JSON sets rdm_configs as a raw list (not json.dumps'd), so
# taskValues.get returns a list — json.loads() on it raises and would silently
# leave rdm_configs=[], making the resolver a no-op for EVERY source. Guard on
# isinstance(str) like Load_Primary_Source / Load_Secondary_Sources do.
try:
    rdm_configs = dbutils.jobs.taskValues.get(
        "Parse_Entity_Model_JSON", "rdm_configs", default="[]"
    )
    rdm_configs = json.loads(rdm_configs) if isinstance(rdm_configs, str) else (rdm_configs or [])
except Exception:
    rdm_configs = []

ref_lookup = {}
for _attr_name, _ref_cfg in (reference_attribute_config or {}).items():
    _ref_table = (_ref_cfg or {}).get("ref_table")
    _output_attr = (_ref_cfg or {}).get("output_attr")
    if not _ref_table or not _output_attr:
        continue
    try:
        _rdf = spark.read.table(_ref_table)
        if "is_current" in _rdf.columns:
            _rdf = _rdf.filter(F.col("is_current") == F.lit(True))
        _rows = _rdf.select(
            F.col("ref_lakefusion_id").cast("string").alias("_id"),
            F.col(_output_attr).cast("string").alias("_disp"),
        ).collect()
        ref_lookup[_attr_name] = {
            r["_id"]: r["_disp"] for r in _rows if r["_id"] is not None
        }
    except Exception:
        continue

_attr_records_by_name = {
    r["name"]: r for r in entity_attribute_records if isinstance(r, dict) and r.get("name")
}
_has_complex_attrs = any(
    (rec.get("is_array") or (rec.get("type") or "").strip().upper() == "STRUCT")
    for rec in entity_attribute_records
)


def _is_complex_attr(name: str) -> bool:
    rec = _attr_records_by_name.get(name)
    if not rec:
        return False
    return bool(rec.get("is_array")) or (rec.get("type") or "").strip().upper() == "STRUCT"


def _parse_engine_complex_value(value):
    """Parse a survivorship-engine complex value back to native dict / list.

    SurvivorshipEngine.apply_survivorship runs every value through safe_str(),
    which JSON-serializes STRUCT / ARRAY values — so the golden record holds a
    JSON string for each complex attribute, not a native dict / list. Parse it
    back so (a) createDataFrame against the typed master schema accepts it as a
    nested column and (b) attributes_combined_from_dict flattens field VALUES
    instead of dumping the raw JSON. This is the driver-side equivalent of the
    from_json(value, complex_ddl) round-trip the dedup / merge notebooks do via
    merged_record_column. Already-native values and unparseable / empty strings
    pass through (-> as-is / None)."""
    if value is None or isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            return json.loads(s)
        except (ValueError, TypeError):
            return None
    return value


# Bootstrap sys.path so the shared `utils.*` modules are importable both in
# Databricks repos and in local dev (no wheel install needed).
import os as _pp_os
import sys as _pp_sys
_pp_parts = _pp_os.getcwd().split(_pp_os.sep)
for _i in range(len(_pp_parts) - 1, -1, -1):
    if _pp_parts[_i] == "src":
        _pp_src_path = _pp_os.sep.join(_pp_parts[: _i + 1])
        if _pp_src_path not in _pp_sys.path:
            _pp_sys.path.insert(0, _pp_src_path)
        break

if experiment_id:
    experiment_id = experiment_id.replace("-", "")

suffix                     = f"_{experiment_id}" if experiment_id else "_prod"
unified_table              = f"{catalog_name}.silver.{entity}_unified{suffix}"
master_table               = f"{catalog_name}.gold.{entity}_master{suffix}"
meta_info_table            = f"{catalog_name}.silver.table_meta_info"
merge_activities_table     = f"{master_table}_merge_activities"
attr_version_sources_table = f"{master_table}_attribute_version_sources"

# run_id stamped on rows routed to the unified RDM error table.
try:
    run_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().toString()
except Exception:
    run_id = None

logger.info("=" * 80)
logger.info(f"Incremental Load To Unified — entity={entity}")
logger.info(f"  unified_table = {unified_table}")
logger.info(f"  master_table  = {master_table}")
logger.info(f"  source tables = {dataset_tables}")
logger.info("=" * 80)

# COMMAND ----------

# Survivorship rules from Parse_Entity_Model_JSON contain dataset IDs in
# strategyRule; the SurvivorshipEngine needs source-path strings. Translate
# only when dataset_objects has the mapping data — if rules already hold
# source paths, this is a no-op.
if survivorship_config and dataset_objects:
    dataset_id_to_path = {
        info["id"]: path
        for path, info in dataset_objects.items()
        if isinstance(info, dict) and info.get("id") is not None
    }
    translated = []
    for rule in survivorship_config:
        rule = dict(rule)
        strategy_rule = rule.get("strategyRule")
        if isinstance(strategy_rule, list) and strategy_rule:
            rule["strategyRule"] = [
                dataset_id_to_path.get(sr, sr) for sr in strategy_rule
            ]
        translated.append(rule)
    survivorship_config = translated

# COMMAND ----------

def get_mapping_for_table(source_table):
    """Return {source_col: entity_attr} for a given source table.

    Parse_Entity_Model_JSON delivers entries as {entity_attr: source_col}; we
    invert here so transform code can rename source columns to entity attrs."""
    for entry in attributes_mapping:
        if source_table in entry:
            return {v: k for k, v in entry[source_table].items()}
    return {}


# scalar type resolution now lives in utils.spark_types
# (consolidated across all notebooks). Kept this comment so future readers
# don't add another copy of the dict.


def _write_version_metadata(version_map):
    """Advance last_processed_version for every source table that contributed
    successfully to this run. Written as the final step so an exception
    earlier in the notebook leaves pointers untouched and the next run safely
    re-processes the same window."""
    if not version_map:
        return
    now = datetime.now()
    meta_rows = [
        {
            "table_name": tbl,
            "last_processed_version": ver,
            "last_processed_timestamp": now,
            "entity_name": entity}
        for tbl, ver in version_map.items()
    ]
    df_meta = spark.createDataFrame(meta_rows)
    (
        DeltaTable.forName(spark, meta_info_table).alias("target")
        .merge(df_meta.alias("source"), "(target.table_name = source.table_name) AND (target.entity_name = source.entity_name)")
        .whenMatchedUpdate(set={
            "last_processed_version": "source.last_processed_version",
            "last_processed_timestamp":             "source.last_processed_timestamp",
        })
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 2: Load, Validate, and Transform Each Source CDF
# MAGIC
# MAGIC Per-table failures are non-fatal: a table that fails validation is skipped
# MAGIC and its `last_processed_version` is **not** advanced, so the next run will
# MAGIC see the same versions and can retry once the upstream issue is resolved.

# COMMAND ----------

source_dfs    = []
version_map   = {}   # {source_table: current_version} — only successful tables
failed_tables = []   # diagnostics; not used to short-circuit the run

for source_table in dataset_tables:
    logger.info("-" * 80)
    logger.info(f"Source table: {source_table}")

    # ------------------------------------------------------------------
    # 2.1 Version bounds
    # ------------------------------------------------------------------
    current_version = (
        spark.sql(f"DESCRIBE HISTORY {source_table} LIMIT 1")
        .first()["version"]
    )

    meta_row = (
        spark.read.table(meta_info_table)
        .filter(F.col("table_name") == source_table)
        .select("last_processed_version")
        .first()
    )
    last_processed_version = meta_row["last_processed_version"] if meta_row else -1
    starting_version = last_processed_version + 1

    if starting_version > current_version:
        logger.info(f"  SKIP: no new versions since last run "
                    f"(last_processed={last_processed_version}, current={current_version})")
        continue

    # ------------------------------------------------------------------
    # 2.2 Read CDF
    # ------------------------------------------------------------------
    try:
        df_cdf = (
            spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", starting_version)
            .option("endingVersion", current_version)
            .table(source_table)
        )
    except Exception as e:
        logger.warning(f"  SKIP: CDF unreadable — {e}")
        failed_tables.append(source_table)
        continue

    # ------------------------------------------------------------------
    # 2.3 Validate
    # ------------------------------------------------------------------
    col_mapping = get_mapping_for_table(source_table)
    if not col_mapping:
        logger.warning(f"  SKIP: no attribute mapping found")
        continue

    missing_cols = set(col_mapping.keys()) - set(df_cdf.columns)
    if missing_cols:
        logger.warning(f"  SKIP: schema drift — missing source columns {missing_cols}")
        failed_tables.append(source_table)
        continue

    earliest_available = (
        spark.sql(f"DESCRIBE HISTORY {source_table}")
        .agg(F.min("version").alias("v"))
        .first()["v"]
    )
    if starting_version < earliest_available:
        logger.warning(
            f"  FAIL: version gap — need v{starting_version}, "
            f"earliest available v{earliest_available}"
        )
        failed_tables.append(source_table)
        continue

    source_pk_col = next(
        (src for src, ent in col_mapping.items() if ent == primary_key), None
    )
    if source_pk_col is None:
        logger.warning(f"  SKIP: primary key '{primary_key}' not present in mapping")
        continue

    # PK null + uniqueness check on the *source* table (not unified, not CDF).
    # One Spark job, one column scanned, both checks via a single groupBy.
    pk_quality = (
        spark.read.table(source_table)
        .select(F.col(source_pk_col).alias("pk"))
        .groupBy("pk")
        .agg(F.count("*").alias("cnt"))
        .agg(
            F.sum(F.when(F.col("pk").isNull(), F.col("cnt")).otherwise(0)).alias("null_count"),
            F.sum(F.when(F.col("cnt") > 1, F.col("cnt") - 1).otherwise(0)).alias("dupe_count"),
        )
        .first()
    )
    # SUM over an empty input returns NULL, not 0 — coalesce so an empty source
    # table (e.g. a CDC window that deleted every row) is treated as "no
    # nulls, no dupes" instead of crashing the comparison.
    null_count = pk_quality["null_count"] or 0
    dupe_count = pk_quality["dupe_count"] or 0
    if null_count > 0:
        logger.warning(f"  FAIL: {null_count} null PKs in source table")
        failed_tables.append(source_table)
        continue
    if dupe_count > 0:
        logger.warning(f"  FAIL: {dupe_count} duplicate PKs in source table")
        failed_tables.append(source_table)
        continue

    # ------------------------------------------------------------------
    # 2.4 Filter preimages and collapse to last-change-wins
    # ------------------------------------------------------------------
    # update_preimage rows are never needed. After filtering, partition by
    # surrogate_key and keep only the highest-version row — that's the
    # record's final state across the entire CDF window. This is what fixes
    # the multi-step transaction bug: delete+insert on a previously-MERGED
    # record collapses to a single insert that then hits whenMatchedUpdate.
    surrogate_key_expr = F.concat(
        F.lit("sk_"),
        F.md5(F.concat_ws("_", F.lit(source_table), F.col(source_pk_col).cast("string"))),
    )
    window = (
        Window.partitionBy("surrogate_key")
        .orderBy(F.desc("_commit_version"), F.desc("_commit_timestamp"))
    )
    df_collapsed = (
        df_cdf
        .filter(F.col("_change_type") != "update_preimage")
        .withColumn("surrogate_key", surrogate_key_expr)
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # ------------------------------------------------------------------
    # 2.5 Transform to unified schema
    # ------------------------------------------------------------------
    # Blocker B — complex-aware source projection. When the
    # entity has any STRUCT / ARRAY attributes AND we have the full mapping
    # records, delegate to project_source_to_target so each target column
    # resolves to the correct nested Spark type (and runtime struct schema
    # mismatches raise StructSchemaMismatchError). Otherwise fall back to
    # the legacy flat rename so scalar-only entities behave exactly as
    # before.
    _table_full_records = None
    for _entry in attributes_mapping_full:
        if source_table in _entry:
            _table_full_records = _entry[source_table]
            break

    if _has_complex_attrs and _table_full_records:
        df_projected = project_source_to_target(
            df_collapsed, _table_full_records, entity_attribute_records
        )
        # Re-attach the CDF / surrogate columns the projector dropped.
        df_renamed = df_projected \
            .withColumn("surrogate_key", df_collapsed["surrogate_key"]) \
            .withColumn("_change_type", df_collapsed["_change_type"]) \
            .withColumn(
                "source_id",
                df_collapsed[source_pk_col].cast("string"),
            )
    else:
        rename_exprs = [
            F.col(src).alias(ent)
            for src, ent in col_mapping.items()
            if src in df_collapsed.columns
        ]
        keep_exprs = [
            F.col("surrogate_key"),
            F.col("_change_type"),
            F.col(source_pk_col).cast("string").alias("source_id"),
        ]
        df_renamed = df_collapsed.select(*rename_exprs, *keep_exprs)

    # Blocker A: fill missing attributes with typed nulls (centralized util
    # — supports STRUCT and ARRAY columns, not just scalars).
    for attr in entity_attributes:
        if attr == "lakefusion_id":
            continue
        if attr not in df_renamed.columns:
            rec = _attr_records_by_name.get(attr)
            if rec and (rec.get("is_array") or (rec.get("type") or "").strip().upper() == "STRUCT"):
                spark_dtype = get_complex_spark_data_type(rec)
            else:
                spark_dtype = get_spark_data_type(entity_attrs_dtype.get(attr, "string"))
            df_renamed = df_renamed.withColumn(attr, F.lit(None).cast(spark_dtype))

    # REFERENCE_ENTITY {attr} columns must be STRING — they hold ref_lakefusion_id
    # after resolution. When the source column is numeric (e.g. a code stored as
    # 1.0 / 0), it arrives as DOUBLE and stays DOUBLE on any row the resolver
    # doesn't rewrite (deletes routed to passthrough, or a source that skips the
    # resolve block). Casting up front makes the type consistent so neither the
    # resolved/passthrough union, the cross-source unionByName, nor the MERGE
    # collides a string ref id against a DOUBLE slot (CAST_INVALID_INPUT).
    for _ref_attr in reference_attribute_config:
        if _ref_attr in df_renamed.columns:
            df_renamed = df_renamed.withColumn(_ref_attr, F.col(_ref_attr).cast("string"))

    # Blocker F (unified side): complex-aware attributes_combined. Scalar-only
    # entities keep the legacy concat_ws output byte-for-byte.
    available_match_attrs = [a for a in match_attributes if a in df_renamed.columns]
    if available_match_attrs:
        # Bake source value -> ref_lakefusion_id into REFERENCE_ENTITY {attr}
        # columns (mapping-table resolution) and route unresolved rows to the
        # RDM error table. Without this the raw source value stays in {attr}
        # (never a ref id) and nothing is parked for Promote_Pending_RDM to drain
        # after a mapping is approved. Mirrors Increment_Inserts. source_id is
        # read from rdm_configs by source_table so the resolver's
        # cfg["source_id"] == source_id filter always matches its configs.
        # Only insert/update rows carry data to resolve; deletes just flip
        # record_status in the MERGE, so leave them untouched.
        src_id_for_resolver = next(
            (cfg["source_id"] for cfg in rdm_configs if cfg.get("source_table") == source_table),
            None,
        )
        # TEMP RDM diagnostic — confirm the resolver sees configs (matched_configs>0)
        # and the ref attr names line up with df columns. Remove once verified.
        _cfg_for_src = [c for c in rdm_configs if c.get("source_id") == src_id_for_resolver]
        logger.info(
            f"[RDM-DIAG] source_table={source_table!r} "
            f"src_id_for_resolver={src_id_for_resolver!r} "
            f"matched_configs={len(_cfg_for_src)} "
            f"all_cfg_source_tables={sorted({c.get('source_table') for c in rdm_configs})} "
            f"ref_attr_names={sorted({c.get('attribute_name') for c in rdm_configs})} "
            f"df_renamed_cols={df_renamed.columns}"
        )
        _is_ins_upd  = F.col("_change_type").isin("insert", "update_postimage")
        to_resolve   = df_renamed.filter(_is_ins_upd)
        passthrough  = df_renamed.filter(~_is_ins_upd)
        resolved_df, pending_df = resolve_reference_attributes(
            spark, to_resolve, rdm_configs, source_id=src_id_for_resolver,
        )
        # Keep the resolver's <attr>__display columns through the union — they
        # carry the canonical display value computed in the SAME join that baked
        # the ref id, so they can't go stale. passthrough (deletes) has no
        # __display; allowMissingColumns fills it null (a delete's
        # attributes_combined is irrelevant — the MERGE only flips record_status).
        df_renamed = resolved_df.unionByName(passthrough, allowMissingColumns=True)

        # Park unresolved (PENDING / NO_MATCH) rows in the RDM error table so a
        # steward can fix the mapping and Promote_Pending_RDM can reprocess them.
        if pending_df is not None and not pending_df.isEmpty():
            UnifiedErrorHandler(spark, unified_table).log_errors(
                pending_df.select(
                    F.col("surrogate_key"),
                    F.col("_rdm_pending_reason").alias("error_message"),
                ),
                stage="RDM",
                run_id=run_id,
            )

        # Build attributes_combined from the resolver's <attr>__display values —
        # the display resolved during baking. Do NOT re-resolve via
        # add_attributes_combined / resolve_reference_display_columns: that second
        # ref-table join coalesces to the RAW ref id whenever it misses, leaking
        # ref ids into the text vector search embeds and the LLM reads. Using
        # __display mirrors the working Load_Primary_Source path. The raw {attr}
        # columns keep their ref ids untouched.
        _ref_display_cols = {
            a: f"{a}__display"
            for a in available_match_attrs
            if f"{a}__display" in df_renamed.columns
        }
        df_renamed = df_renamed.withColumn(
            "attributes_combined",
            build_attributes_combined_column(
                df_renamed, available_match_attrs, entity_attribute_records,
                ref_display_cols=_ref_display_cols,
            ),
        )
        # Drop the resolver's __display helper columns now attributes_combined is built.
        for _a in available_match_attrs:
            _dc = f"{_a}__display"
            if _dc in df_renamed.columns:
                df_renamed = df_renamed.drop(_dc)
    else:
        df_renamed = df_renamed.withColumn("attributes_combined", F.lit(""))

    df_renamed = (
        df_renamed
        .withColumn("source_path",     F.lit(source_table))
        .withColumn("search_results",  F.lit(""))
        .withColumn("scoring_results", F.lit(""))
        # record_status and master_lakefusion_id are set by the MERGE, not here
    )

    source_dfs.append(df_renamed)
    version_map[source_table] = current_version
    logger.info(f"  OK: queued for merge (CDF v{starting_version}-{current_version})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 3: Union and MERGE into Unified

# COMMAND ----------

if not source_dfs:
    logger.info("No source tables produced data — exiting cleanly")
    dbutils.jobs.taskValues.set(key="status", value="NO_DATA")
    dbutils.notebook.exit("no_data")

df_all_changes = reduce(
    lambda a, b: a.unionByName(b, allowMissingColumns=True),
    source_dfs,
)

# Capture the unified table version *before* the merge so Phase 4 can read
# exactly the slice this merge produces.
pre_merge_version = (
    spark.sql(f"DESCRIBE HISTORY {unified_table} LIMIT 1")
    .first()["version"]
)

# COMMAND ----------

# Columns the merge writes from source data. record_status and
# master_lakefusion_id are deliberately absent from the update set: their
# existing values must be preserved on matched rows so a previously-MERGED
# record stays MERGED across an update or a delete+insert collapse.
data_cols = (
    [a for a in entity_attributes if a != "lakefusion_id"]
    + ["attributes_combined", "source_path", "source_id"]
)

update_set = {c: f"source.{c}" for c in data_cols}
update_set["search_results"]  = "''"
update_set["scoring_results"] = "''"

insert_values = {
    **update_set,
    "surrogate_key":        "source.surrogate_key",
    "record_status":        "'ACTIVE'",
    "master_lakefusion_id": "''",
}

(
    DeltaTable.forName(spark, unified_table).alias("target")
    .merge(df_all_changes.alias("source"), "target.surrogate_key = source.surrogate_key")
    .whenMatchedUpdate(
        condition="source._change_type = 'delete'",
        set={"record_status": "'DELETED'"},
    )
    .whenMatchedUpdate(
        condition="source._change_type IN ('insert', 'update_postimage')",
        set=update_set,
    )
    .whenNotMatchedInsert(
        condition="source._change_type IN ('insert', 'update_postimage')",
        values=insert_values,
    )
    # Unmatched deletes (source deleted a record not in unified) → silent no-op.
    .execute()
)

logger.info(f"MERGE complete on {unified_table} (pre-merge version v{pre_merge_version})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 4: Read Unified CDF and Identify Affected Masters
# MAGIC
# MAGIC Bounded read: only the slice written by this merge. Survivorship is needed
# MAGIC for any MERGED unified record that changed (update_postimage) or was
# MAGIC soft-deleted (update_postimage with record_status=DELETED) — net-new ACTIVE
# MAGIC inserts are intentionally excluded because they have no master yet.

# COMMAND ----------

merge_version = (
    spark.sql(f"DESCRIBE HISTORY {unified_table} LIMIT 1")
    .first()["version"]
)

if merge_version == pre_merge_version:
    # Delta wrote no new version — MERGE was a genuine no-op. Still advance
    # version pointers so we don't re-read the same source CDF window forever.
    logger.info("MERGE produced no Delta version — exiting after advancing version metadata")
    _write_version_metadata(version_map)
    dbutils.jobs.taskValues.set(key="status", value="MERGE_NOOP")
    dbutils.notebook.exit("merge_noop")

df_unified_cdf = (
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", merge_version)
    .option("endingVersion",   merge_version)
    .table(unified_table)
    .filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
)

df_affected_masters = (
    df_unified_cdf
    .filter(
        F.col("_change_type").isin("update_postimage", "delete")
        & (
            (F.col("record_status") == "MERGED")
            | (F.col("record_status") == "DELETED")
        )
        & F.col("master_lakefusion_id").isNotNull()
        & (F.col("master_lakefusion_id") != "")
    )
    .select("master_lakefusion_id")
    .distinct()
)

# First bounded .collect() — one row per affected master, not per record.
affected_master_ids = [r["master_lakefusion_id"] for r in df_affected_masters.collect()]
logger.info(f"Affected masters: {len(affected_master_ids)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 5: Survivorship and Master Table Updates

# COMMAND ----------

master_updates       = []
merge_activity_rows  = []
attr_version_rows    = []
masters_to_delete    = []

if affected_master_ids:
    # Single targeted read covering all affected masters in one Spark job.
    df_contributors = (
        spark.read.table(unified_table)
        .filter(
            F.col("master_lakefusion_id").isin(affected_master_ids)
            & (F.col("record_status") == "MERGED")
        )
    )

    # Second bounded .collect() — only contributor rows for affected masters.
    #asDict(recursive=True) so nested Row objects from
    # STRUCT / ARRAY<STRUCT> columns deserialize to native dict/list. The
    # SurvivorshipEngine consumes Python primitives; leaving them as Row
    # would break every per-attribute strategy.
    contributor_rows = df_contributors.collect()
    contributors_by_master = defaultdict(list)
    for row in contributor_rows:
        contributors_by_master[row["master_lakefusion_id"]].append(row.asDict(recursive=True))

    engine = SurvivorshipEngine(
        survivorship_config=survivorship_config,
        entity_attributes=entity_attributes,
        entity_attributes_datatype=entity_attrs_dtype,
        id_key="surrogate_key",
    )

    # Some affected masters may now have zero MERGED contributors — that means
    # every contributor was deleted in this run. They are queued for removal.
    for master_id in affected_master_ids:
        contributors = contributors_by_master.get(master_id, [])
        if not contributors:
            masters_to_delete.append(master_id)
            continue

        result       = engine.apply_survivorship(unified_records=contributors)
        golden       = result.resultant_record
        attr_mapping = result.resultant_master_attribute_source_mapping

        # The engine JSON-serializes STRUCT / ARRAY values via safe_str(), so
        # parse complex attrs back to native dict / list first. Scalars stay as
        # strings (master scalar columns are StringType).
        golden_native = dict(golden)
        for attr in entity_attributes:
            if attr != "lakefusion_id" and _is_complex_attr(attr):
                golden_native[attr] = _parse_engine_complex_value(golden.get(attr))

        #only str()-cast scalar values. STRUCT (dict)
        # and ARRAY (list) values must stay native so the typed master schema
        # (Blocker E) can write them as nested Spark columns. str() on a dict
        # produces Python repr — silently destroying type info.
        master_record = {"lakefusion_id": master_id}
        for attr in entity_attributes:
            if attr == "lakefusion_id":
                continue
            value = golden_native.get(attr)
            if value is None:
                master_record[attr] = None
            elif _is_complex_attr(attr) or isinstance(value, (dict, list)):
                master_record[attr] = value
            else:
                master_record[attr] = str(value)

        #complex-aware attributes_combined post-
        # survivorship. Survivorship runs in Python so we use the Python
        # variant (attributes_combined_from_dict) — same semantics as the
        # Spark builder used on unified rows.
        # Resolve REFERENCE_ENTITY attrs (ref_lakefusion_id -> display) into the
        # master's attributes_combined via ref_lookup; raw values stay ref ids.
        if _has_complex_attrs or ref_lookup:
            master_record["attributes_combined"] = attributes_combined_from_dict(
                golden_native, match_attributes, entity_attribute_records,
                ref_lookup=ref_lookup or None,
            )
        else:
            golden_match_attrs = [
                a for a in match_attributes if a in golden and golden.get(a) is not None
            ]
            if golden_match_attrs:
                master_record["attributes_combined"] = " | ".join(
                    re.sub(r"\s+", " ", str(golden.get(a, "") or "").strip())
                    for a in match_attributes
                    if a in golden
                )
            else:
                master_record["attributes_combined"] = ""
        master_updates.append(master_record)

        #audit table's attribute_value is StringType.
        # JSON-encode complex (dict / list) values so structure is preserved
        # losslessly while keeping the schema unchanged. Scalar values still
        # go in as plain str() for back-compat.
        def _audit_value(v):
            if v is None:
                return None
            if isinstance(v, (dict, list)):
                try:
                    return json.dumps(v, default=str)
                except Exception:
                    return str(v)
            return str(v)

        attr_version_rows.append({
            "lakefusion_id": master_id,
            "version":       None,   # filled in after master write
            "attribute_source_mapping": [
                {
                    "attribute_name":  m.attribute_name,
                    "attribute_value": _audit_value(m.attribute_value),
                    "source":          m.source,
                }
                for m in attr_mapping
            ],
        })

        unique_sources = ",".join(sorted({c["source_path"] for c in contributors if c.get("source_path")}))
        for contributor in contributors:
            merge_activity_rows.append({
                "master_id":   master_id,
                "match_id":    contributor["surrogate_key"],
                "source":      unique_sources,
                "action_type": "SOURCE_UPDATE",
                "version":     None,   # filled in after master write
            })

logger.info(
    f"Survivorship: {len(master_updates)} master(s) to update, "
    f"{len(masters_to_delete)} to delete"
)

# COMMAND ----------

master_version = None

if master_updates:
    #build the master schema via create_schema_fields
    # so STRUCT / ARRAY columns get their real nested Spark types. Previously
    # every entity attribute was forced to StringType, which silently
    # coerced (and broke) complex values written from Blocker D's native
    # dict / list output.
    _attr_fields = create_schema_fields(
        entity_attributes,
        entity_attrs_dtype if isinstance(entity_attrs_dtype, dict) else {},
        include_lakefusion_id=False,
        attributes=entity_attribute_records or None,
    )
    # Survivorship returns scalar values as strings (safe_str); complex attrs
    # were parsed back to native dict/list above. createDataFrame against a typed
    # schema rejects a string in a numeric column (ArrowInvalid: '500500' ->
    # int64). Build the frame with scalar columns as StringType (matching the
    # str() values) and complex columns at their real nested type, then cast each
    # scalar to its declared type — cast() coerces '500500' -> 500500 etc. and
    # matches the typed master table for the MERGE below. REFERENCE_ENTITY attrs
    # are StringType, so their ref ids cast string->string (unchanged).
    _scalar_cast_targets = []
    _creation_fields = []
    for _f in _attr_fields:
        if isinstance(_f.dataType, (StructType, ArrayType)):
            _creation_fields.append(_f)
        else:
            _creation_fields.append(StructField(_f.name, StringType(), _f.nullable))
            _scalar_cast_targets.append((_f.name, _f.dataType))
    master_schema = StructType(
        [StructField("lakefusion_id", StringType(), False)]
        + _creation_fields
        + [StructField("attributes_combined", StringType(), True)]
    )
    df_master_updates = spark.createDataFrame(master_updates, schema=master_schema)
    for _name, _dtype in _scalar_cast_targets:
        df_master_updates = df_master_updates.withColumn(_name, F.col(_name).cast(_dtype))
    update_cols = [a for a in entity_attributes if a != "lakefusion_id"] + ["attributes_combined"]

    (
        DeltaTable.forName(spark, master_table).alias("target")
        .merge(df_master_updates.alias("source"), "target.lakefusion_id = source.lakefusion_id")
        .whenMatchedUpdate(set={c: f"source.{c}" for c in update_cols})
        .execute()
    )
    master_version = (
        spark.sql(f"DESCRIBE HISTORY {master_table} LIMIT 1")
        .first()["version"]
    )

if masters_to_delete:
    ids_quoted = ", ".join(f"'{mid}'" for mid in masters_to_delete)
    DeltaTable.forName(spark, master_table).delete(
        f"lakefusion_id IN ({ids_quoted})"
    )
    if master_version is None:
        master_version = (
            spark.sql(f"DESCRIBE HISTORY {master_table} LIMIT 1")
            .first()["version"]
        )

# Backfill the master version into audit rows now that we know it.
for row in merge_activity_rows:
    row["version"] = master_version
for row in attr_version_rows:
    row["version"] = master_version

# COMMAND ----------

# Clear search_results / scoring_results for ACTIVE unified records that
# reference any master that just changed or was deleted. These records may
# have stale candidate lists and need to be re-matched on the next run.
#
# This regex_like approach scales linearly with the number of affected
# masters in the alternation pattern — fine for typical incremental loads.
# For very large backfill-style runs the alternative join-based path
# discussed in incremental_redesign.md would be preferable, but it always
# requires a full read of every ACTIVE record's search_results column. The
# real fix is the search_candidate_ids array<string> schema change.
all_changed_master_ids = (
    [r["lakefusion_id"] for r in master_updates] + masters_to_delete
)
if all_changed_master_ids:
    pattern = "|".join(re.escape(mid) for mid in all_changed_master_ids)
    DeltaTable.forName(spark, unified_table).update(
        condition=f"record_status = 'ACTIVE' AND regexp_like(search_results, '{pattern}')",
        set={"search_results": "''", "scoring_results": "''"},
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 6: Write Audit Tables
# MAGIC
# MAGIC Both writes use `whenNotMatchedInsertAll` on a composite key — making them
# MAGIC idempotent on retry.

# COMMAND ----------

if merge_activity_rows:
    now = datetime.now()
    for row in merge_activity_rows:
        row["created_at"] = now

    merge_act_schema = StructType([
        StructField("master_id",   StringType(),    False),
        StructField("match_id",    StringType(),    True),
        StructField("source",      StringType(),    False),
        StructField("action_type", StringType(),    False),
        StructField("version",     IntegerType(),   False),
        StructField("created_at",  TimestampType(), False),
    ])
    df_activities = spark.createDataFrame(merge_activity_rows, schema=merge_act_schema)

    (
        DeltaTable.forName(spark, merge_activities_table).alias("target")
        .merge(
            df_activities.alias("source"),
            """target.master_id   = source.master_id
               AND target.match_id    = source.match_id
               AND target.source      = source.source
               AND target.version     = source.version
               AND target.action_type = source.action_type""",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

if attr_version_rows:
    attr_map_struct = StructType([
        StructField("attribute_name",  StringType(), True),
        StructField("attribute_value", StringType(), True),
        StructField("source",          StringType(), True),
    ])
    attr_version_schema = StructType([
        StructField("lakefusion_id",            StringType(),               False),
        StructField("version",                  IntegerType(),              False),
        StructField("attribute_source_mapping", ArrayType(attr_map_struct), True),
    ])
    df_attr_versions = spark.createDataFrame(attr_version_rows, schema=attr_version_schema)

    (
        DeltaTable.forName(spark, attr_version_sources_table).alias("target")
        .merge(
            df_attr_versions.alias("source"),
            "target.lakefusion_id = source.lakefusion_id AND target.version = source.version",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 7: Advance Version Metadata
# MAGIC
# MAGIC Last write of the notebook. If anything above raised, version pointers
# MAGIC stay where they were and the next run safely re-processes the same window.

# COMMAND ----------

_write_version_metadata(version_map)

# COMMAND ----------

dbutils.jobs.taskValues.set(key="status", value="SUCCESS")
dbutils.jobs.taskValues.set(key="masters_updated", value=len(master_updates))
dbutils.jobs.taskValues.set(key="masters_deleted", value=len(masters_to_delete))
dbutils.jobs.taskValues.set(key="failed_tables",   value=json.dumps(failed_tables))
dbutils.jobs.taskValues.set(key="processed_tables", value=json.dumps(list(version_map.keys())))

logger.info("=" * 80)
logger.info(f"Incremental load complete — "
            f"{len(version_map)} table(s) processed, "
            f"{len(master_updates)} master(s) updated, "
            f"{len(masters_to_delete)} master(s) deleted, "
            f"{len(failed_tables)} table(s) failed")
logger.info("=" * 80)

dbutils.notebook.exit("SUCCESS")