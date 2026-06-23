# Databricks notebook source
# MAGIC %pip install --upgrade "databricks-sdk>=0.114.0"

# COMMAND ----------

# Lakebase (w.postgres) needs a databricks-sdk newer than some DBR runtimes
# bundle. Upgrade + restart Python HERE before importing databricks.sdk.
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════════════════════
# Load PIM data → Delta → (forward-sync) → Lakebase   (SCRUM-1929 Phase 3, Flow B)
# ───────────────────────────────────────────────────────────────────────────────
# Set-based, vectorized load of bulk product data into the PIM Delta tables
# (source of truth), then forward-sync to the Lakebase synced tables the UI reads.
# This is the PIM analog of the reference entity's Process_Reference_Table: read a
# source → transform into the entity's `_prod` tables (here: EAV explode) → MERGE
# Delta → trigger_sync (Delta → Lakebase). It is the BULK path for data too large
# for the synchronous service flat_import.
#
#   source UC/Delta table → explode → MERGE {entity}_pim_*_prod (Delta)
#     → trigger_sync → DLT pipeline materializes → {entity}_pim_*_prod_synced (Lakebase)
#
# Wired as a task in the PIM job (mirrors Process_Reference_Table's slot). SKIPs
# when no source_table is set (so plain scheduled runs no-op). NORMAL FILE IMPORT
# is the synchronous service flat_import (Flow A), NOT this notebook — this is the
# bulk table-source path only.
#
# ── n-TIER SOURCE CONVENTION ──────────────────────────────────────────────────
# The source table has one key COLUMN per tier CODE (top→leaf), e.g. for a
# three-tiered entity: columns PRODUCT, VARIANT, ITEM. Every row carries the
# business key at each level; parents are deduped from repeated keys; the leaf
# (item) row's attribute columns attach to the leaf entity. Attribute columns are
# named by attribute CODE.
#
#   PRODUCT | VARIANT  | ITEM | color | size | price
#   P100    | P100-RED | SKU1 | Red   | M    | 19.99
#
# ── EAV / data-type routing ───────────────────────────────────────────────────
# Each attribute column routes to its typed value table by data_type:
#   TEXT→pim_value_text  NUMBER→pim_value_number  BOOLEAN→pim_value_boolean
#   DATE→pim_value_date  SELECT/MULTISELECT→ref_value_key (validated vs options)
#   REFERENCE→pim_value_reference(ref_table, ref_id). MULTISELECT explodes one row
#   per selected option.
#
# ── UPSERT SEMANTICS ──────────────────────────────────────────────────────────
# Blank cell = never written (never erases); non-blank = upsert (overwrite).
# Deterministic sha2 ids make every MERGE idempotent (re-load upserts in place).
# Per-field Add/Overwrite modes are a service flat_import (Flow A) feature only.
#
# Attribute definitions + options are read from the entity's DELTA tables (the
# source of truth — Seed populates them); entity.json is used only for the tier
# subtype. (3a-1 incorrectly assumed attr defs were in entity.json — fixed here.)
# ═══════════════════════════════════════════════════════════════════════════════
import json

from pyspark.sql import functions as F
from lakefusion_core_engine.write_ops import create_write_ops

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("entity_id", "", "Entity Id")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("experiment_id", "prod", "Experiment ID")
dbutils.widgets.text("write_mode", "delta", "Write Mode (delta/lakebase)")
# Source: an existing UC/Delta table (Flow B — bulk load from a dataset). Normal
# file import is the synchronous service flat_import path (Flow A), NOT this job.
# If a file is ever too large for that path, land it as a UC table first, then
# point source_table here.
dbutils.widgets.text("source_table", "", "Source UC table FQN")
dbutils.widgets.text("lakebase_instance_id", "", "lakebase_instance_id")
dbutils.widgets.text("lakebase_branch_id", "", "lakebase_branch_id")
dbutils.widgets.text("lakebase_endpoint_id", "", "lakebase_endpoint_id")

# COMMAND ----------

catalog_name = dbutils.jobs.taskValues.get(
    taskKey="Parse_PIM_Entity_JSON", key="catalog_name",
    debugValue=dbutils.widgets.get("catalog_name"),
)
entity = dbutils.jobs.taskValues.get(
    taskKey="Parse_PIM_Entity_JSON", key="entity",
    debugValue=dbutils.widgets.get("entity"),
)
entity_subtype = dbutils.jobs.taskValues.get(
    taskKey="Parse_PIM_Entity_JSON", key="pim_entity_subtype",
    debugValue="",
)
entity_id = dbutils.widgets.get("entity_id")
experiment_id = dbutils.widgets.get("experiment_id") or "prod"
write_mode = dbutils.widgets.get("write_mode")
source_table = dbutils.widgets.get("source_table")

# No source configured → clean SKIP (not an error). This task sits in the PIM job
# (mirroring Process_Reference_Table's slot); on a scheduled init run with no bulk
# source it simply no-ops, so it never fails the pipeline. A bulk load is driven by
# a run that passes source_table.
if not source_table:
    import json as _json
    dbutils.notebook.exit(_json.dumps({"status": "skipped_no_source"}))

# COMMAND ----------

# Resolve per-entity Delta FQNs (mirrors Create_PIM_Tables naming).
def delta_tbl(pim_table):
    return f"{catalog_name}.gold.{entity}_{pim_table}_prod"

ENTITY_TBL  = delta_tbl("pim_entity")
ATTR_TBL    = delta_tbl("pim_attribute_definition")
OPTION_TBL  = delta_tbl("pim_attribute_option")
TIER_TBL    = delta_tbl("pim_entity_tier")
VALUE_TBL = {
    "TEXT":        delta_tbl("pim_value_text"),
    "NUMBER":      delta_tbl("pim_value_number"),
    "BOOLEAN":     delta_tbl("pim_value_boolean"),
    "DATE":        delta_tbl("pim_value_date"),
    "SELECT":      delta_tbl("pim_value_select"),
    "MULTISELECT": delta_tbl("pim_value_multiselect"),
    "REFERENCE":   delta_tbl("pim_value_reference"),
}

for t in (ENTITY_TBL, ATTR_TBL, TIER_TBL, *VALUE_TBL.values()):
    if not spark.catalog.tableExists(t):
        raise Exception(f"Delta table {t} not found — run init (Create_PIM_Tables) first.")

# COMMAND ----------

# Attribute definitions + options from Delta (the source of truth).
attr_rows = [r.asDict() for r in spark.read.table(ATTR_TBL).collect()]
attr_by_code = {a["code"]: a for a in attr_rows if a.get("code")}
identifier_attr = next((a for a in attr_rows if a.get("is_identifier")), None)
label_attr = next((a for a in attr_rows if a.get("is_label")), None)
if not identifier_attr:
    raise Exception("No is_identifier attribute defined — required to match/MERGE entities.")

# Option keys per attribute (active only) for SELECT/MULTISELECT validation.
option_keys = {}
if spark.catalog.tableExists(OPTION_TBL):
    for r in spark.read.table(OPTION_TBL).filter(F.col("is_active") == F.lit(True)).collect():
        option_keys.setdefault(r["attribute_id"], set()).add(r["value_key"])

# Tiers ordered top→leaf, from entity_subtype ("three-tiered|Product,Variant,Item").
# Codes are derived the same way Seed/templates do: uppercase label, spaces/dashes→_.
def _tier_codes_from_subtype(subtype):
    if "|" in subtype:
        labels = [l.strip() for l in subtype.split("|", 1)[1].split(",") if l.strip()]
        return [l.upper().replace(" ", "_").replace("-", "_") for l in labels]
    return None

tier_codes = _tier_codes_from_subtype(entity_subtype)
if not tier_codes:
    # Fallback: read tier codes from the Delta tier table, ordered by level.
    tier_codes = [r["code"] for r in
                  spark.read.table(TIER_TBL).orderBy("level").select("code").collect()]
if not tier_codes:
    raise Exception("Could not resolve tier codes from entity_subtype or pim_entity_tier.")
leaf_tier = tier_codes[-1]

logger.info(
    f"PIM bulk load: entity='{entity}' tiers={tier_codes} attrs={len(attr_rows)} "
    f"source_table={source_table}"
)

# COMMAND ----------

# Read the source UC/Delta table → raw DataFrame. Its columns are matched to tier
# codes (hierarchy keys) and attribute codes (values) downstream.
raw_df = spark.read.table(source_table)
_source_desc = f"table {source_table}"

raw_count = raw_df.count()
logger.info(f"Read {raw_count} rows from {_source_desc}")
if raw_count == 0:
    dbutils.notebook.exit(json.dumps({"entity": entity, "rows": 0, "status": "empty_source"}))

file_cols = raw_df.columns
present_tier_cols = [tc for tc in tier_codes if tc in file_cols]
if leaf_tier not in file_cols:
    raise Exception(f"Required leaf-tier column '{leaf_tier}' missing from file (headers: {file_cols}).")
mapped_attr_cols = [c for c in file_cols if c in attr_by_code]
unknown = [c for c in file_cols if c not in attr_by_code and c not in tier_codes]
if unknown:
    logger.warning(f"Ignoring {len(unknown)} unmapped column(s): {unknown}")

# Trim tier key columns; drop rows whose leaf key is blank.
for tc in present_tier_cols:
    raw_df = raw_df.withColumn(tc, F.trim(F.col(tc)))
df = raw_df.filter((F.col(leaf_tier).isNotNull()) & (F.col(leaf_tier) != F.lit("")))
valid_count = df.count()
skipped_blank = raw_count - valid_count
df = df.cache()
logger.info(f"{valid_count} rows with non-blank leaf key ({skipped_blank} skipped)")

# COMMAND ----------

now = F.current_timestamp()

# Deterministic entity id per (tier_code, business_key) — stable across re-imports.
def _entity_id(tier_code_col_or_lit, key_col):
    return F.sha2(F.concat_ws("||", tier_code_col_or_lit, key_col), 256)

# Build pim_entity rows for every tier present, with parent_id = the previous
# (non-blank) tier's entity id. Parents are deduped across the file by entity id.
id_attr_id = identifier_attr["id"]

entity_frames = []
prev_present = None  # (tier_code, key_col_name) of the closest non-blank ancestor
for idx, tc in enumerate(tier_codes):
    if tc not in file_cols:
        continue
    key_col = F.trim(F.col(tc))
    this_id = _entity_id(F.lit(tc), key_col)
    parent_id = F.lit(None).cast("string")
    if prev_present is not None:
        ptc, pcol = prev_present
        parent_id = _entity_id(F.lit(ptc), F.trim(F.col(pcol)))
    frame = (
        df.filter((key_col.isNotNull()) & (key_col != F.lit("")))
          .select(
              this_id.alias("id"),
              F.lit(tc).alias("entity_type_id"),
              F.lit(None).cast("string").alias("taxonomy_node_id"),
              parent_id.alias("parent_id"),
              F.lit("Draft").alias("status"),
              F.lit(True).alias("active"),
              F.lit(False).alias("promoted"),
              F.lit(None).cast("timestamp").alias("published_at"),
              now.alias("created_at"),
              now.alias("updated_at"),
          )
    )
    entity_frames.append(frame)
    prev_present = (tc, tc)

entity_df = entity_frames[0]
for f in entity_frames[1:]:
    entity_df = entity_df.unionByName(f)
# One row per entity id (parents repeat across leaf rows). Keep any (all identical
# except parent linkage, which is consistent per id by construction).
entity_df = entity_df.dropDuplicates(["id"])

ops = create_write_ops(mode=write_mode, spark=spark, params={
    "lakebase_instance_id": dbutils.widgets.get("lakebase_instance_id"),
    "lakebase_branch_id": dbutils.widgets.get("lakebase_branch_id") or "production",
    "lakebase_endpoint_id": dbutils.widgets.get("lakebase_endpoint_id") or "primary",
    "lakebase_database": entity.lower().replace(" ", "_"),
})

ops.merge(
    entity_df, ENTITY_TBL, merge_keys=["id"],
    when_matched={"entity_type_id": "source.entity_type_id", "status": "source.status",
                  "active": "source.active", "updated_at": "source.updated_at"},
    when_not_matched="insert",
)
entities_merged = entity_df.count()
logger.info(f"MERGEd {entities_merged} entities into {ENTITY_TBL} across {len(present_tier_cols)} tiers")

# COMMAND ----------

# The identifier value (the leaf business key) is also written as a TEXT value so
# the service's identifier-based reads/match work. product_id = the LEAF entity id.
leaf_pid = _entity_id(F.lit(leaf_tier), F.trim(F.col(leaf_tier)))

def _value_id(pid_col, attr_id, suffix=""):
    return F.sha2(F.concat_ws("||", pid_col, F.lit(attr_id), F.lit(""), F.lit(suffix)), 256)

def _base_value_cols(pid_col, attr_id, suffix=""):
    return [
        _value_id(pid_col, attr_id, suffix).alias("id"),
        pid_col.alias("product_id"),
        F.lit(attr_id).alias("attribute_id"),
        F.lit("").alias("locale"),
        F.lit("IMPORT").alias("source"),
        F.lit(1).alias("version"),
        now.alias("created_at"),
        now.alias("updated_at"),
    ]

def _merge_values(vdf, table, value_set_cols):
    n = vdf.count()
    if n == 0:
        return 0
    matched = {"source": "source.source", "version": "target.version + 1",
               "updated_at": "source.updated_at"}
    matched.update(value_set_cols)
    ops.merge(vdf, table, merge_keys=["id"], when_matched=matched, when_not_matched="insert")
    return n

# Identifier as TEXT value (leaf).
written = {dt: 0 for dt in VALUE_TBL}
idv = F.trim(F.col(leaf_tier))
id_text_df = (df.filter((idv.isNotNull()) & (idv != F.lit("")))
                .select(*_base_value_cols(leaf_pid, id_attr_id), idv.alias("value"))
                .dropDuplicates(["id"]))
written["TEXT"] += _merge_values(id_text_df, VALUE_TBL["TEXT"], {"value": "source.value"})

# COMMAND ----------

# Explode each mapped attribute column to its typed value table. Values attach to
# the LEAF entity (item-grain import). Blank cells are skipped (never erase).
deferred = []
for code in mapped_attr_cols:
    ad = attr_by_code[code]
    attr_id = ad["id"]
    dt = (ad.get("data_type") or "TEXT").upper()
    raw = F.trim(F.col(code))
    present = df.filter((raw.isNotNull()) & (raw != F.lit("")))

    if dt == "TEXT":
        vdf = present.select(*_base_value_cols(leaf_pid, attr_id), raw.alias("value")).dropDuplicates(["id"])
        written["TEXT"] += _merge_values(vdf, VALUE_TBL["TEXT"], {"value": "source.value"})

    elif dt == "NUMBER":
        valc = raw.cast("decimal(38,10)")
        vdf = (present.select(*_base_value_cols(leaf_pid, attr_id), valc.alias("value"),
                              F.lit("").alias("currency"), F.lit("").alias("price_type"),
                              F.lit("").alias("territory"))
                      .filter(F.col("value").isNotNull()).dropDuplicates(["id"]))
        written["NUMBER"] += _merge_values(vdf, VALUE_TBL["NUMBER"], {"value": "source.value"})

    elif dt == "BOOLEAN":
        valc = F.lower(raw).isin("true", "1", "yes", "y", "t")
        vdf = present.select(*_base_value_cols(leaf_pid, attr_id), valc.alias("value")).dropDuplicates(["id"])
        written["BOOLEAN"] += _merge_values(vdf, VALUE_TBL["BOOLEAN"], {"value": "source.value"})

    elif dt == "DATE":
        valc = F.to_date(raw)
        vdf = (present.select(*_base_value_cols(leaf_pid, attr_id), valc.alias("value"))
                      .filter(F.col("value").isNotNull()).dropDuplicates(["id"]))
        written["DATE"] += _merge_values(vdf, VALUE_TBL["DATE"], {"value": "source.value"})

    elif dt == "SELECT":
        valid = option_keys.get(attr_id, set())
        keyc = F.lower(F.regexp_replace(raw, r"[^A-Za-z0-9]+", "_"))  # to_value_key
        keyc = F.regexp_replace(keyc, r"^_+|_+$", "")
        vdf = present.select(*_base_value_cols(leaf_pid, attr_id), keyc.alias("ref_value_key"))
        if valid:
            vdf = vdf.filter(F.col("ref_value_key").isin(list(valid)))
        vdf = vdf.dropDuplicates(["id"])
        written["SELECT"] += _merge_values(vdf, VALUE_TBL["SELECT"], {"ref_value_key": "source.ref_value_key"})

    elif dt == "MULTISELECT":
        valid = option_keys.get(attr_id, set())
        # split on ';' or ',', explode, normalize to value_key, one row per key.
        parts = F.explode(F.split(raw, r"[;,]"))
        ex = present.select(leaf_pid.alias("__pid"), parts.alias("__part"))
        partc = F.lower(F.regexp_replace(F.trim(F.col("__part")), r"[^A-Za-z0-9]+", "_"))
        partc = F.regexp_replace(partc, r"^_+|_+$", "")
        ex = ex.filter((partc.isNotNull()) & (partc != F.lit("")))
        vdf = ex.select(
            F.sha2(F.concat_ws("||", F.col("__pid"), F.lit(attr_id), F.lit(""), partc), 256).alias("id"),
            F.col("__pid").alias("product_id"),
            F.lit(attr_id).alias("attribute_id"),
            F.lit("").alias("locale"), F.lit("IMPORT").alias("source"),
            F.lit(1).alias("version"), now.alias("created_at"), now.alias("updated_at"),
            partc.alias("ref_value_key"),
        )
        if valid:
            vdf = vdf.filter(F.col("ref_value_key").isin(list(valid)))
        vdf = vdf.dropDuplicates(["id"])
        written["MULTISELECT"] += _merge_values(vdf, VALUE_TBL["MULTISELECT"], {"ref_value_key": "source.ref_value_key"})

    elif dt == "REFERENCE":
        ref_table = ad.get("reference_entity_id") or ""
        vdf = present.select(*_base_value_cols(leaf_pid, attr_id),
                             F.lit(ref_table).alias("ref_table"), raw.alias("ref_id")).dropDuplicates(["id"])
        written["REFERENCE"] += _merge_values(vdf, VALUE_TBL["REFERENCE"],
                                              {"ref_table": "source.ref_table", "ref_id": "source.ref_id"})
    else:
        deferred.append((code, dt))

if deferred:
    logger.warning(f"Unhandled data_type(s) skipped: {deferred}")

# COMMAND ----------

# Forward sync touched Delta tables → Lakebase synced tables.
if write_mode == "lakebase":
    for t in (ENTITY_TBL, *VALUE_TBL.values()):
        try:
            ops.trigger_sync(t)
        except Exception as e:
            logger.warning(f"trigger_sync failed for {t}: {e}")

# COMMAND ----------

summary = {
    "entity": entity,
    "source_table": source_table,
    "tiers": tier_codes,
    "rows_read": raw_count,
    "rows_imported": valid_count,
    "skipped_blank_leaf": skipped_blank,
    "entities_merged": entities_merged,
    "values_merged": written,
    "unhandled_types": deferred,
    "subphase": "flow-b-table-load",
    "status": "ok",
}
logger.info(f"PIM import complete: {summary}")
df.unpersist()
dbutils.notebook.exit(json.dumps(summary, default=str))
