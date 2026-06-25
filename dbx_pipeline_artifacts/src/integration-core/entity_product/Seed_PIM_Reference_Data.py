# Databricks notebook source
# MAGIC %pip install --upgrade "databricks-sdk>=0.114.0"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Seed PIM Reference Data (SCRUM-1929)
# MAGIC
# MAGIC Third task of the PIM product-entity pipeline. Ports the seeding logic that
# MAGIC `initialize_pim()` did directly against Lakebase (steps: hierarchy tiers,
# MAGIC default languages, default units, bulk global-attribute sync) into the
# MAGIC pipeline — but writes **via Delta** (`ops.append` → `trigger_sync`), so
# MAGIC Delta stays the source of truth and the rows reach Lakebase through the
# MAGIC synced tables created by `Create_PIM_Tables`.
# MAGIC
# MAGIC Seeds these tables, all now Delta-backed (Create_PIM_Tables builds Delta+synced
# MAGIC for all pim_* tables):
# MAGIC   - pim_entity_tier      (from entity_subtype)
# MAGIC   - pim_language_ref     (defaults)
# MAGIC   - pim_unit_ref         (defaults)
# MAGIC   - pim_tab_groups       (defaults)
# MAGIC   - pim_attribute_definition (global attrs from entity.json)
# MAGIC
# MAGIC Each seed is idempotent: skipped if the Delta table already has rows.

# COMMAND ----------

import json
import uuid
from datetime import datetime

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("pim_entity_subtype", "", "Entity Subtype")
dbutils.widgets.text("lakebase_instance_id", "", "lakebase_instance_id")
dbutils.widgets.text("lakebase_branch_id", "", "lakebase_branch_id")
dbutils.widgets.text("lakebase_endpoint_id", "", "lakebase_endpoint_id")
dbutils.widgets.text("write_mode", "delta", "Write Mode (delta/lakebase)")

# COMMAND ----------

entity = dbutils.jobs.taskValues.get(
    taskKey="Parse_PIM_Entity_JSON", key="entity",
    debugValue=dbutils.widgets.get("entity")
)
catalog_name = dbutils.jobs.taskValues.get(
    taskKey="Parse_PIM_Entity_JSON", key="catalog_name",
    debugValue=dbutils.widgets.get("catalog_name")
)
entity_subtype = dbutils.jobs.taskValues.get(
    taskKey="Parse_PIM_Entity_JSON", key="pim_entity_subtype",
    debugValue=dbutils.widgets.get("pim_entity_subtype")
)
experiment_id = dbutils.widgets.get("experiment_id")
lakebase_instance_id = dbutils.widgets.get("lakebase_instance_id")
lakebase_branch_id = dbutils.widgets.get("lakebase_branch_id")
lakebase_endpoint_id = dbutils.widgets.get("lakebase_endpoint_id")
write_mode = dbutils.widgets.get("write_mode")

# COMMAND ----------

from lakefusion_core_engine.write_ops import create_write_ops

lakebase_params = {
    "lakebase_instance_id": lakebase_instance_id,
    "lakebase_branch_id": lakebase_branch_id,
    "lakebase_endpoint_id": lakebase_endpoint_id,
    "lakebase_database": entity,
}
ops = create_write_ops(mode=write_mode, spark=spark, params=lakebase_params)


def _fqn(table_name):
    return f"{catalog_name}.gold.{entity}_{table_name}_prod"


def _is_empty(table_name):
    """True if the Delta table has no rows (idempotency guard)."""
    fqn = _fqn(table_name)
    try:
        return spark.table(fqn).limit(1).count() == 0
    except Exception as e:
        logger.warning(f"Could not read {fqn} for idempotency check: {e}; assuming empty")
        return True


def _seed(table_name, rows):
    """Append seed rows to the Delta table (then sync), if it is currently empty."""
    if not rows:
        logger.info(f"{table_name}: no rows to seed")
        return 0
    if not _is_empty(table_name):
        logger.info(f"{table_name}: already seeded, skipping")
        return 0
    # Build the DataFrame against the TARGET TABLE'S schema (column set + types),
    # not from dict-key inference — this guarantees alignment and avoids NullType
    # inference for all-None columns. We force every field nullable for the
    # createDataFrame step so a row that omits a column passes (it becomes None);
    # callers are responsible for supplying values for columns that are NOT NULL
    # in Delta and have no DB-side default (Delta does not apply SQLAlchemy
    # model defaults). See _ensure_required() below.
    from pyspark.sql.types import StructType
    target_schema = spark.table(_fqn(table_name)).schema
    nullable_schema = StructType([
        type(f)(f.name, f.dataType, True) for f in target_schema.fields
    ])
    aligned = [
        {field.name: row.get(field.name) for field in target_schema.fields}
        for row in rows
    ]
    df = spark.createDataFrame(aligned, schema=nullable_schema)
    ops.append(df, _fqn(table_name))   # LakebaseWriteOps.append() triggers sync internally
    logger.info(f"Seeded {len(rows)} rows into {table_name}")
    return len(rows)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Constants + attributes — read from entity.json
# MAGIC The fixed seed constants (tier templates, type mapping, default langs/units)
# MAGIC and the entity's attributes are embedded into entity.json SERVICE-side
# MAGIC (build_pim_table_schemas / pim_seed_constants in integration_hub_service),
# MAGIC so this notebook needs NO lakefusion_utility import on the serverless
# MAGIC cluster (not installed there). pim.py / pim_constants stay the single
# MAGIC source of truth (the service reads them).

# COMMAND ----------

import glob

experiment_path = 'prod'
if experiment_id and experiment_id != 'prod':
    experiment_path = f'experiment_{experiment_id}'

_matches = glob.glob(f'/Volumes/{catalog_name}/metadata/metadata_files/*_{experiment_path}_entity.json')
if not _matches:
    raise ValueError(
        f"No entity.json found under /Volumes/{catalog_name}/metadata/metadata_files/ "
        f"for {experiment_path} — the service must upload it at task creation."
    )
with open(_matches[0], 'r') as f:
    _entity_json = json.loads(f.read())

_seed_constants = _entity_json.get("pim_seed_constants") or {}
TIER_TEMPLATES = _seed_constants.get("tier_templates") or {}
TYPE_MAPPING = _seed_constants.get("type_mapping") or {}
DEFAULT_LANGS = _seed_constants.get("default_langs") or []
DEFAULT_UNITS = _seed_constants.get("default_units") or []
DEFAULT_TAB_GROUPS = _seed_constants.get("default_tab_groups") or []
if not TIER_TEMPLATES:
    raise ValueError("entity.json missing 'pim_seed_constants' — service must embed it.")

entity_attrs = _entity_json.get("attributes", []) or []
logger.info(f"Loaded {len(entity_attrs)} global attributes from {_matches[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Hierarchy tiers (from entity_subtype)

# COMMAND ----------


def _resolve_tiers(raw_subtype):
    """Mirror PimEntityBridgeService.get_hierarchy_tiers: parse custom labels
    ("three-tiered|Product,Variant,Item") and apply them over the template."""
    raw_subtype = raw_subtype or "two-tiered"
    custom_labels = None
    if "|" in raw_subtype:
        subtype, labels_str = raw_subtype.split("|", 1)
        custom_labels = [l.strip() for l in labels_str.split(",") if l.strip()]
    else:
        subtype = raw_subtype

    tiers = [dict(t) for t in TIER_TEMPLATES.get(subtype, TIER_TEMPLATES["two-tiered"])]
    if custom_labels and len(custom_labels) == len(tiers):
        for i, tier in enumerate(tiers):
            tier["label"] = custom_labels[i]
            tier["code"] = custom_labels[i].upper().replace(" ", "_").replace("-", "_")
    return tiers


tiers = _resolve_tiers(entity_subtype)
# Mint UUIDs + chain parent_tier_id (root has no parent), matching the
# flush-and-link loop in _seed_hierarchy_tiers.
now = datetime.utcnow()
tier_rows = []
parent_id = None
for t in tiers:
    tid = str(uuid.uuid4())
    tier_rows.append({
        "id": tid,
        "code": t["code"],
        "label": t["label"],
        "level": t["level"],
        "parent_tier_id": parent_id,
        "is_leaf": t["is_leaf"],
        "created_at": now,
        "updated_at": now,
    })
    parent_id = tid

tiers_seeded = _seed("pim_entity_tier", tier_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Default languages & 3. Default units

# COMMAND ----------

# is_active / created_at are NOT NULL in Delta with no DB-side default, so set
# them explicitly here (the SQLAlchemy path relied on model defaults, which Delta
# does not apply).
langs_seeded = _seed(
    "pim_language_ref",
    [{**l, "is_active": True, "created_at": now} for l in DEFAULT_LANGS],
)
units_seeded = _seed(
    "pim_unit_ref",
    [{**u, "is_active": True, "created_at": now} for u in DEFAULT_UNITS],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3b. Tab groups (predefined; users cannot add tabs)

# COMMAND ----------

# display_order = position in the constant list; is_system / created_at are NOT NULL
# in Delta with no DB-side default, so set them explicitly.
tab_groups_seeded = _seed(
    "pim_tab_groups",
    [
        {"name": g["name"], "label": g["label"], "display_order": i, "is_system": True, "created_at": now}
        for i, g in enumerate(DEFAULT_TAB_GROUPS)
    ],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Bulk global attributes (from entity.json)
# MAGIC
# MAGIC Reads the entity's attributes from the volume metadata (entity.json already
# MAGIC carries them, including `type_config`) and maps each to a
# MAGIC `pim_attribute_definition` row. Mirrors PimAttributeSyncService.sync_attribute.
# MAGIC
# MAGIC `type_config` is unvalidated JSON — missing keys fall back to defaults. We
# MAGIC LOG when a key is absent so silent-wrong becomes visible-wrong (per PRD
# MAGIC Gaps & Risks: "Attribute type_config is unvalidated JSON").

# COMMAND ----------

# entity_attrs was already loaded from entity.json near the top. A zero-attribute
# entity is a valid state (everything can be specification-side on PIM), so an
# empty list here is fine — nothing to seed into pim_attribute_definition.
if not entity_attrs:
    logger.info("Entity has no global attributes (all specification-side); nothing to seed into pim_attribute_definition")


def _normalize_code(name):
    return (name or "").strip().lower().replace(" ", "_").replace("-", "_")


attr_rows = []
now = datetime.utcnow()
for attr in entity_attrs:
    name = attr.get("name")
    if not name:
        continue
    mdm_type = attr.get("type", "STRING")
    type_config = attr.get("type_config") or {}
    if not isinstance(type_config, dict):
        type_config = {}

    # Defensive .get with logging when a key is absent (silent-wrong → visible).
    for key in ("group", "display_order", "is_localizable", "level"):
        if key not in type_config:
            logger.info(f"attr '{name}': type_config missing '{key}', using default")

    ref_entity_id = None
    if mdm_type == "REFERENCE_ENTITY":
        ref_entity_id = str(type_config.get("reference_entity_id", "")) or None

    # display_order is unvalidated JSON — coerce defensively so a non-numeric
    # value can't abort the whole attribute seed.
    try:
        display_order = int(type_config.get("display_order", 0))
    except (TypeError, ValueError):
        logger.info(f"attr '{name}': non-numeric display_order, defaulting to 0")
        display_order = 0

    attr_rows.append({
        "id": str(uuid.uuid4()),
        "code": _normalize_code(name),
        "label": attr.get("label") or name,
        "data_type": TYPE_MAPPING.get(mdm_type, "TEXT"),
        "reference_entity_id": ref_entity_id,
        "description": attr.get("description") or None,
        "source_attr_id": attr.get("id"),
        "is_localizable": bool(type_config.get("is_localizable", False)),
        "level": type_config.get("level") or "ALL",
        "scope": "general",
        "is_system": False,
        "is_identifier": bool(attr.get("is_primary_key", False)),
        "is_label": bool(attr.get("is_label", False)),
        "group": type_config.get("group") or "general",
        "display_order": display_order,
        "created_at": now,
        "updated_at": now,
    })

attrs_seeded = _seed("pim_attribute_definition", attr_rows)

# COMMAND ----------

logger.info(
    f"PIM Seed complete — tiers={tiers_seeded}, languages={langs_seeded}, "
    f"units={units_seeded}, tab_groups={tab_groups_seeded}, attributes={attrs_seeded}"
)
