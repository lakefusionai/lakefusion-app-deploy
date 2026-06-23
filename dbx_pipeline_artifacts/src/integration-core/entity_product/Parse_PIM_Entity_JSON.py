# Databricks notebook source
# MAGIC %md
# MAGIC # Parse PIM Entity JSON (SCRUM-1929)
# MAGIC
# MAGIC First task of the PIM product-entity initialization pipeline. Mirrors the
# MAGIC reference-entity `Parse Entity & Model JSON` task: it reads the entity
# MAGIC metadata from the volume and emits values for the downstream tasks
# MAGIC (`Create_PIM_Tables`, `Seed_PIM_Reference_Data`, `RBAC_Permissions`).
# MAGIC
# MAGIC Unlike reference entity, the PIM table SCHEMAS are not derived from the
# MAGIC entity's attributes — the 16 `pim_*` tables have FIXED schemas defined in
# MAGIC `lakefusion_utility/models/pim.py` (SQLAlchemy). To avoid hardcoding those
# MAGIC schemas in the notebook, we INTROSPECT the SQLAlchemy models here and emit
# MAGIC the schema as JSON task values. `pim.py` stays the single source of truth.

# COMMAND ----------

# MAGIC %run ../../utils/taskvalues_enum

# COMMAND ----------

dbutils.widgets.text('entity_id', '', 'Entity ID')
dbutils.widgets.text('experiment_id', '', 'Experiment ID')
dbutils.widgets.text('catalog_name', '', 'lakefusion catalog name')

# COMMAND ----------

entity_id = dbutils.widgets.get('entity_id')
experiment_id = dbutils.widgets.get('experiment_id')
catalog_name = dbutils.widgets.get('catalog_name')

# COMMAND ----------

# MAGIC %md
# MAGIC `execute_utils` MUST run AFTER catalog_name is read above — its
# MAGIC init_logger needs catalog_name to resolve the pipeline_logs volume path.

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

logger.info("Parsing PIM Entity JSON")

# COMMAND ----------

import json

experiment_path = 'prod'
if experiment_id and experiment_id != 'prod':
    experiment_path = f'experiment_{experiment_id}'
entity_json_path = f'/Volumes/{catalog_name}/metadata/metadata_files/entity_{entity_id}_{experiment_path}_entity.json'

# COMMAND ----------

with open(entity_json_path, 'r') as f:
    entity_json = json.loads(f.read())

# Entity (database) name — the per-entity Lakebase DB and the Delta table prefix.
entity = entity_json.get("name", "entity").lower().replace(" ", "_")
entity_type = entity_json.get("entity_type", "product")
storage_type = entity_json.get("storage_type", "delta")

logger.info(f"PIM entity: {entity} (type={entity_type}, storage={storage_type})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## PIM table schemas (read from entity.json)
# MAGIC
# MAGIC The fixed pim_* table schemas are introspected from the SQLAlchemy models
# MAGIC SERVICE-side (where lakefusion_utility is available) and embedded into
# MAGIC entity.json under `pim_table_schemas`. The notebook reads them here —
# MAGIC NO lakefusion_utility import (not installed on the serverless cluster),
# MAGIC same volume-read pattern reference entity uses. pim.py stays the single
# MAGIC source of truth (the service introspects it).

# COMMAND ----------

pim_table_schemas = entity_json.get("pim_table_schemas") or {}
if not pim_table_schemas:
    raise ValueError(
        "entity.json is missing 'pim_table_schemas' — the service must embed it "
        "at task creation (build_pim_table_schemas in integration_hub_service)."
    )

logger.info(
    f"Loaded {len(pim_table_schemas)} pim_* table schemas from entity.json: "
    f"{', '.join(pim_table_schemas.keys())}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hierarchy tiers (from entity_subtype)
# MAGIC
# MAGIC The Seed task creates pim_entity_tier rows from the entity's subtype
# MAGIC (e.g. "two-tiered|Product,Item"). We pass the raw subtype through so the
# MAGIC seed logic stays in one place.

# COMMAND ----------

entity_subtype = entity_json.get("entity_subtype", "")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Emit task values for downstream PIM tasks

# COMMAND ----------

dbutils.jobs.taskValues.set(TaskValueKey.ENTITY.value, entity)
dbutils.jobs.taskValues.set(TaskValueKey.ENTITY_TYPE.value, entity_type)
dbutils.jobs.taskValues.set(TaskValueKey.STORAGE_TYPE.value, storage_type)
dbutils.jobs.taskValues.set(TaskValueKey.CATALOG_NAME.value, catalog_name)
# PIM-specific: the full pim_* schema manifest + raw subtype for tier seeding.
dbutils.jobs.taskValues.set(TaskValueKey.PIM_TABLE_SCHEMAS.value, json.dumps(pim_table_schemas))
dbutils.jobs.taskValues.set(TaskValueKey.PIM_ENTITY_SUBTYPE.value, entity_subtype)

logger.info("PIM entity parse complete — task values emitted for Create_PIM_Tables.")
