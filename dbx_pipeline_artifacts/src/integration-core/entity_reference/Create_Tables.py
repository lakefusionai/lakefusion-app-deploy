# Databricks notebook source
import json
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, FloatType, BooleanType, DateType, TimestampType,
    ShortType, ByteType, ArrayType
)
from pyspark.sql.functions import col, lit, current_timestamp
from delta.tables import DeltaTable

# COMMAND ----------

dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("id_key", "", "ID Key")
dbutils.widgets.text("primary_key", "", "Primary Key")
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")
dbutils.widgets.text("dataset_tables", "", "Dataset Tables")
dbutils.widgets.text("entity_reference_config", "", "entity_reference_config")
dbutils.widgets.text("entity_attributes_datatype", "", "entity_attributes_datatype")

# COMMAND ----------

entity = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON", key="entity",
    debugValue=dbutils.widgets.get("entity")
)
catalog_name = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON", key="catalog_name",
    debugValue=dbutils.widgets.get("catalog_name")
)
id_key = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON", key="id_key",
    debugValue=dbutils.widgets.get("id_key")
)
primary_key = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON", key="primary_key",
    debugValue=dbutils.widgets.get("primary_key")
)
entity_attributes = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON", key="entity_attributes",
    debugValue=dbutils.widgets.get("entity_attributes")
)
dataset_tables = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON", key="dataset_tables",
    debugValue=dbutils.widgets.get("dataset_tables")
)
entity_reference_config = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON", key="entity_reference_config",
    debugValue=dbutils.widgets.get("entity_reference_config")
)
entity_attributes_datatype = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON", key="entity_attributes_datatype",
    debugValue=dbutils.widgets.get("entity_attributes_datatype")
)
experiment_id = dbutils.widgets.get("experiment_id")

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

entity_attributes = json.loads(entity_attributes)
dataset_tables = json.loads(dataset_tables)
entity_attributes_datatype = json.loads(entity_attributes_datatype)

# COMMAND ----------

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 1 — DATA TABLE  (business columns only)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

from typing import Optional


def generate_rdm_data_ddl(
    catalog: str,
    schema: str,
    entity_name: str,
    primary_key: str,
    entity_datatype: dict[str, str],
    pk_comment: Optional[str] = None,
) -> str:
    """DDL for the pure-data table — business columns only, no audit/steward cols."""

    business_cols = []
    for col_name, col_type in entity_datatype.items():
        if col_name.lower() == primary_key.lower():
            comment = (
                f"  -- PK / merge key ({pk_comment})" if pk_comment
                else "  -- PK / merge key"
            )
            business_cols.append(f"  {col_name:<24} {col_type:<10} NOT NULL,{comment}")
        else:
            business_cols.append(f"  {col_name:<24} {col_type},")

    business_block = "\n".join(business_cols).rstrip(',')
    table_fqn = f"{catalog}.{schema}.{entity_name}_reference_prod"

    return f"""
CREATE TABLE IF NOT EXISTS {table_fqn} (
  -- Business columns
{business_block}
)
USING DELTA
COMMENT 'RDM data table for {entity_name} — business columns only'
TBLPROPERTIES (
  'delta.enableChangeDataFeed'          = 'true',
  'delta.feature.allowColumnDefaults'   = 'supported'
);""".strip()

# COMMAND ----------

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 2 — METADATA TABLE  (steward tracking + soft-delete flag)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def generate_rdm_meta_ddl(
    catalog: str,
    schema: str,
    entity_name: str,
    primary_key: str,
    pk_type: str,
) -> str:
    """DDL for the metadata table — steward tracking, soft-delete, keyed by PK."""

    table_fqn = f"{catalog}.{schema}.{entity_name}_reference_meta_prod"

    return f"""
CREATE TABLE IF NOT EXISTS {table_fqn} (
  {primary_key:<24} {pk_type:<10} NOT NULL,  -- FK → data table
  -- Soft delete
  is_active             BOOLEAN   DEFAULT TRUE,
  steward_locked        BOOLEAN   DEFAULT FALSE,
  steward_edited_cols   ARRAY<STRING>,
  steward_edited_by     STRING,
  steward_edited_at     TIMESTAMP,
  created_at            TIMESTAMP,
  updated_at            TIMESTAMP,
  action_type            STRING
)
USING DELTA
COMMENT 'RDM metadata table for {entity_name} — steward & lifecycle tracking'
TBLPROPERTIES (
  'delta.enableChangeDataFeed'          = 'true',
  'delta.feature.allowColumnDefaults'   = 'supported'
);""".strip()

# COMMAND ----------

# ── Create DATA table ──────────────────────────────────────────────────────────
ddl_data = generate_rdm_data_ddl(
    catalog=catalog_name,
    schema="gold",
    entity_name=entity,
    primary_key=primary_key,
    entity_datatype=entity_attributes_datatype,
    pk_comment="",
)
print(ddl_data)
spark.sql(ddl_data)

# ── Create META table ──────────────────────────────────────────────────────────
pk_type = entity_attributes_datatype.get(primary_key, "STRING")
ddl_meta = generate_rdm_meta_ddl(
    catalog=catalog_name,
    schema="gold",
    entity_name=entity,
    primary_key=primary_key,
    pk_type=pk_type,
)
spark.sql(ddl_meta)

# COMMAND ----------

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONFLICT QUEUE  (unchanged — already a separate concern)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

spark.sql(f"""CREATE TABLE IF NOT EXISTS {catalog_name}.gold.{entity}_reference_conflict_queue_prod (
  conflict_id       STRING    NOT NULL,
  run_id            STRING,
  job_id            STRING,
  conflict_type     STRING,
  entity_key_value  STRING,
  field_name        STRING,
  rdm_value         STRING,
  source_value      STRING,
  edited_by         STRING,
  status            STRING  DEFAULT 'PENDING',
  resolved_by       STRING,
  resolved_at       TIMESTAMP,
  created_at        TIMESTAMP
)
USING DELTA
COMMENT 'Conflict queue for {entity}. Steward resolves via Integration Hub UI.'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")

# COMMAND ----------

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEWARD EDIT LOG  (unchanged — already a separate concern)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

spark.sql(f"""CREATE TABLE IF NOT EXISTS {catalog_name}.gold.{entity}_reference_steward_edit_log_prod (
  log_id            STRING  NOT NULL,
  entity_key_value  STRING,
  field_name        STRING,
  old_value         STRING,
  new_value         STRING,
  edited_by         STRING,
  edited_at         TIMESTAMP,
  source            STRING
)
USING DELTA
COMMENT 'Append-only steward edit audit log. Never truncated.'""")

# COMMAND ----------

print(f'{catalog_name}.gold.{entity}_reference_full_vw')

# COMMAND ----------

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# VIEW — JOIN DATA + META back together for querying
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.gold.{entity}_reference_full_vw AS
SELECT
    d.*,
    m.is_active,
    m.steward_locked,
    m.steward_edited_cols,
    m.steward_edited_by,
    m.steward_edited_at,
    m.created_at
FROM {catalog_name}.gold.{entity}_reference_prod  d
LEFT JOIN {catalog_name}.gold.{entity}_reference_meta_prod  m
  ON d.{primary_key} = m.{primary_key}
""")

print(f"View created: {catalog_name}.gold.{entity}_reference_full_vw")