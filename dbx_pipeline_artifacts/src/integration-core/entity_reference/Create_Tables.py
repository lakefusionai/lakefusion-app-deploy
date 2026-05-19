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
from delta.tables import DeltaTable
from pyspark.sql.types import ArrayType, StringType


def create_rdm_master_table(
    catalog: str,
    schema: str,
    entity_name: str,
    primary_key: str,
    entity_datatype: dict[str, str],
    pk_comment: Optional[str] = None,
) -> str:
    """Create the reference MASTER table — current state, business only.

    One row per ref_lakefusion_id. Always reflects the latest known values.
    History / provenance / SCD-2 control all live on the separate audit
    table (<entity>_reference_audit_prod). Readers querying "current state"
    hit this table directly — no is_current filter needed, no joins.

    Schema = ref_lakefusion_id (PK) + business columns. Period.
    """
    table_fqn = f"{catalog}.{schema}.{entity_name}_reference_prod"

    builder = (
        DeltaTable.createIfNotExists(spark)
        .tableName(table_fqn)
        .comment(
            f"RDM master table for {entity_name} — current state, business columns only. "
            f"History + provenance live on {entity_name}_reference_audit_prod."
        )
        .property("delta.enableChangeDataFeed", "true")
        .property("delta.feature.allowColumnDefaults", "supported")
        .addColumn(
            "ref_lakefusion_id",
            "STRING",
            nullable=False,
            comment="Surrogate key. Unique per row (one row per ref_lakefusion_id).",
        )
    )

    for col_name, col_type in entity_datatype.items():
        if col_name.lower() == primary_key.lower():
            comment = f"Business PK ({pk_comment})" if pk_comment else "Business PK"
            builder = builder.addColumn(
                col_name, col_type, nullable=False, comment=comment
            )
        else:
            builder = builder.addColumn(col_name, col_type)

    builder.execute()

    logger.info(f"  Created MASTER {table_fqn}")
    logger.info(f"  Business columns: {len(entity_datatype)} (+ ref_lakefusion_id)")
    logger.info(f"  CDF: Enabled")
    return table_fqn


def create_rdm_audit_table(
    catalog: str,
    schema: str,
    entity_name: str,
    primary_key: str,
    entity_datatype: dict[str, str],
    pk_comment: Optional[str] = None,
) -> str:
    """Create the reference AUDIT table — append-only history + provenance.

    One row per (ref_lakefusion_id, version). Captures every state change
    the master table goes through, plus who/how/from-where it happened.

    Schema = ref_lakefusion_id + business columns (snapshot at this version)
    + 4 SCD-2 control columns (valid_from, valid_to, is_current, version)
    + source + 4 steward/provenance columns.
    """
    table_fqn = f"{catalog}.{schema}.{entity_name}_reference_audit_prod"

    builder = (
        DeltaTable.createIfNotExists(spark)
        .tableName(table_fqn)
        .comment(
            f"RDM audit/history table for {entity_name} — append-only SCD-2 log of every "
            f"version change to the master table. (ref_lakefusion_id, version) is unique."
        )
        .property("delta.enableChangeDataFeed", "true")
        .property("delta.feature.allowColumnDefaults", "supported")
        .addColumn(
            "ref_lakefusion_id",
            "STRING",
            nullable=False,
            comment="FK to master. Multiple rows per id (one per version).",
        )
    )

    # Business columns — snapshot of the row's values at this version
    for col_name, col_type in entity_datatype.items():
        if col_name.lower() == primary_key.lower():
            comment = f"Business PK at this version ({pk_comment})" if pk_comment else "Business PK at this version"
            builder = builder.addColumn(col_name, col_type, comment=comment)
        else:
            builder = builder.addColumn(col_name, col_type)

    # SCD-2 control
    builder = (
        builder
        .addColumn(
            "valid_from",
            "TIMESTAMP",
            nullable=False,
            comment="SCD-2: when this version became active",
        )
        .addColumn(
            "valid_to",
            "TIMESTAMP",
            comment="SCD-2: when this version was superseded; NULL for current",
        )
        .addColumn(
            "is_current",
            "BOOLEAN",
            nullable=False,
            comment="SCD-2: TRUE for the latest version of a ref_lakefusion_id",
        )
        .addColumn(
            "version",
            "INT",
            nullable=False,
            comment="Per-ref_lakefusion_id sequence (1 for first row, +1 per new version).",
        )
        .addColumn(
            "source",
            "STRING",
            comment=(
                "Source table that produced this version (e.g. 'catalog.schema.country_master'). "
                "NULL for steward-driven versions. Join to <entity>_reference_mappings_prod on "
                "(source, ref_lakefusion_id) for source_id / source_attr / source_value."
            ),
        )
        .addColumn(
            "action_type",
            "STRING",
            comment=(
                "How this version was produced: JOB_MERGE | MANUAL_INSERT | MANUAL_UPSERT | "
                "STEWARD_EDIT | DELETE_EXPIRED | SOFT_DELETE | CONFLICT_RESOLVED | KEEP_RDM"
            ),
        )
        .addColumn(
            "steward_locked",
            "BOOLEAN",
            nullable=False,
            comment=(
                "Carried forward across versions. When TRUE, jobs skip this row "
                "in match strategies (steward_wins). Toggled by steward edits / "
                "conflict resolution."
            ),
        )
        .addColumn(
            "steward_edited_cols",
            ArrayType(StringType()),
            comment="Columns changed in THIS version (NULL for job-created versions).",
        )
        .addColumn(
            "steward_edited_by",
            "STRING",
            comment="Steward / user who created this version (NULL for job-created versions).",
        )
    )

    builder.execute()

    spark.sql(f"ALTER TABLE {table_fqn} ALTER COLUMN valid_from SET DEFAULT current_timestamp()")
    spark.sql(f"ALTER TABLE {table_fqn} ALTER COLUMN is_current SET DEFAULT TRUE")
    spark.sql(f"ALTER TABLE {table_fqn} ALTER COLUMN steward_locked SET DEFAULT FALSE")

    logger.info(f"  Created AUDIT {table_fqn}")
    logger.info(
        f"  Business columns: {len(entity_datatype)} "
        f"(+ ref_lakefusion_id, +5 SCD-2/source, +4 steward/provenance)"
    )
    logger.info(f"  CDF: Enabled")
    return table_fqn


# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("CREATING REFERENCE TABLES (master + audit)")
logger.info("="*60)

master_table = create_rdm_master_table(
    catalog=catalog_name,
    schema="gold",
    entity_name=entity,
    primary_key=primary_key,
    entity_datatype=entity_attributes_datatype,
    pk_comment="Source business key",
)

audit_table = create_rdm_audit_table(
    catalog=catalog_name,
    schema="gold",
    entity_name=entity,
    primary_key=primary_key,
    entity_datatype=entity_attributes_datatype,
    pk_comment="Source business key",
)


# COMMAND ----------

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONFLICT QUEUE  (unchanged — already a separate concern)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

spark.sql(f"""CREATE TABLE IF NOT EXISTS {catalog_name}.gold.{entity}_reference_conflict_queue_prod (
  ref_lakefusion_id STRING    NOT NULL,
  conflict_id       STRING    NOT NULL,
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