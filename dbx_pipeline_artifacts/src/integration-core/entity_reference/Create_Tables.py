# MAGIC %pip install --upgrade "databricks-sdk>=0.114.0"

# COMMAND ----------

# Lakebase (w.postgres) needs a databricks-sdk newer than some DBR runtimes
# bundle. Upgrade + restart Python HERE, before anything imports databricks.sdk,
# so WorkspaceClient() in this fresh interpreter always exposes w.postgres.
# (A %pip install only takes effect after the interpreter restarts.)
dbutils.library.restartPython()

# COMMAND ----------

import json
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, FloatType, BooleanType, DateType, TimestampType,
    ShortType, ByteType, ArrayType
)
from pyspark.sql.functions import col, lit, current_timestamp
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

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
dbutils.widgets.text("lakebase_instance_id", "", "lakebase_instance_id")
dbutils.widgets.text("lakebase_branch_id", "", "lakebase_branch_id")
dbutils.widgets.text("lakebase_endpoint_id", "", "lakebase_endpoint_id")
dbutils.widgets.text("lakebase_database", "", "lakebase_database")
dbutils.widgets.text("write_mode", "delta", "Write Mode (delta/lakebase)")

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
lakebase_instance_id = dbutils.widgets.get("lakebase_instance_id")
lakebase_branch_id = dbutils.widgets.get("lakebase_branch_id")
lakebase_endpoint_id = dbutils.widgets.get("lakebase_endpoint_id")
lakebase_database = dbutils.widgets.get("lakebase_database")
write_mode = dbutils.widgets.get("write_mode")

# COMMAND ----------

entity_attributes = json.loads(entity_attributes)
dataset_tables = json.loads(dataset_tables)
entity_attributes_datatype = json.loads(entity_attributes_datatype)

# COMMAND ----------

from lakefusion_core_engine.write_ops import create_write_ops,WriteMode
lakebase_params = {
    # Lakebase database-INSTANCE name (what w.database.get_database_instance
    # expects). If this doesn't resolve, the engine falls back to the sole
    # instance or errors listing the valid names.
    "lakebase_instance_id": lakebase_instance_id,
    "lakebase_branch_id": lakebase_branch_id,    # retained for compat (unused in resolution)
    "lakebase_endpoint_id": lakebase_endpoint_id,  # retained for compat (unused in resolution)
    # Target Postgres logical database for the synced tables + change-log.
    # Per-entity DB (named after the entity); the change-log triggers and the
    # UI write path use this same database. The synced table keeps the source
    # UC schema (e.g. gold).
    "lakebase_database": entity,
}

ops = create_write_ops(mode=write_mode, spark=spark, params=lakebase_params)

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType,
    TimestampType, IntegerType, ArrayType, _parse_datatype_string,
)

# =========================
# MASTER TABLE
# =========================
entity_datatype=entity_attributes_datatype
table_fqn = f"{catalog_name}.gold.{entity}_reference_prod"

master_fields = [
    StructField("ref_lakefusion_id", StringType(), nullable=False)
]

for name, dtype in entity_datatype.items():
    is_pk = name.lower() == primary_key.lower()

    master_fields.append(
        StructField(
            name,
            _parse_datatype_string(dtype),
            nullable=not is_pk
        )
    )

master_schema = StructType(master_fields)

empty_master = spark.createDataFrame([], master_schema)

if ops is not None:

    ops.create_table(
        empty_master,
        table_fqn,
        enable_cdf=True,
        primary_key=["ref_lakefusion_id"],
    )

    logger.info(f"Created MASTER {table_fqn} (via ops)")

else:

    if not spark.catalog.tableExists(table_fqn):

        (
            empty_master.write
            .format("delta")
            .option("delta.enableChangeDataFeed", "true")
            .option("delta.feature.allowColumnDefaults", "supported")
            .saveAsTable(table_fqn)
        )

    logger.info(f"Created MASTER {table_fqn} (delta)")
    logger.info(f"Business columns: {len(entity_datatype)} (+ ref_lakefusion_id)")
    logger.info("CDF: Enabled")


# =========================
# AUDIT TABLE
# =========================

audit_table_fqn = f"{catalog_name}.gold.{entity}_reference_audit_prod"

audit_fields = [
    StructField("ref_lakefusion_id", StringType(), nullable=False)
]

for name, dtype in entity_datatype.items():

    audit_fields.append(
        StructField(
            name,
            _parse_datatype_string(dtype),
            nullable=True
        )
    )

audit_fields += [
    StructField("valid_from", TimestampType(), nullable=False),
    StructField("valid_to", TimestampType(), nullable=True),
    StructField("is_current", BooleanType(), nullable=False),
    StructField("version", IntegerType(), nullable=False),
    StructField("source", StringType(), nullable=True),
    StructField("action_type", StringType(), nullable=True),
    StructField("steward_locked", BooleanType(), nullable=False),
    StructField("steward_edited_cols", ArrayType(StringType()), nullable=True),
    StructField("steward_edited_by", StringType(), nullable=True),
]

audit_schema = StructType(audit_fields)

empty_audit = spark.createDataFrame([], audit_schema)

if ops is not None:

    ops.create_table(
        empty_audit,
        audit_table_fqn,
        enable_cdf=True,
        primary_key=["ref_lakefusion_id", "version"],
    )

    logger.info(f"Created AUDIT {audit_table_fqn} (via ops)")

else:

    if not spark.catalog.tableExists(audit_table_fqn):

        (
            empty_audit.write
            .format("delta")
            .option("delta.enableChangeDataFeed", "true")
            .option("delta.feature.allowColumnDefaults", "supported")
            .saveAsTable(audit_table_fqn)
        )

        spark.sql(
            f"""
            ALTER TABLE {audit_table_fqn}
            ALTER COLUMN valid_from
            SET DEFAULT current_timestamp()
            """
        )

        spark.sql(
            f"""
            ALTER TABLE {audit_table_fqn}
            ALTER COLUMN is_current
            SET DEFAULT TRUE
            """
        )

        spark.sql(
            f"""
            ALTER TABLE {audit_table_fqn}
            ALTER COLUMN steward_locked
            SET DEFAULT FALSE
            """
        )

    logger.info(f"Created AUDIT {audit_table_fqn} (delta)")

    logger.info(
        f"Business columns: {len(entity_datatype)} "
        f"(+ ref_lakefusion_id, +5 SCD-2/source, +4 steward/provenance)"
    )

    logger.info("CDF: Enabled")

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
)

# =========================
# CONFLICT QUEUE TABLE
# =========================

conflict_table_fqn = (
    f"{catalog_name}.gold.{entity}_reference_conflict_queue_prod"
)

conflict_schema = StructType([

    StructField("ref_lakefusion_id", StringType(), nullable=False),
    StructField("conflict_id", StringType(), nullable=False),

    StructField("conflict_type", StringType(), nullable=True),
    StructField("entity_key_value", StringType(), nullable=True),

    StructField("field_name", StringType(), nullable=True),

    StructField("rdm_value", StringType(), nullable=True),
    StructField("source_value", StringType(), nullable=True),

    StructField("edited_by", StringType(), nullable=True),

    StructField("status", StringType(), nullable=True),

    StructField("resolved_by", StringType(), nullable=True),

    StructField("resolved_at", TimestampType(), nullable=True),

    StructField("created_at", TimestampType(), nullable=True),

])

empty_conflict = spark.createDataFrame([], conflict_schema)

if ops is not None:

    ops.create_table(
        empty_conflict,
        conflict_table_fqn,
        enable_cdf=True,
        primary_key=["ref_lakefusion_id", "conflict_id"],
    )

    logger.info(
        f"Created CONFLICT QUEUE {conflict_table_fqn} (via ops)"
    )

else:

    if not spark.catalog.tableExists(conflict_table_fqn):

        (
            empty_conflict.write
            .format("delta")
            .option("delta.enableChangeDataFeed", "true")
            .option("delta.feature.allowColumnDefaults", "supported")
            .saveAsTable(conflict_table_fqn)
        )

        spark.sql(
            f"""
            ALTER TABLE {conflict_table_fqn}
            ALTER COLUMN status
            SET DEFAULT 'PENDING'
            """
        )

    logger.info(
        f"Created CONFLICT QUEUE {conflict_table_fqn} (delta)"
    )

    logger.info("CDF: Enabled")

# Forward sync: push the freshly created tables to the Lakebase synced tables.
# No-op for delta mode; for lakebase SNAPSHOT/TRIGGERED policies this kicks the
# synced-table pipeline. Non-fatal (logged inside).
if write_mode == "lakebase":
   for _t in (table_fqn, audit_table_fqn, conflict_table_fqn):
       ops.trigger_sync(_t)

# COMMAND ----------

# =========================
# CHANGE-DATA CAPTURE (lakebase reverse-sync)
# =========================
# UI edits write the Lakebase synced tables; Postgres AFTER triggers snapshot each
# change (full row as JSONB) into gold."{entity}_change_log". The workflow notebook
# Sync_Lakebase_Changelog_To_Delta.py drains that log back into Delta. App-managed
# (no CDF / Preview enablement). DDL is idempotent — safe to re-run.
#
# Note: the {table}_synced Postgres tables are materialized asynchronously by the
# synced-table pipeline; if a trigger create fails because its table isn't live yet,
# we log and continue — re-running this notebook once the table exists installs it.
if write_mode == "lakebase":
    from lakefusion_core_engine.lakebase_client import LakebasePgClient

    pg_schema = "gold"
    change_log = f"{entity}_change_log"
    log_fn = f"{entity}_log_change"
    synced_targets = [
        (f"{entity}_reference_prod_synced", "master"),
        (f"{entity}_reference_audit_prod_synced", "audit"),
        (f"{entity}_reference_conflict_queue_prod_synced", "conflict"),
    ]

    changelog_ddl = f'''
    CREATE TABLE IF NOT EXISTS "{pg_schema}"."{change_log}" (
        seq          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        source_table TEXT NOT NULL,
        target_kind  TEXT NOT NULL,
        operation    TEXT NOT NULL,
        row_data     JSONB,
        changed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
        delta_sync    BOOLEAN NOT NULL DEFAULT FALSE,
        delta_sync_at TIMESTAMPTZ,
        batch_id     TEXT
    );
    '''
    index_ddl = (
        f'CREATE INDEX IF NOT EXISTS "idx_{change_log}_unprocessed" '
        f'ON "{pg_schema}"."{change_log}" (delta_sync, seq);'
    )
    # to_jsonb(NEW/OLD) captures the full post/pre-change row, including complex
    # (STRUCT/ARRAY) attrs — the workflow rehydrates them via the Delta schema.
    # source_table comes from TG_ARGV[1] (the logical synced-table name), NOT
    # TG_TABLE_NAME: Lakebase synced tables are partitioned, so TG_TABLE_NAME
    # would log the leaf partition (e.g. "partition_27422") instead of the table.
    fn_ddl = f'''
    CREATE OR REPLACE FUNCTION "{pg_schema}"."{log_fn}"() RETURNS trigger AS $LF$
    DECLARE payload JSONB;
    BEGIN
        IF TG_OP = 'DELETE' THEN payload := to_jsonb(OLD);
        ELSE                     payload := to_jsonb(NEW);
        END IF;
        INSERT INTO "{pg_schema}"."{change_log}"(source_table, target_kind, operation, row_data)
        VALUES (TG_ARGV[1], TG_ARGV[0], TG_OP, payload);
        RETURN NULL;
    END;
    $LF$ LANGUAGE plpgsql;
    '''

    with LakebasePgClient(
        instance_id=lakebase_instance_id,
        branch_id=lakebase_branch_id or "production",
        endpoint_id=lakebase_endpoint_id or "primary",
        database=lakebase_params["lakebase_database"],
    ) as pg:
        pg.execute(changelog_ddl)
        pg.execute(index_ddl)
        pg.execute(fn_ddl)
        for synced_table, target_kind in synced_targets:
            try:
                # drop-then-create so the function ref / target_kind arg stays current
                pg.execute(
                    f'DROP TRIGGER IF EXISTS "zz_changelog" '
                    f'ON "{pg_schema}"."{synced_table}";'
                )
                pg.execute(
                    f'CREATE TRIGGER "zz_changelog" '
                    f'AFTER INSERT OR UPDATE OR DELETE ON "{pg_schema}"."{synced_table}" '
                    f"FOR EACH ROW EXECUTE FUNCTION \"{pg_schema}\".\"{log_fn}\"('{target_kind}', '{synced_table}');"
                )
                logger.info(f"Installed change-log trigger on {pg_schema}.{synced_table} ({target_kind})")
            except Exception as e:
                logger.warning(
                    f"Could not install change-log trigger on {pg_schema}.{synced_table} "
                    f"({target_kind}) yet: {e}. Re-run Create_Tables once the synced "
                    f"table is materialized."
                )
    logger.info(f"Change-log ready: {pg_schema}.{change_log}")