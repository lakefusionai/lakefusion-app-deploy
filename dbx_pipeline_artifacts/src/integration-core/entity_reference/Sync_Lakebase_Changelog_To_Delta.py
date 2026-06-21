# MAGIC %pip install --upgrade "databricks-sdk>=0.114.0"

# COMMAND ----------

# Lakebase (w.postgres) needs a databricks-sdk newer than some DBR runtimes
# bundle. Upgrade + restart Python HERE, before anything imports databricks.sdk,
# so WorkspaceClient() in this fresh interpreter always exposes w.postgres.
# (A %pip install only takes effect after the interpreter restarts.)
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

# Databricks notebook source
# ═══════════════════════════════════════════════════════════════════════════════
# Lakebase change-log → Delta reverse sync
# ───────────────────────────────────────────────────────────────────────────────
# UI edits write the Lakebase Postgres tables; Postgres AFTER triggers capture each
# change (full row snapshot) into  gold."{entity}_change_log".  This notebook reads
# the unprocessed change-log rows and MERGEs them into the Delta source-of-truth
# (master / audit / conflict), then marks the rows processed.
#
# Run as the FIRST task of the entity pipeline (before matching/survivorship) so
# downstream steps see reconciled Delta. Idempotent + resumable via the change-log
# `processed` flag (watermark on `seq`).
#
# Replaces the old Lakebase CDF drain (Sync_Lakebase_CDF_To_Delta).
# ═══════════════════════════════════════════════════════════════════════════════
import json

from lakefusion_core_engine.write_ops.lakebase_changelog import LakebaseChangeLogReader
from lakefusion_core_engine.read_ops.lakebase_ops import LakebaseReadOps
from lakefusion_core_engine.lakebase_client import LakebasePgClient

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "catalog name")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("write_mode", "delta", "Write Mode (delta/lakebase)")
dbutils.widgets.text("lakebase_instance_id", "", "lakebase_instance_id")
dbutils.widgets.text("lakebase_branch_id", "", "lakebase_branch_id")
dbutils.widgets.text("lakebase_endpoint_id", "", "lakebase_endpoint_id")
dbutils.widgets.text("lakebase_database", "databricks_postgres", "lakebase_database")
dbutils.widgets.text("soft_delete", "false", "Soft-delete (record_status=DELETED) vs hard delete on master")

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
entity       = dbutils.widgets.get("entity")
write_mode   = dbutils.widgets.get("write_mode")

entity     = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)

# The change-log + synced tables live in the entity's own Lakebase database
# (named after the sanitized entity). This MUST match where Create_Tables.py
# provisions them: it hardcodes the database to the entity name (ignoring the
# lakebase_database widget/job param), as does entity_search_service via
# _entity_pg_schema. The job passes lakebase_database=databricks_postgres, so we
# must NOT use that param here — always derive the DB from the entity name, or
# the change-log read targets the wrong database (relation does not exist).
_entity_db = entity.lower().replace(" ", "_")
lakebase_params = {
    "lakebase_instance_id": dbutils.widgets.get("lakebase_instance_id"),
    "lakebase_branch_id":   dbutils.widgets.get("lakebase_branch_id") or "production",
    "lakebase_endpoint_id": dbutils.widgets.get("lakebase_endpoint_id") or "primary",
    "lakebase_database":    _entity_db,
}
soft_delete = dbutils.widgets.get("soft_delete").strip().lower() == "true"

# COMMAND ----------

# Only lakebase-backed entities have a change-log to drain.
if write_mode != "lakebase":
    dbutils.notebook.exit("write_mode != lakebase — no Lakebase change-log to drain. Skipping.")

# COMMAND ----------

# Delta source-of-truth tables (MERGE targets) and their grains.
MASTER_TABLE   = f"{catalog_name}.gold.{entity}_reference_prod"
AUDIT_TABLE    = f"{catalog_name}.gold.{entity}_reference_audit_prod"
CONFLICT_TABLE = f"{catalog_name}.gold.{entity}_reference_conflict_queue_prod"

TARGETS = [
    # (target_kind, delta_table, pk_cols)
    ("master",   MASTER_TABLE,   ["ref_lakefusion_id"]),
    ("audit",    AUDIT_TABLE,    ["ref_lakefusion_id", "version"]),
    ("conflict", CONFLICT_TABLE, ["ref_lakefusion_id", "conflict_id"]),
]

# Change-log lives beside the synced tables: same DB (lakebase_database), schema `gold`.
CHANGE_LOG_SCHEMA = "gold"
CHANGE_LOG_TABLE  = f"{entity}_change_log"
SOFT_DELETE_SET = {"record_status": "'DELETED'"} if soft_delete else None

if not spark.catalog.tableExists(MASTER_TABLE):
    dbutils.notebook.exit(f"{MASTER_TABLE} not found — reference tables not provisioned; skipping.")

# COMMAND ----------

# Read the unprocessed change-log rows (JDBC). row_data (jsonb) arrives as a string.
read_ops = LakebaseReadOps(spark, lakebase_params)
try:
    changelog = read_ops.read_raw(CHANGE_LOG_SCHEMA, CHANGE_LOG_TABLE)
except Exception as e:
    raise Exception(f"Change-log {CHANGE_LOG_SCHEMA}.{CHANGE_LOG_TABLE} not readable ({e}); skipping.")

from pyspark.sql import functions as F
unprocessed = changelog.filter(F.col("processed") == F.lit(False))

if unprocessed.head(1) == []:
   dbutils.notebook.exit("No unprocessed change-log rows")

max_seq = unprocessed.agg(F.max("seq").alias("m")).collect()[0]["m"]

# COMMAND ----------

reader = LakebaseChangeLogReader(spark)
results = {}
for target_kind, delta_table, pk_cols in TARGETS:
    df_kind = unprocessed.filter(F.col("target_kind") == F.lit(target_kind)) \
                         .select("seq", "operation", "row_data")
    results[target_kind] = reader.merge_to_delta(
        df_kind, delta_table, pk_cols,
        soft_delete_set=(SOFT_DELETE_SET if target_kind == "master" else None),
    )
    print(f"{target_kind} merge:", results[target_kind])

# COMMAND ----------

# Mark the drained rows processed (psycopg2). New rows arriving during the run get a
# higher seq and are picked up next run — lock-free, idempotent.
import time as _time
batch_id = f"sync_{int(_time.time())}"
with LakebasePgClient(
    instance_id=lakebase_params["lakebase_instance_id"],
    branch_id=lakebase_params["lakebase_branch_id"],
    endpoint_id=lakebase_params["lakebase_endpoint_id"],
    database=lakebase_params["lakebase_database"],
) as pg:
    affected = pg.execute(
        f'UPDATE "{CHANGE_LOG_SCHEMA}"."{CHANGE_LOG_TABLE}" '
        f'SET processed = TRUE, processed_at = now(), batch_id = %s '
        f'WHERE seq <= %s AND processed = FALSE',
        (batch_id, int(max_seq)),
    )
print(f"Marked {affected} change-log rows processed (seq <= {max_seq}, batch={batch_id})")

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "entity": entity, "max_seq": int(max_seq), "marked_processed": affected,
    "results": results,
}, default=str))
