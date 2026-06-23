# Databricks notebook source
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

# ═══════════════════════════════════════════════════════════════════════════════
# PIM Lakebase change-log → Delta reverse sync  (SCRUM-1929 Phase 2)
# ───────────────────────────────────────────────────────────────────────────────
# UI edits write the PIM Lakebase synced tables; Postgres AFTER triggers (installed
# by Create_PIM_Tables) capture each change as a full-row JSONB snapshot into
#   gold."{entity}_pim_change_log"
# with target_kind = the pim_* table name. This notebook reads the unprocessed
# change-log rows and MERGEs each target_kind into its Delta source-of-truth table
#   {catalog}.gold.{entity}_{pim_table}_prod
# then marks the rows processed.
#
# Mirrors the reference-entity Sync_Lakebase_Changelog_To_Delta, with two PIM
# differences:
#   1. ONE change-log, MANY targets — the 16 user-editable pim_* tables, not 3.
#      TARGETS is built from pim_table_schemas (the same manifest Create_PIM_Tables
#      uses), so it stays in sync with the models automatically; per-table PK comes
#      from each spec's `primary_key`.
#   2. The 4 reference-only tables (NO_CHANGELOG_TABLES) have no triggers, so they
#      never appear in the log and are skipped.
#
# Run before downstream PIM processing so Delta is reconciled. Idempotent +
# resumable via the change-log `processed` flag (watermark on `seq`).
# ═══════════════════════════════════════════════════════════════════════════════
import json

from lakefusion_core_engine.write_ops.lakebase_changelog import LakebaseChangeLogReader
from lakefusion_core_engine.read_ops.lakebase_ops import LakebaseReadOps
from lakefusion_core_engine.lakebase_client import LakebasePgClient

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "catalog name")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("pim_table_schemas", "", "PIM Table Schemas (JSON)")
dbutils.widgets.text("write_mode", "delta", "Write Mode (delta/lakebase)")
dbutils.widgets.text("lakebase_instance_id", "", "lakebase_instance_id")
dbutils.widgets.text("lakebase_branch_id", "", "lakebase_branch_id")
dbutils.widgets.text("lakebase_endpoint_id", "", "lakebase_endpoint_id")
dbutils.widgets.text("lakebase_database", "databricks_postgres", "lakebase_database")

# COMMAND ----------

catalog_name = dbutils.jobs.taskValues.get(
    taskKey="Parse_PIM_Entity_JSON", key="catalog_name",
    debugValue=dbutils.widgets.get("catalog_name"),
)
entity = dbutils.jobs.taskValues.get(
    taskKey="Parse_PIM_Entity_JSON", key="entity",
    debugValue=dbutils.widgets.get("entity"),
)
pim_table_schemas = dbutils.jobs.taskValues.get(
    taskKey="Parse_PIM_Entity_JSON", key="pim_table_schemas",
    debugValue=dbutils.widgets.get("pim_table_schemas"),
)
write_mode = dbutils.widgets.get("write_mode")

pim_table_schemas = json.loads(pim_table_schemas)

# The change-log + synced tables live in the entity's own Lakebase database (named
# after the sanitized entity), matching Create_PIM_Tables (which hardcodes the DB
# to the entity name and ignores the lakebase_database widget). Always derive the
# DB from the entity name, or the change-log read targets the wrong database.
_entity_db = entity.lower().replace(" ", "_")
lakebase_params = {
    "lakebase_instance_id": dbutils.widgets.get("lakebase_instance_id"),
    "lakebase_branch_id":   dbutils.widgets.get("lakebase_branch_id") or "production",
    "lakebase_endpoint_id": dbutils.widgets.get("lakebase_endpoint_id") or "primary",
    "lakebase_database":    _entity_db,
}

# COMMAND ----------

# Only lakebase-backed entities have a change-log to drain.
if write_mode != "lakebase":
    dbutils.notebook.exit("write_mode != lakebase — no Lakebase change-log to drain. Skipping.")

# COMMAND ----------

# Reference-only tables get no change-log triggers (see Create_PIM_Tables) — keep
# this set identical so the two notebooks agree on which tables reverse-sync.
NO_CHANGELOG_TABLES = {
    "pim_language_ref",
    "pim_unit_ref",
    "pim_entity_tier",
    "pim_resolved_specification",
}

# Build MERGE targets from the manifest: one per user-editable pim_* table.
# target_kind (what the trigger stamps) = the pim_* table name; the Delta target is
# {catalog}.gold.{entity}_{table}_prod; pk_cols = the spec's primary_key.
TARGETS = []  # (target_kind, delta_table, pk_cols)
for table_name, spec in sorted(pim_table_schemas.items()):
    if table_name in NO_CHANGELOG_TABLES:
        continue
    pk_cols = spec.get("primary_key") or []
    if not pk_cols:
        logger.warning(f"{table_name} has no primary_key in manifest — skipping (cannot MERGE without a key).")
        continue
    delta_table = f"{catalog_name}.gold.{entity}_{table_name}_prod"
    TARGETS.append((table_name, delta_table, pk_cols))

logger.info(f"PIM reverse sync: {len(TARGETS)} candidate target tables for entity '{entity}'")

CHANGE_LOG_SCHEMA = "gold"
CHANGE_LOG_TABLE = f"{entity}_pim_change_log"

# COMMAND ----------

# Read the unprocessed change-log rows (JDBC). row_data (jsonb) arrives as a string.
read_ops = LakebaseReadOps(spark, lakebase_params)
try:
    changelog = read_ops.read_raw(CHANGE_LOG_SCHEMA, CHANGE_LOG_TABLE)
except Exception as e:
    dbutils.notebook.exit(
        f"Change-log {CHANGE_LOG_SCHEMA}.{CHANGE_LOG_TABLE} not readable ({e}); "
        f"PIM not initialized or no edits yet — skipping."
    )

from pyspark.sql import functions as F

unprocessed = changelog.filter(F.col("delta_sync") == F.lit(False))

if unprocessed.head(1) == []:
    dbutils.notebook.exit("No unprocessed change-log rows")

max_seq = unprocessed.agg(F.max("seq").alias("m")).collect()[0]["m"]

# COMMAND ----------

# MERGE each target_kind present in the log into its Delta table. A target whose
# Delta table is missing (not yet provisioned) is logged and skipped, not fatal —
# so one un-provisioned table can't block draining the rest.
reader = LakebaseChangeLogReader(spark)
results = {}
for target_kind, delta_table, pk_cols in TARGETS:
    df_kind = unprocessed.filter(F.col("target_kind") == F.lit(target_kind)) \
                         .select("seq", "operation", "row_data")
    if df_kind.head(1) == []:
        continue  # no edits for this table this run
    if not spark.catalog.tableExists(delta_table):
        logger.warning(f"SKIP {target_kind}: Delta table {delta_table} not found.")
        results[target_kind] = {"skipped": "delta_table_missing"}
        continue
    # PIM uses hard delete (rows are soft-deleted in-app via active=FALSE, which is a
    # normal UPDATE; a Postgres DELETE means the row is truly gone).
    results[target_kind] = reader.merge_to_delta(df_kind, delta_table, pk_cols, soft_delete_set=None)
    logger.info(f"{target_kind} merge: {results[target_kind]}")

# COMMAND ----------

# Mark drained rows processed (psycopg2). Rows arriving during the run get a higher
# seq and are picked up next run — lock-free, idempotent. We watermark on seq, so
# any target skipped above is also marked processed; re-running after the Delta
# table is provisioned will NOT re-drain those rows. If that matters, re-run
# Create_PIM_Tables to provision the missing table BEFORE the next sync.
import time as _time

batch_id = f"pimsync_{int(_time.time())}"
with LakebasePgClient(
    instance_id=lakebase_params["lakebase_instance_id"],
    branch_id=lakebase_params["lakebase_branch_id"],
    endpoint_id=lakebase_params["lakebase_endpoint_id"],
    database=lakebase_params["lakebase_database"],
) as pg:
    affected = pg.execute(
        f'UPDATE "{CHANGE_LOG_SCHEMA}"."{CHANGE_LOG_TABLE}" '
        f'SET delta_sync = TRUE, delta_sync_at = now(), batch_id = %s '
        f'WHERE seq <= %s AND delta_sync = FALSE',
        (batch_id, int(max_seq)),
    )
logger.info(f"Marked {affected} change-log rows processed (seq <= {max_seq}, batch={batch_id})")

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "entity": entity, "max_seq": int(max_seq), "marked_processed": affected,
    "targets_merged": list(results.keys()), "results": results,
}, default=str))
