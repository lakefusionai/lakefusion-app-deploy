# Databricks notebook source
# MAGIC %pip install --upgrade "databricks-sdk>=0.114.0"

# COMMAND ----------

# Lakebase (w.postgres) needs a databricks-sdk newer than some DBR runtimes
# bundle. Upgrade + restart Python HERE, before anything imports databricks.sdk,
# so WorkspaceClient() in this fresh interpreter always exposes w.postgres.
# (A %pip install only takes effect after the interpreter restarts.)
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Create PIM Tables (SCRUM-1929)
# MAGIC
# MAGIC Second task of the PIM product-entity pipeline. Mirrors the reference-entity
# MAGIC `Create_Tables` — same `LakebaseWriteOps.create_table()` calls, same
# MAGIC change-log table / trigger-function / AFTER-trigger install — but instead
# MAGIC of three hand-coded tables it LOOPS over the `PIM_TABLE_SCHEMAS` manifest
# MAGIC emitted by `Parse_PIM_Entity_JSON` (introspected from the SQLAlchemy
# MAGIC models). The 4 static tables that need no Delta/sync are skipped here and
# MAGIC seeded directly by `Seed_PIM_Reference_Data`.

# COMMAND ----------

import json
from pyspark.sql.types import StructType, StructField, _parse_datatype_string

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("pim_table_schemas", "", "PIM Table Schemas (JSON)")
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
pim_table_schemas = dbutils.jobs.taskValues.get(
    taskKey="Parse_PIM_Entity_JSON", key="pim_table_schemas",
    debugValue=dbutils.widgets.get("pim_table_schemas")
)
lakebase_instance_id = dbutils.widgets.get("lakebase_instance_id")
lakebase_branch_id = dbutils.widgets.get("lakebase_branch_id")
lakebase_endpoint_id = dbutils.widgets.get("lakebase_endpoint_id")
write_mode = dbutils.widgets.get("write_mode")

# COMMAND ----------

pim_table_schemas = json.loads(pim_table_schemas)

# ALL pim_* tables get a Delta table + synced Lakebase table (Delta is the
# source of truth; pipeline-visible; ready to be REFERENCE-type attribute
# targets — SCRUM-1929 long-term goal). Two concerns are split:
#   - forward sync (Delta → synced Lakebase): applies to ALL tables
#   - reverse change-log triggers (UI edit → Postgres → Delta): only on
#     user-editable tables, NOT the reference-only tables below.
# The reference-only tables are seeded once by Seed_PIM_Reference_Data and are
# never hand-edited in the UI, so they need no change-log capture.
NO_CHANGELOG_TABLES = {
    "pim_language_ref",
    "pim_unit_ref",
    "pim_entity_tier",
    "pim_resolved_specification",
}

# Every table is created with Delta + synced.
sync_tables = dict(pim_table_schemas)
# Subset that also gets reverse-sync change-log triggers.
changelog_tables = {
    name: spec
    for name, spec in sync_tables.items()
    if name not in NO_CHANGELOG_TABLES
}

logger.info(
    f"PIM Create_Tables: {len(sync_tables)} Delta+synced tables; "
    f"{len(changelog_tables)} with change-log triggers "
    f"({len(sync_tables) - len(changelog_tables)} reference-only, no triggers): "
    f"{', '.join(sorted(NO_CHANGELOG_TABLES & set(sync_tables)))}"
)

# COMMAND ----------

from lakefusion_core_engine.write_ops import create_write_ops

lakebase_params = {
    "lakebase_instance_id": lakebase_instance_id,
    "lakebase_branch_id": lakebase_branch_id,      # retained for compat (unused in resolution)
    "lakebase_endpoint_id": lakebase_endpoint_id,  # retained for compat (unused in resolution)
    # Per-entity Postgres database (named after the entity); synced tables +
    # change-log triggers + the UI write path all use this database.
    "lakebase_database": entity,
}

ops = create_write_ops(mode=write_mode, spark=spark, params=lakebase_params)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Delta tables (with CDF) + synced tables — loop over manifest
# MAGIC
# MAGIC Naming convention (matches reference entity): the Delta table is
# MAGIC `{catalog}.gold.{entity}_{pim_table}_prod`, the synced Lakebase table is
# MAGIC `gold."{entity}_{pim_table}_prod_synced"`. `ops.create_table` enables CDF
# MAGIC and provisions the synced table; for `delta` mode it is a plain Delta create.

# COMMAND ----------


def _build_struct(columns):
    """Build a StructType from a manifest column list.

    Each column carries a Spark type-string the Parse task derived from the
    SQLAlchemy model; `_parse_datatype_string` is the same parser the
    reference-entity Create_Tables uses.
    """
    fields = [
        StructField(
            col["name"],
            _parse_datatype_string(col["type"]),
            nullable=bool(col.get("nullable", True)),
        )
        for col in columns
    ]
    return StructType(fields)


created_delta_fqns = []

for table_name in sorted(sync_tables.keys()):
    spec = sync_tables[table_name]
    schema = _build_struct(spec["columns"])
    primary_key = spec["primary_key"]
    if not primary_key:
        # A synced table cannot be created without a PK (null-PK rows would be
        # dropped); every pim_* table has one, but guard explicitly.
        logger.warning(f"Skipping {table_name}: no primary key in manifest")
        continue

    table_fqn = f"{catalog_name}.gold.{entity}_{table_name}_prod"
    empty_df = spark.createDataFrame([], schema)

    if ops is not None:
        ops.create_table(
            empty_df,
            table_fqn,
            enable_cdf=True,
            primary_key=primary_key,
        )
        logger.info(f"Created {table_fqn} (via ops, pk={primary_key})")
    else:
        if not spark.catalog.tableExists(table_fqn):
            (
                empty_df.write
                .format("delta")
                .option("delta.enableChangeDataFeed", "true")
                .option("delta.feature.allowColumnDefaults", "supported")
                .saveAsTable(table_fqn)
            )
        logger.info(f"Created {table_fqn} (delta, pk={primary_key})")

    created_delta_fqns.append(table_fqn)

logger.info(f"Created {len(created_delta_fqns)} PIM Delta tables")

# COMMAND ----------

# Forward sync: push the freshly created tables to the Lakebase synced tables.
# No-op for delta mode; for lakebase SNAPSHOT/TRIGGERED policies this kicks the
# synced-table pipeline. Non-fatal (logged inside).
if write_mode == "lakebase":
    for _t in created_delta_fqns:
        ops.trigger_sync(_t)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Change-data capture (lakebase reverse-sync)
# MAGIC
# MAGIC UI edits write the Lakebase synced tables; Postgres AFTER triggers snapshot
# MAGIC each change (full row as JSONB) into `gold."{entity}_pim_change_log"`. The
# MAGIC `Sync_PIM_Changelog_To_Delta` notebook drains that log back into Delta.
# MAGIC App-managed (no CDF / Preview enablement). DDL is idempotent — safe to re-run.
# MAGIC
# MAGIC One change-log table per entity (not per pim_* table); `target_kind` carries
# MAGIC the pim_* table name so the drain notebook routes each row to the right Delta
# MAGIC table. The {table}_synced Postgres tables are materialized asynchronously;
# MAGIC if a trigger create fails because its table isn't live yet, we log and
# MAGIC continue — re-running this notebook once the table exists installs it.

# COMMAND ----------

if write_mode == "lakebase":
    from lakefusion_core_engine.lakebase_client import LakebasePgClient

    pg_schema = "gold"
    change_log = f"{entity}_pim_change_log"
    log_fn = f"{entity}_pim_log_change"
    # target_kind = the pim_* table name; synced table = {entity}_{table}_prod_synced.
    # Triggers only on user-editable tables (changelog_tables) — reference-only
    # tables are never UI-edited, so they need no reverse-sync change capture.
    synced_targets = [
        (f"{entity}_{table_name}_prod_synced", table_name)
        for table_name in sorted(changelog_tables.keys())
    ]

    changelog_ddl = f'''
    CREATE TABLE IF NOT EXISTS "{pg_schema}"."{change_log}" (
        seq          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        source_table TEXT NOT NULL,
        target_kind  TEXT NOT NULL,
        operation    TEXT NOT NULL,
        row_data     JSONB,
        changed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
        delta_sync    BOOLEAN NOT NULL DEFAULT FALSE,
        delta_sync_at TIMESTAMPTZ,
        batch_id      TEXT
    );
    '''
    index_ddl = (
        f'CREATE INDEX IF NOT EXISTS "idx_{change_log}_unprocessed" '
        f'ON "{pg_schema}"."{change_log}" (delta_sync, seq);'
    )
    # to_jsonb(NEW/OLD) captures the full post/pre-change row; the drain notebook
    # rehydrates it via the target Delta schema.
    fn_ddl = f'''
    CREATE OR REPLACE FUNCTION "{pg_schema}"."{log_fn}"() RETURNS trigger AS $LF$
    DECLARE payload JSONB;
    BEGIN
        IF TG_OP = 'DELETE' THEN payload := to_jsonb(OLD);
        ELSE                     payload := to_jsonb(NEW);
        END IF;
        INSERT INTO "{pg_schema}"."{change_log}"(source_table, target_kind, operation, row_data)
        VALUES (TG_TABLE_NAME, TG_ARGV[0], TG_OP, payload);
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
                    f"FOR EACH ROW EXECUTE FUNCTION \"{pg_schema}\".\"{log_fn}\"('{target_kind}');"
                )
                logger.info(f"Installed change-log trigger on {pg_schema}.{synced_table} ({target_kind})")
            except Exception as e:
                logger.warning(
                    f"Could not install change-log trigger on {pg_schema}.{synced_table} "
                    f"({target_kind}) yet: {e}. Re-run Create_PIM_Tables once the synced "
                    f"table is materialized."
                )

# COMMAND ----------

logger.info("PIM Create_Tables complete.")
