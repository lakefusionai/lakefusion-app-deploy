# Databricks notebook source
# MAGIC %pip install --upgrade "databricks-sdk>=0.89.0" psycopg2-binary
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

import json

# COMMAND ----------

dbutils.widgets.text("experiment_id", "", "Match Maven Experiment")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")
dbutils.widgets.text("catalog_name", "", "catalog name")
dbutils.widgets.text("entity_attributes_datatype", "", "entity_attributes_datatype")
dbutils.widgets.text("entity_reference_config", "", "entity_reference_config")
dbutils.widgets.text("primary_key", "", "merge key")
dbutils.widgets.text("attributes_mapping", "", "attributes_mapping")
dbutils.widgets.text("write_mode", "delta", "Write Mode (delta/lakebase)")
dbutils.widgets.text("lakebase_instance_id", "", "lakebase_instance_id")
dbutils.widgets.text("lakebase_branch_id", "", "lakebase_branch_id")
dbutils.widgets.text("lakebase_endpoint_id", "", "lakebase_endpoint_id")

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
entity = dbutils.widgets.get("entity")
experiment_id = dbutils.widgets.get("experiment_id")
entity_attributes_datatype = dbutils.widgets.get("entity_attributes_datatype")
entity_attributes = dbutils.widgets.get("entity_attributes")
entity_reference_config = dbutils.widgets.get("entity_reference_config")
attributes_mapping = dbutils.widgets.get("attributes_mapping")
primary_key = dbutils.widgets.get("primary_key")
write_mode = dbutils.widgets.get("write_mode")
lakebase_instance_id = dbutils.widgets.get("lakebase_instance_id")
lakebase_branch_id = dbutils.widgets.get("lakebase_branch_id")
lakebase_endpoint_id = dbutils.widgets.get("lakebase_endpoint_id")

# COMMAND ----------

entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
entity_attributes_datatype = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes_datatype", debugValue=entity_attributes_datatype)
entity_attributes = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="entity_attributes", debugValue=entity_attributes)
entity_reference_config = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="entity_reference_config", debugValue=entity_reference_config)
merge_key = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="primary_key", debugValue=primary_key)
attributes_mapping = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="attributes_mapping", debugValue=attributes_mapping)


# COMMAND ----------

from lakefusion_core_engine.write_ops import create_write_ops,WriteMode
lakebase_params = {                                # becomes PG schema name
    "lakebase_instance_id": lakebase_instance_id,  # project display_name
    "lakebase_branch_id": lakebase_branch_id,        # default
    "lakebase_endpoint_id": lakebase_endpoint_id,         # default
    "lakebase_database": entity,
}
 
write_ops = create_write_ops(mode=write_mode, spark=spark, params=lakebase_params)

# COMMAND ----------

entity_attributes_datatype = json.loads(entity_attributes_datatype)
attributes_mapping = json.loads(attributes_mapping)

# COMMAND ----------

from lakefusion_core_engine.identifiers import  generate_ref_key
from pyspark.sql.types import *
generate_ref_key_udf = udf(
    lambda source_path, source_id: generate_ref_key(source_path, str(source_id)), 
    StringType()
)

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime
import uuid

try:
    # Works in Databricks Jobs — set automatically by the job scheduler
    RUN_ID = f"RUN_{dbutils.jobs.taskValues.get(taskKey='run_id', default=str(uuid.uuid4()))}"
except Exception:
    RUN_ID = f"RUN_{uuid.uuid4()}"
RUN_ID=RUN_ID.replace('-','_')

NOW=datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _audit_steward_lock_lookup(delta_audit, ref_ids_df):
    """Read steward_locked from the current audit row per ref so job-created
    versions inherit it. Returns DF: (ref_lakefusion_id, _locked_carry)."""
    return (
        delta_audit
        .filter(F.col("is_current") == F.lit(True))
        .join(ref_ids_df.select("ref_lakefusion_id").distinct(), on="ref_lakefusion_id", how="inner")
        .select(
            "ref_lakefusion_id",
            F.coalesce(F.col("steward_locked"), F.lit(False)).alias("_locked_carry"),
        )
    )


def _audit_next_version_lookup(delta_audit, ref_ids_df):
    """Compute the next per-ref_lakefusion_id version number from audit.
    Reads max(version) across ALL rows so a flip-current+append never reuses
    a burned number. Refs not yet in audit are absent from the result —
    callers coalesce to 1 for brand-new ids."""
    return (
        delta_audit
        .join(ref_ids_df.select("ref_lakefusion_id").distinct(), on="ref_lakefusion_id", how="inner")
        .groupBy("ref_lakefusion_id")
        .agg((F.max("version") + F.lit(1)).alias("_next_version"))
    )


def _master_upsert(src_df, biz_cols):
    """UPSERT (ref_lakefusion_id + biz_cols) into master keyed on ref_lakefusion_id."""
    write_ops.merge(
        src_df.select("ref_lakefusion_id", *biz_cols),
        MASTER_TABLE,
        merge_keys=["ref_lakefusion_id"],
    )


def _audit_expire_current(ref_ids_df):
    """Flip the current audit row(s) for these ref_ids → is_current=FALSE,
    valid_to=now. action_type stays as-is so the row preserves its original
    creation reason."""
    write_ops.update_by_keys(
        ref_ids_df,
        AUDIT_TABLE,
        key_columns=["ref_lakefusion_id"],
        set_expressions={
            "is_current": "FALSE",
            "valid_to":   "CURRENT_TIMESTAMP",
        },
        extra_predicate="is_current = TRUE",
    )


def _scd2_expire_and_insert(delta_audit, src_df, biz_cols, action_type):
    """Job-driven update path. For each ref_id in src_df:
      1. Flip the prior current audit row → is_current=FALSE.
      2. Compute next version + carry steward_locked from prior audit row.
      3. UPSERT master with new biz values.
      4. Append a new audit row (is_current=TRUE) with full provenance.
    """
    # Step 1 — flip prior current audit row(s)
    _audit_expire_current(src_df)

    # Step 2 — carry steward_locked + next version from audit
    locked_carry  = _audit_steward_lock_lookup(delta_audit, src_df)
    version_carry = _audit_next_version_lookup(delta_audit, src_df)
    enriched = (
        src_df.select("ref_lakefusion_id", "source", *biz_cols)
        .join(locked_carry,  on="ref_lakefusion_id", how="left")
        .join(version_carry, on="ref_lakefusion_id", how="left")
        .withColumn("steward_locked", F.coalesce(F.col("_locked_carry"),  F.lit(False)))
        .withColumn("version",        F.coalesce(F.col("_next_version"), F.lit(1)).cast("int"))
        .drop("_locked_carry", "_next_version")
    )

    # Step 3 — UPSERT master with the new biz values
    _master_upsert(enriched, biz_cols)

    # Step 4 — append the new audit version with provenance
    new_versions = (
        enriched
        .withColumn("valid_from",          F.current_timestamp())
        .withColumn("valid_to",            F.lit(None).cast("timestamp"))
        .withColumn("is_current",          F.lit(True))
        .withColumn("action_type",         F.lit(action_type))
        .withColumn("steward_edited_cols", F.lit(None).cast("array<string>"))
        .withColumn("steward_edited_by",   F.lit(None).cast("string"))
    )
    write_ops.append(new_versions, AUDIT_TABLE)


def _scd2_insert_new(src_df, biz_cols, action_type):
    """Brand-new ref_lakefusion_ids — INSERT into master + append v1 to audit.
    No prior audit version, so steward_locked defaults to FALSE."""
    enriched = src_df.select("ref_lakefusion_id", "source", *biz_cols)

    # Master insert — using UPSERT for safety in case of rare re-runs.
    _master_upsert(enriched, biz_cols)

    # Audit v1 row
    new_rows = (
        enriched
        .withColumn("valid_from",          F.current_timestamp())
        .withColumn("valid_to",            F.lit(None).cast("timestamp"))
        .withColumn("is_current",          F.lit(True))
        .withColumn("version",             F.lit(1).cast("int"))
        .withColumn("action_type",         F.lit(action_type))
        .withColumn("steward_locked",      F.lit(False))
        .withColumn("steward_edited_cols", F.lit(None).cast("array<string>"))
        .withColumn("steward_edited_by",   F.lit(None).cast("string"))
    )
    write_ops.append(new_rows, AUDIT_TABLE)


def _scd2_expire(delta_audit, keys_df, action_type="DELETE_EXPIRED"):
    """Deletion path. For each ref_id in keys_df:
      1. Snapshot the prior current audit row (we'll mirror its biz values
         onto the tombstone so audit shows what the row looked like).
      2. Flip the prior current audit row → is_current=FALSE.
      3. DELETE the row from master.
      4. Append a tombstone audit row: same biz values + version+1 +
         action_type=DELETE_EXPIRED|SOFT_DELETE + is_current=FALSE.
    """
    keys_only = keys_df.select("ref_lakefusion_id").distinct()

    # Step 1 — snapshot current audit rows BEFORE the expire flips them.
    audit_cols =spark.read.table(AUDIT_TABLE).columns
    current_snapshot = (
        delta_audit
        .filter(F.col("is_current") == F.lit(True))
        .join(keys_only, on="ref_lakefusion_id", how="inner")
    ).cache()
    current_snapshot.count()  # force materialisation before mutating audit

    # Step 2 — flip prior current
    _audit_expire_current(keys_only)

    # Step 3 — DELETE from master
    write_ops.delete_by_keys(keys_only, MASTER_TABLE, key_columns=["ref_lakefusion_id"])

    # Step 4 — tombstone audit row, biz cols carried from prior current.
    tombstone = (
        current_snapshot
        .withColumn("version",             (F.col("version") + F.lit(1)).cast("int"))
        .withColumn("valid_from",          F.current_timestamp())
        .withColumn("valid_to",            F.current_timestamp())
        .withColumn("is_current",          F.lit(False))
        .withColumn("action_type",         F.lit(action_type))
        .withColumn("steward_edited_cols", F.lit(None).cast("array<string>"))
        .withColumn("steward_edited_by",   F.lit(None).cast("string"))
        .select(*audit_cols)
    )
    write_ops.append(tombstone, AUDIT_TABLE)
    current_snapshot.unpersist()


# ── RESOLVE & LOAD ALL SOURCE TABLES ─────────────────────────────────────────
# attributes_mapping can have multiple source tables
# Each is loaded, renamed to target col names, and unioned into one source df

# ── RESOLVE & LOAD ALL SOURCE TABLES ─────────────────────────────────────────
source = None
for entry in attributes_mapping:
    src_table     = list(entry.keys())[0]
    src_col_map   = entry[src_table]
    src_merge_key = src_col_map.get(merge_key)
    if src_merge_key is None:
        raise ValueError(f"merge_key '{merge_key}' not found in mapping for {src_table}")

    print(f"Loading source : {src_table}")
    print(f"Merge key      : {src_merge_key} → {merge_key}")
    print(f"Columns        : {list(src_col_map.keys())}")

    src_df = (
        spark.table(src_table)
        .filter(F.col(src_merge_key).isNotNull())
        .select([
            F.col(src).alias(tgt)
            for tgt, src in src_col_map.items()
        ])
        # ✅ Mint ref_lakefusion_id deterministically from (source_path, source_id)
        .withColumn(
            "ref_lakefusion_id",
            generate_ref_key_udf(F.lit(src_table), F.col(merge_key)),
        )
        # Carry the originating source table through to the SCD-2 writers so
        # each new version row records which source produced it.
        .withColumn("source", F.lit(src_table))
    )
    source = src_df if source is None else source.union(src_df)

# Dedup on ref_lakefusion_id (same source+id → same surrogate → safe dedup)
source = source.dropDuplicates(["ref_lakefusion_id"])

first_col_map     = list(attributes_mapping[0].values())[0]
entity_attributes = list(first_col_map.keys())


print(f"\nTotal source rows after union + dedup : {source.count()}")
print(f"Entity attributes                     : {entity_attributes}")

# ── TABLE PATHS ───────────────────────────────────────────────────────────────
# Reference data is split: master holds business columns (one row per ref),
# audit holds SCD-2 history + provenance (one row per version per ref).
MASTER_TABLE = f"{catalog_name}.gold.{entity}_reference_prod"
AUDIT_TABLE  = f"{catalog_name}.gold.{entity}_reference_audit_prod"
QUEUE_TABLE  = f"{catalog_name}.gold.{entity}_reference_conflict_queue_prod"

target_master = spark.read.table(MASTER_TABLE)
target_audit  = spark.read.table(AUDIT_TABLE)

# COMMAND ----------

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 2: HANDLE MATCHES  (on_match)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Match on ref_lakefusion_id — stable system key, not business PK.
matched = source.join(target_master.select("ref_lakefusion_id"), on="ref_lakefusion_id", how="inner")


def handle_match(config, matched_df, target_master_df, run_id, now):
    strategy = config["on_match"]
    # delta_audit only used for READS in the lookup helpers; writes go through write_ops.
    delta_audit = spark.read.table(AUDIT_TABLE)
    # Business cols = everything that isn't the surrogate or the business PK
    biz_cols = [c for c in entity_attributes]

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ✅ Materialize all inputs BEFORE any MERGE touches the tables
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    tmp_matched = f"_tmp_matched_{run_id}"
    tmp_master  = f"_tmp_target_master_{run_id}"

    def write_temp_snapshot(df, table_name):
        clean_df = df.select([
            F.col(c).cast(df.schema[c].dataType).alias(c)
            for c in df.columns
        ])
        (
            clean_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("delta.columnMapping.mode", "none")
            .saveAsTable(table_name)
        )
        return spark.table(table_name)

    matched_snap     = write_temp_snapshot(matched_df,        tmp_matched)
    target_master_snap = write_temp_snapshot(target_master_df, tmp_master)

    # steward_locked / steward_edited_by live on audit (current rows only).
    audit_current_meta = (
        spark.read.table(AUDIT_TABLE)
        .filter(F.col("is_current") == F.lit(True))
        .select("ref_lakefusion_id", "steward_locked", "steward_edited_by")
    )

    matched_count = matched_snap.count()
    locked_count = (
        audit_current_meta
        .join(target_master_snap.select("ref_lakefusion_id"), on="ref_lakefusion_id", how="inner")
        .filter(F.col("steward_locked") == True)
        .count()
    )
    print(f"[handle_match] matched_rows={matched_count}, locked_current_rows={locked_count}")

    try:
        # ── Locked rows: current audit row has steward_locked=TRUE ─────────────
        locked_df = (
            audit_current_meta
            .filter(F.col("steward_locked") == True)
            .select("ref_lakefusion_id")
        )

        if strategy == "source_wins":
            # Job update: UPSERT master + flip-current+append in audit. Each
            # new audit row gets action_type='JOB_MERGE' and inherits the
            # prior version's steward_locked value.
            _scd2_expire_and_insert(delta_audit, matched_snap, biz_cols, action_type="JOB_MERGE")
            print(
                f"[on_match=source_wins] Versioned {matched_count} rows "
                f"(UPSERT master + new audit versions)"
            )

        elif strategy == "steward_wins":
            unlocked = matched_snap.join(locked_df, on="ref_lakefusion_id", how="left_anti")
            unlocked_count = unlocked.count()
            # Apply only to unlocked refs — locked rows keep their current
            # version untouched on both master and audit.
            _scd2_expire_and_insert(delta_audit, unlocked, biz_cols, action_type="JOB_MERGE")
            skipped = matched_count - unlocked_count
            print(
                f"[on_match=steward_wins] Versioned {unlocked_count} rows "
                f"(expire+insert), skipped {skipped} locked rows"
            )

        elif strategy == "flag_for_review":
            # ── Already-pending conflicts (dedup by ref_lakefusion_id + field) ──
            already_queued_df = (
                spark.read.table(QUEUE_TABLE)
                .filter(
                    (F.col("conflict_type") == "UPDATE_CONFLICT")
                    & (F.col("status").isin("PENDING", "KEEP_RDM"))
                )
                .select("ref_lakefusion_id", "field_name")
            )

            # ── Compare ALL matched rows against current RDM values ─────────
            # Master IS the current state; join audit for steward_edited_by.
            current_data = (
                target_master_snap
                .join(audit_current_meta.select("ref_lakefusion_id", "steward_edited_by"),
                      on="ref_lakefusion_id", how="left")
            )
            compare = (
                matched_snap.alias("src")
                .join(current_data.alias("tgt_d"), on="ref_lakefusion_id", how="inner")
            )

            # ── Unpivot each business column into conflict rows ─────────────
            unpivoted = None
            for col_name in biz_cols:
                col_df = compare.select(
                    F.col("src.ref_lakefusion_id").alias("ref_lakefusion_id"),
                    F.col(f"src.{merge_key}").cast("string").alias("entity_key_value"),
                    F.lit(col_name).alias("field_name"),
                    F.col(f"tgt_d.{col_name}").cast("string").alias("rdm_value"),
                    F.col(f"src.{col_name}").cast("string").alias("source_value"),
                    F.col("tgt_d.steward_edited_by").alias("edited_by"),
                ).filter(
                    (F.col("source_value").isNotNull())
                    & (F.col("rdm_value").isNotNull())
                    & (F.col("source_value") != F.col("rdm_value"))
                )
                unpivoted = col_df if unpivoted is None else unpivoted.union(col_df)

            if unpivoted is not None:
                candidate_df = (
                    unpivoted
                    .withColumn("conflict_id",   F.expr("uuid()"))
                    .withColumn("conflict_type", F.lit("UPDATE_CONFLICT"))
                    .withColumn("status",        F.lit("PENDING"))
                    .withColumn("resolved_by",   F.lit(None).cast("string"))
                    .withColumn("resolved_at",   F.lit(None).cast("timestamp"))
                    .withColumn("created_at",    F.lit(now).cast("timestamp"))
                    .select(
                        "conflict_id", "conflict_type",
                        "ref_lakefusion_id", "entity_key_value",
                        "field_name", "rdm_value", "source_value",
                        "edited_by", "status",
                        "resolved_by", "resolved_at", "created_at",
                    )
                )

                net_new_df = candidate_df.join(
                    already_queued_df,
                    on=["ref_lakefusion_id", "field_name"],
                    how="left_anti",
                )

                candidate_count = candidate_df.count()
                net_new_count   = net_new_df.count()
                print(
                    f"[on_match=flag_for_review] candidates={candidate_count}, "
                    f"net_new={net_new_count}, skipped={candidate_count - net_new_count}"
                )

                if net_new_count > 0:
                    # conflict_id is a freshly minted UUID, so there is never a
                    # match — the original .whenNotMatchedInsertAll() is
                    # equivalent to a plain append here.
                    write_ops.append(net_new_df, QUEUE_TABLE)
                    print(f"[on_match=flag_for_review] {net_new_count} conflicts written to queue")
                else:
                    print("[on_match=flag_for_review] All conflicts already queued, nothing written")
            else:
                print("[on_match=flag_for_review] No differences found between source and target")

            print("[on_match=flag_for_review] No automatic updates — all changes pending steward review")

    finally:
        spark.sql(f"DROP TABLE IF EXISTS {tmp_matched}")
        spark.sql(f"DROP TABLE IF EXISTS {tmp_master}")


handle_match(entity_reference_config, matched, target_master, RUN_ID, NOW)

# COMMAND ----------

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 3: HANDLE INSERTS  (on_new_record)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Skip any ref_lakefusion_ids already in the pending queue.
pending_keys = (
    spark.read.table(QUEUE_TABLE)
    .filter(F.col("status") == "PENDING")
    .select("ref_lakefusion_id")
    .distinct()
)

# Anti-join on ref_lakefusion_id — both master and queue are keyed by it.
new_rows = (
    source
    .join(target_master.select("ref_lakefusion_id"), on="ref_lakefusion_id", how="left_anti")
    .join(pending_keys, on="ref_lakefusion_id", how="left_anti")
)


def handle_insert(config, new_rows_df, run_id, now):
    strategy = config["on_new_record"]

    # ✅ Materialize into a temp Delta table BEFORE any MERGE touches the master
    #    (prevents lazy re-evaluation of the UDF)
    temp_table = f"_tmp_new_rows_{run_id}"
    (
        new_rows_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(temp_table)
    )

    materialized_df = spark.table(temp_table)
    row_count = materialized_df.count()

    if row_count == 0:
        print("[on_new_record] No new rows to process")
        spark.sql(f"DROP TABLE IF EXISTS {temp_table}")
        return

    try:
        if strategy == "auto_insert":
            # Brand-new ref_lakefusion_ids — INSERT into master + audit v1.
            # Provenance/steward fields baked in by _scd2_insert_new.
            _scd2_insert_new(
                materialized_df,
                list(entity_attributes),
                action_type="JOB_MERGE",
            )
            print(f"[on_new_record=auto_insert] Inserted {row_count} rows (master + audit v1)")

        elif strategy == "flag_for_review":
            pending_df = (
                materialized_df
                .withColumn(
                    "source_value",
                    F.to_json(F.struct([F.col(c) for c in entity_attributes])),
                )
                .withColumn("conflict_id",      F.expr("uuid()"))
                .withColumn("conflict_type",    F.lit("PENDING_INSERT"))
                .withColumn("entity_key_value", F.col(merge_key).cast("string"))
                .withColumn("field_name",       F.lit(None).cast("string"))
                .withColumn("rdm_value",        F.lit(None).cast("string"))
                .withColumn("edited_by",        F.lit(None).cast("string"))
                .withColumn("status",           F.lit("PENDING"))
                .withColumn("resolved_by",      F.lit(None).cast("string"))
                .withColumn("resolved_at",      F.lit(None).cast("timestamp"))
                .withColumn("created_at",       F.lit(now).cast("timestamp"))
                .select(
                    "conflict_id", "conflict_type",
                    "ref_lakefusion_id", "entity_key_value",
                    "field_name", "rdm_value", "source_value",
                    "edited_by", "status",
                    "resolved_by", "resolved_at", "created_at",
                )
            )
            # conflict_id is a freshly minted UUID, so there is never a match —
            # the original .whenNotMatchedInsertAll() is equivalent to append.
            write_ops.append(pending_df, QUEUE_TABLE)
            print(
                f"[on_new_record=flag_for_review] {row_count} new rows queued, "
                f"nothing inserted to data/meta"
            )

    finally:
        spark.sql(f"DROP TABLE IF EXISTS {temp_table}")


handle_insert(entity_reference_config, new_rows, RUN_ID, NOW)

# COMMAND ----------

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 4: HANDLE DELETES  (on_no_match)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# rdm_only: rows in RDM that no longer appear in source.
# Join on business merge_key (source doesn't carry ref_lakefusion_id),
# but the resulting DF inherits ref_lakefusion_id from target_data.
rdm_only = target_master.join(source.select(merge_key), on=merge_key, how="left_anti")


def handle_delete(config, rdm_only_df, run_id, now):
    """
    keep_in_rdm → no action
    delete      → DELETE from master, append tombstone to audit
    soft_delete → same as delete, just a different action_type
    """
    strategy = config["on_no_match"]

    if strategy == "keep_in_rdm":
        print("[on_no_match=keep_in_rdm] RDM-only rows preserved (no action)")
        return

    # ✅ Materialize to temp Delta (consistent with handle_match/handle_insert)
    tmp_delete = f"_tmp_rdm_only_{run_id}"
    (
        rdm_only_df.select("ref_lakefusion_id", merge_key).write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tmp_delete)
    )
    delete_keys_df = spark.table(tmp_delete)
    delete_count = delete_keys_df.count()

    if delete_count == 0:
        print(f"[on_no_match={strategy}] No RDM-only rows found, nothing to process")
        spark.sql(f"DROP TABLE IF EXISTS {tmp_delete}")
        return

    try:
        # delta_audit only used for READS in _scd2_expire; writes go through write_ops.
        delta_audit = DeltaTable.forName(spark, AUDIT_TABLE)

        if strategy == "delete":
            _scd2_expire(delta_audit, delete_keys_df, action_type="DELETE_EXPIRED")
            print(
                f"[on_no_match=delete] Deleted {delete_count} master rows "
                f"(audit history preserved + tombstone appended)"
            )

        elif strategy == "soft_delete":
            _scd2_expire(delta_audit, delete_keys_df, action_type="SOFT_DELETE")
            print(
                f"[on_no_match=soft_delete] Deleted {delete_count} master rows "
                f"as SOFT_DELETE (audit history preserved)"
            )

    finally:
        # ✅ Always clean up the temp table regardless of success/failure
        spark.sql(f"DROP TABLE IF EXISTS {tmp_delete}")


handle_delete(entity_reference_config, rdm_only, RUN_ID, NOW)
logger_instance.shutdown()