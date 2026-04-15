# Databricks notebook source
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

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
entity = dbutils.widgets.get("entity")
experiment_id = dbutils.widgets.get("experiment_id")
entity_attributes_datatype = dbutils.widgets.get("entity_attributes_datatype")
entity_attributes = dbutils.widgets.get("entity_attributes")
entity_reference_config = dbutils.widgets.get("entity_reference_config")
attributes_mapping = dbutils.widgets.get("attributes_mapping")
primary_key = dbutils.widgets.get("primary_key")

# COMMAND ----------

entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
entity_attributes_datatype = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes_datatype", debugValue=entity_attributes_datatype)
entity_attributes = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="entity_attributes", debugValue=entity_attributes)
entity_reference_config = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="entity_reference_config", debugValue=entity_reference_config)
merge_key = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="primary_key", debugValue=primary_key)
attributes_mapping = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="attributes_mapping", debugValue=attributes_mapping)

# COMMAND ----------

entity_attributes_datatype = json.loads(entity_attributes_datatype)
attributes_mapping = json.loads(attributes_mapping)

# COMMAND ----------

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE REFERENCES — now split into DATA + META
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SOURCE_TABLE = [list(entry.keys())[0] for entry in attributes_mapping][0]
DATA_TABLE   = f"{catalog_name}.gold.{entity}_reference_prod"
META_TABLE   = f"{catalog_name}.gold.{entity}_reference_meta_prod"
QUEUE_TABLE  = f"{catalog_name}.gold.{entity}_reference_conflict_queue_prod"
LOG_TABLE    = f"{catalog_name}.gold.{entity}_reference_steward_edit_log_prod"

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime
import uuid

RUN_ID = f"RUN_1"
NOW = datetime.utcnow()

# ── RESOLVE COLUMN MAPPING ────────────────────────────────────────────────────
source_col_map = next(
    (entry[SOURCE_TABLE] for entry in attributes_mapping if SOURCE_TABLE in entry), None
)
if source_col_map is None:
    raise ValueError(f"No column mapping found for source table: {SOURCE_TABLE}")

source_merge_key = source_col_map.get(merge_key)
if source_merge_key is None:
    raise ValueError(
        f"merge_key '{merge_key}' not found in attributes_mapping for {SOURCE_TABLE}"
    )

entity_attributes = list(source_col_map.keys())

print(f"Source merge key : {source_merge_key} → {merge_key}")
print(f"Entity attributes: {entity_attributes}")

# ── STEP 1: LOAD, RENAME & DEDUPLICATE SOURCE ─────────────────────────────────
source = (
    spark.table(SOURCE_TABLE)
    .filter(F.col(source_merge_key).isNotNull())
    .select([F.col(src).alias(tgt) for tgt, src in source_col_map.items()])
)

target_data = spark.table(DATA_TABLE)
target_meta = spark.table(META_TABLE)

# COMMAND ----------


matched = source.join(target_data.select(merge_key), on=merge_key, how="inner")

def handle_match(config, matched_df, target_data_df, target_meta_df, run_id, now):
    strategy = config["on_match"]
    delta_data = DeltaTable.forName(spark, DATA_TABLE)
    biz_cols = [c for c in entity_attributes if c != merge_key]

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ✅ Materialize all inputs BEFORE any MERGE touches DATA/META tables
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    tmp_matched   = f"_tmp_matched_{run_id}"
    tmp_data      = f"_tmp_target_data_{run_id}"
    tmp_meta      = f"_tmp_target_meta_{run_id}"

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ✅ Helper to strip column defaults before writing to temp Delta table
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    def write_temp_snapshot(df, table_name):
        # Cast each column to its own type — this drops any DEFAULT metadata
        clean_df = df.select([
            F.col(c).cast(df.schema[c].dataType).alias(c)
            for c in df.columns
        ])
        (
            clean_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("delta.columnMapping.mode", "none")  # avoid mapping conflicts
            .saveAsTable(table_name)
        )
        return spark.table(table_name)

    matched_snap     = write_temp_snapshot(matched_df,     tmp_matched)
    target_data_snap = write_temp_snapshot(target_data_df, tmp_data)
    target_meta_snap = write_temp_snapshot(target_meta_df, tmp_meta)
    # ── Log snapshot counts for debugging ─────────────────────────────────
    matched_count = matched_snap.count()
    locked_count  = target_meta_snap.filter(F.col("steward_locked") == True).count()
    print(f"[handle_match] matched_rows={matched_count}, locked_rows_in_meta={locked_count}")

    try:
        # ── Locked rows from META snapshot ────────────────────────────────
        locked_df = target_meta_snap.filter(F.col("steward_locked") == True).select(merge_key)

        if strategy == "source_wins":
            update_set = {c: f"src.{c}" for c in biz_cols}
            (
                delta_data.alias("tgt")
                .merge(matched_snap.alias("src"), f"tgt.{merge_key} = src.{merge_key}")
                .whenMatchedUpdate(set=update_set)
                .execute()
            )
            print(f"[on_match=source_wins] Updated {matched_count} rows in DATA table")

        elif strategy == "steward_wins":
            unlocked   = matched_snap.join(locked_df, on=merge_key, how="left_anti")
            update_set = {c: f"src.{c}" for c in biz_cols}
            (
                delta_data.alias("tgt")
                .merge(unlocked.alias("src"), f"tgt.{merge_key} = src.{merge_key}")
                .whenMatchedUpdate(set=update_set)
                .execute()
            )
            skipped = matched_count - unlocked.count()
            print(f"[on_match=steward_wins] Updated {unlocked.count()} rows, skipped {skipped} locked rows")

        elif strategy == "flag_for_review":
            # ── Already-pending conflicts ──────────────────────────────────
            already_queued_df = (
                spark.table(QUEUE_TABLE)
                .filter(
                    (F.col("conflict_type") == "UPDATE_CONFLICT")
                    & (F.col("status").isin("PENDING", "KEEP_RDM"))
                )
                .select("entity_key_value", "field_name")
            )

            # ── Locked matched rows ────────────────────────────────────────
            locked_matched = matched_snap.join(locked_df, on=merge_key, how="inner")
            locked_matched_count = locked_matched.count()
            print(f"[on_match=flag_for_review] locked_matched_rows={locked_matched_count}")

            # ── Compare source vs DATA snapshot, pull edited_by from META ──
            compare = (
                locked_matched.alias("src")
                .join(target_data_snap.alias("tgt_d"), on=merge_key, how="inner")
                .join(target_meta_snap.alias("tgt_m"), on=merge_key, how="inner")
            )

            # ── Unpivot each business column into conflict rows ────────────
            unpivoted = None
            for col_name in biz_cols:
                col_df = compare.select(
                    F.col(f"src.{merge_key}").alias("entity_key_value"),
                    F.lit(col_name).alias("field_name"),
                    F.col(f"tgt_d.{col_name}").cast("string").alias("rdm_value"),
                    F.col(f"src.{col_name}").cast("string").alias("source_value"),
                    F.col("tgt_m.steward_edited_by").alias("edited_by"),
                ).filter(
                    (F.col("source_value").isNotNull())
                    & (F.col("rdm_value").isNotNull())
                    & (F.col("source_value") != F.col("rdm_value"))
                )
                unpivoted = col_df if unpivoted is None else unpivoted.union(col_df)

            if unpivoted is not None:
                candidate_df = (
                    unpivoted
                    .withColumn("conflict_id", F.expr("uuid()"))
                    .withColumn("run_id", F.lit(run_id))
                    .withColumn("conflict_type", F.lit("UPDATE_CONFLICT"))
                    .withColumn("status", F.lit("PENDING"))
                    .withColumn("resolved_by", F.lit(None).cast("string"))
                    .withColumn("resolved_at", F.lit(None).cast("timestamp"))
                    .withColumn("created_at", F.lit(now).cast("timestamp"))
                    .select(
                        "conflict_id", "run_id", "conflict_type",
                        "entity_key_value", "field_name", "rdm_value",
                        "source_value", "edited_by", "status",
                        "resolved_by", "resolved_at", "created_at",
                    )
                )

                net_new_df = candidate_df.join(
                    already_queued_df,
                    on=["entity_key_value", "field_name"],
                    how="left_anti",
                )

                candidate_count = candidate_df.count()
                net_new_count   = net_new_df.count()
                print(
                    f"[on_match=flag_for_review] candidates={candidate_count}, "
                    f"net_new={net_new_count}, skipped={candidate_count - net_new_count}"
                )

                if net_new_count > 0:
                    net_new_df.write.format("delta").mode("append").saveAsTable(QUEUE_TABLE)
                    print(f"[on_match=flag_for_review] {net_new_count} conflicts written to queue")
                else:
                    print("[on_match=flag_for_review] All conflicts already queued")
            else:
                print("[on_match=flag_for_review] No locked rows with conflicts found")

            # ── Update NON-locked rows in DATA table ──────────────────────
            unlocked   = matched_snap.join(locked_df, on=merge_key, how="left_anti")
            update_set = {c: f"src.{c}" for c in biz_cols}
            (
                delta_data.alias("tgt")
                .merge(unlocked.alias("src"), f"tgt.{merge_key} = src.{merge_key}")
                .whenMatchedUpdate(set=update_set)
                .execute()
            )
            print(f"[on_match=flag_for_review] {unlocked.count()} unlocked rows updated in DATA table")

    finally:
        # ✅ Always clean up temp tables
        spark.sql(f"DROP TABLE IF EXISTS {tmp_matched}")
        spark.sql(f"DROP TABLE IF EXISTS {tmp_data}")
        spark.sql(f"DROP TABLE IF EXISTS {tmp_meta}")

handle_match(entity_reference_config, matched, target_data, target_meta, RUN_ID, NOW)

# COMMAND ----------

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 3: HANDLE INSERTS  (on_new_record)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

NOW = datetime.utcnow()

pending_keys = (
    spark.table(QUEUE_TABLE)
    .filter(F.col("status") == "PENDING")
    .select("entity_key_value")
    .distinct()
)

new_rows = source.join(
    target_data.select(merge_key), on=merge_key, how="left_anti"
).join(
    pending_keys,
    source[merge_key] == pending_keys["entity_key_value"],
    how="left_anti",
)

def handle_insert(config, new_rows_df, run_id, now):
    strategy = config["on_new_record"]
    delta_data = DeltaTable.forName(spark, DATA_TABLE)
    delta_meta = DeltaTable.forName(spark, META_TABLE)

    # ✅ Materialize into a temp Delta table BEFORE any MERGE touches DATA_TABLE
    temp_table = f"_tmp_new_rows_{run_id}"
    (
        new_rows_df
        .write
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
            (
                delta_data.alias("tgt")
                .merge(materialized_df.alias("src"), f"tgt.{merge_key} = src.{merge_key}")
                .whenNotMatchedInsertAll()
                .execute()
            )

            meta_insert_df = (
                materialized_df.select(merge_key)   # ✅ reads from temp Delta, not recomputed
                .withColumn("is_active", F.lit(True))
                .withColumn("steward_locked", F.lit(False))
                .withColumn("steward_edited_cols", F.lit(None).cast("array<string>"))
                .withColumn("steward_edited_by", F.lit(None).cast("string"))
                .withColumn("steward_edited_at", F.lit(None).cast("timestamp"))
                .withColumn("created_at", F.lit(now).cast("timestamp"))
                .withColumn("updated_at", F.lit(now).cast("timestamp"))
                .withColumn("action_type", F.lit("JOB_MERGE"))
            )
            (
                delta_meta.alias("tgt")
                .merge(meta_insert_df.alias("src"), f"tgt.{merge_key} = src.{merge_key}")
                .whenNotMatchedInsertAll()
                .execute()
            )
            print(f"[on_new_record=auto_insert] Inserted {row_count} rows into DATA + META")

        elif strategy == "flag_for_review":
            pending_df = (
                materialized_df                     # ✅ stable snapshot
                .withColumn(
                    "source_value",
                    F.to_json(F.struct([F.col(c) for c in entity_attributes])),
                )
                .withColumn("conflict_id", F.expr("uuid()"))
                .withColumn("run_id", F.lit(run_id))
                .withColumn("conflict_type", F.lit("PENDING_INSERT"))
                .withColumn("entity_key_value", F.col(merge_key).cast("string"))
                .withColumn("field_name", F.lit(None).cast("string"))
                .withColumn("rdm_value", F.lit(None).cast("string"))
                .withColumn("edited_by", F.lit(None).cast("string"))
                .withColumn("status", F.lit("PENDING"))
                .withColumn("resolved_by", F.lit(None).cast("string"))
                .withColumn("resolved_at", F.lit(None).cast("timestamp"))
                .withColumn("created_at", F.lit(now).cast("timestamp"))
                .select(
                    "conflict_id", "run_id", "conflict_type",
                    "entity_key_value", "field_name", "rdm_value",
                    "source_value", "edited_by", "status",
                    "resolved_by", "resolved_at", "created_at",
                )
            )
            pending_df.write.format("delta").mode("append").saveAsTable(QUEUE_TABLE)
            print(
                f"[on_new_record=flag_for_review] {row_count} new rows queued, "
                f"nothing inserted to data/meta"
            )

    finally:
        # ✅ Always clean up the temp table regardless of success/failure
        spark.sql(f"DROP TABLE IF EXISTS {temp_table}")
handle_insert(entity_reference_config, new_rows, RUN_ID, NOW)

# COMMAND ----------

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 4: HANDLE DELETES  (on_no_match)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

rdm_only = target_data.join(source.select(merge_key), on=merge_key, how="left_anti")


def handle_delete(config, rdm_only_df, now):
    """
    keep_in_rdm → no action
    delete      → hard-delete from BOTH data + meta
    soft_delete → set is_active=False in META table only
    """
    strategy = config["on_no_match"]

    if strategy == "keep_in_rdm":
        print("[on_no_match=keep_in_rdm] RDM-only rows preserved (no action)")

    elif strategy == "delete":
        keys = [r[merge_key] for r in rdm_only_df.select(merge_key).collect()]
        key_list = ",".join(f"'{k}'" for k in keys)

        spark.sql(f"DELETE FROM {DATA_TABLE} WHERE {merge_key} IN ({key_list})")
        spark.sql(f"DELETE FROM {META_TABLE} WHERE {merge_key} IN ({key_list})")
        print(
            f"[on_no_match=delete] Hard-deleted {len(keys)} rows from DATA + META. "
            f"CAUTION: may break mappings."
        )

    elif strategy == "soft_delete":
        keys = [r[merge_key] for r in rdm_only_df.select(merge_key).collect()]
        key_list = ",".join(f"'{k}'" for k in keys)

        # Soft-delete only touches the META table
        spark.sql(f"""
            UPDATE {META_TABLE}
            SET is_active  = false,
                updated_at = '{now}'
            WHERE {merge_key} IN ({key_list})
        """)
        print(f"[on_no_match=soft_delete] Marked {len(keys)} rows inactive in META table")


handle_delete(entity_reference_config, rdm_only, NOW)