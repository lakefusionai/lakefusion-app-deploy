# Databricks notebook source
# MAGIC %pip install --upgrade "databricks-sdk>=0.89.0"
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

# Materialize the production {rel}_edges_prod Delta table for each configured
# relationship. Plain pyspark — all logic inline. No executor framework.
#
# Pipeline per relationship:
#   1. For each configured dataset mapping:
#        a. read source dataset
#        b. resolve source_pk_column  → src_node_id via source-entity crosswalk
#        c. resolve target_pk_column → dst_node_id via target-entity crosswalk
#        d. failed resolutions → DLQ buffer
#   2. Union all mappings; self-reference filter
#   3. Cardinality enforcement (1:1, 1:N, N:1):
#        Layer A  — pick highest-priority source's claim
#        Layer B  — tie-broken by most-recent start_date
#        Layer C  — still tied → DLQ (steward review)
#   4. Survivorship collapse:
#        per (src, dst, edge_type) keep priority-asc + start_date-desc winner;
#        contributing_sources = collect_set(_source_system)
#   5. Ensure {rel}_edges_prod table exists with mandatory + built-in + attr cols
#   6. MERGE into table honouring merge_config (on_match / on_new_record)
#   7. Reconcile orphans: rows whose endpoint lakefusion_id no longer exists in
#      {entity}_master_prod → status='inactive' (skip rows with origin='manual')
#   8. Write DLQ rows to {rel}_edges_dlq_prod

import json
from datetime import datetime

# COMMAND ----------

dbutils.widgets.text("integration_hub_relationship_id", "", "IntHub Relationship Task ID")
dbutils.widgets.text("entity_id", "", "Driving Entity ID")
dbutils.widgets.text("experiment_id", "prod", "Experiment ID")
dbutils.widgets.text("catalog_name", "", "catalog name")

integration_hub_relationship_id = dbutils.widgets.get("integration_hub_relationship_id")
entity_id = dbutils.widgets.get("entity_id")
experiment_id = dbutils.widgets.get("experiment_id") or "prod"
catalog_name = dbutils.widgets.get("catalog_name")

# COMMAND ----------

config_json = dbutils.jobs.taskValues.get(
    taskKey="Parse_Relationship_JSON",
    key="relationship_config",
    debugValue="{}",
)
relationship_config = json.loads(config_json) if config_json else {}

if not relationship_config.get("relationships"):
    raise ValueError(
        "relationship_config missing or empty. Did Parse_Relationship_JSON run?"
    )

merge_config = relationship_config.get("merge_config") or {
    "on_match": "source_wins",
    "on_new_record": "auto_insert",
}
logger.info(
    f"Processing {len(relationship_config['relationships'])} relationship(s); "
    f"merge_config={merge_config}"
)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StringType, DateType, TimestampType, BooleanType,
    IntegerType, LongType, FloatType, DoubleType, ShortType, ByteType, ArrayType,
)
from delta.tables import DeltaTable

# Attribute type → Spark cast type
_ATTR_SPARK_TYPE = {
    "STRING":    StringType(),
    "BIGINT":    LongType(),
    "INT":       IntegerType(),
    "SMALLINT":  ShortType(),
    "TINYINT":   ByteType(),
    "DOUBLE":    DoubleType(),
    "FLOAT":     FloatType(),
    "BOOLEAN":   BooleanType(),
    "DATE":      DateType(),
    "TIMESTAMP": TimestampType(),
}

# Attribute type → SQL DDL fragment
_ATTR_DDL_TYPE = {
    "STRING": "STRING", "BIGINT": "BIGINT", "INT": "INT",
    "SMALLINT": "SMALLINT", "TINYINT": "TINYINT",
    "DOUBLE": "DOUBLE", "FLOAT": "FLOAT", "BOOLEAN": "BOOLEAN",
    "DATE": "DATE", "TIMESTAMP": "TIMESTAMP",
}

# COMMAND ----------

def _slug(name: str) -> str:
    """Lowercase + snake_case slug matching the entity-pipeline naming."""
    return (name or "").lower().replace(" ", "_")


def _crosswalk_table(entity_name: str) -> str:
    return f"{catalog_name}.gold.{_slug(entity_name)}_crosswalk_{experiment_id}"


def _master_table(entity_name: str) -> str:
    return f"{catalog_name}.gold.{_slug(entity_name)}_master_{experiment_id}"


def _edges_table(relationship_name: str) -> str:
    return f"{catalog_name}.gold.{relationship_name}_edges_{experiment_id}"


def _dlq_table(relationship_name: str) -> str:
    return f"{catalog_name}.gold.{relationship_name}_edges_dlq_{experiment_id}"


def _ensure_edges_table(relationship_name: str, attributes: list) -> str:
    """Create {rel}_edges_{exp} Delta table on first run.

    Schema = mandatory LakeGraph cols + built-in temporal cols + designer-defined
    attribute cols + audit cols. CDF enabled.
    """
    table = _edges_table(relationship_name)

    attr_cols_sql = ""
    for a in attributes:
        ddl_type = _ATTR_DDL_TYPE.get(a["data_type"], "STRING")
        attr_cols_sql += f"  `{a['name']}` {ddl_type},\n"

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
          src_node_id     STRING NOT NULL,
          dst_node_id     STRING NOT NULL,
          src_node_type   STRING NOT NULL,
          dst_node_type   STRING NOT NULL,
          edge_type       STRING NOT NULL,
          start_date      DATE,
          end_date        DATE,
          status          STRING,
          contributing_sources ARRAY<STRING>,
          origin          STRING,
        {attr_cols_sql}  created_at      TIMESTAMP,
          updated_at      TIMESTAMP,
          created_by      STRING,
          updated_by      STRING
        )
        USING DELTA
        PARTITIONED BY (edge_type)
        TBLPROPERTIES (delta.enableChangeDataFeed = true)
        """
    )

    # LakeGraph reads the edges table via Change Data Feed. New tables get CDF
    # from the CREATE above, but tables created before this (or with CDF off) need
    # it enabled. Idempotent: only ALTER when it's actually off, so we don't churn
    # a new table version on every run.
    try:
        _props = (
            spark.sql(f"DESCRIBE DETAIL {table}").select("properties").collect()[0][0]
            or {}
        )
        if str(_props.get("delta.enableChangeDataFeed", "false")).lower() != "true":
            spark.sql(
                f"ALTER TABLE {table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
            )
            logger.info(f"  [cdf] enabled Change Data Feed on {table}")
    except Exception as _e:  # noqa: BLE001
        logger.warning(f"  [cdf] could not verify/enable CDF on {table}: {_e}")

    dlq = _dlq_table(relationship_name)
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {dlq} (
          src_node_id    STRING,
          dst_node_id    STRING,
          edge_type      STRING,
          failure_reason STRING,
          failure_detail STRING,
          src_source_pk  STRING,
          dst_source_pk  STRING,
          source_system  STRING,
          raw_payload    STRING,
          ingested_at    TIMESTAMP
        )
        USING DELTA
        TBLPROPERTIES (delta.enableChangeDataFeed = true)
        """
    )
    return table


def _written_rows(table_name: str, metric_keys) -> int:
    """Row counts from the latest Delta commit's operationMetrics.

    This is a metadata-only read (one tiny history row) — it does NOT trigger a
    DataFrame action, so it never materializes the pipeline DAG. That's the whole
    point: on million-row runs, calling .count()/.collect() on the in-flight
    frames would re-execute every join + window just to log a number. Reading the
    commit metrics after the write gives the real count for free.
    """
    try:
        metrics = (
            spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1")
            .select("operationMetrics")
            .collect()[0][0]
            or {}
        )
        return sum(int(metrics.get(k, 0) or 0) for k in metric_keys)
    except Exception:
        return 0

# COMMAND ----------

def _process_relationship(rel_entry: dict, merge_cfg: dict) -> dict:
    relationship = rel_entry["relationship"]
    attributes   = rel_entry.get("attributes", [])
    mappings     = rel_entry.get("mappings", [])

    rel_name  = relationship["name"]
    edge_type = rel_name
    src_type  = relationship.get("source_entity_name") or ""
    dst_type  = relationship.get("target_entity_name") or ""
    cardinality = relationship.get("cardinality", "N:N")

    table = _ensure_edges_table(rel_name, attributes)
    dlq_table = _dlq_table(rel_name)

    # DLQ = "current unresolved problems for this relationship", not append-only
    # history. Clear this relationship's slice up-front so re-runs don't duplicate
    # failure rows (Delta time-travel still keeps earlier runs).
    spark.sql(f"DELETE FROM {dlq_table} WHERE edge_type = '{rel_name}'")

    src_xw = _crosswalk_table(src_type)
    dst_xw = _crosswalk_table(dst_type)

    # Config-only logging — no DataFrame actions, so it's free even at scale.
    active_mappings = sum(1 for m in mappings if m.get("is_active", True))
    logger.info(f"\n=== relationship '{rel_name}'  {src_type} → {dst_type}  ({cardinality}) ===")
    logger.info(f"  edges:      {table}")
    logger.info(f"  dlq:        {dlq_table}")
    logger.info(f"  crosswalks: {src_xw} | {dst_xw}")
    logger.info(f"  mappings:   {active_mappings} active / {len(mappings)} total")

    # Crosswalks are identical for every mapping of this relationship — read once.
    src_cw_df = spark.read.table(src_xw).select(
        F.col("source_record_id").alias("_src_lookup"),
        F.col("lakefusion_id").alias("src_node_id"),
    )
    dst_cw_df = spark.read.table(dst_xw).select(
        F.col("source_record_id").alias("_dst_lookup"),
        F.col("lakefusion_id").alias("dst_node_id"),
    )

    all_resolved = None
    dlq_rows     = None

    for mapping in mappings:
        if not mapping.get("is_active", True):
            continue
        dataset_path  = mapping["dataset_path"]
        # source_system_tag + priority are server-managed (tag from dataset.name,
        # priority from row position). Defensive fallbacks for older snapshots.
        source_system = (
            mapping.get("source_system_tag")
            or f"DATASET_{mapping.get('dataset_id', 'UNKNOWN')}"
        )
        priority   = int(mapping.get("priority") or 100)
        src_pk     = mapping["source_pk_column"]
        tgt_pk     = mapping["target_pk_column"]
        start_col  = mapping.get("start_date_column")
        end_col    = mapping.get("end_date_column")
        status_col = mapping.get("status_column")
        attr_maps  = mapping.get("attribute_mappings", []) or []

        logger.info(f"  -> {dataset_path}  [src={source_system}, priority={priority}]")
        src_df = spark.read.table(dataset_path)

        joined = (
            src_df.alias("s")
            .join(src_cw_df, F.col(f"s.{src_pk}") == F.col("_src_lookup"), "left")
            .join(dst_cw_df, F.col(f"s.{tgt_pk}") == F.col("_dst_lookup"), "left")
        )

        # ─── Unresolved endpoints → DLQ buffer (lazy; written once at the end) ─
        failed = joined.filter(
            F.col("src_node_id").isNull() | F.col("dst_node_id").isNull()
        ).select(
            F.col("src_node_id"),
            F.col("dst_node_id"),
            F.lit(edge_type).alias("edge_type"),
            F.when(F.col("src_node_id").isNull(), F.lit("src_not_resolved"))
             .when(F.col("dst_node_id").isNull(), F.lit("dst_not_resolved"))
             .otherwise(F.lit("unknown")).alias("failure_reason"),
            F.lit(None).cast("string").alias("failure_detail"),
            F.col(f"s.{src_pk}").cast("string").alias("src_source_pk"),
            F.col(f"s.{tgt_pk}").cast("string").alias("dst_source_pk"),
            F.lit(source_system).alias("source_system"),
            F.to_json(F.struct("s.*")).alias("raw_payload"),
            F.current_timestamp().alias("ingested_at"),
        )
        dlq_rows = failed if dlq_rows is None else dlq_rows.unionByName(failed)

        # ─── Resolved rows (self-reference excluded per PRD) ────────────
        resolved = joined.filter(
            F.col("src_node_id").isNotNull()
            & F.col("dst_node_id").isNotNull()
            & (F.col("src_node_id") != F.col("dst_node_id"))
        )

        select_cols = [
            F.col("src_node_id"),
            F.col("dst_node_id"),
            F.lit(src_type).alias("src_node_type"),
            F.lit(dst_type).alias("dst_node_type"),
            F.lit(edge_type).alias("edge_type"),
            (F.to_date(F.col(f"s.{start_col}")) if start_col else F.lit(None).cast("date")).alias("start_date"),
            (F.to_date(F.col(f"s.{end_col}")) if end_col else F.lit(None).cast("date")).alias("end_date"),
            (F.col(f"s.{status_col}").cast("string") if status_col else F.lit("active")).alias("status"),
            F.array(F.lit(source_system)).alias("contributing_sources"),
            F.lit(f"source_{source_system}").alias("origin"),
        ]
        for am in attr_maps:
            attr_def = next((a for a in attributes if a["id"] == am["attribute_id"]), None)
            if not attr_def or not am.get("column_name"):
                continue
            spark_type = _ATTR_SPARK_TYPE.get(attr_def["data_type"], StringType())
            select_cols.append(
                F.col(f"s.{am['column_name']}").cast(spark_type).alias(attr_def["name"])
            )

        resolved = (
            resolved.select(*select_cols)
            .withColumn("_priority", F.lit(priority))
            .withColumn("_source_system", F.lit(source_system))
        )
        all_resolved = (
            resolved if all_resolved is None
            else all_resolved.unionByName(resolved, allowMissingColumns=True)
        )

    # ─── Cardinality enforcement (pure transformations, no actions) ─────
    # Collapse so each constrained side keeps a single winning endpoint value;
    # losing claims go to the DLQ. Windowed first() picks the winner (priority
    # asc, recency desc); rows sharing the winning value survive into
    # survivorship — so this is correct without any count/emptiness check.
    cardinality_dlq = None

    def _enforce_uniqueness(df, key_col, other_col, reason):
        order = Window.partitionBy(key_col).orderBy(
            F.col("_priority").asc(), F.col("start_date").desc_nulls_last()
        )
        tagged = df.withColumn("_win_other", F.first(other_col).over(order))
        winners = tagged.filter(F.col(other_col) == F.col("_win_other")).drop("_win_other")
        losers = tagged.filter(F.col(other_col) != F.col("_win_other")).select(
            F.col("src_node_id"),
            F.col("dst_node_id"),
            F.lit(edge_type).alias("edge_type"),
            F.lit(reason).alias("failure_reason"),
            F.lit("Lost to higher-priority / more-recent claim").alias("failure_detail"),
            F.lit(None).cast("string").alias("src_source_pk"),
            F.lit(None).cast("string").alias("dst_source_pk"),
            F.col("_source_system").alias("source_system"),
            F.lit(None).cast("string").alias("raw_payload"),
            F.current_timestamp().alias("ingested_at"),
        )
        return winners, losers

    winners = all_resolved
    conflict_reason = f"cardinality_{cardinality.replace(':', '_')}_violation"
    if winners is not None and cardinality in ("1:1", "N:1"):
        winners, losers = _enforce_uniqueness(winners, "src_node_id", "dst_node_id", conflict_reason)
        cardinality_dlq = losers if cardinality_dlq is None else cardinality_dlq.unionByName(losers)
    if winners is not None and cardinality in ("1:1", "1:N"):
        winners, losers = _enforce_uniqueness(winners, "dst_node_id", "src_node_id", conflict_reason)
        cardinality_dlq = losers if cardinality_dlq is None else cardinality_dlq.unionByName(losers)

    merged_rows = 0
    if winners is not None:
        # ─── Survivorship collapse: one row per (src, dst, edge_type) ───
        grp = Window.partitionBy("src_node_id", "dst_node_id", "edge_type")
        final = (
            winners
            .withColumn("contributing_sources", F.collect_set("_source_system").over(grp))
            .withColumn(
                "_winner_rank",
                F.row_number().over(
                    grp.orderBy(F.col("_priority").asc(), F.col("start_date").desc_nulls_last())
                ),
            )
            .filter(F.col("_winner_rank") == 1)
            .drop("_winner_rank", "_priority", "_source_system")
            .withColumn("created_at", F.current_timestamp())
            .withColumn("updated_at", F.current_timestamp())
            .withColumn("created_by", F.lit("system_pipeline"))
            .withColumn("updated_by", F.lit("system_pipeline"))
        )

        # ─── MERGE (single action). An empty source is a cheap no-op, so we
        #     skip the count-gate entirely — no .count() to materialize. ──
        target = DeltaTable.forName(spark, table)
        on_match  = merge_cfg.get("on_match",  "source_wins")
        on_insert = merge_cfg.get("on_new_record", "auto_insert")
        merge_builder = target.alias("t").merge(
            final.alias("s"),
            "t.src_node_id = s.src_node_id AND t.dst_node_id = s.dst_node_id "
            "AND t.edge_type = s.edge_type",
        )
        if on_match == "source_wins":
            merge_builder = merge_builder.whenMatchedUpdateAll()
        elif on_match == "steward_wins":
            # Keep steward-overridden rows; source wins elsewhere.
            set_map = {c: f"s.{c}" for c in final.columns}
            merge_builder = merge_builder.whenMatchedUpdate(condition="t.origin != 'manual'", set=set_map)
        # else "flag_for_review" → don't update existing rows.
        if on_insert == "auto_insert":
            merge_builder = merge_builder.whenNotMatchedInsertAll()
        merge_builder.execute()
        merged_rows = _written_rows(table, ("numTargetRowsInserted", "numTargetRowsUpdated"))
        logger.info(f"  [{rel_name}] merged: {merged_rows} edge row(s) inserted/updated")
    else:
        logger.info(f"  [{rel_name}] no active mappings — nothing to merge")

    # ─── Reconcile orphans: endpoints whose master_id no longer exists ──
    # lakefusion_id is a non-null PK on the master tables, so NOT IN is safe.
    src_master = _master_table(src_type)
    dst_master = _master_table(dst_type)
    spark.sql(
        f"""
        UPDATE {table}
        SET status = 'inactive',
            updated_at = current_timestamp(),
            updated_by = 'auto_orphan'
        WHERE edge_type = '{edge_type}'
          AND (origin IS NULL OR origin != 'manual')
          AND (
            src_node_id NOT IN (SELECT lakefusion_id FROM {src_master})
            OR dst_node_id NOT IN (SELECT lakefusion_id FROM {dst_master})
          )
        """
    )
    orphaned = _written_rows(table, ("numUpdatedRows",))
    if orphaned:
        logger.info(f"  [{rel_name}] reconciled {orphaned} orphan edge(s) → inactive")

    # ─── DLQ write: union both buffers, append once. Count from commit. ─
    dlq_all = dlq_rows
    if cardinality_dlq is not None:
        dlq_all = cardinality_dlq if dlq_all is None else dlq_all.unionByName(cardinality_dlq)
    dlq_count = 0
    if dlq_all is not None:
        dlq_all.write.format("delta").mode("append").saveAsTable(dlq_table)
        dlq_count = _written_rows(dlq_table, ("numOutputRows",))
        logger.info(f"  [{rel_name}] dlq: {dlq_count} unresolved/conflict row(s)")

    return {
        "relationship": rel_name,
        "promoted": merged_rows,
        "dlq": dlq_count,
    }

# COMMAND ----------

summary = {"relationships": [], "rows_promoted": 0, "rows_dlq": 0}
for rel_entry in relationship_config["relationships"]:
    # Errors propagate so the Databricks job fails loud and the IntHub UI
    # shows the run as failed. No swallowing.
    rel_summary = _process_relationship(rel_entry, merge_config)
    summary["relationships"].append(rel_summary)
    summary["rows_promoted"] += rel_summary.get("promoted", 0)
    summary["rows_dlq"]      += rel_summary.get("dlq", 0)

logger.info("\n=== Materialize summary ===")
logger.info(json.dumps(summary, indent=2))

# COMMAND ----------

dbutils.jobs.taskValues.set(key="materialize_summary", value=json.dumps(summary))
