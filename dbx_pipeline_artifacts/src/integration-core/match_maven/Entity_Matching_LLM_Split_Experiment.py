# Databricks notebook source
# ======================================================================
# Entity_Matching_LLM_Split_Experiment  (match_maven)  — REFACTOR 1 splitter
# ======================================================================
# Step 1 of the 3-step LLM flow: Split -> Shards (for_each) -> Assemble.
#
# Runs ONCE (after the deterministic step) when shard_count > 1. It scans the
# source table, builds the deterministic _det_filter, applies the SAME eligibility
# filter the worker used to apply per-shard, and writes the eligible rows ONCE into
# a single input table partitioned by shard_index:
#
#     {source_table}_llm_input  PARTITIONED BY (shard_index)
#       columns: {source_id_key}, attributes_combined, search_results, shard_index
#       shard_index = abs(hash({source_id_key})) % shard_count
#
# Each Entity_Matching_LLM_Experiment shard then reads ONLY its own partition
# (WHERE shard_index = i), so the full-source scan + deterministic join happen once
# here instead of once per shard.
#
# The eligibility filter (_det_filter build + filter_query) is kept in sync with the
# worker's shard_count==1 path (Entity_Matching_LLM_Experiment.py STEP 4). KEEP IN SYNC.
# ======================================================================

%pip install databricks-vectorsearch
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Imports
import json
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Widget Definitions
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("is_single_source", "false", "Is Single Source (Golden Dedup)")
dbutils.widgets.text("shard_count", "1", "Total shards (1 disables sharding)")
dbutils.widgets.text("shard_min_records", "50000", "Min eligible rows to shard (below -> single process)")

catalog_name = dbutils.widgets.get("catalog_name")
entity = dbutils.widgets.get("entity")
experiment_id = dbutils.widgets.get("experiment_id")
is_single_source = dbutils.widgets.get("is_single_source")
shard_count = int(dbutils.widgets.get("shard_count") or "1")
shard_min_records = int(dbutils.widgets.get("shard_min_records") or "50000")

# COMMAND ----------

# DBTITLE 1,Get Task Values (Override Widget Values)
entity = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="entity",
    debugValue=entity,
)
is_single_source = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="is_single_source",
    debugValue=is_single_source,
)
catalog_name = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="catalog_name",
    debugValue=catalog_name,
)

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

if isinstance(is_single_source, str):
    is_single_source = is_single_source.lower() == "true"
else:
    is_single_source = bool(is_single_source)

# COMMAND ----------

# DBTITLE 1,Set Keys Based on Source Type
master_id_key = "lakefusion_id"
merged_desc_column = "attributes_combined"

if is_single_source:
    source_id_key = "lakefusion_id"
    mode_name = "GOLDEN DEDUP (Single Source)"
else:
    source_id_key = "surrogate_key"
    mode_name = "NORMAL DEDUP (Multi Source)"

# COMMAND ----------

# DBTITLE 1,Define Table Names
unified_table = f"{catalog_name}.silver.{entity}_unified"
unified_dedup_table = f"{catalog_name}.silver.{entity}_unified_deduplicate"
unified_deteministic_table = f"{catalog_name}.silver.{entity}_unified_deterministic"
unified_deteministic_dedup_table = f"{catalog_name}.silver.{entity}_unified_deterministic_deduplicate"

if experiment_id:
    unified_table += f"_{experiment_id}"
    unified_dedup_table += f"_{experiment_id}"
    unified_deteministic_dedup_table += f"_{experiment_id}"
    unified_deteministic_table += f"_{experiment_id}"

if is_single_source:
    source_table = unified_dedup_table
    deterministic_table = unified_deteministic_dedup_table
else:
    source_table = unified_table
    deterministic_table = unified_deteministic_table

input_table = f"{source_table}_llm_input"

deteministic_unified_table_exists = spark.catalog.tableExists(deterministic_table)

logger.info("=" * 80)
logger.info(f"LLM SPLIT - EXPERIMENT MODE ({mode_name})")
logger.info("=" * 80)
logger.info(f"Source Table: {source_table}")
logger.info(f"Deterministic Table: {deterministic_table} (exists={deteministic_unified_table_exists})")
logger.info(f"Input Table: {input_table}")
logger.info(f"Shard Count: {shard_count}")

# COMMAND ----------

# DBTITLE 1,Build _det_filter (deterministic eligibility) — KEEP IN SYNC WITH WORKER STEP 4
MATCH_ACTIONS    = ["MATCH"]      # actions that win the record (skip whole record from LLM)
NO_MATCH_ACTIONS = ["NO_MATCH"]   # actions that eliminate one candidate from LLM input

if deteministic_unified_table_exists:
    df_det = spark.table(deterministic_table)

    df_det_agg = (
        df_det
        .groupBy(source_id_key)
        .agg(
            F.max(
                F.when(F.col("exploded_result.match").isin(MATCH_ACTIONS), F.lit(1))
                 .otherwise(F.lit(0))
            ).alias("_has_pos_int"),
            F.collect_set(
                F.when(
                    F.col("exploded_result.match").isin(NO_MATCH_ACTIONS),
                    F.col("exploded_result.lakefusion_id"),
                )
            ).alias("_no_match_ids_with_null"),
        )
        .withColumn("has_positive_match", F.col("_has_pos_int") == 1)
        .withColumn(
            "no_match_ids",
            F.expr("filter(_no_match_ids_with_null, x -> x is not null)"),
        )
        .drop("_has_pos_int", "_no_match_ids_with_null")
    )

    df_source_for_filter = spark.table(source_table).select(source_id_key, "search_results")

    df_det_filter = (df_source_for_filter.join(df_det_agg, on=source_id_key, how="inner")
    .withColumn(
        "_parsed",
        F.from_json(F.col("search_results"), "array<array<string>>"),
    )
    .withColumn(
        "_remaining",
        F.expr("filter(_parsed, x -> NOT array_contains(no_match_ids, x[1]))"),
    )
    .withColumn(
        "filtered_search_results",
        F.when(F.col("has_positive_match"), F.lit(None).cast("string"))
         .when(F.size(F.col("no_match_ids")) == 0, F.col("search_results"))
         .when(F.size(F.col("_remaining")) == 0, F.lit(None).cast("string"))
         .otherwise(F.to_json(F.col("_remaining"))),
    )
    .select(source_id_key, "has_positive_match", "filtered_search_results"))

    df_det_filter.createOrReplaceTempView("_det_filter")
    logger.info("Built _det_filter temp view")
else:
    empty_schema = StructType([
        StructField(source_id_key,             StringType(), True),
        StructField("has_positive_match",      BooleanType(), True),
        StructField("filtered_search_results", StringType(), True),
    ])
    spark.createDataFrame([], empty_schema).createOrReplaceTempView("_det_filter")
    logger.info("No deterministic table — created empty _det_filter temp view as fallback")

# COMMAND ----------

# DBTITLE 1,Eligibility filter_query — KEEP IN SYNC WITH WORKER STEP 4
if is_single_source:
    if deteministic_unified_table_exists:
        filter_query = f"""
          select
            u.{source_id_key},
            u.{merged_desc_column},
            COALESCE(d.filtered_search_results, u.search_results) as search_results
          from
            {source_table} u
            left join _det_filter d
              on u.{master_id_key} = d.{master_id_key}
          where
            (d.has_positive_match IS NULL or d.has_positive_match = false)
            and (d.has_positive_match IS NULL or d.filtered_search_results IS NOT NULL)
            and u.search_results IS NOT NULL
            and u.search_results != ''
            and (u.scoring_results IS NULL or u.scoring_results = '')
        """
    else:
        filter_query = f"""
          select
            u.{source_id_key},
            u.{merged_desc_column},
            u.search_results
          from
            {source_table} u
          where
            u.search_results IS NOT NULL
            and u.search_results != ''
            and (u.scoring_results IS NULL or u.scoring_results = '')
        """
else:
    if deteministic_unified_table_exists:
        filter_query = f"""
          select
            u.{source_id_key},
            u.{merged_desc_column},
            COALESCE(d.filtered_search_results, u.search_results) as search_results
          from
            {source_table} u
            left join _det_filter d
              on u.{source_id_key} = d.{source_id_key}
          where
            (d.has_positive_match IS NULL or d.has_positive_match = false)
            and (d.has_positive_match IS NULL or d.filtered_search_results IS NOT NULL)
            and u.record_status = 'ACTIVE'
            and u.search_results IS NOT NULL
            and u.search_results != ''
            and (u.scoring_results IS NULL or u.scoring_results = '')
        """
    else:
        filter_query = f"""
          select
            u.{source_id_key},
            u.{merged_desc_column},
            u.search_results
          from
            {source_table} u
          where
            u.record_status = 'ACTIVE'
            and u.search_results IS NOT NULL
            and u.search_results != ''
            and (u.scoring_results IS NULL or u.scoring_results = '')
        """

# COMMAND ----------

# DBTITLE 1,Write partitioned input table
# Materialize the eligible rows ONCE into a staging table (the deterministic join +
# eligibility filter run here only — no re-scan, no .cache() on serverless), then decide
# how many shards to ACTUALLY use: below shard_min_records collapse everything into a
# single shard (single process), at/above it keep the configured equal hash-split. Each
# shard reads WHERE shard_index = i; the configured shard_count tasks still run, but the
# extra ones get an empty partition (the shard worker no-ops on those).
staging_table = f"{input_table}_staging"
(
    spark.sql(f"with uf as ({filter_query}) select * from uf")
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(staging_table)
)

input_count = spark.table(staging_table).count()
effective_shard_count = shard_count if input_count >= shard_min_records else 1
logger.info(
    f"Eligible rows: {input_count:,} | min-records floor: {shard_min_records:,} | "
    f"configured shards: {shard_count} -> effective shards: {effective_shard_count} "
    f"({'single process' if effective_shard_count == 1 else 'equal hash-split'})"
)

(
    spark.table(staging_table)
    .withColumn("shard_index", F.expr(f"abs(hash({source_id_key})) % {effective_shard_count}"))
    .write.format("delta")
    .mode("overwrite")
    .partitionBy("shard_index")
    .option("overwriteSchema", "true")
    .saveAsTable(input_table)
)
spark.sql(f"DROP TABLE IF EXISTS {staging_table}")

logger.info(f"Wrote {input_count:,} eligible rows to {input_table} across {effective_shard_count} shard partition(s)")

# Per-shard distribution for observability.
dist = (
    spark.table(input_table)
    .groupBy("shard_index").count().orderBy("shard_index")
    .collect()
)
for row in dist:
    logger.info(f"  shard {row['shard_index']}: {row['count']:,} rows")

# COMMAND ----------

dbutils.jobs.taskValues.set("llm_input_table", input_table)
dbutils.jobs.taskValues.set("llm_input_count", int(input_count))
dbutils.jobs.taskValues.set("shard_count", shard_count)

logger_instance.shutdown()
dbutils.notebook.exit(json.dumps({
    "status": "split_complete",
    "input_table": input_table,
    "input_count": int(input_count),
    "shard_count": shard_count,
}))
