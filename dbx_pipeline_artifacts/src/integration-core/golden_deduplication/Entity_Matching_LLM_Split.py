# Databricks notebook source
# ======================================================================
# Entity_Matching_LLM_Split  (golden_deduplication)  — REFACTOR 1 splitter
# ======================================================================
# Step 1 of the 3-step LLM flow: Split -> Shards (for_each) -> Assemble.
#
# Runs ONCE (after the deterministic step) when shard_count > 1. It scans the
# unified dedup source table, builds the deterministic _det_filter, applies the
# SAME eligibility filter the worker (Entity_Matching_LLM.py STEP 4) applies, and
# writes the eligible rows ONCE into a single input table partitioned by
# shard_index:
#
#     {unified_dedup_table}_llm_input  PARTITIONED BY (shard_index)
#       columns: lakefusion_id, attributes_combined, search_results, shard_index
#       shard_index = abs(hash(lakefusion_id)) % shard_count
#
# Each Entity_Matching_LLM_Shard task then reads ONLY its own partition
# (WHERE shard_index = i), so the full-source scan + deterministic join happen
# once here instead of once per shard.
#
# GOLDEN DEDUP is SINGLE-SOURCE: the source id key is lakefusion_id, there is NO
# record_status='ACTIVE' filter, and the _det_filter MATCH_ACTIONS include both
# MATCH and POTENTIAL_MATCH (verbatim from the golden worker's STEP 4). KEEP the
# _det_filter build + eligibility filter_query in sync with Entity_Matching_LLM.py.
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
dbutils.widgets.text("shard_count", "1", "Total shards (1 disables sharding)")
dbutils.widgets.text("shard_min_records", "50000", "Min eligible rows to shard (below -> single process)")

catalog_name = dbutils.widgets.get("catalog_name")
entity = dbutils.widgets.get("entity")
experiment_id = dbutils.widgets.get("experiment_id")
shard_count = int(dbutils.widgets.get("shard_count") or "1")
shard_min_records = int(dbutils.widgets.get("shard_min_records") or "50000")

# COMMAND ----------

# DBTITLE 1,Get Task Values (Override Widget Values)
entity = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="entity",
    debugValue=entity,
)
catalog_name = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="catalog_name",
    debugValue=catalog_name,
)

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

# DBTITLE 1,Set Keys (Golden Dedup = Single Source)
# GOLDEN DEDUP is single-source: both the source id key and the master id key are
# lakefusion_id, and the merged-description column is attributes_combined. There is
# NO record_status filter in golden dedup (verified against Entity_Matching_LLM.py).
master_id_key = "lakefusion_id"
source_id_key = "lakefusion_id"
merged_desc_column = "attributes_combined"
mode_name = "GOLDEN DEDUP (Single Source)"

# COMMAND ----------

# DBTITLE 1,Define Table Names
# Table naming matches Entity_Matching_LLM.py exactly:
#   unified_dedup_table          = {catalog}.silver.{entity}_unified_deduplicate
#   unified_deteministic_table   = {catalog}.silver.{entity}_unified_deterministic_deduplicate
# integration hub passes experiment_id="prod", which is appended to both.
unified_dedup_table = f"{catalog_name}.silver.{entity}_unified_deduplicate"
unified_deteministic_table = f"{catalog_name}.silver.{entity}_unified_deterministic_deduplicate"

if experiment_id:
    unified_dedup_table += f"_{experiment_id}"
    unified_deteministic_table += f"_{experiment_id}"

source_table = unified_dedup_table
deterministic_table = unified_deteministic_table

input_table = f"{unified_dedup_table}_llm_input"

deteministic_unified_table_exists = spark.catalog.tableExists(deterministic_table)

logger.info("=" * 80)
logger.info(f"LLM SPLIT - {mode_name}")
logger.info("=" * 80)
logger.info(f"Entity: {entity}")
logger.info(f"Experiment: {experiment_id if experiment_id else 'prod'}")
logger.info(f"Source Table: {source_table}")
logger.info(f"Deterministic Table: {deterministic_table} (exists={deteministic_unified_table_exists})")
logger.info(f"Input Table: {input_table}")
logger.info(f"Shard Count: {shard_count}")

# COMMAND ----------

# DBTITLE 1,Build _det_filter (deterministic eligibility) — KEEP IN SYNC WITH WORKER STEP 4
# Verbatim from Entity_Matching_LLM.py STEP 4: golden dedup treats BOTH MATCH and
# POTENTIAL_MATCH as positive (skip-whole-record) actions, and NO_MATCH eliminates a
# single candidate from the LLM input. The deterministic table stores one row per
# (source x matched candidate); aggregate back to per-source shape so the LLM filter
# can join on lakefusion_id.
MATCH_ACTIONS    = ["MATCH", "POTENTIAL_MATCH"]
NO_MATCH_ACTIONS = ["NO_MATCH"]

if deteministic_unified_table_exists:
    df_det = spark.table(deterministic_table)

    df_det_agg = (
        df_det
        .groupBy(master_id_key)
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

    df_source_for_filter = spark.table(source_table).select(master_id_key, "search_results")

    df_det_filter = (
        df_source_for_filter
        .join(df_det_agg, on=master_id_key, how="inner")
        .withColumn("_parsed", F.from_json(F.col("search_results"), "array<array<string>>"))
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
        .select(master_id_key, "has_positive_match", "filtered_search_results")
    )

    df_det_filter.createOrReplaceTempView("_det_filter")
    logger.info("Built _det_filter temp view")
else:
    empty_schema = StructType([
        StructField(master_id_key,             StringType(), True),
        StructField("has_positive_match",      BooleanType(), True),
        StructField("filtered_search_results", StringType(), True),
    ])
    spark.createDataFrame([], empty_schema).createOrReplaceTempView("_det_filter")
    logger.info("No deterministic table — created empty _det_filter temp view as fallback")

# COMMAND ----------

# DBTITLE 1,Eligibility filter_query — KEEP IN SYNC WITH WORKER STEP 4
# Verbatim from Entity_Matching_LLM.py STEP 4. Golden dedup joins on lakefusion_id and
# has NO record_status filter. Select lakefusion_id, attributes_combined, and the
# COALESCE(d.filtered_search_results, u.search_results) search_results for eligible rows.
if deteministic_unified_table_exists:
    filter_query = f"""
      select
        u.{master_id_key},
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
        u.{master_id_key},
        u.{merged_desc_column},
        u.search_results
      from
        {source_table} u
      where
        u.search_results IS NOT NULL
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
