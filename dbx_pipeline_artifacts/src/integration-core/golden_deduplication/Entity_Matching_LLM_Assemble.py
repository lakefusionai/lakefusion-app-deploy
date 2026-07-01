# Databricks notebook source
# ======================================================================
# Entity_Matching_LLM_Assemble  (golden_deduplication)  — REFACTOR 1 fan-in
# ======================================================================
# Runs ONCE after all Entity_Matching_LLM shard tasks complete (DAG fan-in).
# It (1) verifies every shard finished, (2) unions the per-shard scored tables,
# then runs STEP 7-12 — explode → rank-filter → classify → single MERGE into the
# unified dedup table → rebuild processed → optimize → cleanup. This is the ONLY
# task that writes the unified table, so there are no concurrent writes and no
# partial merges.
#
# STEP 7-11 are kept byte-for-byte in sync with Entity_Matching_LLM.py (the worker
# shard_count==1 path). Only STEP 12 differs: it drops the shard tables/markers
# instead of retaining a single temp table. Keep these in sync with the worker.
# ======================================================================

%pip install databricks-vectorsearch

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import builtins
import json
import time
import traceback
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("experiment_id", "", "Match Maven Experiment")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "lakefusion catalog name")
dbutils.widgets.text("config_thresholds", "", "Match Thresholds Config")
dbutils.widgets.text("max_potential_matches", "3", "Max Potential Matches")
dbutils.widgets.text("shard_count", "1", "Total shards to assemble")

experiment_id = dbutils.widgets.get("experiment_id")
entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=dbutils.widgets.get("entity"))
catalog_name = dbutils.widgets.get("catalog_name")
config_thresholds = json.loads(
    dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "config_thresholds",
                                debugValue=dbutils.widgets.get("config_thresholds"))
)
max_potential_matches = int(
    dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "max_potential_matches",
                                debugValue=dbutils.widgets.get("max_potential_matches"))
)
shard_count = int(dbutils.widgets.get("shard_count") or "1")

merged_desc_column = "attributes_combined"
master_id_key = "lakefusion_id"

merge_min, merge_max = config_thresholds.get("merge", [0.9, 1.0])
matches_min, matches_max = config_thresholds.get("matches", [0.7, 0.89])

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

# Real table names (NOT shard-scoped — the assembler is the single writer).
unified_dedup_table = f"{catalog_name}.silver.{entity}_unified_deduplicate"
processed_unified_dedup_table = f"{catalog_name}.silver.{entity}_processed_unified_deduplicate"
master_table = f"{catalog_name}.gold.{entity}_master"
unified_deteministic_table = f"{catalog_name}.silver.{entity}_unified_deterministic_deduplicate"
if experiment_id:
    unified_dedup_table += f"_{experiment_id}"
    processed_unified_dedup_table += f"_{experiment_id}"
    master_table += f"_{experiment_id}"
    unified_deteministic_table += f"_{experiment_id}"

temp_llm_table_base = f"{unified_dedup_table}_llm_temp_v1"
shard_tables = [f"{temp_llm_table_base}_shard_{i}" for i in range(shard_count)]
deteministic_unified_table_exists = spark.catalog.tableExists(unified_deteministic_table)

# COMMAND ----------

# ── COMPLETENESS GATE — every shard's _done marker AND table must exist ─────────
missing = []
for i, st in enumerate(shard_tables):
    if not spark.catalog.tableExists(f"{st}_done"):
        missing.append(f"shard {i}: missing marker {st}_done")
    elif not spark.catalog.tableExists(st):
        missing.append(f"shard {i}: missing table {st}")
if missing:
    raise RuntimeError("Assembly aborted — incomplete shards:\n  " + "\n  ".join(missing))
logger.info(f"All {shard_count} shards present. Assembling: {shard_tables}")

# COMMAND ----------

# ── UNION shard tables -> bind temp_llm_table to the assembled set ──────────────
# functools.reduce is shadowed here by pyspark.sql.functions.reduce (from `import *`), so
# accumulate the union explicitly instead of relying on the bare `reduce` name.
df_assembled = None
for st in shard_tables:
    df_shard = spark.table(st)
    df_assembled = df_shard if df_assembled is None else df_assembled.unionByName(df_shard, allowMissingColumns=True)
df_assembled.createOrReplaceTempView("_llm_assembled")
temp_llm_table = "_llm_assembled"
logger.info(f"Assembled rows: {df_assembled.count():,}")

# COMMAND ----------

# union_clause — verbatim from Entity_Matching_LLM.py STEP 5 (deterministic positive
# matches folded into the explode so they land in processed_unified_dedup).
union_clause = ""
if deteministic_unified_table_exists:
    union_clause = f"""
    UNION
    SELECT
        {master_id_key},
        FIRST(attributes_combined) AS attributes_combined,
        FIRST(search_results) AS search_results,
        CONCAT(
            '[',
            CONCAT_WS(',', COLLECT_LIST(json_item)),
            ']'
        ) AS combined_column,
        FIRST(exploded_result) AS exploded_result
    FROM (
        SELECT
            {master_id_key},
            CONCAT(
                '{{"id": "', exploded_result.id,
                '", "score": ', exploded_result.score,
                ', "reason": "', exploded_result.reason,
                '", "lakefusion_id": "', exploded_result.lakefusion_id,
                '"}}'
            ) AS json_item,

            attributes_combined,
            search_results,

            NAMED_STRUCT(
                'id', exploded_result.id,
                'score', CAST(exploded_result.score AS DOUBLE),
                'reason', exploded_result.reason,
                'lakefusion_id', exploded_result.lakefusion_id
            ) AS exploded_result
        FROM {unified_deteministic_table}
        WHERE exploded_result.match IN ('MATCH', 'POTENTIAL_MATCH')
    )
    GROUP BY {master_id_key}
    """
else:
    union_clause = ""

# COMMAND ----------

# ============== EXPLODE + JOIN to produce df_llm_results ================

logger.info("\n" + "=" * 80)
logger.info("STEP 7: EXPLODE LLM RESULTS")
logger.info("=" * 80)

df_llm_results = spark.sql(f"""
WITH m AS (
    SELECT {master_id_key}, {merged_desc_column}
    FROM {master_table}
),
er AS (
    SELECT
        u.{master_id_key},
        u.{merged_desc_column} AS attributes_combined,
        u.search_results,
        get_json_object(u.scoring_results.result, '$.results') AS combined_column,
        exploded AS exploded_result
    FROM {temp_llm_table} u
    LATERAL VIEW EXPLODE(FROM_JSON(
        get_json_object(u.scoring_results.result, '$.results'),
        'ARRAY<STRUCT<
            id STRING,
            score DOUBLE,
            reason STRING,
            lakefusion_id STRING
        >>'
    )) AS exploded
    WHERE u.scoring_results.result IS NOT NULL
    {union_clause}
)
SELECT
    er.{master_id_key} AS query_{master_id_key},
    er.exploded_result.lakefusion_id AS match_{master_id_key},
    er.exploded_result,
    er.exploded_result.`id` AS exploded_result_id,
    er.combined_column AS scoring_results_json
FROM er
LEFT JOIN m ON er.exploded_result.lakefusion_id = m.{master_id_key}
WHERE m.{master_id_key} IS NOT NULL
""")

logger.info("Exploded raw LLM output into df_llm_results for downstream classification")

# COMMAND ----------

logger.info("\n" + "=" * 80)
logger.info("STEP 7.5: FILTER RESULTS")
logger.info("=" * 80)

window_spec = Window.partitionBy("query_" + master_id_key).orderBy(col("exploded_result.score").desc())

df_filtered = df_llm_results \
    .filter(col("match_" + master_id_key).isNotNull()) \
    .withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") <= max_potential_matches) \
    .drop("rank")

df_llm_results = df_filtered

# COMMAND ----------

logger.info("\n" + "=" * 80)
logger.info("STEP 8: APPLY MATCH CLASSIFICATION")
logger.info("=" * 80)

df_classified = df_llm_results.withColumn(
    "match_status",
    when(col("exploded_result.score").isNull(), "NO_MATCH")
    .when(col("exploded_result.score").between(merge_min, merge_max), "MATCH")
    .when(col("exploded_result.score").between(matches_min, matches_max), "POTENTIAL_MATCH")
    .otherwise("NO_MATCH")
)

df_with_match_status = df_classified.withColumn(
    "exploded_result",
    struct(
        col("exploded_result.id").alias("id"),
        col("match_status").alias("match"),
        col("exploded_result.score").alias("score"),
        col("exploded_result.reason").alias("reason"),
        col("exploded_result.lakefusion_id").alias("lakefusion_id")
    )
).select(
    col("query_" + master_id_key),
    col("match_" + master_id_key),
    "exploded_result",
    "exploded_result_id",
    "scoring_results_json",
    "match_status"
)

# COMMAND ----------

logger.info("\n" + "=" * 80)
logger.info("STEP 9: UPDATE UNIFIED DEDUP TABLE WITH SCORING RESULTS")
logger.info("=" * 80)

df_scoring_updates = df_with_match_status.groupBy("query_" + master_id_key).agg(
    collect_list(
        struct(
            col("exploded_result.id").alias("id"),
            col("exploded_result.match").alias("match"),
            col("exploded_result.score").alias("score"),
            col("exploded_result.reason").alias("reason"),
            col("exploded_result.lakefusion_id").alias("lakefusion_id")
        )
    ).alias("results_array")
).withColumn(
    "scoring_results",
    to_json(col("results_array"))
).select(
    col("query_" + master_id_key).alias(master_id_key),
    "scoring_results"
)

unified_delta = DeltaTable.forName(spark, unified_dedup_table)

unified_delta.alias("target").merge(
    source=df_scoring_updates.alias("source"),
    condition=f"target.{master_id_key} = source.{master_id_key}"
).whenMatchedUpdate(
    set={"scoring_results": "source.scoring_results"}
).execute()

logger.info(f"Unified table updated with scoring_results (single assembler merge)")

# COMMAND ----------

logger.info("\n" + "=" * 80)
logger.info("STEP 10: REBUILD PROCESSED UNIFIED DEDUP TABLE")
logger.info("=" * 80)

rebuild_query = f"""
select
  u.{master_id_key} as query_{master_id_key},
  exploded.lakefusion_id as match_{master_id_key},
  exploded as exploded_result,
  exploded.id as exploded_result_id
from {unified_dedup_table} u
lateral view explode(
  from_json(u.scoring_results, 'array<struct<
    id:string,
    match:string,
    score:double,
    reason:string,
    lakefusion_id:string
  >>')
) as exploded
where u.scoring_results IS NOT NULL
and u.scoring_results != ''
"""

df_processed_unified_dedup = spark.sql(rebuild_query)

logger.info(f"  Building processed_unified_dedup from all records with scoring_results")

df_processed_unified_dedup.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(processed_unified_dedup_table)

# COMMAND ----------

logger.info("\n" + "=" * 80)
logger.info("STEP 11: OPTIMIZE PROCESSED UNIFIED TABLE")
logger.info("=" * 80)

spark.sql(f"""ALTER TABLE {processed_unified_dedup_table} CLUSTER BY (query_{master_id_key}, match_{master_id_key})""")

spark.sql(f"OPTIMIZE {processed_unified_dedup_table}")

# COMMAND ----------

# ============== STEP 12: CLEANUP — drop shard tables + markers =========
logger.info("\n" + "=" * 80)
logger.info("STEP 12: CLEANUP")
logger.info("=" * 80)

for st in shard_tables:
    spark.sql(f"DROP TABLE IF EXISTS {st}")
    spark.sql(f"DROP TABLE IF EXISTS {st}_done")

# Drop the splitter's partitioned input table (created by Entity_Matching_LLM_Split).
spark.sql(f"DROP TABLE IF EXISTS {unified_dedup_table}_llm_input")

dbutils.jobs.taskValues.set("llm_matching_complete", True)
logger.info("Assembly complete; shard tables and markers cleaned up.")
logger_instance.shutdown()
