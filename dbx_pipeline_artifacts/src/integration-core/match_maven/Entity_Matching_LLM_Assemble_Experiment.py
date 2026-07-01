# Databricks notebook source
# ======================================================================
# Entity_Matching_LLM_Assemble_Experiment  (match_maven)  — REFACTOR 1 fan-in
# ======================================================================
# Step 3 of the 3-step LLM flow: Split -> Shard (for_each) -> Assemble.
# Runs ONCE after all Entity_Matching_LLM_Shard_Experiment tasks complete (DAG fan-in).
# It (1) verifies every shard finished, (2) unions the per-shard scored tables into a
# real Delta table, then runs:
#   STEP 2/2.5      — validate + warm up the LLM endpoint
#   STEP 2.75 + 5   — model-param validation + prompt/ai_query build (via llm_scoring_common)
#   STEP 6.5        — validate bad results, RETRY via ai_query (llm_max_retries),
#                     log errors to the unified error table, null out bad rows
#   STEP 7-12       — explode → rank-filter → (no threshold) → single MERGE into the
#                     source table → rebuild processed → optimize → cleanup
# This is the ONLY task that writes the source table AND the unified error table, so
# there are no concurrent writes (the shards never touch the error table — that is what
# caused the ConcurrentAppendException) and no partial merges.
#
# The prompt / ai_query / model-param / validation logic lives in utils/llm_scoring_common
# (shared with the shard worker). STEP 7-11 are kept in sync with the single-shard
# Entity_Matching_LLM_Experiment notebook, including the is_single_source branches. STEP 12
# additionally drops the splitter input, shard tables/markers and retry temp tables.
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

dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "lakefusion catalog name")
dbutils.widgets.text("max_potential_matches", "5", "Max Potential Matches")
dbutils.widgets.text("is_single_source", "false", "Is Single Source (Golden Dedup)")
dbutils.widgets.text("shard_count", "1", "Total shards to assemble")
# LLM / prompt params — the assembler reconstructs the exact ai_query to RETRY bad results
# (STEP 6.5), so it needs the same inputs the worker uses (read from the same task values).
dbutils.widgets.text("attributes", "[]", "Attributes JSON")
dbutils.widgets.text("llm_endpoint", "", "LLM Endpoint")
dbutils.widgets.text("llm_model_source", "", "LLM Model Source")
dbutils.widgets.text("additional_instructions", "", "Additional Instructions")
dbutils.widgets.text("base_prompt", "", "Base Prompt")
dbutils.widgets.text("llm_temperature", "0.0", "LLM Temperature")
dbutils.widgets.dropdown("reasoning_effort", "low", ["low", "medium", "high"], "LLM reasoning_effort")
dbutils.widgets.text("llm_max_tokens", "4096", "LLM max_tokens")
dbutils.widgets.text("llm_max_retries", "2", "LLM Max Retries")
dbutils.widgets.dropdown("llm_retry_on_partial", "true", ["true", "false"], "Retry LLM on partial candidate scores")

experiment_id = dbutils.widgets.get("experiment_id")
entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=dbutils.widgets.get("entity"))
catalog_name = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "catalog_name",
                                           debugValue=dbutils.widgets.get("catalog_name"))
max_potential_matches = int(
    dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "max_potential_matches",
                                debugValue=dbutils.widgets.get("max_potential_matches"))
)
is_single_source = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="is_single_source",
    debugValue=dbutils.widgets.get("is_single_source"),
)
shard_count = int(dbutils.widgets.get("shard_count") or "1")

# LLM / prompt params (mirror Entity_Matching_LLM_Experiment.py reads) — used by STEP 6.5 retry.
attributes = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "match_attributes",
                                         debugValue=dbutils.widgets.get("attributes"))
llm_temperature = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "llm_temperature",
                                              debugValue=dbutils.widgets.get("llm_temperature"))
llm_temperature = float(llm_temperature) if llm_temperature is not None else None
reasoning_effort = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "reasoning_effort",
                                               debugValue=dbutils.widgets.get("reasoning_effort")) or "low"
llm_max_tokens_raw = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "llm_max_tokens",
                                                 debugValue=dbutils.widgets.get("llm_max_tokens"))
llm_max_tokens = int(llm_max_tokens_raw) if llm_max_tokens_raw is not None else None
base_prompt = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "base_prompt",
                                          debugValue=dbutils.widgets.get("base_prompt"))
additional_instructions = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "additional_instructions",
                                                      debugValue=dbutils.widgets.get("additional_instructions"))
attributes = json.loads(attributes) if isinstance(attributes, str) and attributes else []
llm_model_source = dbutils.widgets.get("llm_model_source")
llm_endpoint = dbutils.widgets.get("llm_endpoint")
llm_max_retries = int(dbutils.widgets.get("llm_max_retries") or "2")
llm_retry_on_partial = (dbutils.widgets.get("llm_retry_on_partial") or "true").lower() == "true"

# run_id for the unified-error-table MERGE key (assembler is the single writer).
try:
    run_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().toString()
except Exception:
    import uuid
    run_id = str(uuid.uuid4())

# Normalize is_single_source the same way the worker does.
if isinstance(is_single_source, str):
    is_single_source = is_single_source.lower() == "true"
else:
    is_single_source = bool(is_single_source)

merged_desc_column = "attributes_combined"
master_id_key = "lakefusion_id"
if is_single_source:
    source_id_key = "lakefusion_id"
    mode_name = "GOLDEN DEDUP (Single Source)"
else:
    source_id_key = "surrogate_key"
    mode_name = "NORMAL DEDUP (Multi Source)"

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

# MAGIC %run ../../utils/model_serving

# COMMAND ----------

# MAGIC %run ../../utils/llm_scoring_common

# COMMAND ----------

# Resolve the LLM endpoint the same way the worker does (Endpoints_Mapping wins, else the
# model-source serving endpoint). The retry in STEP 6.5 calls ai_query against this endpoint.
try:
    llm_endpoint_from_mapping = dbutils.jobs.taskValues.get(
        taskKey="Endpoints_Mapping", key="llm_model_endpoint", debugValue=None
    )
    if llm_endpoint_from_mapping:
        llm_endpoint = llm_endpoint_from_mapping
except Exception as e:
    logger.info(f"Endpoints_Mapping task not available, falling back to model source endpoints: {e}")

if not llm_endpoint:
    if llm_model_source == "databricks_foundation":
        llm_endpoint = dbutils.jobs.taskValues.get(
            taskKey="LLM_Foundational_Serving_Endpoint", key="llm_model_endpoint", debugValue=llm_endpoint
        )
    else:
        llm_endpoint = dbutils.jobs.taskValues.get(
            taskKey="LLM_Hugging_Face_Serving_Endpoint", key="llm_model_endpoint", debugValue=llm_endpoint
        )

# COMMAND ----------

# Real table names (NOT shard-scoped — the assembler is the single writer).
unified_table = f"{catalog_name}.silver.{entity}_unified"
unified_dedup_table = f"{catalog_name}.silver.{entity}_unified_deduplicate"
processed_unified_table = f"{catalog_name}.silver.{entity}_processed_unified"
processed_unified_dedup_table = f"{catalog_name}.silver.{entity}_processed_unified_deduplicate"
unified_deteministic_table = f"{catalog_name}.silver.{entity}_unified_deterministic"
unified_deteministic_dedup_table = f"{catalog_name}.silver.{entity}_unified_deterministic_deduplicate"
if experiment_id:
    unified_table += f"_{experiment_id}"
    unified_dedup_table += f"_{experiment_id}"
    processed_unified_table += f"_{experiment_id}"
    processed_unified_dedup_table += f"_{experiment_id}"
    unified_deteministic_dedup_table += f"_{experiment_id}"
    unified_deteministic_table += f"_{experiment_id}"

if is_single_source:
    source_table = unified_dedup_table
    processed_table = processed_unified_dedup_table
    deterministic_table = unified_deteministic_dedup_table
else:
    source_table = unified_table
    processed_table = processed_unified_table
    deterministic_table = unified_deteministic_table

deteministic_unified_table_exists = spark.catalog.tableExists(deterministic_table)

temp_llm_table_base = f"{source_table}_llm_temp_v1"
shard_tables = [f"{temp_llm_table_base}_shard_{i}" for i in range(shard_count)]

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
logger.info(f"All {shard_count} shards present ({mode_name}). Assembling: {shard_tables}")

# COMMAND ----------

# ======================================================================================
# STEP 2 / 2.5 / 2.75 / 5 / 6.5 are ported from Entity_Matching_LLM_Experiment.py (the
# shard_count==1 worker path). In sharded mode the workers do ONLY scoring; the assembler
# is the single writer that validates, retries (ai_query), logs errors and nulls bad rows.
# KEEP THESE BLOCKS IN SYNC WITH THE WORKER (same convention as STEP 7-12 below).
# ======================================================================================

# DBTITLE 1,STEP 2: Validate LLM Endpoint
logger.info("\n" + "=" * 80)
logger.info("STEP 2: VALIDATE LLM ENDPOINT")
logger.info("=" * 80)
try:
    wait_until_serving_endpoint_ready(endpoint_name=llm_endpoint)
    logger.info(f"LLM endpoint '{llm_endpoint}' is ready")
except ValueError as e:
    error_msg = f"ERROR: LLM endpoint not ready: {str(e)}"
    logger.error(error_msg)
    logger_instance.shutdown()
    dbutils.notebook.exit(error_msg)

# COMMAND ----------

# DBTITLE 1,STEP 2.5: Warm Up LLM Endpoint
logger.info("\n" + "=" * 80)
logger.info("STEP 2.5: WARM UP LLM ENDPOINT (SCALE-FROM-ZERO HANDLING)")
logger.info("=" * 80)
warmup_query = f"""SELECT ai_query('{llm_endpoint}', 'Say hello and confirm you are ready.')"""
try:
    warm_up_llm_endpoint(
        spark=spark,
        endpoint_name=llm_endpoint,
        warmup_query=warmup_query,
        max_retries=10,
        retry_interval_seconds=60,
        timeout_minutes=10,
    )
    logger.info(f"LLM endpoint '{llm_endpoint}' is warmed up and ready")
except Exception as e:
    error_msg = f"ERROR: LLM endpoint warm-up failed: {str(e)}"
    logger.error(error_msg)
    logger_instance.shutdown()
    dbutils.notebook.exit(error_msg)

# COMMAND ----------

# DBTITLE 1,STEP 2.75 + STEP 5: Build ai_query (shared llm_scoring_common util)
logger.info("\n" + "=" * 80)
logger.info("STEP 2.75 + STEP 5: BUILD ai_query (model params + prompt) via llm_scoring_common")
logger.info("=" * 80)

# Model-parameter validation (STEP 2.75) + prompt/ai_query build (STEP 5) live in the shared
# util so the shard worker and assembler stay in lockstep. `ai_query_select` is the
# `ai_query(...) AS scoring_results` SELECT expression reused by the STEP 6.5 retry query.
model_params_clause = validate_model_params(
    spark, llm_endpoint, llm_temperature, llm_max_tokens, reasoning_effort, logger=logger
)
safe_prompt = build_safe_prompt(entity, attributes, additional_instructions, base_prompt, max_potential_matches)
prompt_concat_parts = build_prompt_concat_parts(safe_prompt, merged_desc_column)
ai_query_select = build_ai_query_select(llm_endpoint, prompt_concat_parts, model_params_clause)

# COMMAND ----------

# ── MATERIALIZE the union into a REAL Delta table ──────────────────────────────
# STEP 6.5's retry MERGEs into this table, so it must be a real table (a temp VIEW
# cannot be a MERGE target). This assembler is the single writer, so no concurrency.
assembled_table = f"{temp_llm_table_base}_assembled"
# Union all shard outputs. NOTE: functools.reduce is shadowed here by
# pyspark.sql.functions.reduce (from `import *`), so accumulate the union explicitly
# instead of relying on the bare `reduce` name.
df_assembled = None
for st in shard_tables:
    df_shard = spark.table(st)
    df_assembled = df_shard if df_assembled is None else df_assembled.unionByName(df_shard, allowMissingColumns=True)
(
    df_assembled.write.format("delta").mode("overwrite")
    .option("mergeSchema", "true").option("overwriteSchema", "true")
    .saveAsTable(assembled_table)
)
temp_llm_table = assembled_table
logger.info(f"Assembled rows materialized to {assembled_table}: {spark.table(assembled_table).count():,}")

# COMMAND ----------

# DBTITLE 1,STEP 6.5: Validate & Retry Bad LLM Results (single writer)
logger.info("\n" + "=" * 80)
logger.info("STEP 6.5: VALIDATE & RETRY BAD LLM RESULTS")
logger.info("=" * 80)

from lakefusion_core_engine.services.unified_error_handler import UnifiedErrorHandler
from pyspark.sql.functions import size as spark_size, sum as spark_sum

# Assembler owns the unified error table now — guarantee it exists even when there are no errors.
UnifiedErrorHandler(spark, source_table).ensure_table()

MAX_LLM_RETRIES = llm_max_retries

# Validation/retry helpers (parsed_count_expr, expected_count_expr, add_unresolved_count,
# is_bad_llm_result, build_error_message_col, build_retry_query_for_keys) come from the shared
# llm_scoring_common util — the same logic the shard worker uses.

# Initial pass: compute parsed_count, expected_count, unresolved_count, is_bad once and persist.
df_initial = spark.table(temp_llm_table) \
    .withColumn("parsed_count", parsed_count_expr()) \
    .withColumn("expected_count", expected_count_expr())
df_initial = add_unresolved_count(df_initial) \
    .withColumn("is_bad", is_bad_llm_result(llm_retry_on_partial))
df_initial.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(temp_llm_table)

for retry_attempt in range(1, MAX_LLM_RETRIES + 1):
    stats = spark.table(temp_llm_table).agg(
        count("*").alias("total"),
        spark_sum(col("is_bad").cast("int")).alias("bad")
    ).collect()[0]
    bad_count, total_rows = (stats["bad"] or 0), stats["total"]

    if bad_count == 0:
        logger.info(f"Retry {retry_attempt}: No bad results found. Validation passed.")
        break

    bad_pct = (bad_count / total_rows * 100) if total_rows > 0 else 0
    logger.warning(
        f"Retry {retry_attempt}/{MAX_LLM_RETRIES}: {bad_count:,} bad results "
        f"({bad_pct:.1f}% of {total_rows:,} total)"
    )

    retry_keys_table = f"{temp_llm_table}_retry_keys_{retry_attempt}"
    retry_results_table = f"{temp_llm_table}_retry_results_{retry_attempt}"

    spark.table(temp_llm_table).filter(col("is_bad")) \
        .select(source_id_key).write.format("delta").mode("overwrite").saveAsTable(retry_keys_table)

    try:
        retry_query = build_retry_query_for_keys(
            temp_llm_table, retry_keys_table, source_id_key, merged_desc_column, ai_query_select
        )
        df_retry = spark.sql(retry_query) \
            .withColumn("parsed_count", parsed_count_expr()) \
            .withColumn("expected_count", expected_count_expr())
        df_retry = add_unresolved_count(df_retry) \
            .withColumn("is_bad", is_bad_llm_result(llm_retry_on_partial)) \
            .withColumn("chunk_id", lit(-retry_attempt))
        df_retry.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(retry_results_table)

        df_retried = spark.table(retry_results_table)

        temp_delta = DeltaTable.forName(spark, temp_llm_table)
        temp_delta.alias("target").merge(
            source=df_retried.alias("source"),
            condition=f"target.{source_id_key} = source.{source_id_key}"
        ).whenMatchedUpdateAll().execute()

        logger.info(f"Retry {retry_attempt}: Re-evaluated {bad_count:,} records")

    except Exception as e:
        logger.error(f"Retry {retry_attempt} FAILED: {str(e)[:500]}")
        logger.error(traceback.format_exc())
        break

# Final quality report — is_bad is already persisted, just aggregate
final_stats = spark.table(temp_llm_table).agg(
    count("*").alias("total"),
    spark_sum(col("is_bad").cast("int")).alias("bad")
).collect()[0]
final_bad_count, final_total = (final_stats["bad"] or 0), final_stats["total"]
final_good_count = final_total - final_bad_count
final_bad_pct = (final_bad_count / final_total * 100) if final_total > 0 else 0

logger.info("\n" + "-" * 60)
logger.info("LLM RESULT QUALITY REPORT")
logger.info("-" * 60)
logger.info(f"  Total records:       {final_total:,}")
logger.info(f"  Good results:        {final_good_count:,} ({100 - final_bad_pct:.1f}%)")
logger.info(f"  Bad results (nulled): {final_bad_count:,} ({final_bad_pct:.1f}%)")
logger.info("-" * 60)

if final_bad_count > 0:
    df_final_bad = spark.table(temp_llm_table).filter(col("is_bad"))

    df_llm_errors = df_final_bad.withColumn("error_message", build_error_message_col())

    unified_error_handler = UnifiedErrorHandler(spark, source_table)
    unified_error_handler.log_errors(
        df_llm_errors.select(
            col(source_id_key).alias("surrogate_key"),
            col("error_message")
        ),
        stage="LLM_SCORING",
        run_id=run_id
    )
    logger.info(f"Logged {final_bad_count:,} LLM errors to unified error table")

    logger.warning(f"Nulling out {final_bad_count:,} bad results after {MAX_LLM_RETRIES} retries")
    temp_delta = DeltaTable.forName(spark, temp_llm_table)
    temp_delta.alias("target").merge(
        source=df_final_bad.select(source_id_key).alias("source"),
        condition=f"target.{source_id_key} = source.{source_id_key}"
    ).whenMatchedUpdate(
        set={"scoring_results": lit(None).cast("struct<result:string,errorMessage:string>")}
    ).execute()

# COMMAND ----------

# DBTITLE 1,STEP 7: Explode & Filter Results
logger.info("\n" + "=" * 80)
logger.info("STEP 7: EXPLODE & FILTER RESULTS")
logger.info("=" * 80)

if is_single_source:
    final_select = f"""
    SELECT
      er.{source_id_key} AS query_lakefusion_id,
      er.exploded_result.{master_id_key} AS match_lakefusion_id,
      er.exploded_result,
      er.exploded_result.`id` AS exploded_result_id,
      er.combined_column AS scoring_results_json
    FROM er
    WHERE er.exploded_result.{master_id_key} IS NOT NULL
    """
else:
    final_select = f"""
    SELECT
      er.{source_id_key},
      er.exploded_result.{master_id_key} AS {master_id_key},
      er.exploded_result,
      er.exploded_result.`id` AS exploded_result_id,
      er.combined_column AS scoring_results_json
    FROM er
    WHERE er.exploded_result.{master_id_key} IS NOT NULL
    """

df_llm_results = spark.sql(f"""
WITH er AS (
    SELECT
        u.{source_id_key},
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
)
{final_select}
""")

logger.info("Exploded raw LLM output into df_llm_results")

# COMMAND ----------

# DBTITLE 1,STEP 7.5: Filter by Rank
logger.info("\n" + "=" * 80)
logger.info("STEP 7.5: FILTER RESULTS BY RANK")
logger.info("=" * 80)

if is_single_source:
    partition_key = "query_lakefusion_id"
    match_key = "match_lakefusion_id"
else:
    partition_key = source_id_key
    match_key = master_id_key

window_spec = Window.partitionBy(partition_key).orderBy(col("exploded_result.score").desc())

df_filtered = df_llm_results \
    .filter(col(match_key).isNotNull()) \
    .withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") <= max_potential_matches) \
    .drop("rank")

df_llm_results = df_filtered

# COMMAND ----------

# DBTITLE 1,STEP 8: Match Classification - SKIPPED
logger.info("\n" + "=" * 80)
logger.info("STEP 8: MATCH CLASSIFICATION - SKIPPED (EXPERIMENT MODE)")
logger.info("=" * 80)
logger.info("Experiment mode: No threshold-based classification applied")
logger.info("   All scores are raw LLM similarity scores for analysis")

# COMMAND ----------

# DBTITLE 1,STEP 9: Update Source Table with Scoring Results
logger.info("\n" + "=" * 80)
logger.info("STEP 9: UPDATE SOURCE TABLE WITH SCORING RESULTS")
logger.info("=" * 80)

agg_key = "query_lakefusion_id" if is_single_source else source_id_key

df_scoring_agg = df_llm_results \
    .groupBy(agg_key) \
    .agg(
        to_json(
            collect_list(
                struct(
                    col("exploded_result.id").alias("id"),
                    col("exploded_result.score").alias("score"),
                    col("exploded_result.reason").alias("reason"),
                    col("exploded_result.lakefusion_id").alias("lakefusion_id")
                )
            )
        ).alias("scoring_results")
    )

det_join_key = "lakefusion_id" if is_single_source else "surrogate_key"
_union_with_det = (
    deteministic_unified_table_exists
    and "deterministic_scoring_results" in spark.read.table(deterministic_table).columns
)
if _union_with_det:
    _score_array_schema = "array<struct<id:string,score:double,reason:string,lakefusion_id:string>>"
    df_det_scoring = (
        spark.read.table(deterministic_table)
        .filter(col("deterministic_scoring_results").isNotNull())
        .select(
            col(det_join_key).alias("_det_key"),
            col("deterministic_scoring_results").alias("_det_scoring"),
        )
    )
    # Coalesce the JSON STRINGS to '[]' before parsing — using F.array() as the
    # empty default produces array<NullType> which doesn't unify with
    # array<struct<...>> in concat, silently dropping rows. Doing the
    # coalesce on strings means from_json always returns a typed array.
    df_scoring_agg = (
        df_scoring_agg
        .join(df_det_scoring, col(agg_key) == col("_det_key"), "fullouter")
        .withColumn(
            "_merged",
            concat(
                from_json(coalesce(col("scoring_results"), lit("[]")), _score_array_schema),
                from_json(coalesce(col("_det_scoring"), lit("[]")), _score_array_schema),
            ),
        )
        .select(
            coalesce(col(agg_key), col("_det_key")).alias(agg_key),
            to_json(col("_merged")).alias("scoring_results"),
        )
    )
    logger.info("Unioned LLM scores with deterministic NO_MATCH scoring entries")


df_scoring_agg.createOrReplaceTempView("scoring_updates")

source_table_key = "lakefusion_id" if is_single_source else source_id_key

update_sql = f"""
MERGE INTO {source_table} AS target
USING scoring_updates AS source
ON target.{source_table_key} = source.{agg_key}
WHEN MATCHED THEN UPDATE SET
    target.scoring_results = source.scoring_results
"""

spark.sql(update_sql)
logger.info(f"Source table updated with scoring_results (single assembler merge)")

# COMMAND ----------

# DBTITLE 1,STEP 10: Rebuild Processed Table
logger.info("\n" + "=" * 80)
logger.info("STEP 10: REBUILD PROCESSED TABLE")
logger.info("=" * 80)

logger.info("  Building processed table from all records with scoring_results")

if is_single_source:
    processed_query = f"""
    select
      u.lakefusion_id as query_lakefusion_id,
      exploded.lakefusion_id as match_lakefusion_id,
      exploded as exploded_result,
      exploded.id as exploded_result_id
    from {source_table} u
    lateral view explode(
      from_json(u.scoring_results, 'array<struct<
        id:string,
        score:double,
        reason:string,
        lakefusion_id:string
      >>')
    ) as exploded
    where u.scoring_results IS NOT NULL
    and u.scoring_results != ''
    """
else:
    processed_query = f"""
    select
      u.surrogate_key,
      exploded.lakefusion_id as lakefusion_id,
      exploded as exploded_result,
      exploded.id as exploded_result_id
    from {source_table} u
    lateral view explode(
      from_json(u.scoring_results, 'array<struct<
        id:string,
        score:double,
        reason:string,
        lakefusion_id:string
      >>')
    ) as exploded
    where u.record_status = 'ACTIVE'
    and u.scoring_results IS NOT NULL
    and u.scoring_results != ''
    """

df_processed = spark.sql(processed_query)

df_processed.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(processed_table)

# COMMAND ----------

# DBTITLE 1,STEP 11: Optimize Processed Table
logger.info("\n" + "=" * 80)
logger.info("STEP 11: OPTIMIZE PROCESSED TABLE")
logger.info("=" * 80)

try:
    # For single-source mode, processed table uses query_lakefusion_id / match_lakefusion_id
    # not the raw source_id_key (lakefusion_id) which doesn't exist in the processed table.
    if is_single_source:
        optimize_col = f"query_{master_id_key}"
    else:
        optimize_col = source_id_key
    spark.sql(f"OPTIMIZE {processed_table} ZORDER BY ({optimize_col})")
    logger.info(f"Optimized processed table by {optimize_col}")
except Exception as e:
    logger.info(f"Optimization skipped: {str(e)}")

# COMMAND ----------

# DBTITLE 1,STEP 12: Cleanup — drop shard tables + markers
logger.info("\n" + "=" * 80)
logger.info("STEP 12: CLEANUP")
logger.info("=" * 80)

# Single owner of cleanup: drop EVERY temp artifact the 3-step pipeline created. Wrapped in
# try/except so a cleanup hiccup never masks the successful merge above.
try:
    # Splitter input (partitioned) + optional marker.
    spark.sql(f"DROP TABLE IF EXISTS {source_table}_llm_input")
    spark.sql(f"DROP TABLE IF EXISTS {source_table}_llm_input_done")

    # Per-shard scored outputs + _done markers.
    for st in shard_tables:
        spark.sql(f"DROP TABLE IF EXISTS {st}")
        spark.sql(f"DROP TABLE IF EXISTS {st}_done")

    # Assembled table + its retry temp tables.
    for i in range(1, llm_max_retries + 1):
        spark.sql(f"DROP TABLE IF EXISTS {assembled_table}_retry_keys_{i}")
        spark.sql(f"DROP TABLE IF EXISTS {assembled_table}_retry_results_{i}")
    spark.sql(f"DROP TABLE IF EXISTS {assembled_table}")

    # Belt-and-suspenders sweep: drop any leftover _llm_input* / _llm_temp_v1* tables for this
    # source (e.g. orphans from a crashed prior assembler run) so none survive a clean assembly.
    _parts = source_table.split(".")
    _schema, _basename = f"{_parts[0]}.{_parts[1]}", _parts[2]
    leftovers = spark.sql(f"SHOW TABLES IN {_schema} LIKE '{_basename}_llm_%'").collect()
    for row in leftovers:
        spark.sql(f"DROP TABLE IF EXISTS {_schema}.{row['tableName']}")
        logger.info(f"  Swept leftover temp table: {_schema}.{row['tableName']}")
except Exception as e:
    logger.warning(f"Temp-table cleanup encountered an issue (non-fatal): {str(e)[:300]}")

dbutils.jobs.taskValues.set("llm_matching_complete", True)
logger.info(f"Assembly complete ({mode_name}); all LLM temp tables cleaned up.")
logger.info(f"Processed table: {processed_table}")

logger_instance.shutdown()
dbutils.notebook.exit(json.dumps({
    "status": "success",
    "mode": mode_name,
    "processed_table": processed_table
}))
