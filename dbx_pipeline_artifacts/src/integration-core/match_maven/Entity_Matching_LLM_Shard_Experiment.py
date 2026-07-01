# Databricks notebook source
# ======================================================================
# Entity_Matching_LLM_Shard_Experiment  (match_maven)  — REFACTOR 1 shard worker
# ======================================================================
# Step 2 of the 3-step LLM flow: Split -> Shard (for_each) -> Assemble.
#
# This notebook does ONLY LLM scoring for ONE shard. It reads its own partition of
# the splitter's input table ({source}_llm_input WHERE shard_index = i), runs the
# chunked ai_query (with a per-chunk retry for transient failures), and writes the
# raw scored rows to {source}_llm_temp_v1_shard_i (+ a _done marker), then exits.
#
# It does NOT validate / retry bad results, log errors, explode, merge, or rebuild —
# all of that is the single-writer assembler's job. Failed chunks (after retries) are
# left out (records stay NULL at the end); shards NEVER touch the unified error table.
#
# Prompt + ai_query + model-param logic lives in utils/llm_scoring_common (shared with
# the assembler). KEEP behavior in sync with that module.
# ======================================================================

%pip install databricks-vectorsearch
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Imports
import builtins
import json
import math
import time
import traceback
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Widget Definitions
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("attributes", "[]", "Attributes JSON")
dbutils.widgets.text("max_potential_matches", "3", "Max Potential Matches")
dbutils.widgets.text("llm_endpoint", "", "LLM Endpoint")
dbutils.widgets.text("llm_model_source", "", "LLM Model Source")
dbutils.widgets.text("additional_instructions", "", "Additional Instructions")
dbutils.widgets.text("base_prompt", "", "Base Prompt")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("is_single_source", "false", "Is Single Source (Golden Dedup)")
dbutils.widgets.text("llm_temperature", "0.0", "LLM Temperature")
dbutils.widgets.dropdown("reasoning_effort", "low", ["low", "medium", "high"], "LLM reasoning_effort")
dbutils.widgets.text("llm_max_tokens", "4096", "LLM max_tokens")
dbutils.widgets.text("chunk_size", "250000", "Records per LLM chunk")
# Sharding: this task handles one shard_index of shard_count.
dbutils.widgets.text("shard_index", "0", "Shard index handled by this task")
dbutils.widgets.text("shard_count", "1", "Total shards")
dbutils.widgets.text("shard_chunk_retries", "1", "Per-chunk retries (transient failures)")

# COMMAND ----------

# DBTITLE 1,Get Widget Values
catalog_name = dbutils.widgets.get("catalog_name")
entity = dbutils.widgets.get("entity")
attributes = dbutils.widgets.get("attributes")
max_potential_matches = dbutils.widgets.get("max_potential_matches")
llm_endpoint = dbutils.widgets.get("llm_endpoint")
llm_model_source = dbutils.widgets.get("llm_model_source")
additional_instructions = dbutils.widgets.get("additional_instructions")
base_prompt = dbutils.widgets.get("base_prompt")
experiment_id = dbutils.widgets.get("experiment_id")
is_single_source = dbutils.widgets.get("is_single_source")
llm_temperature = dbutils.widgets.get("llm_temperature")
reasoning_effort = dbutils.widgets.get("reasoning_effort") or "low"
llm_max_tokens = int(dbutils.widgets.get("llm_max_tokens") or "4096")
chunk_size = int(dbutils.widgets.get("chunk_size") or "250000")
shard_index = int(dbutils.widgets.get("shard_index") or "0")
shard_count = int(dbutils.widgets.get("shard_count") or "1")
shard_chunk_retries = int(dbutils.widgets.get("shard_chunk_retries") or "1")

try:
    run_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().toString()
except Exception:
    import uuid
    run_id = str(uuid.uuid4())

# COMMAND ----------

# DBTITLE 1,Get Task Values (Override Widget Values)
entity = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="entity", debugValue=entity)
attributes = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="match_attributes", debugValue=attributes)
is_single_source = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="is_single_source", debugValue=is_single_source)
catalog_name = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="catalog_name", debugValue=catalog_name)

llm_temperature = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="llm_temperature", debugValue=llm_temperature)
llm_temperature = float(llm_temperature) if llm_temperature is not None else None
reasoning_effort = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "reasoning_effort", debugValue=reasoning_effort)
llm_max_tokens_raw = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "llm_max_tokens", debugValue=llm_max_tokens)
llm_max_tokens = int(llm_max_tokens_raw) if llm_max_tokens_raw is not None else None
base_prompt = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="base_prompt", debugValue=base_prompt)
additional_instructions = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="additional_instructions", debugValue=additional_instructions)
max_potential_matches = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="max_potential_matches", debugValue=max_potential_matches)

# COMMAND ----------

# DBTITLE 1,Process Parameters
attributes = json.loads(attributes) if isinstance(attributes, str) and attributes else []
max_potential_matches = int(max_potential_matches) if max_potential_matches else 3

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

# MAGIC %run ../../utils/model_serving

# COMMAND ----------

# MAGIC %run ../../utils/llm_scoring_common

# COMMAND ----------

if isinstance(is_single_source, str):
    is_single_source = is_single_source.lower() == "true"
else:
    is_single_source = bool(is_single_source)

# COMMAND ----------

# DBTITLE 1,Resolve LLM Endpoint
try:
    llm_endpoint_from_mapping = dbutils.jobs.taskValues.get(taskKey="Endpoints_Mapping", key="llm_model_endpoint", debugValue=None)
    if llm_endpoint_from_mapping:
        llm_endpoint = llm_endpoint_from_mapping
except Exception as e:
    logger.info(f"Endpoints_Mapping task not available, falling back to model source endpoints: {e}")

if not llm_endpoint:
    if llm_model_source == "databricks_foundation":
        llm_endpoint = dbutils.jobs.taskValues.get(taskKey="LLM_Foundational_Serving_Endpoint", key="llm_model_endpoint", debugValue=llm_endpoint)
    else:
        llm_endpoint = dbutils.jobs.taskValues.get(taskKey="LLM_Hugging_Face_Serving_Endpoint", key="llm_model_endpoint", debugValue=llm_endpoint)

# COMMAND ----------

# DBTITLE 1,Set Keys + Table Names
master_id_key = "lakefusion_id"
merged_desc_column = "attributes_combined"
if is_single_source:
    source_id_key = "lakefusion_id"
    mode_name = "GOLDEN DEDUP (Single Source)"
else:
    source_id_key = "surrogate_key"
    mode_name = "NORMAL DEDUP (Multi Source)"

unified_table = f"{catalog_name}.silver.{entity}_unified"
unified_dedup_table = f"{catalog_name}.silver.{entity}_unified_deduplicate"
if experiment_id:
    unified_table += f"_{experiment_id}"
    unified_dedup_table += f"_{experiment_id}"

source_table = unified_dedup_table if is_single_source else unified_table
input_table = f"{source_table}_llm_input"
temp_llm_table = f"{source_table}_llm_temp_v1_shard_{shard_index}"
marker_table = f"{temp_llm_table}_done"

logger.info("=" * 80)
logger.info(f"LLM SHARD {shard_index}/{shard_count} - EXPERIMENT MODE ({mode_name})")
logger.info("=" * 80)
logger.info(f"Input Table: {input_table}")
logger.info(f"Output (shard) Table: {temp_llm_table}")
logger.info(f"LLM Endpoint: {llm_endpoint}")
logger.info(f"Chunk Size: {chunk_size} | Per-chunk retries: {shard_chunk_retries}")

# COMMAND ----------

# DBTITLE 1,Read this shard's input partition
if not spark.catalog.tableExists(input_table):
    raise RuntimeError(
        f"Shard {shard_index}: input table {input_table} not found. "
        f"Entity_Matching_LLM_Split_Experiment must run first."
    )

_shard_src_view = f"_llm_input_shard_{shard_index}"
spark.table(input_table).filter(F.col("shard_index") == shard_index).createOrReplaceTempView(_shard_src_view)
source_view = _shard_src_view

# Count this shard's rows up front. When the Split collapsed below its min-records floor,
# all rows landed in shard 0 and every other shard's partition is empty — those shards skip
# the costly endpoint warm-up/validation below and just write an empty (correctly-typed)
# output table + _done marker via STEP 6. STEP 6 reuses this count (no re-count).
total_records = spark.table(source_view).count()
logger.info(f"Shard {shard_index} input rows: {total_records:,}")

# COMMAND ----------

# DBTITLE 1,STEP 2: Validate LLM Endpoint
logger.info("\n" + "=" * 80)
logger.info("STEP 2: VALIDATE LLM ENDPOINT")
logger.info("=" * 80)
if total_records > 0:
    try:
        wait_until_serving_endpoint_ready(endpoint_name=llm_endpoint)
        logger.info(f"LLM endpoint '{llm_endpoint}' is ready")
    except ValueError as e:
        error_msg = f"ERROR: LLM endpoint not ready: {str(e)}"
        logger.error(error_msg)
        logger_instance.shutdown()
        dbutils.notebook.exit(error_msg)
else:
    logger.info("Empty shard (0 rows) — skipping endpoint readiness check.")

# COMMAND ----------

# DBTITLE 1,STEP 2.5: Warm Up LLM Endpoint
logger.info("\n" + "=" * 80)
logger.info("STEP 2.5: WARM UP LLM ENDPOINT (SCALE-FROM-ZERO HANDLING)")
logger.info("=" * 80)
warmup_query = f"""SELECT ai_query('{llm_endpoint}', 'Say hello and confirm you are ready.')"""
if total_records > 0:
    try:
        warm_up_llm_endpoint(
            spark=spark, endpoint_name=llm_endpoint, warmup_query=warmup_query,
            max_retries=10, retry_interval_seconds=60, timeout_minutes=10,
        )
        logger.info(f"LLM endpoint '{llm_endpoint}' is warmed up and ready")
    except Exception as e:
        error_msg = f"ERROR: LLM endpoint warm-up failed: {str(e)}"
        logger.error(error_msg)
        logger_instance.shutdown()
        dbutils.notebook.exit(error_msg)
else:
    logger.info("Empty shard (0 rows) — skipping endpoint warm-up.")

# COMMAND ----------

# DBTITLE 1,STEP 2.75 + STEP 5: Build ai_query (shared util)
if total_records > 0:
    model_params_clause = validate_model_params(
        spark, llm_endpoint, llm_temperature, llm_max_tokens, reasoning_effort, logger=logger
    )
else:
    # Empty shard (0 rows): skip endpoint param validation; "" is harmless over 0 rows.
    model_params_clause = ""
    logger.info("Empty shard (0 rows) — skipping model-parameter validation.")
safe_prompt = build_safe_prompt(entity, attributes, additional_instructions, base_prompt, max_potential_matches)
prompt_concat_parts = build_prompt_concat_parts(safe_prompt, merged_desc_column)
ai_query_select = build_ai_query_select(llm_endpoint, prompt_concat_parts, model_params_clause)

# COMMAND ----------

# DBTITLE 1,Crash-safe marker check
# temp_llm_table is retained across runs for debugging. If the prior run wrote its
# _done marker it finished cleanly -> drop both and start fresh. If the marker is
# missing but the table exists, a prior run crashed -> keep it and resume via the
# LEFT ANTI JOIN inside build_partition_scoring_query.
if spark.catalog.tableExists(marker_table):
    try:
        spark.sql(f"DROP TABLE IF EXISTS {temp_llm_table}")
        spark.sql(f"DROP TABLE IF EXISTS {marker_table}")
        logger.info(f"Prior run completed cleanly; dropped retained {temp_llm_table}")
    except Exception as e:
        logger.warning(f"Failed to drop retained temp table: {e}")
elif spark.catalog.tableExists(temp_llm_table):
    logger.info(f"Resuming from prior crashed run; retaining {temp_llm_table}")

# COMMAND ----------

# DBTITLE 1,STEP 6: Execute LLM Query (Chunked) with per-chunk retry
logger.info("\n" + "=" * 80)
logger.info("STEP 6: EXECUTE LLM QUERY & MATERIALIZE (CHUNKED)")
logger.info("=" * 80)

# total_records already computed up front when the partition was read (reused here).
logger.info(f"Shard {shard_index} records to process: {total_records:,}")

already_processed = 0
if spark.catalog.tableExists(temp_llm_table):
    already_processed = spark.table(temp_llm_table).count()
    logger.info(f"Resume: {already_processed:,} records already processed")

records_remaining = total_records - already_processed
chunk_count = builtins.max(1, math.ceil(records_remaining / chunk_size))
logger.info(f"Records remaining: {records_remaining:,} -> {chunk_count} chunks of ~{chunk_size:,}")

# Default shard_chunk_retries=1 -> 2 attempts per chunk for transient ai_query failures.
chunk_attempts = 1 + shard_chunk_retries
overall_start = time.time()
failed_chunks = 0

for chunk_id in range(chunk_count):
    chunk_start = time.time()
    logger.info(f"Chunk {chunk_id + 1}/{chunk_count}: starting")
    chunk_query = build_partition_scoring_query(
        spark, source_view, temp_llm_table, source_id_key, merged_desc_column,
        ai_query_select, chunk_id=chunk_id, chunk_count=chunk_count,
    )

    last_err = None
    last_tb = None
    committed = False
    for attempt in range(1, chunk_attempts + 1):
        try:
            spark.sql(chunk_query) \
                .withColumn("chunk_id", lit(chunk_id)) \
                .write.format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(temp_llm_table)
            elapsed = time.time() - chunk_start
            logger.info(f"Chunk {chunk_id + 1}/{chunk_count}: committed in {elapsed:.1f}s (attempt {attempt}/{chunk_attempts})")
            committed = True
            break
        except Exception as e:
            last_err = e
            last_tb = traceback.format_exc()
            logger.warning(
                f"Chunk {chunk_id + 1}/{chunk_count} attempt {attempt}/{chunk_attempts} failed: "
                f"{type(e).__name__}: {str(e)[:300]}"
            )
            # Delta append is atomic — a failed attempt left no partial rows, so re-running the
            # identical chunk query cannot double-insert. Brief backoff between attempts.
            if attempt < chunk_attempts:
                time.sleep(builtins.min(30, 5 * attempt))

    if not committed:
        # "Let it fail, keep NULL": don't touch the shared error table, don't derive keys.
        # These records are simply absent from this shard's output and stay NULL at the end;
        # the assembler validates/null-handles the rows that ARE present.
        failed_chunks += 1
        elapsed = time.time() - chunk_start
        logger.error(
            f"Chunk {chunk_id + 1}/{chunk_count} FAILED after {elapsed:.1f}s, {chunk_attempts} attempt(s): "
            f"{type(last_err).__name__}: {str(last_err)[:400]}"
        )
        if last_tb:
            logger.error(last_tb)

overall_elapsed = time.time() - overall_start
logger.info(f"Chunked materialization complete in {overall_elapsed:.1f}s ({overall_elapsed / 60:.1f} min); {failed_chunks} chunk(s) failed")

# If the table was never created (every chunk failed, e.g. dead endpoint), fail loudly so the
# shard task FAILS and the assembler's completeness gate trips — better than a silent empty shard.
if not spark.catalog.tableExists(temp_llm_table):
    raise RuntimeError(
        f"Shard {shard_index}: all {chunk_count} chunks failed; no output written. "
        f"Common cause: endpoint rate limiting (429) or misconfiguration."
    )

post_stats = spark.table(temp_llm_table).agg(
    count("*").alias("total"),
    count("scoring_results.result").alias("with_result"),
).collect()[0]
logger.info(f"  Shard rows materialized: {post_stats['total']:,} (with LLM result: {post_stats['with_result']:,})")

# Throughput for this run (rows scored this run / elapsed). already_processed excludes rows
# carried over from a resumed crash so the rate reflects work actually done this run.
scored_this_run = builtins.max(0, post_stats["total"] - already_processed)
if overall_elapsed > 0 and scored_this_run > 0:
    rows_per_sec = scored_this_run / overall_elapsed
    logger.info(
        f"  Shard {shard_index} throughput: {rows_per_sec:.2f} rows/sec "
        f"({rows_per_sec * 60:.1f} rows/min) over {scored_this_run:,} rows scored this run"
    )

# COMMAND ----------

# DBTITLE 1,Write completion marker and exit
spark.sql(f"CREATE TABLE IF NOT EXISTS {marker_table} (placeholder STRING) USING delta")
# NOTE: dbutils.jobs.taskValues.set is NOT supported inside a for_each iteration
# ("setting task values is not supported for iterations"). The shard signals completion
# purely via the _done marker table above — the assembler's completeness gate checks for
# {shard_table}_done, so no task values are needed here.
logger.info(
    f"Shard {shard_index}/{shard_count} scored (raw) -> {temp_llm_table}. "
    f"Validation/retry/explode/merge deferred to Entity_Matching_LLM_Assemble_Experiment."
)
logger_instance.shutdown()
dbutils.notebook.exit(json.dumps({
    "status": "sharded_scored",
    "shard_index": shard_index,
    "shard_count": shard_count,
    "shard_table": temp_llm_table,
}))
