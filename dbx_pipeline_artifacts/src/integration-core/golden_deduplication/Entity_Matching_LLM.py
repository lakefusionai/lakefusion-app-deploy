# Databricks notebook source
# ====================================================================
# Entity_Matching_LLM - Golden Deduplication
#
# Upgrades applied:
#   1. Added modelParameters to ai_query (reasoning_effort, max_tokens).
#   2. Replaced single atomic saveAsTable with chunked appends for
#      crash-safe materialization.
#   3. Added explicit explode cell between materialization and
#      classification to separate concerns.
#   4. Added LLM result validation + retry layer (max 2 retries).
#   5. Better stats and logging throughout.
# ====================================================================

%pip install databricks-vectorsearch

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

from uuid import uuid4
import builtins
import json
import math
import time
import traceback
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pandas as pd
from databricks.sdk import WorkspaceClient
import requests
from dbruntime.databricks_repl_context import get_context
import re

# COMMAND ----------

dbutils.widgets.text("llm_endpoint", "", "LLM Endpoint Name")
dbutils.widgets.text("experiment_id", "", "Match Maven Experiment")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("attributes", "", "Merged Description Attributes")
dbutils.widgets.dropdown("llm_model_source", "databricks_foundation", ["databricks_custom_hugging_face", "databricks_foundation"], "LLM Model Source")
dbutils.widgets.text("catalog_name", "", "lakefusion catalog name")
dbutils.widgets.text("additional_instructions", "", "Additional Instruction")
dbutils.widgets.text("config_thresholds", "", "Match Thresholds Config")
dbutils.widgets.text("max_potential_matches", "3", "Max Potential Matches")
dbutils.widgets.text("base_prompt", "","Base Prompt")
dbutils.widgets.text("llm_temperature", "0.0", "LLM Temperature")
dbutils.widgets.dropdown("reasoning_effort", "low", ["low", "medium", "high"], "LLM reasoning_effort")
dbutils.widgets.text("llm_max_tokens", "4096", "LLM max_tokens")
dbutils.widgets.text("chunk_size", "50000", "Records per LLM chunk")
dbutils.widgets.text("llm_max_retries", "2", "LLM Max Retries")

# COMMAND ----------

llm_endpoint = dbutils.widgets.get("llm_endpoint")
attributes = dbutils.widgets.get("attributes")
experiment_id = dbutils.widgets.get("experiment_id")
entity = dbutils.widgets.get("entity")
merged_desc_column = "attributes_combined"
llm_model_source = dbutils.widgets.get("llm_model_source")
catalog_name = dbutils.widgets.get("catalog_name")
additional_instructions = dbutils.widgets.get("additional_instructions")
config_thresholds = dbutils.widgets.get("config_thresholds")
max_potential_matches = int(dbutils.widgets.get("max_potential_matches"))
base_prompt = dbutils.widgets.get("base_prompt")
llm_temperature = dbutils.widgets.get("llm_temperature")
reasoning_effort = dbutils.widgets.get("reasoning_effort") or "low"
llm_max_tokens = int(dbutils.widgets.get("llm_max_tokens") or "4096")
chunk_size = int(dbutils.widgets.get("chunk_size") or "50000")
llm_max_retries = int(dbutils.widgets.get("llm_max_retries") or "2")
try:
    run_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().toString()
except Exception:
    import uuid
    run_id = str(uuid.uuid4())

# COMMAND ----------

entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
llm_endpoint = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="llm_model_endpoint", debugValue=llm_endpoint)
attributes = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="match_attributes", debugValue=attributes)
llm_model_source = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="llm_model_source", debugValue=llm_model_source)
additional_instructions = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="additional_instructions", debugValue=additional_instructions)
config_thresholds = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="config_thresholds", debugValue=config_thresholds)
max_potential_matches = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="max_potential_matches", debugValue=max_potential_matches)
base_prompt = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "base_prompt", debugValue=base_prompt)
llm_temperature = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "llm_temperature", debugValue=llm_temperature)
llm_temperature = float(llm_temperature) if llm_temperature is not None else None

reasoning_effort = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "reasoning_effort", debugValue=reasoning_effort)
llm_max_tokens_raw = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "llm_max_tokens", debugValue=llm_max_tokens)
llm_max_tokens = int(llm_max_tokens_raw) if llm_max_tokens_raw is not None else None

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

try:
    llm_endpoint_from_mapping = dbutils.jobs.taskValues.get(
        taskKey="Endpoints_Mapping",
        key="llm_model_endpoint",
        debugValue=None
    )

    if llm_endpoint_from_mapping:
        llm_endpoint = llm_endpoint_from_mapping

except Exception as e:
    logger.info(f"Endpoints_Mapping task not available, falling back to model source endpoints: {e}")

if not llm_endpoint:
    if llm_model_source == "databricks_foundation":
        llm_endpoint = dbutils.jobs.taskValues.get(
            taskKey="LLM_Foundational_Serving_Endpoint",
            key="llm_model_endpoint",
            debugValue=llm_endpoint
        )
    else:
        llm_endpoint = dbutils.jobs.taskValues.get(
            taskKey="LLM_Hugging_Face_Serving_Endpoint",
            key="llm_model_endpoint",
            debugValue=llm_endpoint
        )

# COMMAND ----------

attributes = json.loads(attributes)
config_thresholds = json.loads(config_thresholds)
max_potential_matches = int(max_potential_matches)

# full attribute records for LLM prompt enrichment.
try:
    _attr_records_raw = dbutils.jobs.taskValues.get(
        taskKey="Parse_Entity_Model_JSON",
        key="entity_attribute_records",
        debugValue="[]",
    )
    entity_attribute_records = json.loads(_attr_records_raw) if _attr_records_raw else []
except Exception:
    entity_attribute_records = []

import os as _pp_os
import sys as _pp_sys
_pp_parts = _pp_os.getcwd().split(_pp_os.sep)
for _i in range(len(_pp_parts) - 1, -1, -1):
    if _pp_parts[_i] == "src":
        _pp_src_path = _pp_os.sep.join(_pp_parts[: _i + 1])
        if _pp_src_path not in _pp_sys.path:
            _pp_sys.path.insert(0, _pp_src_path)
        break

try:
    from lakefusion_core_engine.utils.attribute_prompt_utils import (
        build_attribute_descriptors as _build_attribute_descriptors,
        merge_selected_sub_fields as _merge_selected_sub_fields,
    )
except Exception:
    def _build_attribute_descriptors(names, records=None, separator=" | "):
        return separator.join(names)
    def _merge_selected_sub_fields(records, selection):
        return list(records or [])

# Per-attribute sub-field selection from model config.
try:
    _selected_sub_fields_raw = dbutils.jobs.taskValues.get(
        taskKey="Parse_Entity_Model_JSON",
        key="model_selected_sub_fields",
        debugValue="{}",
    )
    model_selected_sub_fields = json.loads(_selected_sub_fields_raw) if _selected_sub_fields_raw else {}
except Exception:
    model_selected_sub_fields = {}

entity_attribute_records = _merge_selected_sub_fields(
    entity_attribute_records, model_selected_sub_fields
)

# COMMAND ----------

merge_thresholds = config_thresholds.get('merge', [0.9, 1.0])
matches_thresholds = config_thresholds.get('matches', [0.7, 0.89])
not_match_thresholds = config_thresholds.get('not_match', [0.0, 0.69])

merge_min, merge_max = merge_thresholds[0], merge_thresholds[1]
matches_min, matches_max = matches_thresholds[0], matches_thresholds[1]
not_match_min, not_match_max = not_match_thresholds[0], not_match_thresholds[1]

# COMMAND ----------

master_id_key = "lakefusion_id"
search_results = "search_results"

# COMMAND ----------

unified_dedup_table = f"{catalog_name}.silver.{entity}_unified_deduplicate"
processed_unified_dedup_table = f"{catalog_name}.silver.{entity}_processed_unified_deduplicate"
master_table = f"{catalog_name}.gold.{entity}_master"
unified_deteministic_table = f"{catalog_name}.silver.{entity}_unified_deterministic_deduplicate"

if experiment_id:
    unified_dedup_table += f"_{experiment_id}"
    processed_unified_dedup_table += f"_{experiment_id}"
    master_table += f"_{experiment_id}"
    unified_deteministic_table += f"_{experiment_id}"

# Defined here (before any STEP) so the start-of-run marker check (STEP 4) and
# the end-of-run marker write (STEP 12) reference the same names.
temp_llm_table = f"{unified_dedup_table}_llm_temp_v1"
marker_table = f"{temp_llm_table}_done"

master_table_list = master_table.split(".")
master_catalog, master_schema, master_table_name = master_table_list[0], master_table_list[1], master_table_list[2]

# COMMAND ----------

deteministic_unified_table_exists = spark.catalog.tableExists(unified_deteministic_table)

# COMMAND ----------

# ── Build _det_filter temp view: per-source has_positive_match + filtered_search_results
# The deterministic table now stores one row per (source × matched candidate).
# We aggregate it back into per-source shape so the LLM filter can join on source id:
#   - has_positive_match: any MATCH/POTENTIAL_MATCH-action row for the source → skip LLM
#   - filtered_search_results: original search_results minus NO_MATCH'd candidate ids;
#     null when all candidates were filtered (→ source goes to JOB_INSERT, skip LLM).
MATCH_ACTIONS    = ["MATCH", "POTENTIAL_MATCH"]
NO_MATCH_ACTIONS = ["NO_MATCH"]

if deteministic_unified_table_exists:
    df_det = spark.table(unified_deteministic_table)

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

    df_source_for_filter = spark.table(unified_dedup_table).select(master_id_key, "search_results")

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
else:
    empty_schema = StructType([
        StructField(master_id_key,             StringType(), True),
        StructField("has_positive_match",      BooleanType(), True),
        StructField("filtered_search_results", StringType(), True),
    ])
    spark.createDataFrame([], empty_schema).createOrReplaceTempView("_det_filter")

# COMMAND ----------

logger.info("=" * 80)
logger.info("GOLDEN DEDUP - LLM ENTITY MATCHING")
logger.info("=" * 80)
logger.info(f"Entity: {entity}")
logger.info(f"Experiment: {experiment_id if experiment_id else 'prod'}")
logger.info(f"Unified Dedup Table: {unified_dedup_table}")
logger.info(f"Processed Unified Dedup Table: {processed_unified_dedup_table}")
logger.info(f"Master Table: {master_table}")
logger.info(f"LLM Endpoint: {llm_endpoint}")
logger.info(f"LLM Temperature: {llm_temperature}")
logger.info(f"LLM Reasoning Effort: {reasoning_effort}")
logger.info(f"LLM Max Tokens: {llm_max_tokens}")
logger.info(f"Chunk Size: {chunk_size}")
logger.info(f"LLM Max Retries: {llm_max_retries}")
logger.info(f"Max Potential Matches: {max_potential_matches}")
logger.info(f"Match Thresholds:")
logger.info(f"  MERGE: [{merge_min}-{merge_max}]")
logger.info(f"  POTENTIAL_MATCH: [{matches_min}-{matches_max}]")
logger.info(f"  NO_MATCH: [{not_match_min}-{not_match_max}]")
logger.info("=" * 80)

# COMMAND ----------

# MAGIC %run ../../utils/model_serving

# COMMAND ----------

logger.info("\n" + "=" * 80)
logger.info("STEP 1: VALIDATE VECTOR SEARCH COMPLETION")
logger.info("=" * 80)

try:
    vector_search_complete = dbutils.jobs.taskValues.get(
        taskKey="Entity_Matching_Vector_Search",
        key="vector_search_complete",
        debugValue=True
    )

    if not vector_search_complete:
        error_msg = "ERROR: Vector search was not completed successfully"
        logger.error(error_msg)
        logger_instance.shutdown()
        dbutils.notebook.exit(error_msg)

    logger.info("Vector search completed successfully")

except Exception as e:
    logger.warning(f"Could not verify vector search completion: {e}")
    logger.warning("  Proceeding with LLM matching...")

# COMMAND ----------

logger.info("\n" + "=" * 80)
logger.info("STEP 2: VALIDATE LLM ENDPOINT")
logger.info("=" * 80)

try:
    wait_until_serving_endpoint_ready(endpoint_name=llm_endpoint, timeout_minutes=5)
    logger.info(f"LLM endpoint '{llm_endpoint}' is ready")
except ValueError as e:
    error_msg = f"ERROR: LLM endpoint not ready: {str(e)}"
    logger.error(error_msg)
    logger_instance.shutdown()
    dbutils.notebook.exit(error_msg)

# COMMAND ----------

logger.info("\n" + "=" * 80)
logger.info("STEP 2.5: WARM UP LLM ENDPOINT (SCALE-FROM-ZERO HANDLING)")
logger.info("=" * 80)

warmup_query = f"""
SELECT ai_query('{llm_endpoint}', 'Say hello and confirm you are ready.') """

try:
    warm_up_llm_endpoint(
        spark=spark,
        endpoint_name=llm_endpoint,
        warmup_query=warmup_query,
        max_retries=10,
        retry_interval_seconds=60,
        timeout_minutes=10
    )
    logger.info(f"LLM endpoint '{llm_endpoint}' is warmed up and ready")
except Exception as e:
    error_msg = f"ERROR: LLM endpoint warm-up failed: {str(e)}"
    logger.error(error_msg)
    logger_instance.shutdown()
    dbutils.notebook.exit(error_msg)

# COMMAND ----------

logger.info("\n" + "=" * 80)
logger.info("STEP 2.75: VALIDATE MODEL PARAMETERS")
logger.info("=" * 80)

validated_model_params = {}
candidate_params = {}
if llm_temperature is not None:
    candidate_params["temperature"] = llm_temperature
if llm_max_tokens is not None:
    candidate_params["max_tokens"] = llm_max_tokens
if reasoning_effort and reasoning_effort != 'disabled':
    candidate_params["reasoning_effort"] = reasoning_effort

logger.info(f"Candidate model parameters to validate: {candidate_params}")

for param_name, param_value in candidate_params.items():
    try:
        if isinstance(param_value, str):
            param_clause = f"named_struct('{param_name}', '{param_value}')"
        else:
            param_clause = f"named_struct('{param_name}', {param_value})"

        test_query = f"""
        SELECT ai_query(
            '{llm_endpoint}',
            'Say OK',
            modelParameters => {param_clause},
            failOnError => true
        )
        """
        spark.sql(test_query).collect()
        validated_model_params[param_name] = param_value
        logger.info(f"  {param_name} = {param_value} -> SUPPORTED")
    except Exception as e:
        error_snippet = str(e)[:200]
        logger.warning(f"  {param_name} = {param_value} -> NOT SUPPORTED (removed from config)")
        logger.warning(f"    Error: {error_snippet}")

if validated_model_params:
    param_parts = []
    for k, v in validated_model_params.items():
        if isinstance(v, str):
            param_parts.append(f"'{k}', '{v}'")
        else:
            param_parts.append(f"'{k}', {v}")
    model_params_clause = f"modelParameters => named_struct({', '.join(param_parts)}),"
else:
    model_params_clause = ""

logger.info(f"  Final modelParameters clause: {model_params_clause if model_params_clause else '(none - using defaults)'}")

# COMMAND ----------

logger.info("\n" + "=" * 80)
logger.info("STEP 3: CHECK PROCESSED UNIFIED DEDUP TABLE")
logger.info("=" * 80)

processed_unified_dedup_table_exists = spark.catalog.tableExists(processed_unified_dedup_table)

if processed_unified_dedup_table_exists:
    existing_count = spark.table(processed_unified_dedup_table).count()
    logger.info(f"Processed unified dedup table exists")
    logger.info(f"  Current records: {existing_count}")
else:
    logger.info(f"Processed unified dedup table does not exist")
    logger.info(f"  Will be created: {processed_unified_dedup_table}")

# COMMAND ----------

logger.info("\n" + "=" * 80)
logger.info("STEP 4: FILTER RECORDS FOR LLM PROCESSING")
logger.info("=" * 80)

# Start-of-run cleanup: temp_llm_table is retained across runs to allow
# post-run debugging of empty/bad scoring_results. The prior run's STEP 12
# writes a `_done` marker if it finished cleanly. If the marker exists, drop
# both the temp table and the marker so this run starts fresh. If the marker
# is missing (prior run crashed), leave temp_llm_table alone — the chunked
# LEFT ANTI JOIN in build_llm_query will resume from where the crash left off.
if spark.catalog.tableExists(marker_table):
    try:
        spark.sql(f"DROP TABLE IF EXISTS {temp_llm_table}")
        spark.sql(f"DROP TABLE IF EXISTS {marker_table}")
        logger.info(f"Prior run completed cleanly; dropped retained {temp_llm_table}")
    except Exception as e:
        logger.warning(f"Failed to drop retained temp table: {e}")
elif spark.catalog.tableExists(temp_llm_table):
    logger.info(f"Resuming from prior crashed run; retaining {temp_llm_table}")

if deteministic_unified_table_exists:
    # Skip records with a positive deterministic match (MATCH/POTENTIAL_MATCH).
    # Records with only NO_MATCH rule hits still flow to LLM, but with the
    # NO_MATCH candidates pre-filtered out via filtered_search_results.
    # Records where every candidate was filtered (filtered_search_results IS NULL
    # but a deterministic row exists) skip LLM entirely → JOB_INSERT downstream.
    filter_query = f"""
      select
        u.{master_id_key},
        u.{merged_desc_column},
        COALESCE(d.filtered_search_results, u.search_results) as search_results
      from
        {unified_dedup_table} u
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
        {unified_dedup_table} u
      where
        u.search_results IS NOT NULL
        and u.search_results != ''
        and (u.scoring_results IS NULL or u.scoring_results = '')
    """

count_query = f"""
select count(*) as record_count
from (
  {filter_query}
) subquery
"""

uf_sub_query = f"""uf as ({filter_query})"""

records_to_process = spark.sql(count_query).collect()[0]["record_count"]

logger.info(f"Filter: Records with search_results and empty scoring_results")

if records_to_process == 0:
    logger.info("No new records to process")

    # No records to process means nothing to resume even if a marker is missing —
    # safe to drop both temp_llm_table and marker unconditionally here.
    try:
        spark.sql(f"DROP TABLE IF EXISTS {temp_llm_table}")
        spark.sql(f"DROP TABLE IF EXISTS {marker_table}")
        logger.info(f"Cleaned up stale temp table: {temp_llm_table}")
    except Exception:
        pass

    logger.info("\n" + "=" * 80)
    logger.info("STEP 5: UPDATE PROCESSED UNIFIED DEDUP FROM SCORING RESULTS")
    logger.info("=" * 80)

    with_scoring = spark.sql(f"""
        select count(*) as cnt
        from {unified_dedup_table}
        where scoring_results IS NOT NULL
        and scoring_results != ''
    """).collect()[0]['cnt']

    logger.info(f"  Records with scoring_results: {with_scoring}")

    if with_scoring > 0:
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

        df_from_scoring = spark.sql(rebuild_query)
        df_from_scoring.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(processed_unified_dedup_table)
    else:
        logger.info("  No scoring_results to process")

        if not spark.catalog.tableExists(processed_unified_dedup_table):
            logger.info(f"  Creating empty processed_unified_dedup table: {processed_unified_dedup_table}")
            empty_schema = StructType([
                StructField(f"query_{master_id_key}", StringType(), True),
                StructField(f"match_{master_id_key}", StringType(), True),
                StructField("exploded_result", StructType([
                    StructField("id", StringType(), True),
                    StructField("match", StringType(), True),
                    StructField("score", DoubleType(), True),
                    StructField("reason", StringType(), True),
                    StructField("lakefusion_id", StringType(), True),
                ]), True),
                StructField("exploded_result_id", StringType(), True),
            ])
            empty_df = spark.createDataFrame([], empty_schema)
            empty_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(processed_unified_dedup_table)
            logger.info("  ✓ Empty processed_unified_dedup table created")

    dbutils.jobs.taskValues.set("llm_matching_complete", True)
    dbutils.jobs.taskValues.set("records_processed", 0)

    logger_instance.shutdown()
    dbutils.notebook.exit(json.dumps({
        "status": "skipped",
        "message": "No new records to process",
        "records_processed": 0
    }))

# COMMAND ----------

def safe_format(template, **kwargs):
    class SafeDict(dict):
        def __missing__(self, key):
            return '{' + key + '}'
    return template.format_map(SafeDict(**kwargs))

# COMMAND ----------

union_clause = ""
if deteministic_unified_table_exists:
    # Bring deterministic positive-match rows into df_llm_results so they land in
    # processed_unified alongside LLM-scored entries. New schema: one row per
    # (source × matched candidate); filter to the positive-action rows only.
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

logger.info("\n" + "=" * 80)
logger.info("STEP 5: BUILD LLM QUERY")
logger.info("=" * 80)

attribute_order = _build_attribute_descriptors(attributes, entity_attribute_records)
additional_instructions_text = additional_instructions.strip() if additional_instructions else ""

logger.info(f"Building LLM query for {max_potential_matches} matches per record")

if base_prompt is None or base_prompt == '':
    logger.info("Using default prompt")
    default_prompt = f"""You are an entity matching scorer. Compare an incoming unified record against potential golden matches and score each 0.0-1.0.

## Important Context
- Entity type: {entity}
- Attributes may be incomplete, inconsistent, or contain typos.
- **Empty fields are neutral.** Only conflicting filled values are negative evidence.
- **lakefusion_id is an opaque ID; vector_score is retrieval similarity.** Do not use either for scoring. Copy lakefusion_id to output unchanged.
- Return exactly one scored result per potential match provided in the input.

## Attribute Order
Fields are pipe-delimited in this order:
{attribute_order}

Empty fields between pipes = no data.
Note: Some columns can have aggregated values separated by (bullet)

## Input Format

**Query Record:** A pipe-delimited string in the attribute order above.

**Potential Matches:** A JSON array of 3-element arrays, one per candidate:
[[pipe_delimited_attributes, lakefusion_id, vector_score], ...]

- Index 0: pipe-delimited attribute values (same order as above)
- Index 1: lakefusion_id
- Index 2: vector_score

## Conflict vs. Near-match
Classify carefully — this distinction directly affects scoring:
- **Near-match** (typos/entry errors): character transpositions (Jonh->John), misspellings (SMth->Smith), truncations (10 Main St->101 Main St), date component swaps (1990-05-09->1990-09-05), minor formatting differences (St.->Street), missing accents or special characters (Mueller->Muller), language variations of the same word.
- **Conflict** (genuinely different values): completely different names (Mark->John), different cities (Dallas->Los Angeles), different product models (Sentry->C45), different dates with no transposition pattern (1990-05-09->1993-06-18).

## Additional Instructions
{additional_instructions_text if additional_instructions_text else "(none)"}

## Output Format — STRICT
Return ONLY a plain JSON array. No markdown, no code blocks, no extra text.

The array MUST contain exactly one object per potential match in the input. If the input contains N matches, return N scored results. Each object MUST have exactly these keys:
- id: String (the match_record text from index 0 of the inner array)
- score: Float (YOUR similarity score 0.0-1.0, NOT the vector score)
- reason: String (see Reason Format below)
- lakefusion_id: String (copy character-for-character from index 1 of the input array; do not modify, validate, or generate)

Sort by score descending.

## Reason Format
Format: "Key signals: [2-3 most important field classifications]. Justification: [1-2 sentences explaining the score]."

Each field classification uses this taxonomy: exact_match | near_match | conflict | one_empty | both_empty. For conflicts and near-matches, include the actual values in parentheses.

Example:
"Key signals: companyName exact_match, addressLineOne near_match (123 Main vs 123 Main St), supplierEmail exact_match. Justification: Strong identity agreement across name and email with minor address formatting difference. Scored 0.95 as MATCH."
"""
    safe_prompt = default_prompt.replace("'", "\\'")
else:
    logger.info("Using custom base prompt")
    base_prompt = base_prompt.replace("{query_entity}", "").replace("{merged_desc_column}", "").replace("{search_results}", "")
    formatted_prompt = safe_format(
        base_prompt,
        entity=entity,
        additional_instructions=additional_instructions,
        attributes=attribute_order, # enriched
        max_potential_matches=max_potential_matches
    )
    safe_prompt = formatted_prompt.replace("'", "\\'")

prompt_concat_parts = (
    f"CONCAT('{safe_prompt}', "
    f"'\\nQuery record: ', {merged_desc_column}, "
    f"'\\nList of potential matches with vector search scores (JSON Array): ', search_results)"
)

# JSON response format for ai_query — defined once, reused in main query, sample preview, and retries
response_format_clause = f"""responseFormat => '{{
          "type": "json_schema",
          "json_schema": {{
            "name": "match_results",
            "schema": {{
              "type": "object",
              "properties": {{
                "results": {{
                  "type": "array",
                  "items": {{
                    "type": "object",
                    "properties": {{
                      "id": {{ "type": "string" }},
                      "score": {{ "type": "number" }},
                      "reason": {{ "type": "string" }},
                      "{master_id_key}": {{ "type": "string" }}
                    }},
                    "required": ["id", "score", "reason", "{master_id_key}"],
                    "additionalProperties": false
                  }}
                }}
              }},
              "required": ["results"],
              "additionalProperties": false
            }},
            "strict": true
          }}
        }}',"""

def build_ai_query_select():
    """Build the ai_query SELECT expression reused across main, sample, and retry queries."""
    return f"""ai_query(
        '{llm_endpoint}',
        {prompt_concat_parts},
        {response_format_clause}
        {model_params_clause}
        failOnError => false
      ) AS scoring_results"""

# Build chunked LLM query (temp_llm_table is defined earlier alongside the
# other table names so STEP 4 marker check and STEP 12 marker write share it)

def build_llm_query(chunk_id=None, chunk_count=None):
    chunk_filter = ""
    if chunk_id is not None and chunk_count is not None:
        chunk_filter = f"AND abs(hash(u.{master_id_key})) % {chunk_count} = {chunk_id}"

    temp_anti_join = ""
    if spark.catalog.tableExists(temp_llm_table):
        temp_anti_join = f"LEFT ANTI JOIN {temp_llm_table} t ON t.{master_id_key} = u.{master_id_key}"

    if deteministic_unified_table_exists:
        chunk_filter_query = f"""
          select
            u.{master_id_key},
            u.{merged_desc_column},
            COALESCE(d.filtered_search_results, u.search_results) as search_results
          from
            {unified_dedup_table} u
            left join _det_filter d
              on u.{master_id_key} = d.{master_id_key}
            {temp_anti_join}
          where
            (d.has_positive_match IS NULL or d.has_positive_match = false)
            and (d.has_positive_match IS NULL or d.filtered_search_results IS NOT NULL)
            and u.search_results IS NOT NULL
            and u.search_results != ''
            and (u.scoring_results IS NULL or u.scoring_results = '')
            {chunk_filter}
        """
    else:
        chunk_filter_query = f"""
          select
            u.{master_id_key},
            u.{merged_desc_column},
            u.search_results
          from
            {unified_dedup_table} u
            {temp_anti_join}
          where
            u.search_results IS NOT NULL
            and u.search_results != ''
            and (u.scoring_results IS NULL or u.scoring_results = '')
            {chunk_filter}
        """

    return f"""
    WITH uf AS ({chunk_filter_query})
    SELECT
      {master_id_key},
      {merged_desc_column},
      search_results,
      {build_ai_query_select()}
    FROM uf
    """

# COMMAND ----------

logger.info("\n" + "=" * 80)
logger.info("STEP 5.5: SAMPLE PREVIEW (2 RECORDS)")
logger.info("=" * 80)

try:
    sample_query = f"""
    WITH uf AS ({filter_query})
    SELECT
      {master_id_key},
      {merged_desc_column},
      search_results,
      {build_ai_query_select()}
    FROM uf
    LIMIT 2
    """
    df_sample = spark.sql(sample_query)

    logger.info("Sample preview (2 records):")
    display(df_sample.select(
        master_id_key,
        col("scoring_results.result").alias("llm_result"),
        col("scoring_results.errorMessage").alias("llm_error")
    ))

    sample_count = df_sample.count()
    sample_nulls = df_sample.filter(col("scoring_results.result").isNull()).count()

    if sample_nulls > 0:
        logger.warning(f"  {sample_nulls}/{sample_count} sample records returned null - check endpoint/prompt")
    else:
        logger.info(f"  All {sample_count} sample records returned results successfully")

except Exception as e:
    logger.warning(f"Sample preview failed (non-fatal): {str(e)[:300]}")
    logger.warning("  Proceeding with full run anyway")

# COMMAND ----------

logger.info("\n" + "=" * 80)
logger.info("STEP 6: EXECUTE LLM QUERY & MATERIALIZE (CHUNKED)")
logger.info("=" * 80)

# Guarantee the unified error table exists before scoring starts, so chunk-level
# failures (e.g. 429 rate-limit) have somewhere to land even if the VS layer
# logged no errors upstream.
from lakefusion_core_engine.services.unified_error_handler import UnifiedErrorHandler
UnifiedErrorHandler(spark, unified_dedup_table).ensure_table()

total_records = spark.sql(count_query).collect()[0]["record_count"]
logger.info(f"Total records to process: {total_records:,}")

already_processed = 0
if spark.catalog.tableExists(temp_llm_table):
    already_processed = spark.table(temp_llm_table).count()
    logger.info(f"Resume: {already_processed:,} records already processed")

records_remaining = total_records - already_processed
chunk_count = builtins.max(1, math.ceil(records_remaining / chunk_size))
logger.info(f"Records remaining: {records_remaining:,} -> {chunk_count} chunks of ~{chunk_size:,}")

chunk_timings = []
chunk_failures = []  # list of (chunk_id, err_msg, failed_keys_df_or_None)
overall_start = time.time()

for chunk_id in range(chunk_count):
    chunk_start = time.time()
    logger.info(f"Chunk {chunk_id + 1}/{chunk_count}: starting")
    chunk_query = build_llm_query(chunk_id=chunk_id, chunk_count=chunk_count)

    try:
        spark.sql(chunk_query) \
            .withColumn("chunk_id", lit(chunk_id)) \
            .write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(temp_llm_table)

        elapsed = time.time() - chunk_start
        chunk_timings.append({"chunk_id": chunk_id, "elapsed": elapsed})
        logger.info(f"Chunk {chunk_id + 1}/{chunk_count}: committed in {elapsed:.1f}s")

    except Exception as e:
        elapsed = time.time() - chunk_start
        err_msg = f"LLM chunk {chunk_id} failed: {type(e).__name__}: {str(e)[:400]}"
        logger.error(f"Chunk {chunk_id + 1}/{chunk_count} FAILED after {elapsed:.1f}s: {err_msg}")
        logger.error(traceback.format_exc())
        # Derive the surrogate keys this chunk would have scored, mirroring
        # build_llm_query's slicing predicate at line ~729. The deterministic
        # _det_filter join is intentionally omitted — over-logging a few keys
        # that would have been filtered by has_positive_match is preferable to
        # fabricating keys or skipping the error log entirely.
        try:
            failed_keys_df = spark.sql(f"""
                SELECT {master_id_key} AS surrogate_key
                FROM {unified_dedup_table}
                WHERE search_results IS NOT NULL AND search_results != ''
                  AND (scoring_results IS NULL OR scoring_results = '')
                  AND abs(hash({master_id_key})) % {chunk_count} = {chunk_id}
            """)
            chunk_failures.append((chunk_id, err_msg, failed_keys_df))
        except Exception as keys_err:
            logger.error(f"Could not derive failed keys for chunk {chunk_id}: {keys_err}")
            chunk_failures.append((chunk_id, err_msg, None))
        continue

overall_elapsed = time.time() - overall_start
logger.info(f"Chunked materialization complete in {overall_elapsed:.1f}s ({overall_elapsed / 60:.1f} min)")

# Guard against the all-chunks-failed case where temp_llm_table was never created.
table_exists = spark.catalog.tableExists(temp_llm_table)
if not table_exists:
    logger.error(f"All {chunk_count} chunks failed; temp_llm_table was never created.")
    total_count, raw_count = 0, 0
else:
    post_chunk_stats = spark.table(temp_llm_table).agg(
        count("*").alias("total"),
        count("scoring_results.result").alias("with_result")
    ).collect()[0]
    total_count, raw_count = post_chunk_stats["total"], post_chunk_stats["with_result"]
    logger.info(f"  Total rows materialized: {total_count:,}")
    logger.info(f"  Rows with LLM result: {raw_count:,}")
    logger.info(f"  Rows with null result (errors): {total_count - raw_count:,}")
    if total_count > 0:
        logger.info(f"  Overall rate: {total_count / overall_elapsed:.1f} rows/sec")

if chunk_timings:
    total_chunk_time = builtins.sum(c["elapsed"] for c in chunk_timings)
    logger.info(f"  Chunks processed: {len(chunk_timings)}, total time: {total_chunk_time / 60:.1f} min")

# Log captured chunk-level failures to the unified error table.
if chunk_failures:
    chunk_failure_handler = UnifiedErrorHandler(spark, unified_dedup_table)
    for failed_chunk_id, failed_err_msg, failed_keys_df in chunk_failures:
        if failed_keys_df is None:
            continue
        chunk_failure_handler.log_errors(
            failed_keys_df.withColumn("error_message", lit(failed_err_msg)),
            stage="LLM_SCORING",
            run_id=run_id,
        )
    logger.info(
        f"Logged failures for {len(chunk_failures)} chunk(s) to "
        f"{chunk_failure_handler.full_table_name}"
    )

# If every chunk failed, surface a loud failure so the job task reports FAILED
# rather than silently passing while records are left unscored.
if not table_exists:
    raise RuntimeError(
        f"LLM scoring aborted: all {chunk_count} chunks failed. "
        f"Per-record failures logged to the unified error table. "
        f"Common cause: endpoint rate limiting (429) or endpoint misconfiguration."
    )

# COMMAND ----------

# ============== VALIDATION + RETRY LAYER ================================

logger.info("\n" + "=" * 80)
logger.info("STEP 6.5: VALIDATE & RETRY BAD LLM RESULTS")
logger.info("=" * 80)

MAX_LLM_RETRIES = llm_max_retries

from pyspark.sql.functions import size as spark_size, sum as spark_sum

# Native Spark expressions for validation (no Python UDFs)
parsed_count_expr = coalesce(
    spark_size(from_json(
        get_json_object(col("scoring_results.result"), "$.results"),
        "array<struct<id:string>>"
    )),
    lit(0)
)

expected_count_expr = coalesce(
    spark_size(from_json(col("search_results"), "array<array<string>>")),
    lit(0)
)

# unresolved_count: number of returned lakefusion_id values that don't exist in
# the row's own search_results candidate list. LLMs occasionally hallucinate hex
# strings instead of copying the id from index 1 of the input array; those rows
# pass parsed_count/expected_count but produce empty scoring_results downstream
# because STEP 7's join silently drops them. Comparing against search_results
# (not master_table) is the true definition of hallucination — a stale candidate
# whose id is missing from master_table is not the LLM's fault and retrying
# cannot fix it.
def add_unresolved_count(df):
    # If the caller's df already carries unresolved_count (e.g. read back from a
    # previously-persisted temp_llm_table), drop it so the new column write
    # doesn't collide.
    if "unresolved_count" in df.columns:
        df = df.drop("unresolved_count")
    candidate_ids_expr = transform(
        from_json(col("search_results"), "array<array<string>>"),
        lambda arr: arr[1],
    )
    returned_ids_expr = transform(
        from_json(
            get_json_object(col("scoring_results.result"), "$.results"),
            "array<struct<lakefusion_id:string>>",
        ),
        lambda s: s["lakefusion_id"],
    )
    # spark_size returns -1 for a null array (e.g. scoring_results.result is null
    # or unparseable, so from_json yields null). Clamp to 0 so the persisted
    # column never reports a negative count downstream.
    return df.withColumn(
        "unresolved_count",
        greatest(
            coalesce(
                spark_size(array_except(returned_ids_expr, candidate_ids_expr)),
                lit(0),
            ),
            lit(0),
        ),
    )

def is_bad_llm_result():
    return (
        col("scoring_results.result").isNull() |
        (col("parsed_count") == 0) |
        (col("parsed_count") != col("expected_count")) |
        (col("unresolved_count") > 0)
    )

def build_retry_query_for_keys(keys_table_name):
    return f"""
    WITH uf AS (
      SELECT u.{master_id_key}, u.{merged_desc_column}, u.search_results
      FROM {temp_llm_table} u
      INNER JOIN {keys_table_name} k ON u.{master_id_key} = k.{master_id_key}
    )
    SELECT
      {master_id_key},
      {merged_desc_column},
      search_results,
      {build_ai_query_select()}
    FROM uf
    """

# Initial pass: compute parsed_count, expected_count, unresolved_count, is_bad
# once and persist. unresolved_count must be added BEFORE is_bad so the bad-gate
# can see it.
df_initial = spark.table(temp_llm_table) \
    .withColumn("parsed_count", parsed_count_expr) \
    .withColumn("expected_count", expected_count_expr)
df_initial = add_unresolved_count(df_initial) \
    .withColumn("is_bad", is_bad_llm_result())
df_initial.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(temp_llm_table)

for retry_attempt in range(1, MAX_LLM_RETRIES + 1):
    stats = spark.table(temp_llm_table).agg(
        count("*").alias("total"),
        spark_sum(col("is_bad").cast("int")).alias("bad")
    ).collect()[0]
    bad_count, total_rows = stats["bad"], stats["total"]

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
        .select(master_id_key).write.format("delta").mode("overwrite").saveAsTable(retry_keys_table)

    try:
        retry_query = build_retry_query_for_keys(retry_keys_table)
        df_retry = spark.sql(retry_query) \
            .withColumn("parsed_count", parsed_count_expr) \
            .withColumn("expected_count", expected_count_expr)
        df_retry = add_unresolved_count(df_retry) \
            .withColumn("is_bad", is_bad_llm_result()) \
            .withColumn("chunk_id", lit(-retry_attempt))
        df_retry.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(retry_results_table)

        df_retried = spark.table(retry_results_table)

        temp_delta = DeltaTable.forName(spark, temp_llm_table)
        temp_delta.alias("target").merge(
            source=df_retried.alias("source"),
            condition=f"target.{master_id_key} = source.{master_id_key}"
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
final_bad_count, final_total = final_stats["bad"], final_stats["total"]
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

    df_llm_errors = df_final_bad.withColumn(
        "error_message",
        when(col("scoring_results.result").isNull() & col("scoring_results.errorMessage").isNotNull(),
             concat(lit("LLM error: "), col("scoring_results.errorMessage"))
        ).when(col("parsed_count") == 0,
             concat(lit("LLM unparseable result: "), coalesce(col("scoring_results.result"), lit("NULL")))
        ).when(col("unresolved_count") > 0,
             concat(lit("LLM hallucinated lakefusion_id: "), col("unresolved_count").cast("string"),
                    lit(" of "), col("parsed_count").cast("string"),
                    lit(" returned ids not found in search_results. Raw: "),
                    coalesce(col("scoring_results.result"), lit("NULL")))
        ).otherwise(
             concat(lit("LLM incomplete result: expected "), col("expected_count").cast("string"),
                    lit(" results, got "), col("parsed_count").cast("string"),
                    lit(". Raw: "), coalesce(col("scoring_results.result"), lit("NULL")))
        )
    )

    from lakefusion_core_engine.services.unified_error_handler import UnifiedErrorHandler
    unified_error_handler = UnifiedErrorHandler(spark, unified_dedup_table)
    unified_error_handler.log_errors(
        df_llm_errors.select(
            col(master_id_key).alias("surrogate_key"),
            col("error_message")
        ),
        stage="LLM_SCORING",
        run_id=run_id
    )
    logger.info(f"Logged {final_bad_count:,} LLM errors to unified error table")

    logger.warning(f"Nulling out {final_bad_count:,} bad results after {MAX_LLM_RETRIES} retries")
    temp_delta = DeltaTable.forName(spark, temp_llm_table)
    temp_delta.alias("target").merge(
        source=df_final_bad.select(master_id_key).alias("source"),
        condition=f"target.{master_id_key} = source.{master_id_key}"
    ).whenMatchedUpdate(
        set={"scoring_results": lit(None).cast("struct<result:string,errorMessage:string>")}
    ).execute()

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

logger.info(f"Unified table updated with scoring_results")

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

# ============== PIPELINE STATS & CLEANUP ================================

logger.info("\n" + "=" * 80)
logger.info("STEP 12: PIPELINE STATS & CLEANUP")
logger.info("=" * 80)

logger.info(f"  Total records:        {final_total:,}")
logger.info(f"  Good results:         {final_good_count:,} ({100 - final_bad_pct:.1f}%)")
logger.info(f"  Bad results (nulled): {final_bad_count:,} ({final_bad_pct:.1f}%)")
if chunk_timings:
    total_chunk_time = builtins.sum(c["elapsed"] for c in chunk_timings)
    logger.info(f"  Chunks processed:     {len(chunk_timings)}")
    logger.info(f"  Total chunk time:     {total_chunk_time / 60:.1f} min")

# Retain temp_llm_table for post-run inspection (debugging empty/bad scoring_results).
# Drop retry ephemeral tables — they're per-attempt scratch, not cross-run debugging.
# Write a success marker so the NEXT run knows it's safe to drop temp_llm_table at
# its STEP 4 cleanup. If this run crashes before the marker is written, the next
# run will leave temp_llm_table intact and the LEFT ANTI JOIN at build_llm_query
# resumes from the crash.
try:
    for i in range(1, MAX_LLM_RETRIES + 1):
        spark.sql(f"DROP TABLE IF EXISTS {temp_llm_table}_retry_keys_{i}")
        spark.sql(f"DROP TABLE IF EXISTS {temp_llm_table}_retry_results_{i}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {marker_table} (placeholder STRING) USING delta")
    logger.info(f"Retained {temp_llm_table} for post-run inspection; success marker written")
except Exception as e:
    logger.warning(f"Could not finalize temp table cleanup: {e}")

# COMMAND ----------

dbutils.jobs.taskValues.set("llm_matching_complete", True)
logger.info("\n" + "=" * 80)
logger.info("GOLDEN DEDUP LLM MATCHING COMPLETED SUCCESSFULLY")
logger.info("=" * 80)
