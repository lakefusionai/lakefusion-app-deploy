# Databricks notebook source
# ====================================================================
# Entity_Matching_LLM_Experiment
#
# Upgrades applied:
#   1. Added modelParameters to ai_query (reasoning_effort, max_tokens).
#   2. Replaced single atomic saveAsTable with chunked appends.
#   3. Added explicit explode cell between materialization and filtering.
#   4. Added LLM result validation + retry layer (max 2 retries).
#   5. Better stats and logging throughout.
#
# Note: Experiment mode - no threshold classification applied.
# ====================================================================

%pip install databricks-vectorsearch
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Imports
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
dbutils.widgets.text("chunk_size", "50000", "Records per LLM chunk")
dbutils.widgets.text("llm_max_retries", "2", "LLM Max Retries")

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
chunk_size = int(dbutils.widgets.get("chunk_size") or "50000")
llm_max_retries = int(dbutils.widgets.get("llm_max_retries") or "2")
try:
    run_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().toString()
except Exception:
    import uuid
    run_id = str(uuid.uuid4())

# COMMAND ----------

# DBTITLE 1,Get Task Values (Override Widget Values)
entity = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="entity",
    debugValue=entity
)

attributes = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="match_attributes",
    debugValue=attributes
)

is_single_source = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="is_single_source",
    debugValue=is_single_source
)

catalog_name = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="catalog_name",
    debugValue=catalog_name
)

llm_temperature = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="llm_temperature",
    debugValue=llm_temperature
)
llm_temperature = float(llm_temperature) if llm_temperature is not None else None

reasoning_effort = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "reasoning_effort", debugValue=reasoning_effort)
llm_max_tokens_raw = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "llm_max_tokens", debugValue=llm_max_tokens)
llm_max_tokens = int(llm_max_tokens_raw) if llm_max_tokens_raw is not None else None

base_prompt = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="base_prompt",
    debugValue=base_prompt
)

additional_instructions = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="additional_instructions",
    debugValue=additional_instructions
)

max_potential_matches = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="max_potential_matches",
    debugValue=max_potential_matches
)

# COMMAND ----------

# DBTITLE 1,Process Parameters
attributes = json.loads(attributes) if isinstance(attributes, str) and attributes else []

# full attribute records for LLM prompt enrichment (STRUCT / ARRAY
# descriptors). Empty list -> scalar-only entity, prompt renders bare names.
try:
    _attr_records_raw = dbutils.jobs.taskValues.get(
        taskKey="Parse_Entity_Model_JSON",
        key="entity_attribute_records",
        debugValue="[]",
    )
    entity_attribute_records = json.loads(_attr_records_raw) if _attr_records_raw else []
except Exception:
    entity_attribute_records = []

# Bootstrap sys.path so utils.* resolves in Databricks repos + local dev.
import os as _pp_os
import sys as _pp_sys
_pp_parts = _pp_os.getcwd().split(_pp_os.sep)
for _i in range(len(_pp_parts) - 1, -1, -1):
    if _pp_parts[_i] == "src":
        _pp_src_path = _pp_os.sep.join(_pp_parts[: _i + 1])
        if _pp_src_path not in _pp_sys.path:
            _pp_sys.path.insert(0, _pp_src_path)
        break

# Use the core engine util when available; otherwise fall back to bare-name
# join so scalar-only entities don't regress in environments where the wheel
# hasn't been installed yet.
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

# Per-attribute sub-field selection from model config (sub-field selection
# story). Empty dict = no per-attribute narrowing.
try:
    _selected_sub_fields_raw = dbutils.jobs.taskValues.get(
        taskKey="Parse_Entity_Model_JSON",
        key="model_selected_sub_fields",
        debugValue="{}",
    )
    model_selected_sub_fields = json.loads(_selected_sub_fields_raw) if _selected_sub_fields_raw else {}
except Exception:
    model_selected_sub_fields = {}

# Fold selection into entity_attribute_records so the descriptor narrows.
entity_attribute_records = _merge_selected_sub_fields(
    entity_attribute_records, model_selected_sub_fields
)
max_potential_matches = int(max_potential_matches) if max_potential_matches else 3

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

if isinstance(is_single_source, str):
    is_single_source = is_single_source.lower() == "true"
else:
    is_single_source = bool(is_single_source)

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

# DBTITLE 1,Set Keys Based on Source Type
master_id_key = "lakefusion_id"
search_results = "search_results"

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
processed_unified_table = f"{catalog_name}.silver.{entity}_processed_unified"
processed_unified_dedup_table = f"{catalog_name}.silver.{entity}_processed_unified_deduplicate"
master_table = f"{catalog_name}.gold.{entity}_master"
unified_deteministic_table = f"{catalog_name}.silver.{entity}_unified_deterministic"
unified_deteministic_dedup_table = f"{catalog_name}.silver.{entity}_unified_deterministic_deduplicate"

if experiment_id:
    unified_table += f"_{experiment_id}"
    unified_dedup_table += f"_{experiment_id}"
    processed_unified_table += f"_{experiment_id}"
    processed_unified_dedup_table += f"_{experiment_id}"
    master_table += f"_{experiment_id}"
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

# Defined here (before any STEP) so the start-of-run marker check (STEP 4) and
# the end-of-run marker write (STEP 12) reference the same names.
temp_llm_table = f"{source_table}_llm_temp_v1"
marker_table = f"{temp_llm_table}_done"

master_table_list = master_table.split(".")
master_catalog, master_schema, master_table_name = master_table_list[0], master_table_list[1], master_table_list[2]

# COMMAND ----------

# MAGIC %run ../../utils/model_serving

# COMMAND ----------

# DBTITLE 1,Display Configuration
logger.info("=" * 80)
logger.info(f"LLM ENTITY MATCHING - EXPERIMENT MODE ({mode_name})")
logger.info("=" * 80)
logger.info(f"Entity: {entity}")
logger.info(f"Experiment: {experiment_id}")
logger.info(f"Is Single Source: {is_single_source}")
logger.info(f"Source Table: {source_table}")
logger.info(f"Source ID Key: {source_id_key}")
logger.info(f"Processed Table: {processed_table}")
logger.info(f"Master Table: {master_table}")
logger.info(f"LLM Endpoint: {llm_endpoint}")
logger.info(f"LLM Temperature: {llm_temperature}")
logger.info(f"LLM Reasoning Effort: {reasoning_effort}")
logger.info(f"LLM Max Tokens: {llm_max_tokens}")
logger.info(f"Chunk Size: {chunk_size}")
logger.info(f"LLM Max Retries: {llm_max_retries}")
logger.info(f"Max Potential Matches: {max_potential_matches}")
logger.info("")
logger.info("EXPERIMENT MODE: No threshold classification")
logger.info("   All matches will be scored without MATCH/POTENTIAL_MATCH/NO_MATCH status")
logger.info("=" * 80)

# COMMAND ----------

# DBTITLE 1,STEP 1: Validate Vector Search Completion
logger.info("\n" + "=" * 80)
logger.info("STEP 1: VALIDATE VECTOR SEARCH COMPLETION")
logger.info("=" * 80)

try:
    vector_search_complete = dbutils.jobs.taskValues.get(
        taskKey="Entity_Matching_Vector_Search_Experiment",
        key="vector_search_complete",
        debugValue=True
    )
    logger.info(f"Vector search completed: {vector_search_complete}")
except Exception as e:
    logger.info(f"Could not get task value (running in debug mode): {e}")
    vector_search_complete = True

# COMMAND ----------

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
        timeout_minutes=10
    )
    logger.info(f"LLM endpoint '{llm_endpoint}' is warmed up and ready")
except Exception as e:
    error_msg = f"ERROR: LLM endpoint warm-up failed: {str(e)}"
    logger.error(error_msg)
    logger_instance.shutdown()
    dbutils.notebook.exit(error_msg)

# COMMAND ----------

# DBTITLE 1,STEP 2.75: Validate Model Parameters
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

# DBTITLE 1,STEP 3: Check Processed Table
logger.info("\n" + "=" * 80)
logger.info("STEP 3: CHECK PROCESSED TABLE")
logger.info("=" * 80)

try:
    processed_df = spark.table(processed_table)
    processed_count = processed_df.count()
    logger.info(f"Processed table exists")
    logger.info(f"  Current records: {processed_count}")
except Exception as e:
    logger.info(f"Processed table does not exist yet (will be created)")
    processed_count = 0

# COMMAND ----------

# DBTITLE 1,Define merged_desc_column
merged_desc_column = "attributes_combined"

# COMMAND ----------

deteministic_unified_table_exists = spark.catalog.tableExists(deterministic_table)

# COMMAND ----------

from pyspark.sql import functions as F
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

    logger.info(
        f"Built _det_filter temp view: "
        f" source records have deterministic decisions"
    )
else:
    empty_schema = StructType([
        StructField(source_id_key,             StringType(), True),
        StructField("has_positive_match",      BooleanType(), True),
        StructField("filtered_search_results", StringType(), True),
    ])
    spark.createDataFrame([], empty_schema).createOrReplaceTempView("_det_filter")

    logger.info("No deterministic table — created empty _det_filter temp view as fallback")

# COMMAND ----------

# DBTITLE 1,STEP 4: Filter Records for LLM Processing
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
            -- Skip records where every VS candidate was ruled out by NOT_A_MATCH
            -- rules (filtered_search_results IS NULL with a deterministic row).
            -- These route to JOB_INSERT downstream — no LLM call needed.
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

uf_sub_query = f"""uf as ({filter_query})"""

count_query = f"with {uf_sub_query} select count(*) as record_count from uf"
records_to_process_count = spark.sql(count_query).collect()[0]["record_count"]

logger.info(f"Filter: {'All' if is_single_source else 'ACTIVE'} records with search_results and empty scoring_results")
logger.info(f"  Records to process: {records_to_process_count:,}")

# COMMAND ----------

# DBTITLE 1,Handle No Records to Process
if records_to_process_count == 0:
    logger.info("\nNo new records to process")

    # No records to process means nothing to resume even if a marker is missing —
    # safe to drop both temp_llm_table and marker unconditionally here.
    try:
        spark.sql(f"DROP TABLE IF EXISTS {temp_llm_table}")
        spark.sql(f"DROP TABLE IF EXISTS {marker_table}")
        logger.info(f"Cleaned up stale temp table: {temp_llm_table}")
    except Exception:
        pass

    # Propagate deterministic NO_MATCH scoring entries into source.scoring_results
    # for records that didn't reach the LLM (all-NO_MATCH cases). Without this,
    # the all-NO_MATCH records would have null scoring_results and be invisible
    # to the processed_table rebuild below — leaving JOB_INSERT routing broken.
    _early_det_join_key = "lakefusion_id" if is_single_source else "surrogate_key"
    _early_source_key = "lakefusion_id" if is_single_source else source_id_key
    if (
        deteministic_unified_table_exists
        and "deterministic_scoring_results" in spark.read.table(deterministic_table).columns
    ):
        spark.read.table(deterministic_table) \
            .filter(col("deterministic_scoring_results").isNotNull()) \
            .select(
                col(_early_det_join_key).alias("_det_key"),
                col("deterministic_scoring_results").alias("_det_scoring"),
            ) \
            .createOrReplaceTempView("_early_det_scoring")

        spark.sql(f"""
            MERGE INTO {source_table} AS target
            USING _early_det_scoring AS source
            ON target.{_early_source_key} = source._det_key
            WHEN MATCHED AND (target.scoring_results IS NULL OR target.scoring_results = '')
            THEN UPDATE SET target.scoring_results = source._det_scoring
        """)
        logger.info("Propagated deterministic NO_MATCH entries into source.scoring_results")


    if is_single_source:
        check_query = f"""
        select count(*) as cnt from {source_table}
        where scoring_results IS NOT NULL and scoring_results != ''
        """
    else:
        check_query = f"""
        select count(*) as cnt from {source_table}
        where record_status = 'ACTIVE'
        and scoring_results IS NOT NULL and scoring_results != ''
        """

    existing_scored_count = spark.sql(check_query).collect()[0]["cnt"]

    if existing_scored_count > 0:
        logger.info("  Rebuilding processed table from existing scoring_results...")

        if is_single_source:
            rebuild_query = f"""
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
            rebuild_query = f"""
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

        df_from_scoring = spark.sql(rebuild_query)
        df_from_scoring.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(processed_table)
    else:
        logger.info("  No scoring_results to process")

        if not spark.catalog.tableExists(processed_table):
            logger.info(f"  Creating empty processed table: {processed_table}")
            exploded_struct = StructType([
                StructField("id", StringType(), True),
                StructField("score", DoubleType(), True),
                StructField("reason", StringType(), True),
                StructField("lakefusion_id", StringType(), True),
            ])
            if is_single_source:
                empty_schema = StructType([
                    StructField("query_lakefusion_id", StringType(), True),
                    StructField("match_lakefusion_id", StringType(), True),
                    StructField("exploded_result", exploded_struct, True),
                    StructField("exploded_result_id", StringType(), True),
                ])
            else:
                empty_schema = StructType([
                    StructField("surrogate_key", StringType(), True),
                    StructField("lakefusion_id", StringType(), True),
                    StructField("exploded_result", exploded_struct, True),
                    StructField("exploded_result_id", StringType(), True),
                ])
            empty_df = spark.createDataFrame([], empty_schema)
            empty_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(processed_table)
            logger.info("  ✓ Empty processed table created")

    dbutils.jobs.taskValues.set("llm_matching_complete", True)
    dbutils.jobs.taskValues.set("records_processed", 0)

    logger_instance.shutdown()
    dbutils.notebook.exit(json.dumps({
        "status": "skipped",
        "message": "No new records to process",
        "records_processed": 0
    }))

# COMMAND ----------

# DBTITLE 1,Safe Format Function
def safe_format(template, **kwargs):
    class SafeDict(dict):
        def __missing__(self, key):
            return '{' + key + '}'
    return template.format_map(SafeDict(**kwargs))

# COMMAND ----------

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
            match_rules_results,
            is_deterministic_match,

            NAMED_STRUCT(
                'id', exploded_result.id,
                'score', CAST(exploded_result.score AS DOUBLE),
                'reason', exploded_result.reason,
                'lakefusion_id', exploded_result.lakefusion_id
            ) AS exploded_result
        FROM {deterministic_table}
        WHERE has_positive_match = true
    )
    GROUP BY {master_id_key}
    """
else:
    union_clause = ""

# COMMAND ----------

# DBTITLE 1,STEP 5: Build LLM Query
logger.info("\n" + "=" * 80)
logger.info("STEP 5: BUILD LLM QUERY")
logger.info("=" * 80)

# enriched attribute descriptors for STRUCT / ARRAY attrs.
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
                      "lakefusion_id": {{ "type": "string" }}
                    }},
                    "required": ["id", "score", "reason", "lakefusion_id"],
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

# Build chunked LLM query (temp_llm_table is defined earlier alongside the other
# table names so STEP 4 marker check and STEP 12 marker write share it)

def build_llm_query(chunk_id=None, chunk_count=None):
    chunk_filter = ""
    if chunk_id is not None and chunk_count is not None:
        chunk_filter = f"AND abs(hash(u.{source_id_key})) % {chunk_count} = {chunk_id}"

    temp_anti_join = ""
    if spark.catalog.tableExists(temp_llm_table):
        temp_anti_join = f"LEFT ANTI JOIN {temp_llm_table} t ON t.{source_id_key} = u.{source_id_key}"

    if is_single_source:
        if deteministic_unified_table_exists:
            chunk_filter_query = f"""
              select u.{source_id_key}, u.{merged_desc_column},
                COALESCE(d.filtered_search_results, u.search_results) as search_results
              from {source_table} u
                left join _det_filter d
                  on u.{master_id_key} = d.{master_id_key}
                {temp_anti_join}
              where (d.has_positive_match IS NULL or d.has_positive_match = false)
                and (d.has_positive_match IS NULL or d.filtered_search_results IS NOT NULL)
                and u.search_results IS NOT NULL and u.search_results != ''
                and (u.scoring_results IS NULL or u.scoring_results = '')
                {chunk_filter}
            """
        else:
            chunk_filter_query = f"""
              select u.{source_id_key}, u.{merged_desc_column}, u.search_results
              from {source_table} u
                {temp_anti_join}
              where u.search_results IS NOT NULL and u.search_results != ''
                and (u.scoring_results IS NULL or u.scoring_results = '')
                {chunk_filter}
            """
    else:
        if deteministic_unified_table_exists:
            chunk_filter_query = f"""
              select u.{source_id_key}, u.{merged_desc_column},
                COALESCE(d.filtered_search_results, u.search_results) as search_results
              from {source_table} u
                left join _det_filter d
                  on u.{source_id_key} = d.{source_id_key}
                {temp_anti_join}
              where (d.has_positive_match IS NULL or d.has_positive_match = false)
                and (d.has_positive_match IS NULL or d.filtered_search_results IS NOT NULL)
                and u.record_status = 'ACTIVE'
                and u.search_results IS NOT NULL and u.search_results != ''
                and (u.scoring_results IS NULL or u.scoring_results = '')
                {chunk_filter}
            """
        else:
            chunk_filter_query = f"""
              select u.{source_id_key}, u.{merged_desc_column}, u.search_results
              from {source_table} u
                {temp_anti_join}
              where u.record_status = 'ACTIVE'
                and u.search_results IS NOT NULL and u.search_results != ''
                and (u.scoring_results IS NULL or u.scoring_results = '')
                {chunk_filter}
            """

    return f"""
    WITH uf AS ({chunk_filter_query})
    SELECT
      {source_id_key},
      {merged_desc_column},
      search_results,
      {build_ai_query_select()}
    FROM uf
    """

# COMMAND ----------

# DBTITLE 1,STEP 5.5: Sample Preview (5 Records)
logger.info("\n" + "=" * 80)
logger.info("STEP 5.5: SAMPLE PREVIEW (2 RECORDS)")
logger.info("=" * 80)

try:
    sample_query = f"""
    WITH uf AS ({filter_query})
    SELECT
      {source_id_key},
      {merged_desc_column},
      search_results,
      {build_ai_query_select()}
    FROM uf
    LIMIT 2
    """
    df_sample = spark.sql(sample_query)

    logger.info("Sample preview (2 records):")
    display(df_sample.select(
        source_id_key,
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

# DBTITLE 1,STEP 6: Execute LLM Query (Chunked)
logger.info("\n" + "=" * 80)
logger.info("STEP 6: EXECUTE LLM QUERY & MATERIALIZE (CHUNKED)")
logger.info("=" * 80)

# Guarantee the unified error table exists before scoring starts, so chunk-level
# failures (e.g. 429 rate-limit) have somewhere to land even if the VS layer
# logged no errors upstream.
from lakefusion_core_engine.services.unified_error_handler import UnifiedErrorHandler
UnifiedErrorHandler(spark, source_table).ensure_table()

total_records = records_to_process_count
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
        # build_llm_query's slicing predicate at line ~877. The deterministic
        # _det_filter join is intentionally omitted — over-logging a few keys
        # that would have been filtered by has_positive_match is preferable to
        # fabricating keys or skipping the error log entirely.
        try:
            failed_keys_df = spark.sql(f"""
                SELECT {source_id_key} AS surrogate_key
                FROM {source_table}
                WHERE search_results IS NOT NULL AND search_results != ''
                  AND (scoring_results IS NULL OR scoring_results = '')
                  AND abs(hash({source_id_key})) % {chunk_count} = {chunk_id}
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
    chunk_failure_handler = UnifiedErrorHandler(spark, source_table)
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

# DBTITLE 1,STEP 6.5: Validate & Retry Bad LLM Results
logger.info("\n" + "=" * 80)
logger.info("STEP 6.5: VALIDATE & RETRY BAD LLM RESULTS")
logger.info("=" * 80)

MAX_LLM_RETRIES = llm_max_retries

from pyspark.sql.functions import size as spark_size, sum as spark_sum

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
      SELECT u.{source_id_key}, u.{merged_desc_column}, u.search_results
      FROM {temp_llm_table} u
      INNER JOIN {keys_table_name} k ON u.{source_id_key} = k.{source_id_key}
    )
    SELECT
      {source_id_key},
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
        .select(source_id_key).write.format("delta").mode("overwrite").saveAsTable(retry_keys_table)

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
logger.info(f"Source table updated with scoring_results")

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

# DBTITLE 1,STEP 12: Pipeline Stats & Cleanup
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

# DBTITLE 1,Set Task Values and Exit
dbutils.jobs.taskValues.set("llm_matching_complete", True)

logger.info("\n" + "=" * 80)
logger.info(f"LLM MATCHING COMPLETED SUCCESSFULLY ({mode_name})")
logger.info("=" * 80)
logger.info(f"Mode: {mode_name}")
logger.info(f"Processed table: {processed_table}")
logger.info("")
logger.info("EXPERIMENT MODE:")
logger.info("   - No threshold classification applied")
logger.info("   - All scores are raw LLM similarity scores")
logger.info("   - scoring_results format: [{id, score, reason, lakefusion_id}, ...]")
logger.info("=" * 80)

logger_instance.shutdown()
dbutils.notebook.exit(json.dumps({
    "status": "success",
    "mode": mode_name,
    "processed_table": processed_table
}))
