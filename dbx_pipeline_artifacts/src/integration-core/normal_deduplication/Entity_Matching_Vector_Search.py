# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

from uuid import uuid4
import builtins
import json
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from databricks.sdk import WorkspaceClient
import requests
from dbruntime.databricks_repl_context import get_context
import re
import time as _time

# COMMAND ----------

dbutils.widgets.text("embedding_endpoint", "", "Embedding Endpoint Name")
dbutils.widgets.text("experiment_id", "", "Match Maven Experiment")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("attributes", "", "Merged Description Attributes")
dbutils.widgets.dropdown("embedding_model_source", "databricks_foundation", ["databricks_custom_hugging_face", "databricks_foundation"], "Embedding Model Source")
dbutils.widgets.text("catalog_name", "", "catalog name")
dbutils.widgets.text("vs_endpoint", "", "vs endpointname")
dbutils.widgets.text("max_potential_matches", "", "Max Potential Matches")
dbutils.widgets.text("vs_max_workers", "2", "VS Max Workers (ThreadPool)")
dbutils.widgets.text("vs_max_concurrency", "50", "VS Max Concurrent Calls (Partitions)")
dbutils.widgets.text("vs_max_retries", "2", "VS Max Retries")
dbutils.widgets.dropdown("vs_strict_match_count", "false", ["true", "false"], "Require exact match count from VS")
dbutils.widgets.dropdown("vs_endpoint_type", "STANDARD", ["STANDARD", "STORAGE_OPTIMIZED"], "VS Endpoint Type")
dbutils.widgets.text("vs_index_version", "v2", "VS Index Version (precomputed)")

# COMMAND ----------

embedding_endpoint = dbutils.widgets.get("embedding_endpoint")
attributes = dbutils.widgets.get("attributes")
experiment_id = dbutils.widgets.get("experiment_id")
entity = dbutils.widgets.get("entity")
merged_desc_column = "attributes_combined"
embedding_vector_column = "attributes_combined_embedding"
embedding_model_source = dbutils.widgets.get("embedding_model_source")
catalog_name = dbutils.widgets.get("catalog_name")
vs_endpoint = dbutils.widgets.get("vs_endpoint")
max_potential_matches = dbutils.widgets.get("max_potential_matches")
vs_max_workers = int(dbutils.widgets.get("vs_max_workers") or "2")
vs_max_concurrency = int(dbutils.widgets.get("vs_max_concurrency") or "50")
vs_max_retries = int(dbutils.widgets.get("vs_max_retries") or "2")
vs_strict_match_count = dbutils.widgets.get("vs_strict_match_count").lower() == "true"
vs_endpoint_type = dbutils.widgets.get("vs_endpoint_type") or "STANDARD"
vs_index_version = dbutils.widgets.get("vs_index_version") or "v2"
try:
    run_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().toString()
except Exception:
    import uuid
    run_id = str(uuid.uuid4())

# COMMAND ----------

entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
vs_endpoint = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="vs_endpoint", debugValue=vs_endpoint)
embedding_endpoint = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="embedding_model_endpoint", debugValue=embedding_endpoint)
attributes = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="match_attributes", debugValue=attributes)
max_potential_matches = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="max_potential_matches", debugValue=max_potential_matches)
embedding_model_source = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="embedding_model_source", debugValue=embedding_model_source)
embedding_mode = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="embedding_mode", debugValue="managed")
vs_endpoint_type = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="vs_endpoint_type", debugValue=vs_endpoint_type)
vs_index_version = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="vs_index_version", debugValue=vs_index_version)

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

# MAGIC %run ../../utils/vs_utils

# COMMAND ----------

try:
    embedding_endpoint_from_mapping = dbutils.jobs.taskValues.get(
        taskKey="Endpoints_Mapping", 
        key="embedding_model_endpoint", 
        debugValue=None
    )
    
    # Use mapped endpoints if available
    if embedding_endpoint_from_mapping:
        embedding_endpoint = embedding_endpoint_from_mapping
        
except Exception as e:
    logger.info(f"Endpoints_Mapping task not available, falling back to model source endpoints: {e}")

# Fallback to foundation or hugging face endpoints if not set
if not embedding_endpoint:
    if embedding_model_source == "databricks_foundation":
        embedding_endpoint = dbutils.jobs.taskValues.get(
            taskKey="Embedding_Foundational_Serving_Endpoint", 
            key="embedding_model_endpoint", 
            debugValue=embedding_endpoint
        )
    else:
        embedding_endpoint = dbutils.jobs.taskValues.get(
            taskKey="Embedding_Hugging_Face_Serving_Endpoint", 
            key="embedding_model_endpoint", 
            default="mpnetv2",
            debugValue=embedding_endpoint
        )

# COMMAND ----------

attributes = json.loads(attributes)

# COMMAND ----------

unified_id_key = "surrogate_key"  # Key for unified table
master_id_key = "lakefusion_id"   # Key for master table

# COMMAND ----------

unified_table = f"{catalog_name}.silver.{entity}_unified"
processed_unified_table = f"{catalog_name}.silver.{entity}_processed_unified"
master_table = f"{catalog_name}.gold.{entity}_master"

if experiment_id:
    unified_table += f"_{experiment_id}"
    processed_unified_table += f"_{experiment_id}"
    master_table += f"_{experiment_id}"

# Parse master table components
master_table_list = master_table.split(".")
master_catalog, master_schema, master_table_name = master_table_list[0], master_table_list[1], master_table_list[2]

# Hoist vs_temp_table name so cleanup can reference it before Step 7
vs_temp_table = (
    f"{catalog_name}.silver.{entity}_vs_temp_{embedding_mode}_{experiment_id}"
    if experiment_id
    else f"{catalog_name}.silver.{entity}_vs_temp_{embedding_mode}"
)

# COMMAND ----------

logger.info("="*60)
logger.info("VECTOR SEARCH - ENTITY MATCHING")
logger.info("="*60)
logger.info(f"Entity: {entity}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment: {experiment_id if experiment_id else 'prod'}")
logger.info(f"Unified Table: {unified_table}")
logger.info(f"Master Table: {master_table}")
logger.info(f"Unified ID Key: {unified_id_key}")
logger.info(f"Master ID Key: {master_id_key}")
logger.info(f"Embedding Endpoint: {embedding_endpoint}")
logger.info(f"VS Endpoint: {vs_endpoint}")
logger.info(f"Max Potential Matches: {max_potential_matches}")
logger.info(f"VS Max Retries: {vs_max_retries}")
logger.info(f"VS Strict Match Count: {vs_strict_match_count}")
logger.info(f"VS Max Workers: {vs_max_workers}")
logger.info(f"VS Max Concurrency: {vs_max_concurrency}")
logger.info("="*60)

# COMMAND ----------

# MAGIC %run ../../utils/model_serving

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("DEFENSIVE CLEANUP: remove stale temp tables from prior runs")
logger.info("="*60)

skip_vs_step = False

try:
    spark.sql(f"DROP TABLE IF EXISTS {vs_temp_table}_retry")

    if not spark.catalog.tableExists(vs_temp_table):
        logger.info(f"  No existing {vs_temp_table}, will populate fresh")
    else:
        existing_count = spark.table(vs_temp_table).count()
        logger.info(f"  Existing {vs_temp_table} has {existing_count:,} rows")

        reuse = True
        drop_reason = None

        if existing_count == 0:
            reuse = False
            drop_reason = "empty"

        if reuse:
            pending_count = spark.table(unified_table).filter(
                (col("record_status") == "ACTIVE") & (col("search_results") == "")
            ).count()
            if existing_count != pending_count:
                reuse = False
                drop_reason = f"row count mismatch (temp={existing_count:,}, pending={pending_count:,})"

        if reuse:
            logger.info(f"  Reusing existing VS results, skipping Step 7")
            skip_vs_step = True
        else:
            logger.info(f"  Dropping stale temp: {drop_reason}")
            spark.sql(f"DROP TABLE IF EXISTS {vs_temp_table}")

except Exception as e:
    logger.warning(f"  Defensive cleanup failed (non-fatal): {e}")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 1: VALIDATE EMBEDDING ENDPOINT")
logger.info("="*60)

try:
    wait_until_serving_endpoint_ready(endpoint_name=embedding_endpoint, timeout_minutes=5)
    logger.info(f"Embedding endpoint '{embedding_endpoint}' is ready")
except ValueError as e:
    error_msg = f"ERROR: Embedding endpoint not ready: {str(e)}"
    logger.error(error_msg)
    logger_instance.shutdown()
    dbutils.notebook.exit(error_msg)

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 2: SETUP VECTOR SEARCH ENDPOINT")
logger.info("="*60)

def get_or_create_vs_endpoint(client, endpoint_name, endpoint_type="STANDARD"):
    """Create the VS endpoint if it doesn't already exist (STANDARD or STORAGE_OPTIMIZED)."""
    try:
        response = client.list_endpoints()
        existing = [ep['name'] for ep in response.get('endpoints', [])]
    except Exception as e:
        logger.info(f"Could not fetch Vector Search endpoints: {e}")
        existing = []
    if endpoint_name in existing:
        logger.info(f"Endpoint '{endpoint_name}' already exists. Skipping creation.")
        return
    try:
        logger.info(f"Creating new endpoint '{endpoint_name}' (type={endpoint_type}) ...")
        client.create_endpoint(name=endpoint_name, endpoint_type=endpoint_type)
        logger.info(f"Endpoint '{endpoint_name}' created successfully ({endpoint_type}).")
    except Exception as e:
        logger.info(f"Exception occurred while creating the Vector Search endpoint: {e}")


client = create_vs_client(disable_notice=True)
get_or_create_vs_endpoint(client, vs_endpoint, vs_endpoint_type)

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 3: WAIT FOR VS ENDPOINT")
logger.info("="*60)

try:
    wait_until_vector_search_ready(endpoint_name=vs_endpoint, client=client)
    logger.info(f"Vector Search endpoint '{vs_endpoint}' is ready")
except ValueError as e:
    error_msg = f"ERROR: Vector search endpoint not ready: {str(e)}"
    logger.error(error_msg)
    logger_instance.shutdown()
    dbutils.notebook.exit(error_msg)

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 4: CREATE/SYNC VECTOR SEARCH INDEX")
logger.info("="*60)

import time

index_name = f"{master_table}_index_{vs_index_version}" if embedding_mode == "precomputed" else f"{master_table}_index"

# Precomputed mode needs the embedding dimension produced by the upstream Compute_Embeddings task.
embedding_dim = None
if embedding_mode == "precomputed":
    try:
        embedding_dim_raw = dbutils.jobs.taskValues.get(
            taskKey="Compute_Embeddings", key="embedding_dim", debugValue=None
        )
    except Exception as e:
        # "task key does not exist" => Compute_Embeddings isn't in this job's DAG (e.g. a job
        # created before 4.3.0; the DAG is fixed at job-creation time). Rebuild the DAG and re-run.
        if "task key does not exist" in str(e).lower():
            raise RuntimeError(
                "embedding_mode='precomputed' but the Compute_Embeddings task is not in this "
                "job's DAG. Rebuild the DAG and re-run."
            ) from e
        raise
    if embedding_dim_raw is None:
        raise RuntimeError(
            "embedding_mode='precomputed' requires Compute_Embeddings task to set embedding_dim. "
            "Check DAG ordering -- Compute_Embeddings must run before this task."
        )
    embedding_dim = int(embedding_dim_raw)

# Full create -> register -> provision-wait -> sync -> sync-wait (shared; see utils/vs_utils).
index = create_and_sync_index(
    client, spark,
    endpoint_name=vs_endpoint,
    index_name=index_name,
    source_table=master_table,
    primary_key=master_id_key,
    embedding_mode=embedding_mode,
    embedding_vector_column=embedding_vector_column,
    embedding_dimension=embedding_dim,
    embedding_source_column=merged_desc_column,
    embedding_model_endpoint=embedding_endpoint,
    endpoint_type=vs_endpoint_type,
)


# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 5: FILTER RECORDS FOR VECTOR SEARCH")
logger.info("="*60)

from pyspark.sql.functions import col

# Read unified table
df_unified = spark.table(unified_table)

# CRITICAL: Filter for records with ACTIVE status AND empty search_results
# These are the records that need to be matched against master
select_cols = [unified_id_key, merged_desc_column]
if embedding_mode == "precomputed":
    select_cols.append(embedding_vector_column)

df_pending_search = df_unified.filter(
    (col("record_status") == "ACTIVE") &
    (col("search_results") == "")
).select(*select_cols)

is_empty = df_pending_search.isEmpty()
logger.info(f"Records pending search (ACTIVE + empty search_results): {'none' if is_empty else 'found'}")

if is_empty:
    logger.info("\nNo records to process - all ACTIVE records already have search results")
    logger.info("\n" + "="*60)
    logger.info("EXITING - NO RECORDS TO SEARCH")
    logger.info("="*60)
    
    dbutils.jobs.taskValues.set("vector_search_complete", True)
    dbutils.jobs.taskValues.set("records_searched", 0)
    
    logger_instance.shutdown()
    dbutils.notebook.exit(json.dumps({
        "status": "skipped",
        "message": "No ACTIVE records without search results",
        "records_searched": 0
    }))

# COMMAND ----------

def get_optimal_partitions(row_count: int, max_concurrent_vs_calls: int = 50) -> int:
    """
    Calculate partition count based on VS endpoint capacity, not data size.

    With max_workers=1 on serverless, partitions ≈ max concurrent VS calls.
    Serverless scales resources proportional to partition count, so capping
    partitions prevents overwhelming the VS endpoint with 429s.

    Args:
        row_count: Total number of rows to process
        max_concurrent_vs_calls: Max partitions (= max concurrent VS calls on serverless).
                                 Default 50 — safe for standard VS endpoints.
                                 TODO: Make this configurable per entity/workspace via model JSON
                                 once we know the endpoint's actual QPS capacity.
    Returns:
        Optimal partition count
    """
    min_partitions = 1
    # Aim for ~500 rows per partition, but never exceed the VS endpoint cap
    calculated = builtins.max(min_partitions, row_count // 500)
    return builtins.min(max_concurrent_vs_calls, calculated)
# COMMAND ----------

row_count = df_pending_search.count()
optimal_partitions = get_optimal_partitions(row_count, vs_max_concurrency)
logger.info(f"Records: {row_count} | Partitions: {optimal_partitions} | ~{row_count // builtins.max(1, optimal_partitions)} rows/partition")

# COMMAND ----------

df_pending_search = df_pending_search.repartition(optimal_partitions)

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 6: DEFINE VECTOR SEARCH UDF")
logger.info("="*60)

from databricks.vector_search.client import VectorSearchClient
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, FloatType
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# UDF schema: array of struct with text, lakefusion_id, score, and optional error field
result_schema = ArrayType(StructType([
    StructField("text", StringType()),
    StructField("lakefusion_id", StringType()),
    StructField("score", FloatType()),
    StructField("error", StringType(), nullable=True)
]))
# Index name for VS lookups — actual client/index created inside UDF per-batch
# to avoid stale/cached state from driver-level serialization
_vs_index_name = index_name

if embedding_mode == "precomputed":
    @pandas_udf(result_schema)
    def vector_search_results_udf(embeddings: pd.Series) -> pd.Series:
        _client = create_vs_client()
        _index = _client.get_index(index_name=_vs_index_name)

        def get_results(embedding):
            try:
                query_vec = embedding.tolist() if hasattr(embedding, "tolist") else list(embedding)
                results = _index.similarity_search(
                    query_vector=query_vec,
                    columns=[merged_desc_column, master_id_key],
                    num_results=int(max_potential_matches)
                )

                data = results.get("result", {}).get("data_array", None)
                if not data:
                    raise ValueError(f"Empty VS response: {str(results)[:500]}")

                formatted = []
                for r in data:
                    formatted.append({
                        "text": r[0],
                        "lakefusion_id": r[1],
                        "score": float(r[2]),
                        "error": None
                    })
                return formatted

            except Exception as e:
                error_str = str(e)[:500]
                return [{"text": "", "lakefusion_id": "", "score": 0.0, "error": error_str}]

        if vs_max_workers > 1:
            with ThreadPoolExecutor(max_workers=vs_max_workers) as executor:
                results = list(executor.map(get_results, embeddings))
        else:
            results = [get_results(e) for e in embeddings]
        return pd.Series(results)
else:
    @pandas_udf(result_schema)
    def vector_search_results_udf(descriptions: pd.Series) -> pd.Series:
        # Create fresh client + index per batch to avoid serialization/caching issues
        _client = create_vs_client()
        _index = _client.get_index(index_name=_vs_index_name)

        def get_results(text):
            try:
                results = _index.similarity_search(
                    query_text=text,
                    columns=[merged_desc_column, master_id_key],
                    num_results=int(max_potential_matches)
                )

                data = results.get("result", {}).get("data_array", None)
                if not data:
                    raise ValueError(f"Empty VS response: {str(results)[:500]}")

                formatted = []
                for r in data:
                    formatted.append({
                        "text": r[0],
                        "lakefusion_id": r[1],
                        "score": float(r[2]),
                        "error": None
                    })
                return formatted

            except Exception as e:
                error_str = str(e)[:500]
                return [{"text": "", "lakefusion_id": "", "score": 0.0, "error": error_str}]

        if vs_max_workers > 1:
            with ThreadPoolExecutor(max_workers=vs_max_workers) as executor:
                results = list(executor.map(get_results, descriptions))
        else:
            results = [get_results(desc) for desc in descriptions]
        return pd.Series(results)

logger.info("Vector search UDF defined")
logger.info(f"  - Searches against: {master_table}")
logger.info(f"  - Returns top {max_potential_matches} matches per record")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 7: EXECUTE VECTOR SEARCH")
logger.info("="*60)

# Guarantee the unified error table exists before scoring starts. Lazy creation
# inside log_errors() left a gap: a VS run with zero errors created no table,
# and the downstream LLM layer then had no table to append 429 failures to.
from lakefusion_core_engine.services.unified_error_handler import UnifiedErrorHandler
UnifiedErrorHandler(spark, unified_table).ensure_table()

if skip_vs_step:
    logger.info("Reusing VS results from prior run (skip_vs_step=True)")
    df_with_results = spark.table(vs_temp_table)
else:
    logger.info("Running vector search (this may take a while for large datasets)...")

    input_col = embedding_vector_column if embedding_mode == "precomputed" else merged_desc_column
    df_with_results = df_pending_search.withColumn(
        "search_results_array",
        vector_search_results_udf(df_pending_search[input_col])
    )

    # Materialize VS results (array form) to temp table.
    # String conversion happens AFTER validation+retries, right before the merge.
    df_with_results.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(vs_temp_table)
    df_with_results = spark.table(vs_temp_table)
    logger.info(f"VS results materialized to {vs_temp_table}")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 7.5: VALIDATE & RETRY BAD VS RESULTS")
logger.info("="*60)

from pyspark.sql.functions import expr, size as spark_size, try_element_at, coalesce, to_json, array, sum as spark_sum

MAX_VS_RETRIES = vs_max_retries
expected_result_count = int(max_potential_matches)

def is_bad_vs_result():
    """Native Spark expression to identify bad VS results. No Python UDF needed.
    Uses the dedicated 'error' field in the struct for error detection."""
    first_error = try_element_at(col("search_results_array.error"), lit(1))
    arr_size = spark_size(coalesce(col("search_results_array"), array()))
    bad_expr = (
        col("search_results_array").isNull() |
        (arr_size == 0) |
        first_error.isNotNull()
    )
    if vs_strict_match_count:
        bad_expr = bad_expr | (arr_size < expected_result_count)
    return bad_expr

# Initial pass: compute is_bad once and persist to the temp table
df_initial = spark.table(vs_temp_table).withColumn("is_bad", is_bad_vs_result())
df_initial.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(vs_temp_table)

for retry_attempt in range(1, MAX_VS_RETRIES + 1):
    stats = spark.table(vs_temp_table).agg(
        count("*").alias("total"),
        spark_sum(col("is_bad").cast("int")).alias("bad")
    ).collect()[0]
    bad_count, total_rows = stats["bad"], stats["total"]

    if bad_count == 0:
        logger.info(f"Retry check {retry_attempt}: No bad results. Validation passed.")
        break

    bad_pct = (bad_count / total_rows * 100) if total_rows > 0 else 0
    logger.warning(
        f"Retry {retry_attempt}/{MAX_VS_RETRIES}: {bad_count:,} bad VS results "
        f"({bad_pct:.1f}% of {total_rows:,} total)"
    )

    df_bad = spark.table(vs_temp_table).filter(col("is_bad"))
    bad_select_cols = [unified_id_key, merged_desc_column]
    if embedding_mode == "precomputed":
        bad_select_cols.append(embedding_vector_column)
    df_bad_input = df_bad.select(*bad_select_cols)

    # Re-run UDF and recompute is_bad for retried rows only
    retry_input_col = embedding_vector_column if embedding_mode == "precomputed" else merged_desc_column
    df_retried = df_bad_input.withColumn(
        "search_results_array",
        vector_search_results_udf(df_bad_input[retry_input_col])
    ).withColumn("is_bad", is_bad_vs_result())

    df_retried.write.format("delta").mode("overwrite").saveAsTable(f"{vs_temp_table}_retry")
    df_retry_source = spark.table(f"{vs_temp_table}_retry")

    temp_delta = DeltaTable.forName(spark, vs_temp_table)
    temp_delta.alias("target").merge(
        source=df_retry_source.alias("source"),
        condition=f"target.{unified_id_key} = source.{unified_id_key}"
    ).whenMatchedUpdate(
        set={
            "search_results_array": "source.search_results_array",
            "is_bad": "source.is_bad"
        }
    ).execute()

    logger.info(f"Retry {retry_attempt}: Re-evaluated {bad_count:,} records")

# Final quality report — is_bad is already persisted, just aggregate
final_stats = spark.table(vs_temp_table).agg(
    count("*").alias("total"),
    spark_sum(col("is_bad").cast("int")).alias("bad")
).collect()[0]
final_bad_count, final_total = final_stats["bad"], final_stats["total"]
final_good = final_total - final_bad_count
final_bad_pct = (final_bad_count / final_total * 100) if final_total > 0 else 0

logger.info("\n" + "-" * 60)
logger.info("VS RESULT QUALITY REPORT")
logger.info("-" * 60)
logger.info(f"  Total records:       {final_total:,}")
logger.info(f"  Good results:        {final_good:,} ({100 - final_bad_pct:.1f}%)")
logger.info(f"  Bad results (nulled): {final_bad_count:,} ({final_bad_pct:.1f}%)")
logger.info("-" * 60)

if final_bad_count > 0:
    df_final_bad = spark.table(vs_temp_table).filter(col("is_bad"))

    # Build error message from the structured error field
    first_error = try_element_at(col("search_results_array.error"), lit(1))
    arr_size = spark_size(coalesce(col("search_results_array"), array()))
    df_vs_errors = df_final_bad.withColumn(
        "error_message",
        when(col("search_results_array").isNull(), lit("VS error: null result"))
        .when(first_error.isNotNull(), concat(lit("VS error: "), first_error))
        .otherwise(
              concat(lit("VS incomplete: expected "), lit(expected_result_count),
                     lit(" results, got "), arr_size.cast("string"))
        )
    )

    from lakefusion_core_engine.services.unified_error_handler import UnifiedErrorHandler
    unified_error_handler = UnifiedErrorHandler(spark, unified_table)
    unified_error_handler.log_errors(
        df_vs_errors.select(
            col(unified_id_key).alias("surrogate_key"),
            col("error_message")
        ),
        stage="VECTOR_SEARCH",
        run_id=run_id
    )
    logger.info(f"Logged {final_bad_count:,} VS errors to unified error table")

    # Null out the array so these records are excluded downstream
    logger.warning(f"Nulling out {final_bad_count:,} bad VS results after {MAX_VS_RETRIES} retries")
    temp_delta = DeltaTable.forName(spark, vs_temp_table)
    temp_delta.alias("target").merge(
        source=df_final_bad.select(unified_id_key).alias("source"),
        condition=f"target.{unified_id_key} = source.{unified_id_key}"
    ).whenMatchedUpdate(
        set={"search_results_array": lit(None)}
    ).execute()

# Retry table cleanup deferred to next run's defensive cleanup at notebook top

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 8: CONVERT TO STRING FORMAT, DEDUPLICATE & MERGE")
logger.info("="*60)

# Convert validated array results to JSON array-of-arrays: [[text, id, score], ...]
df_valid_results = spark.table(vs_temp_table).filter(
    col("search_results_array").isNotNull() &
    (spark_size(col("search_results_array")) > 0)
)

df_with_final_results = df_valid_results.withColumn(
    "search_results",
    to_json(expr("""
        transform(search_results_array, x -> array(
            coalesce(x.text, ''),
            coalesce(x.lakefusion_id, ''),
            cast(coalesce(x.score, 0.0) as string)
        ))
    """))
)

logger.info(f"Converting {final_good:,} valid records to JSON string format")

# Deduplicate and drop internal columns before merge
df_deduplicated = df_with_final_results.drop("is_bad", "search_results_array").dropDuplicates([unified_id_key])

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 9: MERGE RESULTS TO UNIFIED TABLE")
logger.info("="*60)

delta_table = DeltaTable.forName(spark, unified_table)

delta_table.alias("target").merge(
    source=df_deduplicated.alias("source"),
    condition=f"target.{unified_id_key} = source.{unified_id_key}"
).whenMatchedUpdate(
    set={"search_results": "source.search_results"}
).execute()

logger.info(f"Merge completed")

# Cleanup temp table
spark.sql(f"DROP TABLE IF EXISTS {vs_temp_table}")
logger.info(f"Cleaned up temp table {vs_temp_table}")

# COMMAND ----------

dbutils.jobs.taskValues.set("vector_search_complete", True)

logger.info("\n" + "="*60)
logger.info("VECTOR SEARCH COMPLETED SUCCESSFULLY")
logger.info("="*60)
