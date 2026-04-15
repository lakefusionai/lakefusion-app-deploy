# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# DBTITLE 1,Import Libraries
from uuid import uuid4
import json
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from databricks.sdk import WorkspaceClient
import requests
from dbruntime.databricks_repl_context import get_context
import re
import time

# COMMAND ----------

# DBTITLE 1,Define Widgets
dbutils.widgets.text("embedding_endpoint", "", "Embedding Endpoint Name")
dbutils.widgets.text("experiment_id", "", "Match Maven Experiment")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("attributes", "", "Merged Description Attributes")
dbutils.widgets.dropdown("embedding_model_source", "databricks_foundation", ["databricks_custom_hugging_face", "databricks_foundation"], "Embedding Model Source")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("vs_endpoint", "", "VS Endpoint Name")
dbutils.widgets.text("max_potential_matches", "5", "Max Potential Matches")
dbutils.widgets.text("is_single_source", "", "Is Single Source")
dbutils.widgets.text("vs_max_workers", "1", "VS Max Workers (ThreadPool)")
dbutils.widgets.text("vs_max_concurrency", "50", "VS Max Concurrent Calls (Partitions)")

# COMMAND ----------

# DBTITLE 1,Get Widget Values
embedding_endpoint = dbutils.widgets.get("embedding_endpoint")
attributes = dbutils.widgets.get("attributes")
experiment_id = dbutils.widgets.get("experiment_id")
entity = dbutils.widgets.get("entity")
merged_desc_column = "attributes_combined"
embedding_model_source = dbutils.widgets.get("embedding_model_source")
catalog_name = dbutils.widgets.get("catalog_name")
vs_endpoint = dbutils.widgets.get("vs_endpoint")
max_potential_matches = dbutils.widgets.get("max_potential_matches")
is_single_source = dbutils.widgets.get("is_single_source")
vs_max_workers = int(dbutils.widgets.get("vs_max_workers") or "1")
vs_max_concurrency = int(dbutils.widgets.get("vs_max_concurrency") or "50")
try:
    run_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().toString()
except Exception:
    import uuid
    run_id = str(uuid.uuid4())

# COMMAND ----------

# DBTITLE 1,Get Task Values from Parse_Entity_Model_JSON
entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
vs_endpoint = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="vs_endpoint", debugValue=vs_endpoint)
embedding_endpoint = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="embedding_model_endpoint", debugValue=embedding_endpoint)
attributes = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="match_attributes", debugValue=attributes)
max_potential_matches = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="max_potential_matches", debugValue=max_potential_matches)
embedding_model_source = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="embedding_model_source", debugValue=embedding_model_source)
is_single_source = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="is_single_source", debugValue=is_single_source)

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

# DBTITLE 1,Get Endpoint from Endpoints_Mapping
try:
    embedding_endpoint_from_mapping = dbutils.jobs.taskValues.get(
        taskKey="Endpoints_Mapping", 
        key="embedding_model_endpoint", 
        debugValue=None
    )
    
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

# DBTITLE 1,Parse Parameters
attributes = json.loads(attributes) if isinstance(attributes, str) else attributes
max_potential_matches = int(max_potential_matches) if max_potential_matches else 5

# COMMAND ----------

# Convert is_single_source to boolean
if isinstance(is_single_source, str):
    is_single_source = is_single_source.lower() == "true"
else:
    is_single_source = bool(is_single_source)

# COMMAND ----------

# DBTITLE 1,Define Table Names and Keys Based on Source Type
master_id_key = "lakefusion_id"

# Define table names
master_table = f"{catalog_name}.gold.{entity}_master"
unified_table = f"{catalog_name}.silver.{entity}_unified"
unified_dedup_table = f"{catalog_name}.silver.{entity}_unified_deduplicate"

# Append experiment_id
if experiment_id:
    master_table += f"_{experiment_id}"
    unified_table += f"_{experiment_id}"
    unified_dedup_table += f"_{experiment_id}"

# ============================================================
# SET SOURCE TABLE AND KEY BASED ON SOURCE TYPE
# ============================================================
if is_single_source:
    # GOLDEN DEDUP: Query unified_deduplicate, search against master index
    # unified_deduplicate uses lakefusion_id as key (cloned from master)
    source_table = unified_dedup_table
    source_id_key = master_id_key  # lakefusion_id
    mode_name = "GOLDEN DEDUP (Single Source)"
    filter_by_status = False  # unified_dedup has no record_status
    exclude_self_matches = True  # Must exclude self-matches
else:
    # NORMAL DEDUP: Query unified, search against master index
    # unified uses surrogate_key as key
    source_table = unified_table
    source_id_key = "surrogate_key"
    mode_name = "NORMAL DEDUP (Multi Source)"
    filter_by_status = True  # Filter by record_status = 'ACTIVE'
    exclude_self_matches = False  # No self-matches possible

# COMMAND ----------

# DBTITLE 1,Print Configuration
logger.info("=" * 70)
logger.info(f"EXPERIMENT VECTOR SEARCH - {mode_name}")
logger.info("=" * 70)
logger.info(f"Entity: {entity}")
logger.info(f"Catalog: {catalog_name}")
logger.info(f"Experiment ID: {experiment_id}")
logger.info(f"Is Single Source: {is_single_source}")
logger.info("-" * 70)
logger.info(f"Source Table: {source_table}")
logger.info(f"Source ID Key: {source_id_key}")
logger.info(f"Master Table: {master_table}")
logger.info(f"Master ID Key: {master_id_key}")
logger.info("-" * 70)
logger.info(f"Embedding Endpoint: {embedding_endpoint}")
logger.info(f"VS Endpoint: {vs_endpoint}")
logger.info(f"Max Potential Matches: {max_potential_matches}")
logger.info(f"Filter by Status: {filter_by_status}")
logger.info(f"Exclude Self-Matches: {exclude_self_matches}")
logger.info("=" * 70)

# COMMAND ----------

# DBTITLE 1,Process Unified Dedup Table (Single Source Only)
# For single source (golden dedup), we need to clone master to unified_deduplicate
# This allows records to be compared against themselves (excluding exact self-matches)

if is_single_source:
    logger.info("\n" + "=" * 70)
    logger.info("STEP 0: PROCESS UNIFIED DEDUP TABLE (Single Source)")
    logger.info("=" * 70)
    
    # Check if unified_dedup table exists
    unified_dedup_exists = spark.catalog.tableExists(unified_dedup_table)
    
    if unified_dedup_exists:
        existing_count = spark.table(unified_dedup_table).count()
        logger.info(f"Unified dedup table exists with {existing_count} records")
    else:
        logger.info(f"Creating unified dedup table: {unified_dedup_table}")
    
    # Read master table
    df_master = spark.table(master_table)
    # master_count = df_master.count()
    # print(f"✓ Master table has {master_count} records")
    
    # Clone master to unified_deduplicate with additional columns for VS/LLM processing
    # Add search_results and scoring_results columns if they don't exist
    df_unified_dedup = df_master.withColumn("search_results", lit("")).withColumn("scoring_results", lit(""))
    
    # Write to unified_dedup table (overwrite to ensure fresh state for experiments)
    df_unified_dedup.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(unified_dedup_table)
    
    #final_count = spark.table(unified_dedup_table).count()
    #print(f"✓ Cloned {final_count} records from master to unified_dedup")
    logger.info(f"  Source: {master_table}")
    logger.info(f"  Target: {unified_dedup_table}")
else:
    logger.info("\n" + "=" * 70)
    logger.info("STEP 0: SKIPPED (Multi Source - Using Unified Table)")
    logger.info("=" * 70)
    logger.info(f"  Multi-source mode uses unified table directly: {source_table}")

# COMMAND ----------

# MAGIC %run ../../utils/model_serving

# COMMAND ----------

# DBTITLE 1,Step 1: Validate Embedding Endpoint
logger.info("\n" + "=" * 70)
logger.info("STEP 1: VALIDATE EMBEDDING ENDPOINT")
logger.info("=" * 70)

try:
    wait_until_serving_endpoint_ready(endpoint_name=embedding_endpoint, timeout_minutes=5)
    logger.info(f"Embedding endpoint '{embedding_endpoint}' is ready")
except ValueError as e:
    error_msg = f"ERROR: Embedding endpoint not ready: {str(e)}"
    logger.error(error_msg)
    raise ValueError(error_msg)

# COMMAND ----------

# MAGIC %run ../../utils/vs_utils

# COMMAND ----------

# DBTITLE 1,Step 2: Setup Vector Search Endpoint
logger.info("\n" + "=" * 70)
logger.info("STEP 2: SETUP VECTOR SEARCH ENDPOINT")
logger.info("=" * 70)

from databricks.vector_search.client import VectorSearchClient

client = create_vs_client(disable_notice=True)

try:
    response = client.list_endpoints()
    existing_endpoints = [ep['name'] for ep in response.get('endpoints', [])]
except Exception as e:
    logger.info(f"Could not fetch Vector Search endpoints: {e}")
    existing_endpoints = []

if vs_endpoint in existing_endpoints:
    logger.info(f"Endpoint '{vs_endpoint}' already exists. Skipping creation.")
else:
    try:
        logger.info(f"Creating new endpoint '{vs_endpoint}' ...")
        client.create_endpoint(
            name=vs_endpoint,
            endpoint_type="STANDARD"
        )
        logger.info(f"Endpoint '{vs_endpoint}' created successfully.")
    except Exception as e:
        logger.info(f"Exception occurred while creating the Vector Search endpoint: {e}")

# COMMAND ----------

# DBTITLE 1,Step 3: Wait for VS Endpoint
logger.info("\n" + "=" * 70)
logger.info("STEP 3: WAIT FOR VS ENDPOINT")
logger.info("=" * 70)

try:
    wait_until_vector_search_ready(endpoint_name=vs_endpoint, client=client)
    logger.info(f"Vector Search endpoint '{vs_endpoint}' is ready")
except ValueError as e:
    error_msg = f"ERROR: Vector search endpoint not ready: {str(e)}"
    logger.error(error_msg)
    raise ValueError(error_msg)

# COMMAND ----------

# DBTITLE 1,Step 4: Create/Sync Vector Search Index - New
logger.info("\n" + "=" * 70)
logger.info("STEP 4: CREATE/SYNC VECTOR SEARCH INDEX ON MASTER TABLE")
logger.info("=" * 70)

import time

index = None
index_name = f"{master_table}_index"

# Configuration constants
REGISTRATION_TIMEOUT = 1800   # 30 minutes for index registration
MAX_WAIT_SECONDS = 3 * 60 * 60  # 3 hours
POLL_INTERVAL = 20
MAX_BACKEND_ERRORS = 10
MAX_PIPELINE_FAILURES = 3

# -------------------------------------------------
# STEP 4.0: Warm up embedding endpoint before index creation
# -------------------------------------------------
logger.info("Warming up embedding endpoint before index creation...")
try:
    warm_up_embedding_endpoint(
        embedding_endpoint=embedding_endpoint,
        max_retries=10,
        retry_interval_seconds=60,
        timeout_minutes=10
    )
    logger.info(f"Embedding endpoint '{embedding_endpoint}' is warm and ready")
except Exception as e:
    logger.warning(f"Embedding warm-up failed: {e}. Proceeding with index creation anyway...")

# -------------------------------------------------
# STEP 4.1: Create index (tolerant approach)
# -------------------------------------------------
try:
    logger.info(f"Attempting to create index: {index_name}")
    index = client.create_delta_sync_index(
        endpoint_name=vs_endpoint,
        source_table_name=master_table,
        index_name=index_name,
        columns_to_sync=[master_id_key, merged_desc_column],
        pipeline_type="TRIGGERED",
        primary_key=master_id_key,
        embedding_source_column=merged_desc_column,
        embedding_model_endpoint_name=embedding_endpoint,
        sync_computed_embeddings=True,
    )
    logger.info(f"Create request submitted for index: {index_name}")

except Exception as e:
    error_msg = str(e)
    error_lower = error_msg.lower()

    # Case 1: Timeout/temporarily unavailable - creation may still be in progress
    if ("temporarily_unavailable" in error_lower or
        "504" in error_lower or
        "taking too long" in error_lower or
        "timeout" in error_lower):
        logger.warning(f"Create request timed out — creation may still be in progress: {error_msg}")
        # Don't raise, proceed to wait loop

    # Case 2: Index already exists - this is fine, proceed to wait loop
    elif "already exists" in error_lower:
        logger.info(f"Index already exists, proceeding to sync: {error_msg}")

    # Case 3: Permanent errors - fail immediately
    elif ("maximum number of indexes" in error_lower or
          "permission denied" in error_lower or
          "unauthorized" in error_lower):
        raise RuntimeError(f"Index creation failed: {error_msg}")

    # Case 4: Other unexpected errors - raise
    else:
        raise RuntimeError(f"Index creation failed unexpectedly: {error_msg}")

# -------------------------------------------------
# STEP 4.2: Wait for index registration
# -------------------------------------------------
logger.info("Waiting for index registration...")
start = time.time()

while index is None:
    if time.time() - start > REGISTRATION_TIMEOUT:
        raise TimeoutError(
            f"Index '{index_name}' not registered within {REGISTRATION_TIMEOUT}s"
        )

    try:
        index = client.get_index(
            endpoint_name=vs_endpoint,
            index_name=index_name,
        )
        logger.info("Index is now registered and accessible")

    except Exception as e:
        error_lower = str(e).lower()
        if "resource_does_not_exist" in error_lower or "not exist" in error_lower or "not_exist" in error_lower:
            logger.info("  Index not visible yet — waiting...")
            time.sleep(20)
        else:
            raise RuntimeError(f"Unexpected error while locating index: {e}")

# -------------------------------------------------
# STEP 4.3: Wait for index to be ready before sync
# -------------------------------------------------
logger.info("Waiting for index to be ready before sync...")
index.wait_until_ready()
logger.info("Index is ready")

# -------------------------------------------------
# STEP 4.4: Trigger sync (if not already running)
# -------------------------------------------------
# Log current status for debugging
index_status = index.describe()['status']
current_status = index_status.get('detailed_state', 'UNKNOWN')
logger.info(f"Current index status: {current_status}")

# Log additional details if pipeline failed
if current_status == "ONLINE_PIPELINE_FAILED":
    status_message = index_status.get('message', 'No message available')
    logger.warning(f"Pipeline failure details: {status_message}")
    logger.warning(f"  Full index status: {index_status}")

# Only trigger sync if not already running (can't sync while RUNNING)
if current_status in ["ONLINE_TRIGGERED_UPDATE", "ONLINE_UPDATING_PIPELINE_RESOURCES", "ONLINE_PIPELINE_FAILED"]:
    logger.info("Sync already in progress or failed. Skipping sync trigger...")
else:
    logger.info("Triggering index sync (with scale-from-zero retry handling)...")
    try:
        sync_index_with_retry(
            index=index,
            embedding_endpoint_name=embedding_endpoint,
            max_retries=10,
            retry_interval_seconds=60,
            timeout_minutes=10
        )
        logger.info("Index sync initiated successfully!")
    except Exception as sync_error:
        error_msg = f"ERROR: Index sync failed: {str(sync_error)}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)

# -------------------------------------------------
# STEP 4.5: Tolerant wait loop for sync completion
# -------------------------------------------------
logger.info("Waiting for index to be ready and synced (tolerant mode)...")

start_time = time.time()
consecutive_backend_errors = 0
pipeline_failed_count = 0

while True:
    try:
        index.wait_until_ready()

        status_info = index.describe()
        status = status_info["status"]["detailed_state"]
        message = status_info["status"].get("message", "")

        logger.info(f"  Index status: {status}")
        if message:
            logger.info(f"    Message: {message}")

        consecutive_backend_errors = 0

        # ✅ Success
        if status == "ONLINE_NO_PENDING_UPDATE":
            logger.info("Index is fully synced and ready.")
            break

        # 🔄 Actively updating - reset timer (no timeout while making progress)
        if status in ["ONLINE_TRIGGERED_UPDATE", "ONLINE_UPDATING_PIPELINE_RESOURCES"]:
            start_time = time.time()  # Reset timer since progress is being made
            logger.info("  Sync in progress, resetting timeout timer...")

        # ⚠️ Transient failure handling
        if status == "ONLINE_PIPELINE_FAILED":
            pipeline_failed_count += 1
            logger.info(
                f"⚠️ ONLINE_PIPELINE_FAILED detected "
                f"({pipeline_failed_count}/{MAX_PIPELINE_FAILURES})"
            )
            logger.warning(f"  Failure reason: {message}")

            # Check timeout only when in failed state
            elapsed = time.time() - start_time
            if elapsed > MAX_WAIT_SECONDS:
                raise TimeoutError(
                    f"Index sync timed out after {int(elapsed)} seconds in failed state"
                )

            if pipeline_failed_count >= MAX_PIPELINE_FAILURES:
                raise RuntimeError(
                    f"Index entered ONLINE_PIPELINE_FAILED state too many times. Last message: {message}"
                )

    except Exception as e:
        if isinstance(e, (TimeoutError, RuntimeError)):
            raise
        consecutive_backend_errors += 1
        logger.info(
            f"⚠️ Backend error tolerated "
            f"({consecutive_backend_errors}/{MAX_BACKEND_ERRORS}): {e}"
        )

        if consecutive_backend_errors >= MAX_BACKEND_ERRORS:
            raise RuntimeError(
                "Too many consecutive backend errors while waiting for index readiness"
            )

    time.sleep(POLL_INTERVAL)


# COMMAND ----------

# DBTITLE 1,Step 5: Filter Records for Vector Search
logger.info("\n" + "=" * 70)
logger.info("STEP 5: FILTER RECORDS FOR VECTOR SEARCH")
logger.info("=" * 70)

# Read source table
df_source = spark.table(source_table)

# Apply appropriate filter based on source type
if filter_by_status:
    # Normal dedup: filter by ACTIVE status and empty search_results
    df_pending_search = df_source.filter(
        (col("record_status") == "ACTIVE") & 
        (col("search_results") == "")
    ).select(source_id_key, merged_desc_column)
    
    # total_count = df_source.count()
    # active_count = df_source.filter(col('record_status') == 'ACTIVE').count()
    # print(f"✓ Source table total records: {total_count}")
    # print(f"✓ Records with ACTIVE status: {active_count}")
else:
    # Golden dedup: no record_status, just filter by empty search_results
    df_pending_search = df_source.filter(
        col("search_results") == ""
    ).select(source_id_key, merged_desc_column)
    
    # total_count = df_source.count()
    # print(f"✓ Source table total records: {total_count}")

pending_count = df_pending_search.isEmpty()
#pending_count = df_pending_search.count()
#print(f"✓ Records pending search (empty search_results): {pending_count}")

if pending_count:
    logger.info("\nNo records to process - all records already have search results")
    logger.info("\n" + "=" * 70)
    logger.info("EXITING - NO RECORDS TO SEARCH")
    logger.info("=" * 70)
    
    dbutils.jobs.taskValues.set("vector_search_complete", True)
    dbutils.jobs.taskValues.set("records_searched", 0)
    
    logger_instance.shutdown()
    dbutils.notebook.exit(json.dumps({
        "status": "skipped",
        "message": "No records without search results",
        "records_searched": 0
    }))
# Calculate optimal partitions
# num_partitions = __builtins__.max(100, pending_count // 3000)
# df_pending_search = df_pending_search.repartition(num_partitions, source_id_key)

#print(f"✓ Repartitioned to {num_partitions} partitions for parallel processing")

# COMMAND ----------

def get_optimal_partitions(row_count: int, max_concurrent_vs_calls: int = 50) -> int:
    """Partition based on VS endpoint capacity. See normal_dedup notebook for details."""
    min_partitions = 1
    calculated = __builtins__.max(min_partitions, row_count // 500)
    return __builtins__.min(max_concurrent_vs_calls, calculated)

# COMMAND ----------

row_count = df_pending_search.count()
optimal_partitions = get_optimal_partitions(row_count, vs_max_concurrency)
logger.info(f"Records: {row_count} | Partitions: {optimal_partitions} | ~{row_count // __builtins__.max(1, optimal_partitions)} rows/partition")

# COMMAND ----------

df_pending_search = df_pending_search.repartition(optimal_partitions, source_id_key)

# COMMAND ----------

df_pending_search.withColumn("pid", spark_partition_id()) \
        .groupBy("pid") \
        .agg(count("*").alias("rows")).show()

# COMMAND ----------

# DBTITLE 1,Step 6: Define Vector Search UDF
logger.info("\n" + "=" * 70)
logger.info("STEP 6: DEFINE VECTOR SEARCH UDF")
logger.info("=" * 70)

from databricks.vector_search.client import VectorSearchClient
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, FloatType
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# UDF schema
result_schema = ArrayType(StructType([
    StructField("text", StringType()),
    StructField("lakefusion_id", StringType()),
    StructField("score", FloatType())
]))

# Broadcast variables for UDF
_master_table = master_table
_merged_desc_column = merged_desc_column
_master_id_key = master_id_key
_max_potential_matches = max_potential_matches
client = create_vs_client()
index = client.get_index(index_name=f"{_master_table}_index")
num_results_requested = int(_max_potential_matches) + 1
if exclude_self_matches:
    # GOLDEN DEDUP UDF - excludes self-matches
    
    @pandas_udf(result_schema)
    def vector_search_results_udf(descriptions: pd.Series, query_ids: pd.Series) -> pd.Series:
        """
        Vector search for Golden Dedup - excludes self-matches.
        Queries come from unified_dedup, searches against master_table index.
        """
        
        
        # Request extra results to account for self-match filtering
        

        def get_results(text, query_id):
            try:
                results = index.similarity_search(
                    query_text=text,
                    columns=[_merged_desc_column, _master_id_key],
                    num_results=num_results_requested
                )

                data = results.get("result", {}).get("data_array", None)
                if not data:
                    raise ValueError(f"Empty VS response: {str(results)[:500]}")

                formatted = []
                for r in data:
                    result_text = r[0]
                    result_id = r[1]
                    result_score = float(r[2])

                    # CRITICAL: Skip self-matches
                    if result_id == query_id:
                        continue

                    formatted.append({
                        "text": result_text,
                        "lakefusion_id": result_id,
                        "score": result_score
                    })

                    if len(formatted) >= int(_max_potential_matches):
                        break

                return formatted

            except Exception as e:
                error_str = str(e)[:500]
                logger.error(f"Error during vector search for query_id {query_id}: {error_str}")
                return [{"text": f'{{ERROR: "{error_str}"}}', "lakefusion_id": "", "score": 0.0}]

        if vs_max_workers > 1:
            with ThreadPoolExecutor(max_workers=vs_max_workers) as executor:
                results = list(executor.map(get_results, descriptions, query_ids))
        else:
            results = [get_results(desc, qid) for desc, qid in zip(descriptions, query_ids)]
        return pd.Series(results)

    logger.info("Golden Dedup UDF defined (with self-match filtering)")
    logger.info(f"  - Requests {int(max_potential_matches) + 1} results to filter self-matches")

else:
    # NORMAL DEDUP UDF - no self-match filtering
    @pandas_udf(result_schema)
    def vector_search_results_udf(descriptions: pd.Series) -> pd.Series:
        

        def get_results(text):
            try:
                results = index.similarity_search(
                    query_text=text,
                    columns=[_merged_desc_column, _master_id_key],
                    num_results=int(_max_potential_matches)
                )

                data = results.get("result", {}).get("data_array", None)
                if not data:
                    raise ValueError(f"Empty VS response: {str(results)[:500]}")

                formatted = []
                for r in data:
                    formatted.append({
                        "text": r[0],
                        "lakefusion_id": r[1],
                        "score": float(r[2])
                    })
                return formatted

            except Exception as e:
                error_str = str(e)[:500]
                logger.error(f"Error during vector search: {error_str}")
                return [{"text": f'{{ERROR: "{error_str}"}}', "lakefusion_id": "", "score": 0.0}]

        if vs_max_workers > 1:
            with ThreadPoolExecutor(max_workers=vs_max_workers) as executor:
                results = list(executor.map(get_results, descriptions))
        else:
            results = [get_results(desc) for desc in descriptions]
        return pd.Series(results)
    
    logger.info("Normal Dedup UDF defined")

logger.info(f"  - Searches against: {master_table} index")
logger.info(f"  - Returns top {max_potential_matches} matches per record")

# COMMAND ----------

# DBTITLE 1,Step 7: Execute Vector Search
logger.info("\n" + "=" * 70)
logger.info("STEP 7: EXECUTE VECTOR SEARCH")
logger.info("=" * 70)

logger.info(f"Running {mode_name} vector search...")

if exclude_self_matches:
    # Golden dedup - pass both description and ID
    df_with_results = df_pending_search.withColumn(
        "search_results_array",
        vector_search_results_udf(
            df_pending_search[merged_desc_column],
            df_pending_search[source_id_key]
        )
    )
else:
    # Normal dedup - just description
    df_with_results = df_pending_search.withColumn(
        "search_results_array",
        vector_search_results_udf(df_pending_search[merged_desc_column])
    )

# Convert array-of-structs to formatted string for storage
df_with_final_results = df_with_results.withColumn(
    "search_results",
    expr("""
      concat_ws('], [', transform(search_results_array, x -> 
        concat(x.text, ', ', x.lakefusion_id, ', ', cast(x.score as string))
      ))
    """).alias("search_results")
)

# Materialize VS results to temp table — avoids recomputing the UDF on serverless
vs_temp_table = f"{catalog_name}.silver.{entity}_vs_temp_{experiment_id}"
df_with_final_results.write.format("delta").mode("overwrite").saveAsTable(vs_temp_table)
df_with_final_results = spark.table(vs_temp_table)
logger.info(f"VS results materialized to {vs_temp_table}")

# COMMAND ----------

# DBTITLE 1,Step 7.5: Split Valid vs Errored Records
logger.info("\n" + "=" * 70)
logger.info("STEP 7.5: SPLIT VALID VS ERRORED RECORDS")
logger.info("=" * 70)

from pyspark.sql.functions import col

df_valid = df_with_final_results.filter(~col("search_results").startswith("{ERROR:"))
df_errored = df_with_final_results.filter(col("search_results").startswith("{ERROR:"))

from lakefusion_core_engine.services.unified_error_handler import UnifiedErrorHandler
unified_error_handler = UnifiedErrorHandler(spark, source_table)
unified_error_handler.log_errors(
    df_errored.select(
        col(source_id_key).alias("surrogate_key"),
        col("search_results").alias("error_message")
    ),
    stage="VECTOR_SEARCH",
    run_id=run_id
)

df_with_final_results = df_valid
logger.info("Errored records (if any) routed to unified_error table; continuing with valid records only")

# COMMAND ----------

# DBTITLE 1,Step 8: Deduplicate Results
logger.info("\n" + "=" * 70)
logger.info("STEP 8: DEDUPLICATE RESULTS")
logger.info("=" * 70)

df_deduplicated = df_with_final_results.dropDuplicates([source_id_key])

# dedup_count = df_deduplicated.count()
# print(f"✓ Deduplicated results: {dedup_count} unique records")

# if dedup_count != search_results_count:
#     print(f"  ⚠️  Removed {search_results_count - dedup_count} duplicate records")

# COMMAND ----------

# DBTITLE 1,Step 9: Merge Results to Source Table
logger.info("\n" + "=" * 70)
logger.info("STEP 9: MERGE RESULTS TO SOURCE TABLE")
logger.info("=" * 70)

delta_table = DeltaTable.forName(spark, source_table)

#before_merge_empty = spark.table(source_table).filter(col("search_results") == "").count()

delta_table.alias("target").merge(
    source=df_deduplicated.alias("source"),
    condition=f"target.{source_id_key} = source.{source_id_key}"
).whenMatchedUpdate(
    set={"search_results": "source.search_results"}
).execute()

#after_merge_empty = spark.table(source_table).filter(col("search_results") == "").count()

logger.info(f"Merge completed to {source_table}")
# print(f"  Records with empty search_results before: {before_merge_empty}")
# print(f"  Records with empty search_results after: {after_merge_empty}")
# print(f"  Records updated: {before_merge_empty - after_merge_empty}")

# COMMAND ----------

# DBTITLE 1,Step 10: Optimize Source Table
logger.info("\n" + "=" * 70)
logger.info("STEP 10: OPTIMIZE SOURCE TABLE")
logger.info("=" * 70)

spark.sql(f"OPTIMIZE {source_table} ZORDER BY ({source_id_key})")

logger.info(f"Optimization completed")
logger.info(f"  Z-Ordered by: {source_id_key}")

# COMMAND ----------

# Cleanup temp table
spark.sql(f"DROP TABLE IF EXISTS {vs_temp_table}")
logger.info(f"Cleaned up temp table {vs_temp_table}")

# DBTITLE 1,Set Task Values and Exit
dbutils.jobs.taskValues.set("vector_search_complete", True)
#dbutils.jobs.taskValues.set("records_searched", search_results_count)

logger.info("\n" + "=" * 70)
logger.info(f"{mode_name} VECTOR SEARCH COMPLETED SUCCESSFULLY")
logger.info("=" * 70)
logger.info(f"Mode: {mode_name}")
logger.info(f"Source table: {source_table}")
#print(f"Total records searched: {search_results_count}")
if exclude_self_matches:
    logger.info("Self-matches were automatically excluded")
logger.info("=" * 70)
