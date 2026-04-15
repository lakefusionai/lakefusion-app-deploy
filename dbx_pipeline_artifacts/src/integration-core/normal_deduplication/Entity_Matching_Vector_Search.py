# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

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
dbutils.widgets.text("vs_max_workers", "1", "VS Max Workers (ThreadPool)")
dbutils.widgets.text("vs_max_concurrency", "50", "VS Max Concurrent Calls (Partitions)")

# COMMAND ----------

embedding_endpoint = dbutils.widgets.get("embedding_endpoint")
attributes = dbutils.widgets.get("attributes")
experiment_id = dbutils.widgets.get("experiment_id")
entity = dbutils.widgets.get("entity")
merged_desc_column = "attributes_combined"
embedding_model_source = dbutils.widgets.get("embedding_model_source")
catalog_name = dbutils.widgets.get("catalog_name")
vs_endpoint = dbutils.widgets.get("vs_endpoint")
max_potential_matches = dbutils.widgets.get("max_potential_matches")
vs_max_workers = int(dbutils.widgets.get("vs_max_workers") or "1")
vs_max_concurrency = int(dbutils.widgets.get("vs_max_concurrency") or "50")
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
logger.info("="*60)

# COMMAND ----------

# MAGIC %run ../../utils/model_serving

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

client = create_vs_client(disable_notice=True)

try:
    # Safely list existing endpoints
    response = client.list_endpoints()
    existing_endpoints = [ep['name'] for ep in response.get('endpoints', [])]
except Exception as e:
    logger.info(f"Could not fetch Vector Search endpoints: {e}")
    existing_endpoints = []

# Check if endpoint already exists
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

index = None
index_name = f"{master_table}_index"

# Configuration constants
MAX_WAIT_SECONDS = 3 * 60 * 60  # 3 hours
POLL_INTERVAL = 20          # seconds
MAX_PIPELINE_FAILURES = 3   # fail on 3rd occurrence
REGISTRATION_TIMEOUT = 1800 # 30 minutes for index registration

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
        pipeline_type="TRIGGERED",
        primary_key=master_id_key,
        embedding_source_column=merged_desc_column,
        embedding_model_endpoint_name=embedding_endpoint,
        sync_computed_embeddings=True
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
        # Handle both "does not exist" (message) and "does_not_exist" (API code)
        if "not exist" in error_lower or "not_exist" in error_lower:
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
if index:
    logger.info("Waiting for index to be ready and synced...")
    start_time = time.time()
    pipeline_failed_count = 0

    try:
        index.wait_until_ready()

        while True:
            index_status_info = index.describe()['status']
            status = index_status_info.get('detailed_state', 'UNKNOWN')
            logger.info(f"  Index status: {status}")

            # ✅ Success
            if status == "ONLINE_NO_PENDING_UPDATE":
                logger.info("Index is fully synced and ready.")
                break

            # 🔄 Actively updating - reset timer (no timeout while making progress)
            if status in ["ONLINE_TRIGGERED_UPDATE", "ONLINE_UPDATING_PIPELINE_RESOURCES"]:
                start_time = time.time()  # Reset timer since progress is being made
                logger.info("  Sync in progress, resetting timeout timer...")

            # ⚠️ Transient failure handling - apply timeout only when failed
            if status == "ONLINE_PIPELINE_FAILED":
                pipeline_failed_count += 1
                # Log failure details
                status_message = index_status_info.get('message', 'No message available')
                logger.warning(
                    f"ONLINE_PIPELINE_FAILED detected "
                    f"({pipeline_failed_count}/{MAX_PIPELINE_FAILURES})"
                )
                logger.warning(f"  Failure reason: {status_message}")

                # Check timeout only when in failed state
                elapsed = time.time() - start_time
                if elapsed > MAX_WAIT_SECONDS:
                    error_message = (
                        f"ERROR: Index sync timed out after "
                        f"{int(elapsed)} seconds in failed state"
                    )
                    logger.error(error_message)
                    raise TimeoutError(error_message)

                if pipeline_failed_count >= MAX_PIPELINE_FAILURES:
                    error_message = (
                        f"ERROR: Index entered ONLINE_PIPELINE_FAILED "
                        f"state 3 times. Last message: {status_message}"
                    )
                    logger.info(f"{error_message}")
                    raise Exception(error_message)

            # ⏳ Still processing
            time.sleep(POLL_INTERVAL)

    except Exception as e:
        error_message = f"ERROR: Index failed to sync: {e}"
        logger.info(f"{error_message}")
        raise

else:
    error_message = "ERROR: Index object is None after creation/retrieval attempts"
    logger.info(f"{error_message}")
    raise Exception(error_message)


# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 5: FILTER RECORDS FOR VECTOR SEARCH")
logger.info("="*60)

from pyspark.sql.functions import col

# Read unified table
df_unified = spark.table(unified_table)

# CRITICAL: Filter for records with ACTIVE status AND empty search_results
# These are the records that need to be matched against master
df_pending_search = df_unified.filter(
    (col("record_status") == "ACTIVE") & 
    (col("search_results") == "")
).select(unified_id_key, merged_desc_column)

pending_count = df_pending_search.isEmpty()

#print(f"✓ Unified table total records: {df_unified.count()}")
#print(f"✓ Records with ACTIVE status: {df_unified.filter(col('record_status') == 'ACTIVE').count()}")
logger.info(f"Records pending search (ACTIVE + empty search_results): {pending_count}")

if pending_count:
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
    calculated = __builtins__.max(min_partitions, row_count // 500)
    return __builtins__.min(max_concurrent_vs_calls, calculated)
# COMMAND ----------

row_count = df_pending_search.count()
optimal_partitions = get_optimal_partitions(row_count, vs_max_concurrency)
logger.info(f"Records: {row_count} | Partitions: {optimal_partitions} | ~{row_count // __builtins__.max(1, optimal_partitions)} rows/partition")

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

# UDF schema: returns array of struct with description, lakefusion_id, and score
result_schema = ArrayType(StructType([
    StructField("text", StringType()),
    StructField("lakefusion_id", StringType()),
    StructField("score", FloatType())
]))
client = create_vs_client()
index = client.get_index(index_name=f"{master_table}_index")
@pandas_udf(result_schema)
def vector_search_results_udf(descriptions: pd.Series) -> pd.Series:
    """
    Perform vector search against master table for each unified record.

    Args:
        descriptions: Series of attribute_combined strings from unified table

    Returns:
        Series of arrays containing top matches from master table with scores
    """

    def get_results(text):
        try:
            results = index.similarity_search(
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
                    "score": float(r[2])
                })
            return formatted

        except Exception as e:
            error_str = str(e)[:500]
            logger.error(f"Error during vector search for input: {text[:50]}... → {error_str}")
            return [{"text": f'{{ERROR: "{error_str}"}}', "lakefusion_id": "", "score": 0.0}]

    if vs_max_workers > 1:
        with ThreadPoolExecutor(max_workers=vs_max_workers) as executor:
            results = list(executor.map(get_results, descriptions))
    else:
        results = [get_results(desc) for desc in descriptions]
    return pd.Series(results)

logger.info("Vector search UDF defined")
logger.info(f"  - Searches against: {master_table}")
logger.info(f"  - Returns top {max_potential_matches} matches per record")
logger.info(f"  - Includes: text, lakefusion_id, score")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 7: EXECUTE VECTOR SEARCH")
logger.info("="*60)

# Apply the UDF
logger.info("Running vector search (this may take a while for large datasets)...")

df_with_results = df_pending_search.withColumn(
    "search_results_array",
    vector_search_results_udf(df_pending_search[merged_desc_column])
)

# Convert array-of-structs to a formatted string for storage
from pyspark.sql.functions import expr

df_with_final_results = df_with_results.withColumn(
    "search_results",
    expr("""
      concat_ws('], [', transform(search_results_array, x -> 
        concat(x.text, ', ', x.lakefusion_id, ', ', cast(x.score as string))
      ))
    """).alias("search_results")
)

# Materialize VS results to temp table — avoids recomputing the UDF on serverless
# (serverless doesn't support .cache()/.persist())
vs_temp_table = f"{catalog_name}.silver.{entity}_vs_temp_{experiment_id}"
df_with_final_results.write.format("delta").mode("overwrite").saveAsTable(vs_temp_table)
df_with_final_results = spark.table(vs_temp_table)
logger.info(f"VS results materialized to {vs_temp_table}")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 7.5: SPLIT VALID VS ERRORED RECORDS")
logger.info("="*60)

from pyspark.sql.functions import col

df_valid = df_with_final_results.filter(~col("search_results").startswith("{ERROR:"))
df_errored = df_with_final_results.filter(col("search_results").startswith("{ERROR:"))

from lakefusion_core_engine.services.unified_error_handler import UnifiedErrorHandler
unified_error_handler = UnifiedErrorHandler(spark, unified_table)
unified_error_handler.log_errors(
    df_errored.select(
        col(unified_id_key).alias("surrogate_key"),
        col("search_results").alias("error_message")
    ),
    stage="VECTOR_SEARCH",
    run_id=run_id
)

df_with_final_results = df_valid
logger.info("Errored records (if any) routed to unified_error table; continuing with valid records only")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 8: DEDUPLICATE RESULTS")
logger.info("="*60)

# Deduplicate by surrogate_key (keeping first occurrence)
df_deduplicated = df_with_final_results.dropDuplicates([unified_id_key])

# dedup_count = df_deduplicated.count()
# print(f"✓ Deduplicated results: {dedup_count} unique records")

# if dedup_count != search_results_count:
#     print(f"  ⚠️  Removed {search_results_count - dedup_count} duplicate records")

# COMMAND ----------

logger.info("\n" + "="*60)
logger.info("STEP 9: MERGE RESULTS TO UNIFIED TABLE")
logger.info("="*60)

delta_table = DeltaTable.forName(spark, unified_table)

# Count records before merge
#before_merge_empty = spark.table(unified_table).filter(col("search_results") == "").count()

# Perform merge operation
delta_table.alias("target").merge(
    source=df_deduplicated.alias("source"),
    condition=f"target.{unified_id_key} = source.{unified_id_key}"
).whenMatchedUpdate(
    set={"search_results": "source.search_results"}
).execute()

# Count records after merge
#after_merge_empty = spark.table(unified_table).filter(col("search_results") == "").count()

logger.info(f"Merge completed")

# Cleanup temp table
spark.sql(f"DROP TABLE IF EXISTS {vs_temp_table}")
logger.info(f"Cleaned up temp table {vs_temp_table}")

# COMMAND ----------


dbutils.jobs.taskValues.set("vector_search_complete", True)
#dbutils.jobs.taskValues.set("records_searched", search_results_count)

logger.info("\n" + "="*60)
logger.info("VECTOR SEARCH COMPLETED SUCCESSFULLY")
logger.info("="*60)
#print(f"Total records searched: {search_results_count}")
logger.info("="*60)
