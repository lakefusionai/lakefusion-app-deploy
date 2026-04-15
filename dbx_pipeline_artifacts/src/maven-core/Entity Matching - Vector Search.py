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

dbutils.widgets.text("llm_endpoint", "", "LLM Endpoint Name")
dbutils.widgets.text("embedding_endpoint", "", "Embedding Endpoint Name")
dbutils.widgets.text("experiment_id", "", "Match Maven Experiment")
dbutils.widgets.text("id_key", "", "Primary Key")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("attributes", "", "Merged Description Attributes")
dbutils.widgets.dropdown("llm_model_source", "databricks_foundation", ["databricks_custom_hugging_face", "databricks_foundation"], "LLM Model Source")
dbutils.widgets.dropdown("embedding_model_source", "databricks_foundation", ["databricks_custom_hugging_face", "databricks_foundation"], "Embedding Model Source")
dbutils.widgets.dropdown("incremental_load", "False", ["True", "False"], "Incremental Load?")
dbutils.widgets.text("catalog_name", "", "catalog name")
dbutils.widgets.text("vs_endpoint", "", "vs endpointname")
dbutils.widgets.text("max_potential_matches", "", "Max Potential Matches")

# COMMAND ----------

llm_endpoint = dbutils.widgets.get("llm_endpoint")
embedding_endpoint = dbutils.widgets.get("embedding_endpoint")
attributes = dbutils.widgets.get("attributes")
experiment_id = dbutils.widgets.get("experiment_id")
id_key = dbutils.widgets.get("id_key")
entity = dbutils.widgets.get("entity")
merged_desc_column = "attributes_combined"
embedding_model_source = dbutils.widgets.get("embedding_model_source")
llm_model_source = dbutils.widgets.get("llm_model_source")
incremental_load_str = dbutils.widgets.get("incremental_load")
incremental_load = bool(incremental_load_str)
catalog_name=dbutils.widgets.get("catalog_name")
vs_endpoint=dbutils.widgets.get("vs_endpoint")
max_potential_matches=dbutils.widgets.get("max_potential_matches")

# COMMAND ----------

entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
id_key = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="id_key", debugValue=id_key)
llm_endpoint = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="llm_model_endpoint", debugValue=llm_endpoint)
vs_endpoint= dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="vs_endpoint", debugValue=vs_endpoint)
embedding_endpoint = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="embedding_model_endpoint", debugValue=embedding_endpoint)
attributes = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="match_attributes", debugValue=attributes)

incremental_load = dbutils.jobs.taskValues.get(taskKey="Prepare_Tables", key="incremental_load", debugValue=incremental_load)
max_potential_matches = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="max_potential_matches", debugValue=max_potential_matches)
embedding_model_source = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="embedding_model_source", debugValue=embedding_model_source)
llm_model_source = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="llm_model_source", debugValue=llm_model_source)

if incremental_load and (experiment_id != 'prod'):
    llm_endpoint = dbutils.jobs.taskValues.get(
        taskKey="Endpoints_Mapping", key="llm_model_endpoint", debugValue=llm_endpoint
    )
    embedding_endpoint = dbutils.jobs.taskValues.get(
        taskKey="Endpoints_Mapping", key="embedding_model_endpoint", debugValue=embedding_endpoint
    )

elif  (experiment_id == 'prod'):
    llm_endpoint = dbutils.jobs.taskValues.get(
        taskKey="Endpoints_Mapping", key="llm_model_endpoint", debugValue=llm_endpoint
    )
    embedding_endpoint = dbutils.jobs.taskValues.get(
        taskKey="Endpoints_Mapping", key="embedding_model_endpoint", debugValue=embedding_endpoint
    )
elif incremental_load and (experiment_id == 'prod'):
    llm_endpoint = dbutils.jobs.taskValues.get(
        taskKey="Endpoints_Mapping", key="llm_model_endpoint", debugValue=llm_endpoint
    )
    embedding_endpoint = dbutils.jobs.taskValues.get(
        taskKey="Endpoints_Mapping", key="embedding_model_endpoint", debugValue=embedding_endpoint
    )
else:
    if embedding_model_source == "databricks_foundation":
        embedding_endpoint = dbutils.jobs.taskValues.get(
            taskKey="Embedding_Foundational_Serving_Endpoint", key="embedding_model_endpoint", debugValue=embedding_endpoint
        )
    if llm_model_source == "databricks_foundation":
        llm_endpoint = dbutils.jobs.taskValues.get(
            taskKey="LLM_Foundational_Serving_Endpoint", key="llm_model_endpoint",
            debugValue=llm_endpoint
        )

    if not llm_endpoint:
        llm_endpoint = dbutils.jobs.taskValues.get(
            taskKey="LLM_Hugging_Face_Serving_Endpoint", key="llm_model_endpoint",
            debugValue=llm_endpoint
        )
    if not embedding_endpoint:
        embedding_endpoint = dbutils.jobs.taskValues.get(
            taskKey="Embedding_Hugging_Face_Serving_Endpoint", key="embedding_model_endpoint", default="mpnetv2",
            debugValue=embedding_endpoint
        )

# COMMAND ----------

attributes = json.loads(attributes)

# COMMAND ----------

id_key="lakefusion_id"

# COMMAND ----------

unified_table = f"{catalog_name}.silver.{entity}_unified"
processed_unified_table = f"{catalog_name}.silver.{entity}_processed_unified"
master_table = f"{catalog_name}.gold.{entity}_master"
#vs_endpoint += f"_{entity}"
if experiment_id:
  unified_table += f"_{experiment_id}"
  processed_unified_table += f"_{experiment_id}"
  master_table += f"_{experiment_id}"
  # vs_endpoint += f"_{experiment_id}"

master_table_list = master_table.split(".")
master_catalog, master_schema, master_table_name = master_table_list[0], master_table_list[1], master_table_list[2]

# COMMAND ----------

spark.sql(f"ALTER TABLE {master_table} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = true)")

# COMMAND ----------

# MAGIC %run ../utils/execute_utils

# COMMAND ----------

# MAGIC %run ../utils/model_serving

# COMMAND ----------

# def check_model_status(llm_endpoint, embedding_endpoint):
#     try:
#         llm_status = get_serving_endpoint_status(llm_endpoint)
#         embedding_status = get_serving_endpoint_status(embedding_endpoint)
#     except ValueError as e:
#         raise ValueError(f"Error getting serving endpoint status: {e}")
#     return llm_status , embedding_status

# COMMAND ----------

# llm_status, embedding_status = check_model_status(llm_endpoint, embedding_endpoint)

# COMMAND ----------

# MAGIC %md
# MAGIC Embedding Endpoint Ready Status Check

# COMMAND ----------

try:
    wait_until_serving_endpoint_ready(endpoint_name=embedding_endpoint, timeout_minutes=5)
except ValueError as e:
    logger.error(str(e))
    logger_instance.shutdown()
    dbutils.notebook.exit(f"ERROR: Embedding endpoint not ready: {str(e)}")

# COMMAND ----------

# MAGIC %run ../utils/vs_utils

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

client = create_vs_client(disable_notice=True)

try:
    # Safely list existing endpoints
    response = client.list_endpoints()
    existing_endpoints = [ep['name'] for ep in response.get('endpoints', [])]
except Exception as e:
    logger.warning(f"Could not fetch Vector Search endpoints: {e}")
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
        logger.error(f"Exception occurred while creating the Vector Search endpoint: {e}")


# COMMAND ----------

# MAGIC %md
# MAGIC VS Endpoint Ready Status Check

# COMMAND ----------

try:
    wait_until_vector_search_ready(endpoint_name=vs_endpoint, client=client)
except ValueError as e:
    logger.error(str(e))
    logger_instance.shutdown()
    dbutils.notebook.exit(f"ERROR: vector search endpoint not ready: {str(e)}")


# COMMAND ----------

import time
index = None
# Step 1: Create or fetch existing index
try:
    logger.info("Attempting to create index...")
    index = client.create_delta_sync_index(
        endpoint_name=vs_endpoint,
        source_table_name=master_table,
        index_name=master_table + "_index",
        pipeline_type="TRIGGERED",
        primary_key=id_key,
        embedding_source_column=merged_desc_column,
        embedding_model_endpoint_name=embedding_endpoint,
        sync_computed_embeddings=True
    )
    logger.info("New index created")
    
except Exception as e:
    error_msg = str(e)
    
    # Check if index already exists
    if "RESOURCE_ALREADY_EXISTS" in error_msg or "already exists" in error_msg.lower():
        logger.info("Index already exists, fetching existing index...")
        vs_endpoint_name = client.get_index(index_name=master_table + "_index")
        index = client.get_index(
            endpoint_name=vs_endpoint_name.endpoint_name,
            index_name=master_table + "_index"
        )
        logger.info("Found existing index")
        
        # Sync the existing index with scale-to-zero error handling
        try:
            index.sync()
            logger.info("Index sync initiated")
        except Exception as sync_error:
            sync_error_msg = str(sync_error)
            if "scale to zero" in sync_error_msg.lower():
                logger.error(
                    "The embedding model endpoint has 'Scale to zero' enabled. "
                    "Please disable 'Scale to zero' in the Databricks Model Serving UI and restart the endpoint."
                )
                raise
            else:
                logger.error(f"Error during sync: {sync_error}")
                raise
    else:
        logger.error(f"Error creating index: {e}")
        raise

# Step 2: Wait for index to be ready
TIMEOUT_SECONDS = 10 * 60   # 10 minutes
POLL_INTERVAL = 20         # seconds
MAX_PIPELINE_FAILURES = 3  # fail on 3rd occurrence

if index:
    logger.info("Waiting for index to be ready and synced...")
    start_time = time.time()
    pipeline_failed_count = 0

    try:
        index.wait_until_ready()

        while True:
            # ⏱️ Timeout check
            elapsed = time.time() - start_time
            if elapsed > TIMEOUT_SECONDS:
                error_message = (
                    f"ERROR: Index sync timed out after "
                    f"{int(elapsed)} seconds"
                )
                logger.error(error_message)
                raise TimeoutError(error_message)

            status = index.describe()['status']['detailed_state']
            logger.info(f"Index status: {status}")

            # ✅ Success
            if status == "ONLINE_NO_PENDING_UPDATE":
                logger.info("Index is fully synced and ready.")
                break

            # ⚠️ Transient failure handling
            if status == "ONLINE_PIPELINE_FAILED":
                pipeline_failed_count += 1
                logger.warning(
                    f"ONLINE_PIPELINE_FAILED detected "
                    f"({pipeline_failed_count}/{MAX_PIPELINE_FAILURES})"
                )

                if pipeline_failed_count >= MAX_PIPELINE_FAILURES:
                    error_message = (
                        "ERROR: Index entered ONLINE_PIPELINE_FAILED "
                        "state 3 times"
                    )
                    logger.error(error_message)
                    raise Exception(error_message)

            # ⏳ Still processing
            time.sleep(POLL_INTERVAL)

    except Exception as e:
        error_message = f"ERROR: Index failed to sync: {e}"
        logger.error(error_message)
        raise

else:
    error_message = "ERROR: Index object is None after creation/retrieval attempts"
    logger.error(error_message)
    raise Exception(error_message)
# COMMAND ----------

from pyspark.sql.functions import col

df_unified = spark.table(unified_table)
df_pending_search = df_unified.filter(col("search_results") == "").select(id_key, merged_desc_column)

num_partitions = df_pending_search.count() // 3000

# Optional: repartition to parallelize better
df_pending_search = df_pending_search.repartition(__builtins__.max(100, num_partitions), id_key)

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, FloatType
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# UDF schema: returns array of struct with description and score
result_schema = ArrayType(StructType([
    StructField("text", StringType()),
   StructField("lakefusion_id", StringType()),
    StructField("score", FloatType())
]))

@pandas_udf(result_schema)
def vector_search_results_udf(descriptions: pd.Series) -> pd.Series:
    client = create_vs_client()
    index = client.get_index(index_name=f"{master_table}_index")

    def get_results(text):
        try:
            results = similarity_search_with_retry(
                index, query_text=text,
                columns=[merged_desc_column, "lakefusion_id"],
                num_results=max_potential_matches
            )

            formatted = []
            for r in results["result"]["data_array"]:
                formatted.append({
                    "text": r[0],
                    "lakefusion_id": r[1],
                    "score": float(r[2])
                })
            return formatted

        except Exception as e:
            logger.error(f"Error during vector search for input: {text[:30]}... - {e}")
            return []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(get_results, desc): i
                   for i, desc in enumerate(descriptions)}
        results = [None] * len(descriptions)
        for future in as_completed(futures):
            results[futures[future]] = future.result()
    return pd.Series(results)

# COMMAND ----------

# Apply the UDF
df_with_results = df_pending_search.withColumn(
    "search_results_array",
    vector_search_results_udf(df_pending_search[merged_desc_column])
)

# Optionally, convert array-of-structs to a display string
from pyspark.sql.functions import expr

df_with_final_results = df_with_results.withColumn(
    "search_results",
    expr("""
      concat_ws('], [', transform(search_results_array, x -> concat(x.text, ', ',x.lakefusion_id,', ',cast(x.score as string))))
    """).alias("search_results")
)

# COMMAND ----------

# Deduplicate the source DataFrame by keeping the first row for each key
df_unified_with_scoring_results_with_id_deduplicated = (
    df_with_final_results
    .dropDuplicates([id_key])
)

# COMMAND ----------

delta_table = DeltaTable.forName(spark, unified_table)

# Perform merge operation
delta_table.alias("target").merge(
    source=df_unified_with_scoring_results_with_id_deduplicated.alias("source"),
    condition=f"target.{id_key} = source.{id_key}"
).whenMatchedUpdate(set=(
    {
        "search_results": "source.search_results"
    }
)).execute()

# COMMAND ----------

optimise_res = spark.sql(f"OPTIMIZE {unified_table} ZORDER BY ({merged_desc_column})")
