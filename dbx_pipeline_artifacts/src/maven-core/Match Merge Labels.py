# Databricks notebook source
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

# COMMAND ----------

dbutils.widgets.text("llm_endpoint", "", "LLM Endpoint Name")
dbutils.widgets.text("embedding_endpoint", "", "Embedding Endpoint Name")
dbutils.widgets.text("experiment_id", "", "Match Maven Experiment")
dbutils.widgets.text("id_key", "", "Primary Key")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("attributes", "", "Merged Description Attributes")
dbutils.widgets.text("vsc_endpoint", "default_endpoint", "Vector Search Compute Endpoint")

# COMMAND ----------

llm_endpoint = dbutils.widgets.get("llm_endpoint")
embedding_endpoint = dbutils.widgets.get("embedding_endpoint")
attributes = json.loads(dbutils.widgets.get("attributes"))
experiment_id = dbutils.widgets.get("experiment_id")
id_key = dbutils.widgets.get("id_key")
vsc_endpoint = dbutils.widgets.get("vsc_endpoint")
entity = dbutils.widgets.get("entity")
merged_desc_column = "attributes_combined"

# COMMAND ----------

id_key = dbutils.jobs.taskValues.get(taskKey="prep_tables", key="id_key", debugValue=id_key)
experiment_id = dbutils.jobs.taskValues.get(taskKey="prep_tables", key="experiment_id", debugValue=experiment_id)
llm_endpoint = dbutils.jobs.taskValues.get(
    taskKey="create_llm_endpoint", key="endpoint_name",
    debugValue=llm_endpoint
)
embedding_endpoint = dbutils.jobs.taskValues.get(
    taskKey="create_embedding_endpoint", key="endpoint_name", default="mpnetv2",
    debugValue=embedding_endpoint
)

# COMMAND ----------

# MAGIC %run ../utils/execute_utils

# COMMAND ----------

logger.info(f"{llm_endpoint}, {embedding_endpoint}, {id_key}, {experiment_id}")

# COMMAND ----------

unified_table = f"lakefusion_ai.silver.{entity}_unified"
processed_unified_table = f"lakefusion_ai.silver.{entity}_processed_unified"
master_table = f"lakefusion_ai.gold.{entity}_master"
if experiment_id:
  unified_table += f"_{experiment_id}"
  processed_unified_table += f"_{experiment_id}"
  master_table += f"_{experiment_id}"
  vsc_endpoint += f"_{experiment_id}"

master_table_list = master_table.split(".")
master_catalog, master_schema, master_table_name = master_table_list[0], master_table_list[1], master_table_list[2]

# COMMAND ----------

# MAGIC %run ../utils/model_serving

# COMMAND ----------

def check_model_status(llm_endpoint, embedding_endpoint):
    try:
        llm_status = get_serving_endpoint_status(llm_endpoint)
        embedding_status = get_serving_endpoint_status(embedding_endpoint)
    except ValueError as e:
        raise ValueError(f"Error getting serving endpoint status: {e}")
    return llm_status , embedding_status

# COMMAND ----------

llm_status, embedding_status = check_model_status(llm_endpoint, embedding_endpoint)

# COMMAND ----------

df_master = spark.table(master_table)
df_unified_with_search_results = spark.table(unified_table)

# COMMAND ----------

df_master_id_attributes = (
  df_master.select(col(id_key), col(merged_desc_column))
)

# COMMAND ----------

# Databricks credentials
DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
DATABRICKS_URL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()

WORKSPACE_URL = get_context().workspaceUrl

# Model endpoint
MODEL_ENDPOINT = f"https://{WORKSPACE_URL}/serving-endpoints/{llm_endpoint}/invocations"

# System prompt
system_prompt = f"""
You are an expert in classifying two entities as matching or not matching. You will be provided with a query entity and a list of possible entities. You need to find the closest matching entity with a similarity score to indicate how similar they are. Take into account the following:
1. The attributes are not mutually exclusive.
2. The attributes are not exhaustive.
3. The attributes are not consistent.
4. The attributes are not complete.
5. The attributes can have typos.

Follow these strict rules for the input and output:
Query Entity Format:
The query entity will be provided as a single string in the following format:
{' | '.join(attributes)}
Each field is separated by a vertical bar (|).

Only return a plain JSON list of objects:
1. No extra text, no headers, no introductory phrases, and no comments.
2. Do not use code blocks, markdown, or any other formatting.
3. The JSON list should contain objects with the following keys:
    a. id: String (the full entity string).
    b. match: "MATCH" or "NOT_A_MATCH".
    c. score: Float (similarity score).
4. Ensure scores are in descending order.
"""

# COMMAND ----------

# @pandas_udf(StringType())
# def llm_scoring_udf_spark(attributes_texts, search_results):
#   def process_row(attributes_text, search_result):
#     try:
#         # Ensure both inputs are strings
#         attributes_text = str(attributes_text) if attributes_text is not None else ""
#         search_result = str(search_result) if search_result is not None else ""

#         # API payload and request
#         payload = {
#             "messages": [
#                 {"role": "system", "content": system_prompt},
#                 {
#                     "role": "user",
#                     "content": f"Query entity: {attributes_text}\nList of Possible entities with similarity search scores: {search_result}",
#                 },
#             ]
#         }
#         headers = {
#             "Authorization": f"Bearer {DATABRICKS_TOKEN}",
#             "Content-Type": "application/json",
#         }
#         response = requests.post(MODEL_ENDPOINT, json=payload, headers=headers)
#         response.raise_for_status()

#         # Extract response
#         choices = response.json().get("choices", [])
#         if choices:
#             return choices[0].get("message", {}).get("content", "")
#         else:
#             return json.dumps({"error": "No choices returned from API"})
#     except Exception as e:
#         # Handle exceptions and return an error string
#         return json.dumps({"error": str(e)})

#   # Process batch of inputs with valid fallback
#   return attributes_texts.combine(search_results, process_row).fillna("")

# COMMAND ----------

def llm_scoring_udf(attributes_text: str, search_results: str) -> str:
    try:
        attributes_text = str(attributes_text)
        search_results = str(search_results)
        payload = {
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Query entity: {attributes_text}\nList of Possible entities with similarity search scores: {search_results}"},
            ]
        }
        headers = {
            "Authorization": f"Bearer {DATABRICKS_TOKEN}",
            "Content-Type": "application/json",
        }
        # Make the API request
        response = requests.post(MODEL_ENDPOINT, json=payload, headers=headers)
        response.raise_for_status()  # Raise error for HTTP codes >= 400

        # Parse the response
        choices = response.json().get("choices", [])
        if choices:
            # Serialize the list as a JSON string
            return choices[0].get('message', {}).get('content', "")
        else:
            return json.dumps({"error": "No choices returned from API"})
    except Exception as e:
        # Handle errors gracefully and log them
        logger.error(f"Error processing input {attributes_text}: {e}")
        # Serialize the error as a JSON string
        return json.dumps({"error": str(e)})

# COMMAND ----------

llm_scoring_udf_spark = udf(llm_scoring_udf, StringType())

# COMMAND ----------

# Apply the UDF to your DataFrame
df_unified_with_search_results = (
     df_unified_with_search_results
    .fillna({
        "attributes_combined": "",
        "search_results": ""
    })
)
df_unified_with_scoring_results = (
    df_unified_with_search_results
    .withColumn(
        "scoring_results",
        llm_scoring_udf_spark(df_unified_with_search_results["attributes_combined"], df_unified_with_search_results["search_results"])
    )
)

df_unified_with_scoring_results.cache()

# COMMAND ----------

# Define the schema of the JSON array
json_schema = ArrayType(
    StructType([
        StructField("id", StringType(), True),
        StructField("match", StringType(), True),
        StructField("score", DoubleType(), True),
    ])
)

# Parse the JSON string into an array of structs
parsed_df = df_unified_with_scoring_results.withColumn("parsed_scoring_results", from_json(col("scoring_results"), json_schema))

# Explode the array into individual rows
exploded_df = parsed_df.withColumn("exploded_result", explode(col("parsed_scoring_results")))

exploded_df.cache()

# COMMAND ----------

exploded_df = (
  exploded_df
  .withColumn("exploded_result_id", col("exploded_result.id"))
)

# COMMAND ----------

join_condition = f"exploded_df.exploded_result_id = df_master_id_attributes.{merged_desc_column}"
master_id_key_column_name = f"master_{id_key}"

df_unified_with_scoring_results_with_id = (
  exploded_df
  .join(
    df_master_id_attributes, 
    exploded_df.exploded_result_id == df_master_id_attributes[merged_desc_column],
    "inner"
  )
  .select(
    exploded_df['*'],
    df_master_id_attributes[id_key].alias(master_id_key_column_name)
  )
)

df_unified_with_scoring_results_with_id.cache()

# COMMAND ----------

df_deduplicated = (
    df_unified_with_scoring_results_with_id
    .dropDuplicates([id_key, master_id_key_column_name])
)

# COMMAND ----------

table_exists = spark.catalog.tableExists(processed_unified_table)
if not table_exists:
  df_unified_with_scoring_results_with_id.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(processed_unified_table)
else:
  # Define the Delta table
  delta_table = DeltaTable.forName(spark, processed_unified_table)

  # Perform merge operation
  delta_table.alias("target").merge(
      source=df_deduplicated.alias("source"),
      condition=f"target.{id_key} = source.{id_key} and target.{master_id_key_column_name} = source.{master_id_key_column_name}"
  ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
