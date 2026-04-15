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
dbutils.widgets.text("catalog_name", "", "lakefusion catalog name")
dbutils.widgets.text("additional_instructions", "", "Additional Instruction")
dbutils.widgets.text("base_prompt", "", "Base Prompt")
dbutils.widgets.text("llm_temperature", "0.0", "LLM Temperature")

# COMMAND ----------

llm_endpoint = dbutils.widgets.get("llm_endpoint")
embedding_endpoint = dbutils.widgets.get("embedding_endpoint")
attributes = dbutils.widgets.get("attributes")
base_prompt = dbutils.widgets.get("base_prompt")
experiment_id = dbutils.widgets.get("experiment_id")
id_key = dbutils.widgets.get("id_key")
vsc_endpoint = "lakefusion_vsc_endpoint"
entity = dbutils.widgets.get("entity")
merged_desc_column = "attributes_combined"
embedding_model_source = dbutils.widgets.get("embedding_model_source")
llm_model_source = dbutils.widgets.get("llm_model_source")
incremental_load = dbutils.widgets.get("incremental_load")
catalog_name = dbutils.widgets.get("catalog_name")
additional_instructions = dbutils.widgets.get("additional_instructions")
llm_temperature = dbutils.widgets.get("llm_temperature")

# COMMAND ----------

entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
id_key = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="id_key", debugValue=id_key)
base_prompt = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "base_prompt", debugValue=base_prompt) or ""
llm_endpoint = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="llm_model_endpoint", debugValue=llm_endpoint)
embedding_endpoint = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="embedding_model_endpoint", debugValue=embedding_endpoint)
attributes = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="match_attributes", debugValue=attributes)
incremental_load = dbutils.jobs.taskValues.get(taskKey="Prepare_Tables", key="incremental_load", debugValue=incremental_load)

embedding_model_source = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="embedding_model_source", debugValue=embedding_model_source)
llm_model_source = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="llm_model_source", debugValue=llm_model_source)
additional_instructions=dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="additional_instructions", debugValue=additional_instructions)
llm_temperature = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="llm_temperature", debugValue=llm_temperature)
llm_temperature = float(llm_temperature) if llm_temperature else 0.0

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
search_results="search_results"

# COMMAND ----------

unified_table = f"{catalog_name}.silver.{entity}_unified"
processed_unified_table = f"{catalog_name}.silver.{entity}_processed_unified"
master_table = f"{catalog_name}.gold.{entity}_master"
if experiment_id:
  unified_table += f"_{experiment_id}"
  processed_unified_table += f"_{experiment_id}"
  master_table += f"_{experiment_id}"

master_table_list = master_table.split(".")
master_catalog, master_schema, master_table_name = master_table_list[0], master_table_list[1], master_table_list[2]

# COMMAND ----------

# MAGIC %run ../utils/execute_utils

# COMMAND ----------

# MAGIC %run ../utils/model_serving

# COMMAND ----------

try:
    wait_until_serving_endpoint_ready(endpoint_name=llm_endpoint, timeout_minutes=5)
except ValueError as e:
    logger.error(str(e))
    logger_instance.shutdown()
    dbutils.notebook.exit(f"ERROR: LLM endpoint not ready: {str(e)}")

# COMMAND ----------

processed_unified_table_exists = spark.catalog.tableExists(processed_unified_table)

# COMMAND ----------

id_key="lakefusion_id"

# COMMAND ----------

def safe_format(template, **kwargs):
    """Format a template, ignoring unused placeholders."""
    # Get all placeholders that exist in the template
    import string
    formatter = string.Formatter()
    field_names = [field_name for _, field_name, _, _ in formatter.parse(template) if field_name]

    logger.debug(f"Field Names--- {field_names}")
    
    # Filter kwargs to only include fields that exist in template
    filtered_kwargs = {k: v for k, v in kwargs.items() if k in field_names}
    
    return template.format(**filtered_kwargs)

# COMMAND ----------

if base_prompt is not None and base_prompt.strip() != "":
    # CUSTOM BASE PROMPT
    logger.info("Custom base prompt provided")

    formatted_prompt = safe_format(
        base_prompt,
        entity=entity,
        additional_instructions=additional_instructions,
        attributes=" | ".join(attributes),
        merged_desc_column="attributes_combined",
        search_results="search_results"
    )
    final_prompt_block = formatted_prompt

# COMMAND ----------

if base_prompt is None or base_prompt == "":
    if processed_unified_table_exists:
      uf_sub_query = f"""uf as (
        select
          u.{id_key},
          u.{merged_desc_column},
          u.search_results
        from
          {unified_table} u left anti
          join {processed_unified_table} pu on u.{id_key} = pu.{id_key}
        )
      """
    else:
      uf_sub_query = f"""uf as (
        select
          u.{id_key},
          u.{merged_desc_column},
          u.search_results
        from
          {unified_table} u
        )
      """
    
    llm_query = f"""
    with m as (
      select
        {id_key},
        {merged_desc_column}
      from
        {master_table}
    ),
    {uf_sub_query},
    llm_results as (
      SELECT
        {merged_desc_column},
        search_results,
        ai_query(
          '{llm_endpoint}',
          CONCAT(
            'You are an expert in classifying two entities as matching or not matching. ',
            'You will be provided with a query entity and a list of possible entities. ',
            'You need to find the closest matching entity with a similarity score to indicate how similar they are.',
            'Take into account:',
            '1. The attributes are not mutually exclusive.',
            '2. The attributes are not exhaustive.',
            '3. The attributes are not consistent.',
            '4. The attributes are not complete.',
            '5. The attributes can have typos.',
            '6. The entity type that is being considered here is - {entity}.',
            '7. Do not consider scores in the search results / query entities. Generate scores by yourself.',
            '8. If additional instructions are present, consider them too.{additional_instructions}',
            'Follow these rules for the input and output:',
            '**Input:**',
            '- Query Entity Format:',
            '  The query entity will be provided as a single string in the following format:',
            '  {' | '.join(attributes)}',
            '  Each field is separated by a vertical bar (|).',
            '  Note: Some columns can have aggregated values and may be separated by (•)',
            '- List of Possible Entities:',
            '  A JSON list of objects with fields: id (entity string) and score (similarity score).',
            '**Output:**',
            'Only return a plain JSON list of objects:',
            '1. No extra text, no headers, no introductory phrases, and no comments.',
            '2. Do not use code blocks, markdown, or any other formatting.',
            '3. The JSON list should contain objects with the following keys:',
            '    a. id: String (the full entity string).',
            '    b. match: "MATCH" or "NOT_A_MATCH".',
            '    c. score: Float (similarity score).',
            '    d. reason: String (explanation for the match decision).',
            '    e. lakefusion_id: String (the lakefusion_id of the entity).',
            '4. Ensure scores are in descending order.',
            'Query entity: ',
            {merged_desc_column},
            'List of Possible entities with similarity search scores: ',
            search_results
          ),
          modelParameters => named_struct('temperature', {llm_temperature})
        ) AS scoring_results
      FROM
        uf
    ),
    er as (
      select
      uf.*,
      explode(from_json(llm_results.scoring_results, 'ARRAY<STRUCT<
            id STRING, 
            match STRING, 
            score DOUBLE, 
            reason STRING,
            lakefusion_id STRING
        >>')) AS exploded_result
    from
      uf,
      llm_results
    where
      uf.{merged_desc_column} = llm_results.{merged_desc_column}
    )
    select
      er.{id_key},
      m.{id_key} as master_{id_key},
      er.exploded_result,
      er.exploded_result.`id` as exploded_result_id
    from
      er inner join m
      on er.exploded_result.`lakefusion_id` = m.lakefusion_id;
    """
else:
    if processed_unified_table_exists:
      uf_sub_query = f"""uf as (
        select
          u.{id_key},
          u.{merged_desc_column},
          u.search_results
        from
          {unified_table} u left anti
          join {processed_unified_table} pu on u.{id_key} = pu.{id_key}
        )
      """
    else:
      uf_sub_query = f"""uf as (
        select
          u.{id_key},
          u.{merged_desc_column},
          u.search_results
        from
          {unified_table} u
        )
      """
    
    llm_query = f"""
    with m as (
      select
        {id_key},
        {merged_desc_column}
      from
        {master_table}
    ),
    {uf_sub_query},
    llm_results as (
      SELECT
        {merged_desc_column},
        search_results,
        ai_query(
          '{llm_endpoint}',
          CONCAT('{final_prompt_block}),
          modelParameters => named_struct('temperature', {llm_temperature})
        ) AS scoring_results
      FROM
        uf
    ),
    er as (
      select
      uf.*,
      explode(from_json(llm_results.scoring_results, 'ARRAY<STRUCT<
            id STRING, 
            match STRING, 
            score DOUBLE, 
            reason STRING,
            lakefusion_id STRING
        >>')) AS exploded_result
    from
      uf,
      llm_results
    where
      uf.{merged_desc_column} = llm_results.{merged_desc_column}
    )
    select
      er.{id_key},
      m.{id_key} as master_{id_key},
      er.exploded_result,
      er.exploded_result.`id` as exploded_result_id
    from
      er inner join m
      on er.exploded_result.`lakefusion_id` = m.lakefusion_id;
    """


# COMMAND ----------

df_unified_with_scoring_results_with_id = spark.sql(llm_query)

# COMMAND ----------

master_id_key_column_name = f"master_{id_key}"

# COMMAND ----------

df_deduplicated = (
    df_unified_with_scoring_results_with_id
    .dropDuplicates([id_key, master_id_key_column_name])
)

# COMMAND ----------

if not processed_unified_table_exists:
  df_deduplicated.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(processed_unified_table)
else:
  # Define the Delta table
  delta_table = DeltaTable.forName(spark, processed_unified_table)

  # Perform merge operation
  delta_table.alias("target").merge(
      source=df_deduplicated.alias("source"),
      condition=f"target.{id_key} = source.{id_key} and target.{master_id_key_column_name} = source.{master_id_key_column_name}"
  ).whenNotMatchedInsertAll().execute()

# COMMAND ----------

optimise_res = spark.sql(f"OPTIMIZE {processed_unified_table} ZORDER BY ({master_id_key_column_name})")
