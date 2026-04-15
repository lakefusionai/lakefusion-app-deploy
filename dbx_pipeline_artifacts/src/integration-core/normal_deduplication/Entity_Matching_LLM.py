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
llm_temperature = float(llm_temperature) if llm_temperature else 0.0

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

try:
    llm_endpoint_from_mapping = dbutils.jobs.taskValues.get(
        taskKey="Endpoints_Mapping", 
        key="llm_model_endpoint", 
        debugValue=None
    )
    
    # Use mapped endpoints if available
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

# COMMAND ----------

merge_thresholds = config_thresholds.get('merge', [0.9, 1.0])
matches_thresholds = config_thresholds.get('matches', [0.7, 0.89])
not_match_thresholds = config_thresholds.get('not_match', [0.0, 0.69])

merge_min, merge_max = merge_thresholds[0], merge_thresholds[1]
matches_min, matches_max = matches_thresholds[0], matches_thresholds[1]
not_match_min, not_match_max = not_match_thresholds[0], not_match_thresholds[1]

# COMMAND ----------

unified_id_key = "surrogate_key"      # Key for unified table (source records)
master_id_key = "lakefusion_id"       # Key for master table (master records)
search_results="search_results"

# COMMAND ----------

unified_table = f"{catalog_name}.silver.{entity}_unified"
processed_unified_table = f"{catalog_name}.silver.{entity}_processed_unified"
master_table = f"{catalog_name}.gold.{entity}_master"
unified_deteministic_table = f"{catalog_name}.silver.{entity}_unified_deterministic"

# Append experiment_id if provided
if experiment_id:
    unified_table += f"_{experiment_id}"
    processed_unified_table += f"_{experiment_id}"
    master_table += f"_{experiment_id}"
    unified_deteministic_table+=f"_{experiment_id}"

# Parse master table components
master_table_list = master_table.split(".")
master_catalog, master_schema, master_table_name = master_table_list[0], master_table_list[1], master_table_list[2]

# COMMAND ----------

deteministic_unified_table_exists = spark.catalog.tableExists(unified_deteministic_table)


# COMMAND ----------

logger.info("="*80)
logger.info("LLM ENTITY MATCHING - THRESHOLD-BASED CLASSIFICATION")
logger.info("="*80)
logger.info(f"Entity: {entity}")
logger.info(f"Experiment: {experiment_id if experiment_id else 'prod'}")
logger.info(f"Unified Table: {unified_table}")
logger.info(f"Processed Unified Table: {processed_unified_table}")
logger.info(f"Master Table: {master_table}")
logger.info(f"LLM Endpoint: {llm_endpoint}")
logger.info(f"LLM Temperature: {llm_temperature}")
logger.info(f"Max Potential Matches: {max_potential_matches}")
logger.info(f"Match Thresholds:")
logger.info(f"  MERGE: [{merge_min}-{merge_max}]")
logger.info(f"  POTENTIAL_MATCH: [{matches_min}-{matches_max}]")
logger.info(f"  NO_MATCH: [{not_match_min}-{not_match_max}]")
logger.info("="*80)

# COMMAND ----------

# MAGIC %run ../../utils/model_serving

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 1: VALIDATE VECTOR SEARCH COMPLETION")
logger.info("="*80)

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

logger.info("\n" + "="*80)
logger.info("STEP 2: VALIDATE LLM ENDPOINT")
logger.info("="*80)

try:
    wait_until_serving_endpoint_ready(endpoint_name=llm_endpoint, timeout_minutes=5)
    logger.info(f"LLM endpoint '{llm_endpoint}' is ready")
except ValueError as e:
    error_msg = f"ERROR: LLM endpoint not ready: {str(e)}"
    logger.error(error_msg)
    logger_instance.shutdown()
    dbutils.notebook.exit(error_msg)

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 2.5: WARM UP LLM ENDPOINT (SCALE-FROM-ZERO HANDLING)")
logger.info("="*80)

# Build warm-up query - uses a single record to wake up the endpoint
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

logger.info("\n" + "="*80)
logger.info("STEP 3: CHECK PROCESSED UNIFIED TABLE")
logger.info("="*80)

processed_unified_table_exists = spark.catalog.tableExists(processed_unified_table)

if processed_unified_table_exists:
    existing_count = spark.table(processed_unified_table).count()
    logger.info(f"Processed unified table exists")
    logger.info(f"  Current records: {existing_count}")
else:
    logger.info(f"Processed unified table does not exist")
    logger.info(f"  Will be created: {processed_unified_table}")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 4: FILTER RECORDS FOR LLM PROCESSING")
logger.info("="*80)

# Filter: ACTIVE records with search_results populated but scoring_results empty
# Build the subquery for filtering

if deteministic_unified_table_exists:
    filter_query = f"""
      select
        u.{unified_id_key},
        u.{merged_desc_column},
        u.search_results
      from
        {unified_table} u left anti join {unified_deteministic_table} ud on u.{unified_id_key} = ud.{unified_id_key}
      where
        u.record_status = 'ACTIVE'
        and u.search_results IS NOT NULL
        and u.search_results != ''
        and (u.scoring_results IS NULL or u.scoring_results = '')
    """
else:
  filter_query = f"""
      select
        u.{unified_id_key},
        u.{merged_desc_column},
        u.search_results
      from
        {unified_table} u 
      where
        u.record_status = 'ACTIVE'
        and u.search_results IS NOT NULL
        and u.search_results != ''
        and (u.scoring_results IS NULL or u.scoring_results = '')
    """


# Count records to process
count_query = f"""
select count(*) as record_count
from (
  {filter_query}
) subquery
"""

uf_sub_query = f"""uf as ({filter_query})"""

records_to_process = spark.sql(count_query).isEmpty()


if records_to_process:
    logger.info("\nNo new records to process")
    
    # Update processed_unified from scoring_results even if no new LLM processing needed
    logger.info("\n" + "="*80)
    logger.info("STEP 5: UPDATE PROCESSED UNIFIED FROM SCORING RESULTS")
    logger.info("="*80)
    
    # Count active records with scoring_results
    active_with_scoring = spark.sql(f"""
        select count(*) as cnt
        from {unified_table}
        where record_status = 'ACTIVE'
        and scoring_results IS NOT NULL
        and scoring_results != ''
    """).isEmpty()
    
    #print(f"  Active records with scoring_results: {active_with_scoring}")
    
    if not active_with_scoring:
        # Rebuild processed_unified from scoring_results
        rebuild_query = f"""
        select
          u.{unified_id_key},
          exploded.{master_id_key} as {master_id_key},
          exploded as exploded_result,
          exploded.id as exploded_result_id
        from {unified_table} u
        lateral view explode(
          from_json(u.scoring_results, 'array<struct<
            id:string,
            match:string,
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
        #result_count = df_from_scoring.count()
        
        #print(f"  Results from scoring_results: {result_count}")
        
        # Overwrite processed_unified table
        df_from_scoring.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(processed_unified_table)
        #print(f"✓ Processed unified table updated with {result_count} records")
    else:
        logger.info("  No scoring_results to process")
    
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
    """Format a template, leaving unrecognized placeholders intact."""
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
        {unified_id_key},
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
            {unified_id_key},
            CONCAT(
                '{{"id": "', exploded_result.id,
                '", "score": ', exploded_result.score,
                ', "reason": "', exploded_result.reason,
                '", "lakefusion_id": "', exploded_result.lakefusion_id,
                '"}}'
            ) AS json_item,
            
            attributes_combined,
            search_results,
            deterministic_match_result,
            is_deterministic_match,

            NAMED_STRUCT(
                'id', exploded_result.id,
                'score', CAST(exploded_result.score AS DOUBLE),
                'reason', exploded_result.reason,
                'lakefusion_id', exploded_result.lakefusion_id
            ) AS exploded_result
        FROM {unified_deteministic_table}
    )
    GROUP BY {unified_id_key}
    """
else:
    union_clause = ""


# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 5: BUILD LLM QUERY")
logger.info("="*80)

# Build additional instructions clause
additional_instructions_clause = ""
if additional_instructions and additional_instructions.strip():
    additional_instructions_clause = f" {additional_instructions}"

logger.info(f"Building LLM query for {max_potential_matches} matches per record")

# Build the prompt - either default or custom
if base_prompt is None or base_prompt == '':
    logger.info("Using default prompt")
    default_prompt = f"""You are an expert in classifying two records as matching or not matching.
You will be provided with a golden record and a list of potential matches.
You need to find the closest matching record with a similarity score to indicate how similar they are.
Take into account:
1. The attributes are not mutually exclusive.
2. The attributes are not exhaustive.
3. The attributes are not consistent.
4. The attributes are not complete.
5. The attributes can have typos.
6. The entity type being considered is: {entity}.
7. Do not consider scores in the search results. Generate your own similarity scores.
8. If additional instructions are present, consider them too.{additional_instructions_clause}
Follow these rules for input and output:
**Input:**
- Golden Record Format:
  The golden record will be provided as a single string in the following format:
  {' | '.join(attributes)}
  Each field is separated by a vertical bar (|).
  Note: Some columns can have aggregated values separated by (bullet)
- List of Potential Matches:
  A string containing comma-separated entries in this EXACT format:
  [match_record, lakefusion_id, score], [match_record, lakefusion_id, score], ...
  Example: [John | Smith | john@email.com | 555-1234, abc123def456, 0.85], [Jane | Doe | jane@email.com | 555-5678, xyz789ghi012, 0.75], ...
  WHERE:
  - match_record: Pipe-separated fields (BEFORE first comma inside brackets)
  - lakefusion_id: 32-character hexadecimal string (BETWEEN first and second comma)
  - score: Decimal number (AFTER second comma, before closing bracket)
**CRITICAL EXTRACTION RULES:**
- For EACH entry in the list, you MUST extract the lakefusion_id
- The lakefusion_id is ALWAYS between the FIRST comma and SECOND comma inside each bracket
- If you cannot find a valid lakefusion_id, DO NOT return that entry
- lakefusion_id format: 32 lowercase hexadecimal characters (a-f, 0-9)
- Example valid lakefusion_id: d728dead49f8c4bbbf0be3dcdc65d215
**Output:**
Only return a plain JSON list of objects:
1. No extra text, no headers, no introductory phrases, and no comments.
2. Do not use code blocks, markdown, or any other formatting.
3. The JSON list MUST contain EXACTLY {max_potential_matches} objects (one for each potential match in the input list).
4. Each object must have these keys:
    a. id: String (the match_record - text BEFORE first comma in brackets).
    b. score: Float (YOUR similarity score from 0.0 to 1.0 - NOT the vector score).
    c. reason: String (brief explanation for YOUR score, comparing to the golden record).
    d. lakefusion_id: String (COPY the 32-char hex string between 1st and 2nd comma EXACTLY).
5. VERIFY each lakefusion_id is exactly 32 characters of a-f and 0-9.
6. Return EXACTLY {max_potential_matches} results, no more, no less.
7. Ensure scores are in descending order (highest similarity first)."""
    safe_prompt = default_prompt.replace("'", "\\'")
else:
    logger.info("Using custom base prompt")
    # Strip SQL column reference placeholders that are handled via CONCAT
    base_prompt = base_prompt.replace("{query_entity}", "").replace("{merged_desc_column}", "").replace("{search_results}", "")
    formatted_prompt = safe_format(
        base_prompt,
        entity=entity,
        additional_instructions=additional_instructions,
        attributes=' | '.join(attributes),
        max_potential_matches=max_potential_matches
    )
    safe_prompt = formatted_prompt.replace("'", "\\'")

prompt_concat_parts = (
    f"CONCAT('{safe_prompt}', "
    f"'\\nQuery record: ', {merged_desc_column}, "
    f"'\\nList of potential matches with vector search scores (JSON Array): ', search_results)"
)

# Construct LLM query
llm_query = f"""
with m as (
  select
    {master_id_key},
    {merged_desc_column}
  from
    {master_table}
),
{uf_sub_query},
llm_results as (
  SELECT
    {unified_id_key},
    {merged_desc_column},
    search_results,
    ai_query(
      '{llm_endpoint}',
      {prompt_concat_parts},
      responseFormat => '{{
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
      }}',
      modelParameters => named_struct('temperature', {llm_temperature}),
      failOnError => false
    ) AS scoring_results
  FROM
    uf
),
er as (
  select
  uf.*,
  get_json_object(llm_results.scoring_results.result, '$.results') as combined_results,
  EXPLODE(FROM_JSON(
    get_json_object(llm_results.scoring_results.result, '$.results'),
    'ARRAY<STRUCT<
        id STRING,
        score DOUBLE,
        reason STRING,
        lakefusion_id STRING
    >>'
  )) AS exploded_result
from
  uf,
  llm_results
where
  uf.{unified_id_key} = llm_results.{unified_id_key}
  {union_clause}
)

select
er.{unified_id_key},
er.exploded_result.{master_id_key} as {master_id_key},
er.exploded_result,
er.exploded_result.`id` as exploded_result_id,
er.combined_results as scoring_results_json
from er left join m on er.exploded_result.{master_id_key} = m.{master_id_key}
 where
m.{master_id_key} is not null
"""

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 6: EXECUTE LLM QUERY")
logger.info("="*80)

logger.info("Running LLM matching (this may take a while)...")

try:
    df_llm_results = spark.sql(llm_query)    
    

except Exception as e:
    error_msg = f"ERROR: LLM query execution failed: {str(e)}"
    logger.error(error_msg)
    import traceback
    logger.error(traceback.format_exc())
    logger_instance.shutdown()
    dbutils.notebook.exit(error_msg)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

df_llm_results_with_priority = df_llm_results.withColumn(
    "priority",
    F.when(
        F.col("exploded_result.reason").contains("Due to Match Rule"),
        1
    ).otherwise(0)
)

w = Window.partitionBy("exploded_result.lakefusion_id") \
          .orderBy(
              F.col("priority").desc(),
              F.col("exploded_result.score").desc()
          )

df_llm_results = (
    df_llm_results_with_priority.withColumn("rn", F.row_number().over(w))
       .filter("rn = 1")
       .drop("rn", "priority")
)

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 8: APPLY MATCH CLASSIFICATION")
logger.info("="*80)

def classify_match_status(score):
    if score is None:
        return "NO_MATCH"
    if merge_min <= score <= merge_max:
        return "MATCH"
    elif matches_min <= score <= matches_max:
        return "POTENTIAL_MATCH"
    elif not_match_min <= score <= not_match_max:
        return "NO_MATCH"
    else:
        return "NO_MATCH"

classify_match_udf = udf(classify_match_status, StringType())

# Apply classification directly to the score in exploded_result
df_classified = df_llm_results.withColumn(
    "match_status",
    classify_match_udf(col("exploded_result.score"))
)

# Reconstruct exploded_result with match_status included
df_with_match_status = df_classified.withColumn(
    "exploded_result",
    struct(
        col("exploded_result.id").alias("id"),
        col("match_status").alias("match"),  # Use the computed match_status
        col("exploded_result.score").alias("score"),
        col("exploded_result.reason").alias("reason"),
        col("exploded_result.lakefusion_id").alias("lakefusion_id")
    )
).select(
    unified_id_key,
    master_id_key,
    "exploded_result",
    "exploded_result_id",
    "scoring_results_json",
    "match_status"
)


# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 9: UPDATE UNIFIED TABLE WITH SCORING RESULTS")
logger.info("="*80)

# IMPORTANT: We need to recreate the scoring_results_json with the match status included
# Group by surrogate_key and collect all exploded_results into an array, then convert to JSON

df_scoring_updates = df_with_match_status.groupBy(unified_id_key).agg(
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
).select(unified_id_key, "scoring_results")



# Update unified table with scoring_results
unified_delta = DeltaTable.forName(spark, unified_table)

unified_delta.alias("target").merge(
    source=df_scoring_updates.alias("source"),
    condition=f"target.{unified_id_key} = source.{unified_id_key}"
).whenMatchedUpdate(
    set={"scoring_results": "source.scoring_results"}
).execute()

logger.info(f"Unified table updated with scoring_results")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 10: REBUILD PROCESSED UNIFIED TABLE")
logger.info("="*80)

# Rebuild processed_unified from ALL active records with scoring_results in unified table
rebuild_query = f"""
select
  u.{unified_id_key},
  exploded.{master_id_key} as {master_id_key},
  exploded as exploded_result,
  exploded.id as exploded_result_id
from {unified_table} u
lateral view explode(
  from_json(u.scoring_results, 'array<struct<
    id:string,
    match:string,
    score:double,
    reason:string,
    lakefusion_id:string
  >>')
) as exploded
where u.record_status = 'ACTIVE'
and u.scoring_results IS NOT NULL
and u.scoring_results != ''
"""

df_processed_unified = spark.sql(rebuild_query)


logger.info(f"  Building processed_unified from all active records with scoring_results")


# Overwrite processed_unified table (full rebuild)
df_processed_unified.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(processed_unified_table)

#print(f"✓ Processed unified table rebuilt with {processed_count} records")

# COMMAND ----------

logger.info("\n" + "="*80)
logger.info("STEP 11: OPTIMIZE PROCESSED UNIFIED TABLE")
logger.info("="*80)

spark.sql(f"""
    ALTER TABLE {processed_unified_table}
    CLUSTER BY (exploded_result.match, {master_id_key})
""")

spark.sql(f"OPTIMIZE {processed_unified_table}")



# COMMAND ----------

# Set task values
dbutils.jobs.taskValues.set("llm_matching_complete", True)


logger.info("\n" + "="*80)
logger.info("LLM MATCHING COMPLETED SUCCESSFULLY")
