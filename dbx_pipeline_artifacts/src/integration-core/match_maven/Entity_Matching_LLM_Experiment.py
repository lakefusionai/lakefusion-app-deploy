# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Imports
import json
from pyspark.sql.functions import col, lit, row_number, collect_list, struct, to_json, when, udf, concat_ws
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, DoubleType

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

# COMMAND ----------

# DBTITLE 1,Get Task Values (Override Widget Values)
# Get values from Parse_Entity_Model_JSON task if running in workflow
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
llm_temperature = float(llm_temperature) if llm_temperature else 0.0

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
max_potential_matches = int(max_potential_matches) if max_potential_matches else 3

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

# Convert is_single_source to boolean
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
unified_deteministic_dedup_table =f"{catalog_name}.silver.{entity}_unified_deterministic_deduplicate"

# Append experiment_id if provided
if experiment_id:
    unified_table += f"_{experiment_id}"
    unified_dedup_table += f"_{experiment_id}"
    processed_unified_table += f"_{experiment_id}"
    processed_unified_dedup_table += f"_{experiment_id}"
    master_table += f"_{experiment_id}"
    unified_deteministic_dedup_table += f"_{experiment_id}"
    unified_deteministic_table += f"_{experiment_id}"

# Set source and processed tables based on mode
if is_single_source:
    source_table = unified_dedup_table
    processed_table = processed_unified_dedup_table
    deterministic_table = unified_deteministic_dedup_table
else:
    source_table = unified_table
    processed_table = processed_unified_table
    deterministic_table=unified_deteministic_table

# Parse master table components
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
logger.info("\n" + "="*80)
logger.info("STEP 2: VALIDATE LLM ENDPOINT")
logger.info("="*80)

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
logger.info("\n" + "="*80)
logger.info("STEP 2.5: WARM UP LLM ENDPOINT (SCALE-FROM-ZERO HANDLING)")
logger.info("="*80)

# Build warm-up query - uses a single record to wake up the endpoint
warmup_query = f"""
SELECT ai_query('{llm_endpoint}', 'Say hello and confirm you are ready.')
FROM {source_table}
WHERE search_results IS NOT NULL AND search_results != ''
LIMIT 1
"""

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

# DBTITLE 1,STEP 4: Filter Records for LLM Processing
logger.info("\n" + "=" * 80)
logger.info("STEP 4: FILTER RECORDS FOR LLM PROCESSING")
logger.info("=" * 80)

# Build filter query based on mode
if is_single_source:
    if deteministic_unified_table_exists:
      filter_query = f"""
        select
          u.{source_id_key},
          u.{merged_desc_column},
          u.search_results
        from
          {source_table} u left anti join {deterministic_table} ud on u.{master_id_key} = ud.{master_id_key}
        where
          u.search_results IS NOT NULL
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
          u.search_results
        from
          {source_table} u left anti join {deterministic_table} ud on u.{source_id_key} = ud.{source_id_key}
        where
          u.record_status = 'ACTIVE'
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

# Count records to process
df_to_process = spark.sql(f"with {uf_sub_query} select count(*) as cnt from uf")
records_to_process = df_to_process.take(1)


logger.info(f"Filter: {'All' if is_single_source else 'ACTIVE'} records with search_results and empty scoring_results")
logger.info(f"  Records to process: {records_to_process}")


# COMMAND ----------

# DBTITLE 1,Handle No Records to Process
if records_to_process == 0:
    logger.info("\nNo new records to process")
    
    # Check if there are existing scoring_results to rebuild processed table
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
    
    #existing_scored = spark.sql(check_query).collect()[0]['cnt']
    existing_scored = spark.sql(check_query).isEmpty()
    
    if not existing_scored:
        logger.info(f"  Found {existing_scored} records with existing scoring_results")
        logger.info("  Rebuilding processed table from existing data...")
        
        # Rebuild processed table
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
        #result_count = df_from_scoring.count()
        
        #print(f"  Results from scoring_results: {result_count}")
        
        # Overwrite processed table
        df_from_scoring.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(processed_table)
        #print(f"✓ Processed table updated with {result_count} records")
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

# DBTITLE 1,Safe Format Function
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
            deterministic_match_result,
            is_deterministic_match,

            NAMED_STRUCT(
                'id', exploded_result.id,
                'score', CAST(exploded_result.score AS DOUBLE),
                'reason', exploded_result.reason,
                'lakefusion_id', exploded_result.lakefusion_id
            ) AS exploded_result
        FROM {deterministic_table}
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

# Construct LLM query - EXPERIMENT MODE: Skip master table validation
# For single source, we need different column names to avoid ambiguity
if is_single_source:
    final_select = f"""
select
  er.{source_id_key} as query_lakefusion_id,
  er.exploded_result.{master_id_key} as match_lakefusion_id,
  er.exploded_result,
  er.exploded_result.`id` as exploded_result_id,
  er.combined_results as scoring_results_json
from
  er
where
  er.exploded_result.{master_id_key} is not null
"""
else:
    final_select = f"""
select
  er.{source_id_key},
  er.exploded_result.{master_id_key} as {master_id_key},
  er.exploded_result,
  er.exploded_result.`id` as exploded_result_id,
  er.combined_results as scoring_results_json
from
  er
where
  er.exploded_result.{master_id_key} is not null
"""

llm_query = f"""
with {uf_sub_query},
llm_results as (
  SELECT
    {source_id_key},
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
  uf.{source_id_key} = llm_results.{source_id_key}
  
)
{final_select}
"""

# COMMAND ----------

# DBTITLE 1,STEP 6: Execute LLM Query
logger.info("\n" + "=" * 80)
logger.info("STEP 6: EXECUTE LLM QUERY")
logger.info("=" * 80)

logger.info("Running LLM matching (this may take a while)...")

try:
    df_llm_results = spark.sql(llm_query)
   
    
    # Use appropriate key for distinct count
    distinct_key = "query_lakefusion_id" if is_single_source else source_id_key
    #unique_source_records = df_llm_results.select(distinct_key).distinct().count()
    
    logger.info(f"LLM processing completed")
  
    
except Exception as e:
    error_msg = f"ERROR: LLM query execution failed: {str(e)}"
    logger.error(error_msg)
    import traceback
    logger.error(traceback.format_exc())
    logger_instance.shutdown()
    dbutils.notebook.exit(error_msg)

# COMMAND ----------

# DBTITLE 1,STEP 7: Filter Results
logger.info("\n" + "=" * 80)
logger.info("STEP 7: FILTER RESULTS")
logger.info("=" * 80)

# For single source, we use query_lakefusion_id as partition key and match_lakefusion_id for null check
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

# Use appropriate key for aggregation
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

#update_count = df_scoring_agg.count()
#print(f"  Updating {update_count} records in source table with scoring_results")

df_scoring_agg.createOrReplaceTempView("scoring_updates")

# For single source, the source table key is lakefusion_id
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
#print(f"✓ Processed table rebuilt with {processed_count} records")

# COMMAND ----------

# DBTITLE 1,STEP 11: Optimize Processed Table
logger.info("\n" + "=" * 80)
logger.info("STEP 11: OPTIMIZE PROCESSED TABLE")
logger.info("=" * 80)

try:
    spark.sql(f"OPTIMIZE {processed_table} ZORDER BY ({source_id_key})")
    logger.info(f"Optimized processed table")
except Exception as e:
    logger.info(f"Optimization skipped: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Set Task Values and Exit
dbutils.jobs.taskValues.set("llm_matching_complete", True)
#dbutils.jobs.taskValues.set("records_processed", results_count)

logger.info("\n" + "=" * 80)
logger.info(f"LLM MATCHING COMPLETED SUCCESSFULLY ({mode_name})")
logger.info("=" * 80)
logger.info(f"Mode: {mode_name}")
#print(f"New records processed in this run: {results_count}")
#print(f"Total records in processed table: {final_processed_count}")
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
    #"records_processed": results_count,
    "processed_table": processed_table,
    #"total_in_processed": final_processed_count
}))
