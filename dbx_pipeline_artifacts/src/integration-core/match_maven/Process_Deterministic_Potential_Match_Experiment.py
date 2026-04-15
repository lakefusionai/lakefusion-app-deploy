# Databricks notebook source
# DBTITLE 1,Imports
import json
from pyspark.sql.functions import col, lit, struct, to_json, collect_list, first

# COMMAND ----------

# DBTITLE 1,Widget Definitions
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("attributes", "[]", "Attributes JSON")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("is_single_source", "false", "Is Single Source (Golden Dedup)")
dbutils.widgets.text("deterministic_rules","")

# COMMAND ----------

# DBTITLE 1,Get Widget Values
catalog_name = dbutils.widgets.get("catalog_name")
entity = dbutils.widgets.get("entity")
attributes = dbutils.widgets.get("attributes")
experiment_id = dbutils.widgets.get("experiment_id")
is_single_source = dbutils.widgets.get("is_single_source")
deterministic_rules=dbutils.widgets.get("deterministic_rules")

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
rules_config = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="deterministic_rules", debugValue=deterministic_rules)
rules_config=json.loads(rules_config)

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

if len(rules_config)==0:
  dbutils.notebook.exit("No deterministic rules found")
else:
  logger.info("Proceed with Deterministic Rules")

# COMMAND ----------

# DBTITLE 1,Process Parameters
attributes = json.loads(attributes) if isinstance(attributes, str) and attributes else []

# Convert is_single_source to boolean
if isinstance(is_single_source, str):
    is_single_source = is_single_source.lower() == "true"
else:
    is_single_source = bool(is_single_source)

# COMMAND ----------

# DBTITLE 1,Set Keys and Table Names
if is_single_source:
    mode_name = "GOLDEN DEDUP (Single Source)"
    source_id_key = "lakefusion_id"
    match_id_key = "match_lakefusion_id"
else:
    mode_name = "NORMAL DEDUP (Multi Source)"
    source_id_key = "surrogate_key"
    match_id_key = "lakefusion_id"

# Define table names
unified_table = f"{catalog_name}.silver.{entity}_unified"
unified_dedup_table = f"{catalog_name}.silver.{entity}_unified_deduplicate"
processed_unified_table = f"{catalog_name}.silver.{entity}_processed_unified"
processed_unified_dedup_table = f"{catalog_name}.silver.{entity}_processed_unified_deduplicate"
master_table = f"{catalog_name}.gold.{entity}_master"
potential_match_table = f"{catalog_name}.silver.{entity}_deterministic_potential_match"
unified_deteministic_table = f"{catalog_name}.silver.{entity}_unified_deterministic"
unified_deteministic_dedup_table =f"{catalog_name}.silver.{entity}_unified_deterministic_deduplicate"

# Append experiment_id if provided
if experiment_id:
    unified_table += f"_{experiment_id}"
    unified_dedup_table += f"_{experiment_id}"
    processed_unified_table += f"_{experiment_id}"
    processed_unified_dedup_table += f"_{experiment_id}"
    master_table += f"_{experiment_id}"
    potential_match_table += f"_{experiment_id}"
    unified_deteministic_dedup_table += f"_{experiment_id}"
    unified_deteministic_table += f"_{experiment_id}"

# Set source tables based on mode
if is_single_source:
    source_table = unified_dedup_table
    deterministic_table = unified_deteministic_dedup_table
else:
    source_table = unified_table
    deterministic_table = unified_deteministic_table

# COMMAND ----------

# DBTITLE 1,Display Configuration
logger.info("=" * 80)
logger.info(f"PROCESS POTENTIAL MATCH - EXPERIMENT MODE ({mode_name})")
logger.info("=" * 80)
logger.info(f"Entity: {entity}")
logger.info(f"Experiment: {experiment_id}")
logger.info(f"Is Single Source: {is_single_source}")
logger.info(f"Source Table: {source_table}")

logger.info(f"Master Table: {master_table}")
logger.info(f"Potential Match Table: {potential_match_table}")
logger.info("")
logger.warning("EXPERIMENT MODE: Shows ALL matches (no match status filtering)")
logger.info("=" * 80)

# COMMAND ----------

# DBTITLE 1,STEP 1: Validate LLM Matching Completion
logger.info("\n" + "=" * 80)
logger.info("STEP 1: VALIDATE LLM MATCHING COMPLETION")
logger.info("=" * 80)

try:
    llm_complete = dbutils.jobs.taskValues.get(
        taskKey="Entity_Matching_LLM_Experiment",
        key="llm_matching_complete",
        debugValue=True
    )
    logger.info(f"LLM matching completed: {llm_complete}")
except Exception as e:
    logger.warning(f"Could not get task value (running in debug mode): {e}")
    llm_complete = True

# COMMAND ----------

# DBTITLE 1,STEP 3: Build Potential Match Query
logger.info("\n" + "=" * 80)
logger.info("STEP 3: BUILD POTENTIAL MATCH QUERY")
logger.info("=" * 80)

# Build attribute select list
attr_names = [attr.name if hasattr(attr, 'name') else attr for attr in attributes]
master_attrs = ", ".join([f"m.{attr}" for attr in attr_names])

if is_single_source:
    # Golden Dedup: query_lakefusion_id -> match_lakefusion_id
    potential_match_query = f"""
    SELECT
        m.lakefusion_id,
        m.attributes_combined,
{master_attrs},
        CONCAT('[', 
            ARRAY_JOIN(
                COLLECT_LIST(
                    TO_JSON(
                        STRUCT(
                            pd.exploded_result.id              as id,
                            pd.exploded_result.match           as match,
                            pd.exploded_result.score           as score,
                            pd.exploded_result.reason          as reason,
                            pd.exploded_result.lakefusion_id   as lakefusion_id,
                            pd.lakefusion_id                   as source_lakefusion_id
                        )
                    )
                ), 
                ','
            ), 
        ']') as potential_matches,
        COUNT(*) as match_count
    FROM {deterministic_table} pd
    JOIN {master_table} m ON pd.exploded_result.lakefusion_id = m.lakefusion_id
    WHERE pd.exploded_result.lakefusion_id IS NOT NULL
    AND pd.is_deterministic_match = true
    GROUP BY m.lakefusion_id, m.attributes_combined, {master_attrs}
    """
else:
    # Normal Dedup: surrogate_key -> lakefusion_id
    potential_match_query = f"""
    SELECT
        m.lakefusion_id,
        m.attributes_combined,
{master_attrs},
        CONCAT('[', 
            ARRAY_JOIN(
                COLLECT_LIST(
                    TO_JSON(
                        STRUCT(
                            pd.exploded_result.id              as id,
                            pd.exploded_result.match           as match,
                            pd.exploded_result.score           as score,
                            pd.exploded_result.reason          as reason,
                            pd.exploded_result.lakefusion_id   as lakefusion_id,
                            pd.surrogate_key                   as source_surrogate_key,
                            u.source_id                        as source_id,
                            u.source_path                      as source_name
                        )
                    )
                ), 
                ','
            ), 
        ']') as potential_matches,
        COUNT(*) as match_count
    FROM {deterministic_table} pd
    JOIN {source_table} u  ON pd.surrogate_key = u.surrogate_key
    JOIN {master_table} m  ON pd.exploded_result.lakefusion_id = m.lakefusion_id
    WHERE pd.exploded_result.lakefusion_id IS NOT NULL
    AND pd.is_deterministic_match = true
    AND u.record_status = 'ACTIVE'
    GROUP BY m.lakefusion_id, m.attributes_combined, {master_attrs}
    """

logger.info(f"Deterministic query built for {mode_name}")

# COMMAND ----------

deterministic_table

# COMMAND ----------

# DBTITLE 1,STEP 4: Execute Query and Create Potential Match Table
logger.info("\n" + "=" * 80)
logger.info("STEP 4: CREATE POTENTIAL MATCH TABLE")
logger.info("=" * 80)

try:
    df_potential = spark.sql(potential_match_query)

    # Write to potential match table
    df_potential.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(potential_match_table)

    logger.info(f"Created potential match table: {potential_match_table}")

except Exception as e:
    error_msg = f"Failed to create potential match table: {str(e)}"
    logger.error(error_msg)
    import traceback
    logger.error(traceback.format_exc())
    dbutils.notebook.exit(error_msg)

# COMMAND ----------

# DBTITLE 1,STEP 5: Optimize Table
logger.info("\n" + "=" * 80)
logger.info("STEP 5: OPTIMIZE POTENTIAL MATCH TABLE")
logger.info("=" * 80)

try:
    spark.sql(f"OPTIMIZE {potential_match_table}")
    logger.info(f"Optimized potential match table")
except Exception as e:
    logger.warning(f"Optimization skipped: {str(e)}")


logger.info("\n" + "=" * 80)
logger.info(f"POTENTIAL MATCH PROCESSING COMPLETED ({mode_name})")
logger.info("=" * 80)
logger.info(f"Mode: {mode_name}")
logger.info(f"Potential Match Table: {potential_match_table}")

logger_instance.shutdown()

dbutils.notebook.exit(json.dumps({
    "status": "success",
    "mode": mode_name
}))
