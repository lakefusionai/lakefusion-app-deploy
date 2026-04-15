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
import time as _time
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("experiment_id", "", "Match Maven Experiment")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("attributes", "", "Merged Description Attributes")
dbutils.widgets.text("catalog_name", "", "catalog name")
dbutils.widgets.text("entity_attributes_datatype","","entity_attributes_datatype")
dbutils.widgets.text("deterministic_rules","")
dbutils.widgets.text("is_single_source", "false", "Is Single Source (Golden Dedup)")

# COMMAND ----------

attributes = dbutils.widgets.get("attributes")
catalog_name=dbutils.widgets.get("catalog_name")
entity = dbutils.widgets.get("entity")
experiment_id = dbutils.widgets.get("experiment_id")
entity_attributes_datatype=dbutils.widgets.get("entity_attributes_datatype")
deterministic_rules=dbutils.widgets.get("deterministic_rules")
is_single_source = dbutils.widgets.get("is_single_source")


# COMMAND ----------

entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
entity_attributes_datatype = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes_datatype", debugValue=entity_attributes_datatype)
attributes = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="match_attributes", debugValue=attributes)
rules_config = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="deterministic_rules", debugValue=deterministic_rules)
is_single_source = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="is_single_source",
    debugValue=is_single_source
)
rules_config=json.loads(rules_config)

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

if len(rules_config)==0:
  dbutils.notebook.exit("ERROR, No deterministic rules found")
else:
  logger.info("Proceed with Deterministic Rules")

# COMMAND ----------

if isinstance(is_single_source, str):
    is_single_source = is_single_source.lower() == "true"
else:
    is_single_source = bool(is_single_source)

# COMMAND ----------

master_id_key = "lakefusion_id"
search_results = "search_results"

if is_single_source:
    source_id_key = "lakefusion_id"
    mode_name = "GOLDEN DEDUP (Single Source)"
else:
    source_id_key = "surrogate_key"
    mode_name = "NORMAL DEDUP (Multi Source)"

# COMMAND ----------

# DBTITLE 1,Untitled
attributes = json.loads(attributes)
id_key="master_lakefusion_id"
entity_attributes_datatype = json.loads(entity_attributes_datatype)


# COMMAND ----------

unified_table = f"{catalog_name}.silver.{entity}_unified"
unified_deteministic_table = f"{catalog_name}.silver.{entity}_unified_deterministic"
master_table = f"{catalog_name}.gold.{entity}_master"
processed_unified_table = f"{catalog_name}.silver.{entity}_processed_unified"
if(is_single_source):
  unified_table += f"_deduplicate"
  processed_unified_table += f"_deduplicate"
  unified_deteministic_table += f"_deduplicate"
if experiment_id:
  unified_table += f"_{experiment_id}"
  master_table += f"_{experiment_id}"
  processed_unified_table += f"_{experiment_id}"
  unified_deteministic_table += f"_{experiment_id}"

merge_activities_table = f"{master_table}_merge_activities"
master_attribute_version_sources_table = f"{master_table}_attribute_version_sources"

master_table_list = master_table.split(".")
master_catalog, master_schema, master_table_name = master_table_list[0], master_table_list[1], master_table_list[2]

# COMMAND ----------

if is_single_source:
    df_unified = (
        spark.read.table(unified_table)
        .filter(F.col("search_results") != "")
        .filter(F.col("scoring_results") == "")
    )
else:
    df_unified = (
        spark.read.table(unified_table)
        .filter(F.col("record_status") == "ACTIVE")
        .filter(F.col("search_results") != "")
        .filter(F.col("scoring_results") == "")
    )

df_master=spark.read.table(master_table)

# COMMAND ----------

def build_dynamic_struct(x_col, attributes, entity_attributes_datatype):
    """Build a struct from a pipe-separated master attribute string."""
    struct_fields = []

    parts = F.split(x_col, "\\|")
    last_attr_idx = len(attributes)  # index of the last attribute in parts

    for idx, attr in enumerate(attributes):
        raw_value = F.trim(F.element_at(parts, idx + 1))

        # Last attribute contains "REF_NEW_006, <uuid>" — strip the UUID part
        if idx == len(attributes) - 1:
            raw_value = F.trim(F.regexp_extract(raw_value, r"^([^,]+),", 1))

        dtype = entity_attributes_datatype.get(attr)
        if dtype:
            raw_value = raw_value.cast(dtype)
        struct_fields.append(raw_value.alias(f"{attr}_matches"))

    # Extract only the UUID from the entire string
    lakefusion_id = F.trim(F.regexp_extract(x_col, r"([a-f0-9]{32})", 1))
    struct_fields.append(lakefusion_id.alias("lakefusion_id"))

    return F.struct(*struct_fields)


# ─────────────────────────────────────────────────────────────
# build_rule  — adapted for new schema keys
#   old: rule_name, conditions[].column, fuzzy_type, logical_op
#   new: name,      conditions[].attribute, function, logical_operator
# ─────────────────────────────────────────────────────────────
def build_rule(rule: dict):
    """
    Build a Spark filter expression for a single rule dict.
    Supports match_type: 'exact' | 'fuzzy'
    Supports function:   'levenshtein' | 'jaro_winkler' | 'soundex'
    """
    rule_name  = rule["name"]
    conditions = rule["conditions"]
    logical_op = rule.get("logical_operator", "AND")

    condition_parts = []

    for cond in conditions:
        attr       = cond["attribute"]                  # ← was "column"
        match_type = cond.get("match_type", "exact")
        fuzzy_func = cond.get("function")               # ← was "fuzzy_type"
        threshold  = cond.get("threshold")
        operator = cond.get("operator",">=")

        if match_type == "exact":
            condition_parts.append(f"(lower({attr}) =lower(x.{attr}_matches))")

        elif match_type == "fuzzy":
            if fuzzy_func == "levenshtein_normalized":
                # threshold is a similarity ratio (0–1); convert to distance-based check
                condition_parts.append(
            f"(1 - (levenshtein({attr}, x.{attr}_matches) / "
            f"greatest(length({attr}), length(x.{attr}_matches)))) {operator} {threshold}"
        )
                
            elif fuzzy_func == "levenshtein_standard":
                condition_parts.append(f"(levenshtein({attr}, x.{attr}_matches) {operator} {threshold})")
            elif fuzzy_func == "jaro_winkler":
                condition_parts.append(
                    f"(jaro_winkler_similarity({attr}, x.{attr}_matches) >= {threshold})"
                )
            elif fuzzy_func == "soundex":
                condition_parts.append(
                    f"(soundex({attr}) = soundex(x.{attr}_matches))"
                )
            else:
                raise ValueError(f"Unsupported fuzzy function: {fuzzy_func}")
        else:
            raise ValueError(f"Unsupported match_type: {match_type}")

    combined = f" {logical_op} ".join(condition_parts)
    expr_str  = f"filter(search_result_parsed, x -> ({combined}))"

    return F.expr(expr_str).alias(rule_name)



def apply_rules(df_parsed: DataFrame, rules_config: list) -> DataFrame:
    df_out = df_parsed
    for rule in rules_config:
        df_out = df_out.withColumn(f"{rule['name']}_results", build_rule(rule))
    return df_out



def compute_deterministic_matches(df_with_rules: DataFrame, rules_config: list) -> DataFrame:

    # Only consider rules that are actual matches
    match_rules = [r for r in rules_config]
    match_cols  = [f"{r['name']}_results" for r in match_rules]

    # 1️⃣ Flag: is there any qualifying deterministic match?
    is_match_expr = F.lit(False)
    for c in match_cols:
        is_match_expr = is_match_expr | (F.size(F.col(c)) > 0)
    df_out = df_with_rules.withColumn("is_deterministic_match", is_match_expr)

    # 2️⃣ Pick first matching rule via coalesce (preserves priority order)
    when_exprs = []
    for rule in match_rules:
        c      = f"{rule['name']}_results"
        action = rule["action_on_match"]
        when_exprs.append(
            F.when(
                F.size(F.col(c)) > 0,
                F.struct(
                    F.col(c).alias("rule_result"),
                    F.lit(rule["name"]).alias("rule_name"),
                    F.lit(action).alias("action_on_match")   # ← new: carried forward
                )
            )
        )

    df_out = df_out.withColumn(
        "deterministic_match_result",
        F.coalesce(*when_exprs) if when_exprs else F.lit(None)
    )

    return df_out



# 2. Parse search_results string into array
df_cleaned = df_unified.withColumn(
    "search_results_array",
    F.split(F.regexp_replace(F.col("search_results"), r"^\[|\]$", ""), "\\], \\[")
)

df_cleaned = df_cleaned.withColumn(
    "search_results_master_lakefusion_ids",
    F.transform(
        F.col("search_results_array"),
        lambda x: F.trim(F.split(F.trim(F.element_at(F.split(x, r"\|"), -1)), ",")[1])
    )
)

# 3. Explode to one row per candidate master record
df_exploded = df_cleaned.withColumn(
    "search_results_master_lakefusion_ids",
    F.explode("search_results_master_lakefusion_ids")
).alias("exp")

df_master = df_master.alias("mst")

# 4. Join unified exploded rows with master table
df_joined = df_exploded.join(
    df_master,
    F.col("exp.search_results_master_lakefusion_ids") == F.col("mst.lakefusion_id"),
    "inner"
)

# 5. Build pipe-separated attribute string for each master record
df_joined = df_joined.withColumn(
    "attributes_combined_master",
    F.concat(
        F.concat_ws(
            " | ",
            *[
                F.when(F.trim(F.col(f"mst.{c}")) == "", "null")
                 .otherwise(F.coalesce(F.col(f"mst.{c}").cast("string"), F.lit("null")))
                for c in attributes
            ]
        ),
        F.lit(", "),
        F.coalesce(F.col("mst.lakefusion_id").cast("string"), F.lit("null"))
    )
)

# # 6. Group back: one row per unified record with list of master candidates
group_cols = [f"exp.{c}" for c in df_exploded.columns]
df_result = (
    df_joined
    .groupBy(*group_cols)
    .agg(F.collect_set("attributes_combined_master").alias("attributes_combined_master_array"))
    .select("exp.*", "attributes_combined_master_array", "search_results_master_lakefusion_ids")
)

# # 7. Parse each master candidate string into a typed struct
df_parsed = df_result.withColumn(
    "search_result_parsed",
    F.transform(
        F.col("attributes_combined_master_array"),
        lambda x: build_dynamic_struct(x, attributes, entity_attributes_datatype)
    )
)

# # 8. Apply deterministic rules
df_with_rules = apply_rules(df_parsed, rules_config)

# 9. Compute first-match deterministic result
df_final = compute_deterministic_matches(df_with_rules, rules_config)

# # 10. Explode matched rule results (one row per matched candidate)
df_exploded = df_final.withColumn(
    "rule", F.explode("deterministic_match_result.rule_result")
)

# # 11. Build exploded_result struct
selected_rule_fields = [
    f.name for f in df_exploded.schema["rule"].dataType.fields
    if f.name != "lakefusion_id"
]
concat_col = F.concat_ws(" | ", *[F.col(f"rule.{f}") for f in selected_rule_fields])

df_final = df_exploded.withColumn(
    "exploded_result",
    F.struct(
        concat_col.alias("id"),
        F.col("deterministic_match_result.action_on_match").alias("match"),
        F.lit(1.0).cast("double").alias("score"),
        F.concat(
            F.lit("Due to Match Rule: "),
            F.col("deterministic_match_result.rule_name")
        ).alias("reason"),
        F.col("rule.lakefusion_id").alias("lakefusion_id")
    )
)

# COMMAND ----------

if not is_single_source:
    df_final = (
        df_final.filter(F.col("is_deterministic_match") == F.lit(True))
        .drop("rule")
        .select(
            "surrogate_key",
            "search_results_master_lakefusion_ids",
            "attributes_combined",
            "search_results",
            "deterministic_match_result",
            "is_deterministic_match",
            "exploded_result",
        )
        .withColumnRenamed("search_results_master_lakefusion_ids", "lakefusion_id")
    )
    window_spec = Window.partitionBy("surrogate_key").orderBy(
    F.lit(1))
else:
    df_final = (
        df_final.filter(F.col("is_deterministic_match") == F.lit(True))
        .drop("rule")
        .select(
            "lakefusion_id",
            "attributes_combined",
            "search_results",
            "deterministic_match_result",
            "is_deterministic_match",
            "exploded_result",
        )
    )
    window_spec = Window.partitionBy("lakefusion_id").orderBy(
    F.lit(1))

df_final=df_final.withColumn("rank", F.row_number().over(window_spec))
df_final = df_final.filter(F.col("rank") == 1).drop("rank")

# COMMAND ----------


unified_deteministic_table_exists = spark.catalog.tableExists(unified_deteministic_table)


values = {col: f"source.{col}" for col in df_final.columns}

if not unified_deteministic_table_exists:
    df_final.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(unified_deteministic_table)
else:
    # Define the Delta table
    delta_table = DeltaTable.forName(spark, unified_deteministic_table)

    # Get the nullable struct type string to use in CAST
    deterministic_match_result_type = df_final.schema["deterministic_match_result"].dataType.simpleString()
    exploded_result_type = df_final.schema["exploded_result"].dataType.simpleString()

    # Perform merge operation
    delta_table.alias("target").merge(
        source=df_final.alias("source"),
        condition=f"target.{source_id_key} = source.{source_id_key}"
    ).whenMatchedUpdate(
        condition=f"""
            target.is_deterministic_match <> source.is_deterministic_match
            OR CAST(target.deterministic_match_result AS {deterministic_match_result_type}) <> CAST(source.deterministic_match_result AS {deterministic_match_result_type})
            OR to_json(CAST(target.exploded_result AS {exploded_result_type})) <> to_json(CAST(source.exploded_result AS {exploded_result_type}))
        """,
        set={
            "is_deterministic_match": "source.is_deterministic_match",
            "deterministic_match_result": "source.deterministic_match_result",
            "exploded_result": "source.exploded_result"
        }
    ).whenNotMatchedInsert(
        condition="source.is_deterministic_match = true",
        values=values
    ).execute()

optimise_res = spark.sql(f"OPTIMIZE {unified_deteministic_table} ZORDER BY ({source_id_key})")

# COMMAND ----------

logger_instance.shutdown()
