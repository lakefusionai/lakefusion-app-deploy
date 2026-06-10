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

# MAGIC %pip install rapidfuzz

# COMMAND ----------

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DoubleType
from rapidfuzz.distance import JaroWinkler
from functools import reduce as ft_reduce

def _jw_similarity(s1, s2):
    if s1 is None or s2 is None:
        return None
    return float(JaroWinkler.similarity(s1, s2))

jaro_winkler_udf = F.udf(_jw_similarity, DoubleType())
spark.udf.register("jaro_winkler_similarity", _jw_similarity, DoubleType())

# COMMAND ----------

def build_dynamic_struct(x_col, attributes, entity_attributes_datatype):
    """Build a struct from a pipe-separated master attribute string."""
    struct_fields = []
    parts = F.split(x_col, "\\|")

    for idx, attr in enumerate(attributes):
        raw_value = F.trim(F.element_at(parts, idx + 1))

        # Last attribute contains "REF_NEW_006, <uuid>" — strip the UUID part
        if idx == len(attributes) - 1:
            raw_value = F.trim(F.regexp_extract(raw_value, r"^([^,]+),", 1))

        # Reverse Step 5's NULL → "null" coercion so allow_nulls comparisons
        # see actual NULL on the candidate side instead of the literal string.
        raw_value = F.when(
            (raw_value == "null") | (raw_value == ""),
            F.lit(None).cast("string"),
        ).otherwise(raw_value)

        dtype = entity_attributes_datatype.get(attr)
        if dtype:
            raw_value = raw_value.cast(dtype)
        struct_fields.append(raw_value.alias(f"{attr}_matches"))

    lakefusion_id = F.trim(F.regexp_extract(x_col, r"([a-f0-9]{32})", 1))
    struct_fields.append(lakefusion_id.alias("lakefusion_id"))

    return F.struct(*struct_fields)


# ─────────────────────────────────────────────────────────────
# build_rule  — adapted for new schema keys
#   old: rule_name, conditions[].column, fuzzy_type, logical_op
#   new: name,      conditions[].attribute, function, logical_operator
# ─────────────────────────────────────────────────────────────
def _apply_operator(col_expr: F.Column, operator: str, threshold) -> F.Column:
    return {
        ">=": col_expr >= threshold,
        "<=": col_expr <= threshold,
        ">":  col_expr >  threshold,
        "<":  col_expr <  threshold,
        "=":  col_expr == threshold,
        "==": col_expr == threshold,
        "!=": col_expr != threshold,
    }[operator]


def build_condition_column(cond: dict, rule_allow_nulls: bool) -> F.Column:
    """Returns a boolean Column: does this condition pass on the current row?
    Row must have flat columns: <attr> (source) and <attr>_matches (candidate)."""
    attr        = cond["attribute"]
    match_type  = cond.get("match_type", "exact")
    fuzzy_func  = cond.get("function")
    threshold   = cond.get("threshold")
    operator    = cond.get("operator", ">=")
    allow_nulls = cond.get("allow_nulls", rule_allow_nulls)

    # Normalize empty/whitespace strings to NULL on both sides. The candidate
    # side already does this in build_dynamic_struct (empty/"null" -> NULL),
    # but the source side comes straight from the DataFrame where empties
    # remain "". Without this, source="" vs candidate=NULL never matches
    # under either allow_nulls branch.
    src_raw  = F.col(attr).cast("string")
    cand_raw = F.col(f"{attr}_matches").cast("string")
    src  = F.when(F.trim(src_raw)  == "", F.lit(None).cast("string")).otherwise(src_raw)
    cand = F.when(F.trim(cand_raw) == "", F.lit(None).cast("string")).otherwise(cand_raw)

    left  = F.lower(src)
    right = F.lower(cand)

    if match_type == "exact":
        base = left == right
    elif match_type == "fuzzy":
        if fuzzy_func == "levenshtein_normalized":
            score = F.lit(1.0) - (
                F.levenshtein(left, right).cast("double") /
                F.greatest(F.length(left), F.length(right)).cast("double")
            )
            base = _apply_operator(score, operator, threshold)
        elif fuzzy_func == "levenshtein_standard":
            base = _apply_operator(F.levenshtein(left, right), operator, threshold)
        elif fuzzy_func == "jaro_winkler":
            # Python UDF on flat columns — works because we're not inside a lambda.
            base = _apply_operator(jaro_winkler_udf(left, right), operator, threshold)
        elif fuzzy_func == "soundex":
            base = F.soundex(src) == F.soundex(cand)
        else:
            raise ValueError(f"Unsupported fuzzy function: {fuzzy_func}")
    else:
        raise ValueError(f"Unsupported match_type: {match_type}")

    null_match = src.isNull()    & cand.isNull()
    not_null   = src.isNotNull() & cand.isNotNull()
    return (null_match | base) if allow_nulls else (not_null & base)

if is_single_source:
    def apply_rules(df_parsed: DataFrame, rules_config: list) -> DataFrame:
       """Evaluate each rule per (source row × candidate), produce one
       `<rule>_results` array column per rule containing matched candidate structs."""
       non_array_cols = [c for c in df_parsed.columns if c != "search_result_parsed"]
    
       # A. Explode: one row per candidate
       df_exp = df_parsed.withColumn("_candidate", F.explode("search_result_parsed"))
    
       # B. Flatten candidate struct fields to top-level columns.
       #    The candidate struct also carries `lakefusion_id`, which collides with
       #    the source row's own `lakefusion_id`. Flatten it under a distinct name
       #    so we can compare the two for self-match detection.
       candidate_fields = [
           f.name for f in
           df_parsed.schema["search_result_parsed"].dataType.elementType.fields
       ]
       for field in candidate_fields:
           if field == "lakefusion_id":
               df_exp = df_exp.withColumn("candidate_lakefusion_id", F.col(f"_candidate.{field}"))
           else:
               df_exp = df_exp.withColumn(field, F.col(f"_candidate.{field}"))
    
       # B.1 Self-match guard: a record must never match against its own id.
       #     Source id stays in the original `lakefusion_id` column (from exp.*).
       df_exp = df_exp.withColumn(
           "_is_self_match",
           F.col("candidate_lakefusion_id") == F.col("lakefusion_id"),
       )
    
       # C. Per rule: evaluate match flag (UDF runs on flat cols, not in a lambda)
       for rule in rules_config:
           rule_name        = rule["name"]
           logical_op       = rule.get("logical_operator", "AND").upper()
           rule_allow_nulls = rule.get("allow_nulls", False)
    
           cond_flags = [build_condition_column(c, rule_allow_nulls) for c in rule["conditions"]]
           rule_flag  = cond_flags[0]
           for flag in cond_flags[1:]:
               rule_flag = (rule_flag & flag) if logical_op == "AND" else (rule_flag | flag)
    
           # Exclude self-match: never count a candidate that is the source itself.
           rule_flag = rule_flag & (~F.col("_is_self_match"))
    
           # Coalesce 3VL NULL → False so passed rows don't disappear silently
           df_exp = df_exp.withColumn(
               f"_match_{rule_name}",
               F.coalesce(rule_flag, F.lit(False)),
           )
    
       # D. Aggregate back: per rule, collect candidates where _match_{rule_name} is true
       per_rule_aggs = []
       for rule in rules_config:
           rule_name = rule["name"]
           per_rule_aggs.append(
               F.collect_list(
                   F.when(F.col(f"_match_{rule_name}"), F.col("_candidate"))
               ).alias(f"{rule_name}_results")
           )
    
       df_grouped = df_exp.groupBy(*[F.col(c) for c in non_array_cols]).agg(*per_rule_aggs)
       return df_grouped

else:

    def apply_rules(df_parsed: DataFrame, rules_config: list) -> DataFrame:
        """Evaluate each rule per (source row × candidate), produce one
        `<rule>_results` array column per rule containing matched candidate structs."""
        non_array_cols = [c for c in df_parsed.columns if c != "search_result_parsed"]
    
        # A. Explode: one row per candidate
        df_exp = df_parsed.withColumn("_candidate", F.explode("search_result_parsed"))
    
        # B. Flatten candidate struct fields to top-level columns
        candidate_fields = [
            f.name for f in
            df_parsed.schema["search_result_parsed"].dataType.elementType.fields
        ]
        for field in candidate_fields:
            df_exp = df_exp.withColumn(field, F.col(f"_candidate.{field}"))
    
        # C. Per rule: evaluate match flag (UDF runs on flat cols, not in a lambda)
        for rule in rules_config:
            rule_name        = rule["name"]
            logical_op       = rule.get("logical_operator", "AND").upper()
            rule_allow_nulls = rule.get("allow_nulls", False)
    
            cond_flags = [build_condition_column(c, rule_allow_nulls) for c in rule["conditions"]]
            rule_flag  = cond_flags[0]
            for flag in cond_flags[1:]:
                rule_flag = (rule_flag & flag) if logical_op == "AND" else (rule_flag | flag)
    
            # Coalesce 3VL NULL → False so passed rows don't disappear silently
            df_exp = df_exp.withColumn(
                f"_match_{rule_name}",
                F.coalesce(rule_flag, F.lit(False)),
            )
    
        # D. Aggregate back: per rule, collect candidates where _match_{rule_name} is true
        per_rule_aggs = []
        for rule in rules_config:
            rule_name = rule["name"]
            per_rule_aggs.append(
                F.collect_list(
                    F.when(F.col(f"_match_{rule_name}"), F.col("_candidate"))
                ).alias(f"{rule_name}_results")
            )
    
        df_grouped = df_exp.groupBy(*[F.col(c) for c in non_array_cols]).agg(*per_rule_aggs)
        return df_grouped



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



df_cleaned = df_unified.withColumn(
    "search_results_array",
    F.split(F.regexp_replace(F.col("search_results"), r"^\[|\]$", ""), r"\],\s*\[")
)

df_cleaned = df_cleaned.withColumn(
    "search_results_master_lakefusion_ids",
    F.transform(
        F.col("search_results_array"),
        lambda x: F.regexp_extract(x, r"([a-f0-9]{32})", 1)
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
                F.coalesce(F.col(f"mst.{c}").cast("string"), F.lit(""))
                for c in attributes
            ]
        ),
        F.lit(", "),
        F.coalesce(F.col("mst.lakefusion_id").cast("string"), F.lit(""))
    )
)


# # # 6. Group back: one row per unified record with list of master candidates
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
        #original logic: 
        #F.lit(1.0)).cast("double").alias("score"),
        F.when(F.col("deterministic_match_result.action_on_match") == "MATCH", F.lit(1.0)) \
            .otherwise(F.lit(0.0)).cast("double").alias("score"),
        F.concat(
            F.lit("Due to Match Rule: "),
            F.col("deterministic_match_result.rule_name")
        ).alias("reason"),
        F.col("rule.lakefusion_id").alias("lakefusion_id")
    )
)

# COMMAND ----------

# Define which actions are "winning" — they suppress all other candidates for the surrogate_key.
MATCH_ACTIONS = ["MATCH"]   # adjust to your rules_config vocabulary

key_col = "surrogate_key" if not is_single_source else "lakefusion_id"

# Per surrogate_key, mark whether any MATCH-action row exists
has_match_window = Window.partitionBy(key_col)
df_final = df_final.withColumn(
    "_key_has_match",
    F.max(
        F.when(
            F.col("exploded_result.match").isin(MATCH_ACTIONS), F.lit(1)
        ).otherwise(F.lit(0))
    ).over(has_match_window),
)

# If the key has a MATCH: keep only MATCH-action rows.
# If the key has only NO_MATCH rows: keep them all.
df_final = df_final.filter(
    (F.col("_key_has_match") == 0)
    | (F.col("exploded_result.match").isin(MATCH_ACTIONS))
).drop("_key_has_match")

# COMMAND ----------

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
    # one row per (source record × matched candidate)
    window_spec = Window.partitionBy(
        "surrogate_key",
        F.when(
            F.col("exploded_result.match").isin(MATCH_ACTIONS), F.lit(None)
        ).otherwise(F.col("lakefusion_id")),
    ).orderBy(F.lit(1))
   
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
    window_spec = Window.partitionBy(
        "lakefusion_id",
        F.when(
            F.col("exploded_result.match").isin(MATCH_ACTIONS), F.lit(None)
        ).otherwise(F.col("exploded_result.lakefusion_id")),
    ).orderBy(F.lit(1))

df_final = df_final.withColumn("rank", F.row_number().over(window_spec))
df_final = df_final.filter(F.col("rank") == 1).drop("rank")

# Serialise the STRUCT to JSON so the column matches the table schema (STRING).
# Required after the deterministic_rules migration: Spark cannot CAST STRING → STRUCT
# in the MERGE condition below, so both sides must be STRING.
df_final = df_final.withColumn(
    "deterministic_match_result",
    F.to_json(F.col("deterministic_match_result")),
)

# COMMAND ----------

# COMMAND ----------

unified_deteministic_table_exists = spark.catalog.tableExists(unified_deteministic_table)

values = {col: f"source.{col}" for col in df_final.columns}

# Merge key: source row id + candidate id, so each (source × candidate) pair is its own row.
if not is_single_source:
    merge_condition = (
        f"target.{source_id_key} = source.{source_id_key} "
        f"AND target.lakefusion_id = source.lakefusion_id"
    )
    zorder_cols = f"{source_id_key}, lakefusion_id"
else:
    merge_condition = (
        f"target.{source_id_key} = source.{source_id_key} "
        f"AND target.exploded_result.lakefusion_id = source.exploded_result.lakefusion_id"
    )
    # Delta can't ZORDER on nested struct fields; ZORDER on source id only.
    zorder_cols = f"{source_id_key}"

if not unified_deteministic_table_exists:
    df_final.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(unified_deteministic_table)
else:
    delta_table = DeltaTable.forName(spark, unified_deteministic_table)

    deterministic_match_result_type = df_final.schema["deterministic_match_result"].dataType.simpleString()
    exploded_result_type = df_final.schema["exploded_result"].dataType.simpleString()

    delta_table.alias("target").merge(
        source=df_final.alias("source"),
        condition=merge_condition,
    ).whenMatchedUpdate(
        condition=f"""
            target.is_deterministic_match <> source.is_deterministic_match
            OR CAST(target.deterministic_match_result AS {deterministic_match_result_type}) <> CAST(source.deterministic_match_result AS {deterministic_match_result_type})
            OR to_json(CAST(target.exploded_result AS {exploded_result_type})) <> to_json(CAST(source.exploded_result AS {exploded_result_type}))
        """,
        set={
            "is_deterministic_match": "source.is_deterministic_match",
            "deterministic_match_result": "source.deterministic_match_result",
            "exploded_result": "source.exploded_result",
        },
    ).whenNotMatchedInsert(
        condition="source.is_deterministic_match = true",
        values=values,
    ).execute()

optimise_res = spark.sql(f"OPTIMIZE {unified_deteministic_table} ZORDER BY ({zorder_cols})")

# COMMAND ----------

logger_instance.shutdown()
