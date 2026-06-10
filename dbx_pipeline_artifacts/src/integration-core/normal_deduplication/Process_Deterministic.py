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
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf
from functools import reduce
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("experiment_id", "", "Match Maven Experiment")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("attributes", "", "Merged Description Attributes")
dbutils.widgets.text("catalog_name", "", "catalog name")
dbutils.widgets.text("entity_attributes_datatype","","entity_attributes_datatype")
dbutils.widgets.text("deterministic_rules","","deterministic_rules")
dbutils.widgets.text("config_thresholds", "", "Match Thresholds Config")

# COMMAND ----------

attributes = dbutils.widgets.get("attributes")
catalog_name=dbutils.widgets.get("catalog_name")
entity = dbutils.widgets.get("entity")
experiment_id = dbutils.widgets.get("experiment_id")
entity_attributes_datatype=dbutils.widgets.get("entity_attributes_datatype")
deterministic_rules=dbutils.widgets.get("deterministic_rules")
config_thresholds = dbutils.widgets.get("config_thresholds")


# COMMAND ----------

entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
entity_attributes_datatype = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes_datatype", debugValue=entity_attributes_datatype)
attributes = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="match_attributes", debugValue=attributes)
rules_config = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="deterministic_rules", debugValue=deterministic_rules)
config_thresholds = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="config_thresholds", debugValue=config_thresholds)

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

entity_attributes_datatype = json.loads(entity_attributes_datatype)
rules_config=json.loads(rules_config)

# COMMAND ----------

attributes = json.loads(attributes)
id_key="surrogate_key"

# COMMAND ----------

unified_table = f"{catalog_name}.silver.{entity}_unified"
unified_deteministic_table = f"{catalog_name}.silver.{entity}_unified_deterministic"
master_table = f"{catalog_name}.gold.{entity}_master"
processed_unified_table = f"{catalog_name}.silver.{entity}_processed_unified"
validation_error_unified_table = f"{catalog_name}.silver.{entity}_unified_validation_error"
if experiment_id:
  unified_table += f"_{experiment_id}"
  master_table += f"_{experiment_id}"
  validation_error_unified_table+=f"_{experiment_id}"
  processed_unified_table += f"_{experiment_id}"
  unified_deteministic_table += f"_{experiment_id}"

merge_activities_table = f"{master_table}_merge_activities"
master_attribute_version_sources_table = f"{master_table}_attribute_version_sources"

master_table_list = master_table.split(".")
master_catalog, master_schema, master_table_name = master_table_list[0], master_table_list[1], master_table_list[2]

# COMMAND ----------

config_thresholds = json.loads(config_thresholds)
merge_thresholds = config_thresholds.get('merge', [0.9, 1.0])
matches_thresholds = config_thresholds.get('matches', [0.7, 0.89])
not_match_thresholds = config_thresholds.get('not_match', [0.0, 0.69])

merge_min, merge_max = merge_thresholds[0], merge_thresholds[1]
matches_min, matches_max = matches_thresholds[0], matches_thresholds[1]
not_match_min, not_match_max = not_match_thresholds[0], not_match_thresholds[1]


# COMMAND ----------

if len(rules_config)==0:
  dbutils.notebook.exit("No deterministic rules found")
else:
  logger.info("Proceed with Deterministic Rules")

# COMMAND ----------

df_unified = spark.read.table(unified_table).filter(F.col("record_status")=='ACTIVE').filter(F.col("search_results")!='').filter(F.col("scoring_results")=='')
df_master=spark.read.table(master_table)

# COMMAND ----------

%pip install rapidfuzz

# COMMAND ----------

# DBTITLE 1,Cell 11
# ═══════════════════════════════════════════════════════════════════════════════
# LakeFusion — Excure Match Pipeline  (v2)
# ───────────────────────────────────────────────────────────────────────────────
# Changes:
#   1. all_rule_results: ALL candidates × ALL rules (passed + failed)
#   2. condition_scores: array<{attribute, score, threshold}> replaces two maps
#   3. NOT_A_MATCH bug fix: excludes individual candidates, not the whole record
#      — only candidates matched by NOT_A_MATCH rules are removed from llm_candidates
#      — remaining candidates still flow to LLM
# ═══════════════════════════════════════════════════════════════════════════════

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DoubleType
from rapidfuzz.distance import JaroWinkler
from functools import reduce as ft_reduce   # alias avoids clash with pyspark.sql.functions.reduce (Spark 3.3+)


# ── 1. JW UDF ──────────────────────────────────────────────────────────────────

def _jw_similarity(s1, s2):
    if s1 is None or s2 is None:
        return None
    return float(JaroWinkler.similarity(s1, s2))

jaro_winkler_udf = F.udf(_jw_similarity, DoubleType())
spark.udf.register("jaro_winkler_similarity", _jw_similarity, DoubleType())


# ── 2. Dynamic struct builder ──────────────────────────────────────────────────

def build_dynamic_struct(x_col, attributes, entity_attributes_datatype):
    struct_fields = []
    parts = F.split(x_col, "\\|")
    for idx, attr in enumerate(attributes):
        raw_value = F.trim(F.element_at(parts, idx + 1))
        if idx == len(attributes) - 1:
            raw_value = F.trim(F.regexp_extract(raw_value, r"^([^,]+),", 1))
        # Reverse Step 5's null→"null" coercion so allow_nulls comparisons see
        # actual NULL on the candidate side instead of the literal string "null".
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


# ── 3. Operator helper ──────────────────────────────────────────────────────────

def _apply_operator(col_expr: F.Column, operator: str, threshold) -> F.Column:
    return {
        ">=": col_expr >= threshold,
        "<=": col_expr <= threshold,
        ">":  col_expr >  threshold,
        "<":  col_expr <  threshold,
        "=":  col_expr == threshold,
        "==": col_expr == threshold,
        "!=": col_expr != threshold
    }[operator]


# ── 4. Condition match Column (True/False) ─────────────────────────────────────

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


# ── 5. Actual score Column for a single condition ──────────────────────────────

def build_score_column(cond: dict) -> F.Column:
    attr       = cond["attribute"]
    match_type = cond.get("match_type", "exact")
    fuzzy_func = cond.get("function")

    left  = F.lower(F.col(attr).cast("string"))
    right = F.lower(F.col(f"{attr}_matches").cast("string"))

    if match_type == "exact":
        return F.when(left == right, F.lit(1.0)).otherwise(F.lit(0.0)).cast("double")

    elif match_type == "fuzzy":
        if fuzzy_func == "jaro_winkler":
            return jaro_winkler_udf(left, right).cast("double")
        elif fuzzy_func == "levenshtein_normalized":
            return (
                F.lit(1.0) - (
                    F.levenshtein(left, right).cast("double") /
                    F.greatest(F.length(left), F.length(right)).cast("double")
                )
            )
        elif fuzzy_func == "levenshtein_standard":
            lev  = F.levenshtein(left, right).cast("double")
            mlen = F.greatest(F.length(left), F.length(right)).cast("double")
            return F.lit(1.0) - (lev / F.when(mlen == 0, F.lit(1.0)).otherwise(mlen))
        elif fuzzy_func == "soundex":
            return F.when(
                F.soundex(F.col(attr).cast("string")) ==
                F.soundex(F.col(f"{attr}_matches").cast("string")),
                F.lit(1.0)
            ).otherwise(F.lit(0.0)).cast("double")

    return F.lit(0.0).cast("double")



def apply_rules(df_parsed: DataFrame, rules_config: list) -> DataFrame:
    """Per-rule, per-candidate match flags. Output: one `<rule_name>_results`
    array column per rule containing only the candidates that satisfied the rule."""
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
    """Pick first matching rule via coalesce (preserves rule priority order),
    wrap matched candidates + rule metadata into deterministic_match_result."""
    match_cols = [f"{r['name']}_results" for r in rules_config]

    is_match_expr = F.lit(False)
    for c in match_cols:
        is_match_expr = is_match_expr | (F.size(F.col(c)) > 0)
    df_out = df_with_rules.withColumn("is_deterministic_match", is_match_expr)

    when_exprs = []
    for rule in rules_config:
        c      = f"{rule['name']}_results"
        action = rule["action_on_match"]
        when_exprs.append(
            F.when(
                F.size(F.col(c)) > 0,
                F.struct(
                    F.col(c).alias("rule_result"),
                    F.lit(rule["name"]).alias("rule_name"),
                    F.lit(action).alias("action_on_match"),
                )
            )
        )

    df_out = df_out.withColumn(
        "deterministic_match_result",
        F.coalesce(*when_exprs) if when_exprs else F.lit(None),
    )
    return df_out


# ═══════════════════════════════════════════════════════════════════════════════
# PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════

# ── Step 1: Parse raw search_results
# Inter-element separator can be `],[` (no space) or `], [`; \s* tolerates both.
df_cleaned = df_unified.withColumn(
    "search_results_array",
    F.split(F.regexp_replace(F.col("search_results"), r"^\[|\]$", ""), r"\],\s*\["),
)

# ── Step 2: Extract lakefusion_id from each search result item
df_cleaned = df_cleaned.withColumn(
    "search_results_master_lakefusion_ids",
    F.transform(
        F.col("search_results_array"),
        lambda x: F.regexp_extract(x, r"([a-f0-9]{32})", 1),
    ),
)

# ── Step 3: Explode to one row per (source × candidate)
df_exploded = df_cleaned.withColumn(
    "search_results_master_lakefusion_ids",
    F.explode("search_results_master_lakefusion_ids"),
).alias("exp")

df_master = df_master.alias("mst")

# ── Step 4: Join with master table
df_joined = df_exploded.join(
    df_master,
    F.col("exp.search_results_master_lakefusion_ids") == F.col("mst.lakefusion_id"),
    "inner",
)

# ── Step 5: Build pipe-separated attribute string per master candidate
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


# ── Step 6: Group back — one row per (source × candidate)
group_cols = [f"exp.{c}" for c in df_exploded.columns]
df_result = (
    df_joined
    .groupBy(*group_cols)
    .agg(F.collect_set("attributes_combined_master").alias("attributes_combined_master_array"))
    .select("exp.*", "attributes_combined_master_array", "search_results_master_lakefusion_ids")
)

# ── Step 7: Parse candidate strings into typed structs
df_parsed = df_result.withColumn(
    "search_result_parsed",
    F.transform(
        F.col("attributes_combined_master_array"),
        lambda x: build_dynamic_struct(x, attributes, entity_attributes_datatype),
    ),
)

# ── Step 8: Apply deterministic rules
df_with_rules = apply_rules(df_parsed, rules_config)

# ── Step 9: Compute deterministic match struct
df_final = compute_deterministic_matches(df_with_rules, rules_config)

# ── Step 10: Explode rule_result so each matched candidate gets its own row
df_exploded = df_final.withColumn("rule", F.explode("deterministic_match_result.rule_result"))

# ── Step 11: Build exploded_result struct (per matched candidate).
# Score uses production thresholds so the downstream classifier sees rule-determined
# entries in the right band (MATCH → merge_max, NO_MATCH → not_match_max).
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
        F.when(
            F.col("deterministic_match_result.action_on_match") == "MATCH",
            F.lit(merge_max),
        ).when(
            F.col("deterministic_match_result.action_on_match") == "NO_MATCH",
            F.lit(not_match_max),
        ).otherwise(F.lit(not_match_max)).cast("double").alias("score"),
        F.concat(
            F.lit("Due to Match Rule: "),
            F.col("deterministic_match_result.rule_name"),
        ).alias("reason"),
        F.col("rule.lakefusion_id").alias("lakefusion_id"),
    ),
)

# COMMAND ----------

# ── Step 12: MATCH suppression — if any MATCH-action row exists for a source,
# drop the NO_MATCH rows for that source. NO_MATCH-only sources keep all their rows.
MATCH_ACTIONS = ["MATCH"]
key_col = "surrogate_key"

has_match_window = Window.partitionBy(key_col)
df_final = df_final.withColumn(
    "_key_has_match",
    F.max(
        F.when(F.col("exploded_result.match").isin(MATCH_ACTIONS), F.lit(1)).otherwise(F.lit(0))
    ).over(has_match_window),
)
df_final = df_final.filter(
    (F.col("_key_has_match") == 0)
    | (F.col("exploded_result.match").isin(MATCH_ACTIONS))
).drop("_key_has_match")

# COMMAND ----------

# ── Step 13: Keep only deterministic-match rows + dedup per (source × candidate).
# Window dedup: per source, MATCH rows collapse to a single row (partition key uses
# NULL for MATCH so all MATCH rows for one source share a partition); NO_MATCH rows
# stay one-per-candidate (partition key is the candidate id).
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

window_spec = Window.partitionBy(
    "surrogate_key",
    F.when(F.col("exploded_result.match").isin(MATCH_ACTIONS), F.lit(None))
     .otherwise(F.col("lakefusion_id")),
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

# ── Write to unified_deterministic. Each row = one (source × matched candidate).
unified_deteministic_table_exists = spark.catalog.tableExists(unified_deteministic_table)
values = {col: f"source.{col}" for col in df_final.columns}

# Composite merge key: source row id + candidate id, so each (source × candidate)
# pair is its own row in the target.
merge_condition = (
    "target.surrogate_key = source.surrogate_key "
    "AND target.lakefusion_id = source.lakefusion_id"
)
zorder_cols = "surrogate_key, lakefusion_id"

if df_final.head(1) is None:
    logger.info("No deterministic results to write")
elif not unified_deteministic_table_exists:
    df_final.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(unified_deteministic_table)
else:
    # Schema autoMerge so newly added columns land on pre-existing tables.
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

match_count = df_final.filter(F.col("exploded_result.match").isin(MATCH_ACTIONS)).count()
no_match_count = df_final.filter(~F.col("exploded_result.match").isin(MATCH_ACTIONS)).count()
logger.info(f"Deterministic results: {match_count} MATCH, {no_match_count} NO_MATCH (per matched candidate)")
