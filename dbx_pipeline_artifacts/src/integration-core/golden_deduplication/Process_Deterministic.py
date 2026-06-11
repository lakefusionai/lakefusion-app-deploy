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
dbutils.widgets.text("reference_attribute_config", "{}", "REFERENCE_ENTITY -> {ref_table, output_attr} map")

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
reference_attribute_config = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="reference_attribute_config",
    debugValue=dbutils.widgets.get("reference_attribute_config"),
)
reference_attribute_config = (
    json.loads(reference_attribute_config)
    if isinstance(reference_attribute_config, str)
    else (reference_attribute_config or {})
)

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

entity_attributes_datatype = json.loads(entity_attributes_datatype)
rules_config=json.loads(rules_config)
attributes = json.loads(attributes)
id_key="lakefusion_id"


# COMMAND ----------

unified_table = f"{catalog_name}.silver.{entity}_unified_deduplicate"
unified_deteministic_table = f"{catalog_name}.silver.{entity}_unified_deterministic_deduplicate"
master_table = f"{catalog_name}.gold.{entity}_master"
processed_unified_table = f"{catalog_name}.silver.{entity}_processed_unified_deduplicate"
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

if len(rules_config)==0:
  dbutils.notebook.exit("No deterministic rules found")
else:
  logger.info("Proceed with Deterministic Rules")

# COMMAND ----------

config_thresholds = json.loads(config_thresholds)
merge_thresholds = config_thresholds.get('merge', [0.9, 1.0])
matches_thresholds = config_thresholds.get('matches', [0.7, 0.89])
not_match_thresholds = config_thresholds.get('not_match', [0.0, 0.69])

merge_min, merge_max = merge_thresholds[0], merge_thresholds[1]
matches_min, matches_max = matches_thresholds[0], matches_thresholds[1]
not_match_min, not_match_max = not_match_thresholds[0], not_match_thresholds[1]

# COMMAND ----------

df_unified = spark.read.table(unified_table).filter(F.col("search_results")!='').filter(F.col("scoring_results")=='')
df_master=spark.read.table(master_table)

# COMMAND ----------
%pip install rapidfuzz

# COMMAND ----------

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

# ── REFERENCE_ENTITY display resolution ──────────────────────────────────────
# Both df_unified and df_master store REFERENCE_ENTITY columns as the raw
# ref_lakefusion_id (UUID). For deterministic rule comparison (especially
# fuzzy: levenshtein / jaro_winkler / soundex), comparing UUIDs is meaningless.
# We LEFT-JOIN the ref view per REFERENCE_ENTITY attribute and overwrite the
# column in place with the human-readable output value, so all downstream
# rule expressions naturally operate on display values.
def _resolve_reference_columns(df):
    for attr, cfg in (reference_attribute_config or {}).items():
        if attr not in df.columns:
            continue
        ref_table   = cfg.get("ref_table")
        output_attr = cfg.get("output_attr")
        if not ref_table or not output_attr:
            continue
        ref_view = (
            spark.read.table(ref_table)
            .select(
                F.col("ref_lakefusion_id").alias(f"_ref_{attr}_id"),
                F.col(output_attr).alias(f"_ref_{attr}_disp"),
            )
        )
        df = (
            df.join(
                F.broadcast(ref_view),
                df[attr] == F.col(f"_ref_{attr}_id"),
                "left",
            )
            .withColumn(attr, F.coalesce(F.col(f"_ref_{attr}_disp"), F.col(attr)))
            .drop(f"_ref_{attr}_id", f"_ref_{attr}_disp")
        )
    return df


df_unified = _resolve_reference_columns(df_unified)
df_master  = _resolve_reference_columns(df_master)

# COMMAND ----------


from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DoubleType, BooleanType, StructType, ArrayType
from rapidfuzz.distance import JaroWinkler, Levenshtein
from functools import reduce as ft_reduce
import operator as _operator

def _jw_similarity(s1, s2):
    if s1 is None or s2 is None:
        return None
    return float(JaroWinkler.similarity(s1, s2))

jaro_winkler_udf = F.udf(_jw_similarity, DoubleType())
spark.udf.register("jaro_winkler_similarity", _jw_similarity, DoubleType())

# COMMAND ----------

# Make utils importable for build_attributes_combined_column (same pattern as
# Process_Unmatched_Records / Promote_Pending_RDM / Load_Primary_Source).
import os as _pp_os, sys as _pp_sys
_pp_parts = _pp_os.getcwd().split(_pp_os.sep)
for _i in range(len(_pp_parts) - 1, -1, -1):
    if _pp_parts[_i] == "src":
        _src_path = _pp_os.sep.join(_pp_parts[: _i + 1])
        if _src_path not in _pp_sys.path:
            _pp_sys.path.insert(0, _src_path)
        break

# COMMAND ----------

# MAGIC %run ../../utils/attributes_combined

# COMMAND ----------


# ── 2. Dynamic struct builder ──────────────────────────────────────────────────

from pyspark.sql import functions as F
# Spark scalar cast targets. Anything else in entity_attributes_datatype (e.g.
# the LakeFusion attribute type "REFERENCE_ENTITY") is NOT a Spark type and must
# not be passed to cast() — it's kept as a string.
_SPARK_SCALAR_TYPES = {
    "STRING", "VARCHAR", "CHAR", "TEXT",
    "INT", "INTEGER", "BIGINT", "LONG", "SMALLINT", "SHORT", "TINYINT", "BYTE",
    "DOUBLE", "FLOAT", "REAL", "BOOLEAN", "BINARY",
    "DATE", "TIMESTAMP", "TIMESTAMP_NTZ",
}


def build_dynamic_struct(x_col, attributes, entity_attributes_datatype, complex_attrs=None):
    """`attributes` = body columns only (id is a separate trailing field).

    `complex_attrs` = set of STRUCT/ARRAY attribute names. Those are carried as
    JSON strings (encoded upstream via to_json), so we keep the raw string and
    skip the scalar dtype cast — comparison re-parses the JSON when needed."""
    complex_attrs = complex_attrs or set()
    struct_fields = []
    parts = F.split(x_col, "\\|")
    for idx, attr in enumerate(attributes):
        raw_value = F.trim(F.element_at(parts, idx + 1))
        raw_value = F.when(
            raw_value.isNull() | (raw_value == "null") | (raw_value == ""),
            F.lit(None).cast("string"),
        ).otherwise(raw_value)
        dtype = entity_attributes_datatype.get(attr)
        # Only cast to a real Spark SCALAR type. Skip complex declared types
        # ("STRUCT"/"ARRAY"/"MAP", carried as JSON strings) and LakeFusion
        # attribute types that aren't Spark types (e.g. "REFERENCE_ENTITY",
        # which raises UNSUPPORTED_DATATYPE) — keep those as the trimmed string.
        _dt_norm = (dtype or "").strip().upper()
        _is_complex_dtype = _dt_norm.startswith(("STRUCT", "ARRAY", "MAP"))
        _is_scalar_dtype = (_dt_norm in _SPARK_SCALAR_TYPES) or _dt_norm.startswith("DECIMAL")
        if dtype and attr not in complex_attrs and not _is_complex_dtype and _is_scalar_dtype:
            raw_value = raw_value.cast(dtype)
        struct_fields.append(raw_value.alias(f"{attr}_matches"))

    # id is the field right after the body columns
    lakefusion_id = F.trim(F.element_at(parts, len(attributes) + 1))
    lakefusion_id = F.when(
        lakefusion_id.isNull() | (lakefusion_id == ""),
        F.lit(None).cast("string"),
    ).otherwise(lakefusion_id)
    struct_fields.append(lakefusion_id.alias("lakefusion_id"))
    return F.struct(*struct_fields)


def build_candidate_struct(cand, scalar_cols, complex_cols, entity_attributes_datatype, complex_attrs):
    """Build a candidate struct from a collected element `cand`:
        struct(_combined: scalar pipe-string, <native complex cols...>)
    Scalar fields are parsed from the pipe-string (via build_dynamic_struct);
    STRUCT/ARRAY fields are kept in their NATIVE Spark type. Output fields:
    `<attr>_matches` for every attr + `lakefusion_id`."""
    base = build_dynamic_struct(
        cand.getField("_combined"), scalar_cols, entity_attributes_datatype, complex_attrs
    )
    fields = [base.getField(f"{a}_matches").alias(f"{a}_matches") for a in scalar_cols]
    fields += [cand.getField(a).alias(f"{a}_matches") for a in complex_cols]
    # carry the master's pre-built attributes_combined for use as the display id
    fields.append(cand.getField("_cand_attributes_combined").alias("_cand_attributes_combined"))
    fields.append(base.getField("lakefusion_id").alias("lakefusion_id"))
    return F.struct(*fields)


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

_PYOPS = {">=": _operator.ge, "<=": _operator.le, ">": _operator.gt,
          "<": _operator.lt, "=": _operator.eq, "==": _operator.eq, "!=": _operator.ne}


def _scalar_condition(src_col, cand_col, match_type, fuzzy_func, threshold,
                      operator, allow_nulls) -> F.Column:
    """Scalar comparison of two columns (cast to string). This is the original
    build_condition_column body, factored out unchanged so scalar attributes,
    STRUCT sub-fields, and serialized whole-struct comparison all share it."""
    src_raw  = src_col.cast("string")
    cand_raw = cand_col.cast("string")
    src  = F.when(F.trim(src_raw)  == "", F.lit(None).cast("string")).otherwise(src_raw)
    cand = F.when(F.trim(cand_raw) == "", F.lit(None).cast("string")).otherwise(cand_raw)

    left  = F.lower(src)
    right = F.lower(cand)

    both_null = src.isNull() & cand.isNull()
    both_set  = src.isNotNull() & cand.isNotNull()
    one_null  = (src.isNull() & ~cand.isNull()) | (~src.isNull() & cand.isNull())  # exactly one null

    is_negation = operator in ("!=", "<>")

    if match_type == "exact":
        if is_negation:
            # explicit, null-safe negation:
            #   both null      -> NOT different (False)
            #   one null       -> different (True)   [present vs absent]
            #   both set       -> left != right
            return (
                F.when(both_null, F.lit(False))
                 .when(one_null,  F.lit(True))
                 .otherwise(left != right)
            )
        else:
            base = (left == right)
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
            base = _apply_operator(jaro_winkler_udf(left, right), operator, threshold)
        elif fuzzy_func == "soundex":
            base = F.soundex(src) == F.soundex(cand)
        else:
            raise ValueError(f"Unsupported fuzzy function: {fuzzy_func}")
    else:
        raise ValueError(f"Unsupported match_type: {match_type}")

    # positive (non-negation) matching
    null_match = both_null
    if allow_nulls:
        return F.when(both_set, base).when(null_match, F.lit(True)).otherwise(F.lit(False))
    else:
        return F.when(both_set, base).otherwise(F.lit(False))


def _array_match_udf(match_type, fuzzy_func, threshold, operator):
    """UDF(arr_src, arr_cand) -> bool: any element of src matches any of cand.
    Runs on two flat string-array columns — Python UDFs (jaro_winkler etc.)
    cannot be called inside Spark higher-order-function lambdas, so the pairwise
    scan happens here instead."""
    def _m(a, b):
        if not a or not b:
            return False
        for x in a:
            if x is None:
                continue
            xs = str(x).lower()
            for y in b:
                if y is None:
                    continue
                ys = str(y).lower()
                if match_type == "exact":
                    ok = (xs != ys) if operator in ("!=", "<>") else (xs == ys)
                    if ok:
                        return True
                    continue
                if fuzzy_func == "jaro_winkler":
                    score = JaroWinkler.similarity(xs, ys)
                elif fuzzy_func == "levenshtein_normalized":
                    ml = max(len(xs), len(ys)) or 1
                    score = 1.0 - (Levenshtein.distance(xs, ys) / ml)
                elif fuzzy_func == "levenshtein_standard":
                    score = Levenshtein.distance(xs, ys)
                elif fuzzy_func == "soundex":
                    return False  # soundex not supported on ARRAY attributes
                else:
                    raise ValueError(f"Unsupported fuzzy function: {fuzzy_func}")
                if _PYOPS[operator](score, threshold):
                    return True
        return False
    return F.udf(_m, BooleanType())


def _array_any_match(src_vals, cand_vals, match_type, fuzzy_func, threshold,
                     operator, allow_nulls) -> F.Column:
    """Element-wise 'any element matches' over two string-array columns."""
    src_empty  = src_vals.isNull()  | (F.size(src_vals)  == 0)
    cand_empty = cand_vals.isNull() | (F.size(cand_vals) == 0)
    base = F.coalesce(
        _array_match_udf(match_type, fuzzy_func, threshold, operator)(src_vals, cand_vals),
        F.lit(False),
    )
    if allow_nulls:
        return (src_empty & cand_empty) | ((~src_empty) & (~cand_empty) & base)
    return (~src_empty) & (~cand_empty) & base


# ARRAY set-membership functions (match_type == "fuzzy", no operator/threshold).
# Semantics use source vs candidate as SETS of element values:
#   intersect -> share ≥1 common element
#   disjoint  -> share NO common element
#   subset    -> source ⊆ candidate (every source element is in candidate)
#   superset  -> source ⊇ candidate (every candidate element is in source)
_ARRAY_SET_FUNCS = {"intersect", "disjoint", "subset", "superset"}


def _array_set_match(src_vals, cand_vals, func, allow_nulls) -> F.Column:
    """Set-based comparison over two string-array columns."""
    _EMPTY     = F.array().cast("array<string>")
    src_empty  = src_vals.isNull()  | (F.size(src_vals)  == 0)
    cand_empty = cand_vals.isNull() | (F.size(cand_vals) == 0)
    # coalesce to empty arrays so array_intersect/array_except never see NULL
    s = F.coalesce(src_vals,  _EMPTY)
    c = F.coalesce(cand_vals, _EMPTY)

    if func == "intersect":
        base = F.size(F.array_intersect(s, c)) > 0
    elif func == "disjoint":
        base = F.size(F.array_intersect(s, c)) == 0
    elif func == "subset":      # source ⊆ candidate
        base = F.size(F.array_except(s, c)) == 0
    elif func == "superset":    # source ⊇ candidate
        base = F.size(F.array_except(c, s)) == 0
    else:
        raise ValueError(f"Unsupported array set function: {func}")

    if allow_nulls:
        return base
    # nulls not allowed → both sides must be present (non-empty)
    return (~src_empty) & (~cand_empty) & base


def build_condition_column(cond: dict, rule_allow_nulls: bool, complex_types: dict) -> F.Column:
    """Boolean Column: does this condition pass for the current (source × candidate) row?

    Type-aware via `complex_types` (attr -> native Spark DataType for STRUCT/ARRAY
    attributes). Such columns are kept NATIVE, so:
      - STRUCT + sub_field    -> col.getField(<sub_field>) on both sides
      - STRUCT  (no sub_field)-> compare to_json(struct) on both sides
      - ARRAY / ARRAY<STRUCT> -> transform native array -> element-wise 'any element matches'
      - scalar                -> unchanged scalar comparison
      - (declared-complex but stored as a string, no native type) -> JSON fallback
        via get_json_object when a sub_field is given
    """
    attr        = cond["attribute"]
    sub_field   = cond.get("sub_field")
    match_type  = cond.get("match_type", "exact")
    fuzzy_func  = cond.get("function")
    threshold   = cond.get("threshold")
    operator    = cond.get("operator", ">=")
    allow_nulls = cond.get("allow_nulls", rule_allow_nulls)
    case_insensitive = cond.get("case_insensitive", False)

    # Normalize sub_field: treat missing / "" / literal "None" as no sub_field.
    if sub_field in (None, "", "None", "null"):
        sub_field = None

    src_col  = F.col(attr)
    cand_col = F.col(f"{attr}_matches")
    dt       = (complex_types or {}).get(attr)

    # ── Scalar attribute (unchanged behaviour) ──
    if dt is None:
        # Fallback: a declared-complex attr stored as a JSON string (no native
        # Spark type captured). With a sub_field we can still extract it.
        if sub_field:
            s = F.get_json_object(src_col,  f"$.{sub_field}")
            c = F.get_json_object(cand_col, f"$.{sub_field}")
            return _scalar_condition(s, c, match_type, fuzzy_func, threshold, operator, allow_nulls)
        return _scalar_condition(src_col, cand_col, match_type, fuzzy_func,
                                 threshold, operator, allow_nulls)

    is_struct   = isinstance(dt, StructType)
    is_array    = isinstance(dt, ArrayType)
    elem_struct = is_array and isinstance(dt.elementType, StructType)

    # ── STRUCT (native) ──
    if is_struct:
        if sub_field:
            s = src_col.getField(sub_field)
            c = cand_col.getField(sub_field)
        else:
            # whole struct → compare canonical JSON serialization (to_json on a
            # native struct is valid and gives a stable, order-preserving string)
            s, c = F.to_json(src_col), F.to_json(cand_col)
        return _scalar_condition(s, c, match_type, fuzzy_func, threshold, operator, allow_nulls)

    # ── ARRAY / ARRAY<STRUCT> (native) → element-wise 'any element matches' ──
    src_arr, cand_arr = src_col, cand_col   # already native arrays
    if elem_struct and sub_field:
        src_vals  = F.transform(src_arr,  lambda e: e.getField(sub_field).cast("string"))
        cand_vals = F.transform(cand_arr, lambda e: e.getField(sub_field).cast("string"))
    elif elem_struct:
        src_vals  = F.transform(src_arr,  lambda e: F.to_json(e))
        cand_vals = F.transform(cand_arr, lambda e: F.to_json(e))
    else:
        src_vals  = F.transform(src_arr,  lambda e: e.cast("string"))
        cand_vals = F.transform(cand_arr, lambda e: e.cast("string"))

    # Set-membership functions (intersect / disjoint / subset / superset) compare
    # the two arrays as SETS; element-wise fuzzy/exact otherwise.
    if fuzzy_func in _ARRAY_SET_FUNCS:
        if case_insensitive:
            src_vals  = F.transform(src_vals,  lambda e: F.lower(e))
            cand_vals = F.transform(cand_vals, lambda e: F.lower(e))
        return _array_set_match(src_vals, cand_vals, fuzzy_func, allow_nulls)

    return _array_any_match(src_vals, cand_vals, match_type, fuzzy_func,
                            threshold, operator, allow_nulls)



def apply_rules(df_parsed: DataFrame, rules_config: list, complex_types: dict = None) -> DataFrame:
    """Evaluate each rule per (source row × candidate), produce one
    `<rule>_results` array column per rule containing matched candidate structs."""
    complex_types = complex_types or {}
    non_array_cols = [c for c in df_parsed.columns if c != "search_result_parsed"]

    df_exp = df_parsed.withColumn("_candidate", F.explode("search_result_parsed"))

    candidate_fields = [
        f.name for f in
        df_parsed.schema["search_result_parsed"].dataType.elementType.fields
    ]
    for field in candidate_fields:
        df_exp = df_exp.withColumn(field, F.col(f"_candidate.{field}"))

    for rule in rules_config:
        rule_name        = rule["name"]
        logical_op       = rule.get("logical_operator", "AND").upper()
        rule_allow_nulls = rule.get("allow_nulls", False)

        cond_flags = [build_condition_column(c, rule_allow_nulls, complex_types) for c in rule["conditions"]]
        rule_flag  = cond_flags[0]
        for flag in cond_flags[1:]:
            rule_flag = (rule_flag & flag) if logical_op == "AND" else (rule_flag | flag)

        df_exp = df_exp.withColumn(
            f"_match_{rule_name}",
            F.coalesce(rule_flag, F.lit(False)),
        )

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
    match_rules = [r for r in rules_config]
    match_cols  = [f"{r['name']}_results" for r in match_rules]

    is_match_expr = F.lit(False)
    for c in match_cols:
        is_match_expr = is_match_expr | (F.size(F.col(c)) > 0)
    df_out = df_with_rules.withColumn("is_deterministic_match", is_match_expr)

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
                    F.lit(action).alias("action_on_match")
                )
            )
        )

    df_out = df_out.withColumn(
        "deterministic_match_result",
        F.coalesce(*when_exprs) if when_exprs else F.lit(None)
    )
    return df_out


# ─────────────────────────────────────────────────────────────
# attribute_combined helpers
#   _noprefix  → single-DataFrame context (no table alias yet)
#   _prefixed  → after a join, where the alias disambiguates columns
# ─────────────────────────────────────────────────────────────
def _safe(col):
    # neutralize the delimiter so a literal "|" in a value can't shift positions
    return F.regexp_replace(F.coalesce(col.cast("string"), F.lit("")), r"\|", "/")


def build_attr_combined_noprefix(cols: list, id_col: str) -> F.Column:
    """cols must already exclude id_col and helpers, in the SAME order used to parse."""
    fields = [_safe(F.col(c)) for c in cols]
    return F.concat_ws("|", *fields, _safe(F.col(id_col)))


def build_attr_combined_prefixed(alias_prefix: str, cols: list, id_col: str) -> F.Column:
    """cols must be the SAME list (same order) as the noprefix side."""
    fields = [_safe(F.col(f"{alias_prefix}.{c}")) for c in cols]
    return F.concat_ws("|", *fields, _safe(F.col(f"{alias_prefix}.{id_col}")))

# ─────────────────────────────────────────────────────────────
# Pipeline
# ─────────────────────────────────────────────────────────────

# 0. Complex (STRUCT / ARRAY / ARRAY<STRUCT>) attributes:
#    snapshot their native Spark types in `complex_types`. They are kept NATIVE
#    (no to_json): excluded from the scalar pipe-string, carried natively into
#    the candidate struct, and compared with native ops (getField / transform)
#    in build_condition_column.
def _native_complex_dt(df, attr):
    """Return the Spark DataType if `attr` is a native STRUCT/ARRAY column in df, else None."""
    if attr in df.columns:
        _dt = df.schema[attr].dataType
        if isinstance(_dt, (StructType, ArrayType)):
            return _dt
    return None

complex_types = {}   # attr -> native Spark DataType (StructType / ArrayType), when the column is native
complex_attrs = set()  # attrs treated as complex (native OR declared STRUCT/ARRAY) — never scalar-cast
for _attr in entity_attributes_datatype.keys():
    _declared = (entity_attributes_datatype.get(_attr) or "").strip().upper()
    _declared_complex = _declared.startswith(("STRUCT", "ARRAY"))
    # Prefer a native type from either side (they should match; tolerate divergence).
    _native_dt = _native_complex_dt(df_master, _attr) or _native_complex_dt(df_unified, _attr)
    if _native_dt is not None:
        complex_types[_attr] = _native_dt
        complex_attrs.add(_attr)
    elif _declared_complex:
        # Declared complex but stored as a string (e.g. pre-serialized JSON):
        # still skip the scalar cast; STRUCT sub-fields resolve via get_json_object.
        complex_attrs.add(_attr)

# Complex columns are kept in their NATIVE Spark types (STRUCT / ARRAY). They are
# excluded from the scalar pipe-string round-trip, carried natively into the
# candidate struct, and compared with native ops (getField / transform). No to_json.

# 1. Parse scoring result → candidate lakefusion_ids
df_cleaned = df_unified.withColumn(
    "search_results_array",
    F.split(F.regexp_replace(F.col("search_results"), r"^\[|\]$", ""), r"\],\s*\["),
)
df_cleaned = df_cleaned.withColumn(
    "search_results_master_lakefusion_ids",
    F.transform(
        F.col("search_results_array"),
        lambda x: F.regexp_extract(x, r"([a-f0-9]{32})", 1),
    ),
)

# 2. Explode to one row per candidate master record
df_exploded = df_cleaned.withColumn(
    "search_results_master_lakefusion_ids",
    F.explode("search_results_master_lakefusion_ids"),
)

# 3. Build attribute_combined for the SOURCE side from ALL data columns,
#    excluding the id and the scoring/helper scaffolding columns.

helper_cols = {
    id_key, "search_results", "search_results_array",
    "search_results_master_lakefusion_ids", "attributes_combined","search_results","scoring_results","attributes_combined_embedding","record_status","source_path"
    # add any other non-attribute columns here (embeddings, scores, etc.)
}
schema_order = list(entity_attributes_datatype.keys())

# Preserve schema order, drop helpers AND complex attrs (complex stay native,
# never go through the scalar pipe-string), keep only cols actually present
present = set(df_exploded.columns)
source_cols = [c for c in schema_order if c not in helper_cols and c not in complex_attrs and c in present]

df_exploded = df_exploded.withColumn(
    "attributes_combined_source",
    build_attr_combined_noprefix(source_cols, id_key),
).alias("exp")   # alias AFTER the column exists

# # # 4. Join to master
df_master = df_master.alias("mst")
df_joined = df_exploded.join(
    df_master,
    F.col("exp.search_results_master_lakefusion_ids") == F.col("mst.lakefusion_id"),
    "inner",
)

# # 5. Build attribute_combined for the MASTER side from ALL its data columns,
# #    excluding lakefusion_id and any non-attribute master columns.
MASTER_ID = "lakefusion_id"

master_exclude = {
    MASTER_ID, "attributes_combined", "attributes_combined_master",
    "attributes_combined_embedding"
}

# Reuse the same schema_order derived earlier. Scalars go through the pipe-string;
# complex master cols are carried natively into the candidate struct.
present_master = set(df_master.columns)
master_cols = [c for c in schema_order if c not in master_exclude and c not in complex_attrs and c in present_master]
complex_master_cols = [c for c in schema_order if c in complex_attrs and c not in master_exclude and c in present_master]

# Per-candidate struct: scalar pipe-string (_combined) + the master's pre-built
# attributes_combined (for the display id) + NATIVE complex columns.
master_candidate = F.struct(
    build_attr_combined_prefixed("mst", master_cols, MASTER_ID).alias("_combined"),
    F.col("mst.attributes_combined").alias("_cand_attributes_combined"),
    *[F.col(f"mst.{c}").alias(c) for c in complex_master_cols],
)

# # # 6. Group back: one row per source record + list of master candidates.
#     collect_list (NOT collect_set) — structs containing ARRAY fields aren't hashable.
group_cols = [f"exp.{c}" for c in df_exploded.columns]
df_result = (
    df_joined
    .groupBy(*group_cols)
    .agg(F.collect_list(master_candidate).alias("_master_candidates"))
    .select("exp.*", "_master_candidates", "search_results_master_lakefusion_ids")
)

# # # 7. Parse BOTH sides into structs. Scalars round-trip through the pipe-string;
#     complex stay native. Source complex cols (excluded from source_cols) remain
#     native columns already present from exp.*.
df_parsed = df_result.withColumn(
    "search_result_parsed",
    F.transform(
        F.col("_master_candidates"),
        lambda c: build_candidate_struct(c, master_cols, complex_master_cols, entity_attributes_datatype, complex_attrs),
    ),
).withColumn(
    "source_parsed",
    build_dynamic_struct(
        F.col("attributes_combined_source"), source_cols, entity_attributes_datatype, complex_attrs
    ),
)

# # Flatten source scalar struct → plain <col> columns (complex source cols stay native)
for col in source_cols:
    df_parsed = df_parsed.withColumn(col, F.col(f"source_parsed.{col}_matches"))

# # 8. Apply deterministic rules
df_with_rules = apply_rules(df_parsed, rules_config, complex_types)

# 9. Compute first-match deterministic result
df_final = compute_deterministic_matches(df_with_rules, rules_config)

# 10. Explode matched rule results (one row per matched candidate)
df_exploded = df_final.withColumn(
    "rule", F.explode("deterministic_match_result.rule_result")
)

# # 11. Build exploded_result struct
# `id` = the matched candidate's attributes_combined, which was built at load time
# by build_attributes_combined_column (utils/attributes_combined). Reusing it means
# the id is byte-identical to what the LLM / other steps display — STRUCT flattened
# ("5 Elm Ave Quincy MA 02169"), etc. — and it works regardless of whether complex
# columns are stored natively or as JSON strings (no re-flattening here).
concat_col = F.col("rule._cand_attributes_combined")

df_final = df_exploded.withColumn(
    "exploded_result",
    F.struct(
        concat_col.alias("id"),
        F.col("deterministic_match_result.action_on_match").alias("match"),
        F.lit(1.0).cast("double").alias("score"),
        F.concat(
            F.lit("Due to Match Rule: "),
            F.col("deterministic_match_result.rule_name"),
        ).alias("reason"),
        F.col("rule.lakefusion_id").alias("lakefusion_id"),
    )
)

# COMMAND ----------

# ── Step 12: MATCH suppression — if any MATCH-action row exists for a source,
# drop the NO_MATCH rows for that source. NO_MATCH-only sources keep all their rows.
MATCH_ACTIONS = ["MATCH"]
key_col = "lakefusion_id"   # single-source: the source's own lakefusion_id

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
# Single-source: candidate id lives in exploded_result.lakefusion_id; the bare
# `lakefusion_id` column on the row is the SOURCE id.
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
    F.when(F.col("exploded_result.match").isin(MATCH_ACTIONS), F.lit(None))
     .otherwise(F.col("exploded_result.lakefusion_id")),
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

# Composite merge key: source row id + candidate id (nested under exploded_result).
# Delta can't ZORDER on nested struct fields, so ZORDER on source id only.
merge_condition = (
    "target.lakefusion_id = source.lakefusion_id "
    "AND target.exploded_result.lakefusion_id = source.exploded_result.lakefusion_id"
)
zorder_cols = "lakefusion_id"

if df_final.head(1) is None:
    logger.info("No deterministic results to write")
elif not unified_deteministic_table_exists:
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

match_count = df_final.filter(F.col("exploded_result.match").isin(MATCH_ACTIONS)).count()
no_match_count = df_final.filter(~F.col("exploded_result.match").isin(MATCH_ACTIONS)).count()
logger.info(f"Deterministic results: {match_count} MATCH, {no_match_count} NO_MATCH (per matched candidate)")
