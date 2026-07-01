# Databricks notebook source
# ======================================================================
# llm_scoring_common — shared LLM-scoring helpers for the SHARDED entity-matching flow
# ======================================================================
# %run-included by the shard worker (Entity_Matching_LLM_Shard_*) and the assembler
# (Entity_Matching_LLM_Assemble_*). Designed to be reused by normal/golden dedup
# sharding too. Pure function/constant defs ONLY — no top-level side effects, no
# notebook globals; every function takes what it needs as a parameter.
#
# This centralizes the prompt + ai_query construction, model-parameter validation,
# and the bad-result validation/retry helpers so the sharded notebooks don't each
# carry their own copy. (The single-shard Entity_Matching_LLM_Experiment notebook is
# kept as the release/4.3.0 inline version and does NOT use this module.)
# ======================================================================

from pyspark.sql.functions import (
    col, lit, coalesce, from_json, get_json_object, transform,
    array_except, greatest, when, concat,
)
from pyspark.sql.functions import size as spark_size

# COMMAND ----------

# ── Prompt construction ─────────────────────────────────────────────────────────

def safe_format(template, **kwargs):
    """str.format that leaves unknown {placeholders} untouched."""
    class SafeDict(dict):
        def __missing__(self, key):
            return '{' + key + '}'
    return template.format_map(SafeDict(**kwargs))


def build_safe_prompt(entity, attributes, additional_instructions, base_prompt, max_potential_matches):
    """Return the single-quote-escaped system prompt (default template or custom base_prompt)."""
    attribute_order = ' | '.join(attributes)
    additional_instructions_text = additional_instructions.strip() if additional_instructions else ""

    if base_prompt is None or base_prompt == '':
        default_prompt = f"""You are an entity matching scorer. Compare an incoming unified record against potential golden matches and score each 0.0-1.0.

## Important Context
- Entity type: {entity}
- Attributes may be incomplete, inconsistent, or contain typos.
- **Empty fields are neutral.** Only conflicting filled values are negative evidence.
- **lakefusion_id is an opaque ID; vector_score is retrieval similarity.** Do not use either for scoring. Copy lakefusion_id to output unchanged.
- Return exactly one scored result per potential match provided in the input.

## Attribute Order
Fields are pipe-delimited in this order:
{attribute_order}

Empty fields between pipes = no data.
Note: Some columns can have aggregated values separated by (bullet)

## Input Format

**Query Record:** A pipe-delimited string in the attribute order above.

**Potential Matches:** A JSON array of 3-element arrays, one per candidate:
[[pipe_delimited_attributes, lakefusion_id, vector_score], ...]

- Index 0: pipe-delimited attribute values (same order as above)
- Index 1: lakefusion_id
- Index 2: vector_score

## Conflict vs. Near-match
Classify carefully — this distinction directly affects scoring:
- **Near-match** (typos/entry errors): character transpositions (Jonh->John), misspellings (SMth->Smith), truncations (10 Main St->101 Main St), date component swaps (1990-05-09->1990-09-05), minor formatting differences (St.->Street), missing accents or special characters (Mueller->Muller), language variations of the same word.
- **Conflict** (genuinely different values): completely different names (Mark->John), different cities (Dallas->Los Angeles), different product models (Sentry->C45), different dates with no transposition pattern (1990-05-09->1993-06-18).

## Additional Instructions
{additional_instructions_text if additional_instructions_text else "(none)"}

## Output Format — STRICT
Return ONLY a plain JSON array. No markdown, no code blocks, no extra text.

The array MUST contain exactly one object per potential match in the input. If the input contains N matches, return N scored results. Each object MUST have exactly these keys:
- id: String (the match_record text from index 0 of the inner array)
- score: Float (YOUR similarity score 0.0-1.0, NOT the vector score)
- reason: String (see Reason Format below)
- lakefusion_id: String (copy character-for-character from index 1 of the input array; do not modify, validate, or generate)

Sort by score descending.

## Reason Format
Format: "Key signals: [2-3 most important field classifications]. Justification: [1-2 sentences explaining the score]."

Each field classification uses this taxonomy: exact_match | near_match | conflict | one_empty | both_empty. For conflicts and near-matches, include the actual values in parentheses.

Example:
"Key signals: companyName exact_match, addressLineOne near_match (123 Main vs 123 Main St), supplierEmail exact_match. Justification: Strong identity agreement across name and email with minor address formatting difference. Scored 0.95 as MATCH."
"""
        return default_prompt.replace("'", "\\'")

    base_prompt = base_prompt.replace("{query_entity}", "").replace("{merged_desc_column}", "").replace("{search_results}", "")
    formatted_prompt = safe_format(
        base_prompt,
        entity=entity,
        additional_instructions=additional_instructions,
        attributes=attribute_order,
        max_potential_matches=max_potential_matches,
    )
    return formatted_prompt.replace("'", "\\'")


def build_prompt_concat_parts(safe_prompt, merged_desc_column):
    """SQL CONCAT(...) expression that feeds the system prompt + query record + candidates to ai_query."""
    return (
        f"CONCAT('{safe_prompt}', "
        f"'\\nQuery record: ', {merged_desc_column}, "
        f"'\\nList of potential matches with vector search scores (JSON Array): ', search_results)"
    )


# Static JSON response-format clause for ai_query (strict json_schema). Plain string
# (single braces) — NOT an f-string, so no escaping is needed.
LLM_RESPONSE_FORMAT_CLAUSE = """responseFormat => '{
          "type": "json_schema",
          "json_schema": {
            "name": "match_results",
            "schema": {
              "type": "object",
              "properties": {
                "results": {
                  "type": "array",
                  "items": {
                    "type": "object",
                    "properties": {
                      "id": { "type": "string" },
                      "score": { "type": "number" },
                      "reason": { "type": "string" },
                      "lakefusion_id": { "type": "string" }
                    },
                    "required": ["id", "score", "reason", "lakefusion_id"],
                    "additionalProperties": false
                  }
                }
              },
              "required": ["results"],
              "additionalProperties": false
            },
            "strict": true
          }
        }',"""


def build_ai_query_select(llm_endpoint, prompt_concat_parts, model_params_clause):
    """The `ai_query(...) AS scoring_results` SELECT expression reused across scoring + retry queries."""
    return f"""ai_query(
        '{llm_endpoint}',
        {prompt_concat_parts},
        {LLM_RESPONSE_FORMAT_CLAUSE}
        {model_params_clause}
        failOnError => false
      ) AS scoring_results"""

# COMMAND ----------

# ── Model-parameter validation (STEP 2.75) ───────────────────────────────────────

def validate_model_params(spark, llm_endpoint, llm_temperature, llm_max_tokens, reasoning_effort, logger=None):
    """Probe each candidate modelParameter against the endpoint; return the supported
    `modelParameters => named_struct(...),` clause (or '' if none supported)."""
    def _info(msg):
        if logger is not None:
            logger.info(msg)

    def _warn(msg):
        if logger is not None:
            logger.warning(msg)

    candidate_params = {}
    if llm_temperature is not None:
        candidate_params["temperature"] = llm_temperature
    if llm_max_tokens is not None:
        candidate_params["max_tokens"] = llm_max_tokens
    if reasoning_effort and reasoning_effort != 'disabled':
        candidate_params["reasoning_effort"] = reasoning_effort

    _info(f"Candidate model parameters to validate: {candidate_params}")

    validated_model_params = {}
    for param_name, param_value in candidate_params.items():
        try:
            if isinstance(param_value, str):
                param_clause = f"named_struct('{param_name}', '{param_value}')"
            else:
                param_clause = f"named_struct('{param_name}', {param_value})"
            test_query = f"""
            SELECT ai_query(
                '{llm_endpoint}',
                'Say OK',
                modelParameters => {param_clause},
                failOnError => true
            )
            """
            spark.sql(test_query).collect()
            validated_model_params[param_name] = param_value
            _info(f"  {param_name} = {param_value} -> SUPPORTED")
        except Exception as e:
            _warn(f"  {param_name} = {param_value} -> NOT SUPPORTED (removed from config): {str(e)[:200]}")

    if validated_model_params:
        param_parts = []
        for k, v in validated_model_params.items():
            if isinstance(v, str):
                param_parts.append(f"'{k}', '{v}'")
            else:
                param_parts.append(f"'{k}', {v}")
        clause = f"modelParameters => named_struct({', '.join(param_parts)}),"
    else:
        clause = ""
    _info(f"  Final modelParameters clause: {clause if clause else '(none - using defaults)'}")
    return clause

# COMMAND ----------

# ── Bad-result validation / retry helpers (STEP 6.5) ──────────────────────────────

def parsed_count_expr():
    """Number of result objects the LLM returned (0 if null/unparseable)."""
    return coalesce(
        spark_size(from_json(
            get_json_object(col("scoring_results.result"), "$.results"),
            "array<struct<id:string>>"
        )),
        lit(0),
    )


def expected_count_expr():
    """Number of candidates that were sent to the LLM (from search_results)."""
    return coalesce(
        spark_size(from_json(col("search_results"), "array<array<string>>")),
        lit(0),
    )


def add_unresolved_count(df):
    """Add unresolved_count: returned lakefusion_ids not present in the row's own
    search_results candidate list (hallucinations). Compared against search_results,
    not master_table — the true definition of hallucination."""
    if "unresolved_count" in df.columns:
        df = df.drop("unresolved_count")
    candidate_ids_expr = transform(
        from_json(col("search_results"), "array<array<string>>"),
        lambda arr: arr[1],
    )
    returned_ids_expr = transform(
        from_json(
            get_json_object(col("scoring_results.result"), "$.results"),
            "array<struct<lakefusion_id:string>>",
        ),
        lambda s: s["lakefusion_id"],
    )
    return df.withColumn(
        "unresolved_count",
        greatest(
            coalesce(spark_size(array_except(returned_ids_expr, candidate_ids_expr)), lit(0)),
            lit(0),
        ),
    )


def is_bad_llm_result(llm_retry_on_partial):
    """Column predicate marking rows that should be retried/nulled. Requires
    parsed_count/expected_count/unresolved_count columns to already be present."""
    bad = (
        col("scoring_results.result").isNull() |
        (col("parsed_count") == 0) |
        (col("unresolved_count") > 0)
    )
    if llm_retry_on_partial:
        bad = bad | (col("parsed_count") != col("expected_count"))
    return bad


def build_error_message_col():
    """Human-readable error_message Column for the unified error table."""
    return (
        when(col("scoring_results.result").isNull() & col("scoring_results.errorMessage").isNotNull(),
             concat(lit("LLM error: "), col("scoring_results.errorMessage"))
        ).when(col("parsed_count") == 0,
             concat(lit("LLM unparseable result: "), coalesce(col("scoring_results.result"), lit("NULL")))
        ).when(col("unresolved_count") > 0,
             concat(lit("LLM hallucinated lakefusion_id: "), col("unresolved_count").cast("string"),
                    lit(" of "), col("parsed_count").cast("string"),
                    lit(" returned ids not found in search_results. Raw: "),
                    coalesce(col("scoring_results.result"), lit("NULL")))
        ).otherwise(
             concat(lit("LLM incomplete result: expected "), col("expected_count").cast("string"),
                    lit(" results, got "), col("parsed_count").cast("string"),
                    lit(". Raw: "), coalesce(col("scoring_results.result"), lit("NULL")))
        )
    )


def build_retry_query_for_keys(temp_table, keys_table, source_id_key, merged_desc_column, ai_query_select):
    """Re-score the keys in keys_table by reading their attributes_combined + search_results
    from temp_table itself (self-contained — no source read)."""
    return f"""
    WITH uf AS (
      SELECT u.{source_id_key}, u.{merged_desc_column}, u.search_results
      FROM {temp_table} u
      INNER JOIN {keys_table} k ON u.{source_id_key} = k.{source_id_key}
    )
    SELECT
      {source_id_key},
      {merged_desc_column},
      search_results,
      {ai_query_select}
    FROM uf
    """


def build_partition_scoring_query(spark, source_view, temp_llm_table, source_id_key,
                                  merged_desc_column, ai_query_select, chunk_id=None, chunk_count=None):
    """Score one chunk of a shard's pre-filtered input partition (source_view). Adds the
    chunk hash-slice filter and a LEFT ANTI JOIN against temp_llm_table for crash-resume."""
    chunk_filter = ""
    if chunk_id is not None and chunk_count is not None:
        chunk_filter = f"AND abs(hash(u.{source_id_key})) % {chunk_count} = {chunk_id}"
    temp_anti_join = ""
    if spark.catalog.tableExists(temp_llm_table):
        temp_anti_join = f"LEFT ANTI JOIN {temp_llm_table} t ON t.{source_id_key} = u.{source_id_key}"
    return f"""
    WITH uf AS (
      SELECT u.{source_id_key}, u.{merged_desc_column}, u.search_results
      FROM {source_view} u
      {temp_anti_join}
      WHERE 1=1 {chunk_filter}
    )
    SELECT
      {source_id_key},
      {merged_desc_column},
      search_results,
      {ai_query_select}
    FROM uf
    """
