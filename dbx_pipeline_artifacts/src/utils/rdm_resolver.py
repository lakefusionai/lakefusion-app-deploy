# Databricks notebook source
# MAGIC %pip install rapidfuzz --quiet

# COMMAND ----------

"""
RDM Resolver — extracted from Process_RDM.py Steps 1-5.

Used by ingestion notebooks (Load_Primary_Source, Load_Secondary_Sources,
Load_Experiment_Data, Increment_Updates_To_Unified) to populate the mapping
table inline at ingestion time, so the unified table's REFERENCE_ENTITY
columns hold ref_lakefusion_id and attributes_combined holds canonical
display values on first write — no separate Process_RDM rewrite pass needed.

Public API:
    resolve_reference_attributes(spark, batch_df, rdm_configs, source_id) ->
        (approved_df, pending_df)

Approved rows:
    - REFERENCE_ENTITY columns replaced with ref_lakefusion_id from mapping
    - Per-attr `<attr>__display` column added (ref_value), for use in
      attributes_combined; caller should drop it before writing to unified.

Pending rows:
    - Rows where ANY REFERENCE_ENTITY attr resolved to PENDING / NO_MATCH /
      unresolved. Original column values preserved (raw source values).
    - Caller writes to a separate `{entity}_unified_reference_pending` table.

Status policy:
    AUTO_APPROVED, KEEP_RDM → row is approved for unified
    PENDING, NO_MATCH, missing → row goes to pending table
"""

from functools import reduce
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from rapidfuzz.distance import JaroWinkler


# Separator AggregationStrategy.concat uses to join multiple ref_ids in a
# single master column value. Must match
# lakefusion_core_engine.survivorship.strategies.aggregation_strategy.AggregationStrategy.DEFAULT_SEPARATOR.
AGG_CONCAT_SEPARATOR = " • "


# ── Similarity helpers ─────────────────────────────────────────────────────

def _jw_similarity(s1, s2):
    if s1 is None or s2 is None:
        return None
    return float(JaroWinkler.similarity(s1, s2))


_jaro_winkler_udf = F.udf(_jw_similarity, DoubleType())


def _levenshtein_normalized(s1, s2):
    return F.lit(1.0) - (
        F.levenshtein(s1, s2).cast("double") /
        F.greatest(F.length(s1), F.length(s2)).cast("double")
    )


def _levenshtein_standard(s1, s2):
    lev = F.levenshtein(s1, s2).cast("double")
    mlen = F.greatest(F.length(s1), F.length(s2)).cast("double")
    return F.lit(1.0) - (lev / F.when(mlen == 0, F.lit(1.0)).otherwise(mlen))


# ── Constants ──────────────────────────────────────────────────────────────

_RESULT_COLS = [
    "mapping_table", "source", "source_id", "source_attr", "source_value",
    "ref_attr", "attribute_name",
    "ref_lakefusion_id", "ref_value", "match", "method", "score", "status", "ref_record",
]

_INSERT_COLS = [
    "ref_lakefusion_id", "source", "source_id", "source_attr", "source_value",
    "ref_attr", "ref_value", "match", "method", "score",
    "status", "ref_record", "updated_at",
]

_MERGE_COND = (
    "tgt.source_id    = src.source_id    AND "
    "tgt.source_attr  = src.source_attr  AND "
    "tgt.ref_attr     = src.ref_attr     AND "
    "tgt.source_value = src.source_value"
)

_APPROVED_STATUSES = ("AUTO_APPROVED", "KEEP_RDM")


# ── Step 1: ensure mapping tables exist ────────────────────────────────────

def _ensure_mapping_tables(spark, mapping_tables):
    for mt in mapping_tables:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {mt} (
                mapping_id        BIGINT GENERATED ALWAYS AS IDENTITY,
                ref_lakefusion_id STRING,
                source            STRING,
                source_id         INT,
                source_attr       STRING,
                source_value      STRING,
                ref_attr          STRING,
                ref_value         STRING,
                match             STRING,
                method            STRING,
                score             DOUBLE,
                status            STRING,
                ref_record        STRING,
                updated_at        TIMESTAMP
            )
            USING DELTA
            TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """)


# ── Step 2-3: pull distinct source values from batch, split fast/new ───────

def _build_source_long(batch_df, configs):
    """Distinct source_values present in the batch, scoped to this source's configs.

    Yields one part per config (per-attr) — same shape as Process_RDM Step 2,
    but reading from batch_df instead of full source_table.
    """
    parts = []
    for cfg in configs:
        attr_name = cfg["attribute_name"]
        if attr_name not in batch_df.columns:
            continue
        part = (
            batch_df
            .select(F.col(attr_name).cast("string").alias("source_value"))
            .filter(F.col("source_value").isNotNull() & (F.col("source_value") != ""))
            .distinct()
            .withColumn("source",         F.lit(cfg["source_table"]))
            .withColumn("source_id",      F.lit(cfg["source_id"]).cast(IntegerType()))
            .withColumn("source_attr",    F.lit(cfg["source_attr"]))
            .withColumn("ref_attr",       F.lit(cfg["ref_attr"]))
            .withColumn("attribute_name", F.lit(attr_name))
            .withColumn("mapping_table",  F.lit(cfg["mapping_table"]))
        )
        parts.append(part)
    if not parts:
        return None
    return reduce(lambda a, b: a.unionByName(b), parts)


def _split_existing_vs_new(spark, source_long_df, mapping_tables):
    """Step 3 from Process_RDM — what's already mapped vs what needs resolution."""
    match_keys = ["mapping_table", "source_id", "source_attr", "ref_attr", "source_value"]

    existing_parts = []
    for mt in mapping_tables:
        existing_parts.append(
            spark.table(mt)
            .select(
                "source_id", "source_attr", "ref_attr", "source_value",
                "ref_lakefusion_id", "ref_value",
            )
            .withColumn("mapping_table", F.lit(mt))
        )
    existing_df = (
        reduce(DataFrame.unionByName, existing_parts)
        .dropDuplicates(match_keys)
    )
    new_values_df = source_long_df.join(
        existing_df.select(*match_keys),
        on=match_keys,
        how="left_anti",
    )
    return new_values_df


# ── Step 4: resolve new values per config (exact / fuzzy) ──────────────────

def _resolve_new_values(spark, new_values_df, configs):
    result_parts = []
    for cfg in configs:
        this_cfg_values = new_values_df.filter(
            (F.col("source_id")      == cfg["source_id"]) &
            (F.col("source_attr")    == cfg["source_attr"]) &
            (F.col("attribute_name") == cfg["attribute_name"])
        )
        if not this_cfg_values.head(1):
            continue

        ref_full = spark.table(cfg["ref_table"])
        # SCD-2: only match against current versions. Falls back to all rows
        # for non-SCD-2 ref tables (no is_current column).
        if "is_current" in ref_full.columns:
            ref_full = ref_full.filter(F.col("is_current") == F.lit(True))
        ref_df = (
            ref_full
            .withColumn("__ref_record", F.to_json(F.struct("*")))
            .select(
                F.col(cfg["ref_attr"]).cast("string").alias("ref_key_raw"),
                F.col(cfg["ref_output_attr"]).cast("string").alias("ref_output_val"),
                F.col("ref_lakefusion_id").cast("string").alias("ref_lakefusion_id"),
                F.col("__ref_record").alias("ref_record"),
            )
        )

        input_norm = (
            F.upper(F.trim(F.col("source_value"))) if cfg["case_insensitive"]
            else F.col("source_value")
        )

        if cfg["match_strategy"] == "exact":
            ref_with_norm = ref_df.withColumn(
                "ref_key_norm", F.upper(F.trim(F.col("ref_key_raw")))
            )
            matched = F.col("ref_lakefusion_id").isNotNull()

            resolved = (
                this_cfg_values
                .withColumn("input_norm", input_norm)
                .join(ref_with_norm, F.col("input_norm") == F.col("ref_key_norm"), "left")
                .withColumn("method", F.lit("exact"))
                .withColumn("score",
                    F.when(matched, F.lit(1.0)).otherwise(F.lit(0.0)).cast(DoubleType())
                )
                .withColumn("status",
                    F.when(matched, F.lit("AUTO_APPROVED")).otherwise(F.lit("NO_MATCH"))
                )
                .withColumn("ref_lakefusion_id", F.when(matched, F.col("ref_lakefusion_id")))
                .withColumn("ref_value",         F.when(matched, F.col("ref_output_val")))
                .withColumn("match",             F.when(matched, F.col("ref_key_raw")))
                .withColumn("ref_record",        F.when(matched, F.col("ref_record")))
                .select(*_RESULT_COLS)
            )
            result_parts.append(resolved)

        elif cfg["match_strategy"] == "fuzzy":
            algo = cfg["fuzzy_algorithm"]
            ref_with_s2 = ref_df.withColumn("s2", F.upper(F.trim(F.col("ref_key_raw"))))

            crossed = (
                this_cfg_values
                .withColumn("s1", input_norm)
                .crossJoin(ref_with_s2)
            )

            if algo == "jaro_winkler":
                scored = crossed.withColumn("score", _jaro_winkler_udf("s1", "s2"))
            elif algo == "levenshtein_normalized":
                scored = crossed.withColumn("score", _levenshtein_normalized(F.col("s1"), F.col("s2")))
            elif algo == "levenshtein_standard":
                scored = crossed.withColumn("score", _levenshtein_standard(F.col("s1"), F.col("s2")))
            else:
                raise ValueError(f"Unsupported fuzzy algorithm: {algo}")

            w = Window.partitionBy("source_id", "source_attr", "ref_attr", "source_value") \
                      .orderBy(F.col("score").desc())
            matched = F.col("score") >= F.lit(cfg["low_threshold"])

            top1 = (
                scored
                .withColumn("rank", F.row_number().over(w))
                .filter(F.col("rank") == 1)
                .drop("rank", "s1", "s2")
                .withColumn("method", F.lit(algo))
                .withColumn("score",  F.col("score").cast(DoubleType()))
                .withColumn("status",
                    F.when(F.col("score") >= F.lit(cfg["high_threshold"]), F.lit("AUTO_APPROVED"))
                     .when(matched,                                         F.lit("PENDING"))
                     .otherwise(                                            F.lit("NO_MATCH"))
                )
                .withColumn("ref_lakefusion_id", F.when(matched, F.col("ref_lakefusion_id")))
                .withColumn("ref_value",         F.when(matched, F.col("ref_output_val")))
                .withColumn("match",             F.when(matched, F.col("ref_key_raw")))
                .withColumn("ref_record",        F.when(matched, F.col("ref_record")))
                .select(*_RESULT_COLS)
            )
            result_parts.append(top1)
        else:
            raise ValueError(f"Unknown match_strategy: {cfg['match_strategy']}")

    if not result_parts:
        return None
    return (
        reduce(lambda a, b: a.unionByName(b), result_parts)
        .withColumn("updated_at", F.current_timestamp())
    )


# ── Step 5: MERGE new mappings into mapping table(s) ──────────────────────

def _merge_into_mapping_tables(spark, new_mappings_df, mapping_tables):
    if new_mappings_df is None:
        return
    for mt in mapping_tables:
        merge_src = (
            new_mappings_df
            .filter(F.col("mapping_table") == mt)
            .select(*_INSERT_COLS)
            .dropDuplicates(["source_id", "source_attr", "ref_attr", "source_value"])
        )
        if not merge_src.head(1):
            continue
        (
            DeltaTable.forName(spark, mt).alias("tgt")
            .merge(merge_src.alias("src"), _MERGE_COND)
            .whenNotMatchedInsert(values={c: F.col(f"src.{c}") for c in _INSERT_COLS})
            .execute()
        )


# ── Public entry point ───────────────────────────────────────────────────

def resolve_reference_attributes(spark, batch_df, rdm_configs, source_id):
    """Run Steps 1-5 then split batch into (approved, pending) DataFrames.

    Args:
        spark: SparkSession
        batch_df: DataFrame to enrich. Columns named by entity attribute_name
                  (i.e. after source→entity rename has happened).
        rdm_configs: list of RDM config dicts (may span multiple sources;
                     we filter to source_id ourselves).
        source_id: dataset_id of the source currently being ingested.

    Returns:
        (approved_df, pending_df)

        approved_df has the same columns as batch_df, plus one
        `<attribute_name>__display` column per REFERENCE_ENTITY attr,
        carrying the canonical display value (ref_value) for use in
        attributes_combined. The original column is replaced with
        ref_lakefusion_id. Caller should drop the __display columns
        before writing to unified.

        pending_df has the same columns as batch_df with original raw
        values preserved, plus a `_rdm_pending_reason` column describing
        which REF attrs blocked the row (e.g.
        "country=PENDING(United Statz); state=NO_MATCH(XX)"). Caller routes
        to UnifiedErrorHandler with stage="RDM" using surrogate_key as the
        key and _rdm_pending_reason as error_message.

    No-op behavior:
        If rdm_configs has no entries for source_id, returns (batch_df,
        empty_df) without touching any mapping table.
    """
    # Filter to current source
    configs = [c for c in (rdm_configs or []) if c.get("source_id") == source_id]
    empty_df = batch_df.limit(0)

    if not configs:
        return batch_df, empty_df

    mapping_tables = sorted({c["mapping_table"] for c in configs if c.get("mapping_table")})
    if not mapping_tables:
        return batch_df, empty_df

    # Step 1: ensure mapping tables exist
    _ensure_mapping_tables(spark, mapping_tables)

    # Step 2: distinct source_values in batch
    source_long_df = _build_source_long(batch_df, configs)
    if source_long_df is None:
        # No REF attrs in batch_df columns
        return batch_df, empty_df

    # Step 3: split fast-path vs new
    new_values_df = _split_existing_vs_new(spark, source_long_df, mapping_tables)

    # Step 4: resolve new values
    new_mappings_df = _resolve_new_values(spark, new_values_df, configs)

    # Step 5: MERGE into mapping table
    _merge_into_mapping_tables(spark, new_mappings_df, mapping_tables)

    # ── Enrich batch_df from mapping, then split approved vs pending ──────
    enriched = batch_df
    pending_clauses = []
    saw_ref_attr = False

    for cfg in configs:
        attr_name = cfg["attribute_name"]
        if attr_name not in batch_df.columns:
            continue

        mt = cfg["mapping_table"]
        # Per-attr slice: only this (source_id, source_attr) — avoids mixing
        # rows from sibling sources or sibling attrs that happen to share value
        attr_mapping = (
            spark.table(mt)
            .filter((F.col("source_id") == cfg["source_id"]) &
                    (F.col("source_attr") == cfg["source_attr"]))
            .select(
                F.col("source_value").alias(f"_mp_{attr_name}_src"),
                F.col("ref_lakefusion_id").alias(f"_mp_{attr_name}_id"),
                F.col("ref_value").alias(f"_mp_{attr_name}_display"),
                F.col("status").alias(f"_mp_{attr_name}_status"),
            )
        )

        enriched = enriched.join(
            attr_mapping,
            enriched[attr_name].cast("string") == F.col(f"_mp_{attr_name}_src"),
            "left",
        )

        # Row is UNRESOLVED for this attr if:
        #   - source value is non-null AND
        #   - status is PENDING / NO_MATCH / missing (no row in mapping)
        # If source value is null/empty, attr doesn't block (nothing to resolve).
        # An unresolved attr only routes the row to the error table when its
        # unresolved_action is "move_to_error". The default "keep_null" keeps
        # the row and nulls the attr instead (handled in the replacement loop).
        attr_str = F.col(attr_name).cast("string")
        is_unresolved = (
            attr_str.isNotNull() & (attr_str != "") &
            (
                F.col(f"_mp_{attr_name}_status").isNull() |
                ~F.col(f"_mp_{attr_name}_status").isin(*_APPROVED_STATUSES)
            )
        )
        saw_ref_attr = True
        if (cfg.get("unresolved_action") or "keep_null") == "move_to_error":
            pending_clauses.append(is_unresolved)

    if not saw_ref_attr:
        # No REF attrs present in batch columns — nothing to resolve
        return batch_df, empty_df

    # Combine — row is pending if ANY move_to_error ref attr is unresolved
    if pending_clauses:
        any_pending = pending_clauses[0]
        for c in pending_clauses[1:]:
            any_pending = any_pending | c
        enriched = enriched.withColumn("_any_pending", any_pending)
        approved_df = enriched.filter(~F.col("_any_pending") | F.col("_any_pending").isNull())
        pending_df = enriched.filter(F.col("_any_pending"))
    else:
        # All ref attrs are keep_null → no row is ever routed to the error table
        enriched = enriched.withColumn("_any_pending", F.lit(False))
        approved_df = enriched
        pending_df = enriched.filter(F.col("_any_pending"))

    # In approved_df: replace REF column with its resolved ref_id and add a
    # __display column. For move_to_error attrs, approved rows are always
    # resolved (unresolved ones were split into pending_df). For keep_null
    # attrs an unresolved value is nulled out — the row is kept but the attr
    # (and its display) become NULL.
    for cfg in configs:
        attr_name = cfg["attribute_name"]
        if attr_name not in batch_df.columns:
            continue
        if (cfg.get("unresolved_action") or "keep_null") == "keep_null":
            is_resolved = F.col(f"_mp_{attr_name}_status").isin(*_APPROVED_STATUSES)
            approved_df = (
                approved_df
                .withColumn(
                    attr_name,
                    F.when(is_resolved, F.col(f"_mp_{attr_name}_id"))
                     .otherwise(F.lit(None).cast("string")),
                )
                .withColumn(
                    f"{attr_name}__display",
                    F.when(is_resolved, F.col(f"_mp_{attr_name}_display"))
                     .otherwise(F.lit(None).cast("string")),
                )
            )
        else:
            approved_df = (
                approved_df
                .withColumn(
                    attr_name,
                    F.coalesce(F.col(f"_mp_{attr_name}_id"), F.col(attr_name)),
                )
                .withColumn(
                    f"{attr_name}__display",
                    F.coalesce(F.col(f"_mp_{attr_name}_display"), F.col(attr_name)),
                )
            )

    # Build _rdm_pending_reason per pending row — a human-readable string of
    # which REF attrs blocked this row, with their status and raw value:
    #   "country=PENDING(United Statz); state=NO_MATCH(XX)"
    # Used as `error_message` when callers route to UnifiedErrorHandler.
    reason_parts = []
    for cfg in configs:
        attr_name = cfg["attribute_name"]
        if attr_name not in batch_df.columns:
            continue
        # Only move_to_error attrs can put a row in pending_df, so the reason
        # string must describe those attrs only.
        if (cfg.get("unresolved_action") or "keep_null") != "move_to_error":
            continue
        attr_str = F.col(attr_name).cast("string")
        is_blocking = (
            attr_str.isNotNull() & (attr_str != "") &
            (
                F.col(f"_mp_{attr_name}_status").isNull() |
                ~F.col(f"_mp_{attr_name}_status").isin(*_APPROVED_STATUSES)
            )
        )
        status_label = F.coalesce(F.col(f"_mp_{attr_name}_status"), F.lit("UNRESOLVED"))
        attr_part = F.when(
            is_blocking,
            F.concat(
                F.lit(f"{attr_name}="),
                status_label,
                F.lit("("),
                F.coalesce(attr_str, F.lit("")),
                F.lit(")"),
            ),
        ).otherwise(F.lit(None).cast("string"))
        reason_parts.append(attr_part)

    if reason_parts:
        # array_join with skipNulls=True is unavailable; use concat_ws which skips nulls
        pending_df = pending_df.withColumn(
            "_rdm_pending_reason",
            F.concat_ws("; ", *reason_parts),
        )
    else:
        # No move_to_error attrs → pending_df is empty, but keep the column so
        # callers can select it unconditionally.
        pending_df = pending_df.withColumn(
            "_rdm_pending_reason", F.lit(None).cast("string")
        )

    # Drop intermediate cols (keep _rdm_pending_reason on pending_df)
    drop_cols = ["_any_pending"]
    for cfg in configs:
        attr_name = cfg["attribute_name"]
        drop_cols += [
            f"_mp_{attr_name}_src",
            f"_mp_{attr_name}_id",
            f"_mp_{attr_name}_display",
            f"_mp_{attr_name}_status",
        ]
    approved_df = approved_df.drop(*drop_cols)
    pending_df = pending_df.drop(*drop_cols)

    return approved_df, pending_df


def display_column_names(rdm_configs, source_id):
    """Helper: returns dict {attribute_name: display_column_name} for the
    REF attrs in this source. Lets the caller build attributes_combined
    using the right column for each match attribute."""
    return {
        cfg["attribute_name"]: f"{cfg['attribute_name']}__display"
        for cfg in (rdm_configs or [])
        if cfg.get("source_id") == source_id
    }


# ── attributes_combined builder for unified / master writes ────────────────

def build_attributes_combined_column(
    spark,
    match_attributes,
    entity_attributes,
    id_key,
    reference_attribute_config=None,
    source_prefix=None,
    separator=" | ",
    concat_separator=AGG_CONCAT_SEPARATOR,
):
    """Returns a Column expression for `attributes_combined`, resolving each
    REFERENCE_ENTITY attribute through its ref table.

    Handles BOTH a single ref_lakefusion_id AND a concat-aggregated multi-id
    string in the source column (the latter is what survivorship's
    AggregationStrategy.concat produces). For multi-id, the value is split by
    `concat_separator`, each id is looked up in the ref table, and the
    resolved displays are re-joined with the same separator.

    Args:
        spark: SparkSession
        match_attributes: ordered list of attribute names to combine
        entity_attributes: full list of entity attrs (skips attrs not in here)
        id_key: skipped from the combined output (e.g. "lakefusion_id")
        reference_attribute_config: dict {attr_name: {ref_table, output_attr}}
            for REFERENCE_ENTITY attrs. Empty/None → no ref resolution applied.
        source_prefix: optional struct prefix for the attribute columns
            (e.g. "merged_record" when reading survivorship UDF output).
            None means columns are at top-level of the DataFrame.
        separator: separator BETWEEN attributes (default " | ")
        concat_separator: separator WITHIN a single attribute when it carries
            multiple ref_ids from concat aggregation (default " • ", matches
            AggregationStrategy.DEFAULT_SEPARATOR).

    Returns:
        Column expression — pass to df.withColumn("attributes_combined", ...)
        or include in a select projection.
    """
    udf_cache = {}

    def _get_resolver_udf(ref_table, output_attr):
        cache_key = (ref_table, output_attr)
        cached = udf_cache.get(cache_key)
        if cached is not None:
            return cached

        # SCD-2: ref_table now holds historical versions per ref_lakefusion_id.
        # Filter to is_current=TRUE so only the latest version contributes to
        # attributes_combined. Schema-evolution-safe: if the column doesn't
        # exist (older / non-SCD-2 ref tables), fall back to no filter.
        ref_df = spark.read.table(ref_table)
        if "is_current" in ref_df.columns:
            ref_df = ref_df.filter(F.col("is_current") == F.lit(True))
        rows = (
            ref_df
            .select(F.col("ref_lakefusion_id"), F.col(output_attr).alias("_d"))
            .collect()
        )
        ref_dict = {
            r["ref_lakefusion_id"]: r["_d"]
            for r in rows
            if r["ref_lakefusion_id"] is not None
        }

        def _resolve(s):
            if s is None:
                return None
            s = str(s)
            if s == "":
                return s
            parts = [p.strip() for p in s.split(concat_separator)]
            resolved = [str(ref_dict.get(p, p)) for p in parts if p != ""]
            return concat_separator.join(resolved)

        udf_fn = F.udf(_resolve, StringType())
        udf_cache[cache_key] = udf_fn
        return udf_fn

    # Complex (STRUCT / ARRAY / ARRAY<STRUCT>) values arrive as JSON strings
    # because the survivorship engine JSON-serialises non-scalar outputs.
    # Strip keys + brackets and join leaf values so attributes_combined
    # carries only the VALUES — keys inside JSON pollute vector search.
    import json as _json

    def _flatten_json_to_values(s):
        if s is None or s == "":
            return s
        try:
            parsed = _json.loads(s)
        except Exception:
            return s
        def walk(x):
            if x is None:
                return ""
            if isinstance(x, list):
                return " ".join(p for p in (walk(i) for i in x) if p)
            if isinstance(x, dict):
                return " ".join(p for p in (walk(v) for v in x.values()) if p)
            if isinstance(x, bool):
                return "true" if x else "false"
            return str(x)
        return walk(parsed)

    _flatten_udf = F.udf(_flatten_json_to_values, StringType())

    concat_cols = []
    for attr in match_attributes:
        if attr not in entity_attributes or attr == id_key:
            continue

        col_path = f"{source_prefix}.{attr}" if source_prefix else attr
        col_ref = F.col(col_path).cast("string")

        ref_cfg = (reference_attribute_config or {}).get(attr)
        if ref_cfg and ref_cfg.get("ref_table"):
            col_ref = _get_resolver_udf(
                ref_cfg["ref_table"],
                ref_cfg.get("output_attr", attr),
            )(col_ref)
        else:
            # Non-ref attrs: pass through JSON-flatten so complex types
            # collapse to value-only strings. Scalar JSON-shape strings are
            # rare; on parse failure the value is returned unchanged.
            col_ref = _flatten_udf(col_ref)

        concat_cols.append(F.coalesce(col_ref, F.lit("")))

    if not concat_cols:
        return F.lit("")

    return F.concat_ws(separator, *concat_cols)
