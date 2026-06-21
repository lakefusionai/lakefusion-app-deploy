# Databricks notebook source
"""attributes_combined flattening for complex-type aware ingestion.

`attributes_combined` is the flat string that downstream vector search
indexes. With ARRAY / STRUCT support it must remain a flat string while
faithfully representing nested content:

- Scalar attribute  -> trimmed string of the value.
- ARRAY<scalar>     -> JSON-serialized array, e.g. '["a","b"]'.
- STRUCT            -> sub-field values joined in struct-field order (the
                       same flat representation the embedding model already
                       trained on for flat attributes).
- ARRAY<STRUCT>     -> JSON-serialized array of structs.

Concat order across attributes follows the `match_attributes` list — same
as the legacy single-line `concat_ws(" | ", …)` pattern in the existing
notebooks. We replace that single expression with a complex-type-aware
builder so all six representations above produce a single string column.
"""
from __future__ import annotations

from typing import Any, Iterable, List, Mapping, Optional, Sequence

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame
from pyspark.sql.types import ArrayType, StructType


def _normalize_type(t: Any) -> str:
    return str(t or "").strip().upper()


def _attr_record_lookup(
    attribute_records: Iterable[Mapping[str, Any]],
) -> dict:
    out: dict = {}
    for rec in attribute_records:
        if isinstance(rec, Mapping) and rec.get("name"):
            out[rec["name"]] = rec
    return out


def _expr_for_column(
    df: DataFrame,
    attr_name: str,
    record: Mapping[str, Any],
    selected_sub_fields: Optional[Sequence[str]] = None,
) -> Column:
    """Return a Column producing the flattened-string representation for a
    single attribute. Falls back to a trimmed string cast when the attribute
    record doesn't describe a complex type.

    `selected_sub_fields`, when non-empty, restricts STRUCT (and ARRAY<STRUCT>)
    sub-fields to the named subset — used by MatchMaven models where the
    user picked specific sub-fields for embedding/LLM matching."""
    col = F.col(attr_name)
    raw_type = _normalize_type(record.get("type"))
    is_array = bool(record.get("is_array", False))

    if attr_name not in df.schema.fieldNames():
        return F.lit("")

    df_dtype = df.schema[attr_name].dataType
    sub_filter = set(selected_sub_fields or [])

    # ARRAY<STRUCT> -> collapse each element to its sub-field VALUES joined
    # by space, then join elements by space too. Output looks like
    # "email alice@crm.com true sms 617-555-0001 true" instead of JSON.
    # JSON keys pollute embeddings/vector search.
    if is_array and (raw_type == "STRUCT" or (
        isinstance(df_dtype, ArrayType) and isinstance(df_dtype.elementType, StructType)
    )):
        inner = (
            df_dtype.elementType
            if isinstance(df_dtype, ArrayType) and isinstance(df_dtype.elementType, StructType)
            else None
        )
        if inner is not None and inner.fields:
            kept_names = (
                [f.name for f in inner.fields if f.name in sub_filter]
                if sub_filter
                else [f.name for f in inner.fields]
            ) or [f.name for f in inner.fields]
            # Per-element value list then space-join across elements.
            per_elem = F.transform(
                col,
                lambda x: F.regexp_replace(
                    F.trim(
                        F.concat_ws(
                            " ",
                            *[
                                F.coalesce(x.getField(n).cast("string"), F.lit(""))
                                for n in kept_names
                            ],
                        )
                    ),
                    r"\s+",
                    " ",
                ),
            )
            return F.coalesce(F.array_join(per_elem, " "), F.lit(""))
        # Schema not resolvable — fall back to JSON dump.
        return F.coalesce(F.to_json(col), F.lit(""))

    # ARRAY<scalar> -> space-joined values (no brackets / quotes).
    if is_array or isinstance(df_dtype, ArrayType):
        return F.coalesce(
            F.array_join(
                F.transform(col, lambda x: F.coalesce(x.cast("string"), F.lit(""))),
                " ",
            ),
            F.lit(""),
        )

    # STRUCT (non-array) -> concatenate fields in struct order.
    if raw_type == "STRUCT" or isinstance(df_dtype, StructType):
        struct_type = df_dtype if isinstance(df_dtype, StructType) else None
        if struct_type is not None and struct_type.fields:
            kept_fields = (
                [f for f in struct_type.fields if f.name in sub_filter]
                if sub_filter
                else list(struct_type.fields)
            )
            if not kept_fields:
                kept_fields = list(struct_type.fields)
            pieces = [
                F.coalesce(col.getField(f.name).cast("string"), F.lit(""))
                for f in kept_fields
            ]
            return F.regexp_replace(F.trim(F.concat_ws(" ", *pieces)), r"\s+", " ")
        # Fallback if Spark struct schema isn't resolvable for any reason.
        return F.coalesce(F.to_json(col), F.lit(""))

    # Scalar.
    return F.regexp_replace(F.trim(col.cast("string")), r"\s+", " ")


def build_attributes_combined_column(
    df: DataFrame,
    attributes_list: Sequence[str],
    attribute_records: Sequence[Mapping[str, Any]],
    separator: str = " | ",
    selected_sub_fields_by_attr: Optional[Mapping[str, Sequence[str]]] = None,
    ref_display_cols: Optional[Mapping[str, str]] = None,
) -> Column:
    """Construct the `attributes_combined` Column.

    Args:
        df: DataFrame whose schema describes the actual Spark column types.
        attributes_list: ordered list of attribute names to fold into the
            combined string (typically `match_attributes`).
        attribute_records: full attribute records (name + type + is_array +
            struct_definition). Used to decide per-attribute flattening.
        separator: between-attribute separator. Defaults to " | " matching
            the existing notebook output.
        selected_sub_fields_by_attr: optional per-attribute restriction of
            STRUCT sub-fields (and inner fields of ARRAY<STRUCT>) — used by
            MatchMaven model configs where the user picked specific
            sub-fields for embedding / LLM matching.
        ref_display_cols: optional {attr -> temp column name} for
            REFERENCE_ENTITY attributes whose raw ref_lakefusion_id has been
            resolved to a display value in `<temp column>` (see
            `resolve_reference_display_columns`). For those attrs the combined
            string uses the resolved display (a scalar string), NOT the raw id.
    """
    records_by_name = _attr_record_lookup(attribute_records)
    selection = dict(selected_sub_fields_by_attr or {})
    ref_cols = dict(ref_display_cols or {})
    pieces: List[Column] = []
    for name in attributes_list:
        if name in ref_cols and ref_cols[name] in df.schema.fieldNames():
            # Resolved REFERENCE_ENTITY display value — treat as a scalar string.
            disp = F.col(ref_cols[name]).cast("string")
            pieces.append(F.regexp_replace(F.trim(disp), r"\s+", " "))
            continue
        rec = records_by_name.get(name, {})
        pieces.append(_expr_for_column(df, name, rec, selection.get(name)))

    if not pieces:
        return F.lit("")
    return F.concat_ws(separator, *pieces)


def resolve_reference_display_columns(
    df: DataFrame,
    reference_attribute_config: Optional[Mapping[str, Mapping[str, Any]]],
    spark,
    prefix: str = "__refdisp_",
):
    """Left-join each REFERENCE_ENTITY attribute to its reference table and add a
    temporary display column `<prefix><attr>` holding the referenced entity's
    `output_attr` value (coalesced to the raw id when the join misses).

    The original `{attr}` column is left UNTOUCHED — only the temporary display
    columns are added, so the raw ref_lakefusion_id stays stored. Use the
    returned mapping with `build_attributes_combined_column(..., ref_display_cols=...)`
    so only `attributes_combined` carries the resolved display value.

    Returns (df_with_temp_cols, {attr: temp_col_name}).
    """
    cfg = reference_attribute_config or {}
    if not cfg or spark is None:
        return df, {}
    mapping: dict = {}
    for attr, c in cfg.items():
        if attr not in df.columns:
            continue
        ref_table = (c or {}).get("ref_table")
        output_attr = (c or {}).get("output_attr")
        if not ref_table or not output_attr:
            continue
        tmp = f"{prefix}{attr}"
        try:
            ref_df = spark.read.table(ref_table)
            # SCD-2 ref tables hold history — keep only current rows.
            if "is_current" in ref_df.columns:
                ref_df = ref_df.filter(F.col("is_current") == F.lit(True))
            ref_view = ref_df.select(
                F.col("ref_lakefusion_id").cast("string").alias(f"{tmp}__id"),
                F.col(output_attr).cast("string").alias(f"{tmp}__disp"),
            )
            df = (
                df.join(
                    F.broadcast(ref_view),
                    df[attr].cast("string") == F.col(f"{tmp}__id"),
                    "left",
                )
                .withColumn(tmp, F.coalesce(F.col(f"{tmp}__disp"), df[attr].cast("string")))
                .drop(f"{tmp}__id", f"{tmp}__disp")
            )
            mapping[attr] = tmp
        except Exception:
            # Ref table unreadable / column missing — skip; the raw id is used.
            continue
    return df, mapping


def add_attributes_combined(
    df: DataFrame,
    attributes_list: Sequence[str],
    attribute_records: Sequence[Mapping[str, Any]],
    reference_attribute_config: Optional[Mapping[str, Mapping[str, Any]]] = None,
    spark=None,
    separator: str = " | ",
    selected_sub_fields_by_attr: Optional[Mapping[str, Sequence[str]]] = None,
    out_col: str = "attributes_combined",
) -> DataFrame:
    """Add the `attributes_combined` column to `df`, resolving REFERENCE_ENTITY
    attributes to their display value FIRST (so matching/embedding never sees a
    raw ref id) while keeping the raw `{attr}` columns unchanged.

    Convenience wrapper around `resolve_reference_display_columns` +
    `build_attributes_combined_column` that cleans up the temp columns.
    """
    df_disp, ref_map = resolve_reference_display_columns(
        df, reference_attribute_config, spark
    )
    col = build_attributes_combined_column(
        df_disp, attributes_list, attribute_records,
        separator=separator,
        selected_sub_fields_by_attr=selected_sub_fields_by_attr,
        ref_display_cols=ref_map,
    )
    out = df_disp.withColumn(out_col, col)
    if ref_map:
        out = out.drop(*ref_map.values())
    return out


def attributes_combined_from_dict(
    record: Mapping[str, Any],
    attributes_list: Sequence[str],
    attribute_records: Sequence[Mapping[str, Any]],
    separator: str = " | ",
    selected_sub_fields_by_attr: Optional[Mapping[str, Sequence[str]]] = None,
    ref_lookup: Optional[Mapping[str, Mapping[str, str]]] = None,
    concat_separator: str = " • ",
) -> str:
    """Pure-Python equivalent for survivorship engine output (master records
    are assembled in Python after survivorship runs, not in a Spark job).

    Mirrors `build_attributes_combined_column` semantics:
    - ARRAY -> JSON (filtered to selected sub-fields per element for
      ARRAY<STRUCT> when selection given)
    - STRUCT (dict) -> space-joined values in struct field order, restricted
      to selected sub-fields when given
    - scalar -> trimmed string

    `ref_lookup` (optional): {attr -> {ref_lakefusion_id -> display}} for
    REFERENCE_ENTITY attrs. When given, a ref attr's stored value (a ref id, or
    a `concat_separator`-joined multi-id from concat aggregation) is replaced
    with the resolved display value BEFORE folding into the combined string —
    so the matching text never carries a raw ref id. The raw value in `record`
    is not mutated.
    """
    import json
    import re

    records_by_name = _attr_record_lookup(attribute_records)
    selection = dict(selected_sub_fields_by_attr or {})
    ref_lookup = ref_lookup or {}

    def _resolve_ref(attr_name, val):
        table = ref_lookup.get(attr_name)
        if not table or val is None:
            return val
        s = str(val)
        if s == "":
            return s
        parts_ids = [p.strip() for p in s.split(concat_separator)]
        return concat_separator.join(str(table.get(p, p)) for p in parts_ids if p != "")

    parts: List[str] = []
    for name in attributes_list:
        if name not in record:
            parts.append("")
            continue
        value = record[name]
        if name in ref_lookup:
            # Resolved REFERENCE_ENTITY display — scalar string, skip complex paths.
            resolved = _resolve_ref(name, value)
            parts.append("" if resolved is None else re.sub(r"\s+", " ", str(resolved).strip()))
            continue
        rec = records_by_name.get(name, {})
        is_array = bool(rec.get("is_array", False))
        raw_type = _normalize_type(rec.get("type"))
        sub_filter = set(selection.get(name) or [])

        if value is None:
            parts.append("")
            continue

        # ARRAY<STRUCT> -> collapse each element to its values joined by
        # space, then join elements by space. Keys stripped (noise for VS).
        if is_array and (raw_type == "STRUCT" or (
            isinstance(value, list) and value and isinstance(value[0], Mapping)
        )):
            if isinstance(value, list):
                pieces = []
                for item in value:
                    if isinstance(item, Mapping):
                        keys = (
                            [k for k in item.keys() if k in sub_filter]
                            if sub_filter
                            else list(item.keys())
                        ) or list(item.keys())
                        elem_vals = [
                            "" if item.get(k) is None else str(item.get(k))
                            for k in keys
                        ]
                        pieces.append(re.sub(r"\s+", " ", " ".join(elem_vals).strip()))
                parts.append(" ".join(p for p in pieces if p))
                continue

        # ARRAY<scalar> -> space-joined values.
        if is_array or isinstance(value, list):
            if isinstance(value, list):
                parts.append(" ".join("" if v is None else str(v) for v in value).strip())
            else:
                parts.append(str(value))
            continue

        if raw_type == "STRUCT" or isinstance(value, Mapping):
            # Order by struct_definition.fields when available, otherwise
            # by dict iteration order. Restrict to selected sub-fields if given.
            sd = rec.get("struct_definition") or {}
            field_names = [f["name"] for f in (sd.get("fields") or []) if isinstance(f, Mapping)]
            if not field_names and isinstance(value, Mapping):
                field_names = list(value.keys())
            if sub_filter:
                kept = [fn for fn in field_names if fn in sub_filter]
                if kept:
                    field_names = kept
            ordered = [
                "" if value.get(fn) is None else str(value.get(fn))
                for fn in field_names
            ]
            parts.append(re.sub(r"\s+", " ", " ".join(ordered).strip()))
            continue

        parts.append(re.sub(r"\s+", " ", str(value).strip()))

    return separator.join(parts)
