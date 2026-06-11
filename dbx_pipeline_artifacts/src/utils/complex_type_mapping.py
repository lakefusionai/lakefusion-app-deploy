# Databricks notebook source
# MAGIC %run ./spark_types

# COMMAND ----------

"""Runtime transforms for complex-type dataset mappings.

This module is the *pipeline* side of the contract. The save-time
side lives in `lakefusion-utility/lakefusion_utility/services/complex_mapping_validation.py`.

Two responsibilities:

1. Take a source DataFrame and a list of mapping records (already augmented
   with each target attribute's full record — type, is_array,
   struct_definition) and produce a DataFrame where every target column is
   typed correctly:
       - scalar -> scalar  (existing behavior, no change)
       - scalar -> ARRAY<scalar>  (auto-wrap)
       - flat scalars -> STRUCT  (subfield assembly)
       - flat scalars -> ARRAY<STRUCT>  (subfield assembly + auto-wrap)
       - ARRAY<scalar> -> ARRAY<scalar>  (direct)
       - STRUCT -> STRUCT  (direct, schema validated)
       - STRUCT -> ARRAY<STRUCT>  (direct + auto-wrap, schema validated)
       - ARRAY<STRUCT> -> ARRAY<STRUCT>  (direct, schema validated)

2. Validate source struct schemas against the target struct definition at
   pipeline runtime and raise `StructSchemaMismatchError` with an
   actionable message when they diverge.

The functions here only produce Column / DataFrame transforms — they do not
read or write Delta. Wiring into the notebooks happens in the loader scripts.
"""
from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame
from pyspark.sql.types import (
    ArrayType,
    DataType,
    StructField,
    StructType,
)


class StructSchemaMismatchError(RuntimeError):
    """Raised when a source struct column does not match the target struct
    definition. Message format is intentionally identical across Direct
    Column and Sub-field Assembly modes so operators can grep for it."""


def _normalize_type(t: Any) -> str:
    return str(t or "").strip().upper()


def _expected_struct_fields(struct_definition: Mapping[str, Any]) -> List[Dict[str, str]]:
    """Return [{name, data_type}, ...] sorted by ordinal_position."""
    fields = list((struct_definition or {}).get("fields") or [])
    fields.sort(key=lambda f: f.get("ordinal_position", 0) if isinstance(f, Mapping) else 0)
    return [
        {"name": f["name"], "data_type": _normalize_type(f.get("data_type"))}
        for f in fields
        if isinstance(f, Mapping) and f.get("name")
    ]


def _spark_dtype_name(dt: DataType) -> str:
    """Render a Spark DataType into the LakeFusion type vocabulary so error
    messages line up with the labels users see in the UI."""
    # Cast via the simpleString() then map a handful of obvious cases.
    s = dt.simpleString().upper()
    # simpleString returns things like "string", "int", "bigint", etc.
    aliases = {
        "INT": "INT",
        "BIGINT": "BIGINT",
        "STRING": "STRING",
        "DOUBLE": "DOUBLE",
        "FLOAT": "FLOAT",
        "BOOLEAN": "BOOLEAN",
        "DATE": "DATE",
        "TIMESTAMP": "TIMESTAMP",
    }
    return aliases.get(s, s)


def _validate_struct_schema_match(
    source_struct_type: StructType,
    expected_fields: Sequence[Mapping[str, str]],
    source_column: str,
    target_attr_name: str,
    target_struct_name: str,
) -> None:
    """Compare source struct schema to expected target struct fields.

    Field order does NOT need to match — the pipeline reorders. Field names,
    field types, and field count MUST all match. Raises
    StructSchemaMismatchError on mismatch with an operator-readable message.
    """
    expected_by_name = {f["name"]: f["data_type"] for f in expected_fields}
    source_by_name = {f.name: _spark_dtype_name(f.dataType) for f in source_struct_type.fields}

    if set(expected_by_name) != set(source_by_name):
        raise StructSchemaMismatchError(
            f"Source column '{source_column}' struct schema does not match "
            f"target struct definition '{target_struct_name}' for attribute "
            f"'{target_attr_name}'. Expected fields: "
            f"{sorted(expected_by_name)}. Found: {sorted(source_by_name)}."
        )

    type_mismatches = [
        (name, expected_by_name[name], source_by_name[name])
        for name in expected_by_name
        if expected_by_name[name] != source_by_name[name]
        # STRING is allowed to absorb anything castable -> we treat string as a wildcard
        and not (expected_by_name[name] == "STRING")
    ]
    if type_mismatches:
        details = ", ".join(
            f"{n}: expected {e}, got {a}" for n, e, a in type_mismatches
        )
        raise StructSchemaMismatchError(
            f"Source column '{source_column}' struct field types do not match "
            f"target struct definition '{target_struct_name}' for attribute "
            f"'{target_attr_name}'. Mismatches: {details}."
        )


def _struct_target_type(struct_definition: Mapping[str, Any]) -> StructType:
    """Resolve a target StructType (Spark) from the struct_definition payload."""
    spark_t = get_complex_spark_data_type(
        {"type": "STRUCT", "is_array": False, "struct_definition": struct_definition}
    )
    assert isinstance(spark_t, StructType)
    return spark_t


def _resolve_source_struct_type(df: DataFrame, source_column: str) -> StructType:
    schema = df.schema
    if source_column not in schema.fieldNames():
        raise StructSchemaMismatchError(
            f"Source column '{source_column}' not found in dataset. "
            f"Available columns: {schema.fieldNames()}."
        )
    src_field = schema[source_column].dataType
    inner = src_field.elementType if isinstance(src_field, ArrayType) else src_field
    if not isinstance(inner, StructType):
        raise StructSchemaMismatchError(
            f"Source column '{source_column}' is not a STRUCT or ARRAY<STRUCT> — "
            f"cannot be used with mode='direct_column'."
        )
    return inner


def _direct_column_expr(
    df: DataFrame,
    source_column: str,
    target_record: Mapping[str, Any],
) -> Column:
    """Build a Column for a direct_column mapping with runtime schema check.

    Reorders source struct fields to match target ordering so downstream
    Spark logic can rely on positional access if needed.
    """
    target_struct = _struct_target_type(target_record.get("struct_definition") or {})
    target_is_array = bool(target_record.get("is_array"))
    expected_fields = _expected_struct_fields(target_record.get("struct_definition") or {})

    src_dt = df.schema[source_column].dataType
    source_struct = _resolve_source_struct_type(df, source_column)

    _validate_struct_schema_match(
        source_struct,
        expected_fields,
        source_column=source_column,
        target_attr_name=target_record.get("name", "?"),
        target_struct_name=(target_record.get("struct_definition") or {}).get("name")
        or "(unnamed)",
    )

    # Build a reordered struct expression.
    def _reorder(struct_col: Column) -> Column:
        return F.struct(
            *[
                struct_col.getField(f["name"]).cast(get_spark_data_type(f["data_type"])).alias(f["name"])
                for f in expected_fields
            ]
        )

    src_col = F.col(source_column)
    if isinstance(src_dt, ArrayType):
        # ARRAY<STRUCT> source -> reorder each element.
        reordered = F.transform(src_col, lambda elem: _reorder(elem))
        return reordered if target_is_array else reordered.getItem(0)
    else:
        # Single STRUCT source -> auto-wrap when target is ARRAY<STRUCT>.
        reordered = _reorder(src_col)
        return F.array(reordered) if target_is_array else reordered


def _subfield_assembly_expr(
    sub_field_map: Mapping[str, str],
    target_record: Mapping[str, Any],
) -> Column:
    """Build a struct() column from flat scalar source columns. Missing
    target fields produce null. Wraps in a 1-element array for ARRAY<STRUCT>.
    """
    expected_fields = _expected_struct_fields(target_record.get("struct_definition") or {})
    target_is_array = bool(target_record.get("is_array"))

    pieces: List[Column] = []
    for f in expected_fields:
        target_name = f["name"]
        target_type = f["data_type"]
        src_col_name = sub_field_map.get(target_name)
        if src_col_name:
            pieces.append(
                F.col(src_col_name).cast(get_spark_data_type(target_type)).alias(target_name)
            )
        else:
            pieces.append(
                F.lit(None).cast(get_spark_data_type(target_type)).alias(target_name)
            )

    struct_expr = F.struct(*pieces)
    return F.array(struct_expr) if target_is_array else struct_expr


def _scalar_to_array_expr(
    source_column: str,
    target_record: Mapping[str, Any],
) -> Column:
    """Wrap a single scalar value in a 1-element array, cast to the element
    type the target attribute declares."""
    scalar_type_str = target_record.get("type") or "STRING"
    element_dtype = get_spark_data_type(scalar_type_str)
    return F.array(F.col(source_column).cast(element_dtype))


def project_source_to_target(
    df: DataFrame,
    mapping_records: Sequence[Mapping[str, Any]],
    target_attribute_records: Sequence[Mapping[str, Any]],
) -> DataFrame:
    """Project a source DataFrame into target-typed columns.

    Args:
        df: Source DataFrame (post-CDF or freshly loaded).
        mapping_records: list of MappingAttribute dicts for the source table.
        target_attribute_records: list of entity attribute records (name,
            type, is_array, struct_definition).

    Returns:
        DataFrame with one column per *mapped* entity attribute, named
        after the entity attribute (not the source column). Columns not in
        the mapping are dropped — the caller fills any unmapped attributes
        with nulls separately.
    """
    target_by_name: Dict[str, Mapping[str, Any]] = {
        rec["name"]: rec for rec in target_attribute_records if isinstance(rec, Mapping) and rec.get("name")
    }

    select_exprs: List[Column] = []

    for rec in mapping_records:
        if not isinstance(rec, Mapping):
            continue
        ent_attr = rec.get("entity_attribute")
        if not ent_attr:
            continue
        target = target_by_name.get(ent_attr)
        if target is None:
            # No matching entity attribute (likely already deleted) — skip.
            continue

        mode = rec.get("mode")
        target_is_struct = _normalize_type(target.get("type")) == "STRUCT"
        target_is_array = bool(target.get("is_array"))

        if target_is_struct and mode == "direct_column":
            src_col = rec.get("dataset_attribute")
            select_exprs.append(_direct_column_expr(df, src_col, target).alias(ent_attr))

        elif target_is_struct and mode == "subfield_assembly":
            sub_map = rec.get("sub_field_map") or {}
            select_exprs.append(_subfield_assembly_expr(sub_map, target).alias(ent_attr))

        elif (not target_is_struct) and target_is_array:
            # Scalar ARRAY target. Inspect source: if already array, pass through; else auto-wrap.
            src_col_name = rec.get("dataset_attribute")
            if not src_col_name:
                continue
            src_dt = df.schema[src_col_name].dataType if src_col_name in df.schema.fieldNames() else None
            if isinstance(src_dt, ArrayType):
                # Direct pass-through (cast elements if needed).
                element_dtype = get_spark_data_type(target.get("type") or "STRING")
                select_exprs.append(
                    F.transform(F.col(src_col_name), lambda x: x.cast(element_dtype)).alias(ent_attr)
                )
            else:
                select_exprs.append(_scalar_to_array_expr(src_col_name, target).alias(ent_attr))

        else:
            # Plain scalar target — rename + cast.
            src_col_name = rec.get("dataset_attribute")
            if not src_col_name:
                continue
            select_exprs.append(
                F.col(src_col_name).cast(get_spark_data_type(target.get("type") or "STRING")).alias(ent_attr)
            )

    return df.select(*select_exprs) if select_exprs else df.select()
