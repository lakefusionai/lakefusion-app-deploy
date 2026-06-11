# Databricks notebook source
"""Centralized Spark type resolution for LakeFusion entity attributes.

Consolidates the scalar-type dictionary that previously lived (copy-pasted) in
six notebooks across initial_load, match_maven, incremental_load, and
schema-evolution, and adds support for complex types (STRUCT, ARRAY<scalar>,
ARRAY<STRUCT>) introduced by Epic

Two entry points
----------------
- get_spark_data_type(dtype_str): string scalar type -> Spark DataType. Existing
  behavior, unchanged for the legacy callers. Returns StringType() as a safe
  fallback when the type is unknown.
- get_complex_spark_data_type(attribute): attribute *record* (dict-shaped) ->
  Spark DataType. Honours is_array + struct_definition.fields. Falls back to
  get_spark_data_type for plain scalar records (or any record without
  is_array/STRUCT shape) so it is safe to call from any code path.

Schema construction
-------------------
- create_schema_fields(attributes_list, attributes_datatype_dict,
                       include_lakefusion_id=True, id_key='lakefusion_id',
                       attributes=None): builds a list of StructField for an
  entity. Backward-compatible with all existing callers (the new `attributes`
  param defaults to None, in which case the function behaves exactly as before
  and uses get_spark_data_type for every attribute).

When `attributes` is supplied it must be the entity's attribute list as
exported in the entity JSON (each record carrying name, type, is_array,
struct_definition); for each name the function then dispatches through
get_complex_spark_data_type so STRUCT / ARRAY columns resolve to nested Spark
types instead of the StringType fallback.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Optional

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

DEFAULT_ID_KEY = "lakefusion_id"

# Canonical scalar map. Uppercase keys are the source-of-truth; lowercase
# variants are kept for backward compatibility with the legacy entity JSON
# (which mixed cases freely).
_SCALAR_TYPE_MAP: Dict[str, DataType] = {
    # New uppercase types (primary)
    "BIGINT": LongType(),
    "BOOLEAN": BooleanType(),
    "DATE": DateType(),
    "DOUBLE": DoubleType(),
    "FLOAT": FloatType(),
    "INT": IntegerType(),
    "SMALLINT": ShortType(),
    "STRING": StringType(),
    "TINYINT": ByteType(),
    "TIMESTAMP": TimestampType(),
    "DECIMAL": DecimalType(10, 2),
    # Legacy lowercase / verbose variants
    "bigint": LongType(),
    "boolean": BooleanType(),
    "char": StringType(),
    "varchar": StringType(),
    "date": DateType(),
    "double precision": DoubleType(),
    "double": DoubleType(),
    "integer": IntegerType(),
    "int": IntegerType(),
    "long": LongType(),
    "numeric": FloatType(),
    "real": FloatType(),
    "smallint": ShortType(),
    "text": StringType(),
    "string": StringType(),
    "timestamp": TimestampType(),
    "float": FloatType(),
    "decimal": DecimalType(10, 2),
}


def get_spark_data_type(dtype_str: Any) -> DataType:
    """Resolve a *scalar* type name to a Spark DataType.

    Falls back to StringType() on unknown/None inputs so an unexpected legacy
    record never breaks table creation.
    """
    if dtype_str is None:
        return StringType()
    if dtype_str in _SCALAR_TYPE_MAP:
        return _SCALAR_TYPE_MAP[dtype_str]
    # Tolerate any case variant.
    return _SCALAR_TYPE_MAP.get(str(dtype_str).lower(), StringType())


def _struct_type_from_fields(fields: Iterable[Mapping[str, Any]]) -> StructType:
    """Build a StructType from a list of struct field records.

    Each record must carry `name` and `data_type`. `ordinal_position` is
    respected when present so the resulting StructType preserves the schema
    ordering the user defined in the UI.
    """
    sorted_fields = sorted(
        list(fields),
        key=lambda f: f.get("ordinal_position", 0)
        if isinstance(f, Mapping)
        else 0,
    )
    return StructType(
        [
            StructField(
                f["name"],
                get_spark_data_type(f.get("data_type")),
                True,
            )
            for f in sorted_fields
            if isinstance(f, Mapping) and f.get("name")
        ]
    )


def get_complex_spark_data_type(attribute: Mapping[str, Any]) -> DataType:
    """Resolve any attribute record (scalar or complex) to a Spark DataType.

    Required keys on `attribute`:
        - type: string scalar name OR the literal "STRUCT"
        - is_array (optional, default False)
        - struct_definition (optional dict with `fields` list) — required when
          `type == "STRUCT"` (covers both STRUCT and ARRAY<STRUCT>).

    Resolution table
    ----------------
        is_array=False, type=STRING        -> StringType
        is_array=False, type=INT           -> IntegerType
        is_array=False, type=STRUCT        -> StructType(...)
        is_array=True,  type=STRING        -> ArrayType(StringType)
        is_array=True,  type=INT           -> ArrayType(IntegerType)
        is_array=True,  type=STRUCT        -> ArrayType(StructType(...))

    Returns StringType() as a safe default when `attribute` lacks a `type`
    field (matches the existing fallback in get_spark_data_type).
    """
    if not isinstance(attribute, Mapping):
        return StringType()

    raw_type = attribute.get("type")
    type_norm = str(raw_type).strip().upper() if raw_type is not None else ""
    is_array = bool(attribute.get("is_array", False))

    if type_norm == "STRUCT":
        struct_def = attribute.get("struct_definition") or {}
        fields = (struct_def.get("fields") if isinstance(struct_def, Mapping) else None) or []
        struct_type = _struct_type_from_fields(fields)
        return ArrayType(struct_type) if is_array else struct_type

    scalar_type = get_spark_data_type(raw_type)
    return ArrayType(scalar_type) if is_array else scalar_type


def create_schema_fields(
    attributes_list: List[str],
    attributes_datatype_dict: Mapping[str, Any],
    include_lakefusion_id: bool = True,
    id_key: str = DEFAULT_ID_KEY,
    attributes: Optional[Iterable[Mapping[str, Any]]] = None,
) -> List[StructField]:
    """Build the StructField list for an entity table.

    Backward-compatible: `attributes` defaults to None, in which case every
    column resolves via `get_spark_data_type(attributes_datatype_dict[name])`
    — identical to the legacy notebook behavior.

    When `attributes` is supplied (typically the `attributes` array from the
    entity JSON export, with each record carrying `type`, `is_array`, and
    `struct_definition`), STRUCT and ARRAY columns resolve to nested Spark
    types via `get_complex_spark_data_type`. Scalar columns continue through
    `attributes_datatype_dict` so callers that only carry datatype strings
    keep working.
    """
    fields: List[StructField] = []

    if include_lakefusion_id:
        fields.append(StructField(id_key, StringType(), True))

    attr_record_by_name: Dict[str, Mapping[str, Any]] = {}
    if attributes:
        for record in attributes:
            if isinstance(record, Mapping):
                name = record.get("name")
                if name:
                    attr_record_by_name[name] = record

    for attr_name in attributes_list:
        if attr_name == id_key:
            continue

        record = attr_record_by_name.get(attr_name)
        if record is not None and (
            str(record.get("type", "")).strip().upper() == "STRUCT"
            or bool(record.get("is_array", False))
        ):
            spark_dtype: DataType = get_complex_spark_data_type(record)
        else:
            dtype_str = attributes_datatype_dict.get(attr_name, "string")
            spark_dtype = get_spark_data_type(dtype_str)

        fields.append(StructField(attr_name, spark_dtype, True))

    return fields
