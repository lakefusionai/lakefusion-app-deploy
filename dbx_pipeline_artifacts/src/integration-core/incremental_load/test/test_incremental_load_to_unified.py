"""
Pytest suite for `Incremental_Load_To_Unified.py`.

Strategy
--------
The notebook is a procedural Databricks script. To exercise it as realistically
as possible we:

1. Spin up a local Spark session with Delta Lake + Change Data Feed enabled.
2. Seed real Delta tables (source / unified / master / meta_info / audit) with
   purpose-built mock data for each scenario.
3. Stub out only the things that can't run locally — `dbutils`, `logger`,
   `setup_lakefusion_engine`, and `SurvivorshipEngine`.
4. Execute the notebook source in that controlled namespace and inspect the
   resulting Delta tables.

Each test owns an isolated set of catalogs/schemas (one Spark database per
test class) so tests don't bleed into each other.

Run from this directory with:

    pip install pyspark delta-spark pytest
    pytest test_incremental_load_to_unified.py -v

(The whole file `pytest.importorskip`s pyspark + delta, so it skips silently
on environments that don't have them installed.)
"""

from __future__ import annotations

import json
import os
import re
import sys
import types
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

# --------------------------------------------------------------------------- #
# pyspark + delta are runtime requirements; gracefully skip the whole file if
# they're missing instead of failing collection.
# --------------------------------------------------------------------------- #
pyspark = pytest.importorskip("pyspark", reason="pyspark required")
delta_pkg = pytest.importorskip("delta", reason="delta-spark required")

import pyspark.sql.functions as F  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    ArrayType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from delta import configure_spark_with_delta_pip  # noqa: E402
from delta.tables import DeltaTable  # noqa: E402


NOTEBOOK_PATH = Path(__file__).resolve().parent.parent / "Incremental_Load_To_Unified.py"


# =========================================================================== #
# Mocks for things we can't run locally
# =========================================================================== #
class _NotebookExit(Exception):
    """Raised by `dbutils.notebook.exit(...)` so the notebook can return early."""

    def __init__(self, status: str):
        super().__init__(status)
        self.status = status


class FakeTaskValues:
    """Stand-in for `dbutils.jobs.taskValues`."""

    def __init__(self, values: dict[str, Any]):
        self._values = dict(values)
        self._set: dict[str, Any] = {}

    def get(self, _task: str, key: str, default: Any = None):
        if key in self._values:
            return self._values[key]
        return default if default is not None else ""

    def set(self, key: str, value: Any):
        self._set[key] = value


class FakeNotebook:
    def exit(self, status: str):
        raise _NotebookExit(status)


class FakeDbutils:
    def __init__(self, task_values: FakeTaskValues):
        self.jobs = types.SimpleNamespace(taskValues=task_values)
        self.notebook = FakeNotebook()


class FakeLogger:
    """Records every log line so tests can assert on observable behaviour."""

    def __init__(self):
        self.records: list[tuple[str, str]] = []

    def _log(self, level: str, msg: str):
        self.records.append((level, msg))

    def info(self, msg: str):
        self._log("INFO", msg)

    def warning(self, msg: str):
        self._log("WARN", msg)

    def error(self, msg: str):
        self._log("ERROR", msg)

    def debug(self, msg: str):
        self._log("DEBUG", msg)

    def lines(self, level: str | None = None) -> list[str]:
        if level is None:
            return [m for _, m in self.records]
        return [m for lvl, m in self.records if lvl == level]


class _AttributeSourceMapping:
    """Mirrors `lakefusion_core_engine.survivorship.AttributeSourceMapping`."""

    def __init__(self, attribute_name, attribute_value, source):
        self.attribute_name = attribute_name
        self.attribute_value = attribute_value
        self.source = source


class _SurvivorshipResult:
    def __init__(self, record, mappings):
        self.resultant_record = record
        self.resultant_master_attribute_source_mapping = mappings


class FakeSurvivorshipEngine:
    """Deterministic stand-in for the real engine.

    Per attribute, picks the first contributor in priority-order
    (`survivorship_config[0].strategyRule` if it's a Source-System rule,
    otherwise input order) that has a non-None value. The picked source is
    recorded in the attribute-source mapping. This is enough realism to drive
    every assertion the notebook makes against the engine's output.
    """

    instances: list["FakeSurvivorshipEngine"] = []

    def __init__(self, survivorship_config, entity_attributes,
                 entity_attributes_datatype, id_key):
        self.survivorship_config = survivorship_config
        self.entity_attributes = entity_attributes
        self.entity_attributes_datatype = entity_attributes_datatype
        self.id_key = id_key
        self.calls: list[list[dict]] = []
        FakeSurvivorshipEngine.instances.append(self)

    @classmethod
    def reset(cls):
        cls.instances = []

    def _priority(self) -> list[str]:
        for rule in self.survivorship_config or []:
            if rule.get("strategy") == "Source System":
                sr = rule.get("strategyRule")
                if isinstance(sr, list) and sr:
                    return list(sr)
        return []

    def apply_survivorship(self, unified_records):
        self.calls.append([dict(r) for r in unified_records])
        priority = self._priority()
        # Order contributors: priority sources first, then the rest as-given.
        ordered: list[dict] = []
        seen: set[int] = set()
        for src in priority:
            for r in unified_records:
                if r.get("source_path") == src and id(r) not in seen:
                    ordered.append(r)
                    seen.add(id(r))
        for r in unified_records:
            if id(r) not in seen:
                ordered.append(r)
                seen.add(id(r))

        record: dict[str, Any] = {}
        mappings: list[_AttributeSourceMapping] = []
        for attr in self.entity_attributes:
            if attr == "lakefusion_id":
                continue
            picked_value = None
            picked_source = None
            for r in ordered:
                v = r.get(attr)
                if v is not None and v != "":
                    picked_value = v
                    picked_source = r.get("source_path")
                    break
            record[attr] = picked_value
            if picked_value is not None:
                mappings.append(_AttributeSourceMapping(attr, str(picked_value),
                                                        picked_source or ""))
        # `attributes_combined` is treated as just another data column by the
        # notebook's master writer; pass through the first non-empty.
        for r in ordered:
            ac = r.get("attributes_combined")
            if ac:
                record["attributes_combined"] = ac
                break
        else:
            record["attributes_combined"] = ""
        return _SurvivorshipResult(record, mappings)


# Inject a fake `lakefusion_core_engine.survivorship` so the notebook's
# `from lakefusion_core_engine.survivorship import SurvivorshipEngine` works
# even when the real package isn't installed.
def _install_survivorship_module():
    pkg_name = "lakefusion_core_engine"
    sub_name = f"{pkg_name}.survivorship"
    if pkg_name not in sys.modules:
        sys.modules[pkg_name] = types.ModuleType(pkg_name)
    if sub_name not in sys.modules:
        m = types.ModuleType(sub_name)
        m.SurvivorshipEngine = FakeSurvivorshipEngine
        sys.modules[sub_name] = m
        setattr(sys.modules[pkg_name], "survivorship", m)
    else:
        # Make sure the *current* fake class is what gets imported.
        sys.modules[sub_name].SurvivorshipEngine = FakeSurvivorshipEngine


_install_survivorship_module()


# =========================================================================== #
# Notebook source loader
# =========================================================================== #
def _load_notebook_source() -> str:
    """Read the notebook source and strip Databricks-magic comment lines.

    The two lines that matter to the parser:
      * `# MAGIC %run ../../utils/execute_utils` — already a comment, no-op
        when the bare `setup_lakefusion_engine` and `logger` are pre-injected.
      * `# COMMAND ----------` — also already a comment.
    Both are left alone; we just return the raw text.
    """
    return NOTEBOOK_PATH.read_text()


def _split_at_phase2(source: str) -> tuple[str, str]:
    """Return (phase1_code, rest) so tests can execute helper definitions
    without running the full pipeline."""
    marker = "## Phase 2: Load, Validate, and Transform Each Source CDF"
    idx = source.index(marker)
    return source[:idx], source[idx:]


# =========================================================================== #
# Spark session fixture (session-scoped — expensive to create)
# =========================================================================== #
@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("warehouse")
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("test_incremental_load_to_unified")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


# =========================================================================== #
# Mock data fixtures + helpers
# =========================================================================== #
ENTITY = "customer"
PRIMARY_KEY = "customer_key"
ENTITY_ATTRIBUTES = [
    "lakefusion_id", "customer_key", "name", "email", "phone", "city",
]
ENTITY_ATTRS_DTYPE = {
    "customer_key": "string",
    "name": "string",
    "email": "string",
    "phone": "string",
    "city": "string",
}
MATCH_ATTRIBUTES = ["name", "email", "city"]


def _qualified(database: str, name: str) -> str:
    return f"spark_catalog.{database}.{name}"


def _make_databases(spark: SparkSession, db_token: str) -> tuple[str, str]:
    """Create unique per-test databases for silver + gold and return them."""
    silver = f"silver_{db_token}"
    gold = f"gold_{db_token}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS spark_catalog.{silver}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS spark_catalog.{gold}")
    return silver, gold


def _drop_databases(spark: SparkSession, *databases: str):
    for db in databases:
        try:
            spark.sql(f"DROP DATABASE IF EXISTS spark_catalog.{db} CASCADE")
        except Exception:
            pass


def _build_source_table(spark: SparkSession, name: str, schema: StructType,
                        initial_rows: list[dict]):
    """Create a CDF-enabled source table named (db.tbl) and seed it."""
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    empty = spark.createDataFrame([], schema=schema)
    (empty.write
     .format("delta")
     .option("delta.enableChangeDataFeed", "true")
     .saveAsTable(name))
    if initial_rows:
        df = spark.createDataFrame(initial_rows, schema=schema)
        df.write.format("delta").mode("append").saveAsTable(name)


def _build_unified_table(spark: SparkSession, name: str,
                         entity_attributes: list[str]) -> StructType:
    """Create the unified table with the schema the notebook MERGEs into."""
    fields = [
        StructField("surrogate_key", StringType(), False),
        StructField("source_id", StringType(), True),
        StructField("source_path", StringType(), True),
        StructField("record_status", StringType(), True),
        StructField("master_lakefusion_id", StringType(), True),
        StructField("attributes_combined", StringType(), True),
        StructField("search_results", StringType(), True),
        StructField("scoring_results", StringType(), True),
    ]
    for attr in entity_attributes:
        if attr == "lakefusion_id":
            continue
        fields.append(StructField(attr, StringType(), True))
    schema = StructType(fields)
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    empty = spark.createDataFrame([], schema=schema)
    (empty.write
     .format("delta")
     .option("delta.enableChangeDataFeed", "true")
     .saveAsTable(name))
    return schema


def _build_master_table(spark: SparkSession, name: str,
                        entity_attributes: list[str]) -> StructType:
    fields = [StructField("lakefusion_id", StringType(), False)]
    for attr in entity_attributes:
        if attr == "lakefusion_id":
            continue
        fields.append(StructField(attr, StringType(), True))
    fields.append(StructField("attributes_combined", StringType(), True))
    schema = StructType(fields)
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    empty = spark.createDataFrame([], schema=schema)
    empty.write.format("delta").saveAsTable(name)
    return schema


def _build_meta_info_table(spark: SparkSession, name: str):
    # `last_processed_version` is LongType to match what `spark.createDataFrame`
    # infers from Python ints inside `_write_version_metadata` — keeps the
    # MERGE happy without an explicit cast.
    schema = StructType([
        StructField("table_name", StringType(), False),
        StructField("last_processed_version", LongType(), True),
        StructField("last_processed_timestamp", TimestampType(), True),
        StructField("entity_name", StringType(), True),
    ])
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    empty = spark.createDataFrame([], schema=schema)
    empty.write.format("delta").saveAsTable(name)


def _build_merge_activities_table(spark: SparkSession, name: str):
    schema = StructType([
        StructField("master_id", StringType(), False),
        StructField("match_id", StringType(), True),
        StructField("source", StringType(), False),
        StructField("action_type", StringType(), False),
        StructField("version", IntegerType(), False),
        StructField("created_at", TimestampType(), False),
    ])
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    empty = spark.createDataFrame([], schema=schema)
    empty.write.format("delta").saveAsTable(name)


def _build_attr_version_sources_table(spark: SparkSession, name: str):
    attr_struct = StructType([
        StructField("attribute_name", StringType(), True),
        StructField("attribute_value", StringType(), True),
        StructField("source", StringType(), True),
    ])
    schema = StructType([
        StructField("lakefusion_id", StringType(), False),
        StructField("version", IntegerType(), False),
        StructField("attribute_source_mapping", ArrayType(attr_struct), True),
    ])
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    empty = spark.createDataFrame([], schema=schema)
    empty.write.format("delta").saveAsTable(name)


def _seed_unified(spark: SparkSession, name: str, rows: list[dict],
                  schema: StructType):
    if not rows:
        return
    df = spark.createDataFrame(rows, schema=schema)
    df.write.format("delta").mode("append").saveAsTable(name)


def _seed_master(spark: SparkSession, name: str, rows: list[dict],
                 schema: StructType):
    if not rows:
        return
    df = spark.createDataFrame(rows, schema=schema)
    df.write.format("delta").mode("append").saveAsTable(name)


# =========================================================================== #
# Notebook executor
# =========================================================================== #
def run_notebook(spark: SparkSession, *,
                 entity: str,
                 primary_key: str,
                 entity_attributes: list[str],
                 entity_attrs_dtype: dict,
                 attributes_mapping: list,
                 match_attributes: list,
                 dataset_tables: list[str],
                 dataset_objects: dict,
                 survivorship_config: list,
                 catalog_name: str,
                 silver_db: str,
                 gold_db: str,
                 logger: FakeLogger | None = None,
                 ) -> dict:
    """Execute the notebook end-to-end against the given Spark session.

    Returns the namespace dict the notebook ran in (so tests can read out
    `version_map`, `master_updates`, `merge_activity_rows`, etc.) plus
    `_status`, `_task_values_set`, and `_logger`.

    The notebook expects table names like
    `{catalog_name}.silver.{entity}_unified`. Because we run inside the local
    `spark_catalog`, we set the schema names to `silver_<token>` / `gold_<token>`
    and override entity to a name that interpolates correctly. To keep the
    notebook's f-string layout intact while still pointing at our temp
    schemas, we monkey-patch `silver` / `gold` literals in a thin pre-amble.
    """
    FakeSurvivorshipEngine.reset()
    log = logger or FakeLogger()
    task_values = FakeTaskValues({
        "entity": entity,
        "primary_key": primary_key,
        "entity_attributes": json.dumps(entity_attributes),
        "entity_attributes_datatype": json.dumps(entity_attrs_dtype),
        "attributes_mapping": json.dumps(attributes_mapping),
        "match_attributes": json.dumps(match_attributes),
        "dataset_tables": json.dumps(dataset_tables),
        "dataset_objects": json.dumps(dataset_objects),
        "default_survivorship_rules": json.dumps(survivorship_config),
        "catalog_name": catalog_name,
        "experiment_id": "",
    })
    dbutils = FakeDbutils(task_values)

    src = _load_notebook_source()

    # The notebook hardcodes `.silver.` and `.gold.` schema names. Rewrite them
    # to the per-test schemas so all `f"{catalog}.silver.{entity}_..."` strings
    # resolve to real tables in our local `spark_catalog`.
    src = src.replace(".silver.", f".{silver_db}.").replace(".gold.", f".{gold_db}.")

    g: dict[str, Any] = {
        "__name__": "incremental_load_under_test",
        "spark": spark,
        "dbutils": dbutils,
        "logger": log,
        # `setup_lakefusion_engine()` is called once after the magic %run; the
        # real version configures logging. A no-op is fine here.
        "setup_lakefusion_engine": lambda: None,
    }

    status = "SUCCESS"
    try:
        exec(compile(src, str(NOTEBOOK_PATH), "exec"), g)
    except _NotebookExit as exc:
        status = exc.status

    g["_status"] = status
    g["_task_values_set"] = dict(task_values._set)
    g["_logger"] = log
    g["_dbutils"] = dbutils
    return g


def load_notebook_helpers(spark: SparkSession, *,
                          attributes_mapping=None,
                          dataset_objects=None,
                          survivorship_config=None,
                          entity_attributes=None,
                          entity_attrs_dtype=None,
                          match_attributes=None,
                          dataset_tables=None,
                          primary_key="customer_key",
                          entity="customer",
                          catalog_name="spark_catalog",
                          silver_db="silver_helpers",
                          gold_db="gold_helpers"):
    """Execute *only* Phase 1 of the notebook so tests can poke at the helper
    functions and Phase-1 transforms (e.g. survivorship_config translation)
    without running the rest of the pipeline."""
    FakeSurvivorshipEngine.reset()
    log = FakeLogger()
    task_values = FakeTaskValues({
        "entity": entity,
        "primary_key": primary_key,
        "entity_attributes": json.dumps(entity_attributes or []),
        "entity_attributes_datatype": json.dumps(entity_attrs_dtype or {}),
        "attributes_mapping": json.dumps(attributes_mapping or []),
        "match_attributes": json.dumps(match_attributes or []),
        "dataset_tables": json.dumps(dataset_tables or []),
        "dataset_objects": json.dumps(dataset_objects or {}),
        "default_survivorship_rules": json.dumps(survivorship_config or []),
        "catalog_name": catalog_name,
        "experiment_id": "",
    })
    dbutils = FakeDbutils(task_values)

    src = _load_notebook_source()
    src = src.replace(".silver.", f".{silver_db}.").replace(".gold.", f".{gold_db}.")
    phase1, _ = _split_at_phase2(src)

    g: dict[str, Any] = {
        "__name__": "incremental_load_helpers",
        "spark": spark,
        "dbutils": dbutils,
        "logger": log,
        "setup_lakefusion_engine": lambda: None,
    }
    exec(compile(phase1, str(NOTEBOOK_PATH), "exec"), g)
    g["_logger"] = log
    return g


# =========================================================================== #
# Per-test scaffolding fixture
# =========================================================================== #
@pytest.fixture
def env(spark):
    """Provision per-test schemas + standard table names. Cleans up after."""
    token = uuid.uuid4().hex[:10]
    silver, gold = _make_databases(spark, token)

    source_table_a = _qualified(silver, f"src_a_{token}")
    source_table_b = _qualified(silver, f"src_b_{token}")
    unified_table = _qualified(silver, f"{ENTITY}_unified_prod")
    master_table = _qualified(gold, f"{ENTITY}_master_prod")
    meta_info_table = _qualified(silver, "table_meta_info")
    merge_activities_table = f"{master_table}_merge_activities"
    attr_version_sources_table = f"{master_table}_attribute_version_sources"

    yield types.SimpleNamespace(
        spark=spark,
        silver_db=silver,
        gold_db=gold,
        source_table_a=source_table_a,
        source_table_b=source_table_b,
        unified_table=unified_table,
        master_table=master_table,
        meta_info_table=meta_info_table,
        merge_activities_table=merge_activities_table,
        attr_version_sources_table=attr_version_sources_table,
    )

    _drop_databases(spark, silver, gold)


# Schema helpers used by many tests. Source tables look like a typical CDC
# upstream — surrogate `customer_key`, plus business columns that get
# remapped by `attributes_mapping`.
def _src_schema() -> StructType:
    # customer_key is *intentionally* nullable so the null-PK test can write
    # a row with key=None and exercise the notebook's PK quality check.
    return StructType([
        StructField("customer_key", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("customer_email", StringType(), True),
        StructField("customer_phone", StringType(), True),
        StructField("customer_city", StringType(), True),
    ])


def _attributes_mapping_for(source_a: str, source_b: str | None = None) -> list:
    """`{source_table: {entity_attr: source_col}}` — same structure
    Parse_Entity_Model_JSON delivers."""
    common = {
        "customer_key": "customer_key",
        "name": "customer_name",
        "email": "customer_email",
        "phone": "customer_phone",
        "city": "customer_city",
    }
    out = [{source_a: dict(common)}]
    if source_b:
        out.append({source_b: dict(common)})
    return out


def _dataset_objects_for(*tables: str) -> dict:
    return {tbl: {"id": f"id-of-{tbl}"} for tbl in tables}


def _survivorship_priority(*tables: str) -> list:
    return [{
        "attribute": "name",
        "strategy": "Source System",
        "strategyRule": list(tables),
        "ignoreNull": False,
    }]


def _seed_meta(spark, env, table: str, last_processed_version: int):
    df = spark.createDataFrame(
        [{"table_name": table,
          "last_processed_version": last_processed_version,
          "last_processed_timestamp": datetime.now(timezone.utc),
          "entity_name": ENTITY}],
        schema=StructType([
            StructField("table_name", StringType(), False),
            StructField("last_processed_version", LongType(), True),
            StructField("last_processed_timestamp", TimestampType(), True),
            StructField("entity_name", StringType(), True),
        ]),
    )
    df.write.format("delta").mode("append").saveAsTable(env.meta_info_table)


def _common_run(env, *, dataset_tables, survivorship_config=None,
                logger: FakeLogger | None = None):
    """Defaults that match the env's schemas/tables."""
    return run_notebook(
        env.spark,
        entity=ENTITY,
        primary_key=PRIMARY_KEY,
        entity_attributes=ENTITY_ATTRIBUTES,
        entity_attrs_dtype=ENTITY_ATTRS_DTYPE,
        attributes_mapping=_attributes_mapping_for(*dataset_tables),
        match_attributes=MATCH_ATTRIBUTES,
        dataset_tables=dataset_tables,
        dataset_objects=_dataset_objects_for(*dataset_tables),
        survivorship_config=(survivorship_config
                             if survivorship_config is not None
                             else _survivorship_priority(*dataset_tables)),
        catalog_name="spark_catalog",
        silver_db=env.silver_db,
        gold_db=env.gold_db,
        logger=logger,
    )


# =========================================================================== #
# Phase 1: Helpers
# =========================================================================== #
class TestPhase1Helpers:
    def test_get_mapping_for_table_inverts_entry(self, spark):
        g = load_notebook_helpers(spark, attributes_mapping=[
            {"db.silver.src_a": {
                "customer_key": "ck",
                "name": "nm",
            }}
        ])
        # `attributes_mapping` arrives as {entity_attr: source_col};
        # the helper inverts so source-col → entity-attr.
        assert g["get_mapping_for_table"]("db.silver.src_a") == {
            "ck": "customer_key",
            "nm": "name",
        }

    def test_get_mapping_for_table_unknown_returns_empty(self, spark):
        g = load_notebook_helpers(spark, attributes_mapping=[
            {"db.silver.src_a": {"customer_key": "ck"}}
        ])
        assert g["get_mapping_for_table"]("does.not.exist") == {}

    def test_spark_dtype_map_covers_common_types(self, spark):
        g = load_notebook_helpers(spark)
        m = g["SPARK_DTYPE_MAP"]
        # A few invariants we rely on in Phase 2.5 to pad missing attrs.
        assert m["bigint"] == "bigint"
        assert m["int"] == "int"
        assert m["text"] == "string"
        assert m["double precision"] == "double"
        assert m["decimal"] == "decimal(10,2)"

    def test_survivorship_config_translates_dataset_ids_to_paths(self, spark):
        # Rules arrive with dataset IDs in `strategyRule`; Phase 1 must swap
        # them out for source-path strings.
        g = load_notebook_helpers(
            spark,
            dataset_objects={
                "db.silver.src_a": {"id": "id-A"},
                "db.silver.src_b": {"id": "id-B"},
            },
            survivorship_config=[
                {"attribute": "name", "strategy": "Source System",
                 "strategyRule": ["id-A", "id-B"]},
                # A non-Source-System rule shouldn't be touched.
                {"attribute": "phone", "strategy": "Recency",
                 "strategyRule": "updated_at"},
            ],
        )
        rules = g["survivorship_config"]
        assert rules[0]["strategyRule"] == ["db.silver.src_a", "db.silver.src_b"]
        assert rules[1]["strategyRule"] == "updated_at"

    def test_survivorship_config_passes_through_source_paths_unchanged(self, spark):
        # If `strategyRule` already holds paths (no dataset_objects mapping),
        # it must remain untouched.
        g = load_notebook_helpers(
            spark,
            dataset_objects={
                "db.silver.src_a": {"id": "id-A"},
            },
            survivorship_config=[{
                "attribute": "name",
                "strategy": "Source System",
                "strategyRule": ["already.a.path", "id-A"],
            }],
        )
        # `id-A` translates; an unrecognised "already.a.path" stays.
        assert g["survivorship_config"][0]["strategyRule"] == [
            "already.a.path", "db.silver.src_a"
        ]

    def test_write_version_metadata_inserts_then_updates(self, env):
        g = load_notebook_helpers(
            env.spark,
            silver_db=env.silver_db,
            gold_db=env.gold_db,
            entity_attributes=ENTITY_ATTRIBUTES,
            entity_attrs_dtype=ENTITY_ATTRS_DTYPE,
        )
        _build_meta_info_table(env.spark, env.meta_info_table)

        # First call inserts.
        g["_write_version_metadata"]({env.source_table_a: 7})
        rows = (env.spark.read.table(env.meta_info_table)
                .filter(F.col("table_name") == env.source_table_a)
                .collect())
        assert len(rows) == 1
        assert rows[0]["last_processed_version"] == 7

        # Second call updates the existing row (no duplicate insert).
        g["_write_version_metadata"]({env.source_table_a: 11})
        rows = (env.spark.read.table(env.meta_info_table)
                .filter(F.col("table_name") == env.source_table_a)
                .collect())
        assert len(rows) == 1
        assert rows[0]["last_processed_version"] == 11

    def test_write_version_metadata_empty_map_is_noop(self, env):
        g = load_notebook_helpers(
            env.spark,
            silver_db=env.silver_db,
            gold_db=env.gold_db,
        )
        _build_meta_info_table(env.spark, env.meta_info_table)
        g["_write_version_metadata"]({})  # must not raise
        assert env.spark.read.table(env.meta_info_table).count() == 0


# =========================================================================== #
# End-to-end scaffolding (used by Phase 2-7 tests)
# =========================================================================== #
def _full_setup(env, *, sources: list[tuple[str, list[dict]]],
                unified_seed: list[dict] | None = None,
                master_seed: list[dict] | None = None,
                meta_seed: dict[str, int] | None = None):
    """Build all the Delta tables + initial seeds for an end-to-end run."""
    src_schema = _src_schema()
    for name, rows in sources:
        _build_source_table(env.spark, name, src_schema, rows)

    unified_schema = _build_unified_table(env.spark, env.unified_table,
                                          ENTITY_ATTRIBUTES)
    master_schema = _build_master_table(env.spark, env.master_table,
                                        ENTITY_ATTRIBUTES)
    _build_meta_info_table(env.spark, env.meta_info_table)
    _build_merge_activities_table(env.spark, env.merge_activities_table)
    _build_attr_version_sources_table(env.spark, env.attr_version_sources_table)

    if unified_seed:
        _seed_unified(env.spark, env.unified_table, unified_seed, unified_schema)
    if master_seed:
        _seed_master(env.spark, env.master_table, master_seed, master_schema)
    if meta_seed:
        for table, ver in meta_seed.items():
            _seed_meta(env.spark, env, table, ver)
    return unified_schema, master_schema


def _surrogate(spark, source_table: str, pk: str) -> str:
    """Mirror the notebook's surrogate-key formula so tests can predict it."""
    row = (spark.createDataFrame([{"k": pk}])
           .select(F.concat(
               F.lit("sk_"),
               F.md5(F.concat_ws("_", F.lit(source_table), F.col("k")))
           ).alias("sk"))
           .first())
    return row["sk"]


def _apply_source_change(spark, table: str, schema: StructType,
                         *, insert: list[dict] = (), update: list[dict] = (),
                         delete_keys: list[str] = ()):
    """Apply CDC-style changes to a Delta source table via a MERGE so the CDF
    records the right `_change_type` values."""
    updates = list(update)
    inserts = list(insert)
    deletes = list(delete_keys)

    # Inserts + updates go through a single MERGE on customer_key.
    if updates or inserts:
        rows = updates + inserts
        df = spark.createDataFrame(rows, schema=schema)
        (DeltaTable.forName(spark, table).alias("t")
         .merge(df.alias("s"), "t.customer_key = s.customer_key")
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())

    if deletes:
        ids = ", ".join(f"'{k}'" for k in deletes)
        DeltaTable.forName(spark, table).delete(f"customer_key IN ({ids})")


def _read_unified(env) -> list[dict]:
    return [r.asDict() for r in env.spark.read.table(env.unified_table).collect()]


def _read_master(env) -> list[dict]:
    return [r.asDict() for r in env.spark.read.table(env.master_table).collect()]


def _read_merge_activities(env) -> list[dict]:
    return [r.asDict() for r in
            env.spark.read.table(env.merge_activities_table).collect()]


def _read_attr_version_sources(env) -> list[dict]:
    return [r.asDict(recursive=True) for r in
            env.spark.read.table(env.attr_version_sources_table).collect()]


def _read_meta(env) -> dict[str, int]:
    return {r["table_name"]: r["last_processed_version"]
            for r in env.spark.read.table(env.meta_info_table).collect()}


# =========================================================================== #
# Phase 2: per-source CDF read, validation, transform
# =========================================================================== #
class TestPhase2Validation:
    def test_skip_when_no_new_versions(self, env):
        # Source has only the empty initial commit (v0). meta says we already
        # processed v0, so starting_version (1) > current_version (0) → skip.
        _full_setup(env,
                    sources=[(env.source_table_a, [])],
                    meta_seed={env.source_table_a: 0})
        out = _common_run(env, dataset_tables=[env.source_table_a])
        assert out["_status"] == "no_data"
        assert "SKIP: no new versions" in "\n".join(out["_logger"].lines("INFO"))

    def test_skip_when_no_attribute_mapping(self, env):
        _full_setup(env,
                    sources=[(env.source_table_a,
                              [{"customer_key": "C1", "customer_name": "Ada",
                                "customer_email": None, "customer_phone": None,
                                "customer_city": None}])])
        # Mapping deliberately empty.
        out = run_notebook(
            env.spark,
            entity=ENTITY, primary_key=PRIMARY_KEY,
            entity_attributes=ENTITY_ATTRIBUTES,
            entity_attrs_dtype=ENTITY_ATTRS_DTYPE,
            attributes_mapping=[],
            match_attributes=MATCH_ATTRIBUTES,
            dataset_tables=[env.source_table_a],
            dataset_objects={},
            survivorship_config=[],
            catalog_name="spark_catalog",
            silver_db=env.silver_db, gold_db=env.gold_db,
        )
        assert out["_status"] == "no_data"
        assert any("no attribute mapping" in m
                   for m in out["_logger"].lines("WARN"))

    def test_skip_on_schema_drift(self, env):
        # Drop the `customer_email` column the mapping expects to find.
        odd_schema = StructType([
            StructField("customer_key", StringType(), False),
            StructField("customer_name", StringType(), True),
            # customer_email intentionally missing
            StructField("customer_phone", StringType(), True),
            StructField("customer_city", StringType(), True),
        ])
        _build_source_table(env.spark, env.source_table_a, odd_schema,
                            [{"customer_key": "C1", "customer_name": "Ada",
                              "customer_phone": None, "customer_city": None}])
        _build_unified_table(env.spark, env.unified_table, ENTITY_ATTRIBUTES)
        _build_master_table(env.spark, env.master_table, ENTITY_ATTRIBUTES)
        _build_meta_info_table(env.spark, env.meta_info_table)
        _build_merge_activities_table(env.spark, env.merge_activities_table)
        _build_attr_version_sources_table(env.spark, env.attr_version_sources_table)

        out = _common_run(env, dataset_tables=[env.source_table_a])
        assert out["_status"] == "no_data"
        assert any("schema drift" in m for m in out["_logger"].lines("WARN"))
        assert env.source_table_a in out["failed_tables"]

    def test_fail_on_null_pk(self, env):
        # Two rows: one valid, one with a null PK.
        _full_setup(env, sources=[(env.source_table_a, [
            {"customer_key": "C1", "customer_name": "Ada",
             "customer_email": None, "customer_phone": None, "customer_city": None},
            {"customer_key": None, "customer_name": "Bad",
             "customer_email": None, "customer_phone": None, "customer_city": None},
        ])])
        out = _common_run(env, dataset_tables=[env.source_table_a])
        assert out["_status"] == "no_data"
        warns = "\n".join(out["_logger"].lines("WARN"))
        assert "null PKs" in warns
        assert env.source_table_a in out["failed_tables"]

    def test_fail_on_duplicate_pk(self, env):
        # Two rows sharing the same customer_key.
        _full_setup(env, sources=[(env.source_table_a, [
            {"customer_key": "C1", "customer_name": "Ada",
             "customer_email": None, "customer_phone": None, "customer_city": None},
            {"customer_key": "C1", "customer_name": "Ada-2",
             "customer_email": None, "customer_phone": None, "customer_city": None},
        ])])
        out = _common_run(env, dataset_tables=[env.source_table_a])
        assert out["_status"] == "no_data"
        warns = "\n".join(out["_logger"].lines("WARN"))
        assert "duplicate PKs" in warns
        assert env.source_table_a in out["failed_tables"]

    def test_skip_when_pk_not_in_mapping(self, env):
        _full_setup(env, sources=[(env.source_table_a, [
            {"customer_key": "C1", "customer_name": "Ada",
             "customer_email": None, "customer_phone": None, "customer_city": None},
        ])])
        # Mapping omits the primary key.
        out = run_notebook(
            env.spark,
            entity=ENTITY, primary_key=PRIMARY_KEY,
            entity_attributes=ENTITY_ATTRIBUTES,
            entity_attrs_dtype=ENTITY_ATTRS_DTYPE,
            attributes_mapping=[{env.source_table_a: {
                "name": "customer_name",
                "email": "customer_email",
            }}],
            match_attributes=MATCH_ATTRIBUTES,
            dataset_tables=[env.source_table_a],
            dataset_objects=_dataset_objects_for(env.source_table_a),
            survivorship_config=_survivorship_priority(env.source_table_a),
            catalog_name="spark_catalog",
            silver_db=env.silver_db, gold_db=env.gold_db,
        )
        assert out["_status"] == "no_data"
        assert any("primary key" in m for m in out["_logger"].lines("WARN"))


# =========================================================================== #
# Phase 3: union + MERGE into unified
# =========================================================================== #
class TestPhase3Merge:
    def test_insert_creates_active_unified_row(self, env):
        _full_setup(env, sources=[(env.source_table_a, [
            {"customer_key": "C1", "customer_name": "Ada Lovelace",
             "customer_email": "ada@example.com", "customer_phone": "555-0001",
             "customer_city": "London"},
        ])])
        out = _common_run(env, dataset_tables=[env.source_table_a])
        assert out["_status"] == "SUCCESS"

        rows = _read_unified(env)
        assert len(rows) == 1
        r = rows[0]
        assert r["record_status"] == "ACTIVE"
        assert r["master_lakefusion_id"] == ""  # net-new, no master yet
        assert r["name"] == "Ada Lovelace"
        assert r["email"] == "ada@example.com"
        assert r["source_id"] == "C1"
        assert r["source_path"] == env.source_table_a
        # `attributes_combined` concatenates match_attributes (name|email|city).
        assert r["attributes_combined"] == "Ada Lovelace | ada@example.com | London"
        assert r["surrogate_key"] == _surrogate(env.spark, env.source_table_a, "C1")

    def test_update_preserves_record_status_and_master_id(self, env):
        # Pre-existing MERGED unified row that's already linked to a master.
        sk = _surrogate(env.spark, env.source_table_a, "C1")
        unified_seed = [{
            "surrogate_key": sk,
            "source_id": "C1",
            "source_path": env.source_table_a,
            "record_status": "MERGED",
            "master_lakefusion_id": "M-1",
            "attributes_combined": "Old | old@x | London",
            "search_results": "",
            "scoring_results": "",
            "customer_key": "C1",
            "name": "Old Name",
            "email": "old@x.com",
            "phone": "555-old",
            "city": "London",
        }]
        master_seed = [{
            "lakefusion_id": "M-1",
            "customer_key": "C1",
            "name": "Old Name",
            "email": "old@x.com",
            "phone": "555-old",
            "city": "London",
            "attributes_combined": "Old | old@x | London",
        }]
        _full_setup(env,
                    sources=[(env.source_table_a, [
                        {"customer_key": "C1", "customer_name": "Old Name",
                         "customer_email": "old@x.com", "customer_phone": "555-old",
                         "customer_city": "London"},
                    ])],
                    unified_seed=unified_seed,
                    master_seed=master_seed,
                    meta_seed={env.source_table_a: 1})

        # Apply an update on the source.
        _apply_source_change(env.spark, env.source_table_a, _src_schema(),
                             update=[{"customer_key": "C1",
                                      "customer_name": "Ada Lovelace",
                                      "customer_email": "ada@example.com",
                                      "customer_phone": "555-new",
                                      "customer_city": "London"}])

        out = _common_run(env, dataset_tables=[env.source_table_a])
        assert out["_status"] == "SUCCESS"

        rows = _read_unified(env)
        assert len(rows) == 1
        r = rows[0]
        # Updated columns reflect new source values.
        assert r["name"] == "Ada Lovelace"
        assert r["email"] == "ada@example.com"
        # CRITICAL: record_status & master_lakefusion_id preserved.
        assert r["record_status"] == "MERGED"
        assert r["master_lakefusion_id"] == "M-1"

    def test_delete_marks_unified_as_deleted(self, env):
        sk = _surrogate(env.spark, env.source_table_a, "C1")
        unified_seed = [{
            "surrogate_key": sk, "source_id": "C1", "source_path": env.source_table_a,
            "record_status": "MERGED", "master_lakefusion_id": "M-1",
            "attributes_combined": "Ada | ada@x | London",
            "search_results": "", "scoring_results": "",
            "customer_key": "C1", "name": "Ada", "email": "ada@x.com",
            "phone": "555-1", "city": "London",
        }]
        master_seed = [{
            "lakefusion_id": "M-1", "customer_key": "C1", "name": "Ada",
            "email": "ada@x.com", "phone": "555-1", "city": "London",
            "attributes_combined": "Ada | ada@x | London",
        }]
        _full_setup(env,
                    sources=[(env.source_table_a, [
                        {"customer_key": "C1", "customer_name": "Ada",
                         "customer_email": "ada@x.com", "customer_phone": "555-1",
                         "customer_city": "London"},
                    ])],
                    unified_seed=unified_seed,
                    master_seed=master_seed,
                    meta_seed={env.source_table_a: 1})

        _apply_source_change(env.spark, env.source_table_a, _src_schema(),
                             delete_keys=["C1"])

        out = _common_run(env, dataset_tables=[env.source_table_a])
        assert out["_status"] == "SUCCESS"

        r = _read_unified(env)[0]
        assert r["record_status"] == "DELETED"
        assert r["master_lakefusion_id"] == "M-1"  # link preserved for survivorship
        # `name` is unchanged: only record_status is set on a delete.
        assert r["name"] == "Ada"

    def test_unmatched_delete_is_silent_noop(self, env):
        # Source records a delete for a key that was never in unified.
        _full_setup(env, sources=[(env.source_table_a, [
            {"customer_key": "C1", "customer_name": "Ada",
             "customer_email": None, "customer_phone": None, "customer_city": None},
        ])], meta_seed={env.source_table_a: 1})

        _apply_source_change(env.spark, env.source_table_a, _src_schema(),
                             delete_keys=["C1"])

        out = _common_run(env, dataset_tables=[env.source_table_a])
        # The notebook may exit MERGE_NOOP if the merge produced no version.
        assert out["_status"] in {"SUCCESS", "merge_noop"}
        # No unified row inserted for the delete-only change.
        assert _read_unified(env) == []

    def test_delete_then_insert_collapses_to_single_change(self, env):
        # Pre-existing MERGED unified row.
        sk = _surrogate(env.spark, env.source_table_a, "C1")
        unified_seed = [{
            "surrogate_key": sk, "source_id": "C1", "source_path": env.source_table_a,
            "record_status": "MERGED", "master_lakefusion_id": "M-1",
            "attributes_combined": "Old | old@x | London",
            "search_results": "", "scoring_results": "",
            "customer_key": "C1", "name": "Old", "email": "old@x.com",
            "phone": "555-old", "city": "London",
        }]
        master_seed = [{
            "lakefusion_id": "M-1", "customer_key": "C1", "name": "Old",
            "email": "old@x.com", "phone": "555-old", "city": "London",
            "attributes_combined": "Old | old@x | London",
        }]
        _full_setup(env,
                    sources=[(env.source_table_a, [
                        {"customer_key": "C1", "customer_name": "Old",
                         "customer_email": "old@x.com", "customer_phone": "555-old",
                         "customer_city": "London"},
                    ])],
                    unified_seed=unified_seed,
                    master_seed=master_seed,
                    meta_seed={env.source_table_a: 1})

        # Two CDF events in this window: delete then re-insert. Last-change-wins
        # collapse should treat this as a single update_postimage / insert and
        # the existing MERGED row must remain MERGED.
        _apply_source_change(env.spark, env.source_table_a, _src_schema(),
                             delete_keys=["C1"])
        _apply_source_change(env.spark, env.source_table_a, _src_schema(),
                             insert=[{"customer_key": "C1",
                                      "customer_name": "Ada New",
                                      "customer_email": "ada@new.com",
                                      "customer_phone": "555-new",
                                      "customer_city": "Paris"}])

        out = _common_run(env, dataset_tables=[env.source_table_a])
        assert out["_status"] == "SUCCESS"

        r = _read_unified(env)[0]
        assert r["record_status"] == "MERGED"
        assert r["master_lakefusion_id"] == "M-1"
        assert r["name"] == "Ada New"
        assert r["city"] == "Paris"

    def test_no_data_path_exits_cleanly(self, env):
        # No source tables produced any data → notebook exits "no_data".
        _full_setup(env, sources=[(env.source_table_a, [])],
                    meta_seed={env.source_table_a: 0})
        out = _common_run(env, dataset_tables=[env.source_table_a])
        assert out["_status"] == "no_data"
        assert out["_task_values_set"].get("status") == "NO_DATA"


# =========================================================================== #
# Phase 4: affected-master identification
# =========================================================================== #
class TestPhase4AffectedMasters:
    def test_only_merged_or_deleted_with_master_id_collected(self, env):
        # Three pre-existing rows: one MERGED, one ACTIVE-no-master, one with
        # empty master_id. After update, only the MERGED one should be
        # collected as affected.
        sk1 = _surrogate(env.spark, env.source_table_a, "C1")
        sk2 = _surrogate(env.spark, env.source_table_a, "C2")
        sk3 = _surrogate(env.spark, env.source_table_a, "C3")
        unified_seed = [
            {"surrogate_key": sk1, "source_id": "C1", "source_path": env.source_table_a,
             "record_status": "MERGED", "master_lakefusion_id": "M-1",
             "attributes_combined": "", "search_results": "", "scoring_results": "",
             "customer_key": "C1", "name": "Old1", "email": None,
             "phone": None, "city": None},
            {"surrogate_key": sk2, "source_id": "C2", "source_path": env.source_table_a,
             "record_status": "ACTIVE", "master_lakefusion_id": "",
             "attributes_combined": "", "search_results": "", "scoring_results": "",
             "customer_key": "C2", "name": "Old2", "email": None,
             "phone": None, "city": None},
            {"surrogate_key": sk3, "source_id": "C3", "source_path": env.source_table_a,
             "record_status": "ACTIVE", "master_lakefusion_id": None,
             "attributes_combined": "", "search_results": "", "scoring_results": "",
             "customer_key": "C3", "name": "Old3", "email": None,
             "phone": None, "city": None},
        ]
        master_seed = [{
            "lakefusion_id": "M-1", "customer_key": "C1", "name": "Old1",
            "email": None, "phone": None, "city": None, "attributes_combined": "",
        }]
        _full_setup(env,
                    sources=[(env.source_table_a, [
                        {"customer_key": k, "customer_name": f"Old{i+1}",
                         "customer_email": None, "customer_phone": None,
                         "customer_city": None}
                        for i, k in enumerate(["C1", "C2", "C3"])
                    ])],
                    unified_seed=unified_seed,
                    master_seed=master_seed,
                    meta_seed={env.source_table_a: 1})

        # Update all three on the source.
        _apply_source_change(env.spark, env.source_table_a, _src_schema(),
                             update=[
                                 {"customer_key": "C1", "customer_name": "New1",
                                  "customer_email": None, "customer_phone": None,
                                  "customer_city": None},
                                 {"customer_key": "C2", "customer_name": "New2",
                                  "customer_email": None, "customer_phone": None,
                                  "customer_city": None},
                                 {"customer_key": "C3", "customer_name": "New3",
                                  "customer_email": None, "customer_phone": None,
                                  "customer_city": None},
                             ])
        out = _common_run(env, dataset_tables=[env.source_table_a])
        assert out["_status"] == "SUCCESS"
        # Only M-1 should be in affected_master_ids.
        assert out["affected_master_ids"] == ["M-1"]
        # Survivorship engine called exactly once with M-1's contributors.
        assert len(FakeSurvivorshipEngine.instances) == 1
        engine = FakeSurvivorshipEngine.instances[0]
        assert len(engine.calls) == 1
        contribs = engine.calls[0]
        assert {c["surrogate_key"] for c in contribs} == {sk1}


# =========================================================================== #
# Phase 5: survivorship-driven master updates + deletes + search clearing
# =========================================================================== #
class TestPhase5Survivorship:
    def test_master_updated_when_contributors_remain(self, env):
        sk = _surrogate(env.spark, env.source_table_a, "C1")
        unified_seed = [{
            "surrogate_key": sk, "source_id": "C1", "source_path": env.source_table_a,
            "record_status": "MERGED", "master_lakefusion_id": "M-1",
            "attributes_combined": "Old | old@x | London",
            "search_results": "", "scoring_results": "",
            "customer_key": "C1", "name": "Old", "email": "old@x.com",
            "phone": "555-old", "city": "London",
        }]
        master_seed = [{
            "lakefusion_id": "M-1", "customer_key": "C1", "name": "Old",
            "email": "old@x.com", "phone": "555-old", "city": "London",
            "attributes_combined": "Old | old@x | London",
        }]
        _full_setup(env,
                    sources=[(env.source_table_a, [
                        {"customer_key": "C1", "customer_name": "Old",
                         "customer_email": "old@x.com", "customer_phone": "555-old",
                         "customer_city": "London"},
                    ])],
                    unified_seed=unified_seed,
                    master_seed=master_seed,
                    meta_seed={env.source_table_a: 1})

        _apply_source_change(env.spark, env.source_table_a, _src_schema(),
                             update=[{"customer_key": "C1",
                                      "customer_name": "Ada New",
                                      "customer_email": "ada@new.com",
                                      "customer_phone": "555-new",
                                      "customer_city": "Paris"}])

        out = _common_run(env, dataset_tables=[env.source_table_a])
        assert out["_status"] == "SUCCESS"

        masters = _read_master(env)
        assert len(masters) == 1
        m = masters[0]
        # FakeSurvivorshipEngine picks the first contributor's value.
        assert m["lakefusion_id"] == "M-1"
        assert m["name"] == "Ada New"
        assert m["email"] == "ada@new.com"
        assert m["city"] == "Paris"
        assert out["masters_to_delete"] == []
        assert out["_task_values_set"]["masters_updated"] == 1
        assert out["_task_values_set"]["masters_deleted"] == 0

    def test_master_deleted_when_all_contributors_gone(self, env):
        sk = _surrogate(env.spark, env.source_table_a, "C1")
        unified_seed = [{
            "surrogate_key": sk, "source_id": "C1", "source_path": env.source_table_a,
            "record_status": "MERGED", "master_lakefusion_id": "M-1",
            "attributes_combined": "Ada | ada@x | London",
            "search_results": "", "scoring_results": "",
            "customer_key": "C1", "name": "Ada", "email": "ada@x.com",
            "phone": "555-1", "city": "London",
        }]
        master_seed = [{
            "lakefusion_id": "M-1", "customer_key": "C1", "name": "Ada",
            "email": "ada@x.com", "phone": "555-1", "city": "London",
            "attributes_combined": "Ada | ada@x | London",
        }]
        _full_setup(env,
                    sources=[(env.source_table_a, [
                        {"customer_key": "C1", "customer_name": "Ada",
                         "customer_email": "ada@x.com", "customer_phone": "555-1",
                         "customer_city": "London"},
                    ])],
                    unified_seed=unified_seed,
                    master_seed=master_seed,
                    meta_seed={env.source_table_a: 1})

        _apply_source_change(env.spark, env.source_table_a, _src_schema(),
                             delete_keys=["C1"])

        out = _common_run(env, dataset_tables=[env.source_table_a])
        assert out["_status"] == "SUCCESS"
        assert out["masters_to_delete"] == ["M-1"]
        assert _read_master(env) == []
        assert out["_task_values_set"]["masters_deleted"] == 1
        # Survivorship engine should NOT have been invoked for a master with
        # zero remaining contributors.
        engine = FakeSurvivorshipEngine.instances[0]
        assert engine.calls == []

    def test_master_with_remaining_sibling_contributor_survives(self, env):
        # Two sources, both contribute to the same master M-1. Source A
        # deletes its contributor; source B's contributor still exists →
        # master stays, gets re-survived from B alone.
        sk_a = _surrogate(env.spark, env.source_table_a, "C1")
        sk_b = _surrogate(env.spark, env.source_table_b, "B1")

        unified_seed = [
            {"surrogate_key": sk_a, "source_id": "C1",
             "source_path": env.source_table_a,
             "record_status": "MERGED", "master_lakefusion_id": "M-1",
             "attributes_combined": "AdaA | a@a | London",
             "search_results": "", "scoring_results": "",
             "customer_key": "C1", "name": "AdaA", "email": "a@a.com",
             "phone": "555-A", "city": "London"},
            {"surrogate_key": sk_b, "source_id": "B1",
             "source_path": env.source_table_b,
             "record_status": "MERGED", "master_lakefusion_id": "M-1",
             "attributes_combined": "AdaB | a@b | Paris",
             "search_results": "", "scoring_results": "",
             "customer_key": "B1", "name": "AdaB", "email": "a@b.com",
             "phone": "555-B", "city": "Paris"},
        ]
        master_seed = [{
            "lakefusion_id": "M-1", "customer_key": "C1", "name": "AdaA",
            "email": "a@a.com", "phone": "555-A", "city": "London",
            "attributes_combined": "AdaA | a@a | London",
        }]
        _full_setup(env,
                    sources=[
                        (env.source_table_a, [{
                            "customer_key": "C1", "customer_name": "AdaA",
                            "customer_email": "a@a.com", "customer_phone": "555-A",
                            "customer_city": "London"}]),
                        (env.source_table_b, [{
                            "customer_key": "B1", "customer_name": "AdaB",
                            "customer_email": "a@b.com", "customer_phone": "555-B",
                            "customer_city": "Paris"}]),
                    ],
                    unified_seed=unified_seed,
                    master_seed=master_seed,
                    meta_seed={env.source_table_a: 1, env.source_table_b: 1})

        _apply_source_change(env.spark, env.source_table_a, _src_schema(),
                             delete_keys=["C1"])

        # Set survivorship priority so source_b wins.
        out = _common_run(env,
                          dataset_tables=[env.source_table_a, env.source_table_b],
                          survivorship_config=_survivorship_priority(
                              env.source_table_b, env.source_table_a))
        assert out["_status"] == "SUCCESS"
        assert out["masters_to_delete"] == []
        m = _read_master(env)[0]
        assert m["lakefusion_id"] == "M-1"
        assert m["name"] == "AdaB"  # B's value wins under Source-System priority
        assert m["city"] == "Paris"

    def test_search_results_cleared_for_active_records_referencing_master(self, env):
        # Pre-existing ACTIVE record whose `search_results` mentions M-1.
        # When M-1's contributor changes and master M-1 is updated, the
        # ACTIVE record's stale candidate list must be wiped.
        sk_active = _surrogate(env.spark, env.source_table_a, "ACT")
        sk_merged = _surrogate(env.spark, env.source_table_a, "C1")
        unified_seed = [
            {"surrogate_key": sk_active, "source_id": "ACT",
             "source_path": env.source_table_a,
             "record_status": "ACTIVE", "master_lakefusion_id": "",
             "attributes_combined": "Active | act@x | NYC",
             "search_results": "candidates: M-1, M-2",
             "scoring_results": "scored: M-1=0.8",
             "customer_key": "ACT", "name": "Active", "email": "act@x.com",
             "phone": None, "city": "NYC"},
            {"surrogate_key": sk_merged, "source_id": "C1",
             "source_path": env.source_table_a,
             "record_status": "MERGED", "master_lakefusion_id": "M-1",
             "attributes_combined": "Old | old@x | London",
             "search_results": "", "scoring_results": "",
             "customer_key": "C1", "name": "Old", "email": "old@x.com",
             "phone": "555-old", "city": "London"},
        ]
        master_seed = [{
            "lakefusion_id": "M-1", "customer_key": "C1", "name": "Old",
            "email": "old@x.com", "phone": "555-old", "city": "London",
            "attributes_combined": "Old | old@x | London",
        }]
        _full_setup(env,
                    sources=[(env.source_table_a, [
                        {"customer_key": "ACT", "customer_name": "Active",
                         "customer_email": "act@x.com", "customer_phone": None,
                         "customer_city": "NYC"},
                        {"customer_key": "C1", "customer_name": "Old",
                         "customer_email": "old@x.com", "customer_phone": "555-old",
                         "customer_city": "London"},
                    ])],
                    unified_seed=unified_seed,
                    master_seed=master_seed,
                    meta_seed={env.source_table_a: 1})

        _apply_source_change(env.spark, env.source_table_a, _src_schema(),
                             update=[{"customer_key": "C1",
                                      "customer_name": "Ada New",
                                      "customer_email": "ada@new.com",
                                      "customer_phone": "555-new",
                                      "customer_city": "Paris"}])

        out = _common_run(env, dataset_tables=[env.source_table_a])
        assert out["_status"] == "SUCCESS"

        # ACTIVE row referencing M-1 should now have empty search/scoring.
        active = next(r for r in _read_unified(env) if r["surrogate_key"] == sk_active)
        assert active["search_results"] == ""
        assert active["scoring_results"] == ""


# =========================================================================== #
# Phase 6: audit table writes + idempotency
# =========================================================================== #
class TestPhase6Audit:
    def test_merge_activities_and_attr_versions_written(self, env):
        sk = _surrogate(env.spark, env.source_table_a, "C1")
        unified_seed = [{
            "surrogate_key": sk, "source_id": "C1", "source_path": env.source_table_a,
            "record_status": "MERGED", "master_lakefusion_id": "M-1",
            "attributes_combined": "Old | old@x | London",
            "search_results": "", "scoring_results": "",
            "customer_key": "C1", "name": "Old", "email": "old@x.com",
            "phone": "555-old", "city": "London",
        }]
        master_seed = [{
            "lakefusion_id": "M-1", "customer_key": "C1", "name": "Old",
            "email": "old@x.com", "phone": "555-old", "city": "London",
            "attributes_combined": "Old | old@x | London",
        }]
        _full_setup(env,
                    sources=[(env.source_table_a, [
                        {"customer_key": "C1", "customer_name": "Old",
                         "customer_email": "old@x.com", "customer_phone": "555-old",
                         "customer_city": "London"},
                    ])],
                    unified_seed=unified_seed,
                    master_seed=master_seed,
                    meta_seed={env.source_table_a: 1})

        _apply_source_change(env.spark, env.source_table_a, _src_schema(),
                             update=[{"customer_key": "C1",
                                      "customer_name": "Ada New",
                                      "customer_email": "ada@new.com",
                                      "customer_phone": "555-new",
                                      "customer_city": "Paris"}])
        out = _common_run(env, dataset_tables=[env.source_table_a])
        assert out["_status"] == "SUCCESS"

        activities = _read_merge_activities(env)
        assert len(activities) == 1
        a = activities[0]
        assert a["master_id"] == "M-1"
        assert a["match_id"] == sk
        assert a["action_type"] == "SOURCE_UPDATE"
        assert a["source"] == env.source_table_a
        assert isinstance(a["version"], int)
        assert a["created_at"] is not None

        avs = _read_attr_version_sources(env)
        assert len(avs) == 1
        v = avs[0]
        assert v["lakefusion_id"] == "M-1"
        assert v["version"] == a["version"]
        # The attribute_source_mapping should include picked attributes.
        attrs_recorded = {m["attribute_name"] for m in v["attribute_source_mapping"]}
        # Mock engine emits mappings for any non-null attribute.
        assert "name" in attrs_recorded
        assert "email" in attrs_recorded

    def test_audit_merge_idempotent_within_same_master_version(self, env):
        # Phase 6's audit MERGE uses `whenNotMatchedInsertAll` on a composite
        # key that includes `version` (the master_table commit version). For a
        # *given* master version, replaying the audit write must not
        # duplicate. This test exercises that guarantee directly: run the
        # full pipeline once, then replay just the merge_activities MERGE
        # with the same input — row count stays put.
        sk = _surrogate(env.spark, env.source_table_a, "C1")
        unified_seed = [{
            "surrogate_key": sk, "source_id": "C1", "source_path": env.source_table_a,
            "record_status": "MERGED", "master_lakefusion_id": "M-1",
            "attributes_combined": "Old | old@x | London",
            "search_results": "", "scoring_results": "",
            "customer_key": "C1", "name": "Old", "email": "old@x.com",
            "phone": "555-old", "city": "London",
        }]
        master_seed = [{
            "lakefusion_id": "M-1", "customer_key": "C1", "name": "Old",
            "email": "old@x.com", "phone": "555-old", "city": "London",
            "attributes_combined": "Old | old@x | London",
        }]
        _full_setup(env,
                    sources=[(env.source_table_a, [
                        {"customer_key": "C1", "customer_name": "Old",
                         "customer_email": "old@x.com", "customer_phone": "555-old",
                         "customer_city": "London"},
                    ])],
                    unified_seed=unified_seed,
                    master_seed=master_seed,
                    meta_seed={env.source_table_a: 1})

        _apply_source_change(env.spark, env.source_table_a, _src_schema(),
                             update=[{"customer_key": "C1",
                                      "customer_name": "Ada",
                                      "customer_email": "ada@new.com",
                                      "customer_phone": "555-new",
                                      "customer_city": "Paris"}])

        out1 = _common_run(env, dataset_tables=[env.source_table_a])
        assert out1["_status"] == "SUCCESS"
        first_rows = _read_merge_activities(env)
        assert len(first_rows) == 1
        recorded = first_rows[0]

        # Replay the audit MERGE with the same input — same master version, so
        # the composite key matches and `whenNotMatchedInsertAll` is a no-op.
        replay_schema = StructType([
            StructField("master_id", StringType(), False),
            StructField("match_id", StringType(), True),
            StructField("source", StringType(), False),
            StructField("action_type", StringType(), False),
            StructField("version", IntegerType(), False),
            StructField("created_at", TimestampType(), False),
        ])
        replay_df = env.spark.createDataFrame([{
            "master_id": recorded["master_id"],
            "match_id": recorded["match_id"],
            "source": recorded["source"],
            "action_type": recorded["action_type"],
            "version": recorded["version"],
            # different timestamp — must NOT cause a duplicate.
            "created_at": datetime.now(timezone.utc),
        }], schema=replay_schema)
        (DeltaTable.forName(env.spark, env.merge_activities_table).alias("target")
         .merge(replay_df.alias("source"),
                """target.master_id   = source.master_id
                   AND target.match_id    = source.match_id
                   AND target.source      = source.source
                   AND target.version     = source.version
                   AND target.action_type = source.action_type""")
         .whenNotMatchedInsertAll()
         .execute())
        assert len(_read_merge_activities(env)) == 1

    def test_audit_history_accumulates_per_master_version_on_pipeline_retry(self, env):
        # When the whole pipeline is replayed (e.g. after a mid-run failure
        # left the version pointers untouched), the master MERGE bumps the
        # master_table version. The audit MERGE's composite key includes
        # that version, so the retry adds a *new* per-version row rather than
        # deduplicating with the prior run. This is intentional — per-master
        # -version audit history is the design.
        sk = _surrogate(env.spark, env.source_table_a, "C1")
        unified_seed = [{
            "surrogate_key": sk, "source_id": "C1", "source_path": env.source_table_a,
            "record_status": "MERGED", "master_lakefusion_id": "M-1",
            "attributes_combined": "Old | old@x | London",
            "search_results": "", "scoring_results": "",
            "customer_key": "C1", "name": "Old", "email": "old@x.com",
            "phone": "555-old", "city": "London",
        }]
        master_seed = [{
            "lakefusion_id": "M-1", "customer_key": "C1", "name": "Old",
            "email": "old@x.com", "phone": "555-old", "city": "London",
            "attributes_combined": "Old | old@x | London",
        }]
        _full_setup(env,
                    sources=[(env.source_table_a, [
                        {"customer_key": "C1", "customer_name": "Old",
                         "customer_email": "old@x.com", "customer_phone": "555-old",
                         "customer_city": "London"},
                    ])],
                    unified_seed=unified_seed,
                    master_seed=master_seed,
                    meta_seed={env.source_table_a: 1})

        _apply_source_change(env.spark, env.source_table_a, _src_schema(),
                             update=[{"customer_key": "C1",
                                      "customer_name": "Ada",
                                      "customer_email": "ada@new.com",
                                      "customer_phone": "555-new",
                                      "customer_city": "Paris"}])

        out1 = _common_run(env, dataset_tables=[env.source_table_a])
        assert out1["_status"] == "SUCCESS"
        first_rows = _read_merge_activities(env)
        assert len(first_rows) == 1
        v1 = first_rows[0]["version"]

        # Reset meta so the second run replays the same source CDF window.
        env.spark.sql(
            f"DELETE FROM {env.meta_info_table} "
            f"WHERE table_name = '{env.source_table_a}'"
        )
        _seed_meta(env.spark, env, env.source_table_a, 1)

        out2 = _common_run(env, dataset_tables=[env.source_table_a])
        assert out2["_status"] == "SUCCESS"

        rows = _read_merge_activities(env)
        assert len(rows) == 2
        versions = sorted(r["version"] for r in rows)
        # Same logical activity, two different master_table versions.
        assert versions[0] == v1
        assert versions[1] > v1
        # The master_id / match_id / source / action_type tuple is identical
        # across the two rows — only `version` differs.
        for r in rows:
            assert r["master_id"] == "M-1"
            assert r["match_id"] == sk
            assert r["source"] == env.source_table_a
            assert r["action_type"] == "SOURCE_UPDATE"

        # attribute_version_sources follows the same per-master-version model.
        avs_rows = _read_attr_version_sources(env)
        assert len(avs_rows) == 2
        assert sorted(r["version"] for r in avs_rows) == versions


# =========================================================================== #
# Phase 7: version metadata advancement + final task values
# =========================================================================== #
class TestPhase7VersionMetadata:
    def test_successful_table_advances_version(self, env):
        _full_setup(env, sources=[(env.source_table_a, [
            {"customer_key": "C1", "customer_name": "Ada",
             "customer_email": "ada@x.com", "customer_phone": "555-1",
             "customer_city": "London"},
        ])])
        out = _common_run(env, dataset_tables=[env.source_table_a])
        assert out["_status"] == "SUCCESS"
        meta = _read_meta(env)
        assert env.source_table_a in meta
        # Source got an empty initial commit (v0) + one append (v1).
        assert meta[env.source_table_a] == 1

    def test_failed_table_version_not_advanced(self, env):
        # Source A is healthy. Source B has duplicate PKs — should be
        # rejected, and only A's version should advance.
        _full_setup(env, sources=[
            (env.source_table_a, [
                {"customer_key": "A1", "customer_name": "Ada",
                 "customer_email": None, "customer_phone": None, "customer_city": None},
            ]),
            (env.source_table_b, [
                {"customer_key": "B1", "customer_name": "Bob",
                 "customer_email": None, "customer_phone": None, "customer_city": None},
                {"customer_key": "B1", "customer_name": "Bob-dupe",
                 "customer_email": None, "customer_phone": None, "customer_city": None},
            ]),
        ])
        out = _common_run(env,
                          dataset_tables=[env.source_table_a, env.source_table_b])
        assert out["_status"] == "SUCCESS"
        meta = _read_meta(env)
        assert env.source_table_a in meta
        assert env.source_table_b not in meta
        assert env.source_table_b in out["failed_tables"]

    def test_task_values_reflect_run_summary(self, env):
        _full_setup(env, sources=[(env.source_table_a, [
            {"customer_key": "C1", "customer_name": "Ada",
             "customer_email": None, "customer_phone": None, "customer_city": None},
        ])])
        out = _common_run(env, dataset_tables=[env.source_table_a])
        tv = out["_task_values_set"]
        assert tv["status"] == "SUCCESS"
        assert tv["masters_updated"] == 0      # net-new insert, no master yet
        assert tv["masters_deleted"] == 0
        # processed_tables / failed_tables are JSON-encoded.
        assert json.loads(tv["processed_tables"]) == [env.source_table_a]
        assert json.loads(tv["failed_tables"]) == []


# =========================================================================== #
# End-to-end happy path across multiple sources
# =========================================================================== #
class TestEndToEnd:
    def test_two_sources_merge_into_unified_in_one_run(self, env):
        _full_setup(env, sources=[
            (env.source_table_a, [
                {"customer_key": "A1", "customer_name": "Ada A",
                 "customer_email": "ada@a.com", "customer_phone": "111",
                 "customer_city": "London"},
            ]),
            (env.source_table_b, [
                {"customer_key": "B1", "customer_name": "Bob B",
                 "customer_email": "bob@b.com", "customer_phone": "222",
                 "customer_city": "Paris"},
            ]),
        ])
        out = _common_run(env,
                          dataset_tables=[env.source_table_a, env.source_table_b])
        assert out["_status"] == "SUCCESS"
        rows = _read_unified(env)
        assert len(rows) == 2
        by_source_id = {r["source_id"]: r for r in rows}
        assert by_source_id["A1"]["source_path"] == env.source_table_a
        assert by_source_id["B1"]["source_path"] == env.source_table_b
        assert all(r["record_status"] == "ACTIVE" for r in rows)
        assert all(r["master_lakefusion_id"] == "" for r in rows)
        # version_map captured per source.
        assert set(out["version_map"].keys()) == {env.source_table_a, env.source_table_b}

    def test_mixed_run_insert_update_delete_and_merge_noop_behavior(self, env):
        # Pre-existing MERGED contributor for C1, MERGED ACTIVE record for C2.
        sk1 = _surrogate(env.spark, env.source_table_a, "C1")
        sk2 = _surrogate(env.spark, env.source_table_a, "C2")
        unified_seed = [
            {"surrogate_key": sk1, "source_id": "C1",
             "source_path": env.source_table_a,
             "record_status": "MERGED", "master_lakefusion_id": "M-1",
             "attributes_combined": "Ada | ada@x | London",
             "search_results": "", "scoring_results": "",
             "customer_key": "C1", "name": "Ada", "email": "ada@x.com",
             "phone": "555-1", "city": "London"},
            {"surrogate_key": sk2, "source_id": "C2",
             "source_path": env.source_table_a,
             "record_status": "ACTIVE", "master_lakefusion_id": "",
             "attributes_combined": "Bob | bob@x | Paris",
             "search_results": "", "scoring_results": "",
             "customer_key": "C2", "name": "Bob", "email": "bob@x.com",
             "phone": "555-2", "city": "Paris"},
        ]
        master_seed = [{
            "lakefusion_id": "M-1", "customer_key": "C1", "name": "Ada",
            "email": "ada@x.com", "phone": "555-1", "city": "London",
            "attributes_combined": "Ada | ada@x | London",
        }]
        _full_setup(env,
                    sources=[(env.source_table_a, [
                        {"customer_key": "C1", "customer_name": "Ada",
                         "customer_email": "ada@x.com", "customer_phone": "555-1",
                         "customer_city": "London"},
                        {"customer_key": "C2", "customer_name": "Bob",
                         "customer_email": "bob@x.com", "customer_phone": "555-2",
                         "customer_city": "Paris"},
                    ])],
                    unified_seed=unified_seed,
                    master_seed=master_seed,
                    meta_seed={env.source_table_a: 1})

        # Apply: update C1, insert C3, delete C2.
        _apply_source_change(env.spark, env.source_table_a, _src_schema(),
                             update=[{"customer_key": "C1",
                                      "customer_name": "Ada Lovelace",
                                      "customer_email": "ada@new.com",
                                      "customer_phone": "555-new",
                                      "customer_city": "London"}],
                             insert=[{"customer_key": "C3",
                                      "customer_name": "Charlie",
                                      "customer_email": "c@x.com",
                                      "customer_phone": "555-3",
                                      "customer_city": "Berlin"}],
                             delete_keys=["C2"])

        out = _common_run(env, dataset_tables=[env.source_table_a])
        assert out["_status"] == "SUCCESS"

        rows = {r["source_id"]: r for r in _read_unified(env)}
        # C1 updated, MERGED preserved.
        assert rows["C1"]["record_status"] == "MERGED"
        assert rows["C1"]["master_lakefusion_id"] == "M-1"
        assert rows["C1"]["name"] == "Ada Lovelace"
        # C2 deleted (had no master, so no master-side action).
        assert rows["C2"]["record_status"] == "DELETED"
        # C3 inserted as ACTIVE.
        assert rows["C3"]["record_status"] == "ACTIVE"
        assert rows["C3"]["master_lakefusion_id"] == ""

        # Master M-1 was updated (C1 still has a contributor).
        master = _read_master(env)[0]
        assert master["lakefusion_id"] == "M-1"
        assert master["name"] == "Ada Lovelace"
