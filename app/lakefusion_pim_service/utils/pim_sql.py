"""Raw-SQL foundation for the PIM service (SCRUM-1929 Phase 6).

On Databricks (``DATA_DB_TYPE=lakebase``) the PIM pipeline materializes each
pim_* table as a Delta-synced Lakebase table named
``gold."{entity}_{table}_prod_synced"`` (entity-prefixed, shared ``gold`` schema,
per-entity Postgres database). The fixed-``__tablename__`` ORM cannot reach those
names, so the service builds them at runtime — exactly as the reference-entity
service does. On local dev (``DATA_DB_TYPE=postgresql``) the same SQL runs against
plain ``gold."pim_*"`` tables created by ``init_local_pim``; the schema is
identical, only the table-name shape differs. ``pim_tbl`` is the single place that
resolves the right name for the active environment.

This module is the single place that:
  * builds the correct physical table name (``pim_tbl``) — NEVER hardcode a bare
    ``pim_*`` name in raw SQL, or the change-log triggers won't fire,
  * runs raw SQL over the per-entity SQLAlchemy session (``fetch_all`` / etc.),
  * generates the values the ORM used to auto-apply (UUID ids, created_at,
    updated_at) and builds INSERT/UPDATE statements with them,
  * enforces the CheckConstraints the synced tables don't carry (data_type,
    value ``source``).

Writes MUST go through the read/write Lakebase connection (the per-entity engine
in ``app_db.py`` already uses ``read_write_dns``), and MUST target the
``_prod_synced`` name so the ``zz_changelog`` AFTER triggers capture the edit for
Delta reconciliation.

Phase 6.0 is purely additive — nothing imports this yet. Services migrate onto it
in Phases 6.1+.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.lakefusion_pim_service.config import data_db_type
from app.lakefusion_pim_service.utils.app_db import derive_db_name

# ── Constants ────────────────────────────────────────────────────────────────

# All PIM tables live in this schema in BOTH environments (Lakebase and local
# Postgres). Keeping the schema identical means only the table NAME differs by
# environment — the schema component never does. Per-entity isolation is the
# per-entity Postgres DATABASE, not the schema.
PIM_SCHEMA = "gold"

# Allowed values for the CheckConstraints that the Delta-synced tables do not
# enforce (the ORM models declared them; synced tables don't carry them).
ATTR_DATA_TYPES = {"TEXT", "NUMBER", "BOOLEAN", "DATE", "SELECT", "MULTISELECT", "REFERENCE"}
VALUE_SOURCES = {"INHERITED", "USER_SET", "PIPELINE", "IMPORT"}


# ── NULL-coercion for synced reads ────────────────────────────────────────────
# The Delta/synced tables are created with ALL columns nullable and do NOT apply
# the SQLAlchemy model defaults (Seed_PIM_Reference_Data forces nullable on write).
# So a column a writer didn't set comes back NULL — which 500s a non-nullable
# response field. coerce_row fills the known NOT-NULL columns of a pim_* table with
# their ORM-declared default when the stored value is NULL. Values verified against
# the model defaults (note: several default to True/0, not False — do NOT blanket-False).
NOT_NULL_DEFAULTS = {
    "pim_language_ref":          {"is_active": True},
    "pim_unit_ref":              {"is_active": True},
    "pim_taxonomy_node":         {"level": 0, "display_order": 0, "is_active": True},
    "pim_attribute_option":      {"display_order": 0, "is_active": True},
    "pim_attribute_definition":  {"is_localizable": False, "is_system": False,
                                  "is_identifier": False, "is_label": False,
                                  "level": "ALL", "scope": "specifications",
                                  "group": "General", "display_order": 0},
    "pim_specification_config":  {"is_required": False, "inherit_to_children": True,
                                  "override_allowed": True, "display_order": 0},
    "pim_resolved_specification": {"is_required": False, "display_order": 0},
    "pim_entity_tier":           {"level": 0, "is_leaf": False},
    "pim_entity":                {"active": True, "promoted": False,
                                  "status": "Draft"},
    "pim_value_text":     {"source": "USER_SET", "version": 1, "locale": ""},
    "pim_value_number":   {"source": "USER_SET", "version": 1, "locale": ""},
    "pim_value_boolean":  {"source": "USER_SET", "version": 1, "locale": ""},
    "pim_value_date":     {"source": "USER_SET", "version": 1, "locale": ""},
    "pim_value_select":   {"source": "USER_SET", "version": 1, "locale": ""},
    "pim_value_multiselect": {"source": "USER_SET", "version": 1, "locale": ""},
    "pim_value_reference": {"source": "USER_SET", "version": 1, "locale": ""},
}


def coerce_row(table: str, row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Fill NULLs in a fetched pim_* row with the column's ORM default.

    Synced tables don't enforce NOT NULL / model defaults, so a writer that omits a
    column leaves it NULL — which breaks non-nullable response fields. This applies
    the verified default ONLY when the stored value is None (an explicit value is
    never overwritten). No-op for tables not in NOT_NULL_DEFAULTS.
    """
    if not row:
        return row
    defaults = NOT_NULL_DEFAULTS.get(table)
    if not defaults:
        return row
    for col, default in defaults.items():
        if col in row and row[col] is None:
            row[col] = default
    return row


def coerce_rows(table: str, rows):
    """coerce_row applied to a list of rows (in place); returns the list."""
    if rows:
        for r in rows:
            coerce_row(table, r)
    return rows


# ── Table-name builder ───────────────────────────────────────────────────────

def pim_tbl(entity_name: str, table: str) -> str:
    """Return the fully-qualified physical table name for raw SQL.

    The SCHEMA is always ``gold``; only the table NAME shape differs by
    environment (``DATA_DB_TYPE``):

      * **lakebase** (Databricks dev/stage/prod) — the pipeline materializes each
        pim_* table as an entity-prefixed Delta-synced table::

            pim_tbl("Test LS Prod", "pim_entity")
                -> 'gold."test_ls_prod_pim_entity_prod_synced"'

        The entity token comes from ``derive_db_name`` (the same sanitizer the
        engine uses to pick the per-entity database), so the name matches what
        ``Create_PIM_Tables`` created, and the ``_prod_synced`` suffix is what the
        ``zz_changelog`` AFTER triggers watch for Delta reconciliation.

      * **postgresql** (local dev) — plain per-table names created by the local
        ``create_all`` helper (see ``init_local_pim``); no entity prefix, no
        ``_prod_synced`` suffix, no sync/triggers::

            pim_tbl("Test LS Prod", "pim_entity") -> 'gold."pim_entity"'

    The schema is always fully qualified in the SQL string (mirrors the
    reference-entity pattern); do NOT rely on the connection ``search_path``.

    Args:
        entity_name: the raw entity name OR the already-sanitized token (the
            sanitizer is idempotent, so either is safe). Ignored for local
            Postgres, where tables are not entity-prefixed.
        table: the logical pim_* table name, e.g. "pim_entity", "pim_value_text".
    """
    if not table.startswith("pim_"):
        raise ValueError(f"pim_tbl expects a pim_* table name, got {table!r}")
    if data_db_type == "lakebase":
        entity = derive_db_name(entity_name)
        physical = f"{entity}_{table}_prod_synced"
    else:
        # local Postgres: plain table name, same `gold` schema
        physical = table
    return f'{PIM_SCHEMA}."{physical}"'


# ── Value generators (what the ORM used to auto-apply) ───────────────────────

def new_uuid() -> str:
    """Generate a string UUID PK (mirrors the ORM ``default=_uuid``)."""
    return str(uuid.uuid4())


def utcnow() -> datetime:
    """Current UTC timestamp (mirrors ``default=datetime.utcnow``)."""
    return datetime.utcnow()


# ── Raw-SQL execution wrappers ───────────────────────────────────────────────
#
# Thin helpers over session.execute(text()) bound to the per-entity engine
# (obtained via Depends(get_data_db) in routes). Params are always passed as a
# bound dict — never string-interpolate user values into SQL. Only the table
# NAME (from pim_tbl) is interpolated, and it is derived from a sanitized entity
# token + a fixed table literal, so it is safe.

def fetch_all(db: Session, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Run a SELECT and return all rows as a list of dicts."""
    result = db.execute(text(sql), params or {})
    return [dict(row) for row in result.mappings().all()]


def fetch_one(db: Session, sql: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """Run a SELECT and return the first row as a dict, or None."""
    result = db.execute(text(sql), params or {})
    row = result.mappings().first()
    return dict(row) if row is not None else None


def fetch_scalar(db: Session, sql: str, params: Optional[Dict[str, Any]] = None) -> Any:
    """Run a SELECT and return the first column of the first row (or None)."""
    result = db.execute(text(sql), params or {})
    return result.scalar()


def execute(db: Session, sql: str, params: Optional[Dict[str, Any]] = None) -> int:
    """Run an INSERT/UPDATE/DELETE. Returns affected rowcount. Does NOT commit —
    the caller controls the transaction boundary (so multi-table writes stay
    atomic)."""
    result = db.execute(text(sql), params or {})
    return result.rowcount


def bulk_insert(db: Session, entity_name: str, table: str, rows: list,
                page_size: int = 1000) -> int:
    """Batched INSERT of many fully-formed row dicts into a pim_* table — the
    reference-entity bulk pattern (psycopg2 ``execute_values``: a few multi-row
    statements instead of one round-trip per row). Essential on Lakebase, where
    per-row round-trips + the change-log trigger make row-wise inserts pathological.

    Every dict in ``rows`` MUST have the SAME keys (the column set); callers supply
    id/timestamps explicitly (no auto-injection here). Does NOT commit. Uses the
    session's underlying psycopg2 cursor so it runs inside the same transaction.
    Returns the number of rows inserted.
    """
    if not rows:
        return 0
    from psycopg2.extras import execute_values

    cols = list(rows[0].keys())
    fq = pim_tbl(entity_name, table)
    col_sql = ", ".join(f'"{c}"' for c in cols)
    sql = f'INSERT INTO {fq} ({col_sql}) VALUES %s'
    tuples = [tuple(r[c] for c in cols) for r in rows]

    # Reach the raw psycopg2 cursor through the SQLAlchemy connection so the batch
    # runs in the caller's transaction (commit stays with the caller).
    raw = db.connection().connection  # DBAPI (psycopg2) connection
    with raw.cursor() as cur:
        execute_values(cur, sql, tuples, page_size=page_size)
    return len(rows)


# ── INSERT / UPDATE builders ─────────────────────────────────────────────────

def build_insert(
    entity_name: str,
    table: str,
    values: Dict[str, Any],
    *,
    auto_id: bool = True,
    auto_created_at: bool = True,
    auto_updated_at: bool = True,
    returning: Optional[Sequence[str]] = None,
) -> tuple[str, Dict[str, Any]]:
    """Build an INSERT statement + bound params for a synced pim_* table.

    Auto-injects the values the ORM used to apply, unless the caller already
    supplied them or disables them:
      * ``id`` = new_uuid()        (skip for pim_language_ref / pim_unit_ref —
                                     pass auto_id=False, those PKs are caller-set)
      * ``created_at`` = utcnow()
      * ``updated_at`` = utcnow()  (tables without this column: pass
                                     auto_updated_at=False — e.g. pim_language_ref,
                                     pim_unit_ref, pim_resolved_specification)

    Returns (sql, params). The caller runs it via ``execute`` and controls commit.
    """
    row = dict(values)
    if auto_id and "id" not in row:
        row["id"] = new_uuid()
    now = utcnow()
    if auto_created_at and "created_at" not in row:
        row["created_at"] = now
    if auto_updated_at and "updated_at" not in row:
        row["updated_at"] = now

    cols = list(row.keys())
    col_list = ", ".join(f'"{c}"' for c in cols)
    placeholders = ", ".join(f":{c}" for c in cols)
    sql = f'INSERT INTO {pim_tbl(entity_name, table)} ({col_list}) VALUES ({placeholders})'
    if returning:
        sql += " RETURNING " + ", ".join(f'"{c}"' for c in returning)
    return sql, row


def build_update(
    entity_name: str,
    table: str,
    set_values: Dict[str, Any],
    where: str,
    where_params: Dict[str, Any],
    *,
    auto_updated_at: bool = True,
) -> tuple[str, Dict[str, Any]]:
    """Build an UPDATE statement + params for a synced pim_* table.

    Auto-sets ``updated_at = utcnow()`` (mirrors ORM ``onupdate``) unless the
    table has no updated_at column (pass auto_updated_at=False).

    ``where`` is a raw SQL predicate using bound params, e.g. '"id" = :id'.
    ``where_params`` supplies those bindings. SET-value param names are prefixed
    with ``set_`` to avoid colliding with where params.
    """
    sets = dict(set_values)
    if auto_updated_at and "updated_at" not in sets:
        sets["updated_at"] = utcnow()

    set_clause = ", ".join(f'"{c}" = :set_{c}' for c in sets)
    params: Dict[str, Any] = {f"set_{c}": v for c, v in sets.items()}
    params.update(where_params)
    sql = f'UPDATE {pim_tbl(entity_name, table)} SET {set_clause} WHERE {where}'
    return sql, params


# ── CheckConstraint validators (synced tables don't enforce these) ───────────

def validate_data_type(data_type: str) -> str:
    """Enforce chk_pim_attr_type. Returns the value; raises ValueError if invalid."""
    if data_type not in ATTR_DATA_TYPES:
        raise ValueError(
            f"Invalid attribute data_type {data_type!r}; must be one of {sorted(ATTR_DATA_TYPES)}"
        )
    return data_type


def validate_value_source(source: str) -> str:
    """Enforce chk_pim_v*_source. Returns the value; raises ValueError if invalid."""
    if source not in VALUE_SOURCES:
        raise ValueError(
            f"Invalid value source {source!r}; must be one of {sorted(VALUE_SOURCES)}"
        )
    return source
