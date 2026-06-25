"""Standalone unit tests for pim_sql pure logic (SCRUM-1929 Phase 6.0).

Runs without a DB or the app.config chain. Re-defines the small pure helpers
inline where importing pim_sql would pull app_db -> app.config. The goal is to
lock the table-name convention and the INSERT/UPDATE/validator contracts.

Run: python app/utils/test_pim_sql.py
"""
import re
import sys


# ── replicate the pure pieces (avoid app.config import chain) ────────────────
PIM_SCHEMA = "gold"
ATTR_DATA_TYPES = {"TEXT", "NUMBER", "BOOLEAN", "DATE", "SELECT", "MULTISELECT", "REFERENCE"}
VALUE_SOURCES = {"INHERITED", "USER_SET", "PIPELINE", "IMPORT"}


def derive_db_name(name, dep_env=""):
    return re.sub(r"[^a-z0-9]", "_", name.lower()).strip("_")


def pim_tbl(entity_name, table, data_db_type="lakebase"):
    # Mirrors app.utils.pim_sql.pim_tbl: same `gold` schema in both envs,
    # only the table-name shape differs by DATA_DB_TYPE.
    if not table.startswith("pim_"):
        raise ValueError(table)
    if data_db_type == "lakebase":
        physical = f"{derive_db_name(entity_name)}_{table}_prod_synced"
    else:
        physical = table
    return f'{PIM_SCHEMA}."{physical}"'


def build_insert(entity, table, values, auto_id=True, auto_created_at=True,
                 auto_updated_at=True, returning=None):
    row = dict(values)
    if auto_id and "id" not in row:
        row["id"] = "GEN_UUID"
    if auto_created_at and "created_at" not in row:
        row["created_at"] = "GEN_TS"
    if auto_updated_at and "updated_at" not in row:
        row["updated_at"] = "GEN_TS"
    cols = list(row.keys())
    col_list = ", ".join(f'"{c}"' for c in cols)
    placeholders = ", ".join(f":{c}" for c in cols)
    sql = f"INSERT INTO {pim_tbl(entity, table)} ({col_list}) VALUES ({placeholders})"
    if returning:
        sql += " RETURNING " + ", ".join(f'"{c}"' for c in returning)
    return sql, row


def build_update(entity, table, set_values, where, where_params, auto_updated_at=True):
    sets = dict(set_values)
    if auto_updated_at and "updated_at" not in sets:
        sets["updated_at"] = "GEN_TS"
    set_clause = ", ".join(f'"{c}" = :set_{c}' for c in sets)
    params = {f"set_{c}": v for c, v in sets.items()}
    params.update(where_params)
    return f"UPDATE {pim_tbl(entity, table)} SET {set_clause} WHERE {where}", params


def validate_data_type(dt):
    if dt not in ATTR_DATA_TYPES:
        raise ValueError(dt)
    return dt


def validate_value_source(s):
    if s not in VALUE_SOURCES:
        raise ValueError(s)
    return s


# ── tests ────────────────────────────────────────────────────────────────────
def run():
    # 1. name builder + idempotent sanitize (lakebase: entity-prefixed + _prod_synced)
    assert pim_tbl("Test LS Prod", "pim_entity") == 'gold."test_ls_prod_pim_entity_prod_synced"'
    assert pim_tbl("test_ls_prod", "pim_entity") == pim_tbl("Test LS Prod", "pim_entity")
    assert pim_tbl("e", "pim_value_text") == 'gold."e_pim_value_text_prod_synced"'

    # 1b. local Postgres: same `gold` schema, plain table name (no prefix/suffix)
    assert pim_tbl("Test LS Prod", "pim_entity", data_db_type="postgresql") == 'gold."pim_entity"'
    assert pim_tbl("e", "pim_value_text", data_db_type="postgresql") == 'gold."pim_value_text"'

    # 2. non-pim table name rejected
    try:
        pim_tbl("e", "entity"); assert False, "should reject"
    except ValueError:
        pass

    # 3. build_insert auto-injects id/created_at/updated_at
    sql, params = build_insert("e", "pim_entity", {"status": "Draft"})
    assert "id" in params and "created_at" in params and "updated_at" in params
    assert 'INSERT INTO gold."e_pim_entity_prod_synced"' in sql
    assert ":status" in sql and ":id" in sql

    # 4. build_insert respects caller-supplied id + disabled auto fields
    sql2, p2 = build_insert("e", "pim_language_ref", {"id": "en", "language_name": "English"},
                            auto_id=False, auto_updated_at=False)
    assert p2["id"] == "en" and "updated_at" not in p2 and "created_at" in p2

    # 5. build_insert RETURNING
    sql3, _ = build_insert("e", "pim_entity", {"status": "Draft"}, returning=["id"])
    assert sql3.rstrip().endswith('RETURNING "id"')

    # 6. build_update auto-sets updated_at, namespaces set params
    usql, uparams = build_update("e", "pim_entity", {"status": "Active"},
                                 where='"id" = :id', where_params={"id": "x"})
    assert '"status" = :set_status' in usql and '"updated_at" = :set_updated_at' in usql
    assert uparams["set_status"] == "Active" and uparams["id"] == "x"
    assert "UPDATE gold.\"e_pim_entity_prod_synced\" SET" in usql

    # 7. build_update with no updated_at column
    usql2, up2 = build_update("e", "pim_unit_ref", {"category": "weight"},
                              where='"name" = :name', where_params={"name": "kg"},
                              auto_updated_at=False)
    assert "updated_at" not in usql2

    # 8. validators
    assert validate_data_type("TEXT") == "TEXT"
    assert validate_value_source("USER_SET") == "USER_SET"
    for bad in ("text", "STRING", ""):
        try:
            validate_data_type(bad); assert False
        except ValueError:
            pass
    try:
        validate_value_source("MANUAL"); assert False
    except ValueError:
        pass

    print("pim_sql pure-logic tests: ALL PASS (8 groups)")


if __name__ == "__main__":
    try:
        run()
    except AssertionError as e:
        print(f"FAIL: {e}")
        sys.exit(1)
