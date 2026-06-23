from decimal import Decimal
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from app.lakefusion_pim_service.utils.app_db import get_data_db, token_required_wrapper
from app.lakefusion_pim_service.utils import pim_sql
from lakefusion_utility.models.pim import (
    PimLanguageRefCreate, PimLanguageRefResponse, PimLanguageRefUpdate,
    PimUnitRefCreate, PimUnitRefResponse, PimUnitRefUpdate,
)
from lakefusion_utility.utils.app_db import db_commit_auto_rollback
from lakefusion_utility.utils.logging_utils import get_logger
from typing import List

app_logger = get_logger(__name__)

pim_reference_router = APIRouter(tags=["PIM Reference"], prefix='/reference')

# SCRUM-1929 Phase 6.1 — converted from ORM to raw SQL against the Delta-synced
# tables gold."{entity}_pim_language_ref_prod_synced" / "..._pim_unit_ref_...".
# pim_language_ref & pim_unit_ref have a caller-set PK (id / name) and NO
# updated_at column, so inserts pass auto_id=False / auto_updated_at=False.
# These tables are seed-only today (no change-log trigger); writes here are kept
# for parity but will only sync to Delta once triggers are added (PRD §Phase 6
# challenge #8).

LANG_TBL = "pim_language_ref"
UNIT_TBL = "pim_unit_ref"


# ---------------------------------------------------------------------------
# LANGUAGES
# ---------------------------------------------------------------------------

@pim_reference_router.get("/languages", response_model=List[PimLanguageRefResponse])
def list_languages(
    entity_name: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    t = pim_sql.pim_tbl(entity_name, LANG_TBL)
    return pim_sql.fetch_all(
        db, f'SELECT * FROM {t} WHERE "is_active" = TRUE ORDER BY "id"'
    )


@pim_reference_router.post("/languages", response_model=PimLanguageRefResponse)
def create_language(
    entity_name: str,
    data: PimLanguageRefCreate,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    t = pim_sql.pim_tbl(entity_name, LANG_TBL)
    existing = pim_sql.fetch_one(db, f'SELECT "id" FROM {t} WHERE "id" = :id', {"id": data.id})
    if existing:
        raise HTTPException(status_code=409, detail=f"Language '{data.id}' already exists.")
    sql, params = pim_sql.build_insert(
        entity_name, LANG_TBL,
        {"id": data.id, "language_name": data.language_name, "is_active": data.is_active or True},
        auto_id=False, auto_updated_at=False,
    )
    pim_sql.execute(db, sql, params)
    db_commit_auto_rollback(db=db)
    return pim_sql.fetch_one(db, f'SELECT * FROM {t} WHERE "id" = :id', {"id": data.id})


@pim_reference_router.put("/languages/{lang_id}", response_model=PimLanguageRefResponse)
def update_language(
    entity_name: str,
    lang_id: str,
    data: PimLanguageRefUpdate,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    t = pim_sql.pim_tbl(entity_name, LANG_TBL)
    existing = pim_sql.fetch_one(db, f'SELECT "id" FROM {t} WHERE "id" = :id', {"id": lang_id})
    if not existing:
        raise HTTPException(status_code=404, detail="Language not found.")
    set_values = {}
    if data.language_name is not None:
        set_values["language_name"] = data.language_name
    if data.is_active is not None:
        set_values["is_active"] = data.is_active
    if set_values:
        sql, params = pim_sql.build_update(
            entity_name, LANG_TBL, set_values,
            where='"id" = :id', where_params={"id": lang_id},
            auto_updated_at=False,
        )
        pim_sql.execute(db, sql, params)
        db_commit_auto_rollback(db=db)
    return pim_sql.fetch_one(db, f'SELECT * FROM {t} WHERE "id" = :id', {"id": lang_id})


@pim_reference_router.delete("/languages/{lang_id}")
def delete_language(
    entity_name: str,
    lang_id: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    t = pim_sql.pim_tbl(entity_name, LANG_TBL)
    existing = pim_sql.fetch_one(db, f'SELECT "id" FROM {t} WHERE "id" = :id', {"id": lang_id})
    if not existing:
        raise HTTPException(status_code=404, detail="Language not found.")
    sql, params = pim_sql.build_update(
        entity_name, LANG_TBL, {"is_active": False},
        where='"id" = :id', where_params={"id": lang_id}, auto_updated_at=False,
    )
    pim_sql.execute(db, sql, params)
    db_commit_auto_rollback(db=db)
    return {"message": f"Language '{lang_id}' soft-deleted."}


# ---------------------------------------------------------------------------
# UNITS
# ---------------------------------------------------------------------------

_DEFAULT_UNITS = [
    {"name": "kg", "category": "weight", "description": "Kilogram", "base_unit": "kg", "to_base_factor": 1.0},
    {"name": "g", "category": "weight", "description": "Gram", "base_unit": "kg", "to_base_factor": 0.001},
    {"name": "lbs", "category": "weight", "description": "Pound", "base_unit": "kg", "to_base_factor": 0.453592},
    {"name": "oz", "category": "weight", "description": "Ounce", "base_unit": "kg", "to_base_factor": 0.0283495},
    {"name": "m", "category": "length", "description": "Meter", "base_unit": "m", "to_base_factor": 1.0},
    {"name": "cm", "category": "length", "description": "Centimeter", "base_unit": "m", "to_base_factor": 0.01},
    {"name": "mm", "category": "length", "description": "Millimeter", "base_unit": "m", "to_base_factor": 0.001},
    {"name": "in", "category": "length", "description": "Inch", "base_unit": "m", "to_base_factor": 0.0254},
    {"name": "ft", "category": "length", "description": "Foot", "base_unit": "m", "to_base_factor": 0.3048},
    {"name": "L", "category": "volume", "description": "Liter", "base_unit": "L", "to_base_factor": 1.0},
    {"name": "mL", "category": "volume", "description": "Milliliter", "base_unit": "L", "to_base_factor": 0.001},
    {"name": "gal", "category": "volume", "description": "US Gallon", "base_unit": "L", "to_base_factor": 3.78541},
    {"name": "fl oz", "category": "volume", "description": "US Fluid Ounce", "base_unit": "L", "to_base_factor": 0.0295735},
]


@pim_reference_router.get("/units", response_model=List[PimUnitRefResponse])
def list_units(
    entity_name: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    t = pim_sql.pim_tbl(entity_name, UNIT_TBL)
    # Auto-seed default units if table is empty (entities initialized before unit seeding).
    count = pim_sql.fetch_scalar(db, f'SELECT COUNT(*) FROM {t}')
    if not count:
        for unit in _DEFAULT_UNITS:
            sql, params = pim_sql.build_insert(
                entity_name, UNIT_TBL, dict(unit), auto_id=False, auto_updated_at=False,
            )
            pim_sql.execute(db, sql, params)
        db_commit_auto_rollback(db=db)
        app_logger.info(f"Auto-seeded {len(_DEFAULT_UNITS)} default measurement units")
    return pim_sql.fetch_all(
        db, f'SELECT * FROM {t} WHERE "is_active" = TRUE ORDER BY "name"'
    )


@pim_reference_router.get("/units/convert")
def convert_unit(
    entity_name: str,
    value: float = Query(..., description="Value to convert"),
    from_unit: str = Query(..., description="Source unit name"),
    to_unit: str = Query(..., description="Target unit name"),
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    """Convert a value between two units in the same category."""
    t = pim_sql.pim_tbl(entity_name, UNIT_TBL)
    source = pim_sql.fetch_one(
        db, f'SELECT * FROM {t} WHERE "name" = :n AND "is_active" = TRUE', {"n": from_unit}
    )
    if not source:
        raise HTTPException(status_code=404, detail=f"Source unit '{from_unit}' not found.")

    target = pim_sql.fetch_one(
        db, f'SELECT * FROM {t} WHERE "name" = :n AND "is_active" = TRUE', {"n": to_unit}
    )
    if not target:
        raise HTTPException(status_code=404, detail=f"Target unit '{to_unit}' not found.")

    if source["category"] != target["category"]:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot convert between different categories: '{source['category']}' and '{target['category']}'.",
        )

    if not source["to_base_factor"] or not target["to_base_factor"]:
        raise HTTPException(
            status_code=400,
            detail=f"Conversion factors not configured for '{from_unit}' and/or '{to_unit}'. Set base_unit and to_base_factor.",
        )

    # Convert: source_value → base → target
    base_value = Decimal(str(value)) * Decimal(str(source["to_base_factor"]))
    converted = float(base_value / Decimal(str(target["to_base_factor"])))

    return {
        "from_unit": from_unit,
        "to_unit": to_unit,
        "from_value": value,
        "to_value": round(converted, 6),
        "category": source["category"],
    }


@pim_reference_router.post("/units", response_model=PimUnitRefResponse)
def create_unit(
    entity_name: str,
    data: PimUnitRefCreate,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    t = pim_sql.pim_tbl(entity_name, UNIT_TBL)
    existing = pim_sql.fetch_one(db, f'SELECT "name" FROM {t} WHERE "name" = :n', {"n": data.name})
    if existing:
        raise HTTPException(status_code=409, detail=f"Unit '{data.name}' already exists.")
    sql, params = pim_sql.build_insert(
        entity_name, UNIT_TBL,
        {
            "name": data.name, "category": data.category, "description": data.description,
            "base_unit": data.base_unit, "to_base_factor": data.to_base_factor,
            "is_active": data.is_active or True,
        },
        auto_id=False, auto_updated_at=False,
    )
    pim_sql.execute(db, sql, params)
    db_commit_auto_rollback(db=db)
    return pim_sql.fetch_one(db, f'SELECT * FROM {t} WHERE "name" = :n', {"n": data.name})


@pim_reference_router.put("/units/{unit_name}", response_model=PimUnitRefResponse)
def update_unit(
    entity_name: str,
    unit_name: str,
    data: PimUnitRefUpdate,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    t = pim_sql.pim_tbl(entity_name, UNIT_TBL)
    existing = pim_sql.fetch_one(db, f'SELECT "name" FROM {t} WHERE "name" = :n', {"n": unit_name})
    if not existing:
        raise HTTPException(status_code=404, detail="Unit not found.")
    set_values = {}
    if data.category is not None:
        set_values["category"] = data.category
    if data.description is not None:
        set_values["description"] = data.description
    if data.base_unit is not None:
        set_values["base_unit"] = data.base_unit
    if data.to_base_factor is not None:
        set_values["to_base_factor"] = data.to_base_factor
    if data.is_active is not None:
        set_values["is_active"] = data.is_active
    if set_values:
        sql, params = pim_sql.build_update(
            entity_name, UNIT_TBL, set_values,
            where='"name" = :n', where_params={"n": unit_name}, auto_updated_at=False,
        )
        pim_sql.execute(db, sql, params)
        db_commit_auto_rollback(db=db)
    return pim_sql.fetch_one(db, f'SELECT * FROM {t} WHERE "name" = :n', {"n": unit_name})


@pim_reference_router.delete("/units/{unit_name}")
def delete_unit(
    entity_name: str,
    unit_name: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    t = pim_sql.pim_tbl(entity_name, UNIT_TBL)
    existing = pim_sql.fetch_one(db, f'SELECT "name" FROM {t} WHERE "name" = :n', {"n": unit_name})
    if not existing:
        raise HTTPException(status_code=404, detail="Unit not found.")
    sql, params = pim_sql.build_update(
        entity_name, UNIT_TBL, {"is_active": False},
        where='"name" = :n', where_params={"n": unit_name}, auto_updated_at=False,
    )
    pim_sql.execute(db, sql, params)
    db_commit_auto_rollback(db=db)
    return {"message": f"Unit '{unit_name}' soft-deleted."}
