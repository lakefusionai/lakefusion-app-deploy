import traceback
from decimal import Decimal
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from app.lakefusion_pim_service.utils.app_db import get_data_db, token_required_wrapper
from lakefusion_utility.models.pim import (
    PimLanguageRef, PimLanguageRefCreate, PimLanguageRefResponse, PimLanguageRefUpdate,
    PimUnitRef, PimUnitRefCreate, PimUnitRefResponse, PimUnitRefUpdate,
)
from lakefusion_utility.utils.app_db import db_commit_auto_rollback
from lakefusion_utility.utils.logging_utils import get_logger
from typing import List, Optional

app_logger = get_logger(__name__)

pim_reference_router = APIRouter(tags=["PIM Reference"], prefix='/reference')


# ---------------------------------------------------------------------------
# LANGUAGES
# ---------------------------------------------------------------------------

@pim_reference_router.get("/languages", response_model=List[PimLanguageRefResponse])
def list_languages(
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    return db.query(PimLanguageRef).filter(PimLanguageRef.is_active == True).order_by(PimLanguageRef.id).all()


@pim_reference_router.post("/languages", response_model=PimLanguageRefResponse)
def create_language(
    data: PimLanguageRefCreate,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    existing = db.query(PimLanguageRef).filter(PimLanguageRef.id == data.id).first()
    if existing:
        raise HTTPException(status_code=409, detail=f"Language '{data.id}' already exists.")
    lang = PimLanguageRef(id=data.id, language_name=data.language_name, is_active=data.is_active or True)
    db.add(lang)
    db_commit_auto_rollback(db=db)
    db.refresh(lang)
    return lang


@pim_reference_router.put("/languages/{lang_id}", response_model=PimLanguageRefResponse)
def update_language(
    lang_id: str,
    data: PimLanguageRefUpdate,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    lang = db.query(PimLanguageRef).filter(PimLanguageRef.id == lang_id).first()
    if not lang:
        raise HTTPException(status_code=404, detail="Language not found.")
    if data.language_name is not None:
        lang.language_name = data.language_name
    if data.is_active is not None:
        lang.is_active = data.is_active
    db_commit_auto_rollback(db=db)
    db.refresh(lang)
    return lang


@pim_reference_router.delete("/languages/{lang_id}")
def delete_language(
    lang_id: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    lang = db.query(PimLanguageRef).filter(PimLanguageRef.id == lang_id).first()
    if not lang:
        raise HTTPException(status_code=404, detail="Language not found.")
    lang.is_active = False
    db_commit_auto_rollback(db=db)
    return {"message": f"Language '{lang_id}' soft-deleted."}


# ---------------------------------------------------------------------------
# UNITS
# ---------------------------------------------------------------------------

@pim_reference_router.get("/units", response_model=List[PimUnitRefResponse])
def list_units(
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    # Auto-seed default units if table is empty (for entities initialized before unit seeding was added)
    count = db.query(PimUnitRef).count()
    if count == 0:
        DEFAULT_UNITS = [
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
        for unit in DEFAULT_UNITS:
            db.add(PimUnitRef(**unit))
        db_commit_auto_rollback(db=db)
        app_logger.info(f"Auto-seeded {len(DEFAULT_UNITS)} default measurement units")
    return db.query(PimUnitRef).filter(PimUnitRef.is_active == True).order_by(PimUnitRef.name).all()


@pim_reference_router.get("/units/convert")
def convert_unit(
    value: float = Query(..., description="Value to convert"),
    from_unit: str = Query(..., description="Source unit name"),
    to_unit: str = Query(..., description="Target unit name"),
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    """Convert a value between two units in the same category."""
    source = db.query(PimUnitRef).filter(PimUnitRef.name == from_unit, PimUnitRef.is_active == True).first()
    if not source:
        raise HTTPException(status_code=404, detail=f"Source unit '{from_unit}' not found.")

    target = db.query(PimUnitRef).filter(PimUnitRef.name == to_unit, PimUnitRef.is_active == True).first()
    if not target:
        raise HTTPException(status_code=404, detail=f"Target unit '{to_unit}' not found.")

    if source.category != target.category:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot convert between different categories: '{source.category}' and '{target.category}'.",
        )

    if not source.to_base_factor or not target.to_base_factor:
        raise HTTPException(
            status_code=400,
            detail=f"Conversion factors not configured for '{from_unit}' and/or '{to_unit}'. Set base_unit and to_base_factor.",
        )

    # Convert: source_value → base → target
    base_value = Decimal(str(value)) * Decimal(str(source.to_base_factor))
    converted = float(base_value / Decimal(str(target.to_base_factor)))

    return {
        "from_unit": from_unit,
        "to_unit": to_unit,
        "from_value": value,
        "to_value": round(converted, 6),
        "category": source.category,
    }


@pim_reference_router.post("/units", response_model=PimUnitRefResponse)
def create_unit(
    data: PimUnitRefCreate,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    existing = db.query(PimUnitRef).filter(PimUnitRef.name == data.name).first()
    if existing:
        raise HTTPException(status_code=409, detail=f"Unit '{data.name}' already exists.")
    unit = PimUnitRef(
        name=data.name, category=data.category, description=data.description,
        base_unit=data.base_unit, to_base_factor=data.to_base_factor,
        is_active=data.is_active or True,
    )
    db.add(unit)
    db_commit_auto_rollback(db=db)
    db.refresh(unit)
    return unit


@pim_reference_router.put("/units/{unit_name}", response_model=PimUnitRefResponse)
def update_unit(
    unit_name: str,
    data: PimUnitRefUpdate,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    unit = db.query(PimUnitRef).filter(PimUnitRef.name == unit_name).first()
    if not unit:
        raise HTTPException(status_code=404, detail="Unit not found.")
    if data.category is not None:
        unit.category = data.category
    if data.description is not None:
        unit.description = data.description
    if data.base_unit is not None:
        unit.base_unit = data.base_unit
    if data.to_base_factor is not None:
        unit.to_base_factor = data.to_base_factor
    if data.is_active is not None:
        unit.is_active = data.is_active
    db_commit_auto_rollback(db=db)
    db.refresh(unit)
    return unit


@pim_reference_router.delete("/units/{unit_name}")
def delete_unit(
    unit_name: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    unit = db.query(PimUnitRef).filter(PimUnitRef.name == unit_name).first()
    if not unit:
        raise HTTPException(status_code=404, detail="Unit not found.")
    unit.is_active = False
    db_commit_auto_rollback(db=db)
    return {"message": f"Unit '{unit_name}' soft-deleted."}
