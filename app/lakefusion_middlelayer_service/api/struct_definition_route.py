"""Struct Definition CRUD routes.

All endpoints are gated behind the ENABLE_COMPLEX_DATA_TYPES feature flag.
"""
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.lakefusion_middlelayer_service.utils.app_db import get_db, token_required_wrapper
from lakefusion_utility.models.struct_definition import (
    StructDefinitionCreate,
    StructDefinitionResponse,
    StructDefinitionUpdate,
)
from lakefusion_utility.services.feature_flags_service import FeatureFlagService
from lakefusion_utility.services.struct_definition_service import (
    StructDefinitionService,
)

COMPLEX_TYPES_FLAG = "ENABLE_COMPLEX_DATA_TYPES"

struct_definition_router = APIRouter(
    tags=["Struct Definitions API"], prefix="/struct-definitions"
)


def _require_flag(db: Session) -> None:
    """Reject the request if ENABLE_COMPLEX_DATA_TYPES is not ACTIVE."""
    if not FeatureFlagService._is_feature_flag_enabled(db, COMPLEX_TYPES_FLAG):
        raise HTTPException(
            status_code=400,
            detail=(
                f"Complex data types are disabled. Enable the "
                f"'{COMPLEX_TYPES_FLAG}' feature flag to use struct definitions."
            ),
        )


@struct_definition_router.post("/", response_model=StructDefinitionResponse)
def create_struct_definition(
    payload: StructDefinitionCreate,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    _require_flag(db)
    if not payload.created_by:
        payload.created_by = check.get("decoded", {}).get("sub", "")
    service = StructDefinitionService(db)
    return service.create(payload)


@struct_definition_router.get("/", response_model=List[StructDefinitionResponse])
def list_struct_definitions(
    is_active: Optional[bool] = None,
    entity_id: Optional[int] = None,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    _require_flag(db)
    service = StructDefinitionService(db)
    return service.list_all(is_active=is_active, entity_id=entity_id)


@struct_definition_router.get(
    "/{struct_definition_id}", response_model=StructDefinitionResponse
)
def get_struct_definition(
    struct_definition_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    _require_flag(db)
    service = StructDefinitionService(db)
    return service.get_by_id(struct_definition_id)


@struct_definition_router.get("/{struct_definition_id}/usage")
def get_struct_definition_usage(
    struct_definition_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    _require_flag(db)
    service = StructDefinitionService(db)
    return service.get_usage(struct_definition_id)


@struct_definition_router.put(
    "/{struct_definition_id}", response_model=StructDefinitionResponse
)
def update_struct_definition(
    struct_definition_id: int,
    payload: StructDefinitionUpdate,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    _require_flag(db)
    service = StructDefinitionService(db)
    return service.update(struct_definition_id, payload)


@struct_definition_router.delete("/{struct_definition_id}")
def delete_struct_definition(
    struct_definition_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    _require_flag(db)
    service = StructDefinitionService(db)
    return service.delete(struct_definition_id)
