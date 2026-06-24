"""
Relationship API routes (Story 1 — configuration foundation).

Gated by the ENABLE_RELATIONSHIPS feature flag: the service layer raises 403
when the flag is INACTIVE so neither the UI nor any other caller can interact
with relationships until ops flips it on.
"""

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.lakefusion_middlelayer_service.utils.app_db import get_db, token_required_wrapper
from lakefusion_utility.models.relationship import (
    RelationshipCreate,
    RelationshipUpdate,
    RelationshipResponse,
    RelationshipAttributeCreate,
    RelationshipAttributeResponse,
    RelationshipDatasetMappingCreate,
    RelationshipDatasetMappingResponse,
    EligibleEntity,
)
from lakefusion_utility.services.relationship_service import (
    RelationshipService,
    RelationshipAttributeService,
    RelationshipDatasetMappingService,
)


relationship_router = APIRouter(tags=["Relationship API"], prefix="/relationship")


# ---------------------------------------------------------------------------
# Eligibility
# ---------------------------------------------------------------------------

@relationship_router.get("/eligible-entities", response_model=List[EligibleEntity])
def list_eligible_entities(
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    """Return Master entities annotated with eligibility flags. Designer's
    source/target dropdowns use this to disable ineligible rows with a reason."""
    token = check.get("token", "")
    service = RelationshipService(db)
    return service.list_eligible_entities(token)


# ---------------------------------------------------------------------------
# Relationship CRUD
# ---------------------------------------------------------------------------

@relationship_router.post("/", response_model=RelationshipResponse)
def create_relationship(
    payload: RelationshipCreate,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    payload.created_by = check.get("decoded", {}).get("sub", "")
    token = check.get("token", "")
    service = RelationshipService(db)
    return service.create_relationship(payload, token)


@relationship_router.get("/", response_model=List[RelationshipResponse])
def list_relationships(
    is_active: Optional[bool] = None,
    status: Optional[str] = None,
    entity_id: Optional[int] = None,
    q: Optional[str] = None,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    service = RelationshipService(db)
    return service.list_relationships(
        is_active=is_active, status_filter=status, entity_id=entity_id, q=q
    )


@relationship_router.get("/{relationship_id}", response_model=RelationshipResponse)
def get_relationship(
    relationship_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    service = RelationshipService(db)
    return service.get_relationship(relationship_id)


@relationship_router.patch("/{relationship_id}", response_model=RelationshipResponse)
def update_relationship(
    relationship_id: int,
    payload: RelationshipUpdate,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    service = RelationshipService(db)
    return service.update_relationship(relationship_id, payload)


@relationship_router.patch("/{relationship_id}/toggle", response_model=RelationshipResponse)
def toggle_relationship(
    relationship_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    service = RelationshipService(db)
    return service.toggle_relationship_active(relationship_id)


@relationship_router.delete("/{relationship_id}")
def delete_relationship(
    relationship_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    service = RelationshipService(db)
    return service.delete_relationship(relationship_id)


# ---------------------------------------------------------------------------
# Attributes
# ---------------------------------------------------------------------------

@relationship_router.get(
    "/{relationship_id}/attribute", response_model=List[RelationshipAttributeResponse]
)
def list_attributes(
    relationship_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    service = RelationshipAttributeService(db)
    return service.list_attributes(relationship_id)


@relationship_router.post(
    "/{relationship_id}/attribute", response_model=RelationshipAttributeResponse
)
def create_attribute(
    relationship_id: int,
    payload: RelationshipAttributeCreate,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    created_by = check.get("decoded", {}).get("sub", "")
    service = RelationshipAttributeService(db)
    return service.create_attribute(relationship_id, payload, created_by=created_by)


@relationship_router.patch(
    "/{relationship_id}/attribute/{attribute_id}",
    response_model=RelationshipAttributeResponse,
)
def update_attribute(
    relationship_id: int,
    attribute_id: int,
    payload: RelationshipAttributeCreate,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    service = RelationshipAttributeService(db)
    return service.update_attribute(relationship_id, attribute_id, payload)


@relationship_router.delete("/{relationship_id}/attribute/{attribute_id}")
def delete_attribute(
    relationship_id: int,
    attribute_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    service = RelationshipAttributeService(db)
    return service.delete_attribute(relationship_id, attribute_id)


# ---------------------------------------------------------------------------
# Dataset mappings
# ---------------------------------------------------------------------------

@relationship_router.get(
    "/{relationship_id}/mapping",
    response_model=List[RelationshipDatasetMappingResponse],
)
def list_mappings(
    relationship_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    service = RelationshipDatasetMappingService(db)
    return service.list_mappings(relationship_id)


@relationship_router.post(
    "/{relationship_id}/mapping",
    response_model=RelationshipDatasetMappingResponse,
)
def create_mapping(
    relationship_id: int,
    payload: RelationshipDatasetMappingCreate,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    created_by = check.get("decoded", {}).get("sub", "")
    service = RelationshipDatasetMappingService(db)
    return service.create_mapping(relationship_id, payload, created_by=created_by)


# NOTE — the /reorder route MUST be declared before any /{mapping_id} route
# because FastAPI matches in declaration order. Otherwise "reorder" gets
# parsed as `mapping_id` and the int-parser errors out.

class _ReorderMappingsPayload(BaseModel):
    ordered_mapping_ids: List[int]


@relationship_router.patch(
    "/{relationship_id}/mapping/reorder",
    response_model=List[RelationshipDatasetMappingResponse],
)
def reorder_mappings(
    relationship_id: int,
    payload: _ReorderMappingsPayload,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    """Reorder dataset mappings. Top of the list becomes highest priority
    (lowest numeric value)."""
    service = RelationshipDatasetMappingService(db)
    return service.reorder_mappings(relationship_id, payload.ordered_mapping_ids)


@relationship_router.patch(
    "/{relationship_id}/mapping/{mapping_id}",
    response_model=RelationshipDatasetMappingResponse,
)
def update_mapping(
    relationship_id: int,
    mapping_id: int,
    payload: RelationshipDatasetMappingCreate,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    service = RelationshipDatasetMappingService(db)
    return service.update_mapping(relationship_id, mapping_id, payload)


@relationship_router.delete("/{relationship_id}/mapping/{mapping_id}")
def delete_mapping(
    relationship_id: int,
    mapping_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    service = RelationshipDatasetMappingService(db)
    return service.delete_mapping(relationship_id, mapping_id)


@relationship_router.patch(
    "/{relationship_id}/mapping/{mapping_id}/toggle",
    response_model=RelationshipDatasetMappingResponse,
)
def toggle_mapping(
    relationship_id: int,
    mapping_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    service = RelationshipDatasetMappingService(db)
    return service.toggle_mapping_active(relationship_id, mapping_id)
