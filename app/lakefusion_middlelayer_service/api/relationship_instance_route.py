"""
Relationship Instance routes (Story 3 — Steward surface).

Endpoints:
- GET  /relationship-instance/entity/{entity_id}/{master_id}
- GET  /relationship-instance/{relationship_id}/lineage
- POST /relationship-instance/{relationship_id}/override
- GET  /relationship-instance/{relationship_id}/override
"""

from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.lakefusion_middlelayer_service.utils.app_db import get_db, token_required_wrapper
from lakefusion_utility.models.relationship import (
    LineageRow,
    RelationshipInstance,
    StewardOverrideCreate,
    StewardOverrideResponse,
)
from lakefusion_utility.services.relationship_instance_service import (
    RelationshipInstanceService,
)


relationship_instance_router = APIRouter(
    tags=["Relationship Instance API"], prefix="/relationship-instance"
)


@relationship_instance_router.get(
    "/entity/{entity_id}/{master_id}", response_model=List[RelationshipInstance]
)
def list_instances_for_entity(
    entity_id: int,
    master_id: str,
    warehouse_id: str = Query(..., description="Databricks SQL warehouse"),
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    token = check.get("token", "")
    return RelationshipInstanceService(db).list_instances_for_entity(
        entity_id, master_id, token, warehouse_id
    )


@relationship_instance_router.get(
    "/{relationship_id}/lineage", response_model=List[LineageRow]
)
def get_lineage(
    relationship_id: int,
    src_node_id: str,
    dst_node_id: str,
    warehouse_id: str,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    token = check.get("token", "")
    return RelationshipInstanceService(db).get_lineage(
        relationship_id, src_node_id, dst_node_id, token, warehouse_id
    )


@relationship_instance_router.post(
    "/{relationship_id}/override", response_model=StewardOverrideResponse
)
def create_override(
    relationship_id: int,
    payload: StewardOverrideCreate,
    warehouse_id: str,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    token = check.get("token", "")
    steward_id = check.get("decoded", {}).get("sub", "")
    return RelationshipInstanceService(db).create_override(
        relationship_id, payload, token, warehouse_id, steward_id
    )


@relationship_instance_router.get(
    "/{relationship_id}/override", response_model=List[StewardOverrideResponse]
)
def list_overrides(
    relationship_id: int,
    master_id: Optional[str] = None,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    return RelationshipInstanceService(db).list_overrides(relationship_id, master_id)
