from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.lakefusion_pim_service.utils.app_db import get_data_db, token_required_wrapper
from app.lakefusion_pim_service.services.pim_entity_tier_service import PimEntityTierService
from lakefusion_utility.models.pim import PimEntityTierCreate, PimEntityTierResponse
from lakefusion_utility.utils.logging_utils import get_logger
from typing import List

app_logger = get_logger(__name__)

pim_entity_tier_router = APIRouter(tags=["PIM Entity Tiers"])


@pim_entity_tier_router.get("/entity-tiers", response_model=List[PimEntityTierResponse])
def list_tiers(
    entity_name: str,
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_data_db),
):
    """List all entity tiers ordered by level (top-tier first)."""
    service = PimEntityTierService(db, entity_name)
    return service.list_tiers()


@pim_entity_tier_router.get("/entity-tiers/{tier_id}", response_model=PimEntityTierResponse)
def get_tier(
    entity_name: str,
    tier_id: str,
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_data_db),
):
    """Get a single entity tier by ID."""
    service = PimEntityTierService(db, entity_name)
    return service.get_tier(tier_id)


@pim_entity_tier_router.post("/entity-tiers", response_model=PimEntityTierResponse, status_code=201)
def create_tier(
    entity_name: str,
    data: PimEntityTierCreate,
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_data_db),
):
    """Create a new entity tier."""
    service = PimEntityTierService(db, entity_name)
    return service.create_tier(data)


@pim_entity_tier_router.patch("/entity-tiers/{tier_id}", response_model=PimEntityTierResponse)
def update_tier(
    entity_name: str,
    tier_id: str,
    data: dict,
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_data_db),
):
    """Update a tier's label or is_leaf flag."""
    service = PimEntityTierService(db, entity_name)
    return service.update_tier(tier_id, data)


@pim_entity_tier_router.delete("/entity-tiers/{tier_id}", status_code=204)
def delete_tier(
    entity_name: str,
    tier_id: str,
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_data_db),
):
    """Delete an entity tier (fails if products still use it)."""
    service = PimEntityTierService(db, entity_name)
    service.delete_tier(tier_id)
