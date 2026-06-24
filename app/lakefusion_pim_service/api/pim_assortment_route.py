from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from app.lakefusion_pim_service.utils.app_db import get_data_db, token_required_wrapper
from lakefusion_utility.models.pim import (
    PimAssortmentCreate, PimAssortmentUpdate, PimAssortmentResponse,
)
from app.lakefusion_pim_service.services.pim_assortment_service import PimAssortmentService
from app.lakefusion_pim_service.services.pim_entity_service import PimEntityService
from lakefusion_utility.utils.logging_utils import get_logger
from typing import List, Optional

app_logger = get_logger(__name__)

pim_assortment_router = APIRouter(tags=["PIM Assortments"], prefix='/assortments')


@pim_assortment_router.post("/", response_model=PimAssortmentResponse)
def create_assortment(
    entity_name: str,
    data: PimAssortmentCreate,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimAssortmentService(db, entity_name)
    return service.create_assortment(data)


@pim_assortment_router.get("/", response_model=List[PimAssortmentResponse])
def list_assortments(
    entity_name: str,
    steward_user_id: Optional[str] = Query(None),
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimAssortmentService(db, entity_name)
    return service.list_assortments(steward_user_id=steward_user_id)


@pim_assortment_router.get("/my")
def get_my_assortments(
    entity_name: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    decoded = check.get("decoded", {}) or {}
    user_email = decoded.get("sub", "") or decoded.get("email", "") or ""
    assortment_service = PimAssortmentService(db, entity_name)
    product_service = PimEntityService(db, entity_name)
    return assortment_service.get_my_assortments(user_email, product_service)


@pim_assortment_router.get("/unassigned")
def get_unassigned_products(
    entity_name: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    assortment_service = PimAssortmentService(db, entity_name)
    product_service = PimEntityService(db, entity_name)
    return assortment_service.get_unassigned_products(product_service)


@pim_assortment_router.get("/{assortment_id}", response_model=PimAssortmentResponse)
def get_assortment(
    entity_name: str,
    assortment_id: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimAssortmentService(db, entity_name)
    return service.get_assortment(assortment_id)


@pim_assortment_router.put("/{assortment_id}", response_model=PimAssortmentResponse)
def update_assortment(
    entity_name: str,
    assortment_id: str,
    data: PimAssortmentUpdate,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimAssortmentService(db, entity_name)
    return service.update_assortment(assortment_id, data)


@pim_assortment_router.delete("/{assortment_id}")
def delete_assortment(
    entity_name: str,
    assortment_id: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimAssortmentService(db, entity_name)
    return service.delete_assortment(assortment_id)
