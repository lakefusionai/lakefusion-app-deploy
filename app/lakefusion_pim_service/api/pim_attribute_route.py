from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.lakefusion_pim_service.utils.app_db import get_data_db, token_required_wrapper
from lakefusion_utility.models.pim import (
    PimAttributeDefinitionCreate, PimAttributeDefinitionResponse,
    PimAttributeDefinitionEnrichedResponse, PimAttributeDefinitionUpdate,
    PimAttributeBulkCreateResponse, PimAttributeBulkImportResponse,
    PimAttributeOptionCreate, PimAttributeOptionResponse, PimAttributeOptionUpdate,
)
from app.lakefusion_pim_service.services.pim_attribute_service import PimAttributeService
from lakefusion_utility.utils.logging_utils import get_logger
from typing import List
from pydantic import BaseModel

app_logger = get_logger(__name__)

pim_attribute_router = APIRouter(tags=["PIM Attributes"], prefix='/attributes')


@pim_attribute_router.post("/", response_model=PimAttributeDefinitionResponse)
def create_attribute(
    data: PimAttributeDefinitionCreate,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimAttributeService(db)
    return service.create_definition(data)


@pim_attribute_router.post("/bulk", response_model=PimAttributeBulkCreateResponse)
def bulk_create_attributes(
    data: List[PimAttributeDefinitionCreate],
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimAttributeService(db)
    return service.bulk_create_definitions(data)


@pim_attribute_router.post("/import", response_model=PimAttributeBulkImportResponse)
def bulk_import_attributes(
    data: List[PimAttributeDefinitionCreate],
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    """Flat-file import (Specifications tab): skips existing codes, seeds SELECT/MULTISELECT options."""
    service = PimAttributeService(db)
    return service.bulk_import_definitions(data)


@pim_attribute_router.get("/", response_model=List[PimAttributeDefinitionEnrichedResponse])
def list_attributes(
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimAttributeService(db)
    return service.list_definitions()


# ---------------------------------------------------------------------------
# OPTIONS (SELECT / MULTISELECT controlled vocabulary)
# ---------------------------------------------------------------------------
class _AddOptionBody(BaseModel):
    label: str


class _ReorderBody(BaseModel):
    order: List[str]


@pim_attribute_router.get("/{attr_id}/options", response_model=List[PimAttributeOptionResponse])
def list_attribute_options(
    attr_id: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimAttributeService(db)
    return service.list_options(attr_id)


@pim_attribute_router.post("/{attr_id}/options", response_model=PimAttributeOptionResponse)
def add_attribute_option(
    attr_id: str,
    data: _AddOptionBody,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimAttributeService(db)
    return service.add_option(attr_id, data.label)


@pim_attribute_router.put("/{attr_id}/options/reorder", response_model=List[PimAttributeOptionResponse])
def reorder_attribute_options(
    attr_id: str,
    data: _ReorderBody,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimAttributeService(db)
    return service.reorder_options(attr_id, data.order)


@pim_attribute_router.put("/options/{option_id}", response_model=PimAttributeOptionResponse)
def update_attribute_option(
    option_id: str,
    data: PimAttributeOptionUpdate,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimAttributeService(db)
    return service.update_option(option_id, data)


@pim_attribute_router.delete("/options/{option_id}")
def delete_attribute_option(
    option_id: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimAttributeService(db)
    return service.delete_option(option_id)


@pim_attribute_router.get("/{attr_id}/usage")
def get_attribute_usage(
    attr_id: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimAttributeService(db)
    return service.get_usage_stats(attr_id)


@pim_attribute_router.get("/{attr_id}/usage-details")
def get_attribute_usage_details(
    attr_id: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimAttributeService(db)
    return service.get_usage_details(attr_id)


@pim_attribute_router.get("/{attr_id}", response_model=PimAttributeDefinitionResponse)
def get_attribute(
    attr_id: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimAttributeService(db)
    return service.get_definition(attr_id)


@pim_attribute_router.put("/{attr_id}", response_model=PimAttributeDefinitionResponse)
def update_attribute(
    attr_id: str,
    data: PimAttributeDefinitionUpdate,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimAttributeService(db)
    return service.update_definition(attr_id, data)


@pim_attribute_router.patch("/{attr_id}/toggle-identifier", response_model=PimAttributeDefinitionResponse)
def toggle_identifier(
    attr_id: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    """Toggle is_identifier on/off. Auto-swaps: only one attribute can be identifier at a time."""
    service = PimAttributeService(db)
    return service.toggle_identifier(attr_id)


@pim_attribute_router.patch("/{attr_id}/toggle-label", response_model=PimAttributeDefinitionResponse)
def toggle_label(
    attr_id: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    """Toggle is_label on/off. Auto-swaps: only one attribute can be label at a time."""
    service = PimAttributeService(db)
    return service.toggle_label(attr_id)


@pim_attribute_router.delete("/{attr_id}")
def delete_attribute(
    attr_id: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimAttributeService(db)
    return service.delete_definition(attr_id)
