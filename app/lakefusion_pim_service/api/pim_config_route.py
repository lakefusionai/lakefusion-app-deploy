from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import inspect
import app.lakefusion_pim_service.utils.app_db as app_db
from app.lakefusion_pim_service.utils.app_db import get_db, get_data_db, token_required_wrapper
from lakefusion_utility.models.pim import (
    PimSpecificationConfigCreate, PimSpecificationConfigResponse,
    PimSpecificationConfigUpdate,
    PimTaxonomyDefaultScalarCreate, PimTaxonomyDefaultScalarResponse,
    PimTaxonomyDefaultRefCreate, PimTaxonomyDefaultRefResponse,
    PimResolvedSpecificationResponse, PimResolvedSpecificationEnriched,
)
from app.lakefusion_pim_service.services.pim_config_service import PimConfigService
from lakefusion_utility.utils.logging_utils import get_logger
from typing import List

app_logger = get_logger(__name__)

pim_config_router = APIRouter(tags=["PIM Config"], prefix='/config')


@pim_config_router.post("/", response_model=PimSpecificationConfigResponse)
def create_config(
    entity_name: str,
    data: PimSpecificationConfigCreate,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimConfigService(db, entity_name)
    return service.create_config(data)


@pim_config_router.get("/node/{node_id}", response_model=List[PimSpecificationConfigResponse])
def get_configs_for_node(
    entity_name: str,
    node_id: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimConfigService(db, entity_name)
    return service.get_configs_for_node(node_id)


@pim_config_router.get("/node/{node_id}/resolved", response_model=List[PimResolvedSpecificationEnriched])
def get_resolved_for_node(
    entity_name: str,
    node_id: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimConfigService(db, entity_name)
    return service.get_resolved_for_node(node_id)


@pim_config_router.put("/{config_id}", response_model=PimSpecificationConfigResponse)
def update_config(
    entity_name: str,
    config_id: str,
    data: PimSpecificationConfigUpdate,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimConfigService(db, entity_name)
    return service.update_config(config_id, data)


@pim_config_router.delete("/{config_id}")
def delete_config(
    entity_name: str,
    config_id: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimConfigService(db, entity_name)
    return service.delete_config(config_id)


@pim_config_router.put("/{config_id}/default-scalar", response_model=PimTaxonomyDefaultScalarResponse)
def set_scalar_default(
    entity_name: str,
    config_id: str,
    data: PimTaxonomyDefaultScalarCreate,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimConfigService(db, entity_name)
    return service.set_scalar_default(config_id, data)


@pim_config_router.put("/{config_id}/default-ref", response_model=List[PimTaxonomyDefaultRefResponse])
def set_ref_defaults(
    entity_name: str,
    config_id: str,
    data: PimTaxonomyDefaultRefCreate,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimConfigService(db, entity_name)
    return service.set_ref_defaults(config_id, data)


# Known PIM system tables — exclude from reference table dropdown
PIM_SYSTEM_TABLES = {
    "pim_attribute_definition", "pim_taxonomy_node", "pim_specification_config",
    "pim_taxonomy_default_scalar", "pim_taxonomy_default_ref", "pim_resolved_specification",
    "pim_entity", "pim_value_text", "pim_value_number", "pim_value_boolean",
    "pim_value_date", "pim_value_select", "pim_value_multiselect", "pim_value_reference",
    "pim_language_ref", "pim_unit_ref", "pim_languages", "pim_prices",
}


@pim_config_router.get("/reference-tables")
def list_reference_tables(
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_data_db),
):
    """List all non-system tables in the data DB that can be used as reference entities."""
    try:
        inspector = inspect(db.get_bind())
        all_tables = inspector.get_table_names(schema="public")
        ref_tables = [t for t in all_tables if t not in PIM_SYSTEM_TABLES]
        return sorted(ref_tables)
    except Exception as e:
        app_logger.error(f"Error listing reference tables: {e}")
        raise HTTPException(status_code=500, detail=str(e))
