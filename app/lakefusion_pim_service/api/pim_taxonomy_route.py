from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.lakefusion_pim_service.utils.app_db import get_data_db, token_required_wrapper
from lakefusion_utility.models.pim import (
    PimTaxonomyNodeCreate, PimTaxonomyNodeResponse, PimTaxonomyNodeUpdate,
    PimTaxonomyBulkCreateResponse,
    PimTaxonomyBulkImportRequest, PimTaxonomyBulkImportResponse,
)
from app.lakefusion_pim_service.services.pim_taxonomy_service import PimTaxonomyService
from lakefusion_utility.utils.logging_utils import get_logger
from typing import List

app_logger = get_logger(__name__)

pim_taxonomy_router = APIRouter(tags=["PIM Taxonomy"], prefix='/taxonomy')


@pim_taxonomy_router.post("/import", response_model=PimTaxonomyBulkImportResponse)
def bulk_import_taxonomy(
    entity_name: str,
    data: PimTaxonomyBulkImportRequest,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    """Bulk import taxonomy nodes, attributes, and bindings in a single transaction."""
    service = PimTaxonomyService(db, entity_name)
    return service.bulk_import_taxonomy(data)


@pim_taxonomy_router.post("/bulk", response_model=PimTaxonomyBulkCreateResponse)
def bulk_create_taxonomy_nodes(
    entity_name: str,
    data: List[PimTaxonomyNodeCreate],
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimTaxonomyService(db, entity_name)
    return service.bulk_create_nodes(data)


@pim_taxonomy_router.post("/", response_model=PimTaxonomyNodeResponse)
def create_taxonomy_node(
    entity_name: str,
    data: PimTaxonomyNodeCreate,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimTaxonomyService(db, entity_name)
    return service.create_node(data)


@pim_taxonomy_router.get("/tree")
def get_taxonomy_tree(
    entity_name: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimTaxonomyService(db, entity_name)
    return service.get_tree()


@pim_taxonomy_router.get("/{node_id}", response_model=PimTaxonomyNodeResponse)
def get_taxonomy_node(
    entity_name: str,
    node_id: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimTaxonomyService(db, entity_name)
    return service.get_node(node_id)


@pim_taxonomy_router.get("/{node_id}/subtree")
def get_taxonomy_subtree(
    entity_name: str,
    node_id: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimTaxonomyService(db, entity_name)
    return service.get_subtree(node_id)


@pim_taxonomy_router.put("/{node_id}", response_model=PimTaxonomyNodeResponse)
def update_taxonomy_node(
    entity_name: str,
    node_id: str,
    data: PimTaxonomyNodeUpdate,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimTaxonomyService(db, entity_name)
    return service.update_node(node_id, data)


@pim_taxonomy_router.delete("/{node_id}")
def delete_taxonomy_node(
    entity_name: str,
    node_id: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimTaxonomyService(db, entity_name)
    return service.delete_node(node_id)


@pim_taxonomy_router.delete("/{node_id}/children")
def delete_taxonomy_node_children(
    entity_name: str,
    node_id: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimTaxonomyService(db, entity_name)
    return service.delete_children(node_id)


@pim_taxonomy_router.post("/rebuild-cache")
def rebuild_resolved_cache(
    entity_name: str,
    db: Session = Depends(get_data_db),
    check: dict = Depends(token_required_wrapper),
):
    service = PimTaxonomyService(db, entity_name)
    return service.rebuild_resolved_cache()
