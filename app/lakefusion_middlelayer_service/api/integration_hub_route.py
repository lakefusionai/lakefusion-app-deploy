from fastapi import HTTPException, Depends, APIRouter
from sqlalchemy.orm import Session
from app.lakefusion_middlelayer_service.utils.app_db import get_db,token_required_wrapper  # Importing get_db from the specified location
from lakefusion_utility.models.integration_hub import (
    Integration_HubCreate, Integration_HubResponse, Integration_HubUpdate,
    RefEntitySyncPipelineCreate, RefEntitySyncPipelineUpdate, RefEntitySyncPipelineResponse,
)
from lakefusion_utility.services.integration_hub_service import Integration_HubService, RefEntitySyncPipelineService
from typing import List
from pydantic import BaseModel
from lakefusion_utility.services.model_experiment_service import compare_versions

# Initialize the router with a prefix and tag
integration_hub_router = APIRouter(tags=["Integration Hub API"], prefix='/integration-hub')

# Create a new Entity
@integration_hub_router.post("/", response_model=Integration_HubResponse)
def create_integration_hub(task: Integration_HubCreate, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    task.created_by = check.get('decoded', {}).get('sub', '')
    token = check.get('token')
    service = Integration_HubService(db)  # Create an instance of EntityService
    return service.create_integration_hub(task, token)

# Read all Entitys with an optional `is_active` filter
@integration_hub_router.get("/", response_model=List[Integration_HubResponse])
def read_integration_hub(is_active: bool = True, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = Integration_HubService(db)
    return service.read_integration_hub(is_active)


class DeleteIntegrationHubPayload(BaseModel):
    job_ids: List[int]

@integration_hub_router.post("/sync", response_model=Integration_HubResponse)
def sync_integration_hub(integration_hub_id: int , db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = Integration_HubService(db)
    token = check.get('token')
    return service.recreate_jobs_with_backup(integration_hub_id,token)


@integration_hub_router.delete("/{integration_id}")
def delete_integration_hub(integration_id: int, payload: DeleteIntegrationHubPayload, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    token = check.get('token')
    service = Integration_HubService(db)
    return service.delete_integration_hub(integration_id, payload.job_ids, token)

@integration_hub_router.patch("/{task_id}", response_model=Integration_HubResponse)
def update_integration_hub(task: Integration_HubUpdate, task_id: int, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = Integration_HubService(db)
    token = check.get('token')
    return service.update_integration_hub(task_id, task, token)

@integration_hub_router.post("/{integration_id}/upload-metadata")
def upload_metadata(integration_id: int, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    token = check.get('token')
    service = Integration_HubService(db)
    return service.upload_metadata_files(integration_id, token)

@integration_hub_router.get("/{entity_id}/version")
def get_entity_version(entity_id: int, db: Session = Depends(get_db)):
    return Integration_HubService.get_integration_hub_task_version(db, entity_id)


@integration_hub_router.post("/{entity_id}/dashboard")
def create_dashboard(entity_id:int,warehouse_id:str, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = Integration_HubService(db)
    entity_version = Integration_HubService.get_integration_hub_task_version(db, entity_id)
    token = check.get('token')
    created_by = check.get('decoded', {}).get('sub', '')
    if compare_versions(entity_version, "4.0.0") != -1:
        return service.create_dashboard_updated(entity_id, warehouse_id, token)
    else:
        return service.create_dashboard(entity_id, warehouse_id, token)


# ──────────────────────────────────────────────────────────────────────────────
# Reference Entity Sync Pipelines
# ──────────────────────────────────────────────────────────────────────────────

@integration_hub_router.post("/ref-sync/", response_model=RefEntitySyncPipelineResponse)
def create_ref_sync_pipeline(
    payload: RefEntitySyncPipelineCreate,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    payload.created_by = check.get("decoded", {}).get("sub", "")
    token = check.get('token')
    return RefEntitySyncPipelineService(db).create(payload,token)


@integration_hub_router.get("/ref-sync/", response_model=List[RefEntitySyncPipelineResponse])
def list_ref_sync_pipelines(
    is_active: bool = True,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    return RefEntitySyncPipelineService(db).read_all(is_active)


@integration_hub_router.patch("/ref-sync/{task_id}", response_model=RefEntitySyncPipelineResponse)
def update_ref_sync_pipeline(
    task_id: int,
    payload: RefEntitySyncPipelineUpdate,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    token = check.get("token")
    return RefEntitySyncPipelineService(db).update(task_id, payload,token)


@integration_hub_router.delete("/ref-sync/{task_id}")
def delete_ref_sync_pipeline(
    task_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    return RefEntitySyncPipelineService(db).delete(task_id)