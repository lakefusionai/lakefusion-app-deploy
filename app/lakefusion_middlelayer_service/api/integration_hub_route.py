import os
import requests
from fastapi import HTTPException, Depends, APIRouter, Query
from sqlalchemy.orm import Session
from app.lakefusion_middlelayer_service.utils.app_db import get_db,token_required_wrapper  # Importing get_db from the specified location
from lakefusion_utility.utils.logging_utils import get_logger
from lakefusion_utility.models.integration_hub import (
    Integration_HubCreate, Integration_HubResponse, Integration_HubUpdate,
    RefEntitySyncPipelineCreate, RefEntitySyncPipelineUpdate, RefEntitySyncPipelineResponse,
    RelationshipSyncPipelineCreate, RelationshipSyncPipelineUpdate, RelationshipSyncPipelineResponse,
)
from lakefusion_utility.services.integration_hub_service import (
    Integration_HubService, RefEntitySyncPipelineService,
)
from lakefusion_utility.services.relationship_sync_service import RelationshipSyncPipelineService
from typing import List, Optional
from pydantic import BaseModel
from lakefusion_utility.services.model_experiment_service import compare_versions

# Initialize the router with a prefix and tag
integration_hub_router = APIRouter(tags=["Integration Hub API"], prefix='/integration-hub')

_logger = get_logger(__name__)

# Local dev only: the PIM init pipeline is Databricks-bound and cannot run against
# local Postgres. Set PIM_LOCAL_INIT=true (middlelayer-owned flag) ONLY in a local
# Postgres dev setup — then the hub skips the Databricks submit and triggers PIM's
# /initialize-local over HTTP instead. Default = false (submit the Databricks job).
# (We deliberately do NOT key off DATA_DB_TYPE: that describes the PIM service's data
# DB, which the middlelayer doesn't own and usually doesn't set.)
_SKIP_PIM_PIPELINE = os.getenv("PIM_LOCAL_INIT", "false").lower() == "true"


def _trigger_pim_local_init(entity_id: int, token: str = ""):
    """Call the PIM service to create + seed PIM tables in local Postgres."""
    try:
        pim_service_url = os.getenv("PIM_SERVICE_URL", "http://localhost:8006")
        url = f"{pim_service_url}/api/pim/entity-bridge/{entity_id}/initialize-local"
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        resp = requests.post(url, headers=headers, timeout=60)
        if resp.status_code >= 400:
            _logger.warning(f"PIM local init failed ({resp.status_code}): {resp.text}")
        else:
            _logger.info(f"PIM local init done for entity {entity_id}: {resp.text}")
    except Exception as e:
        _logger.warning(f"PIM local init call failed (service unavailable?): {e}")


# Create a new Entity
@integration_hub_router.post("/", response_model=Integration_HubResponse)
def create_integration_hub(task: Integration_HubCreate, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    task.created_by = check.get('decoded', {}).get('sub', '')
    token = check.get('token')
    service = Integration_HubService(db)  # Create an instance of EntityService
    created = service.create_integration_hub(task, token, skip_pipeline_submit=_SKIP_PIM_PIPELINE)
    # Local-Postgres dev only (PIM_LOCAL_INIT=true): the Databricks pipeline was
    # skipped — create + seed the PIM tables locally by calling the PIM service
    # (synchronous so the caller sees init complete before using the entity).
    if _SKIP_PIM_PIPELINE and getattr(created, "task_type", None) == "pim":
        _trigger_pim_local_init(created.entity_id, token)
    return created

# Read all Entitys with an optional `is_active` filter
@integration_hub_router.get("/", response_model=List[Integration_HubResponse])
def read_integration_hub(is_active: bool = True, task_type: Optional[str] = Query(None, description="Filter by task_type: mdm, pim, ref_sync"), db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = Integration_HubService(db)
    return service.read_integration_hub(is_active, task_type=task_type)


class DeleteIntegrationHubPayload(BaseModel):
    job_ids: List[int]
    drop_tables: bool = False  # When True, also DROP entity tables/views and the VS index
    warehouse_id: str | None = None  # SQL warehouse to run the DROP statements against

@integration_hub_router.post("/sync", response_model=Integration_HubResponse)
def sync_integration_hub(integration_hub_id: int , db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = Integration_HubService(db)
    token = check.get('token')
    return service.recreate_jobs_with_backup(integration_hub_id,token)


@integration_hub_router.delete("/{integration_id}")
def delete_integration_hub(integration_id: int, payload: DeleteIntegrationHubPayload, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    token = check.get('token')
    service = Integration_HubService(db)
    return service.delete_integration_hub(
        integration_id,
        payload.job_ids,
        token,
        drop_tables=payload.drop_tables,
        warehouse_id=payload.warehouse_id,
    )

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
    drop_tables: bool = False,
    delete_job: bool = False,
    warehouse_id: str | None = None,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    token = check.get('token')
    return RefEntitySyncPipelineService(db).delete(
        task_id,
        token=token,
        drop_tables=drop_tables,
        delete_job=delete_job,
        warehouse_id=warehouse_id,
    )

# ──────────────────────────────────────────────────────────────────────────────
# Relationship Sync Pipelines (Story 2)
# ──────────────────────────────────────────────────────────────────────────────

@integration_hub_router.post(
    "/relationship-sync/", response_model=RelationshipSyncPipelineResponse
)
def create_relationship_sync_pipeline(
    payload: RelationshipSyncPipelineCreate,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    payload.created_by = check.get("decoded", {}).get("sub", "")
    token = check.get("token")
    return RelationshipSyncPipelineService(db).create(payload, token)


@integration_hub_router.get(
    "/relationship-sync/", response_model=List[RelationshipSyncPipelineResponse]
)
def list_relationship_sync_pipelines(
    is_active: bool = True,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    return RelationshipSyncPipelineService(db).read_all(is_active)


@integration_hub_router.patch(
    "/relationship-sync/{task_id}", response_model=RelationshipSyncPipelineResponse
)
def update_relationship_sync_pipeline(
    task_id: int,
    payload: RelationshipSyncPipelineUpdate,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    token = check.get("token")
    return RelationshipSyncPipelineService(db).update(task_id, payload, token)


@integration_hub_router.delete("/relationship-sync/{task_id}")
def delete_relationship_sync_pipeline(
    task_id: int,
    delete_job: bool = False,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    token = check.get("token")
    return RelationshipSyncPipelineService(db).delete(
        task_id,
        token=token,
        delete_job=delete_job,
    )

# ──────────────────────────────────────────────────────────────────────────────
# Product Entity Integration Tasks
# ──────────────────────────────────────────────────────────────────────────────

# Product entity endpoints removed — PIM tasks use main /integration-hub/ with task_type='pim'
