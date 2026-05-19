from fastapi import Depends, APIRouter
from sqlalchemy.orm import Session
from app.lakefusion_middlelayer_service.utils.app_db import get_db, token_required_wrapper
from lakefusion_utility.models.schema_evolution import (
    SchemaEvolutionJobCreate, SchemaEvolutionJobResponse,
    SchemaEvolutionEditCreate, SchemaEvolutionEditResponse,
    SchemaEvolutionValidateResponse
)
from lakefusion_utility.services.schema_evolution_service import SchemaEvolutionService
from typing import List
from pydantic import BaseModel

schema_evolution_router = APIRouter(
    tags=["Schema Evolution API"],
    prefix='/entities/{entity_id}/schema-evolution'
)

# ─── Global router (not entity-scoped) ────────────────────────
schema_evolution_global_router = APIRouter(
    tags=["Schema Evolution API"],
    prefix='/schema-evolution'
)


@schema_evolution_global_router.get("/jobs")
def get_all_jobs(db: Session = Depends(get_db),
                 check: dict = Depends(token_required_wrapper)):
    service = SchemaEvolutionService(db)
    return service.get_all_jobs()


class ToggleEnabledPayload(BaseModel):
    enabled: bool


# ─── Entity-level toggle ────────────────────────────────────

@schema_evolution_router.patch("/enabled")
def toggle_schema_evolution(entity_id: int, payload: ToggleEnabledPayload,
                            db: Session = Depends(get_db),
                            check: dict = Depends(token_required_wrapper)):
    service = SchemaEvolutionService(db)
    return service.toggle_schema_evolution(entity_id, payload.enabled)


# ─── Job CRUD ────────────────────────────────────────────────

@schema_evolution_router.post("/jobs", response_model=SchemaEvolutionJobResponse)
def create_job(entity_id: int, data: SchemaEvolutionJobCreate,
               db: Session = Depends(get_db),
               check: dict = Depends(token_required_wrapper)):
    created_by = check.get('decoded', {}).get('sub', '')
    service = SchemaEvolutionService(db)
    return service.create_job(entity_id, data, created_by)


@schema_evolution_router.get("/jobs", response_model=List[SchemaEvolutionJobResponse])
def get_jobs(entity_id: int,
             db: Session = Depends(get_db),
             check: dict = Depends(token_required_wrapper)):
    service = SchemaEvolutionService(db)
    return service.get_jobs_for_entity(entity_id)


@schema_evolution_router.get("/jobs/{job_id}", response_model=SchemaEvolutionJobResponse)
def get_job(entity_id: int, job_id: int,
            db: Session = Depends(get_db),
            check: dict = Depends(token_required_wrapper)):
    service = SchemaEvolutionService(db)
    return service.get_job(job_id)


@schema_evolution_router.delete("/jobs/{job_id}")
def delete_job(entity_id: int, job_id: int,
               db: Session = Depends(get_db),
               check: dict = Depends(token_required_wrapper)):
    token = check.get('token')
    service = SchemaEvolutionService(db)
    return service.delete_job(job_id, token)


# ─── Edit CRUD ───────────────────────────────────────────────

@schema_evolution_router.post("/jobs/{job_id}/edits", response_model=SchemaEvolutionEditResponse)
def add_edit(entity_id: int, job_id: int, data: SchemaEvolutionEditCreate,
             db: Session = Depends(get_db),
             check: dict = Depends(token_required_wrapper)):
    service = SchemaEvolutionService(db)
    return service.add_edit(job_id, data)


@schema_evolution_router.put("/jobs/{job_id}/edits/{edit_id}", response_model=SchemaEvolutionEditResponse)
def update_edit(entity_id: int, job_id: int, edit_id: int,
                data: SchemaEvolutionEditCreate,
                db: Session = Depends(get_db),
                check: dict = Depends(token_required_wrapper)):
    service = SchemaEvolutionService(db)
    return service.update_edit(edit_id, data)


@schema_evolution_router.delete("/jobs/{job_id}/edits/{edit_id}")
def delete_edit(entity_id: int, job_id: int, edit_id: int,
                db: Session = Depends(get_db),
                check: dict = Depends(token_required_wrapper)):
    service = SchemaEvolutionService(db)
    return service.delete_edit(edit_id)


# ─── Job Lifecycle ───────────────────────────────────────────

@schema_evolution_router.post("/jobs/{job_id}/validate", response_model=SchemaEvolutionValidateResponse)
def validate_job(entity_id: int, job_id: int,
                 db: Session = Depends(get_db),
                 check: dict = Depends(token_required_wrapper)):
    token = check.get('token')
    service = SchemaEvolutionService(db)
    return service.validate_job(job_id, token)


@schema_evolution_router.post("/jobs/{job_id}/publish", response_model=SchemaEvolutionJobResponse)
def publish_job(entity_id: int, job_id: int,
                db: Session = Depends(get_db),
                check: dict = Depends(token_required_wrapper)):
    token = check.get('token')
    service = SchemaEvolutionService(db)
    return service.publish_job(job_id, token)


@schema_evolution_router.post("/jobs/{job_id}/run", response_model=SchemaEvolutionJobResponse)
def run_job(entity_id: int, job_id: int,
            db: Session = Depends(get_db),
            check: dict = Depends(token_required_wrapper)):
    token = check.get('token')
    service = SchemaEvolutionService(db)
    return service.run_job(job_id, token)


@schema_evolution_router.post("/jobs/{job_id}/apply-updates", response_model=SchemaEvolutionJobResponse)
def apply_updates(entity_id: int, job_id: int,
                  db: Session = Depends(get_db),
                  check: dict = Depends(token_required_wrapper)):
    token = check.get('token')
    service = SchemaEvolutionService(db)
    return service.apply_updates(job_id, token)


@schema_evolution_router.get("/jobs/{job_id}/status")
def poll_job_status(entity_id: int, job_id: int,
                    db: Session = Depends(get_db),
                    check: dict = Depends(token_required_wrapper)):
    token = check.get('token')
    service = SchemaEvolutionService(db)
    return service.poll_job_status(job_id, token)


@schema_evolution_router.post("/jobs/{job_id}/sync-status")
def sync_job_status(entity_id: int, job_id: int,
                    db: Session = Depends(get_db),
                    check: dict = Depends(token_required_wrapper)):
    token = check.get('token')
    service = SchemaEvolutionService(db)
    return service.sync_job_status(job_id, token)


@schema_evolution_router.post("/jobs/{job_id}/rollback")
def rollback_job(entity_id: int, job_id: int,
                 db: Session = Depends(get_db),
                 check: dict = Depends(token_required_wrapper)):
    token = check.get('token')
    service = SchemaEvolutionService(db)
    return service.rollback_job(job_id, token)
