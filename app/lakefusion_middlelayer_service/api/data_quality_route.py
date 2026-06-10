from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from app.lakefusion_middlelayer_service.utils.app_db import get_db, token_required_wrapper
from lakefusion_utility.models.data_quality import DiagramCreate, DiagramUpdate, DiagramResponse, ExecutionLogResponse
from lakefusion_utility.services.data_quality_service import QualityDiagramService



quality_diagram_router = APIRouter(tags=["Quality Diagram API"], prefix='/quality-diagrams')

@quality_diagram_router.post("/", response_model=DiagramResponse)
def create_diagram(
    diagram: DiagramCreate,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """Create or update a quality diagram for a dataset."""
    created_by = check.get('decoded', {}).get('sub', '')
    service = QualityDiagramService(db)
    return service.create_diagram(diagram, created_by)

@quality_diagram_router.get("/dataset/{dataset_id}", response_model=DiagramResponse)
def get_diagram(
    dataset_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """Get a diagram by dataset ID."""
    service = QualityDiagramService(db)
    return service.get_diagram(dataset_id)

@quality_diagram_router.get("/", response_model=List[DiagramResponse])
def get_all_diagrams(
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """Get all active diagrams."""
    service = QualityDiagramService(db)
    return service.get_all_diagrams()

@quality_diagram_router.put("/dataset/{dataset_id}", response_model=DiagramResponse)
def update_diagram(
    dataset_id: int,
    diagram: DiagramUpdate,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """Update an existing diagram."""
    service = QualityDiagramService(db)
    return service.update_diagram(dataset_id, diagram)

@quality_diagram_router.delete("/dataset/{dataset_id}")
def delete_diagram(
    dataset_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """Soft delete a diagram."""
    service = QualityDiagramService(db)
    return service.delete_diagram(dataset_id)

@quality_diagram_router.post("/dataset/{dataset_id}/execute", response_model=ExecutionLogResponse)
def execute_diagram(
    dataset_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """Execute a diagram's transformations."""
    created_by = check.get('decoded', {}).get('sub', '')
    token = check.get('token', '')
    service = QualityDiagramService(db)
    return service.execute_diagram(dataset_id, created_by, token)
