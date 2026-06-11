from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Dict, Any
from app.lakefusion_middlelayer_service.utils.app_db import get_db, token_required_wrapper
from lakefusion_utility.models.quality_tasks import (
    TaskCreate, NotebookConfigUpdate, DiagramConfigUpdate,
    ScheduleUpdate, TaskResponse, TaskType
)
from lakefusion_utility.services.quality_tasks import QualityTaskService

quality_task_router = APIRouter(tags=["Quality Task API"], prefix='/quality-tasks')

# Task Management Endpoints
@quality_task_router.post("/", response_model=TaskResponse)
def create_task(
    task: TaskCreate,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """Create a new quality task in DRAFT status."""
    created_by = check.get('decoded', {}).get('sub', '')
    service = QualityTaskService(db)
    return service.create_task(task, created_by)

@quality_task_router.get("/template")
def get_template_file(
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """Get the notebook template file."""
    service = QualityTaskService(db)
    return service.get_template_file()

@quality_task_router.get("/", response_model=List[TaskResponse])
def get_all_tasks(
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """Get all tasks."""
    service = QualityTaskService(db)
    return service.get_all_tasks()

@quality_task_router.get("/{task_id}", response_model=TaskResponse)
def get_task(
    task_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """Get task details."""
    token = check.get('token', '')
    service = QualityTaskService(db)
    return service.get_task(task_id)

@quality_task_router.delete("/{task_id}")
def delete_task(
    task_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """Delete task and its Databricks job."""
    token = check.get('token', '')
    service = QualityTaskService(db)
    return service.delete_task(task_id, token)

# Task Configuration Endpoints
@quality_task_router.put("/{task_id}/notebook-config", response_model=TaskResponse)
def update_notebook_config(
    task_id: int,
    config: NotebookConfigUpdate,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """Update notebook task configuration."""
    created_by = check.get('decoded', {}).get('sub', '')
    token = check.get('token', '')
    service = QualityTaskService(db)
    return service.update_notebook_config(task_id, config, created_by, token)

@quality_task_router.put("/{task_id}/diagram-config", response_model=TaskResponse)
def update_diagram_config(
    task_id: int,
    config: DiagramConfigUpdate,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """Update diagram task configuration."""
    created_by = check.get('decoded', {}).get('sub', '')
    token = check.get('token', '')
    service = QualityTaskService(db)
    return service.update_diagram_config_new(task_id, config, created_by, token)

@quality_task_router.put("/{task_id}/schedule", response_model=TaskResponse)
def update_schedule(
    task_id: int,
    schedule: ScheduleUpdate,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """Update task schedule."""
    token = check.get('token', '')
    service = QualityTaskService(db)
    return service.update_schedule(task_id, schedule, token)

# Task Execution Endpoint
@quality_task_router.post("/{task_id}/execute")
def execute_task(
    task_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """Execute a task directly in Databricks."""
    created_by = check.get('decoded', {}).get('sub', '')
    token = check.get('token', '')
    service = QualityTaskService(db)
    return service.execute_task(task_id, created_by, token)

@quality_task_router.get("/type/{task_type}", response_model=List[TaskResponse])
def get_tasks_by_type(
    task_type: TaskType,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """Get all tasks of a specific type."""
    service = QualityTaskService(db)
    tasks = service.get_all_tasks()
    return [task for task in tasks if task.task_type == task_type]