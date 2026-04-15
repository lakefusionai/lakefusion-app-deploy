from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from typing import List
from app.lakefusion_middlelayer_service.utils.app_db import get_db,token_required_wrapper  # Importing get_db from the specified location
from lakefusion_utility.schemas.profiling_tasks import ProfilingTaskCreate, ProfilingTaskResponse
from lakefusion_utility.services.profiling_tasks import ProfilingTaskService  # Import the ProfilingTaskService class

# Initialize the router with a prefix and tag
profiling_task_router = APIRouter(tags=["Profiling Task API"], prefix='/profiling-tasks')

# Route to create a new profiling task
@profiling_task_router.post("/", response_model=ProfilingTaskResponse)
def create_profiling_task(
    profiling_task: ProfilingTaskCreate,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """
    Endpoint to create a new profiling task.
    
    Args:
    - profiling_task: Pydantic model containing task details
    - db: The database session to be used for operations
    - check: Token validation and extraction for the current user

    Returns:
    - ProfilingTaskResponse: The created profiling task
    """
    created_by = check.get('decoded', {}).get('sub', '')  # Extract created_by from the token
    token = check.get('token', '')  # Extract the token
    service = ProfilingTaskService(db)  # Create an instance of ProfilingTaskService
    return service.create_profiling_task(profiling_task, created_by, token)

#Route to refresh a profiling task manually
@profiling_task_router.post("/{profiling_task_id}/refresh", response_model=ProfilingTaskResponse)
def refresh_profiling_task(
    profiling_task_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """
    Endpoint to manually refresh a profiling task.
    
    Args:
    - profiling_task_id: ID of the profiling task to refresh
    - db: The database session to be used for operations
    - check: Token validation and extraction for the current user

    Returns:
    - ProfilingTaskResponse: The refreshed profiling task
    """
    token = check.get('token', '')  # Extract the token
    service = ProfilingTaskService(db)  # Create an instance of ProfilingTaskService
    return service.refresh_profiling_task(profiling_task_id, token)

# Route to get all profiling tasks
@profiling_task_router.get("/", response_model=List[ProfilingTaskResponse])
def get_all_profiling_tasks(db: Session = Depends(get_db)):
    """
    Endpoint to retrieve all profiling tasks.
    
    Args:
    - db: The database session to be used for operations

    Returns:
    - List[ProfilingTaskResponse]: A list of profiling tasks
    """
    service = ProfilingTaskService(db)  # Create an instance of ProfilingTaskService
    return service.get_all_profiling_tasks()

# Route to get a single profiling task by its ID
@profiling_task_router.get("/{profiling_task_id}", response_model=ProfilingTaskResponse)
def get_profiling_task(profiling_task_id: int, db: Session = Depends(get_db)):
    """
    Endpoint to retrieve a specific profiling task by its ID.
    
    Args:
    - profiling_task_id: ID of the profiling task to retrieve
    - db: The database session to be used for operations

    Returns:
    - ProfilingTaskResponse: The retrieved profiling task
    """
    service = ProfilingTaskService(db)  # Create an instance of ProfilingTaskService
    return service.get_profiling_task(profiling_task_id)

# Route to delete a profiling task
@profiling_task_router.delete("/{profiling_task_id}", response_model=ProfilingTaskResponse)
def delete_profiling_task(profiling_task_id: int, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    """
    Endpoint to delete a profiling task by its ID.
    
    Args:
    - profiling_task_id: ID of the profiling task to delete
    - db: The database session to be used for operations
    - check: Token validation and extraction for the current user

    Returns:
    - ProfilingTaskResponse: The deleted profiling task
    """
    token = check.get('token', '')  # Extract the token
    service = ProfilingTaskService(db)  # Create an instance of ProfilingTaskService
    return service.delete_profiling_task(profiling_task_id, token)

@profiling_task_router.get("/{profiling_task_id}/frequency/{attribute_name}")
def get_attribute_frequency(
    profiling_task_id: int,
    attribute_name: str,
    warehouse_id: str,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """
    Endpoint to retrieve frequency distribution of values for a specific attribute.
    
    Args:
    - profiling_task_id: ID of the profiling task
    - attribute_name: Name of the attribute to analyze
    - warehouse_id: ID of the warehouse to connect to
    - db: The database session
    - check: Token validation and extraction

    Returns:
    - dict: Frequency distribution metrics
    """
    token = check.get('token', '')
    service = ProfilingTaskService(db)
    return service.get_attribute_frequency(profiling_task_id, attribute_name, warehouse_id, token) 

@profiling_task_router.get("/{profiling_task_id}/numerical-distribution/{attribute_name}")
def get_numerical_distribution(
    profiling_task_id: int,
    attribute_name: str,
    warehouse_id: str,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """
    Endpoint to retrieve numerical distribution analysis for a numeric attribute.
    
    Args:
    - profiling_task_id: ID of the profiling task
    - attribute_name: Name of the attribute to analyze
    - warehouse_id: ID of the warehouse to connect to
    - db: The database session
    - check: Token validation and extraction

    Returns:
    - dict: Numerical distribution metrics
    """
    token = check.get('token', '')
    service = ProfilingTaskService(db)
    return service.get_numerical_distribution(profiling_task_id, attribute_name, warehouse_id, token)

@profiling_task_router.get("/{profiling_task_id}/current-metrics")
def get_current_metrics(
    profiling_task_id: int,
    warehouse_id: str,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """
    Get current metrics including table metrics and attribute metrics (fill rate and uniqueness)
    for all attributes.
    """
    token = check.get('token', '')
    service = ProfilingTaskService(db)
    return service.get_current_metrics(profiling_task_id, warehouse_id, token)

@profiling_task_router.get("/{profiling_task_id}/historical-metrics/{attribute_name}")
def get_historical_metrics(
    profiling_task_id: int,
    attribute_name: str,
    warehouse_id: str,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """
    Get historical metrics (fill rate and uniqueness over time) for a specific attribute.
    """
    token = check.get('token', '')
    service = ProfilingTaskService(db)
    return service.get_historical_metrics(
        profiling_task_id, 
        attribute_name, 
        warehouse_id, 
        token,
    )

@profiling_task_router.get("/{profiling_task_id}/check-profile-metrics-table-exists")
def check_profile_metrics_table_exists(
    profiling_task_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """
    Endpoint to check if the profile metrics table exists for a given profiling task.
    
    Args:
    - profiling_task_id: ID of the profiling task
    - db: The database session
    - check: Token validation and extraction

    Returns:
    - dict: A dictionary indicating whether the profile metrics table exists
    """
    token = check.get('token', '')
    service = ProfilingTaskService(db)
    return service.check_profile_metrics_table_exists(profiling_task_id, token)