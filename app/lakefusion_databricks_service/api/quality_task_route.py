from fastapi import APIRouter, Depends, HTTPException
from lakefusion_utility.models.httpresponse import HttpResponse
from lakefusion_utility.models.dbconfig import DBConfigProperties
from lakefusion_utility.utils.logging_utils import get_logger
from app.lakefusion_databricks_service.services.quality_task_service import execute_quality_task_sample_output, get_quality_task_runs
from sqlalchemy.orm import Session
from app.lakefusion_databricks_service.utils.app_db import get_db,token_required_wrapper

# Initialize the logger for capturing log messages and handling errors
app_logger = get_logger(__name__)

# Initialize the FastAPI router for handling quality task-related API endpoints
quality_task_router = APIRouter(tags=["Quality Task API"], prefix='/quality-tasks')

# Endpoint to execute and retrieve a sample output for a quality task by its ID.
# This function handles the request and response for executing a quality task.
# Args:
#     task_id (int): The unique ID of the quality task to execute.
#     warehouse_id (str): The ID of the SQL warehouse where the task is executed.
#     check (dict): Authentication information passed by the Depends function.
#     db (Session): The database session to interact with the database.
# Returns:
#     An HttpResponse containing the sample output data if successful, or raises an HTTPException if an error occurs.
@quality_task_router.get("/{task_id}/sample-output")
async def get_quality_task_sample_output(
    task_id: int, 
    warehouse_id: str, 
    check: dict = Depends(token_required_wrapper), 
    db: Session = Depends(get_db)
):
    try:
        # Extract the token from the authentication check
        token = check.get('token')
        # Call the execute_quality_task_sample_output function to execute the quality task and fetch sample output
        return execute_quality_task_sample_output(token=token, db=db, task_id=task_id, warehouse_id=warehouse_id)
    except Exception as e:
        # Log the error and raise an HTTPException if execution fails
        app_logger.error(f"Failed to execute quality task {task_id}. Reason: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to execute quality task")

@quality_task_router.get("/{task_id}/runs")
async def get_task_runs(
    task_id: int,
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db)
):
    """
    API endpoint to fetch all runs for a specific quality task.
    
    Args:
        task_id (int): The ID of the quality task
        check (dict): Authentication information
        db (Session): Database session
        
    Returns:
        HttpResponse containing task runs data or error details
    """
    try:
        token = check.get('token')
        return get_quality_task_runs(token=token, db=db, task_id=task_id)
    except Exception as e:
        app_logger.error(f"Failed to fetch task runs for task {task_id}. Reason: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to fetch task runs")