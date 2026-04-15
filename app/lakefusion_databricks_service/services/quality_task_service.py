from fastapi import Depends, HTTPException, Response
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from lakefusion_utility.utils.logging_utils import get_logger
from lakefusion_utility.utils.databricks_util import DataSetSQLService, ComputeService, JobService, CommonUtilities
from lakefusion_utility.models.httpresponse import HttpResponse
from lakefusion_utility.services.quality_tasks import QualityTaskService
from databricks import sql
import traceback

# Initialize security scheme for token-based authentication and logger for logging activities
token_auth_scheme = HTTPBearer()
app_logger = get_logger(__name__)
common_utils = CommonUtilities()

# Function to execute a quality task and retrieve sample output.
# Args:
#     token (str): The authentication token used to validate the user.
#     db: The database session used for retrieving quality task information.
#     task_id (int): The unique ID of the quality task to execute.
#     warehouse_id (str): The ID of the SQL warehouse where the task is executed.
# Returns:
#     An HttpResponse object containing the sample output data if successful.
# Raises:
#     HttpResponse: Returns a 500 internal server error response if an exception occurs.
def execute_quality_task_sample_output(token: str, db, task_id: int, warehouse_id: str):
    try:
        # Initialize the QualityTaskService to interact with the quality task metadata
        quality_task_service = QualityTaskService(db)
        
        # Fetch the quality task by its ID from the database
        quality_task = quality_task_service.get_task(task_id=task_id)
        
        # Get the cleaned table path
        cleaned_table_path = common_utils.apply_tilde(quality_task.dataset.path + "_cleaned") 
        
        # Initialize DataSetSQLService to execute SQL queries related to the task
        sqlservice_conn = DataSetSQLService(token, warehouse_id)
        
        # Build and execute the SQL query to fetch the quality task output (limiting to 100 rows)
        data_query = f"SELECT * FROM {cleaned_table_path} LIMIT 100"
        data = sqlservice_conn.execute_dataset(data_query)
        
        # Get total row count with a fresh connection (to avoid closed connection error)
        count_conn = DataSetSQLService(token, warehouse_id)
        count_query = f"SELECT COUNT(*) as total_rows FROM {cleaned_table_path}"
        count_result = count_conn.execute_dataset(count_query)
        total_rows = count_result[0]['total_rows'] if count_result and len(count_result) > 0 else 0
        
        # Create the response structure
        response = {
            "data": data,
            "metadata": {
                "tableFullPath": cleaned_table_path,
                "tableName": quality_task.dataset.name,
                "taskId": task_id,
                "taskName": quality_task.name,
                "totalRows": total_rows
            }
        }
        
        # Return a successful HTTP response with both data and metadata
        return HttpResponse(status=200, data=response)
        
    except Exception as e:
        # Log the full exception traceback and error message
        message = traceback.format_exc()
        app_logger.exception(f'Unable to fetch the data. Reason - {message}')
        
        # Fixed the parameter name from status_code to status
        return HttpResponse(
            status=500,
            detail="An internal server error occurred while fetching the data",
            headers={"WWW-Authenticate": "Bearer"},
        )

from fastapi import Depends, HTTPException
from lakefusion_utility.utils.logging_utils import get_logger
from lakefusion_utility.models.httpresponse import HttpResponse
from databricks.sdk.service.jobs import RunState
import traceback

app_logger = get_logger(__name__)

def get_quality_task_runs(token: str, db, task_id: int):
    """
    Fetches all task runs for a specific quality task from Databricks.
    
    Args:
        token (str): The authentication token for Databricks API
        db: Database session
        task_id (int): The ID of the quality task
        
    Returns:
        HttpResponse: Contains list of task runs with their status and details
    """
    try:
        # Initialize QualityTaskService to get task information
        quality_task_service = QualityTaskService(db)
        quality_task = quality_task_service.get_task(task_id=task_id)
        
        if not quality_task or not quality_task.databricks_job_id:
            return HttpResponse(
                status=404,
                detail=f"No job found for quality task {task_id}"
            )

        # Initialize JobService to get runs
        job_service = JobService(token)
        
        # Get job runs from Databricks
        runs = job_service.w.jobs.list_runs(
            job_id=quality_task.databricks_job_id,
            expand_tasks=True,
            limit=25
        )

        # Transform runs data for frontend
        formatted_runs = []
        for run in runs:
            run_state = run.state.life_cycle_state if run.state else "UNKNOWN"
            state_message = run.state.state_message if run.state else None
            result_state = run.state.result_state if run.state else None
            
            formatted_runs.append({
                "run_id": run.run_id,
                "start_time": run.start_time,
                "end_time": run.end_time,
                "state": run_state,
                "result_state": result_state,
                "state_message": state_message,
                "run_name": run.run_name,
                "run_page_url": run.run_page_url,
                "setup_duration": run.setup_duration,
                "execution_duration": run.execution_duration,
                "cleanup_duration": run.cleanup_duration,
                "creator_user_name": run.creator_user_name,
                "trigger": run.trigger,
                "run_type": run.run_type
            })

        response = {
            "task_id": task_id,
            "job_id": quality_task.databricks_job_id,
            "runs": formatted_runs,
            "total_runs": len(formatted_runs)
        }

        return HttpResponse(status=200, data=response)
        
    except Exception as e:
        message = traceback.format_exc()
        app_logger.exception(f'Failed to fetch task runs. Reason - {message}')
        
        return HttpResponse(
            status=500,
            detail="An internal server error occurred while fetching task runs",
            headers={"WWW-Authenticate": "Bearer"}
        )