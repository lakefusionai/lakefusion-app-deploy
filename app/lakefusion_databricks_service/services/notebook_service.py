from fastapi.security import HTTPBearer
from lakefusion_utility.utils.logging_utils import get_logger
from lakefusion_utility.utils.databricks_util import NotebookService
from lakefusion_utility.models.httpresponse import HttpResponse
import traceback

# Initialize an HTTPBearer security scheme for token-based authentication
token_auth_scheme = HTTPBearer()

# Initialize logger for the application
app_logger = get_logger(__name__)

# Function to fetch directory contents from a given path in the Databricks workspace.
# This function requires an authentication token and a path.
# Args:
#     token (str): The authentication token used to validate and fetch workspace contents.
#     path (str): The workspace path from which contents are to be listed.
# Returns:
#     List of workspace objects retrieved from the specified path if successful.
# Raises:
#     HTTPException: Returns a 500 internal server error response if an exception occurs.
def list_workspace_contents(token: str, path: str):
    try:
        # Initialize the NotebookService with the provided token
        notebook_service = NotebookService(token=token)
        
        # Fetch the list of workspace contents for the specified path
        data = notebook_service.list_workspace_contents(path=path)
        
        # Return the retrieved directory contents
        return data
    except Exception as e:
        # Capture the traceback in case of an exception and log the error message
        message = traceback.format_exc()
        app_logger.exception(f"Unable to fetch workspace contents for path '{path}'. Reason - {message}")
        
        # Raise an HTTPException with a 500 status code, indicating internal server error
        raise HttpResponse(
            status_code=500,
            detail=f"An internal server error occurred while fetching the workspace contents for path '{path}'",
            headers={"WWW-Authenticate": "Bearer"},
        )