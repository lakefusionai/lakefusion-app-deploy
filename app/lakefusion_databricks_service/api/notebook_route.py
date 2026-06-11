from fastapi import APIRouter, Depends, HTTPException
from lakefusion_utility.utils.logging_utils import get_logger
from app.lakefusion_databricks_service.services.notebook_service import list_workspace_contents
from sqlalchemy.orm import Session
from app.lakefusion_databricks_service.utils.app_db import get_db,token_required_wrapper
from typing import List
import os
from databricks.sdk.service.workspace import ObjectInfo

# Initialize logger for the Notebook Management API
app_logger = get_logger(__name__)

DATABRICKS_HOST = os.environ.get('DATABRICKS_HOST', 'https://databricks.com')

# Initialize API Router for notebook-related operations with the prefix '/catalog'
notebook_router = APIRouter(tags=["Notebook Management API"], prefix='/notebook')

# Route to list all catalogs in Databricks
# This route requires a valid token for authentication and connects to a database session.
# Args:
#     check (dict): The token check dependency to ensure the request is authenticated.
#     db (Session): The database session, if needed (though unused in this route).
# Returns:
#     List of CatalogInfo objects representing the catalogs in Databricks.
@notebook_router.get("/list-notebooks", response_model=List[ObjectInfo])
async def list_notebooks(path: str = '/', check: dict = Depends(token_required_wrapper)):
    try:
        # Extract the token from the authentication check
        token = check.get('token')
        data = list_workspace_contents(path=path, token=token)
        return data
    except Exception as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail, headers=e.headers)
    except Exception as e:
        app_logger.exception(f"An unexpected error occurred while listing notebooks at path {path}.")
        raise HTTPException(status_code=500, detail=f"Internal server error occurred while listing notebooks at path {path}")
