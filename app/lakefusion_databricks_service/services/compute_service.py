from fastapi import Depends, HTTPException, Response
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from lakefusion_utility.utils.logging_utils import get_logger
from lakefusion_utility.utils.databricks_util import ComputeService
from lakefusion_utility.models.httpresponse import HttpResponse
from databricks.sdk.service.compute import ListClustersFilterBy, ClusterSource
import traceback

# Initialize security scheme and logger
token_auth_scheme = HTTPBearer()

app_logger = get_logger(__name__)

# Dependency to require a valid token, with an optional flag to run Databricks validation
def list_clusters(token: str):
    try:
        cs = ComputeService(token=token)
        data = cs.list_clusters(filter_by=ListClustersFilterBy(is_pinned=True, cluster_sources=[ClusterSource.UI]))
        return data
    except Exception as e:
        message = traceback.format_exc()
        app_logger.exception(f'Unable to fetch the clusters list. Reason - {message}')
        raise HttpResponse(
            status_code=500,
            detail="An internal server error occurred while fetching the clusters list",
            headers={"WWW-Authenticate": "Bearer"},
        )

# Dependency to require a valid token, with an optional flag to run Databricks validation
def list_warehouses(token: str):
    try:
        cs = ComputeService(token=token)
        data = cs.list_warehouses()
        return data
    except Exception as e:
        message = traceback.format_exc()
        app_logger.exception(f'Unable to fetch the warehouses list. Reason - {message}')
        raise HttpResponse(
            status_code=500,
            detail="An internal server error occurred while fetching the warehouses list",
            headers={"WWW-Authenticate": "Bearer"},
        )
    

def get_warehouse(token: str,warehouse_id:str):
    try:
        cs = ComputeService(token=token)
        data = cs.get_warehouse(warehouse_id=warehouse_id)
        return data
    except Exception as e:
        message = traceback.format_exc()
        app_logger.exception(f'Unable to fetch the warehouses list. Reason - {message}')
        raise HttpResponse(
            status_code=500,
            detail="An internal server error occurred while fetching the warehouses list",
            headers={"WWW-Authenticate": "Bearer"},
        )