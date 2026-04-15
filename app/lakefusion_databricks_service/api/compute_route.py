from fastapi import APIRouter, Depends, HTTPException
from lakefusion_utility.models.httpresponse import HttpResponse
from lakefusion_utility.models.dbconfig import DBConfigProperties
from lakefusion_utility.utils.logging_utils import get_logger
from app.lakefusion_databricks_service.services.compute_service import list_clusters, list_warehouses,get_warehouse
from sqlalchemy.orm import Session
from app.lakefusion_databricks_service.utils.app_db import get_db,token_required_wrapper
from typing import List
from databricks.sdk.service.compute import ClusterDetails
from databricks.sdk.service.sql import EndpointInfo

app_logger = get_logger(__name__)

compute_router = APIRouter(tags=["Compute Management API"], prefix='/compute')

@compute_router.get("/list", response_model=List[ClusterDetails])
async def list_clusters_route(check: dict = Depends(token_required_wrapper), db: Session = Depends(get_db)):
    try:
        token = check.get('token')
        return list_clusters(token=token)
    except Exception as e:
        return HTTPException(status_code=500, detail="Failed to get clusters list")

@compute_router.get("/list-warehouses", response_model=List[EndpointInfo])
async def list_warehouses_route(check: dict = Depends(token_required_wrapper), db: Session = Depends(get_db)):
    try:
        token = check.get('token')
        return list_warehouses(token=token)
    except Exception as e:
        return HTTPException(status_code=500, detail="Failed to get warehouses list")
    
@compute_router.get("/get-warehouses/{warehouse_id}")
async def get_warehouses_route(warehouse_id:str,check: dict = Depends(token_required_wrapper), db: Session = Depends(get_db)):
    try:
        token = check.get('token')
        return get_warehouse(token=token,warehouse_id=warehouse_id)
    except Exception as e:
        return HTTPException(status_code=500, detail=f"Failed to get warehouse of {warehouse_id}")