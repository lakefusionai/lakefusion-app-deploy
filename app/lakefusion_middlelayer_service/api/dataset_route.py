from fastapi import HTTPException, Depends, APIRouter, Body
from sqlalchemy.orm import Session
from app.lakefusion_middlelayer_service.utils.app_db import get_db,token_required_wrapper # Importing get_db from the specified location
from lakefusion_utility.models.dataset import DatasetCreate, DatasetResponse  # Import your Pydantic models
from lakefusion_utility.models.httpresponse import HttpResponse
from lakefusion_utility.services.dataset_service import DatasetService  # Import the DatasetService class
from typing import List, Optional
from lakefusion_utility.models.api_response import ApiResponse
from app.lakefusion_middlelayer_service.config import AppConfig


# Initialize the router with a prefix and tag
dataset_router = APIRouter(tags=["Dataset API"], prefix='/dataset')


# Create a new dataset
@dataset_router.post("/", response_model=ApiResponse[DatasetResponse])
def create_dataset(dataset: DatasetCreate, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    dataset.created_by = check.get('decoded', {}).get('sub', '')
    service = DatasetService(db)  # Create an instance of DatasetService
    return service.create_dataset(dataset)

# Read all datasets with an optional `is_active` filter
@dataset_router.get("/", response_model=List[DatasetResponse])
def read_datasets(is_active: Optional[bool] = None, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = DatasetService(db)
    return service.read_datasets(is_active)

# Delete a dataset by id
@dataset_router.delete("/{dataset_id}")
def delete_dataset(dataset_id: int, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = DatasetService(db)  # Create an instance of DatasetService
    return service.delete_dataset(dataset_id)

# Retrieve a dataset by ID
@dataset_router.get("/{dataset_id}", response_model=DatasetResponse)
def get_dataset(dataset_id: int, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = DatasetService(db)  # Create an instance of DatasetService
    dataset = service.get_dataset_by_id(dataset_id)
    if dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return dataset

# Toggle `is_active` flag for a dataset
@dataset_router.patch("/{dataset_id}/toggle", response_model=DatasetResponse)
def toggle_dataset_active(dataset_id: int, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = DatasetService(db)
    dataset = service.toggle_dataset_active(dataset_id)
    if dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return dataset

# Set `primary_field` to a dataset 
@dataset_router.patch("/{dataset_id}/primary-field")
def update_dataset_primary_field(
    dataset_id: int, 
    db: Session = Depends(get_db), 
    primary_field: str = Body(..., embed=True), 
    check: dict = Depends(token_required_wrapper)
    ):
    service = DatasetService(db)  # Create an instance of DatasetService
    return service.update_dataset_primary_field(dataset_id, primary_field)

@dataset_router.get("/{dataset_id}/check-cleaned-table", response_model=dict)
def check_cleaned_table_exists(dataset_id: int, dataset_path: str, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = DatasetService(db)  # Create an instance of DatasetService
    token = check.get('token', '')
    return  service.check_cleaned_table_exists(token, dataset_id, dataset_path)
    
@dataset_router.get("/{dataset_id}/check-path-exists", response_model=dict)
def check_dataset_path_exists_route(
    dataset_id:int,
    dataset_path:str,
    check: dict = Depends(token_required_wrapper), 
    db: Session = Depends(get_db)
):
    # Extract the token from the authentication check
    token = check.get('token')
    service = DatasetService(db)
    return service.check_dataset_path_exists(token, dataset_path)
