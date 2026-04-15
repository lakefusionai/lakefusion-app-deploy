from fastapi import HTTPException, Depends, APIRouter
from sqlalchemy.orm import Session
from app.lakefusion_matchmaven_service.utils.app_db import get_db,token_required_wrapper  # Importing get_db from the specified location
from lakefusion_utility.models.httpresponse import HttpResponse
from lakefusion_utility.services.entity_service import EntityService
from lakefusion_utility.models.match_maven import *
from lakefusion_utility.models.databricks_model import *
from lakefusion_utility.services.model_experiment_service import Model_Databricks_Job_Service
from typing import Optional
from lakefusion_utility.utils.databricks_util import CatalogService 
# Experiments API Router
# This router handles all experiments-related API endpoints.
experiments_router = APIRouter(tags=["Experiments API"], prefix='/entities/{entity_id}/models/{model_id}/experiments')

# Start an experiment for a specific model
@experiments_router.post("/start")
def start_experiment(
    entity_id: int,
    model_id: int,
    is_more_records: bool,
    existing_cluster: Optional[str]=None,
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db)
):
    try:
        created_by = check.get('decoded', {}).get('sub', '')  # Extract created_by from the token
        token = check.get('token', '')  # Extract the token
        
        
        job_response = Model_Databricks_Job_Service(db)
        job_id = job_response.start_experiment(entity_id, model_id, is_more_records, existing_cluster, created_by, token)
        
        ## Commented out the below code as it's now handled within ExperimentService.start_experiment

        # create_volume_folder = CatalogService(token, db)  # creating path in volumne
        # folder_path = create_volume_folder.create_volume_folder(entity_id, model_id)
        
        # entity_service = EntityService(db)
        # entity_data = entity_service.get_full_entity_by_id(entity_id).dict()
        # model_data = experiment_service.get_model(model_id).dict()
        
        # create_volume_folder.upload_data_as_json(folder_path, 'entity.json', entity_data)
        # create_volume_folder.upload_data_as_json(folder_path, 'model.json', model_data)
        
        # job_status = Model_Databricks_Job_Service(db)
        # job_id = job_status.create_and_run_job(entity_id, model_id, entity_data["name"], is_more_records, existing_cluster, created_by, token)
        
        return job_id
        
    except Exception as e:
        print(f"Error in run experiment: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to start an experiment: {str(e)}"
        )

# Repair an experiment run
@experiments_router.post("/repair")
def repair_experiment(entity_id:int,model_id:int,check: dict = Depends(token_required_wrapper),db: Session = Depends(get_db)):
    created_by = check.get('decoded', {}).get('sub', '')  # Extract created_by from the token
    token = check.get('token', '')  # Extract the token
    job_response = Model_Databricks_Job_Service(db)
    job_response_status = job_response.repair_run_job(entity_id,model_id,created_by,token)
    #job_response=JobStatusResponse(**job_response_status)
    return job_response_status

# Cancel an experiment run
@experiments_router.post("/cancel")
def cancel_experiment(entity_id:int,model_id:int,check: dict = Depends(token_required_wrapper),db: Session = Depends(get_db)):
    created_by = check.get('decoded', {}).get('sub', '')  # Extract created_by from the token
    token = check.get('token', '')  # Extract the token
    job_response = Model_Databricks_Job_Service(db)
    job_response_status = job_response.cancel_run_job(entity_id,model_id,created_by,token)
    #job_response=JobStatusResponse(**job_response_status)
    return job_response_status

