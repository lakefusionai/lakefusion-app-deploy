from fastapi import HTTPException, Depends, APIRouter
from lakefusion_utility.models.api_response import ApiResponse
from sqlalchemy.orm import Session
from app.lakefusion_matchmaven_service.utils.app_db import get_db,token_required_wrapper  # Importing get_db from the specified location
#from app.lakefusion_matchmaven_service.models.match_maven import Model_ExperimentCreate,Model_ExperimentResponse, Model_ExperimentUpdateRequest
#from app.lakefusion_matchmaven_service.models.databricks_model_job import Model_Databricks_Job_Create,JobStatusResponse
from lakefusion_utility.models.httpresponse import HttpResponse
from lakefusion_utility.services.entity_service import EntityService
from lakefusion_utility.models.match_maven import *
from lakefusion_utility.models.databricks_model import *
from lakefusion_utility.services.model_experiment_service import ExperimentService,Model_Databricks_Job_Service
from typing import List,Union,Optional
from lakefusion_utility.utils.databricks_util import FoundationModelService,DatabricksJobManager,CatalogService,DataSetSQLService,VectorSearchService
from lakefusion_utility.models.match_maven import Model_GetExperimentResponse
import io
import csv
from fastapi import APIRouter, HTTPException, Query, Depends
from fastapi.responses import StreamingResponse


# Models API Router
# This router handles all model-related API endpoints.
models_router = APIRouter(tags=["Models API"], prefix='/entities/{entity_id}/models')

#--------------------------------------------------------------#
# Get a list of models for an entity
@models_router.get("/", response_model=List[Model_GetExperimentResponse])
def list_models(entity_id: int, db: Session = Depends(get_db),check: dict = Depends(token_required_wrapper)):
    """
    Fetch available model details for a given entity (ID).
    
    MIGRATION NOTICE:
    - Previous endpoint: GET /match_maven/list-models (DEPRECATED)
    - New endpoint: GET /entities/{entity_id}/models
    
    Changes made:
    - Moved to nested resource structure following REST conventions
    - Added entity_id as path parameter for proper resource hierarchy
    - Maintained same response structure for backward compatibility
    """
    experiment_service = ExperimentService(db)
    return experiment_service.list_models(entity=entity_id)

#--------------------------------------------------------------#
# CRUD Operations for Models

# Create a new model for an entity
@models_router.post("/")
def create_model(experiment_create: Model_ExperimentCreate, db: Session = Depends(get_db),check: dict = Depends(token_required_wrapper)):
    """
    Endpoint to create an experiment model and store its details in the database.
    
    MIGRATION NOTICE:
    - Previous endpoint: POST /match_maven/create-model (DEPRECATED)
    - New endpoint: POST /entities/{entity_id}/models
    
    Changes made:
    - Moved to nested resource structure following REST conventions
    - Added entity_id as path parameter for proper resource hierarchy
    - Maintained same response structure for backward compatibility
    """
    print("-------------------",experiment_create)
    experiment_create.created_by = check.get('decoded', {}).get('sub', '')
   
    experiment_service = ExperimentService(db)
    return experiment_service.create_experiment(experiment_create)

# Get a model by its ID
@models_router.get("/{model_id}", response_model=Model_ExperimentResponse)
def get_model_by_id(model_id: int, db: Session = Depends(get_db),check: dict = Depends(token_required_wrapper)):
    """
    Fetch model and configuration details for a specific model ID.
    
    MIGRATION NOTICE:
    - Previous endpoint: GET /match_maven/get-model (DEPRECATED)
    - New endpoint: GET /entities/{entity_id}/models/{model_id}
    
    Changes made:
    - Moved to nested resource structure following REST conventions
    - Changed model_id from query parameter to path parameter
    - Added entity_id as path parameter for proper resource hierarchy
    - Maintained same response structure for backward compatibility
    """
    experiment_service = ExperimentService(db)
    return experiment_service.get_model(model_id)

# Update a model by its ID
@models_router.put("/{model_id}", response_model=Model_ExperimentResponse)
def update_model(entity_id:int,model_id: int,request: Model_ExperimentUpdateRequest,is_integration_hub:bool=False,db: Session = Depends(get_db),check: dict = Depends(token_required_wrapper)):
    """
    Update the model's info with fields provided in the request body.
    """
    created_by = check.get('decoded', {}).get('sub', '')  # Extract created_by from the token
    token = check.get('token', '')  # Extract the token
    experiment_service = ExperimentService(db)
    updated_model = experiment_service.update_model(model_id, request)
    model_data = experiment_service.get_model(model_id).dict()
    create_volume_folder = CatalogService(token, db)  # creating path in volumne
    model_id='prod' if(is_integration_hub) else model_id
    folder_path = create_volume_folder.create_volume_folder(entity_id, model_id)
    create_volume_folder.upload_data_as_json(folder_path, 'model.json', model_data)
    return updated_model

# Delete a model by its ID
@models_router.delete("/{model_id}")
def delete_model(model_id: int, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    """
    Delete a model by its ID.
    
    MIGRATION NOTICE:
    - Previous endpoint: DELETE /match_maven/delete-model (DEPRECATED)
    - New endpoint: DELETE /entities/{entity_id}/models/{model_id}
    
    Changes made:
    - Moved to nested resource structure following REST conventions
    - Changed model_id from query parameter to path parameter
    - Added entity_id as path parameter for proper resource hierarchy
    - Maintained same response structure for backward compatibility
    """
    experiment_service = ExperimentService(db)
    return experiment_service.delete_model(model_id)

#----------------------------------------------------------------#
# Potential Matches API
# Get potential matches for an entity using a specific model
@models_router.get("/{model_id}/potential-matches")
async def get_potential_matches_by_model(
    entity_id: int,
    model_id: int,
    warehouse_id: str,
    page: int = 1,
    page_size: int = 1000,
    filters: Optional[str] = None,
    scoreFilter: Optional[str] = None,
    check: dict = Depends(token_required_wrapper), 
    db: Session = Depends(get_db)
):
    """
    Fetch potential matches for an entity using a specific model.
    
    MIGRATION NOTICE:
    - Previous endpoint: GET /match_maven/{entity_id}/{model_id}/potential-matches (DEPRECATED)
    - New endpoint: GET /entities/{entity_id}/models/{model_id}/potential-matches
    
    Changes made:
    - Moved to nested resource structure following REST conventions
    - Restructured URL hierarchy: entities → models → potential-matches
    - Maintained same query parameters and response structure for backward compatibility
    """
    token = check.get('token')
    experiment_service = ExperimentService(db)
    return experiment_service.fetch_potential_matches(
        entity_id=entity_id, 
        model_id=model_id,
        token=token, 
        warehouse_id=warehouse_id,
        page=page,
        page_size=page_size,
        filters=filters,
        scoreFilter=scoreFilter
    )


@models_router.get("/{model_id}/deterministic-matches")
async def get_deterministic_matches_by_model(
    entity_id: int,
    model_id: int,
    action_type: ActionType,
    warehouse_id: str,
    page: int = 1,
    page_size: int = 1000,
    filters: Optional[str] = None,
    scoreFilter: Optional[str] = None,
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db)
):
    """
    Fetch potential matches for an entity using a specific model.
    
    MIGRATION NOTICE:
    - Previous endpoint: GET /match_maven/{entity_id}/{model_id}/potential-matches (DEPRECATED)
    - New endpoint: GET /entities/{entity_id}/models/{model_id}/potential-matches
    
    Changes made:
    - Moved to nested resource structure following REST conventions
    - Restructured URL hierarchy: entities → models → potential-matches
    - Maintained same query parameters and response structure for backward compatibility
    """
    token = check.get('token')
    experiment_service = ExperimentService(db)
    return experiment_service.fetch_deterministic_matches(
        entity_id=entity_id, 
        model_id=model_id,
        action_type = action_type.value,
        token=token, 
        warehouse_id=warehouse_id,
        page=page,
        page_size=page_size,
        filters=filters,
        scoreFilter=scoreFilter
    )

@models_router.get("/{model_id}/potential-matches/download")
async def download_potential_matches_csv(
    entity_id: int,
    model_id: int,
    warehouse_id: str,
    page: int = 1,
    page_size: int = 1000,
    filters: Optional[str] = None,
    scoreFilter: Optional[str] = None,
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db)
):
    try:
        token = check.get('token')

        experiment_service = ExperimentService(db)
        result = experiment_service.fetch_potential_matches_download(
            entity_id=entity_id,
            model_id=model_id,
            token=token,
            warehouse_id=warehouse_id,
            page=page,
            page_size=page_size,
            filters=filters,
            scoreFilter=scoreFilter
        )
        rows = result.get("data", [])
            
        if not rows:
            raise HTTPException(status_code=404, detail="No data found")

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        output.seek(0)

        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={entity_id}_Model_{model_id}_potential_matches.csv"}
        )
            

       

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate CSV: {str(e)}")
