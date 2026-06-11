from fastapi import HTTPException, Depends, APIRouter
from sqlalchemy.orm import Session
from app.lakefusion_matchmaven_service.utils.app_db import get_db,token_required_wrapper  # Importing get_db from the specified location
from lakefusion_utility.models.httpresponse import HttpResponse
from lakefusion_utility.services.entity_service import EntityService
from lakefusion_utility.models.match_maven import *
from lakefusion_utility.models.databricks_model import *
from lakefusion_utility.services.model_experiment_service import ExperimentService,Model_Databricks_Job_Service
from typing import List,Union,Optional
from lakefusion_utility.utils.databricks_util import FoundationModelService,DatabricksJobManager,CatalogService,DataSetSQLService,VectorSearchService
from lakefusion_utility.services.feature_flags_service import FeatureFlagService
from lakefusion_utility.services.integration_hub_service import Integration_HubService
from lakefusion_utility.services.model_experiment_service import compare_versions



# Initialize the router with a prefix and tag
match_maven_router = APIRouter(tags=["Match Maven API"], prefix='/match_maven')

@match_maven_router.get("/foundation-model")
def list_foundation_model(check: dict = Depends(token_required_wrapper)):
    """
    Update the model's info with fields provided in the request body.
    """
    token = check.get('token')
    list_foundation_model = FoundationModelService(token)
    return list_foundation_model.list_foundation_models()

@match_maven_router.get("/vs-endpoint")
def list_foundation_model(check: dict = Depends(token_required_wrapper)):
    """
    Update the model's info with fields provided in the request body.
    """
    token = check.get('token')
    list_endpoints= VectorSearchService(token)
    return list_endpoints.list_vectorsearch_endpoint()

@match_maven_router.get("/base-instructions")
def base_instructions(db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    """
    The base instructions used while creating a new model.
    """
    # token = check.get('token')
    experiment_service = ExperimentService(db)
    # list_endpoints= VectorSearchService(token)
    return experiment_service.get_base_instructions()


@match_maven_router.get("/{entity_id}/potential-matches")
async def get_potential_matches_prod(
    entity_id: int,
    warehouse_id: str,
    page: int = 1,
    page_size: int = 1000,
    show_merged: bool = False,
    filters: Optional[str] = None,
    scoreFilter: Optional[str] = None,
    check: dict = Depends(token_required_wrapper), 
    db: Session = Depends(get_db)
):
    token = check.get('token')
    experiment_service = ExperimentService(db)
    return experiment_service.fetch_potential_matches_prod(
        entity_id=entity_id,
        token=token, 
        warehouse_id=warehouse_id,
        page=page,
        page_size=page_size,
        show_merged=show_merged,
        filters=filters,
        scoreFilter=scoreFilter
    )


@match_maven_router.get("/{entity_id}/potential-matches-deduplicate")
async def get_potential_matches_deduplicate_prod(
    entity_id: int,
    warehouse_id: str,
    page: int = 1,
    page_size: int = 1000,
    show_merged: bool = False,
    filters: Optional[str] = None,
    scoreFilter: Optional[str] = None,  
    check: dict = Depends(token_required_wrapper), 
    db: Session = Depends(get_db)
):
    token = check.get('token')
    experiment_service = ExperimentService(db)
    return experiment_service.fetch_potential_matches_deduplicate_prod(
        entity_id=entity_id,
        token=token, 
        warehouse_id=warehouse_id,
        page=page,
        page_size=page_size,
        show_merged=show_merged,
        filters=filters,
        scoreFilter=scoreFilter
    )

@match_maven_router.get("/{entity_id}/potential-matches-dnbintegration")
async def get_potential_matches_dnbintegration_prod(
    entity_id: int,
    warehouse_id: str,
    page: int = 1,
    page_size: int = 1000,
    show_merged: bool = False,
    filters: Optional[str] = None,
    check: dict = Depends(token_required_wrapper), 
    db: Session = Depends(get_db)
):
    token = check.get('token')
    experiment_service = ExperimentService(db)
    return experiment_service.fetch_dnb_matches_prod(
        entity_id=entity_id,
        token=token, 
        warehouse_id=warehouse_id,
        page=page,
        page_size=page_size,
        show_merged=show_merged,
        filters=filters
    )

@match_maven_router.get("/{entity_id}/profile/{profile_id}/potential-matches")
async def get_profile_potential_matches(
    entity_id: int,
    profile_id: str,
    warehouse_id: str,
    check: dict = Depends(token_required_wrapper), 
    db: Session = Depends(get_db)
):
    token = check.get('token')
    experiment_service = ExperimentService(db)
    entity_version = Integration_HubService.get_integration_hub_task_version(db, entity_id)
    print("entity_version", entity_version)
    if compare_versions(entity_version, "4.0.0") != -1:
        return experiment_service.fetch_profile_potential_matches_updated(
            entity_id=entity_id, 
            profile_id=profile_id,
            token=token, 
            warehouse_id=warehouse_id
        )
    else:
        return experiment_service.fetch_profile_potential_matches(
            entity_id=entity_id, 
            profile_id=profile_id,
            token=token, 
            warehouse_id=warehouse_id
        )
    
@match_maven_router.get("/{entity_id}/profile/{profile_id}/potential-matches-dnbintegration")
async def get_profile_potential_matches(
    entity_id: int,
    profile_id: str,
    warehouse_id: str,
    check: dict = Depends(token_required_wrapper), 
    db: Session = Depends(get_db)
):
    token = check.get('token')
    experiment_service = ExperimentService(db)
    return experiment_service.fetch_profile_potential_matches_dnbintegration(
        entity_id=entity_id, 
        profile_id=profile_id,
        token=token, 
        warehouse_id=warehouse_id
    )

@match_maven_router.get("/{entity_id}/profile/{profile_id}/dnb-hierarchy-enrichment")
async def get_profile_dnb_hierarchy_enrichment(
    entity_id: int,
    profile_id: str,
    warehouse_id: str,
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db)
):
    token = check.get('token')
    experiment_service = ExperimentService(db)
    return experiment_service.fetch_profile_dnb_hierarchy_enrichment(
        entity_id=entity_id,
        profile_id=profile_id,
        token=token,
        warehouse_id=warehouse_id
    )

@match_maven_router.get("/{entity_id}/profile/{profile_id}/potential-matches-deduplicate")
async def get_profile_potential_matches_deduplicate(
    entity_id: int,
    profile_id: str,
    warehouse_id: str,
    check: dict = Depends(token_required_wrapper), 
    db: Session = Depends(get_db)
):
    token = check.get('token')
    experiment_service = ExperimentService(db)
    return experiment_service.fetch_profile_potential_matches_deduplicate(
        entity_id=entity_id, 
        profile_id=profile_id,
        token=token, 
        warehouse_id=warehouse_id
    )

@match_maven_router.get("/{entity_id}/lineage-link")
async def get_lineage_link(
    entity_id: int,
    warehouse_id: str,
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db)
):
    """
    Get the lineage link for a specific entity's master table.
    """
    token = check.get('token')
    experiment_service = ExperimentService(db)
    return experiment_service.get_lineage_link(entity_id=entity_id, token=token, warehouse_id=warehouse_id)

@match_maven_router.get("/get-match-maven-job-status",response_model=JobStatusResponse)
def get_status_databricks_job(job_run_id:int,check: dict = Depends(token_required_wrapper),db: Session = Depends(get_db)):
    created_by = check.get('decoded', {}).get('sub', '')  # Extract created_by from the token
    token = check.get('token', '')  # Extract the token
    job_response = Model_Databricks_Job_Service(db)
    job_response_status = job_response.get_job_run_status(job_run_id,token)
    job_response=JobStatusResponse(**job_response_status)
    return job_response



