from fastapi import HTTPException, Depends, APIRouter, Query
from pydantic import BaseModel as PydanticBaseModel
from sqlalchemy.orm import Session
from app.lakefusion_middlelayer_service.utils.app_db import get_db,token_required_wrapper  # Importing get_db from the specified location
from lakefusion_utility.models.httpresponse import HttpResponse
from lakefusion_utility.models.match_maven import PotentialMatchesResponse, PotentialMatchDeduplicationRecord
from lakefusion_utility.services.entity_search_service import EntitySearchService # Import the EntityService class
from lakefusion_utility.services.integration_hub_service import Integration_HubService
from lakefusion_utility.services.model_experiment_service import compare_versions
from lakefusion_utility.services.steward_reason_service import StewardReasonService
from typing import List,Optional
from lakefusion_utility.models.entity import EntityResponseTags
from lakefusion_utility.services.feature_flags_service import FeatureFlagService


class StewardReasonRequest(PydanticBaseModel):
    reason: str
    reason_category: Optional[str] = None

# Initialize the router with a prefix and tag
entity_search_router = APIRouter(tags=["Entity Search API"], prefix='/entity-search')


# Read all Entitys with an optional `is_active` filter
@entity_search_router.get("/{entity_id}/profile")
async def read_entity_search_profile(entity_id:int,warehouse_id:str,db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntitySearchService(db)
    token = check.get('token')
    return service.read_entity_search_profile(entity_id,warehouse_id,token)

@entity_search_router.get("/{entity_id}/profile/{id_value}")
async def read_entity_search_profile_id(entity_id:int,id_value:str,warehouse_id:str,db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntitySearchService(db)
    token = check.get('token')
    return service.read_entity_search_profile_id(entity_id,id_value,warehouse_id,token)

@entity_search_router.patch("/{entity_id}/profile/{id_value}")
async def update_entity_search_profile_id(entity_id:int,id_value:str,update_attributes:dict, warehouse_id:str, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntitySearchService(db)
    token = check.get('token')
    created_by = check.get('decoded', {}).get('sub', '')
    return service.update_entity_search_profile(entity_id=entity_id,id_value=id_value,attributes_values=update_attributes,warehouse_id=warehouse_id,token=token,created_by=created_by)

@entity_search_router.post("/{entity_id}/run-potential-matches")
async def get_entity_match_merge(entity_id:int,potential_record:PotentialMatchesResponse,warehouse_id:Optional[str]=None,db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntitySearchService(db)
    entity_version = Integration_HubService.get_integration_hub_task_version(db, entity_id)
    token = check.get('token')
    created_by = check.get('decoded', {}).get('sub', '')

    # SQL stewardship path (feature-flagged)
    sql_stewardship_enabled = FeatureFlagService._is_feature_flag_enabled(db, "ENABLE_SQL_STEWARDSHIP")
    if sql_stewardship_enabled and warehouse_id:
        if potential_record.operation_type in ['MERGE', 'MERGE_ALL']:
            return service.run_match_merge_sql(entity_id, potential_record, token, created_by, warehouse_id)
        elif potential_record.operation_type == 'NOT_A_MATCH':
            return service.run_not_a_match_sql(entity_id, potential_record, token, created_by, warehouse_id)

    # Existing notebook job path
    if compare_versions(entity_version, "4.0.0") != -1:
        return service.run_match_merge_updated(entity_id,potential_record,token,created_by)
    else:
        return service.run_match_merge(entity_id,potential_record,token,created_by)

@entity_search_router.post("/{entity_id}/run-potential-matches-deduplication")
async def get_entity_match_merge_deduplication(entity_id:int,potential_record:PotentialMatchDeduplicationRecord,warehouse_id:Optional[str]=None,db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntitySearchService(db)
    entity_version = Integration_HubService.get_integration_hub_task_version(db, entity_id)
    token = check.get('token')
    created_by = check.get('decoded', {}).get('sub', '')

    # SQL stewardship path (feature-flagged)
    sql_stewardship_enabled = FeatureFlagService._is_feature_flag_enabled(db, "ENABLE_SQL_STEWARDSHIP")
    if sql_stewardship_enabled and warehouse_id:
        if potential_record.operation_type in ['MERGE', 'MERGE_ALL']:
            return service.run_golden_dedup_merge_sql(entity_id, potential_record, token, created_by, warehouse_id)
        elif potential_record.operation_type == 'NOT_A_MATCH':
            return service.run_golden_dedup_not_a_match_sql(entity_id, potential_record, token, created_by, warehouse_id)

    # Existing notebook job path
    if compare_versions(entity_version, "4.0.0") != -1:
        return service.run_match_merge_deduplication_updated(entity_id,potential_record,token,created_by)
    else:
        return service.run_match_merge_deduplication(entity_id,potential_record,token,created_by)

@entity_search_router.post("/{entity_id}/run-unmerge")
async def run_entity_unmerge(entity_id: int, unmerge_record: PotentialMatchesResponse, warehouse_id: Optional[str] = None, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntitySearchService(db)
    entity_version = Integration_HubService.get_integration_hub_task_version(db, entity_id)
    token = check.get('token')
    created_by = check.get('decoded', {}).get('sub', '')

    # SQL stewardship path (feature-flagged)
    sql_stewardship_enabled = FeatureFlagService._is_feature_flag_enabled(db, "ENABLE_SQL_STEWARDSHIP")
    if sql_stewardship_enabled and warehouse_id:
        return service.run_unmerge_sql(entity_id, unmerge_record, token, created_by, warehouse_id)

    # Existing notebook job path
    if compare_versions(entity_version, "4.0.0") != -1:
        return service.run_unmerge_updated(entity_id, unmerge_record, token, created_by)
    else:
        return service.run_unmerge(entity_id, unmerge_record, token, created_by)
    
@entity_search_router.post("/{entity_id}/force-merge")
async def get_entity_force_merge(entity_id:int,potential_record:PotentialMatchDeduplicationRecord,warehouse_id:Optional[str]=None,db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntitySearchService(db)
    entity_version = Integration_HubService.get_integration_hub_task_version(db, entity_id)
    token = check.get('token')
    created_by = check.get('decoded', {}).get('sub', '')

    # SQL stewardship path (feature-flagged)
    sql_stewardship_enabled = FeatureFlagService._is_feature_flag_enabled(db, "ENABLE_SQL_STEWARDSHIP")
    if sql_stewardship_enabled and warehouse_id:
        return service.run_force_merge_sql(entity_id, potential_record, token, created_by, warehouse_id)

    # Existing notebook job path
    if compare_versions(entity_version, "4.0.0") != -1:
        return service.run_force_merge_deduplication_updated(entity_id,potential_record,token,created_by)
    else:
        return service.run_force_merge_deduplication(entity_id,potential_record,token,created_by)


@entity_search_router.post("/{entity_id}/{id_value}/update-potential-matches")
def update_entity_search(entity_id:int,id_value:str,attributes_values:dict,warehouse_id:str,db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntitySearchService(db)
    token = check.get('token')
    created_by = check.get('decoded', {}).get('sub', '')
    return service.update_entity_search(entity_id,id_value,attributes_values,warehouse_id,token,created_by)

@entity_search_router.post("/{entity_id}/run-potential-matches-dnbintegration")
async def get_entity_match_merge_dnbintegration(entity_id:int,potential_record:PotentialMatchesResponse,db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntitySearchService(db)
    token = check.get('token')
    created_by = check.get('decoded', {}).get('sub', '')
    return service.run_match_merge_dnbintegration(entity_id,potential_record,token,created_by)

@entity_search_router.get("/{entity_id}/merge-statuses/{master_id}")
async def get_merge_statuses(
    entity_id: int,
    master_id: str,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    service = EntitySearchService(db)
    return service.get_merge_statuses(entity_id, master_id)

@entity_search_router.get("/{entity_id}/master/{master_id}/match/{match_id}/force-merge-status")
async def force_merge_status(
    entity_id: int,
    master_id: str,
    match_id: str,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    service = EntitySearchService(db)
    return service.get_force_merge_status(entity_id, master_id, match_id)

@entity_search_router.get("/{entity_id}/profile/{profile_id}/sources")
async def get_attribute_sources(entity_id:int, profile_id:str, warehouse_id:str,db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntitySearchService(db)
    token = check.get('token')
    return service.get_attribute_sources(entity_id,profile_id,warehouse_id,token)

@entity_search_router.get("/{entity_id}/profile/{profile_id}/history")
async def get_merge_activities(entity_id:int, profile_id:str, warehouse_id:str,db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntitySearchService(db)
    token = check.get('token')
    return service.get_merge_activities(entity_id,profile_id,warehouse_id,token)

@entity_search_router.get("/{entity_id}/all-records")
async def get_all_records(
    entity_id: int, 
    warehouse_id: str, 
    page: int = 1,
    page_size: int = 1000,
    filters: Optional[str] = None, 
    db: Session = Depends(get_db), 
    check: dict = Depends(token_required_wrapper)
):
    service = EntitySearchService(db)
    token = check.get('token')
    return service.get_all_records(entity_id, warehouse_id, token, page, page_size, filters)

@entity_search_router.get("/{entity_id}/validation-data")
async def read_all_validation_records(entity_id:int, warehouse_id:str,validation_type:str,db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntitySearchService(db)
    token = check.get('token')
    return service.read_entity_validation_data(entity_id,warehouse_id,validation_type,token)

@entity_search_router.get("/{entity_id}/profile/validation-records-id")
async def read_entity_search_profile_validation_id(entity_id:int,id_value:str,warehouse_id:str,validation_type:str,db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntitySearchService(db)
    token = check.get('token')
    return service.read_entity_search_profile_validation_id(entity_id,id_value,warehouse_id,validation_type,token)

@entity_search_router.get("/{entity_id}/profile/validation-records")
async def read_entity_search_profile_validation(entity_id:int,warehouse_id:str,validation_type:str,db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntitySearchService(db)
    token = check.get('token')
    return service.read_entity_search_profile_validation(entity_id,warehouse_id,validation_type,token)

@entity_search_router.post("/{entity_id}/accept-dnb-candidate")
async def accept_dnb_candidate(entity_id: int, payload: dict, warehouse_id: str, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntitySearchService(db)
    token = check.get('token')
    return service.accept_dnb_candidate(entity_id, payload["master_id"], payload["dnb_duns_value"], warehouse_id, token)

@entity_search_router.post("/{entity_id}/reject-all-dnb-candidates")
async def reject_all_dnb_candidates(entity_id: int, payload: dict, warehouse_id: str, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntitySearchService(db)
    token = check.get('token')
    return service.reject_all_dnb_candidates(entity_id, payload["master_id"], warehouse_id, token)


@entity_search_router.patch("/{entity_id}/steward-reason/{job_id}")
async def add_steward_reason(entity_id: int, job_id: int, request: StewardReasonRequest, warehouse_id: Optional[str] = None, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = StewardReasonService(db)
    token = check.get('token')
    updated_by = check.get('decoded', {}).get('sub', '')
    return service.add_reason(entity_id, job_id, request.reason, request.reason_category, updated_by, token=token, warehouse_id=warehouse_id)


@entity_search_router.get("/{entity_id}/stewardship-history")
async def get_stewardship_history(
    entity_id: int,
    warehouse_id: Optional[str] = Query(None),
    action_type: Optional[str] = Query(None),
    has_reason: Optional[bool] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    token = check.get('token')
    service = StewardReasonService(db)
    return service.get_stewardship_history(entity_id, token, warehouse_id, action_type, has_reason, page, page_size)
