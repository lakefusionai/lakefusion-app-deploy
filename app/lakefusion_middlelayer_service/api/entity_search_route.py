from fastapi import HTTPException, Depends, APIRouter, Query, Body
from pydantic import BaseModel as PydanticBaseModel
from sqlalchemy.orm import Session
from app.lakefusion_middlelayer_service.utils.app_db import get_db,token_required_wrapper  # Importing get_db from the specified location
from lakefusion_utility.models.httpresponse import HttpResponse
from lakefusion_utility.models.match_maven import PotentialMatchesResponse, PotentialMatchDeduplicationRecord
from lakefusion_utility.services.entity_search_service import EntitySearchService # Import the EntityService class
from lakefusion_utility.services.integration_hub_service import Integration_HubService
from lakefusion_utility.services.model_experiment_service import compare_versions
from lakefusion_utility.services.steward_reason_service import StewardReasonService
from typing import List, Optional, Any
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

@entity_search_router.get("/{entity_id}/profile/{profile_id}/rdm-resolution", response_model=dict, summary="Resolve all REFERENCE_ENTITY attributes for a master profile record")
async def get_rdm_resolution(entity_id: int, profile_id: str, warehouse_id: str, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntitySearchService(db)
    token = check.get('token')
    return service.get_rdm_resolution_for_profile(entity_id, profile_id, warehouse_id, token)

@entity_search_router.get("/{entity_id}/reference-entity-attr-options", summary="Get select options for a REFERENCE_ENTITY attribute")
async def fetch_reference_entity_attr_options(
    entity_id: int,
    attr_name: str = Query(..., description="Name of the REFERENCE_ENTITY attribute on the master entity"),
    warehouse_id: str = Query(..., description="SQL warehouse ID"),
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    try:
        service = EntitySearchService(db)
        token = check.get("token")
        return service.fetch_reference_entity_attr_option(entity_id, attr_name, warehouse_id, token)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@entity_search_router.get("/{entity_id}/reference-profile/{ref_lakefusion_id}", summary="Fetch a single reference record by ref_lakefusion_id")
async def get_reference_profile(entity_id: int, ref_lakefusion_id: str, warehouse_id: str = Query(...), db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntitySearchService(db)
    token = check.get('token')
    return service.get_reference_profile_by_id(token, entity_id, ref_lakefusion_id, warehouse_id)

@entity_search_router.get("/{entity_id}/reference-profile/{ref_lakefusion_id}/history", summary="Fetch SCD-2 version history for a reference record")
async def get_reference_profile_history(entity_id: int, ref_lakefusion_id: str, warehouse_id: str = Query(...), db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntitySearchService(db)
    token = check.get('token')
    return service.get_reference_record_history(token, entity_id, ref_lakefusion_id, warehouse_id)

@entity_search_router.get("/{entity_id}/profile/{profile_id}/history")
async def get_merge_activities(entity_id:int, profile_id:str, warehouse_id:str,db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntitySearchService(db)
    token = check.get('token')
    return service.get_merge_activities(entity_id,profile_id,warehouse_id,token)

@entity_search_router.get("/{entity_id}/errored-records", summary="Fetch errored records from unified_error_prod")
async def get_errored_records(
    entity_id: int,
    warehouse_id: str = Query(...),
    page: int = Query(1),
    page_size: int = Query(50),
    error_stage: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    service = EntitySearchService(db)
    token = check.get('token')
    return service.fetch_errored_records(token, entity_id, warehouse_id, page, page_size, error_stage)

@entity_search_router.get("/{entity_id}/errored-records/{surrogate_key}/detail", summary="Fetch full record for errored record detail dialog")
async def get_errored_record_detail(
    entity_id: int,
    surrogate_key: str,
    warehouse_id: str = Query(...),
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    service = EntitySearchService(db)
    token = check.get('token')
    return service.fetch_errored_record_detail(token, entity_id, warehouse_id, surrogate_key)

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

from lakefusion_utility.utils.logging_utils import get_logger as _get_logger
_ref_logger = _get_logger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# Reference Entity Routes (merged from reference_entity_route.py)
# ═══════════════════════════════════════════════════════════════════════════════

@entity_search_router.get("/{entity_id}/check-reference-tables", summary="Check existence of all reference-related tables in a single call")
async def check_reference_tables(
    entity_id: int,
    warehouse_id: str = Query(..., description="SQL warehouse ID"),
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db),
):
    try:
        service = EntitySearchService(db)
        token = check.get("token")
        return service.check_reference_entity_tables_existence(token, entity_id, warehouse_id)
    except HTTPException:
        raise
    except Exception as e:
        _ref_logger.error(f"Failed to check reference tables for entity {entity_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@entity_search_router.get("/{entity_id}/reference/records", summary="Fetch paginated records from the reference table")
async def get_reference_records(
    entity_id: int,
    warehouse_id: str = Query(..., description="SQL warehouse ID"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=1000),
    filters: Optional[str] = Query(None),
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db),
):
    try:
        service = EntitySearchService(db)
        token = check.get("token")
        return service.fetch_reference_records(token=token, entity_id=entity_id, warehouse_id=warehouse_id, page=page, page_size=page_size, filters=filters)
    except HTTPException:
        raise
    except Exception as e:
        _ref_logger.error(f"Failed to fetch reference records for entity {entity_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@entity_search_router.post(
    "/{entity_id}/reference/conflicts/{conflict_id}/resolve",
    summary="Resolve a conflict from the conflict queue table"
)
async def resolve_conflict(
    entity_id: int,
    conflict_id: str,
    warehouse_id: str = Query(..., description="SQL warehouse ID"),
    decision: str = Query(..., description="KEEP_RDM | USE_SOURCE | APPROVED | REJECTED"),
    resolved_by: str = Query(..., description="User resolving the conflict"),
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db),
):
    try:
        service = EntitySearchService(db)
        token = check.get("token")
        return service.resolve_conflict(
            token=token,
            warehouse_id=warehouse_id,
            entity_id=entity_id,
            conflict_id=conflict_id,
            decision=decision,
            resolved_by=resolved_by
        )
    except HTTPException:
        raise
    except Exception as e:
        _ref_logger.error(
            f"Failed to resolve conflict {conflict_id} for entity {entity_id}: {str(e)}"
        )
        raise HTTPException(status_code=500, detail=str(e))

@entity_search_router.post("/{entity_id}/reference/records", summary="Insert a new record into the reference table")
async def insert_reference_record(
    entity_id: int,
    warehouse_id: str = Query(..., description="SQL warehouse ID"),
    data_object: dict = Body(...),
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db),
):
    try:
        service = EntitySearchService(db)
        token = check.get("token")
        return service.create_reference_record(token=token, entity_id=entity_id, warehouse_id=warehouse_id, data_object=data_object)
    except HTTPException:
        raise
    except Exception as e:
        _ref_logger.error(f"Failed to insert reference record for entity {entity_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@entity_search_router.patch("/{entity_id}/reference/records", summary="Bulk-update records in the reference table")
async def patch_reference_records(
    entity_id: int,
    warehouse_id: str = Query(..., description="SQL warehouse ID"),
    update_data: dict = Body(...),
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db),
):
    try:
        service = EntitySearchService(db)
        token = check.get("token")
        edited_by = check.get("decoded", {}).get("sub", "")
        primary_field = service.get_primary_field_for_entity(entity_id)
        if not primary_field:
            raise HTTPException(status_code=400, detail="No primary key attribute set for this entity.")
        return service.update_reference_record(token=token, entity_id=entity_id, warehouse_id=warehouse_id, primary_field=primary_field, updates=update_data.get("updates", []), edited_by=edited_by)
    except HTTPException:
        raise
    except Exception as e:
        _ref_logger.error(f"Failed to update reference records for entity {entity_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@entity_search_router.delete("/{entity_id}/reference/records", summary="Delete a record from the reference table by primary key")
async def remove_reference_record(
    entity_id: int,
    warehouse_id: str = Query(..., description="SQL warehouse ID"),
    primary_field_value: Any = Body(..., embed=True),
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db),
):
    try:
        service = EntitySearchService(db)
        token = check.get("token")
        if primary_field_value is None:
            raise HTTPException(status_code=400, detail="'primary_field_value' is required.")
        primary_field = service.get_primary_field_for_entity(entity_id)
        if not primary_field:
            raise HTTPException(status_code=400, detail="No primary key attribute set for this entity.")
        affected = service.delete_reference_record(token=token, entity_id=entity_id, warehouse_id=warehouse_id, primary_field=primary_field, primary_field_value=primary_field_value)
        return {"message": f"Record where {primary_field} = '{primary_field_value}' deleted successfully.", "affected_rows": affected}
    except HTTPException:
        raise
    except Exception as e:
        _ref_logger.error(f"Failed to delete reference record for entity {entity_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@entity_search_router.get("/{entity_id}/reference-review/records", summary="Fetch paginated records from the reference review table")
async def get_reference_review_records(
    entity_id: int,
    warehouse_id: str = Query(..., description="SQL warehouse ID"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=1000),
    filters: Optional[str] = Query(None),
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db),
):
    try:
        service = EntitySearchService(db)
        token = check.get("token")
        return service.fetch_conflict_records(token=token, entity_id=entity_id, warehouse_id=warehouse_id, page=page, page_size=page_size, filters=filters)
    except HTTPException:
        raise
    except Exception as e:
        _ref_logger.error(f"Failed to fetch reference review records for entity {entity_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@entity_search_router.get("/{entity_id}/reference-mappings/records", summary="Fetch paginated records from the reference mappings table")
async def get_reference_mappings_records(
    entity_id: int,
    warehouse_id: str = Query(..., description="SQL warehouse ID"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=1000),
    view: Optional[str] = Query(None, description="'review' for PENDING/NO_MATCH, 'approved' for AUTO_APPROVED/APPROVED/MANUALLY_ADDED"),
    filters: Optional[str] = Query(None),
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db),
):
    try:
        service = EntitySearchService(db)
        token = check.get("token")
        status_filter = None
        if view == "review":
            status_filter = list(service.REVIEW_STATUSES)
        elif view == "approved":
            status_filter = list(service.APPROVED_STATUSES)
        return service.fetch_reference_mappings_records(token=token, entity_id=entity_id, warehouse_id=warehouse_id, page=page, page_size=page_size, status_filter=status_filter, filters=filters)
    except HTTPException:
        raise
    except Exception as e:
        _ref_logger.error(f"Failed to fetch reference mappings for entity {entity_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@entity_search_router.post("/{entity_id}/reference-mappings/records/approve-all", summary="Bulk-approve all PENDING/NO_MATCH mapping records")
async def approve_all_reference_mapping_records_endpoint(
    entity_id: int,
    warehouse_id: str = Query(..., description="SQL warehouse ID"),
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db),
):
    try:
        service = EntitySearchService(db)
        token = check.get("token")
        return service.approve_all_reference_mapping_records(token=token, entity_id=entity_id, warehouse_id=warehouse_id)
    except HTTPException:
        raise
    except Exception as e:
        _ref_logger.error(f"Failed to bulk-approve mappings for entity {entity_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@entity_search_router.post("/{entity_id}/reference-mappings/records", summary="Insert a new mapping record (MANUALLY_ADDED) into the reference mappings table")
async def insert_reference_mapping_record(
    entity_id: int,
    warehouse_id: str = Query(..., description="SQL warehouse ID"),
    data_object: dict = Body(...),
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db),
):
    try:
        service = EntitySearchService(db)
        token = check.get("token")
        return service.create_reference_mapping_record(token=token, entity_id=entity_id, warehouse_id=warehouse_id, data_object=data_object)
    except HTTPException:
        raise
    except Exception as e:
        _ref_logger.error(f"Failed to insert reference mapping for entity {entity_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@entity_search_router.post("/{entity_id}/reference/records/import", summary="Bulk insert records into the reference table, creating it if it does not exist")
async def import_reference_records(
    entity_id: int,
    warehouse_id: str = Query(..., description="SQL warehouse ID"),
    records: List[dict] = Body(...),
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db),
):
    try:
        service = EntitySearchService(db)
        token = check.get("token")
        return service.import_reference_records(token=token, entity_id=entity_id, warehouse_id=warehouse_id, records=records)
    except HTTPException:
        raise
    except Exception as e:
        _ref_logger.error(f"Failed to bulk import reference records for entity {entity_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@entity_search_router.post("/{entity_id}/reference-mappings/records/import", summary="Bulk insert mapping records; raises 400 if mappings table does not exist")
async def import_reference_mapping_records(
    entity_id: int,
    warehouse_id: str = Query(..., description="SQL warehouse ID"),
    records: List[dict] = Body(...),
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db),
):
    try:
        service = EntitySearchService(db)
        token = check.get("token")
        return service.import_reference_mapping_records(token=token, entity_id=entity_id, warehouse_id=warehouse_id, records=records)
    except HTTPException:
        raise
    except Exception as e:
        _ref_logger.error(f"Failed to bulk import reference mapping records for entity {entity_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@entity_search_router.patch("/{entity_id}/reference-mappings/records/{record_id}", summary="Update a mapping record in the reference mappings table")
async def patch_reference_mapping_record(
    entity_id: int,
    record_id: int,
    warehouse_id: str = Query(..., description="SQL warehouse ID"),
    data_object: dict = Body(...),
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db),
):
    try:
        service = EntitySearchService(db)
        token = check.get("token")
        return service.update_reference_mapping_record(token=token, entity_id=entity_id, warehouse_id=warehouse_id, record_id=record_id, data_object=data_object)
    except HTTPException:
        raise
    except Exception as e:
        _ref_logger.error(f"Failed to update reference mapping {record_id} for entity {entity_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@entity_search_router.delete("/{entity_id}/reference-mappings/records/{record_id}", summary="Delete a mapping record from the reference mappings table")
async def remove_reference_mapping_record(
    entity_id: int,
    record_id: int,
    warehouse_id: str = Query(..., description="SQL warehouse ID"),
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db),
):
    try:
        service = EntitySearchService(db)
        token = check.get("token")
        affected = service.delete_reference_mapping_record(token=token, entity_id=entity_id, warehouse_id=warehouse_id, record_id=record_id)
        return {"message": f"Mapping record {record_id} deleted successfully.", "affected_rows": affected}
    except HTTPException:
        raise
    except Exception as e:
        _ref_logger.error(f"Failed to delete reference mapping {record_id} for entity {entity_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
