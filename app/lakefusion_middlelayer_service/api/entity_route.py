from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Body
from lakefusion_utility.services.validation_functions_service import ValidationFunctionsService
from sqlalchemy.orm import Session
from app.lakefusion_middlelayer_service.utils.app_db import get_db,token_required_wrapper  # Importing get_db from the specified location
from lakefusion_utility.models.entity import EntityCreate, EntityResponse,EntityDnBCreate, EntityAttributeCreate, EntityAttributeResponse, EntityDatasetMappingCreate, EntityDatasetMappingResponse,EntityResponseTags, EntityDnBCreate,EntityDnBResponse, SurvivorshipRulesCreate, SurvivorshipRulesResponse, ValidationFunctionCreate, ValidationFunctionResponse  # Import your Pydantic models
from lakefusion_utility.models.api_response import ApiResponse
from lakefusion_utility.services.entity_service import EntityService, EntityDnBService, EntityAttributeService, SurvivorshipRuleService, EntityDatasetMappingService # Import the EntityService class
from fastapi.responses import FileResponse
from typing import List, Optional

# Initialize the router with a prefix and tag
entity_router = APIRouter(tags=["Entity API"], prefix='/entity')

# Create a new Entity
@entity_router.post("/", response_model=EntityResponse)
def create_entity(entity: EntityCreate, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    entity.created_by = check.get('decoded', {}).get('sub', '')
    service = EntityService(db)  # Create an instance of EntityService
    return service.create_entity(entity)

@entity_router.post("/{entity_id}/dnb",response_model=EntityDnBResponse)
def create_or_update_entity_dnb(entity_id:int,entity_dnb_input: EntityDnBCreate, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper), is_integration_job: bool = False):
    entity_dnb_input.created_by = check.get('decoded', {}).get('sub', '')
    token = check.get('token', '')
    service = EntityDnBService(db)
    return service.create_or_update_entity_dnb(entity_id, entity_dnb_input, token, is_integration_job)

# Read all Entitys with an optional `is_active` filter
@entity_router.get("/tags", response_model=List[EntityResponseTags])
def read_entitys_tags(is_active: bool = True,db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntityService(db)
    return service.read_entity_tags(is_active)

@entity_router.get("/", response_model=List[EntityResponse])
def read_entitys(is_active: Optional[bool] = None,entity_type:Optional[str]=None,db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntityService(db)
    return service.read_entity(is_active,entity_type)

# Soft delete a Entity by id
@entity_router.delete("/{entity_id}")
def delete_entity(entity_id: int, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntityService(db)  # Create an instance of EntityService
    return service.delete_entity(entity_id)

# Retrieve a Entity by ID
@entity_router.get("/{entity_id}", response_model=EntityResponse)
def get_entity(entity_id: int, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntityService(db)  # Create an instance of EntityService
    Entity = service.get_entity_by_id(entity_id)
    if Entity is None:
        raise HTTPException(status_code=404, detail="Entity not found")
    return Entity

# Toggle `is_active` flag for a Entity
@entity_router.patch("/{entity_id}/toggle", response_model=EntityResponse)
def toggle_Entity_active(entity_id: int, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntityService(db)
    Entity = service.toggle_entity_active(entity_id)
    if Entity is None:
        raise HTTPException(status_code=404, detail="Entity not found")
    return Entity

# Check entity tables exist
@entity_router.get("/{entity_id}/check-tables", response_model=dict)
def check_entity_tables_existence(
    entity_id: int,
    entity_name: str,
    db: Session = Depends(get_db), 
    check: dict = Depends(token_required_wrapper)
):
    """
    Check existence of all tables related to an entity.
    
    Args:
        entity_id: The entity ID (from path)
        entity_name: The entity name (from query params)
    """
    service = EntityService(db)
    token = check.get('token', '')
    return service.check_entity_tables_existence(token, entity_name)

@entity_router.post("/{entity_id}/attribute", response_model=ApiResponse[EntityAttributeResponse])
def create_entity_attribute(entity_id:int, entity_attribute: EntityAttributeCreate, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    created_by = check.get('decoded', {}).get('sub', '')
    service = EntityAttributeService(db)  # Create an instance of EntityAttributeService
    result = service.create_entity_attribute(entity_attribute, entity_id, created_by=created_by)
    return result

@entity_router.patch("/{entity_id}/attribute/{attribute_id}", response_model=ApiResponse[EntityAttributeResponse])
def update_entity_attribute(entity_id: int, attribute_id: int, entity_attribute: EntityAttributeCreate, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    created_by = check.get('decoded', {}).get('sub', '')
    service = EntityAttributeService(db)
    result = service.update_entity_attribute(entity_id, attribute_id, entity_attribute, created_by=created_by)
    return result

@entity_router.delete("/{entity_id}/attribute/{attribute_id}")
def delete_entity_attribute(entity_id: int, attribute_id: int, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntityAttributeService(db)
    result = service.delete_entity_attribute(entity_id, attribute_id)
    return result 

@entity_router.get("/{entity_id}/attribute", response_model=List[EntityAttributeResponse])
def read_entityattribute(entity_id:int, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntityAttributeService(db)
    return service.read_entityattributes(entity_id)

# Toggle `is_active` flag for a Entity
@entity_router.patch("/attribute/{attribute_id}/toggle", response_model=EntityAttributeResponse)
def toggle_attribute_active(attribute_id: int,db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntityAttributeService(db)
    Entity = service.toggle_attribute_active(attribute_id)
    if Entity is None:
        raise HTTPException(status_code=404, detail="Entity not found")
    return Entity

@entity_router.patch("/attribute/{attribute_id}/show-in-ui", response_model=EntityAttributeResponse)
def toggle_attribute_show_in_ui(attribute_id: int,db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntityAttributeService(db)
    Attribute = service.toggle_attribute_show_in_ui(attribute_id)
    if Attribute is None:
        raise HTTPException(status_code=404, detail="Attribute not found")
    return Attribute

@entity_router.patch("/{entity_id}/attribute/{attribute_id}/set-as-primary-key", response_model=EntityAttributeResponse)
def update_attribute_primary(
    entity_id: int,
    attribute_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """
    Update the entity's primary key attribute.
    """
    service = EntityAttributeService(db)
    return service.update_entity_attribute_primary_key(entity_id, attribute_id)

@entity_router.patch("/{entity_id}/attribute/{attribute_id}/set-as-label", response_model=EntityAttributeResponse)
def update_attribute_label(
    entity_id: int,
    attribute_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """
    Update the entity's label status attribute.
    """
    service = EntityAttributeService(db)
    return service.update_entity_attribute_label(entity_id, attribute_id)

# RETRIEVE Attribute Dataset Mapping for an Entity
@entity_router.get("/{entity_id}/attributemapping", response_model=List[EntityDatasetMappingResponse])
def read_entity_dataset_mapping(entity_id:int, is_active: bool = True, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntityDatasetMappingService(db)
    return service.read_entity_dataset_mapping(entity_id,is_active)

# CREATE Attribute Dataset Mapping for an Entity--------------------------
@entity_router.post("/{entity_id}/attributemapping", response_model=ApiResponse[EntityDatasetMappingResponse])
def create_entity_dataset_mapping(entity_id:int, dataset_id:int, entity_attribute: EntityDatasetMappingCreate, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper), is_integration_job: bool = False, use_cleaned_table: Optional[bool] = False):
    created_by = check.get('decoded', {}).get('sub', '')
    token = check.get('token', '')  # Extract the token
    service = EntityDatasetMappingService(db)  # Create an instance of EntityService
    return service.create_entity_dataset_mapping(entity_attribute, entity_id, dataset_id, token, is_integration_job, use_cleaned_table, created_by=created_by)

# UPDATE Attribute Dataset Mapping for an Entity --------------------------Done
@entity_router.put("/{entity_id}/attributemapping/{dataset_id}", response_model=ApiResponse[EntityDatasetMappingResponse])
def update_entity_dataset_mapping(entity_id: int, dataset_id: int, entityattribute: EntityDatasetMappingCreate, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper), is_integration_job: bool = False, use_cleaned_table: Optional[bool] = False):
    created_by = check.get('decoded', {}).get('sub', '')
    token = check.get('token', '')  # Extract the token
    service = EntityDatasetMappingService(db)
    return service.update_entity_dataset_mapping(entityattribute, entity_id, dataset_id, token, is_integration_job, use_cleaned_table, created_by=created_by)

# DELETE Attribute Dataset Mapping by Entity & Dataset ID----------------------
@entity_router.delete("/{entity_id}/attributemapping/{dataset_id}")
def delete_entity_dataset_mapping(entity_id: int,dataset_id:int, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper), is_integration_job: bool = False):
    token = check.get('token', '')  # Extract the token
    service = EntityDatasetMappingService(db)  # Create an instance of EntityService
    return service.delete_entity_dataset_mapping(entity_id, dataset_id, token, is_integration_job)


@entity_router.patch("/{entity_id}/attributemapping/update-primary", response_model=EntityDatasetMappingResponse)
def update_dataset_primary(
    entity_id: int,
    dataset_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """
    Update the default status of a survivorship rule.
    If set to default, all other rules for the entity will be set to non-default.
    """
    service = EntityDatasetMappingService(db)
    return service.update_entity_dataset_mapping_primary(entity_id, dataset_id)

# Retrieve a Entity by ID
@entity_router.get("/{entity_id}/attributemapping", response_model=EntityAttributeResponse)
def get_entity_dataset_mapping_by_id(entity_id: int,dataset_id:int, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntityDatasetMappingService(db)  # Create an instance of EntityService
    Entity = service.get_entity_dataset_mapping_by_id(entity_id,dataset_id)
    if Entity is None:
        raise HTTPException(status_code=404, detail="Entity not found")
    return Entity

# Toggle `is_active` flag for a Entity
@entity_router.patch("/{entity_id}/attributemapping/toggle", response_model=EntityAttributeResponse)
def toggle_EntityDatasetMapping_active(entity_id: int,dataset_id:int,db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = EntityDatasetMappingService(db)
    Entity = service.toggle_entity_dataset_mapping_active(entity_id,dataset_id)
    if Entity is None:
        raise HTTPException(status_code=404, detail="Entity not found")
    return Entity

# Survivorship Rule Endpoints
@entity_router.post("/{entity_id}/survivorship", response_model=ApiResponse[SurvivorshipRulesResponse])
def create_survivorship_rule(entity_id: int, survivorship: SurvivorshipRulesCreate, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper), is_integration_job: bool = False):
    survivorship.created_by = check.get('decoded', {}).get('sub', '')
    token = check.get('token', '')
    service = SurvivorshipRuleService(db)
    return service.create_survivorship_rule(survivorship, entity_id, token, is_integration_job)

@entity_router.put("/{entity_id}/survivorship/{survivorship_id}", response_model=ApiResponse[SurvivorshipRulesResponse])
def update_survivorship_rule(entity_id: int, survivorship_id: int, survivorship: SurvivorshipRulesCreate, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper), is_integration_job: bool = False):
    survivorship.created_by = check.get('decoded', {}).get('sub', '')
    token = check.get('token', '')
    service = SurvivorshipRuleService(db)
    return service.update_survivorship_rule(survivorship, entity_id, survivorship_id, token, is_integration_job)

@entity_router.delete("/{entity_id}/survivorship/{survivorship_id}")
def delete_survivorship_rule(entity_id: int, survivorship_id: int, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper), is_integration_job: bool = False):
    token = check.get('token', '')
    service = SurvivorshipRuleService(db) 
    return service.delete_survivorship_rule(entity_id, survivorship_id, token, is_integration_job)

@entity_router.get("/{entity_id}/survivorship",response_model=List[SurvivorshipRulesResponse])
def read_survivorship_rule(entity_id:int,is_active: bool = True, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = SurvivorshipRuleService(db)
    return service.read_survivorship_rule(entity_id,is_active)

# Validation Function Endpoints
@entity_router.post("/{entity_id}/validation-functions", response_model=ValidationFunctionResponse)
def create_validation_functions_entry(entity_id: int, validation_create: ValidationFunctionCreate, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper), is_integration_job: bool = False):
    token = check.get('token', '')
    validation_create.created_by = check.get('decoded', {}).get('sub', '')
    service = ValidationFunctionsService(db)
    return service.create_validation_function_entry(validation_create, entity_id, token, is_integration_job)

@entity_router.get("/{entity_id}/validation-functions", response_model=List[ValidationFunctionResponse])
def read_validation_functions_entries(entity_id: int, is_active: bool = True, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = ValidationFunctionsService(db)
    return service.read_validationfunctionsentries(entity_id, is_active)

@entity_router.put("/{entity_id}/validation-functions/{validation_id}")
def update_validation_functions_entries_by_id(entity_id: int, validation_id: int, validation_update: ValidationFunctionCreate, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper), is_integration_job: bool = False):
    token = check.get('token', '')
    service = ValidationFunctionsService(db)
    return service.update_validation_function(entity_id, validation_id, validation_update, token, is_integration_job)

@entity_router.delete("/{entity_id}/validation-functions/{validation_id}")
def delete_validation_functions_entries_by_id(entity_id: int, validation_id: int, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper), is_integration_job: bool = False):
    token = check.get('token', '')
    service = ValidationFunctionsService(db)
    return service.delete_validationfunctionentries_by_id(entity_id, validation_id, token, is_integration_job)

@entity_router.get(
    "/{entity_id}/export",
    response_class=FileResponse,  # Ensures Swagger/OpenAPI knows this endpoint returns a file
    dependencies=[Depends(token_required_wrapper)],  # Validates token before executing the endpoint
    responses={
        200: {
            "content": {"application/zip": {}},  # Specifies that the response content type is ZIP
            "description": "ZIP file containing entity export",
        },
        401: {"description": "Unauthorized"},  # Returned if token validation fails
        500: {"description": "Internal Server Error"},  # For unexpected errors
    },
)
def export_entity(
    entity_id: int,  # ID of the entity to export
    db: Session = Depends(get_db),  # Database session dependency
    check: dict = Depends(token_required_wrapper)  # Token validation result (from dependency)
):
    """
    Endpoint to export a full entity along with related tasks and models.
    This endpoint delegates the export logic to the EntityService's `export_entity` function.
    It ensures token validation before executing and returns a ZIP file containing:
        - entity.json
        - profiling_tasks.json
        - quality_tasks.json
        - model_experiments.json
        - dq_notebooks/<quality_task_id>.dbc for each quality notebook
    Args:
        entity_id (int): Entity ID to export
        db (Session): Active database session
        check (dict): Token validation result (from token_required_wrapper)
    Returns:
        FileResponse: A ZIP file containing the exported entity and related data
    Raises:
        HTTPException: Returns 500 if export fails due to any internal error
    """
    # Initialize the service that contains the export logic
    service = EntityService(db)
    try:
        # Delegate the export operation to the service
        # The service handles:
        #   - Fetching entity, profiling, quality, and model data
        #   - Exporting quality notebooks as DBC files
        #   - Zipping all files into a single archive
        #   - Returning a FileResponse pointing to the ZIP file
        return service.export_entity(entity_id=entity_id, check=check)

    except HTTPException:
        # If an HTTPException is raised in the service, propagate it as-is
        raise
    except Exception as e:
        # Wrap any other unexpected errors in an HTTP 500 response
        raise HTTPException(
            status_code=500,
            detail=f"Failed to export entity: {str(e)}"
        )

@entity_router.post(
    "/import",
    dependencies=[Depends(token_required_wrapper)],
    responses={
        200: {"description": "Entity imported successfully and returned as JSON"},
        400: {"description": "Invalid file or missing entity.json"},
        401: {"description": "Unauthorized"},
        500: {"description": "Internal Server Error"},
    },
)
async def import_entity(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    """
    Import an entity from a ZIP file exported by the `export_entity` endpoint.
    The ZIP file is expected to contain:
        - entity.json
        - profiling_tasks.json
        - quality_tasks.json
        - model_experiments.json
        - dq_notebooks/<quality_task_id>.dbc for each quality notebook
    This endpoint will:
        1. Read the uploaded ZIP file.
        2. Extract `entity.json`.
        3. Load it as a Python dictionary.
        4. Print the entity payload (for debug/logging).
        5. Return the entity payload as JSON response.
    Args:
        file (UploadFile): ZIP file containing exported entity.
        db (Session): Database session.
        check (dict): Token validation data from `token_required`.
    Returns:
        JSONResponse: The deserialized imported entity payload.
    """
    try:
        service = EntityService(db)
        entity_payload = await service.import_entity_from_zip(file, check)
        return entity_payload
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to import entity: {str(e)}"
        )
