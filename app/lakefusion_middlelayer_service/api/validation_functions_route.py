from fastapi import HTTPException, Depends, APIRouter
from sqlalchemy.orm import Session
from app.lakefusion_middlelayer_service.utils.app_db import get_db,token_required_wrapper
from lakefusion_utility.models.httpresponse import HttpResponse
from lakefusion_utility.models.entity import ValidationFunctionCreate, ValidationFunctionResponse, ValidationUserDefinedFunctionResponse, ValidationUserDefinedFunctionCreate
from lakefusion_utility.services.validation_functions_service import ValidationFunctionsService, ValidationUserdefinedFunctionsService
from typing import List

validation_functions_router = APIRouter(tags=["Validation Functions API"], prefix='/validation_functions')

# User-defined functions routes - More specific routes should come first
@validation_functions_router.post("/user-defined", response_model=ValidationUserDefinedFunctionResponse)
def create_validation_userdefined_function(validation_create: ValidationUserDefinedFunctionCreate, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    validation_create.created_by = check.get('decoded', {}).get('sub', '')
    service = ValidationUserdefinedFunctionsService(db)
    return service.create_validation_userdefined_function(validation_create)

@validation_functions_router.get("/user-defined", response_model=List[ValidationUserDefinedFunctionResponse])
def read_validation_userdefined_functions(is_active: bool = True, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = ValidationUserdefinedFunctionsService(db)
    return service.read_validation_userdefined_function_entries(is_active)

@validation_functions_router.get("/user-defined/{function_id}", response_model=ValidationUserDefinedFunctionResponse)
def read_validation_userdefined_function_by_id(function_id: int, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = ValidationUserdefinedFunctionsService(db)
    return service.get_validation_userdefinedfunction_entries_by_id(function_id)

@validation_functions_router.put("/user-defined/{function_id}", response_model=ValidationUserDefinedFunctionResponse)
def update_validation_userdefined_function(function_id: int, validation_update: ValidationUserDefinedFunctionCreate, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = ValidationUserdefinedFunctionsService(db)
    return service.update_validation_userdefined_function(function_id, validation_update)

@validation_functions_router.delete("/user-defined/{function_id}")
def delete_validation_userdefined_function_by_id(function_id: int, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = ValidationUserdefinedFunctionsService(db)
    return service.delete_validation_userdefined_function_entry_by_id(function_id)

# Regular validation functions routes
@validation_functions_router.get("/list-validation-functions")
def list_validation_functions(db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = ValidationFunctionsService(db)
    return service.read_validation_functions()

@validation_functions_router.post("/", response_model=ValidationFunctionResponse)
def create_validation_functions_entry(entity_id: int, validation_create: ValidationFunctionCreate, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    validation_create.created_by = check.get('decoded', {}).get('sub', '')
    service = ValidationFunctionsService(db)
    return service.create_validationfunctionentry(validation_create, entity_id)

@validation_functions_router.get("/", response_model=List[ValidationFunctionResponse])
def read_validation_functions_entries(entity_id: int, is_active: bool = True, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = ValidationFunctionsService(db)
    return service.read_validationfunctionsentries(entity_id, is_active)

@validation_functions_router.get("/{validation_id}", response_model=ValidationFunctionResponse)
def read_validation_functions_entries_by_id(entity_id: int, validation_id: int, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = ValidationFunctionsService(db)
    return service.get_validationfunctionsentry_by_id(entity_id, validation_id)

@validation_functions_router.delete("/{validation_id}")
def delete_validation_functions_entries_by_id(entity_id: int, validation_id: int, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = ValidationFunctionsService(db)
    return service.delete_validationfunctionentries_by_id(entity_id, validation_id)

@validation_functions_router.put("/{validation_id}")
def update_validation_functions_entries_by_id(entity_id: int, validation_id: int, validation_update: ValidationFunctionCreate, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = ValidationFunctionsService(db)
    return service.update_validation_function(entity_id, validation_id, validation_update)





   