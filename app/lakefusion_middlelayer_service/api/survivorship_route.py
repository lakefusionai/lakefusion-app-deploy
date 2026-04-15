from fastapi import HTTPException, Depends, APIRouter
from sqlalchemy.orm import Session
from app.lakefusion_middlelayer_service.utils.app_db import get_db,token_required_wrapper # Importing get_db from the specified location
from lakefusion_utility.models.entity import SurvivorshipRulesResponse,SurvivorshipRulesCreate # Import your Pydantic models
from lakefusion_utility.models.httpresponse import HttpResponse
from lakefusion_utility.services.entity_service import SurvivorshipRuleService  # Import the EntityService class
from typing import List

# Initialize the router with a prefix and tag
survivorship_router = APIRouter(tags=["Survivorship API"], prefix='/survivorship')

# Create a new Survivorshiprule
@survivorship_router.post("/", response_model=SurvivorshipRulesResponse)
def create_survivorship_rule(entity_id:int,survivorship: SurvivorshipRulesCreate, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    survivorship.created_by = check.get('decoded', {}).get('sub', '')
    service = SurvivorshipRuleService(db)  # Create an instance of EntityService
    return service.create_survivorship_rule(survivorship,entity_id)

# Read all Survivorshiprules with an optional `is_active` filter
@survivorship_router.get("/",response_model=List[SurvivorshipRulesResponse])
def read_survivorship_rule(entity_id:int,is_active: bool = True, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = SurvivorshipRuleService(db)
    return service.read_survivorship_rule(entity_id,is_active)


# Update a Survivorshiprule
@survivorship_router.put("/", response_model=SurvivorshipRulesResponse)
def update_survivorship_rule(survivorship_id: int, survivorship: SurvivorshipRulesCreate, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    survivorship.created_by = check.get('decoded', {}).get('sub', '')
    service = SurvivorshipRuleService(db)  # Create an instance of EntityService
    return service.update_survivorship_rule(survivorship, survivorship_id)

# Soft delete a Survivorshiprule by id
@survivorship_router.delete("/{survivorship_id}")
def delete_survivorship_rule(entity_id: int,survivorship_id:int,db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = SurvivorshipRuleService(db)  # Create an instance of EntityService
    return service.delete_survivorship_rule(entity_id,survivorship_id)

# Retrieve a Survivorshiprule by ID
@survivorship_router.get("/{survivorship_id}", response_model=SurvivorshipRulesResponse)
def get_survivorship_rule(entity_id: int,survivorship_id:int, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = SurvivorshipRuleService(db)  # Create an instance of EntityService
    Entity = service.get_survivorship_rule_by_id(entity_id,survivorship_id)
    if Entity is None:
        raise HTTPException(status_code=404, detail="Entity not found")
    return Entity

# Toggle `is_active` flag for a Survivorshiprule
@survivorship_router.patch("/{survivorship_id}/toggle", response_model=SurvivorshipRulesResponse)
def toggle_survivorship_rule_active(entity_id: int,survivorship_id:int, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = SurvivorshipRuleService(db)
    Entity = service.toggle_survivorship_rule_active(entity_id,survivorship_id)
    if Entity is None:
        raise HTTPException(status_code=404, detail="Entity not found")
    return Entity

@survivorship_router.patch("/{survivorship_id}/update-default", response_model=SurvivorshipRulesResponse)
def update_survivorship_default(
    entity_id: int,
    survivorship_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """
    Update the default status of a survivorship rule.
    If set to default, all other rules for the entity will be set to non-default.
    """
    service = SurvivorshipRuleService(db)
    return service.update_survivorship_default(entity_id, survivorship_id)
