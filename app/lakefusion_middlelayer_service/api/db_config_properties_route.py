from fastapi import Depends, APIRouter, Query
from sqlalchemy.orm import Session
from typing import Optional
from app.lakefusion_middlelayer_service.utils.app_db import get_db, token_required_wrapper
from lakefusion_utility.utils.db_config_utility import DBConfigPropertiesService
from lakefusion_utility.models.dbconfig import DBConfigPropertiesResponse, DBConfigPropertiesUpdate
from lakefusion_utility.services.feature_flags_service import FeatureFlagService
from lakefusion_utility.utils.logging_utils import get_logger
from lakefusion_utility.services.databricks_sync_service import import_custom_tags_file

app_logger = get_logger(__name__)

# Initialize router with prefix and tag
db_config_properties_router = APIRouter(
    tags=["DB Config Properties API"], 
    prefix='/db-config-properties'
)

# GET endpoint to fetch all configuration properties
@db_config_properties_router.get("/", response_model=list[DBConfigPropertiesResponse])
def get_all_db_config_properties(
    category: Optional[str] = Query(None, description="Filter by config category (case-insensitive)"),
    db: Session = Depends(get_db), 
    check: dict = Depends(token_required_wrapper)
):
    """
    Fetch all configuration properties from the database.
    
    Returns:
        List of configuration property objects
    """
    # Create service instance with database session
    service = DBConfigPropertiesService(db)
    
    # Call service method to get all config properties
    return service.get_all_db_config_properties(category=category)


@db_config_properties_router.put("/", response_model=DBConfigPropertiesResponse)
def update_db_config_property(
    config_update: DBConfigPropertiesUpdate,
    db: Session = Depends(get_db), 
    check: dict = Depends(token_required_wrapper)
):
    """
    Update a specific configuration property by key.
    """
    service = DBConfigPropertiesService(db)
    return service.update_db_config_property(
        config_key=config_update.config_key,
        config_value=config_update.config_value,
        extended_values=config_update.extended_values,
        updated_by=check.get('decoded', {}).get('sub', ''),
        token=check.get('token', '')
    )


@db_config_properties_router.put("/lakefusion-tags", response_model=DBConfigPropertiesResponse)
def update_lakefusion_tag_property(
    config_update: DBConfigPropertiesUpdate,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """
    Dedicated endpoint to update LakeFusion Tag-related configuration.
    Internally reuses DBConfigPropertiesService.update_db_config_property
    and executes additional tag-specific logic.
    """
    user = check.get('decoded', {}).get('sub', '')

    service = DBConfigPropertiesService(db)

    # ✅ Step 1: Call existing reusable logic
    result = service.update_db_config_property(
        config_key=config_update.config_key,
        config_value=config_update.config_value,
        updated_by=user
    )
    # ✅ Step 2: Additional operations only for LakeFusion tags
    # ✅ Feature flag logic
    if FeatureFlagService._is_feature_flag_enabled(db=db, name="ENABLE_LAKEFUSION_TAG_USAGE"):
        app_logger.info("✅ Feature flag ENABLE_LAKEFUSION_TAG_USAGE is ACTIVE — importing custom_tags.json")
        import_custom_tags_file(
            db=db
        )
    else:
        app_logger.info("Feature flag ENABLE_LAKEFUSION_TAG_USAGE is INACTIVE — skipping custom_tags.json")

    return result
