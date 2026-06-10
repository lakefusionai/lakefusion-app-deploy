"""
API routes for PT Models Config management.
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Optional

from lakefusion_utility.utils.database import get_db, token_required_wrapper
from lakefusion_utility.services.pt_models_service import PTModelsConfigService
from lakefusion_utility.models.pt_models_config import (
    PTModelsConfig,
    PTModelsConfigCreate,
    PTModelsConfigUpdate,
    PTModelsConfigResponse,
    PTModelsConfigValidateRequest,
    PTModelsConfigValidateResponse,
)
from lakefusion_utility.models.api_response import ApiResponse
from lakefusion_utility.utils.logging_utils import get_logger

app_logger = get_logger(__name__)

pt_models_router = APIRouter(
    tags=["PT Models API"],
    prefix="/settings/pt-models"
)


@pt_models_router.get(
    "/",
    response_model=ApiResponse[List[PTModelsConfigResponse]],
    summary="List all PT model configurations",
    description="Retrieve all PT model configurations with optional filtering by active status."
)
def get_all_pt_configs(
    is_active: Optional[bool] = None,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """
    List all PT model configurations.

    Args:
        is_active: Filter by active status (true=active only (default), false=inactive only, null=all)
        db: Database session
        check: Authentication check

    Returns:
        List of PT model configurations
    """
    try:
        service = PTModelsConfigService(db)
        return service.read_pt_configs(is_active=is_active)
    except Exception as e:
        app_logger.exception("Failed to fetch PT configs")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch PT configs: {str(e)}"
        )


@pt_models_router.get(
    "/{config_id}",
    response_model=ApiResponse[PTModelsConfigResponse],
    summary="Get PT config by ID",
    description="Retrieve a single PT model configuration by its ID."
)
def get_pt_config_by_id(
    config_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """
    Get a single PT config by ID.

    Args:
        config_id: PT configuration ID
        db: Database session
        check: Authentication check

    Returns:
        PT model configuration

    Raises:
        HTTPException: If config not found
    """
    try:
        service = PTModelsConfigService(db)
        config = service.get_pt_config_by_id(config_id)
        return ApiResponse[PTModelsConfigResponse](
            status="success",
            message="PT config retrieved successfully",
            data=config
        )
    except HTTPException:
        raise
    except Exception as e:
        app_logger.exception(f"Failed to fetch PT config {config_id}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch PT config: {str(e)}"
        )


@pt_models_router.post(
    "/",
    response_model=ApiResponse[PTModelsConfigResponse],
    status_code=status.HTTP_201_CREATED,
    summary="Create PT model configuration",
    description="Create a new PT model configuration and sync to Databricks Volume."
)
def create_pt_config(
    pt_config: PTModelsConfigCreate,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """
    Create a new PT model configuration.

    Args:
        pt_config: PT configuration data
        db: Database session
        check: Authentication check

    Returns:
        Created PT configuration

    Raises:
        HTTPException: If config already exists or creation fails
    """
    try:
        token = check.get('token')
        service = PTModelsConfigService(db)
        return service.create_pt_config(pt_config, token)
    except HTTPException:
        raise
    except Exception as e:
        app_logger.exception("Failed to create PT config")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create PT config: {str(e)}"
        )


@pt_models_router.put(
    "/{config_id}",
    response_model=ApiResponse[PTModelsConfigResponse],
    summary="Update PT model configuration",
    description="Update an existing PT model configuration and sync to Databricks Volume."
)
def update_pt_config(
    config_id: int,
    update_data: PTModelsConfigUpdate,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """
    Update an existing PT model configuration.

    Args:
        config_id: PT configuration ID
        update_data: Updated configuration data
        db: Database session
        check: Authentication check

    Returns:
        Updated PT configuration

    Raises:
        HTTPException: If config not found or update fails
    """
    try:
        token = check.get('token')
        service = PTModelsConfigService(db)
        return service.update_pt_config(config_id, update_data, token)
    except HTTPException:
        raise
    except Exception as e:
        app_logger.exception(f"Failed to update PT config {config_id}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update PT config: {str(e)}"
        )


@pt_models_router.delete(
    "/{config_id}",
    response_model=ApiResponse[str],
    summary="Delete PT model configuration",
    description="Hard delete a PT model configuration (permanently removes from database) and sync to Volume."
)
def delete_pt_config(
    config_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """
    Hard delete a PT model configuration.

    Args:
        config_id: PT configuration ID
        db: Database session
        check: Authentication check

    Returns:
        Confirmation message

    Raises:
        HTTPException: If config not found or deletion fails
    """
    try:
        token = check.get('token')
        service = PTModelsConfigService(db)
        return service.delete_pt_config(config_id, token)
    except HTTPException:
        raise
    except Exception as e:
        app_logger.exception(f"Failed to delete PT config {config_id}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete PT config: {str(e)}"
        )


@pt_models_router.patch(
    "/{config_id}/toggle",
    response_model=ApiResponse[PTModelsConfigResponse],
    summary="Toggle PT model configuration status",
    description="Enable or disable a PT model configuration by toggling is_active status (does NOT sync to Volume)."
)
def toggle_pt_config(
    config_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """
    Toggle PT model configuration active status (enable/disable).

    Note: This does NOT automatically sync to Databricks Volume.
    Inactive configs are not synced to Volume until manually synced.

    Args:
        config_id: PT configuration ID
        db: Database session
        check: Authentication check

    Returns:
        Updated PT configuration with new is_active status

    Raises:
        HTTPException: If config not found or toggle fails
    """
    try:
        token = check.get('token')
        service = PTModelsConfigService(db)
        return service.toggle_pt_config(config_id, token)
    except HTTPException:
        raise
    except Exception as e:
        app_logger.exception(f"Failed to toggle PT config {config_id}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to toggle PT config: {str(e)}"
        )


@pt_models_router.post(
    "/validate",
    response_model=ApiResponse[PTModelsConfigValidateResponse],
    summary="Validate PT model eligibility",
    description="Check if a model supports PT and get recommended default configuration."
)
def validate_pt_model(
    request: PTModelsConfigValidateRequest,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """
    Validate if a model supports provisioned throughput.

    This endpoint calls the Databricks optimization API to determine:
    - Whether the model supports PT
    - Which PT type (model_units or legacy)
    - Recommended chunk size and defaults
    - Model category (llm or embedding)

    Args:
        request: Validation request with entity name and version
        db: Database session
        check: Authentication check

    Returns:
        Validation result with defaults

    Raises:
        HTTPException: If model doesn't support PT or validation fails
    """
    try:
        token = check.get('token')
        service = PTModelsConfigService(db)
        return service.validate_pt_model(request, token)
    except HTTPException:
        raise
    except Exception as e:
        app_logger.exception(f"Failed to validate PT model {request.entity_name}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to validate PT model: {str(e)}"
        )


@pt_models_router.post(
    "/sync",
    response_model=ApiResponse[str],
    summary="Sync PT configs to Databricks Volume",
    description="Manually trigger sync of active PT configurations to Databricks Volume JSON file."
)
def sync_pt_configs_to_volume(
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """
    Manually sync PT configs to Databricks Volume.

    This endpoint writes all active PT model configurations to a JSON file
    on the Databricks Volume that the pipeline notebooks read from.

    Args:
        db: Database session
        check: Authentication check

    Returns:
        Success confirmation message

    Raises:
        HTTPException: If sync fails
    """
    try:
        token = check.get('token')
        service = PTModelsConfigService(db)
        service.sync_to_volume(token)
        return ApiResponse[str](
            status="success",
            message="PT configurations synced to Databricks Volume successfully",
            data="Sync completed"
        )
    except HTTPException:
        raise
    except Exception as e:
        app_logger.exception("Failed to sync PT configs to Volume")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to sync PT configs to Volume: {str(e)}"
        )
