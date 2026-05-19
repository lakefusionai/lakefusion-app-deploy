# routes/feature_flags.py
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from lakefusion_utility.models.feature_flags import FeatureFlagStatus
from lakefusion_utility.services.feature_flags_service import FeatureFlagService
from app.lakefusion_middlelayer_service.utils.app_db import get_db, token_required_wrapper

router = APIRouter(prefix="/feature-flags", tags=["Feature Flags"])

@router.get("")
def list_feature_flags(
    status: FeatureFlagStatus = Query(default=FeatureFlagStatus.ACTIVE, description="Filter by status"),
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    service = FeatureFlagService(db)
    return service.list_feature_flags(status=status)

@router.get("/")
def list_feature_flags(
    status: FeatureFlagStatus = Query(default=FeatureFlagStatus.ACTIVE, description="Filter by status"),
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    service = FeatureFlagService(db)
    return service.list_feature_flags(status=status)
