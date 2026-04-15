from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from app.lakefusion_middlelayer_service.utils.app_db import get_db, token_required_wrapper
from lakefusion_utility.models.quality_assets import (
    AssetCreate, AssetUpdate, AssetResponse
)
from lakefusion_utility.services.quality_assets_service import QualityAssetService

quality_asset_router = APIRouter(tags=["Quality Asset API"], prefix='/assets')

@quality_asset_router.post("/", response_model=AssetResponse)
def create_asset(
    asset: AssetCreate,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    created_by = check.get('decoded', {}).get('sub', '')
    service = QualityAssetService(db)
    return service.create_asset(asset, created_by)

@quality_asset_router.get("/", response_model=List[AssetResponse])
def get_all_assets(
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    service = QualityAssetService(db)
    return service.get_all_assets()

@quality_asset_router.get("/{asset_id}", response_model=AssetResponse)
def get_asset(
    asset_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    service = QualityAssetService(db)
    return service.get_asset(asset_id)

@quality_asset_router.put("/{asset_id}", response_model=AssetResponse)
def update_asset(
    asset_id: int,
    asset_update: AssetUpdate,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    service = QualityAssetService(db)
    return service.update_asset(asset_id, asset_update)

@quality_asset_router.delete("/{asset_id}")
def delete_asset(
    asset_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    service = QualityAssetService(db)
    return service.delete_asset(asset_id)