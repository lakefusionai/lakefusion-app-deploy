# app/models/requests.py
from pydantic import BaseModel, Field, field_validator
from typing import Dict, Any


class SearchEntityRequest(BaseModel):
    entity_id: int = Field(..., gt=0, description="Entity type ID (positive integer)")
    attributes: Dict[str, Any] = Field(..., min_length=1, description="Entity attributes")
    warehouse_id: str = Field(..., min_length=1, description="Warehouse ID")
    top_n: int = Field(default=3, ge=1, le=20, description="Number of results (1-20)")
    
    @field_validator("attributes")
    @classmethod
    def attributes_not_empty(cls, v):
        if not v:
            raise ValueError("attributes cannot be empty")
        return v


class MatchEntityRequest(BaseModel):
    entity_id: int = Field(..., gt=0, description="Entity type ID (positive integer)")
    attributes: Dict[str, Any] = Field(..., min_length=1, description="Entity attributes")
    warehouse_id: str = Field(..., min_length=1, description="Warehouse ID")

    @field_validator("attributes")
    @classmethod
    def attributes_not_empty(cls, v):
        if not v:
            raise ValueError("attributes cannot be empty")
        return v


class GetEntitiesRequest(BaseModel):
    is_active: bool = Field(default=True, description="Filter by active status")