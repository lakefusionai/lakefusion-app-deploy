from typing import List, Dict, Any, Optional, Literal
from pydantic import BaseModel, Field


class GoldenRecordAttributes(BaseModel):
    """Dynamic attributes - allows any fields from golden record."""
    model_config = {"extra": "allow"}


class SearchResult(BaseModel):
    """Single result from entity search."""
    lakefusion_id: str
    score: float = Field(..., ge=0, le=1)
    golden_record_attributes: Dict[str, Any]


class SearchResponse(BaseModel):
    """Response from search_entity API."""
    entity_type: str
    results: List[SearchResult]


class MatchedGoldenRecord(BaseModel):
    """Golden record returned from match."""
    lakefusion_id: str
    golden_record_attributes: Dict[str, Any]


class MatchResponse(BaseModel):
    """Response from match_entity API."""
    entity_type: str
    match_status: Literal["ExactMatch", "PotentialMatch", "NoMatch"]
    score: Optional[float] = Field(None, ge=0, le=1)
    reason: Optional[str] = None
    matched_golden_record: Optional[MatchedGoldenRecord] = None


class EntityInfo(BaseModel):
    """Simplified entity information for MCP tool response."""
    id: int
    task_name: str
    entity_id: int
    entity_name: str
    entity_description: Optional[str] = None
    model_id: int
    model_name: Optional[str] = None
    is_active: bool


class GetEntitiesResponse(BaseModel):
    """Response from get_entities API."""
    entities: List[EntityInfo]
    count: int


# ============================================================================
# Health Check Response Models
# ============================================================================

class HealthPingResponse(BaseModel):
    """Response from health ping endpoint."""
    status: str = Field(..., description="Health status", examples=["ok"])


class MCPEndpoints(BaseModel):
    """MCP service endpoints information."""
    docs: str = Field(..., description="Documentation endpoint")
    health: str = Field(..., description="Health check endpoint")
    health_dependencies: str = Field(..., description="Dependencies health endpoint")
    auth_callback: str = Field(..., description="OAuth callback endpoint")
    mcp: str = Field(..., description="MCP transport endpoint")


class MCPToolsInfo(BaseModel):
    """MCP tools information."""
    transport: str = Field(..., description="MCP transport type")
    tools: List[str] = Field(..., description="Available MCP tools")


class ServiceInfoResponse(BaseModel):
    """Response from service info endpoint."""
    service: str = Field(..., description="Service name")
    version: str = Field(..., description="Service version")
    status: str = Field(..., description="Service status")
    endpoints: MCPEndpoints = Field(..., description="Available endpoints")
    mcp: MCPToolsInfo = Field(..., description="MCP configuration")


class ServiceHealthStatus(BaseModel):
    """Health status of a dependent service."""
    status: Literal["healthy", "unhealthy"] = Field(..., description="Service health status")
    url: str = Field(..., description="Service URL")
    response_time_ms: Optional[float] = Field(None, description="Response time in milliseconds")
    error: Optional[str] = Field(None, description="Error message if unhealthy")


class SessionsHealthStatus(BaseModel):
    """Health status of session management."""
    status: str = Field(..., description="Sessions status")
    active_sessions: int = Field(..., description="Number of active sessions")
    authenticated_sessions: int = Field(..., description="Number of authenticated sessions")


class DependenciesStatus(BaseModel):
    """Status of all dependencies."""
    auth_service: ServiceHealthStatus = Field(..., description="Auth service health")
    matchmaven_service: ServiceHealthStatus = Field(..., description="MatchMaven service health")
    sessions: SessionsHealthStatus = Field(..., description="Session management health")


class DependenciesHealthResponse(BaseModel):
    """Response from dependencies health check endpoint."""
    status: Literal["healthy", "unhealthy"] = Field(..., description="Overall health status")
    service: str = Field(..., description="Service name")
    version: str = Field(..., description="Service version")
    dependencies: DependenciesStatus = Field(..., description="Dependencies health status")


# ============================================================================
# Auth Response Models
# ============================================================================

class AuthCallbackSuccessResponse(BaseModel):
    """Response model for successful OAuth callback (returned as HTML in practice)."""
    status: str = Field(..., description="Authentication status")
    username: str = Field(..., description="Authenticated username")
    message: str = Field(..., description="Success message")