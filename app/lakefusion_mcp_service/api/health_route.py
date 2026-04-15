import time
import httpx
from fastapi import APIRouter
from typing import Dict, Any
from fastapi.responses import JSONResponse
from app.lakefusion_mcp_service.services.session_manager import session_manager
from app.lakefusion_mcp_service.utils.logger import get_logger, log_to_stderr
from app.lakefusion_mcp_service.config import AUTH_SERVICE_URL, MATCHMAVEN_SERVICE_URL, MCP_SERVER_NAME, MCP_VERSION
from app.lakefusion_mcp_service.models.responses import (
    HealthPingResponse,
    ServiceInfoResponse,
    DependenciesHealthResponse,
)

logger = get_logger(__name__)

router = APIRouter(tags=["Health"])

# Health check timeout in seconds
HEALTH_CHECK_TIMEOUT = 5.0


@router.get("/", include_in_schema=False)
async def api_root():
    """Redirect /api/mcp/ to docs."""
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/api/mcp/docs")


@router.get("/health/ping", response_model=HealthPingResponse)
async def health_ping() -> HealthPingResponse:
    """Health check endpoint."""
    logger.debug("Health check requested")
    return HealthPingResponse(status="ok")


@router.get("/info", response_model=ServiceInfoResponse)
async def api_info() -> ServiceInfoResponse:
    """Get service information and available endpoints."""
    return ServiceInfoResponse(
        service=MCP_SERVER_NAME,
        version=MCP_VERSION,
        status="running",
        endpoints={
            "docs": "/api/mcp/docs",
            "health": "/api/mcp/health/ping",
            "health_dependencies": "/api/mcp/health/dependencies",
            "auth_callback": "/api/mcp/auth/callback",
            "mcp": "/api/mcp",
        },
        mcp={
            "transport": "streamable-http",
            "tools": [
                "login",
                "logout",
                "auth_status",
                "search_entity",
                "match_entity",
                "get_entities",
            ],
        },
    )


async def check_service_health(
    service_name: str, base_url: str
) -> Dict[str, Any]:
    """
    Check health of a dependent service.

    Args:
        service_name: Name of the service for logging
        base_url: Base URL of the service

    Returns:
        Dict with status, url, response_time_ms, and optional error
    """
    result: Dict[str, Any] = {
        "status": "unhealthy",
        "url": base_url,
        "response_time_ms": None,
        "error": None,
    }

    # Try common health check endpoints
    health_endpoints = [
        "/health/ping",
        "/health",
        "/ping",
        "",  # Try base URL as fallback
    ]

    start_time = time.time()

    async with httpx.AsyncClient(timeout=HEALTH_CHECK_TIMEOUT) as client:
        for endpoint in health_endpoints:
            try:
                url = f"{base_url.rstrip('/')}{endpoint}"
                response = await client.get(url)

                elapsed_ms = (time.time() - start_time) * 1000

                if response.status_code < 500:
                    # Consider any non-5xx response as healthy
                    result["status"] = "healthy"
                    result["response_time_ms"] = round(elapsed_ms, 2)
                    log_to_stderr(
                        f"[HEALTH] {service_name} is healthy "
                        f"(endpoint: {endpoint or '/'}, {elapsed_ms:.0f}ms)"
                    )
                    return result

            except httpx.TimeoutException:
                result["error"] = f"Timeout after {HEALTH_CHECK_TIMEOUT}s"
            except httpx.ConnectError as e:
                result["error"] = f"Connection failed: {str(e)}"
            except Exception as e:
                result["error"] = f"Error: {str(e)}"

    elapsed_ms = (time.time() - start_time) * 1000
    result["response_time_ms"] = round(elapsed_ms, 2)
    log_to_stderr(
        f"[HEALTH] {service_name} is unhealthy - {result['error']}", "ERROR"
    )
    return result


@router.get("/health/dependencies", response_model=DependenciesHealthResponse)
async def health_dependencies() -> DependenciesHealthResponse:
    """
    Check health of dependent services (Auth and MatchMaven).
    Returns 503 if any critical dependency is unhealthy.
    """
    log_to_stderr("[HEALTH] Checking dependency health...")
    logger.info("Dependency health check requested")

    # Check both services
    auth_health = await check_service_health("auth_service", AUTH_SERVICE_URL)
    matchmaven_health = await check_service_health("matchmaven_service", MATCHMAVEN_SERVICE_URL)

    # Determine overall status
    all_healthy = (
        auth_health["status"] == "healthy"
        and matchmaven_health["status"] == "healthy"
    )
    overall_status = "healthy" if all_healthy else "unhealthy"

    log_to_stderr(f"[HEALTH] Overall dependency status: {overall_status}")

    response = DependenciesHealthResponse(
        status=overall_status,
        service=MCP_SERVER_NAME,
        version=MCP_VERSION,
        dependencies={
            "auth_service": auth_health,
            "matchmaven_service": matchmaven_health,
            "sessions": {
                "status": "healthy",
                "active_sessions": session_manager.get_active_session_count(),
                "authenticated_sessions": session_manager.get_authenticated_session_count(),
            },
        },
    )

    # Return 503 if unhealthy (for load balancers/k8s)
    if not all_healthy:
        return JSONResponse(content=response.model_dump(), status_code=503)

    return response
