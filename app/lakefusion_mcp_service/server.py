"""
FastAPI server for health checks and monitoring.
MCP tools are exposed via StreamableHTTP transport for Claude Desktop.
"""
import asyncio
import signal
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI, APIRouter
from fastapi.staticfiles import StaticFiles
from fastmcp import FastMCP
from fastapi.middleware.cors import CORSMiddleware
from app.lakefusion_mcp_service.utils.logger import get_logger
from app.lakefusion_mcp_service.core.tools import load_tools
from app.lakefusion_mcp_service.api.health_route import router as health_router
from app.lakefusion_mcp_service.api.auth_route import router as auth_router
from app.lakefusion_mcp_service.config import MCP_SERVER_NAME, MCP_VERSION
from app.lakefusion_mcp_service.services.session_manager import session_manager

# Root router for / endpoint (needs to be included without prefix)
root_router = APIRouter()


logger = get_logger(__name__)

# ============================================================================
# MCP Server Setup
# ============================================================================

mcp_server = FastMCP(name=MCP_SERVER_NAME)
load_tools(mcp_server)
mcp_app = mcp_server.http_app()

# API prefix
app_prefix = "/api/mcp"


# ============================================================================
# Application Lifespan & Graceful Shutdown
# ============================================================================

# Graceful shutdown timeout (seconds) - how long to wait for active sessions
SHUTDOWN_TIMEOUT = 30

async def cleanup_expired_sessions_task():
    """Background task to cleanup expired sessions periodically."""
    while True:
        await asyncio.sleep(3600)  # Run every hour
        await session_manager.cleanup_expired_sessions()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan - handles startup and shutdown."""
    logger.info(f"Starting {MCP_SERVER_NAME} v{MCP_VERSION}")
    logger.info(f"API docs available at {app_prefix}/docs")
    logger.info("MCP StreamableHTTP endpoint available at /api/mcp")

    # Setup graceful shutdown handler
    shutdown_event = asyncio.Event()

    def handle_shutdown(signum, _frame):
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        shutdown_event.set()

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    # Start background cleanup task
    cleanup_task = asyncio.create_task(cleanup_expired_sessions_task())

    # Initialize MCP app's lifespan (required for StreamableHTTP)
    async with mcp_app.lifespan(app):
        yield

    # Shutdown: cancel cleanup task
    logger.info(f"Shutting down gracefully (timeout: {SHUTDOWN_TIMEOUT}s)...")
    cleanup_task.cancel()
    try:
        await cleanup_task
    except asyncio.CancelledError:
        pass

    # Cleanup all remaining sessions with timeout
    active_sessions = list(session_manager.sessions.keys())
    if active_sessions:
        logger.info(f"Cleaning up {len(active_sessions)} active session(s)...")
        for session_id in active_sessions:
            await session_manager.cleanup_session(session_id)

    logger.info(f"Shutdown complete for {MCP_SERVER_NAME}")


# ============================================================================
# Main Application
# ============================================================================

app = FastAPI(
    title=MCP_SERVER_NAME,
    description="LakeFusion MCP service with MatchMaven tools for entity matching and search",
    version=MCP_VERSION,
    docs_url=f"{app_prefix}/docs",
    redoc_url=f"{app_prefix}/redoc",
    openapi_url=f"{app_prefix}/openapi.json",
    lifespan=lifespan,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files for CSS/JS assets
static_path = Path(__file__).parent / "static"
app.mount(f"{app_prefix}/static", StaticFiles(directory=static_path), name="static")

# Include routers BEFORE mounting MCP app (mounted apps catch all unmatched routes)
app.include_router(root_router)  # Root redirect at /
app.include_router(health_router, prefix=app_prefix, tags=["Health"])
app.include_router(auth_router, prefix=app_prefix, tags=["Authentication"])

# Mount MCP StreamableHTTP app at /api - must be AFTER routers
# http_app() defaults to /mcp internally, so mounting at /api gives us /api/mcp
app.mount("/api", mcp_app)




