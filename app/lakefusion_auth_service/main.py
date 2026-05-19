import os
import sys
from fastapi import FastAPI
from starlette.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware

from lakefusion_utility.utils.logging_utils import get_logger, init_logger
# Initialize logger
init_logger(service="auth_service")

from app.lakefusion_auth_service.api.health_route import health_router
from app.lakefusion_auth_service.api.auth_route import auth_router
from lakefusion_utility.models.oidc_request import *
from lakefusion_utility.utils.database import get_db
from lakefusion_utility.services.audit_log_service import AuditLogMiddleware

sys.path.extend(os.path.dirname(__file__))  # Add current directory to path

# Prefix
app_prefix = "/api/auth"

# Logger
logger = get_logger(__name__)

from lakefusion_utility.utils.database import lifespan

app = FastAPI(
    title="Auth Service API",
    description="APIs related to authentication service",
    docs_url=f"{app_prefix}/docs",
    redoc_url=f"{app_prefix}/redoc",
    openapi_url=f"{app_prefix}/openapi.json",
    lifespan=lifespan,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add Audit Log middleware (logs all API requests with masked sensitive data)
app.add_middleware(AuditLogMiddleware, db_session_factory=get_db)

# Register additional API routes
app.include_router(health_router, prefix=f'{app_prefix}')
app.include_router(auth_router, prefix=f'{app_prefix}')

logger.info("API up and running")

@app.get("/", include_in_schema=False)
async def original_endpoint():
    return RedirectResponse(url=f"{app_prefix}/docs")
