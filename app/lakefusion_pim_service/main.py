import os
import sys
from contextlib import asynccontextmanager
from fastapi import Depends, FastAPI, HTTPException, Request
from starlette.responses import JSONResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware

from lakefusion_utility.utils.logging_utils import get_logger, init_logger
# Initialize logger
init_logger(service="pim_service")

from sqlalchemy.orm import Session
from lakefusion_utility.utils.database import engine, get_db, SessionLocal
from lakefusion_utility.utils.database import lifespan as base_lifespan
from lakefusion_utility.services.audit_log_service import AuditLogMiddleware
from lakefusion_utility.utils.app_db import Base
from app.lakefusion_pim_service.utils.app_db import stop_all_token_refresh
from app.lakefusion_pim_service.api.health_route import health_router
from app.lakefusion_pim_service.api.pim_taxonomy_route import pim_taxonomy_router
from app.lakefusion_pim_service.api.pim_attribute_route import pim_attribute_router
from app.lakefusion_pim_service.api.pim_config_route import pim_config_router
from app.lakefusion_pim_service.api.pim_reference_route import pim_reference_router
from app.lakefusion_pim_service.api.pim_entity_route import pim_entity_router
from app.lakefusion_pim_service.api.pim_assortment_route import pim_assortment_router
from app.lakefusion_pim_service.api.pim_entity_tier_route import pim_entity_tier_router
from app.lakefusion_pim_service.api.pim_tab_group_route import pim_tab_group_router
from app.lakefusion_pim_service.api.pim_entity_bridge_route import pim_entity_bridge_router
from app.lakefusion_pim_service.api.pim_model_serving_route import pim_model_serving_router


sys.path.extend(os.path.dirname(__file__))  # Add current directory to path

# Prefix
app_prefix = "/api/pim"

# Logger
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize both database engines on startup."""
    async with base_lifespan(app):
        try:
            # ----------------------------------------------------------
            # Step 1: Create PIM tables on Data DB (Postgres/Lakebase)
            # ----------------------------------------------------------
            # ----------------------------------------------------------
            # Data DB initialization is LAZY — triggered by
            # "Initialize PIM" button in the MDM portal via
            # POST /api/pim/entity-bridge/{entity_id}/initialize
            #
            # The PIM service starts with MySQL only (feature flags,
            # auth, entity bridge). Data DB endpoints return 503
            # until initialization is triggered.
            # ----------------------------------------------------------
            logger.info("Data DB: waiting for initialization via entity bridge (lazy mode)")

        except Exception as e:
            logger.error(f"Startup error: {e}")

        yield

        # Shutdown: stop all entity token refresh tasks
        await stop_all_token_refresh()


app = FastAPI(
    title="PIM Service API",
    description="Product Information Management APIs",
    docs_url=f"{app_prefix}/docs",
    redoc_url=f"{app_prefix}/redoc",
    openapi_url=f"{app_prefix}/openapi.json",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add Audit Log middleware — write audit logs to MySQL (transactional DB)
app.add_middleware(AuditLogMiddleware, db_session_factory=SessionLocal)

# ---------------------------------------------------------------------------
# Feature Flag  (SCRUM-845) — gates all PIM endpoints via MySQL feature_flags table
# Default: enabled (True). Set ENABLE_PIM to INACTIVE in feature_flags table to disable.
# ---------------------------------------------------------------------------
def require_pim_enabled(db: Session = Depends(get_db)):
    from lakefusion_utility.services.feature_flags_service import FeatureFlagService
    flag = FeatureFlagService._get_feature_flag(db=db, name="ENABLE_PIM")
    if flag and flag["status"] == "INACTIVE":
        raise HTTPException(status_code=404, detail="PIM features are not enabled.")


pim_dependency = [Depends(require_pim_enabled)]

# ---------------------------------------------------------------------------
# Meta routes (no entity context) — stay at /api/pim
# ---------------------------------------------------------------------------
app.include_router(health_router, prefix=f'{app_prefix}')
app.include_router(pim_entity_bridge_router, prefix=f'{app_prefix}', dependencies=pim_dependency)
app.include_router(pim_model_serving_router, prefix=f'{app_prefix}', dependencies=pim_dependency)

# ---------------------------------------------------------------------------
# Entity-scoped routes — under /api/pim/{entity_name}
# The {entity_name} path parameter is auto-resolved by get_data_db() dependency
# via request.path_params["entity_name"]. Zero changes to endpoint functions.
# ---------------------------------------------------------------------------
from fastapi import APIRouter
entity_scoped_router = APIRouter(prefix="/{entity_name}")
entity_scoped_router.include_router(pim_taxonomy_router)
entity_scoped_router.include_router(pim_attribute_router)
entity_scoped_router.include_router(pim_config_router)
entity_scoped_router.include_router(pim_reference_router)
entity_scoped_router.include_router(pim_entity_router)
entity_scoped_router.include_router(pim_assortment_router)
entity_scoped_router.include_router(pim_entity_tier_router)
entity_scoped_router.include_router(pim_tab_group_router)
app.include_router(entity_scoped_router, prefix=f'{app_prefix}', dependencies=pim_dependency)

logger.info("PIM Service API up and running")

@app.get("/", include_in_schema=False)
async def original_endpoint():
    return RedirectResponse(url=f"{app_prefix}/docs")


# Intercept HTTP exceptions and return custom JSON responses
@app.exception_handler(HTTPException)
async def custom_http_exception_handler(request: Request, exc: HTTPException):
    # If detail is a dict with our structure, return it directly
    if isinstance(exc.detail, dict) and {"status", "message", "data"} <= exc.detail.keys():
        return JSONResponse(status_code=exc.status_code, content=exc.detail)

    # fallback for default FastAPI behavior
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "status": exc.status_code,
            "message": str(exc.detail),
            "data": None
        }
    )
