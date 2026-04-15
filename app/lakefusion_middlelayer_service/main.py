import os
import sys
from fastapi import FastAPI, HTTPException, Request
from starlette.responses import JSONResponse, RedirectResponse
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware

from lakefusion_utility.utils.logging_utils import get_logger, init_logger
# Initialize logger
init_logger(service="middle_layer_service")

from lakefusion_utility.utils.database import engine, get_db
from lakefusion_utility.services.audit_log_service import AuditLogMiddleware
from lakefusion_utility.utils.app_db import Base
from app.lakefusion_middlelayer_service.api.health_route import health_router
from app.lakefusion_middlelayer_service.api.dataset_route import dataset_router
from app.lakefusion_middlelayer_service.api.entity_route import entity_router
from app.lakefusion_middlelayer_service.api.survivorship_route import survivorship_router
from app.lakefusion_middlelayer_service.api.validation_functions_route import validation_functions_router
from app.lakefusion_middlelayer_service.api.profiling_task_route import profiling_task_router
from app.lakefusion_middlelayer_service.api.quality_task_route import quality_task_router
from app.lakefusion_middlelayer_service.api.entity_search_route import entity_search_router
from app.lakefusion_middlelayer_service.api.integration_hub_route import integration_hub_router
from app.lakefusion_middlelayer_service.api.business_glossary_route import business_glossary_router
from app.lakefusion_middlelayer_service.api.business_glossary_categories_route import business_glossary_categories_router
from app.lakefusion_middlelayer_service.api.quality_assets_route import quality_asset_router
from app.lakefusion_middlelayer_service.api.feature_flags_route import router as feature_flags_router
from app.lakefusion_middlelayer_service.api.db_config_properties_route import db_config_properties_router
from app.lakefusion_middlelayer_service.api.base_prompt_route import base_prompt_router
from app.lakefusion_middlelayer_service.api.dnb_integration_route import dnb_router
from app.lakefusion_middlelayer_service.api.logs_route import logs_router
from app.lakefusion_middlelayer_service.api.pt_models_route import pt_models_router
from app.lakefusion_middlelayer_service.api.schema_evolution_route import schema_evolution_router, schema_evolution_global_router
from app.lakefusion_middlelayer_service.api.spn_oauth_route import spn_router
from app.lakefusion_middlelayer_service.api.notebook_sync_route import notebook_sync_router
from lakefusion_utility.routes.ops import ops_router


sys.path.extend(os.path.dirname(__file__))  # Add current directory to path

# Prefix
app_prefix = "/api/middle-layer"

# Logger
logger = get_logger(__name__)

from lakefusion_utility.utils.database import lifespan


app = FastAPI(
    title="Middle Layer Service API",
    description="APIs related to middle layer service",
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

# Add Audit Log middleware (logs all API requests with masked sensitive data)
app.add_middleware(AuditLogMiddleware, db_session_factory=get_db)

# Register additional API routes
app.include_router(health_router, prefix=f'{app_prefix}')
app.include_router(dataset_router, prefix=f'{app_prefix}')
app.include_router(entity_router, prefix=f'{app_prefix}')
app.include_router(survivorship_router, prefix=f'{app_prefix}')
app.include_router(validation_functions_router, prefix=f'{app_prefix}')
app.include_router(profiling_task_router, prefix=f'{app_prefix}')
app.include_router(quality_task_router, prefix=f'{app_prefix}')
app.include_router(entity_search_router, prefix=f'{app_prefix}')
app.include_router(integration_hub_router, prefix=f'{app_prefix}')
app.include_router(business_glossary_router, prefix=f'{app_prefix}')
app.include_router(business_glossary_categories_router, prefix=f'{app_prefix}')
app.include_router(quality_asset_router, prefix=f'{app_prefix}')
app.include_router(feature_flags_router, prefix=f'{app_prefix}')
app.include_router(db_config_properties_router, prefix=f'{app_prefix}')
app.include_router(ops_router, prefix=f'{app_prefix}')
app.include_router(base_prompt_router, prefix=f'{app_prefix}')
app.include_router(pt_models_router, prefix=f'{app_prefix}')
app.include_router(dnb_router, prefix=f'{app_prefix}')
app.include_router(logs_router, prefix=f'{app_prefix}')
app.include_router(schema_evolution_router, prefix=f'{app_prefix}')
app.include_router(notebook_sync_router, prefix=f'{app_prefix}')
app.include_router(schema_evolution_global_router, prefix=f'{app_prefix}')
app.include_router(spn_router, prefix=f'{app_prefix}')

logger.info("API up and running")

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
