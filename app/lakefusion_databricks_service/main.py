import os
import sys
from fastapi import FastAPI
from starlette.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware

from lakefusion_utility.utils.logging_utils import get_logger, init_logger
# Initialize logger
init_logger(service="databricks_service")

from app.lakefusion_databricks_service.utils.app_db import engine
from lakefusion_utility.utils.app_db import Base
from app.lakefusion_databricks_service.middleware import DBCleanupMiddleware
from app.lakefusion_databricks_service.utils.app_db import get_db
from lakefusion_utility.services.audit_log_service import AuditLogMiddleware
from app.lakefusion_databricks_service.api.health_route import health_router
from app.lakefusion_databricks_service.api.compute_route import compute_router
from app.lakefusion_databricks_service.api.dataset_route import dataset_router
from app.lakefusion_databricks_service.api.catalog_route import catalog_router
from app.lakefusion_databricks_service.api.notebook_route import notebook_router
from app.lakefusion_databricks_service.api.quality_task_route import quality_task_router
from lakefusion_utility.routes.ops import ops_router
# from app.lakefusion_databricks_service.api.reference_entity_route import reference_entity_router

sys.path.extend(os.path.dirname(__file__))  # Add current directory to path

# Prefix
app_prefix = "/api/databricks"

# Logger
logger = get_logger(__name__)

from lakefusion_utility.utils.database import lifespan

app = FastAPI(
    title="Databricks Service API",
    description="APIs related to databricks service",
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
app.add_middleware(DBCleanupMiddleware, db_session=get_db)

# Add Audit Log middleware (logs all API requests with masked sensitive data)
app.add_middleware(AuditLogMiddleware, db_session_factory=get_db)

# Register additional API routes
app.include_router(health_router, prefix=f'{app_prefix}')
app.include_router(compute_router, prefix=f'{app_prefix}')
app.include_router(dataset_router, prefix=f'{app_prefix}')
app.include_router(catalog_router, prefix=f'{app_prefix}')
app.include_router(notebook_router, prefix=f'{app_prefix}')
app.include_router(quality_task_router, prefix=f'{app_prefix}')
app.include_router(ops_router, prefix=f'{app_prefix}')
#app.include_router(reference_entity_router, prefix=f'{app_prefix}')

logger.info("API up and running")

@app.get("/", include_in_schema=False)
async def original_endpoint():
    return RedirectResponse(url=f"{app_prefix}/docs")