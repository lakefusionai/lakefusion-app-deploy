import os
import sys
from fastapi import FastAPI
from starlette.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware

from lakefusion_utility.utils.logging_utils import get_logger, init_logger
# Initialize logger
init_logger(service="cron_service")

from app.lakefusion_cron_service.utils.app_db import engine
from app.lakefusion_cron_service.middleware import DBCleanupMiddleware
from lakefusion_utility.utils.app_db import Base
from app.lakefusion_cron_service.utils.app_db import get_db
from lakefusion_utility.services.audit_log_service import AuditLogMiddleware
from lakefusion_utility.models.dbconfig import DBConfigProperties
from lakefusion_utility.config_defaults import CONFIG_DEFAULTS
from sqlalchemy.orm import Session
from app.lakefusion_cron_service.config import run_dbx_pipeline_artifacts_import

from lakefusion_utility.routes.ops import ops_router

# Make sure we can import utils
sys.path.extend(os.path.dirname(__file__))

# Prefix
app_prefix = "/api/cron"

# Logger
logger = get_logger(__name__)

from lakefusion_utility.utils.database import lifespan as base_lifespan
from contextlib import asynccontextmanager

# Import your run_migrations helper
from app.lakefusion_cron_service.utils.run_migrations import run_migrations
from app.lakefusion_cron_service.utils.app_db import db_context
from lakefusion_utility.services.databricks_sync_service import import_lakefusion_artifacts
import lakefusion_utility.models.notebook_sync  # noqa: ensure tables are registered with Base
from app.lakefusion_cron_service.services.notebook_sync_executor import execute_sync
from app.lakefusion_cron_service.api.notebook_sync_route import notebook_sync_router
from lakefusion_utility.services.integration_hub_service import Integration_HubService
from lakefusion_utility.services.pt_models_service import PTModelsConfigService



@asynccontextmanager
async def lifespan(app):
    async with base_lifespan(app):
        try:    
            with db_context() as db:
                # ------------------------------------------------------
                # Step 1: Run create_all to ensure all tables are created (Hotfix 3.4.1)
                # ------------------------------------------------------
                try:
                    logger.info("Creating database tables if they don't exist...")
                    Base.metadata.create_all(bind=db.get_bind())
                    logger.info("Database tables created successfully")
                except Exception as e:
                    logger.error(f"Error creating database tables: {e}")

                # ------------------------------------------------------
                # Step 2: Run Alembic migrations to update schema
                # ------------------------------------------------------
                try:
                    logger.info("Running Alembic migrations...")
                    run_migrations()   # This ensures DB schema is up-to-date
                    logger.info("Migrations completed successfully")
                except Exception as e:
                    logger.error(f"Migration failed: {e}")
                    raise  # Fail fast if migrations can't be applied

                # ------------------------------------------------------
                # Step 3: Insert default config values
                # ------------------------------------------------------
                logger.info("Inserting default config values if not present")
                for config_item in CONFIG_DEFAULTS:
                    config_key = config_item.get("config_key")
                    
                    # Check if a record with this config_key already exists
                    existing = db.query(DBConfigProperties).filter(
                        DBConfigProperties.config_key == config_key
                    ).first()
                    
                    if existing:
                        # Update only empty/None fields
                        for field_name, field_value in config_item.items():
                            if field_name == "config_key":
                                continue  # Skip the primary key
                            
                            # Get the current value of the field
                            current_value = getattr(existing, field_name, None)
                            
                            # Update only if current value is None or empty string
                            if current_value is None or current_value == "":
                                setattr(existing, field_name, field_value)
                    else:
                        # Create new record with all fields from config_item
                        db.add(DBConfigProperties(**config_item))

                db.commit()
                logger.info("Default config values inserted successfully")

                # ------------------------------------------------------
                # Step 4: Sync Databricks Jobs Versions
                # ------------------------------------------------------

                try:
                    service = Integration_HubService(db=db)
                    token = os.environ.get('LAKEFUSION_DATABRICKS_DAPI', '')
                    service.sync_all_job_versions(token)
                except Exception as e:
                    logger.error(f"Error syncing jobs versions: {e}")


                # ------------------------------------------------------
                # Step 5: Import Lakefusion Artifacts (registry-aware)
                # ------------------------------------------------------
                try:
                    if run_dbx_pipeline_artifacts_import:
                        logger.info("Running notebook sync (registry-aware)...")
                        result = execute_sync(db=db, force=False)
                        logger.info(f"Notebook sync complete: {result}")
                    else:
                        logger.info("Skipping Lakefusion artifacts import as per configuration")
                except Exception as e:
                    logger.error(f"Error during notebook sync: {e}")

                # ------------------------------------------------------
                # Step 6: Sync PT config to Volume
                # ------------------------------------------------------
                try:
                    token = os.environ.get('LAKEFUSION_DATABRICKS_DAPI', '')
                    if token:
                        logger.info("Syncing PT config to Volume on startup...")
                        service = PTModelsConfigService(db)
                        service.sync_to_volume(token)
                        logger.info("PT config synced to Volume on startup")
                    else:
                        logger.warning("LAKEFUSION_DATABRICKS_DAPI not set, skipping PT config startup sync")
                except Exception as e:
                    logger.error(f"Error during PT config startup sync: {e}")

        except Exception as e:
            logger.error(f"Error during startup: {e}")
            raise

        yield  # continue the application lifecycle

app = FastAPI(
    title="Cron Service API",
    description="APIs related to cron service",
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

app.include_router(ops_router, prefix=f'{app_prefix}')
app.include_router(notebook_sync_router, prefix=f'{app_prefix}')

logger.info("API up and running")

@app.get("/", include_in_schema=False)
async def original_endpoint():
    return RedirectResponse(url=f"{app_prefix}/docs")

# APScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from app.lakefusion_cron_service.cron_jobs import get_scheduler_jobs

# Create the scheduler instance
scheduler = BackgroundScheduler()
scheduler = get_scheduler_jobs(scheduler=scheduler)
scheduler.start()

# Print all registered jobs
print("\n--- Registered APScheduler Jobs ---")
for job in scheduler.get_jobs():
    print(f"ID: {job.id}, Next Run: {job.next_run_time}, Trigger: {job.trigger}")
print("-----------------------------------\n")
