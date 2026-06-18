import os
import sys
from fastapi import FastAPI
from starlette.responses import RedirectResponse
import logging
from fastapi.middleware.cors import CORSMiddleware

from lakefusion_utility.utils.logging_utils import get_logger, init_logger
# Initialize logger BEFORE any lakefusion_utility import that touches
# app_db / models — those modules call get_logger() at module-load time
# and will raise RuntimeError("Logging not initialized") if we don't.
init_logger(service="cron_service")

# NOTE: do NOT import get_app_sp_token at module level here. The installed
# lakefusion_utility wheel has a latent circular import between
# databricks_util.py ↔ models/__init__.py ↔ utils/auth.py that only fires
# when databricks_util is the FIRST submodule touched. Other imports below
# (app_db, services, models) load the package along a safe path; once that
# settles, lazy-importing databricks_util inside functions works fine.

from app.lakefusion_cron_service.utils.app_db import engine
from app.lakefusion_cron_service.middleware import DBCleanupMiddleware
from lakefusion_utility.utils.app_db import Base
from app.lakefusion_cron_service.utils.app_db import get_db
from lakefusion_utility.services.audit_log_service import AuditLogMiddleware
from lakefusion_utility.models.dbconfig import DBConfigProperties
from lakefusion_utility.config_defaults import CONFIG_DEFAULTS
from sqlalchemy.orm import Session
from app.lakefusion_cron_service.config import run_dbx_pipeline_artifacts_import
# EVENT_JOB_ERROR fires for APScheduler-level failures (thread-pool exhaustion,
# misfire resolution errors) that bypass job_wrapper entirely — distinct from
# job-level exceptions which job_wrapper catches and logs itself.
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MAX_INSTANCES, EVENT_JOB_MISSED

from lakefusion_utility.routes.ops import ops_router

# Make sure we can import utils
sys.path.extend(os.path.dirname(__file__))

# Prefix
app_prefix = "/api/cron"

# Logger
logger = get_logger(__name__)


def _apscheduler_event_listener(event):
    job_id = getattr(event, "job_id", "<unknown>")
    if event.code == EVENT_JOB_MISSED:
        logger.warning(
            f"APS job missed job_id={job_id} scheduled_run_time={getattr(event, 'scheduled_run_time', None)}"
        )
    elif event.code == EVENT_JOB_MAX_INSTANCES:
        logger.warning(
            f"APS job NOT started - previous instance still running (likely hung) job_id={job_id}"
        )
    elif event.code == EVENT_JOB_ERROR:
        # Scheduler-level error (thread-pool exhaustion, misfire resolution, etc.).
        # These bypass job_wrapper, so this is the only place they are logged.
        # Note: exc_info=True would capture the *current* exception context, which
        # is empty inside an event listener.  Log the exception object directly and
        # emit the traceback string (when present) as a separate debug line.
        logger.error(
            f"APS scheduler error for job_id={job_id}: {getattr(event, 'exception', None)}",
        )
        tb = getattr(event, "traceback", None)
        if tb:
            logger.debug("APS scheduler error traceback for job_id=%s:\n%s", job_id, tb)

from lakefusion_utility.utils.database import lifespan as base_lifespan
from contextlib import asynccontextmanager

# Import your run_migrations helper
from app.lakefusion_cron_service.utils.run_migrations import run_migrations
from app.lakefusion_cron_service.utils.app_db import db_context, enable_pool_pre_ping
from lakefusion_utility.services.databricks_sync_service import import_lakefusion_artifacts
import lakefusion_utility.models.notebook_sync  # noqa: ensure tables are registered with Base
from app.lakefusion_cron_service.services.notebook_sync_executor import execute_sync
from app.lakefusion_cron_service.api.notebook_sync_route import notebook_sync_router
from lakefusion_utility.services.integration_hub_service import Integration_HubService
from lakefusion_utility.services.pt_models_service import PTModelsConfigService



@asynccontextmanager
async def lifespan(app):
    async with base_lifespan(app):
        enable_pool_pre_ping()
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
                    # Lazy import — see note at top of file about the
                    # latent circular import in the lakefusion_utility wheel.
                    from lakefusion_utility.utils.databricks_util import get_app_sp_token
                    service = Integration_HubService(db=db)
                    from lakefusion_utility.utils.databricks_util import get_app_sp_token
                    token = get_app_sp_token()
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
                    from lakefusion_utility.utils.databricks_util import get_app_sp_token, generate_pat
                    token = get_app_sp_token() or generate_pat()
                    if token:
                        logger.info("Syncing PT config to Volume on startup...")
                        service = PTModelsConfigService(db)
                        service.sync_to_volume(token)
                        logger.info("PT config synced to Volume on startup")
                    else:
                        logger.warning("No token available — skipping PT config startup sync")
                except Exception as e:
                    logger.error(f"Error during PT config startup sync: {e}")

        except Exception as e:
            logger.error(f"Error during startup: {e}")
            raise

        from apscheduler.schedulers.background import BackgroundScheduler
        from app.lakefusion_cron_service.cron_jobs import get_scheduler_jobs

        scheduler = BackgroundScheduler()
        scheduler.add_listener(
            _apscheduler_event_listener,
            EVENT_JOB_MISSED | EVENT_JOB_MAX_INSTANCES | EVENT_JOB_ERROR,
        )
        scheduler = get_scheduler_jobs(scheduler=scheduler)
        scheduler.start()

        aps_logger = get_logger("apscheduler")
        aps_logger.setLevel(logging.INFO)

        logger.info("\n--- Registered APScheduler Jobs ---")
        for job in scheduler.get_jobs():
            logger.info(f"ID: {job.id}, Next Run: {getattr(job, 'next_run_time', None)}, Trigger: {job.trigger}")
        logger.info("-----------------------------------\n")

        try:
            yield  # continue the application lifecycle
        finally:
            logger.info("Shutting down APScheduler")
            scheduler.shutdown(wait=False)

app = FastAPI(
    title="Cron Service API",
    description="APIs related to cron service",
    docs_url=f"{app_prefix}/docs",
    redoc_url=f"{app_prefix}/redoc",
    openapi_url=f"{app_prefix}/openapi.json",
    lifespan=lifespan,
)

# Add CORS middleware — use explicit origin when DATABRICKS_APP_URL is set
_app_url = os.environ.get("DATABRICKS_APP_URL", "")
_allowed_origins = [_app_url] if _app_url else ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
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

