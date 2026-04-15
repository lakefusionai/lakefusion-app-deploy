#!/usr/bin/env bash
# =============================================================================
# build_lakefusion_app.sh
#
# Assembles the unified LakeFusion Databricks App from the monorepo services.
# - Builds lakefusion_utility as a wheel (installed via requirements.txt)
# - Copies each service's app/ folder as a subpackage, rewrites `from app.` imports
# - Generates a unified main.py, merged requirements.txt, and app.yaml
#
# Usage:  bash scripts/build_lakefusion_app.sh
# =============================================================================

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$REPO_ROOT/lakefusion-app"

# ── Colors ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# ── Cross-platform sed -i (macOS vs Linux) ──────────────────────────────────
if [[ "$(uname)" == "Darwin" ]]; then
    sedi() { sed -i '' "$@"; }
else
    sedi() { sed -i "$@"; }
fi

# ── Step 0: Clean previous build (preserve .env and venv) ────────────────────
info "Cleaning previous build at $OUT"
# Preserve .env if it exists
if [[ -f "$OUT/.env" ]]; then
    cp "$OUT/.env" /tmp/.lakefusion-app-env-preserve
fi
rm -rf "$OUT"
mkdir -p "$OUT/app"
# Restore .env
if [[ -f /tmp/.lakefusion-app-env-preserve ]]; then
    mv /tmp/.lakefusion-app-env-preserve "$OUT/.env"
    info "Preserved existing .env"
fi

# ── Service definitions ─────────────────────────────────────────────────────
# Format: source_dir:subpackage_name
SERVICES=(
    "lakefusion-auth-service:lakefusion_auth_service"
    "lakefusion-middlelayer-service:lakefusion_middlelayer_service"
    "lakefusion-databricks-service:lakefusion_databricks_service"
    "lakefusion-cron-service:lakefusion_cron_service"
    "lakefusion-matchmaven-service:lakefusion_matchmaven_service"
    "lakefusion-mcp-service:lakefusion_mcp_service"
)

# ── Step 1: Build lakefusion_utility wheel ──────────────────────────────────
info "Building lakefusion_utility wheel"
UTIL_DIR="$REPO_ROOT/lakefusion-utility"
# Use timestamp-based version so the wheel filename changes on every build.
# This forces Databricks Apps to reinstall even when requirements.txt is cached.
BUILD_VERSION="1.0.$(date +%Y%m%d%H%M%S)"
(
    cd "$UTIL_DIR"
    sed "s/version=\"1.0.0\"/version=\"${BUILD_VERSION}\"/" setup.py > /tmp/lf_util_setup.py
    cp /tmp/lf_util_setup.py setup.py
    python3 -m pip wheel --no-deps --wheel-dir "$OUT/wheels" . 2>&1 | tail -3
    git checkout setup.py 2>/dev/null  # restore original
)
WHEEL_FILE=$(ls "$OUT/wheels"/lakefusion_utility-*.whl 2>/dev/null | head -1)
if [[ -z "$WHEEL_FILE" ]]; then
    err "Failed to build lakefusion_utility wheel"
    exit 1
fi
WHEEL_BASENAME=$(basename "$WHEEL_FILE")
info "Built wheel: $WHEEL_BASENAME"

# ── Step 2: Copy each service's app/ folder ─────────────────────────────────
for entry in "${SERVICES[@]}"; do
    SVC_DIR="${entry%%:*}"
    PKG_NAME="${entry##*:}"
    SRC="$REPO_ROOT/$SVC_DIR/app"

    if [[ ! -d "$SRC" ]]; then
        warn "Skipping $SVC_DIR (no app/ directory found)"
        continue
    fi

    info "Copying $SVC_DIR/app → app/$PKG_NAME"
    cp -R "$SRC" "$OUT/app/$PKG_NAME"

    # Ensure __init__.py exists
    touch "$OUT/app/$PKG_NAME/__init__.py"

    # Remove __pycache__ and tests dirs
    find "$OUT/app/$PKG_NAME" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
    find "$OUT/app/$PKG_NAME" -name "tests" -type d -exec rm -rf {} + 2>/dev/null || true

    # Copy service-level middleware.py if it exists (cron, databricks, matchmaven have this)
    if [[ -f "$REPO_ROOT/$SVC_DIR/middleware.py" ]]; then
        info "  Copying $SVC_DIR/middleware.py → app/$PKG_NAME/middleware.py"
        cp "$REPO_ROOT/$SVC_DIR/middleware.py" "$OUT/app/$PKG_NAME/middleware.py"
    fi
done

# ── Step 3: Copy alembic infrastructure from cron service ───────────────────
info "Copying alembic infrastructure from lakefusion-cron-service"
cp "$REPO_ROOT/lakefusion-cron-service/alembic.ini" "$OUT/alembic.ini"
cp -R "$REPO_ROOT/lakefusion-cron-service/alembic" "$OUT/alembic"
find "$OUT/alembic" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true

# ── Step 3b: Copy version.json (read at runtime by lakefusion_utility) ──────
info "Copying version.json"
cp "$REPO_ROOT/version.json" "$OUT/version.json"

# ── Step 3c: Copy dbx_pipeline_artifacts (notebook sync on startup) ─────────
info "Copying dbx_pipeline_artifacts"
cp -R "$REPO_ROOT/dbx_pipeline_artifacts" "$OUT/dbx_pipeline_artifacts"
find "$OUT/dbx_pipeline_artifacts" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
find "$OUT/dbx_pipeline_artifacts" -name "*.pyc" -delete 2>/dev/null || true

# ── Step 3d: Copy deployment utilities ───────────────────────────────────────
if [[ -d "$REPO_ROOT/utilities" ]]; then
    info "Copying utilities → deployments/"
    mkdir -p "$OUT/deployments"
    cp -R "$REPO_ROOT/utilities/"* "$OUT/deployments/"
fi

# ── Step 4: Copy MCP static files if they exist ────────────────────────────
if [[ -d "$REPO_ROOT/lakefusion-mcp-service/app/static" ]]; then
    info "Copying MCP static files"
    cp -R "$REPO_ROOT/lakefusion-mcp-service/app/static" "$OUT/app/lakefusion_mcp_service/static"
fi

# ── Step 5: Rewrite imports ─────────────────────────────────────────────────
# Only rewrite `from app.xxx` (service-internal) and `from middleware import`.
# `from lakefusion_utility.` stays as-is (installed via wheel).
info "Rewriting imports in copied files"

for entry in "${SERVICES[@]}"; do
    PKG_NAME="${entry##*:}"
    PKG_DIR="$OUT/app/$PKG_NAME"

    [[ ! -d "$PKG_DIR" ]] && continue

    info "  Rewriting imports in $PKG_NAME"

    # Find all .py files in this service package
    while IFS= read -r pyfile; do
        # 1. Rewrite "from app.xxx" → "from app.<pkg_name>.xxx"
        sedi -E "s/from app\.([a-zA-Z_][a-zA-Z0-9_]*)/from app.${PKG_NAME}.\1/g" "$pyfile"

        # 2. Rewrite "import app.xxx" → "import app.<pkg_name>.xxx"
        sedi -E "s/^import app\.([a-zA-Z_])/import app.${PKG_NAME}.\1/g" "$pyfile"

        # 3. Rewrite "from middleware import" → "from app.<pkg_name>.middleware import"
        sedi -E "s/from middleware import/from app.${PKG_NAME}.middleware import/g" "$pyfile"

    done < <(find "$PKG_DIR" -name "*.py" -type f)
done

# ── Step 6: Generate app/__init__.py ────────────────────────────────────────
info "Generating app/__init__.py"
cat > "$OUT/app/__init__.py" << 'PYEOF'
# LakeFusion Unified Databricks App
PYEOF

# ── Step 6b: Generate app/utils/app_db.py shim ─────────────────────────────
# The installed lakefusion_utility wheel imports `from app.utils.app_db import ...`
# and `from app.config import AppConfig`. These shims delegate to the cron service's
# copies which have the full set of helpers (get_db, db_context, token_required_wrapper, etc.)
info "Generating app/utils/app_db.py shim"
mkdir -p "$OUT/app/utils"
touch "$OUT/app/utils/__init__.py"
cat > "$OUT/app/utils/app_db.py" << 'PYEOF'
"""
Shim for lakefusion_utility wheel imports.
Re-exports from the cron service's app_db.
"""
from app.lakefusion_cron_service.utils.app_db import *  # noqa: F401,F403
PYEOF

# ── Step 7: Generate unified app/main.py ────────────────────────────────────
info "Generating unified app/main.py"
cat > "$OUT/app/main.py" << 'PYEOF'
"""
LakeFusion Unified Databricks App
==================================
Single FastAPI entry point that mounts all service routers.
Generated by scripts/build_lakefusion_app.sh
"""
import os
import sys
from fastapi import FastAPI
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import RedirectResponse

# ---------------------------------------------------------------------------
# Logger init (must happen before any service imports)
# ---------------------------------------------------------------------------
from lakefusion_utility.utils.logging_utils import get_logger, init_logger
init_logger(service="lakefusion_app")
logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Database & ORM imports
# ---------------------------------------------------------------------------
from lakefusion_utility.utils.database import lifespan as base_lifespan
from lakefusion_utility.utils.app_db import Base
from lakefusion_utility.models.dbconfig import DBConfigProperties
from lakefusion_utility.config_defaults import CONFIG_DEFAULTS

# Cron-service specific imports for lifespan
from app.lakefusion_cron_service.utils.app_db import db_context, get_db, engine
from app.lakefusion_cron_service.utils.run_migrations import run_migrations
from app.lakefusion_cron_service.config import run_dbx_pipeline_artifacts_import
from app.lakefusion_cron_service.services.notebook_sync_executor import execute_sync
from lakefusion_utility.services.integration_hub_service import Integration_HubService
from lakefusion_utility.services.pt_models_service import PTModelsConfigService
import lakefusion_utility.models.notebook_sync  # noqa: ensure tables are registered

# ---------------------------------------------------------------------------
# Auth service routers
# ---------------------------------------------------------------------------
from app.lakefusion_auth_service.api.health_route import health_router as auth_health_router
from app.lakefusion_auth_service.api.auth_route import auth_router

# ---------------------------------------------------------------------------
# Middlelayer service routers
# ---------------------------------------------------------------------------
from app.lakefusion_middlelayer_service.api.health_route import health_router as ml_health_router
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
from app.lakefusion_middlelayer_service.api.notebook_sync_route import notebook_sync_router as ml_notebook_sync_router

# ---------------------------------------------------------------------------
# Databricks service routers
# ---------------------------------------------------------------------------
from app.lakefusion_databricks_service.api.health_route import health_router as dbx_health_router
from app.lakefusion_databricks_service.api.compute_route import compute_router
from app.lakefusion_databricks_service.api.dataset_route import dataset_router as dbx_dataset_router
from app.lakefusion_databricks_service.api.catalog_route import catalog_router
from app.lakefusion_databricks_service.api.notebook_route import notebook_router
from app.lakefusion_databricks_service.api.quality_task_route import quality_task_router as dbx_quality_task_router

# ---------------------------------------------------------------------------
# Cron service routers
# ---------------------------------------------------------------------------
from app.lakefusion_cron_service.api.notebook_sync_route import notebook_sync_router as cron_notebook_sync_router

# ---------------------------------------------------------------------------
# MatchMaven service routers
# ---------------------------------------------------------------------------
from app.lakefusion_matchmaven_service.api.health_route import health_router as mm_health_router
from app.lakefusion_matchmaven_service.api.match_maven import match_maven_router
from app.lakefusion_matchmaven_service.api.models import models_router
from app.lakefusion_matchmaven_service.api.experiments import experiments_router
from app.lakefusion_matchmaven_service.api.match_search import match_search_router
from app.lakefusion_matchmaven_service.api.playground import playground_router

# ---------------------------------------------------------------------------
# MCP service routers (health + auth only; MCP streaming mount skipped)
# ---------------------------------------------------------------------------
from app.lakefusion_mcp_service.api.health_route import router as mcp_health_router
from app.lakefusion_mcp_service.api.auth_route import router as mcp_auth_router

# ---------------------------------------------------------------------------
# Shared ops router (from utility — installed via wheel)
# ---------------------------------------------------------------------------
from lakefusion_utility.routes.ops import ops_router

# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------
from lakefusion_utility.services.audit_log_service import AuditLogMiddleware
from app.lakefusion_cron_service.middleware import DBCleanupMiddleware


# ===========================================================================
# Lifespan (startup/shutdown)
# ===========================================================================
@asynccontextmanager
async def lifespan(app):
    """
    Unified lifespan combining:
    - base_lifespan from lakefusion_utility (DB engine init, connection pool warm-up, token refresh)
    - Cron service startup steps (create_all, alembic, config defaults, job sync, notebook sync, PT sync)
    """
    async with base_lifespan(app):
        try:
            with db_context() as db:
                # Step 1: Create all tables (fallback for skipped migrations)
                try:
                    logger.info("Creating database tables if they don't exist...")
                    Base.metadata.create_all(bind=db.get_bind())
                    logger.info("Database tables created successfully")
                except Exception as e:
                    logger.error(f"Error creating database tables: {e}")

                # Step 2: Run Alembic migrations
                try:
                    logger.info("Running Alembic migrations...")
                    run_migrations()
                    logger.info("Migrations completed successfully")
                except Exception as e:
                    logger.error(f"Migration failed: {e}")
                    raise

                # Step 3: Insert default config values
                logger.info("Inserting default config values if not present")
                for config_item in CONFIG_DEFAULTS:
                    config_key = config_item.get("config_key")
                    existing = db.query(DBConfigProperties).filter(
                        DBConfigProperties.config_key == config_key
                    ).first()
                    if existing:
                        for field_name, field_value in config_item.items():
                            if field_name == "config_key":
                                continue
                            current_value = getattr(existing, field_name, None)
                            if current_value is None or current_value == "":
                                setattr(existing, field_name, field_value)
                    else:
                        db.add(DBConfigProperties(**config_item))
                db.commit()
                logger.info("Default config values inserted successfully")

                # Step 4: Sync Databricks Jobs Versions
                try:
                    service = Integration_HubService(db=db)
                    token = os.environ.get('LAKEFUSION_DATABRICKS_DAPI', '')
                    service.sync_all_job_versions(token)
                except Exception as e:
                    logger.error(f"Error syncing jobs versions: {e}")

                # Step 5: Import Lakefusion Artifacts (registry-aware)
                try:
                    if run_dbx_pipeline_artifacts_import:
                        logger.info("Running notebook sync (registry-aware)...")
                        result = execute_sync(db=db, force=False)
                        logger.info(f"Notebook sync complete: {result}")
                    else:
                        logger.info("Skipping Lakefusion artifacts import as per configuration")
                except Exception as e:
                    logger.error(f"Error during notebook sync: {e}")

                # Step 6: Sync PT config to Volume
                try:
                    token = os.environ.get('LAKEFUSION_DATABRICKS_DAPI', '')
                    if token:
                        logger.info("Syncing PT config to Volume on startup...")
                        pt_service = PTModelsConfigService(db)
                        pt_service.sync_to_volume(token)
                        logger.info("PT config synced to Volume on startup")
                    else:
                        logger.warning("LAKEFUSION_DATABRICKS_DAPI not set, skipping PT config startup sync")
                except Exception as e:
                    logger.error(f"Error during PT config startup sync: {e}")

        except Exception as e:
            logger.error(f"Error during startup: {e}")
            raise

        # Step 7: Start APScheduler (after all DB work is done to avoid deadlocks on PG)
        from apscheduler.schedulers.background import BackgroundScheduler
        from app.lakefusion_cron_service.cron_jobs import get_scheduler_jobs

        scheduler = BackgroundScheduler()
        scheduler = get_scheduler_jobs(scheduler=scheduler)
        scheduler.start()

        logger.info("--- Registered APScheduler Jobs ---")
        for job in scheduler.get_jobs():
            logger.info(f"ID: {job.id}, Next Run: {job.next_run_time}, Trigger: {job.trigger}")
        logger.info("-----------------------------------")

        logger.info("LakeFusion Unified App startup complete")
        yield

        # Shutdown: stop scheduler
        scheduler.shutdown(wait=False)
        logger.info("APScheduler shut down")


# ===========================================================================
# FastAPI App
# ===========================================================================
app = FastAPI(
    title="LakeFusion",
    description="LakeFusion Unified Databricks App",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
)

# Middleware to handle Databricks Apps authentication
from fastapi import Request as FastAPIRequest

@app.middleware("http")
async def databricks_auth_middleware(request: FastAPIRequest, call_next):
    """
    Handle Databricks authentication for API routes.
    In Databricks Apps, authentication is handled automatically via cookies/headers.
    This middleware ensures proper header propagation.
    """
    response = await call_next(request)
    response.headers["Access-Control-Allow-Credentials"] = "true"
    response.headers["X-Frame-Options"] = "SAMEORIGIN"
    return response

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(DBCleanupMiddleware, db_session=get_db)
app.add_middleware(AuditLogMiddleware, db_session_factory=get_db)

# ===========================================================================
# Route Registration
# ===========================================================================

# --- Auth Service (/api/auth) ---
app.include_router(auth_health_router, prefix="/api/auth")
app.include_router(auth_router, prefix="/api/auth")

# --- Middlelayer Service (/api/middle-layer) ---
app.include_router(ml_health_router, prefix="/api/middle-layer")
app.include_router(dataset_router, prefix="/api/middle-layer")
app.include_router(entity_router, prefix="/api/middle-layer")
app.include_router(survivorship_router, prefix="/api/middle-layer")
app.include_router(validation_functions_router, prefix="/api/middle-layer")
app.include_router(profiling_task_router, prefix="/api/middle-layer")
app.include_router(quality_task_router, prefix="/api/middle-layer")
app.include_router(entity_search_router, prefix="/api/middle-layer")
app.include_router(integration_hub_router, prefix="/api/middle-layer")
app.include_router(business_glossary_router, prefix="/api/middle-layer")
app.include_router(business_glossary_categories_router, prefix="/api/middle-layer")
app.include_router(quality_asset_router, prefix="/api/middle-layer")
app.include_router(feature_flags_router, prefix="/api/middle-layer")
app.include_router(db_config_properties_router, prefix="/api/middle-layer")
app.include_router(base_prompt_router, prefix="/api/middle-layer")
app.include_router(pt_models_router, prefix="/api/middle-layer")
app.include_router(dnb_router, prefix="/api/middle-layer")
app.include_router(logs_router, prefix="/api/middle-layer")
app.include_router(schema_evolution_router, prefix="/api/middle-layer")
app.include_router(ml_notebook_sync_router, prefix="/api/middle-layer")
app.include_router(schema_evolution_global_router, prefix="/api/middle-layer")
app.include_router(spn_router, prefix="/api/middle-layer")

# --- Databricks Service (/api/databricks) ---
app.include_router(dbx_health_router, prefix="/api/databricks")
app.include_router(compute_router, prefix="/api/databricks")
app.include_router(dbx_dataset_router, prefix="/api/databricks")
app.include_router(catalog_router, prefix="/api/databricks")
app.include_router(notebook_router, prefix="/api/databricks")
app.include_router(dbx_quality_task_router, prefix="/api/databricks")

# --- Cron Service (/api/cron) ---
app.include_router(cron_notebook_sync_router, prefix="/api/cron")

# --- MatchMaven Service (/api/match-maven) ---
app.include_router(mm_health_router, prefix="/api/match-maven")
app.include_router(match_maven_router, prefix="/api/match-maven")
app.include_router(models_router, prefix="/api/match-maven")
app.include_router(experiments_router, prefix="/api/match-maven")
app.include_router(match_search_router, prefix="/api/match-maven")
app.include_router(playground_router, prefix="/api/match-maven")

# --- MCP Service (/api/mcp) --- (health + auth only; MCP streaming mount skipped)
app.include_router(mcp_health_router, prefix="/api/mcp")
app.include_router(mcp_auth_router, prefix="/api/mcp")

# --- Shared ops router (mounted under each service prefix for compatibility) ---
app.include_router(ops_router, prefix="/api/middle-layer")
app.include_router(ops_router, prefix="/api/databricks")
app.include_router(ops_router, prefix="/api/cron")
app.include_router(ops_router, prefix="/api/match-maven")

# ===========================================================================
# Static Files & SPA Serving (UI)
# ===========================================================================
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse
from pathlib import Path

static_dir = Path(__file__).parent.parent / "static"

if static_dir.exists():
    # Mount static files for JS bundles, CSS, images
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
    logger.info(f"Static files mounted from {static_dir}")

    # Serve config.js and importmap.json from root (index.html references them as /config.js)
    @app.get("/config.js", include_in_schema=False)
    async def serve_config():
        config_file = static_dir / "config.js"
        if config_file.is_file():
            return FileResponse(config_file, media_type="application/javascript")
        return HTMLResponse("// config.js not found", status_code=404)

    @app.get("/importmap.json", include_in_schema=False)
    async def serve_importmap():
        importmap_file = static_dir / "importmap.json"
        if importmap_file.is_file():
            return FileResponse(importmap_file, media_type="application/json")
        return HTMLResponse("{}", status_code=404)

    @app.get("/version.json", include_in_schema=False)
    async def serve_version():
        # Try static/ first, then app root
        for loc in [static_dir / "version.json", Path(__file__).parent.parent / "version.json"]:
            if loc.is_file():
                return FileResponse(loc, media_type="application/json")
        return HTMLResponse("{}", status_code=404)

    # Serve logo/favicon from static
    @app.get("/logo.svg", include_in_schema=False)
    async def serve_logo():
        logo = static_dir / "ic_logomark.svg"
        if logo.is_file():
            return FileResponse(logo, media_type="image/svg+xml")
        return HTMLResponse("", status_code=404)

    # SPA catch-all: everything that's not /api/* or /docs gets index.html
    @app.get("/{full_path:path}", include_in_schema=False)
    async def serve_spa(full_path: str):
        # Don't intercept API routes or docs
        if full_path.startswith("api/"):
            return HTMLResponse('{"error": "Not found"}', status_code=404)
        if full_path == "docs" or full_path.startswith("docs/") or full_path == "redoc" or full_path == "openapi.json":
            return HTMLResponse('{"error": "Not found"}', status_code=404)

        # Try to serve exact file from static/
        file_path = static_dir / full_path
        if file_path.is_file():
            return FileResponse(file_path)

        # Fallback: serve index.html for SPA client-side routing
        index_path = static_dir / "index.html"
        if index_path.is_file():
            return FileResponse(index_path)

        return HTMLResponse("LakeFusion UI not built. Run build script with BUILD_UI=true.", status_code=404)

else:
    # No static dir — redirect root to API docs
    @app.get("/", include_in_schema=False)
    async def root_redirect():
        return RedirectResponse(url="/docs")

logger.info("LakeFusion Unified App module loaded")
PYEOF

# ── Step 8: Generate app/config.py ──────────────────────────────────────────
info "Generating app/config.py"
cat > "$OUT/app/config.py" << 'PYEOF'
"""
Shared config for the unified LakeFusion app.
Reads environment variables common to all services.
"""
import os
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv('.env')

# PG* vars (same convention as LakeGraph)
PGHOST = os.environ.get("PGHOST", "")
PGPORT = os.environ.get("PGPORT", "5432")
PGDATABASE = os.environ.get("PGDATABASE", "")
PGUSER = os.environ.get("PGUSER", "")
PGPASSWORD = os.environ.get("PGPASSWORD", "")

# SQL_* vars (for mysql/mssql/postgresql legacy)
sql_username = os.environ.get('SQL_USERNAME', 'root')
sql_password = os.environ.get('SQL_PASSWORD', '')
sql_server = os.environ.get('SQL_SERVER', 'localhost:3306')
sql_db_name = os.environ.get('SQL_DBNAME', 'lakefusion_transactional_db')
deployment_env = os.environ.get('DEPLOYMENT_ENV', 'dev')
db_type = os.environ.get("DB_TYPE", "lakebase").lower()

if deployment_env:
    sql_db_name = f'{sql_db_name}_{deployment_env}'

lakefusion_databricks_dapi = os.environ.get("LAKEFUSION_DATABRICKS_DAPI", "")
run_dbx_pipeline_artifacts_import = os.environ.get("RUN_DBX_PIPELINE_ARTIFACTS_IMPORT", "True").lower() == "true"

class AppConfig(object):
    if PGHOST and PGDATABASE:
        # PG* vars present → local PostgreSQL (takes priority)
        encoded_password = quote_plus(PGPASSWORD) if PGPASSWORD else ""
        DATABASE_URL = (
            f"postgresql+psycopg2://{PGUSER}:{encoded_password}@{PGHOST}:{PGPORT}/{PGDATABASE}"
        )
    elif db_type == "mysql":
        encoded_password = quote_plus(sql_password)
        DATABASE_URL = (
            f"mysql+pymysql://{sql_username}:{encoded_password}@{sql_server}/{sql_db_name}"
        )
    elif db_type == "mssql":
        encoded_password = quote_plus(sql_password)
        DATABASE_URL = (
            f"mssql+pyodbc://{sql_username}:{encoded_password}@{sql_server}/{sql_db_name}"
            "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
        )
    elif db_type == "postgresql":
        encoded_password = quote_plus(sql_password)
        DATABASE_URL = (
            f"postgresql+psycopg2://{sql_username}:{encoded_password}@{sql_server}/{sql_db_name}"
        )
    elif db_type == "lakebase":
        DATABASE_URL = ""  # Generated dynamically in database.py via Databricks SDK
    else:
        raise ValueError(f"Unsupported DB_TYPE: {db_type}")
PYEOF

# ── Step 9: Generate merged requirements.txt ────────────────────────────────
info "Generating merged requirements.txt"

export REPO_ROOT OUT WHEEL_BASENAME
python3 << 'MERGE_SCRIPT'
import os, re, sys
from pathlib import Path

repo_root = os.environ.get("REPO_ROOT", ".")
out_dir = os.environ.get("OUT", ".")
wheel_basename = os.environ.get("WHEEL_BASENAME", "")

# All requirements.txt files to merge
req_files = [
    f"{repo_root}/lakefusion-auth-service/requirements.txt",
    f"{repo_root}/lakefusion-middlelayer-service/requirements.txt",
    f"{repo_root}/lakefusion-databricks-service/requirements.txt",
    f"{repo_root}/lakefusion-cron-service/requirements.txt",
    f"{repo_root}/lakefusion-matchmaven-service/requirements.txt",
    f"{repo_root}/lakefusion-mcp-service/requirements.txt",
    f"{repo_root}/lakefusion-utility/requirements.txt",
]

# Parse package==version or package>=version
pkg_re = re.compile(r'^([a-zA-Z0-9_-]+(?:\[[a-zA-Z0-9_,-]+\])?)\s*(==|>=|<=|~=|!=|>|<)?\s*(.*)$')

# Packages to skip — pulled transitively or cause build issues with newer Python
# Only pin packages we directly use and that won't conflict with transitive deps.
# Everything else gets resolved by pip from the pinned packages' dependencies.
KEEP_PACKAGES = {
    # Core framework
    'fastapi', 'starlette', 'uvicorn', 'pydantic',
    # Database
    'sqlalchemy', 'alembic', 'pymysql', 'psycopg2-binary', 'pyodbc',
    # Databricks
    'databricks-sdk', 'databricks-sql-connector', 'databricks-vectorsearch',
    # MCP
    'fastmcp',
    # Data processing
    'pandas', 'numpy', 'openpyxl',
    # Caching & async
    'aioredis', 'fastapi-cache', 'fastapi-cache2', 'apscheduler',
    # Auth & crypto
    'cryptography', 'pycryptodome', 'oauthlib', 'google-auth',
    # HTTP & networking
    'requests', 'httpx',
    # Utilities
    'marshmallow', 'beautifulsoup4', 'python-multipart', 'lz4',
    'sendgrid', 'mlflow-skinny', 'pendulum', 'time-machine',
    'hiredis', 'thrift',
    # Testing
    'pytest', 'pytest-asyncio',
}

packages = {}  # normalized_name -> best line

def normalize(name):
    return re.sub(r'[-_.]+', '-', name.split('[')[0]).lower()

def parse_version(v):
    parts = []
    for p in v.split('.'):
        try:
            parts.append(int(p))
        except ValueError:
            parts.append(p)
    return tuple(parts)

for req_file in req_files:
    if not os.path.exists(req_file):
        continue
    with open(req_file) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or line.startswith('-'):
                continue
            m = pkg_re.match(line)
            if not m:
                continue
            pkg_name, op, version = m.group(1), m.group(2), m.group(3)
            norm = normalize(pkg_name)

            if norm not in KEEP_PACKAGES:
                continue

            if norm not in packages:
                packages[norm] = line
            elif op == '==' and version:
                existing = packages[norm]
                em = pkg_re.match(existing)
                if em and em.group(2) == '==' and em.group(3):
                    if parse_version(version) > parse_version(em.group(3)):
                        packages[norm] = line
                elif em and em.group(2) in ('>=', '>', None):
                    packages[norm] = line

# Sort by normalized name
sorted_pkgs = sorted(packages.items(), key=lambda x: x[0])

out_path = os.path.join(out_dir, "requirements.txt")
with open(out_path, 'w') as f:
    f.write("# LakeFusion Unified App - Merged Requirements\n")
    f.write("# Generated by scripts/build_lakefusion_app.sh\n\n")
    # Include the utility wheel first
    f.write(f"# LakeFusion utility (built from source)\n")
    f.write(f"./wheels/{wheel_basename}\n\n")
    for norm, line in sorted_pkgs:
        f.write(line + '\n')

print(f"Wrote {len(sorted_pkgs)} packages + utility wheel to {out_path}")
MERGE_SCRIPT

# ── Step 10: Generate app.yaml ──────────────────────────────────────────────
info "Generating app.yaml"
cat > "$OUT/app.yaml" << 'YAMLEOF'
name: lakefusion-app

# Allow Authorization header to pass through for API routes
auth:
  unauthenticated_paths:
    - /api/*

command:
  - "uvicorn"
  - "app.main:app"
  - "--host"
  - "0.0.0.0"
  - "--port"
  - "8000"

resources:
  - name: lakefusion-db
    type: database
    database: lakefusion_transactional_db
    permission: CAN_CONNECT_AND_CREATE

env:
  - name: SERVICE_NAME
    value: LakeFusionApp
  - name: DB_TYPE
    value: "lakebase"
  - name: DEPLOYMENT_ENV
    value: "production"
  - name: DATABRICKS_DATABASE_INSTANCE
    value: lakefusion-db
  - name: DATABRICKS_DATABASE_NAME
    value: lakefusion_transactional_db
  - name: DATABRICKS_DATABASE_PORT
    value: "5432"
  - name: PGPORT
    value: "5432"
  - name: PGDATABASE
    value: lakefusion_transactional_db
  - name: DB_POOL_SIZE
    value: "10"
  - name: DB_MAX_OVERFLOW
    value: "20"
  - name: DB_POOL_TIMEOUT
    value: "60"
  - name: DB_POOL_RECYCLE
    value: "1200"
  - name: RUN_DBX_PIPELINE_ARTIFACTS_IMPORT
    value: "True"
  - name: DATABRICKS_OIDC_CLIENT_ID
    valueFrom: DATABRICKS_OIDC_CLIENT_ID
  - name: DATABRICKS_OIDC_CLIENT_SECRET
    valueFrom: DATABRICKS_OIDC_CLIENT_SECRET
  - name: LAKEFUSION_DATABRICKS_DAPI
    valueFrom: LAKEFUSION_DATABRICKS_DAPI
YAMLEOF

# ── Step 11: Build and assemble UI portals ──────────────────────────────────
info "Assembling UI portals into static/"
mkdir -p "$OUT/static"

PORTALS=(
    "lakefusion-root-portal"
    "lakefusion-main-portal"
    "lakefusion-utility-portal"
    "lakefusion-selfservice-portal"
)

BUILD_UI="${BUILD_UI:-false}"

for portal in "${PORTALS[@]}"; do
    PORTAL_DIR="$REPO_ROOT/$portal"
    DIST_DIR="$PORTAL_DIR/dist"

    if [[ "$BUILD_UI" == "true" ]]; then
        info "  Building $portal"

        # Copy docs/mdx/ to selfservice-portal/src/docs/ (gitignored, needed for build)
        if [[ "$portal" == "lakefusion-selfservice-portal" && -d "$REPO_ROOT/docs/mdx" ]]; then
            info "    Copying docs/mdx/ → $portal/src/docs/"
            rm -rf "$PORTAL_DIR/src/docs"
            mkdir -p "$PORTAL_DIR/src/docs"
            cp -R "$REPO_ROOT/docs/mdx/"* "$PORTAL_DIR/src/docs/"
        fi

        BUILD_LOG=$(mktemp)
        if ! (cd "$PORTAL_DIR" && yarn install --frozen-lockfile && yarn build) > "$BUILD_LOG" 2>&1; then
            err "  Build failed for $portal:"
            cat "$BUILD_LOG"
            rm -f "$BUILD_LOG"
            exit 1
        fi
        tail -3 "$BUILD_LOG"
        rm -f "$BUILD_LOG"
    fi

    if [[ ! -d "$DIST_DIR" ]]; then
        warn "  Skipping $portal (no dist/ — run with BUILD_UI=true or build manually)"
        continue
    fi

    if [[ "$portal" == "lakefusion-root-portal" ]]; then
        # Root portal: copy index.html, root-config.js, and assets to static root
        info "  Copying $portal → static/ (root)"
        cp "$DIST_DIR/index.html" "$OUT/static/"
        cp "$DIST_DIR/lakefusion-root-config.js" "$OUT/static/"
        # Copy logo/icons if present
        cp "$DIST_DIR"/*.svg "$OUT/static/" 2>/dev/null || true
        cp "$DIST_DIR"/*.png "$OUT/static/" 2>/dev/null || true
        cp "$DIST_DIR"/*.ico "$OUT/static/" 2>/dev/null || true
    else
        # Micro frontend: copy all JS files to static/
        info "  Copying $portal → static/"
        cp "$DIST_DIR"/*.js "$OUT/static/" 2>/dev/null || true
        cp "$DIST_DIR"/*.js.map "$OUT/static/" 2>/dev/null || true
    fi
done

# Generate config.js — API URLs are relative (same server)
info "Generating static/config.js"
cat > "$OUT/static/config.js" << 'JSEOF'
window.env = {
  DATABRICKS_SERVICE_URL: "/api/databricks/",
  MIDDLELAYER_SERVICE_URL: "/api/middle-layer/",
  AUTH_SERVICE_URL: "/api/auth/",
  AIML_SERVICE_URL: "/api/match-maven/",
  CRON_SERVICE_URL: "/api/cron/",
  DATABRICKS_HOST: "",
  AUTH_PROVIDER: "databricks"
};
JSEOF

# Generate importmap.json — all microfrontends served from /static/
info "Generating static/importmap.json"
cat > "$OUT/static/importmap.json" << 'JSEOF'
{
  "imports": {
    "@lakefusion/root-config": "/static/lakefusion-root-config.js",
    "@lakefusion/main-portal": "/static/lakefusion-main.js",
    "@lakefusion/utility-portal": "/static/lakefusion-utility.js",
    "@lakefusion/selfservice": "/static/lakefusion-selfservice.js"
  }
}
JSEOF

# Copy version.json to static/ as well (root-config fetches it)
cp "$REPO_ROOT/version.json" "$OUT/static/version.json"

info "UI assembly complete — $(ls "$OUT/static/"/*.js 2>/dev/null | wc -l | tr -d ' ') JS files in static/"

# ── Step 12: Copy build script itself for reference ─────────────────────────
info "Copying build script to output"
mkdir -p "$OUT/scripts"
cp "$REPO_ROOT/scripts/build_lakefusion_app.sh" "$OUT/scripts/build.sh"

# ── Done ────────────────────────────────────────────────────────────────────
echo ""
info "Build complete! Output at: $OUT"
echo ""
echo "  Structure:"
find "$OUT" -maxdepth 3 -type f | sort | head -30
echo "  ..."
echo ""
echo "  Next steps:"
echo "    1. cd lakefusion-app"
echo "    2. pip install -r requirements.txt"
echo "    3. python -c \"from app.main import app; print('OK:', len(app.routes), 'routes')\""
echo "    4. uvicorn app.main:app --port 8000"
echo "    5. databricks apps deploy lakefusion-app --source-code-path /Workspace/Users/.../lakefusion-app"
