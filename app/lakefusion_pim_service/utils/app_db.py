import asyncio
import os
import re
import threading
import uuid
from contextlib import contextmanager
from typing import Dict, Optional
from urllib.parse import quote_plus

import jwt
import lakefusion_utility.utils.database as db_module
from lakefusion_utility.utils.databricks_util import _create_workspace_client
from fastapi import Depends, HTTPException, Request
from sqlalchemy import URL, create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.lakefusion_pim_service.config import data_db_type, data_db_search_path, deployment_env
from lakefusion_utility.utils.logging_utils import get_logger

logger = get_logger(__name__)

# --- Primary DB (MySQL — shared transactional via lakefusion-utility) ---
# get_db() → MySQL: db_config_properties, feature_flags, audit_logs, entities, etc.
@contextmanager
def db_context():
    with db_module.SessionLocal() as session:
        try:
            yield session
        finally:
            session.close()

engine = db_module.engine
get_db = db_module.get_db
token_required_wrapper = db_module.token_required_wrapper


# ===========================================================================
# Data DB — Per-Entity Engine Pool (Postgres/Lakebase)
# ===========================================================================
# Each product entity has its own Lakebase database. The pool maintains
# one engine + sessionmaker per entity, keyed by sanitized entity name.
# get_data_db() reads entity_name from the URL path parameter and returns
# the correct session — zero changes to endpoint functions.

_entity_engines: Dict[str, Engine] = {}
_entity_sessions: Dict[str, sessionmaker] = {}
_entity_passwords: Dict[str, str] = {}          # Lakebase JWT per entity
_entity_refresh_tasks: Dict[str, asyncio.Task] = {}
_lock = threading.Lock()

# Shared Lakebase infrastructure (created once, reused across entities)
_workspace_client = None
_db_instance = None


# --- Backward-compat aliases (point to last-used entity) ---
# Some code references these directly (e.g., entity_bridge_service create_all).
# Prefer using the pool via get_data_db() or get_engine_for_entity().
data_engine: Optional[Engine] = None
DataSessionLocal: Optional[sessionmaker] = None


def _get_workspace_client():
    """Get or create the shared Databricks WorkspaceClient (singleton)."""
    global _workspace_client, _db_instance
    if _workspace_client is None:
        databricks_dapi = os.getenv("LAKEFUSION_DATABRICKS_DAPI")
        _workspace_client = _create_workspace_client(databricks_dapi or None)
        databricks_db_instance = os.getenv("DATA_DATABRICKS_DATABASE_INSTANCE", "lakefusion-db")
        _db_instance = _workspace_client.database.get_database_instance(name=databricks_db_instance)
    return _workspace_client, _db_instance


def derive_db_name(entity_name: str, dep_env: str = "") -> str:
    """Derive the entity-specific database name from entity name.

    Sanitizes to lowercase alphanumeric + underscore.
    Note: deployment_env suffix is NOT appended — each entity gets a clean
    database name. Multi-environment isolation is handled at the Lakebase
    instance level, not by naming convention.
    """
    return re.sub(r'[^a-z0-9]', '_', entity_name.lower()).strip('_')


def init_engine_for_entity(entity_name: str, db_name: str) -> sessionmaker:
    """Initialize a data DB engine for a specific entity and store it in the pool.

    Called by:
    - initialize_pim() after creating the DB
    - get_or_create_engine() for lazy auto-reconnect
    """
    global data_engine, DataSessionLocal

    databricks_db_port = int(os.getenv("DATABRICKS_DATABASE_PORT", "5432"))

    if data_db_type == "lakebase":
        ws, instance = _get_workspace_client()

        cred = ws.database.generate_database_credential(
            request_id=str(uuid.uuid4()), instance_names=[instance.name]
        )
        _entity_passwords[entity_name] = cred.token
        logger.info(f"Data DB (Lakebase): Credentials generated for entity '{entity_name}'")

        decoded = jwt.decode(cred.token, options={"verify_signature": False})
        username = decoded.get("sub", "")

        url = URL.create(
            drivername="postgresql+psycopg2",
            username=username,
            password="",
            host=instance.read_write_dns,
            port=databricks_db_port,
            database=db_name,
        )

        exec_options = {}
        if data_db_search_path and data_db_search_path != "public":
            target_schema = data_db_search_path.split(",")[0].strip()
            exec_options["schema_translate_map"] = {None: target_schema}

        eng = create_engine(
            url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
            pool_reset_on_return="commit",
            pool_use_lifo=True,
            execution_options=exec_options,
            connect_args={
                "application_name": f"pim_{entity_name}",
                "options": f"-c search_path={data_db_search_path}",
                "sslmode": "require",
            },
        )

        # Token injection on each connection — capture entity_name for closure
        ename = entity_name

        @event.listens_for(eng, "do_connect")
        def provide_token(dialect, conn_rec, cargs, cparams):
            cparams["password"] = _entity_passwords.get(ename, "")

        logger.info(f"Data DB engine initialized (Lakebase: {db_name}) for entity '{entity_name}'")

        # Start per-entity token refresh
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_start_entity_token_refresh(entity_name))
        except RuntimeError:
            pass  # No event loop — will refresh on next connection attempt

    else:
        # Local PostgreSQL
        from app.lakefusion_pim_service.config import data_sql_username, data_sql_password, data_sql_server
        db_url = (
            f"postgresql+psycopg2://{data_sql_username}:{quote_plus(data_sql_password)}"
            f"@{data_sql_server}/{db_name}"
            f"?options=-csearch_path%3D{data_db_search_path}"
        )
        eng = create_engine(
            db_url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
        )
        logger.info(f"Data DB engine initialized (PostgreSQL: {db_name}) for entity '{entity_name}'")

    session_factory = sessionmaker(autocommit=False, autoflush=False, bind=eng)

    with _lock:
        _entity_engines[entity_name] = eng
        _entity_sessions[entity_name] = session_factory

    # Update backward-compat aliases to last-used entity
    data_engine = eng
    DataSessionLocal = session_factory

    return session_factory


def get_engine_for_entity(entity_name: str) -> Optional[Engine]:
    """Get the engine for a specific entity from the pool, or None."""
    return _entity_engines.get(entity_name)


def is_entity_initialized(entity_name: str) -> bool:
    """Check if an entity has an active engine in the pool."""
    return entity_name in _entity_sessions


def get_or_create_engine(entity_name: str, mysql_db: Session) -> sessionmaker:
    """Get existing engine for entity or create one by looking up the entity in MySQL.

    Used by get_data_db() for lazy auto-reconnect.
    """
    if entity_name in _entity_sessions:
        # Verify connection is alive
        try:
            test_db = _entity_sessions[entity_name]()
            test_db.execute(text("SELECT 1"))
            test_db.close()
            return _entity_sessions[entity_name]
        except Exception:
            # Stale connection — dispose and recreate
            logger.warning(f"Stale connection for entity '{entity_name}', reconnecting...")
            dispose_entity_engine(entity_name)

    # Look up entity from MySQL — entity_name is sanitized (lowercase, underscores)
    # so we need to match against all product entities and compare sanitized names
    from lakefusion_utility.models.entity import Entity
    all_product_entities = mysql_db.query(Entity).filter(
        Entity.entity_type == "product", Entity.is_active == True
    ).all()
    entity = None
    for e in all_product_entities:
        if derive_db_name(e.name) == entity_name:
            entity = e
            break
    if not entity:
        raise HTTPException(status_code=404, detail=f"Product entity '{entity_name}' not found.")

    db_name = derive_db_name(entity.name, deployment_env)

    if data_db_type == "lakebase":
        try:
            return init_engine_for_entity(entity_name, db_name)
        except Exception as e:
            logger.warning(f"Failed to connect to Lakebase DB '{db_name}': {e}")
            raise HTTPException(
                status_code=503,
                detail=f"PIM not initialized for entity '{entity_name}'. Initialize PIM in the Integration Hub."
            )
    else:
        # For local PostgreSQL, check if DB exists
        from app.lakefusion_pim_service.config import data_sql_username, data_sql_password, data_sql_server
        admin_url = f"postgresql+psycopg2://{data_sql_username}:{quote_plus(data_sql_password)}@{data_sql_server}/postgres"
        admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
        try:
            with admin_engine.connect() as conn:
                exists = conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")).fetchone()
            if not exists:
                raise HTTPException(
                    status_code=503,
                    detail=f"PIM not initialized for entity '{entity_name}'. Initialize PIM in the Integration Hub."
                )
        finally:
            admin_engine.dispose()

        return init_engine_for_entity(entity_name, db_name)


def dispose_entity_engine(entity_name: str):
    """Dispose an entity's engine and remove it from the pool."""
    with _lock:
        eng = _entity_engines.pop(entity_name, None)
        _entity_sessions.pop(entity_name, None)
        _entity_passwords.pop(entity_name, None)
    if eng:
        try:
            eng.dispose()
        except Exception:
            pass
    # Cancel token refresh task
    task = _entity_refresh_tasks.pop(entity_name, None)
    if task and not task.done():
        task.cancel()
    logger.info(f"Engine disposed for entity '{entity_name}'")


def get_data_db(request: Request, mysql_db: Session = Depends(get_db)):
    """Dependency that yields a Data DB session for a specific product entity.

    Reads entity_name from the URL path parameter (injected by FastAPI from
    the router prefix /{entity_name}/). Lazily creates the engine if needed.

    Zero changes required to endpoint functions — they still use:
        db: Session = Depends(get_data_db)
    """
    entity_name = request.path_params.get("entity_name")
    if not entity_name:
        raise HTTPException(
            status_code=400,
            detail="entity_name path parameter required. Use /api/pim/{entity_name}/... endpoints."
        )

    session_factory = get_or_create_engine(entity_name, mysql_db)
    db = session_factory()
    try:
        yield db
    finally:
        db.close()


# ===========================================================================
# Token Refresh (Lakebase only — per entity)
# ===========================================================================

async def _refresh_entity_token(entity_name: str):
    """Background task to refresh Lakebase OAuth token for a specific entity."""
    while True:
        try:
            await asyncio.sleep(50 * 60)  # Refresh every 50 minutes (tokens expire ~60 min)
            ws, instance = _get_workspace_client()
            cred = ws.database.generate_database_credential(
                request_id=str(uuid.uuid4()), instance_names=[instance.name]
            )
            _entity_passwords[entity_name] = cred.token
            logger.info(f"Data DB: Token refreshed for entity '{entity_name}'")
        except Exception as e:
            logger.error(f"Data DB: Token refresh failed for entity '{entity_name}': {e}")


async def _start_entity_token_refresh(entity_name: str):
    """Start token refresh for a specific entity if not already running."""
    if data_db_type != "lakebase":
        return
    if entity_name not in _entity_refresh_tasks or _entity_refresh_tasks[entity_name].done():
        _entity_refresh_tasks[entity_name] = asyncio.create_task(_refresh_entity_token(entity_name))
        logger.info(f"Data DB: Token refresh started for entity '{entity_name}'")


async def stop_all_token_refresh():
    """Stop all entity token refresh tasks (called on shutdown)."""
    for entity_name, task in list(_entity_refresh_tasks.items()):
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
    _entity_refresh_tasks.clear()
    logger.info("Data DB: All token refresh tasks stopped")


# Backward compat alias
stop_data_token_refresh = stop_all_token_refresh


# ===========================================================================
# Legacy init_data_engine() — kept for backward compat during migration
# ===========================================================================

def init_data_engine():
    """Legacy: Initialize the Data DB engine from env vars.

    DEPRECATED: Use init_engine_for_entity() instead.
    Kept for backward compat — reads DATABRICKS_DATABASE_NAME or DATA_SQL_DBNAME
    from env vars and initializes a single engine.
    """
    from app.lakefusion_pim_service.config import DATA_DATABASE_URL

    if not DATA_DATABASE_URL and data_db_type != "lakebase":
        logger.info("Data DB: No DATA_SQL_DBNAME configured — skipping engine init (lazy mode)")
        return

    if data_db_type == "lakebase":
        db_name = os.getenv("DATABRICKS_DATABASE_NAME", "lakefusion_pim_db")
        if deployment_env:
            db_name = f"{db_name}_{deployment_env}"
        entity_name = db_name.replace(f"_{deployment_env}", "") if deployment_env else db_name
        init_engine_for_entity(entity_name, db_name)
    else:
        from app.lakefusion_pim_service.config import data_sql_db_name
        entity_name = data_sql_db_name.replace(f"_{deployment_env}", "") if deployment_env else data_sql_db_name
        if entity_name:
            init_engine_for_entity(entity_name, data_sql_db_name)
