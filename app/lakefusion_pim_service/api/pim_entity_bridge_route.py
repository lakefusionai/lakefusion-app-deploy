# pyrefly: ignore [missing-import]
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.lakefusion_pim_service.utils.app_db import (
    get_db, token_required_wrapper, derive_db_name,
    get_or_create_engine, dispose_entity_engine,
)
from app.lakefusion_pim_service.services.pim_entity_bridge_service import PimEntityBridgeService
from app.lakefusion_pim_service.utils import pim_sql
# from app.lakefusion_pim_service.config import deployment_env  # No longer needed — DB names don't use env suffix
from lakefusion_utility.utils.logging_utils import get_logger

app_logger = get_logger(__name__)

pim_entity_bridge_router = APIRouter(tags=["PIM Entity Bridge"])


# ---------------------------------------------------------------------------
# List all product entities with initialization status
# ---------------------------------------------------------------------------
@pim_entity_bridge_router.get("/entities")
def list_product_entities(
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db),
):
    """List all product entities with their PIM initialization status.

    Used by the EntitySelector dropdown in the PIM portal header.
    """
    from lakefusion_utility.models.entity import Entity
    entities = db.query(Entity).filter(
        Entity.entity_type == "product", Entity.is_active == True
    ).all()

    results = []
    for entity in entities:
        entity_name = derive_db_name(entity.name)
        initialized = False

        # Always verify with a real query — engine may be cached but DB deleted
        try:
            from sqlalchemy import text
            session_factory = get_or_create_engine(entity_name, db)
            test_session = session_factory()
            test_session.execute(text(f"SELECT 1 FROM {pim_sql.pim_tbl(entity_name, 'pim_entity_tier')} LIMIT 1"))
            test_session.close()
            initialized = True
        except Exception:
            dispose_entity_engine(entity_name)
            initialized = False

        results.append({
            "id": entity.id,
            "name": entity.name,
            "entity_name": entity_name,
            "entity_type": entity.entity_type,
            "entity_subtype": entity.entity_subtype,
            "description": entity.description,
            "is_active": entity.is_active,
            "is_initialized": initialized,
            "lakebase_db_name": derive_db_name(entity.name),
        })
    return results


# ---------------------------------------------------------------------------
# Legacy: Get active product entity (backward compat for PIM portal)
# ---------------------------------------------------------------------------
@pim_entity_bridge_router.get("/entity-bridge/active")
def get_active_product_entity(
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db),
):
    """Get the active product entity and its initialization status.

    LEGACY: The PIM portal's entityContext.ts calls this on mount.
    With multi-entity support, this returns the first initialized entity.
    Will be replaced by GET /entities + URL-driven entity selection.
    """
    service = PimEntityBridgeService(db)
    entity = service.get_active_product_entity()
    if not entity:
        return {"entity": None, "is_initialized": False}

    entity_name = derive_db_name(entity["name"])
    initialized = False

    # Always verify with a real query — engine may be cached but DB deleted
    try:
        from sqlalchemy import text
        session_factory = get_or_create_engine(entity_name, db)
        test_session = session_factory()
        test_session.execute(text(f"SELECT 1 FROM {pim_sql.pim_tbl(entity_name, 'pim_entity_tier')} LIMIT 1"))
        test_session.close()
        initialized = True
    except Exception:
        dispose_entity_engine(entity_name)
        initialized = False

    return {"entity": entity, "is_initialized": initialized, "entity_id": entity["id"]}


@pim_entity_bridge_router.get("/entity-bridge/{entity_id}/hierarchy")
def get_hierarchy_tiers(
    entity_id: int,
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db),
):
    """Get hierarchy tiers for a product entity."""
    service = PimEntityBridgeService(db)
    return service.get_hierarchy_tiers(entity_id)


@pim_entity_bridge_router.get("/entity-bridge/{entity_id}/global-attributes")
def get_global_attributes(
    entity_id: int,
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db),
):
    """Get global attributes from MySQL entityattributes."""
    service = PimEntityBridgeService(db)
    return service.get_global_attributes(entity_id)


@pim_entity_bridge_router.get("/entity-bridge/{entity_id}/tab-groups")
def get_tab_groups(
    entity_id: int,
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db),
):
    """Get predefined tab groups for a product entity (drives the Tab Group dropdown)."""
    service = PimEntityBridgeService(db)
    return service.get_tab_groups(entity_id)


@pim_entity_bridge_router.post("/entity-bridge/{entity_id}/initialize-local")
def initialize_pim_local(
    entity_id: int,
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db),
):
    """LOCAL DEV ONLY — create + seed the PIM tables in local Postgres.

    Local counterpart of the Databricks init pipeline. Refuses unless
    DATA_DB_TYPE=postgresql. Does not create Delta/synced tables or change-log
    triggers (Databricks-only). The Integration Hub calls this automatically on
    PIM task creation when running against local Postgres.
    """
    service = PimEntityBridgeService(db)
    return service.init_local(entity_id)


@pim_entity_bridge_router.post("/entity-bridge/{entity_id}/sync-attribute")
def sync_single_attribute(
    entity_id: int,
    attr_data: dict,
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db),
):
    """Sync a single attribute from MySQL to Lakebase (webhook target)."""
    service = PimEntityBridgeService(db)
    result = service.sync_single_attribute(entity_id, attr_data)
    return result


@pim_entity_bridge_router.delete("/entity-bridge/{entity_id}/cleanup")
def cleanup_pim(
    entity_id: int,
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db),
):
    """
    Drop the PIM database and clear engine cache for a product entity.
    Called when a product entity is deleted from the MDM portal.
    """
    service = PimEntityBridgeService(db)
    # Dispose engine from pool before dropping DB
    from lakefusion_utility.models.entity import Entity
    entity = db.query(Entity).filter_by(id=entity_id).first()
    if entity:
        dispose_entity_engine(derive_db_name(entity.name))
    result = service.cleanup_pim(entity_id)
    return result
