from sqlalchemy.orm import Session
from fastapi import HTTPException
from lakefusion_utility.models.entity import Entity, EntityAttributes
from lakefusion_utility.utils.logging_utils import get_logger
# Single source of truth for PIM seed constants — shared with the pipeline
# Seed_PIM_Reference_Data notebook (SCRUM-1929). Do not redefine here.
from lakefusion_utility.models.pim_constants import (
    TIER_TEMPLATES,
    TYPE_MAPPING,
    DEFAULT_LANGS,
    DEFAULT_UNITS,
    DEFAULT_TAB_GROUPS,
)

app_logger = get_logger(__name__)


class PimEntityBridgeService:
    """Bridge between MySQL entity config and PIM Lakebase data."""

    def __init__(self, mysql_db: Session):
        self.mysql_db = mysql_db

    def get_active_product_entity(self):
        """Return the first active product entity. MVP: single entity."""
        entity = (
            self.mysql_db.query(Entity)
            .filter(Entity.entity_type == "product", Entity.is_active == True)
            .first()
        )
        if not entity:
            return None
        return self._entity_to_dict(entity)

    def get_product_entity(self, entity_id: int):
        """Get a specific product entity by ID."""
        entity = (
            self.mysql_db.query(Entity)
            .filter(Entity.id == entity_id, Entity.entity_type == "product")
            .first()
        )
        if not entity:
            raise HTTPException(status_code=404, detail="Product entity not found.")
        return self._entity_to_dict(entity)

    def get_hierarchy_tiers(self, entity_id: int):
        """Get hierarchy tiers derived from entity_subtype.

        Supports custom tier labels encoded as: "three-tiered|Product,Variant,Item"
        If no custom labels, uses defaults from TIER_TEMPLATES.
        """
        entity = self._get_entity(entity_id)
        raw_subtype = entity.entity_subtype or "two-tiered"

        # Parse custom labels: "three-tiered|Product,Variant,Item"
        custom_labels = None
        if "|" in raw_subtype:
            subtype, labels_str = raw_subtype.split("|", 1)
            custom_labels = [l.strip() for l in labels_str.split(",") if l.strip()]
        else:
            subtype = raw_subtype

        tiers = TIER_TEMPLATES.get(subtype, TIER_TEMPLATES["two-tiered"])
        # Deep copy to avoid mutating the template
        tiers = [dict(t) for t in tiers]

        # Apply custom labels if provided and count matches
        if custom_labels and len(custom_labels) == len(tiers):
            for i, tier in enumerate(tiers):
                tier["label"] = custom_labels[i]
                # Derive code from custom label: "Brand Family" -> "BRAND_FAMILY"
                tier["code"] = custom_labels[i].upper().replace(" ", "_").replace("-", "_")

        return tiers

    def get_global_attributes(self, entity_id: int):
        """Get entity attributes from MySQL (global attribute definitions)."""
        self._get_entity(entity_id)  # validate entity exists
        attrs = (
            self.mysql_db.query(EntityAttributes)
            .filter(EntityAttributes.entity_id == entity_id, EntityAttributes.is_active == True)
            .all()
        )
        return [
            {
                "source_attr_id": attr.id,
                "name": attr.name,
                "label": attr.label,
                "type": attr.type,
                "type_config": attr.type_config,
                "pim_data_type": TYPE_MAPPING.get(attr.type, "TEXT"),
                "is_primary_key": attr.is_primary_key,
                "is_label": attr.is_label,
                "show_in_ui": attr.show_in_ui,
            }
            for attr in attrs
        ]

    def get_lakebase_db_name(self, entity_id: int, deployment_env: str = ""):
        """Derive the entity-specific Lakebase database name.

        Note: deployment_env suffix is NOT appended — each entity gets a clean
        database name. Multi-environment isolation is handled at the Lakebase
        instance level.
        """
        import re
        entity = self._get_entity(entity_id)
        return re.sub(r'[^a-z0-9]', '_', entity.name.lower()).strip('_')

    def sync_single_attribute(self, entity_id: int, attr_data: dict):
        """Sync a single attribute from MySQL to Lakebase pim_attribute_definition."""
        from app.lakefusion_pim_service.services.pim_attribute_sync_service import PimAttributeSyncService
        from app.lakefusion_pim_service.utils.app_db import derive_db_name, get_or_create_engine

        entity = self._get_entity(entity_id)
        entity_name = derive_db_name(entity.name)

        # get_or_create_engine handles auto-reconnect with lazy initialization
        try:
            session_factory = get_or_create_engine(entity_name, self.mysql_db)
        except HTTPException:
            raise
        except Exception as e:
            app_logger.warning(f"Auto-reconnect for sync failed: {e}")
            raise HTTPException(status_code=503, detail="PIM not initialized. Run Initialize PIM first.")

        data_db = session_factory()
        try:
            sync_service = PimAttributeSyncService(data_db, entity_name)
            result = sync_service.sync_attribute(attr_data)
            data_db.commit()
            return result
        except Exception:
            data_db.rollback()
            raise
        finally:
            data_db.close()

    def get_tab_groups(self, entity_id: int):
        """List predefined tab groups for a product entity, ordered by display_order."""
        from app.lakefusion_pim_service.utils.app_db import derive_db_name, get_or_create_engine
        from lakefusion_utility.models.pim import PimTabGroup

        entity = self._get_entity(entity_id)
        entity_name = derive_db_name(entity.name)

        try:
            session_factory = get_or_create_engine(entity_name, self.mysql_db)
        except HTTPException:
            raise
        except Exception as e:
            app_logger.warning(f"Auto-reconnect for tab-groups failed: {e}")
            raise HTTPException(status_code=503, detail="PIM not initialized. Run Initialize PIM first.")

        data_db = session_factory()
        try:
            rows = data_db.query(PimTabGroup).order_by(PimTabGroup.display_order).all()
            return [
                {
                    "name": r.name,
                    "label": r.label,
                    "display_order": r.display_order,
                    "is_system": r.is_system,
                }
                for r in rows
            ]
        finally:
            data_db.close()

    def cleanup_pim(self, entity_id: int):
        """Drop the PIM database and clear engine cache for a deleted product entity."""
        from app.lakefusion_pim_service.config import deployment_env, data_db_type
        from app.lakefusion_pim_service.utils.app_db import derive_db_name, dispose_entity_engine

        # Derive DB name — entity may already be deleted from MySQL, so try both ways
        try:
            entity = self._get_entity(entity_id)
            entity_name = derive_db_name(entity.name)
            db_name = derive_db_name(entity.name)
        except Exception:
            app_logger.warning(f"Cannot derive DB name for deleted entity {entity_id}, skipping DB drop")
            return {"entity_id": entity_id, "status": "cache_cleared", "db_dropped": False}

        # Dispose the engine from the pool
        dispose_entity_engine(entity_name)

        # Drop the database
        db_dropped = False
        try:
            if data_db_type == "postgresql":
                from app.lakefusion_pim_service.config import data_sql_username, data_sql_password, data_sql_server
                from sqlalchemy import create_engine, text
                from urllib.parse import quote_plus
                admin_url = f"postgresql+psycopg2://{data_sql_username}:{quote_plus(data_sql_password)}@{data_sql_server}/postgres"
                admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
                with admin_engine.connect() as conn:
                    # Terminate active connections
                    conn.execute(text(
                        f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                        f"WHERE datname = '{db_name}' AND pid <> pg_backend_pid()"
                    ))
                    # Drop DB
                    conn.execute(text(f'DROP DATABASE IF EXISTS "{db_name}"'))
                admin_engine.dispose()
                db_dropped = True
                app_logger.info(f"Dropped database: {db_name}")
            elif data_db_type == "lakebase":
                self._drop_lakebase_db(db_name)
                db_dropped = True
        except Exception as e:
            app_logger.error(f"Failed to drop database {db_name}: {e}")

        result = {
            "entity_id": entity_id,
            "db_name": db_name,
            "db_dropped": db_dropped,
            "cache_cleared": True,
            "status": "cleaned_up",
        }
        app_logger.info(f"PIM cleanup: {result}")
        return result

    def _drop_lakebase_db(self, db_name: str):
        """Drop a Lakebase database."""
        import os, uuid, jwt
        from sqlalchemy import create_engine, text
        from sqlalchemy.engine import URL
        from lakefusion_utility.utils.databricks_util import _create_workspace_client

        databricks_dapi = os.getenv("LAKEFUSION_DATABRICKS_DAPI")
        databricks_db_instance = os.getenv("DATA_DATABRICKS_DATABASE_INSTANCE", "lakefusion-db")
        databricks_db_port = int(os.getenv("DATABRICKS_DATABASE_PORT", "5432"))

        ws = _create_workspace_client(databricks_dapi or None)
        instance = ws.database.get_database_instance(name=databricks_db_instance)
        cred = ws.database.generate_database_credential(
            request_id=str(uuid.uuid4()), instance_names=[instance.name]
        )
        decoded = jwt.decode(cred.token, options={"verify_signature": False})
        username = decoded.get("sub", "")

        admin_url = URL.create(
            drivername="postgresql+psycopg2", username=username, password="",
            host=instance.read_write_dns, port=databricks_db_port, database="postgres",
        )
        admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT",
            connect_args={"sslmode": "require", "password": cred.token})
        with admin_engine.connect() as conn:
            conn.execute(text(
                f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                f"WHERE datname = '{db_name}' AND pid <> pg_backend_pid()"
            ))
            conn.execute(text(f'DROP DATABASE IF EXISTS "{db_name}"'))
        admin_engine.dispose()
        app_logger.info(f"Dropped Lakebase database: {db_name}")

    # ------------------------------------------------------------------
    # LOCAL DEV INITIALIZATION (postgresql only — Lakebase init is the pipeline)
    # ------------------------------------------------------------------
    def init_local(self, entity_id: int):
        """Create + seed the PIM tables in a LOCAL Postgres DB for dev/testing.

        Local counterpart of the Databricks pipeline (Create_PIM_Tables +
        Seed_PIM_Reference_Data). Creates the per-entity DB, the ``gold`` schema,
        and the pim_* tables (schema from the ORM models — same source the
        pipeline introspects), then seeds tiers/languages/units via raw SQL into
        the same ``gold."pim_*"`` tables the service reads.

        Hard-guarded to DATA_DB_TYPE=postgresql. Does NOT create Delta tables,
        synced tables, or change-log triggers — those are Databricks-only and not
        reproducible locally (see the local-dev note). Idempotent.
        """
        from app.lakefusion_pim_service.config import data_db_type
        if data_db_type != "postgresql":
            raise HTTPException(
                status_code=400,
                detail=(
                    f"init_local is for DATA_DB_TYPE=postgresql only (current: "
                    f"'{data_db_type}'). On Lakebase, initialization is the "
                    f"Databricks pipeline submitted at task creation."
                ),
            )

        from urllib.parse import quote_plus
        from sqlalchemy import create_engine, text as _text
        from sqlalchemy.schema import CreateSchema
        from sqlalchemy.orm import sessionmaker
        from app.lakefusion_pim_service.config import data_sql_username, data_sql_password, data_sql_server
        from app.lakefusion_pim_service.utils.app_db import derive_db_name
        from app.lakefusion_pim_service.utils import pim_sql
        from lakefusion_utility.utils.app_db import Base
        import lakefusion_utility.models.pim  # noqa: F401 — register pim_* on Base.metadata

        entity = self._get_entity(entity_id)
        db_name = derive_db_name(entity.name)
        schema = pim_sql.PIM_SCHEMA  # "gold" in both envs

        # 1. Create the per-entity database if missing.
        admin_url = (
            f"postgresql+psycopg2://{data_sql_username}:{quote_plus(data_sql_password)}"
            f"@{data_sql_server}/postgres"
        )
        admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
        try:
            with admin_engine.connect() as conn:
                exists = conn.execute(
                    _text("SELECT 1 FROM pg_database WHERE datname = :n"), {"n": db_name}
                ).fetchone()
                if not exists:
                    conn.execute(_text(f'CREATE DATABASE "{db_name}"'))
                    app_logger.info(f"[local] Created database {db_name}")
        finally:
            admin_engine.dispose()

        # 2. Create the `gold` schema + pim_* tables in it (schema from ORM models).
        base_url = (
            f"postgresql+psycopg2://{data_sql_username}:{quote_plus(data_sql_password)}"
            f"@{data_sql_server}/{db_name}"
        )
        eng = create_engine(base_url)
        try:
            with eng.connect() as conn:
                conn.execute(CreateSchema(schema, if_not_exists=True))
                conn.commit()
        finally:
            eng.dispose()

        gold_eng = create_engine(base_url, connect_args={"options": f"-csearch_path={schema}"})
        pim_tables = [t for t in Base.metadata.tables.values() if t.name.startswith("pim_")]
        try:
            Base.metadata.create_all(bind=gold_eng, tables=pim_tables)
            app_logger.info(f"[local] Created {len(pim_tables)} pim_* tables in {db_name}.{schema}")
        finally:
            gold_eng.dispose()

        # 3. Seed tiers/languages/units via raw SQL into gold."pim_*" (idempotent).
        session_factory = sessionmaker(
            bind=create_engine(base_url, connect_args={"options": f"-csearch_path={schema}"})
        )
        data_db = session_factory()
        try:
            self._seed_local(data_db, entity, db_name)
            data_db.commit()
        except Exception:
            data_db.rollback()
            raise
        finally:
            data_db.close()
            data_db.bind.dispose()

        return {
            "entity_id": entity_id,
            "entity_name": entity.name,
            "db_name": db_name,
            "schema": schema,
            "tables_created": len(pim_tables),
            "mode": "local_postgresql",
            "status": "initialized",
        }

    def _seed_local(self, data_db, entity, entity_name):
        """Seed tiers (from entity_subtype) + default languages/units/tab groups +
        global attributes via raw SQL.

        Mirrors Seed_PIM_Reference_Data for the local path. Uses pim_sql so the
        rows land in the same gold."pim_*" tables the service reads. Skips a table
        if it already has rows (idempotent re-init)."""
        from app.lakefusion_pim_service.utils import pim_sql

        # --- tiers ---
        tier_tbl = pim_sql.pim_tbl(entity_name, "pim_entity_tier")
        existing = pim_sql.fetch_scalar(data_db, f'SELECT COUNT(*) FROM {tier_tbl}') or 0
        if existing == 0:
            tiers = self.get_hierarchy_tiers(entity.id)
            parent_id = None
            for tier in tiers:
                sql, params = pim_sql.build_insert(
                    entity_name, "pim_entity_tier",
                    {"code": tier["code"], "label": tier["label"], "level": tier["level"],
                     "parent_tier_id": parent_id, "is_leaf": tier["is_leaf"]},
                    auto_updated_at=True,
                )
                pim_sql.execute(data_db, sql, params)
                parent_id = params["id"]
            app_logger.info(f"[local] Seeded {len(tiers)} hierarchy tiers")

        # --- languages (caller PK = id, no updated_at column) ---
        lang_tbl = pim_sql.pim_tbl(entity_name, "pim_language_ref")
        if (pim_sql.fetch_scalar(data_db, f'SELECT COUNT(*) FROM {lang_tbl}') or 0) == 0:
            for lang in DEFAULT_LANGS:
                sql, params = pim_sql.build_insert(
                    entity_name, "pim_language_ref",
                    # is_active is NOT NULL with no DB-side default — set explicitly
                    # (raw SQL doesn't apply the ORM model default).
                    {"id": lang["id"], "language_name": lang["language_name"], "is_active": True},
                    auto_id=False, auto_updated_at=False,
                )
                pim_sql.execute(data_db, sql, params)
            app_logger.info(f"[local] Seeded {len(DEFAULT_LANGS)} default languages")

        # --- units (caller PK = name, no updated_at column) ---
        unit_tbl = pim_sql.pim_tbl(entity_name, "pim_unit_ref")
        if (pim_sql.fetch_scalar(data_db, f'SELECT COUNT(*) FROM {unit_tbl}') or 0) == 0:
            for unit in DEFAULT_UNITS:
                sql, params = pim_sql.build_insert(
                    entity_name, "pim_unit_ref",
                    # is_active is NOT NULL with no DB-side default — set explicitly.
                    {**dict(unit), "is_active": True},
                    auto_id=False, auto_updated_at=False,
                )
                pim_sql.execute(data_db, sql, params)
            app_logger.info(f"[local] Seeded {len(DEFAULT_UNITS)} default measurement units")

        # --- tab groups (caller PK = name, no updated_at column) ---
        tab_tbl = pim_sql.pim_tbl(entity_name, "pim_tab_groups")
        if (pim_sql.fetch_scalar(data_db, f'SELECT COUNT(*) FROM {tab_tbl}') or 0) == 0:
            for order, g in enumerate(DEFAULT_TAB_GROUPS):
                sql, params = pim_sql.build_insert(
                    entity_name, "pim_tab_groups",
                    {"name": g["name"], "label": g["label"], "display_order": order, "is_system": True},
                    auto_id=False, auto_updated_at=False,
                )
                pim_sql.execute(data_db, sql, params)
            app_logger.info(f"[local] Seeded {len(DEFAULT_TAB_GROUPS)} tab groups")

        # --- global attributes (MySQL entityattributes -> pim_attribute_definition) ---
        # Mirrors Seed_PIM_Reference_Data step 4: sync the entity's global attributes
        # (and their SELECT/MULTISELECT options) into the local gold schema.
        attr_tbl = pim_sql.pim_tbl(entity_name, "pim_attribute_definition")
        if (pim_sql.fetch_scalar(data_db, f'SELECT COUNT(*) FROM {attr_tbl}') or 0) == 0:
            from app.lakefusion_pim_service.services.pim_attribute_sync_service import PimAttributeSyncService
            global_attrs = self.get_global_attributes(entity.id)
            sync_service = PimAttributeSyncService(data_db, entity_name)
            for attr in global_attrs:
                sync_service.sync_attribute(attr)
            app_logger.info(f"[local] Synced {len(global_attrs)} global attributes")

    # --- Private helpers ---

    def _get_entity(self, entity_id: int) -> Entity:
        entity = (
            self.mysql_db.query(Entity)
            .filter(Entity.id == entity_id, Entity.entity_type == "product")
            .first()
        )
        if not entity:
            raise HTTPException(status_code=404, detail="Product entity not found.")
        return entity

    def _entity_to_dict(self, entity: Entity) -> dict:
        subtype = entity.entity_subtype or "two-tiered"
        return {
            "id": entity.id,
            "name": entity.name,
            "entity_type": entity.entity_type,
            "entity_subtype": subtype,
            "storage_type": entity.storage_type,
            "description": entity.description,
            "is_active": entity.is_active,
            "hierarchy_tiers": TIER_TEMPLATES.get(subtype, TIER_TEMPLATES["two-tiered"]),
            "lakebase_db_name": self.get_lakebase_db_name(entity.id),
        }

