from sqlalchemy.orm import Session
from fastapi import HTTPException
from lakefusion_utility.models.entity import Entity, EntityAttributes
from lakefusion_utility.utils.logging_utils import get_logger

app_logger = get_logger(__name__)

# Tier templates derived from entity_subtype
TIER_TEMPLATES = {
    "one-tiered": [
        {"code": "PRODUCT", "label": "Product", "level": 0, "is_leaf": True},
    ],
    "two-tiered": [
        {"code": "PRODUCT", "label": "Product", "level": 0, "is_leaf": False},
        {"code": "ITEM", "label": "Item", "level": 1, "is_leaf": True},
    ],
    "three-tiered": [
        {"code": "PRODUCT", "label": "Product", "level": 0, "is_leaf": False},
        {"code": "VARIANT", "label": "Variant", "level": 1, "is_leaf": False},
        {"code": "ITEM", "label": "Item", "level": 2, "is_leaf": True},
    ],
    "four-tiered": [
        {"code": "PRODUCT", "label": "Product", "level": 0, "is_leaf": False},
        {"code": "VARIANT", "label": "Variant", "level": 1, "is_leaf": False},
        {"code": "ITEM", "label": "Item", "level": 2, "is_leaf": False},
        {"code": "PACK", "label": "Pack", "level": 3, "is_leaf": True},
    ],
    "five-tiered": [
        {"code": "LEVEL_0", "label": "Level 0", "level": 0, "is_leaf": False},
        {"code": "LEVEL_1", "label": "Level 1", "level": 1, "is_leaf": False},
        {"code": "LEVEL_2", "label": "Level 2", "level": 2, "is_leaf": False},
        {"code": "LEVEL_3", "label": "Level 3", "level": 3, "is_leaf": False},
        {"code": "ITEM", "label": "Item", "level": 4, "is_leaf": True},
    ],
    "six-tiered": [
        {"code": "LEVEL_0", "label": "Level 0", "level": 0, "is_leaf": False},
        {"code": "LEVEL_1", "label": "Level 1", "level": 1, "is_leaf": False},
        {"code": "LEVEL_2", "label": "Level 2", "level": 2, "is_leaf": False},
        {"code": "LEVEL_3", "label": "Level 3", "level": 3, "is_leaf": False},
        {"code": "LEVEL_4", "label": "Level 4", "level": 4, "is_leaf": False},
        {"code": "ITEM", "label": "Item", "level": 5, "is_leaf": True},
    ],
}

# MySQL attribute type → PIM attribute data_type
TYPE_MAPPING = {
    "STRING": "TEXT",
    "INT": "NUMBER",
    "BIGINT": "NUMBER",
    "FLOAT": "NUMBER",
    "DOUBLE": "NUMBER",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "TIMESTAMP": "DATE",
    "SELECT": "SELECT",
    "MULTISELECT": "MULTISELECT",
    "REFERENCE_ENTITY": "REFERENCE",
}


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

    def initialize_pim(self, entity_id: int):
        """
        Initialize PIM for a product entity:
        1. Derive DB name
        2. Create entity-specific database
        3. Initialize engine in pool
        4. Create pim_* tables
        5. Seed hierarchy tiers
        6. Bulk sync global attributes

        Returns initialization result.
        """
        from app.lakefusion_pim_service.config import deployment_env, data_db_type
        from app.lakefusion_pim_service.utils.app_db import init_engine_for_entity, derive_db_name, get_engine_for_entity

        entity = self._get_entity(entity_id)
        entity_name = derive_db_name(entity.name)
        db_name = derive_db_name(entity.name)

        app_logger.info(f"Initializing PIM for entity '{entity.name}' (id={entity_id}), DB: {db_name}")

        # Step 1: Create DB if it doesn't exist
        if data_db_type == "postgresql":
            self._create_local_db(db_name)
        elif data_db_type == "lakebase":
            self._create_lakebase_db(db_name)

        # Step 2: Initialize engine in pool (no importlib.reload needed)
        init_engine_for_entity(entity_name, db_name)
        data_engine = get_engine_for_entity(entity_name)

        # Step 3: Create pim_* tables
        from lakefusion_utility.utils.app_db import Base
        import lakefusion_utility.models.pim  # noqa: ensure models loaded
        pim_tables = [t for t in Base.metadata.tables.values() if t.name.startswith("pim_")]
        Base.metadata.create_all(bind=data_engine, tables=pim_tables)
        app_logger.info(f"Created {len(pim_tables)} pim_* tables in {db_name}")

        # Step 3b: Add new columns to existing tables (create_all won't add columns to existing tables)
        from sqlalchemy import text
        with data_engine.connect() as conn:
            # pim_value_text — translation columns
            for col, col_type in [
                ("translation_model", "VARCHAR(255)"),
                ("translation_source_locale", "VARCHAR(10)"),
                ("translated_at", "TIMESTAMP"),
            ]:
                try:
                    conn.execute(text(f"ALTER TABLE pim_value_text ADD COLUMN {col} {col_type}"))
                    app_logger.info(f"Added column {col} to pim_value_text")
                except Exception:
                    pass  # Column already exists

            # pim_attribute_definition — group, display_order, is_identifier, is_label
            for col, col_type, default in [
                ("\"group\"", "VARCHAR(100) NOT NULL", "'General'"),
                ("display_order", "INTEGER NOT NULL", "0"),
                ("is_identifier", "BOOLEAN NOT NULL", "FALSE"),
                ("is_label", "BOOLEAN NOT NULL", "FALSE"),
            ]:
                try:
                    conn.execute(text(f"ALTER TABLE pim_attribute_definition ADD COLUMN {col} {col_type} DEFAULT {default}"))
                    app_logger.info(f"Added column {col} to pim_attribute_definition")
                except Exception:
                    pass  # Column already exists

            # Drop sku/name/ean from pim_entity if they exist (old schema)
            for col in ["sku", "name", "ean"]:
                try:
                    conn.execute(text(f"ALTER TABLE pim_entity DROP COLUMN IF EXISTS {col}"))
                except Exception:
                    pass

            # Rename old tables if they exist
            for old_name, new_name in [
                ("pim_taxonomy_attribute_config", "pim_specification_config"),
                ("pim_resolved_attribute_config", "pim_resolved_specification"),
            ]:
                try:
                    conn.execute(text(f"ALTER TABLE {old_name} RENAME TO {new_name}"))
                    app_logger.info(f"Renamed table {old_name} → {new_name}")
                except Exception:
                    pass  # Already renamed or doesn't exist

            conn.commit()

        # Step 4: Seed hierarchy tiers (uses engine from pool via backward-compat aliases)
        import app.lakefusion_pim_service.utils.app_db as app_db_module
        self._seed_hierarchy_tiers(entity_id, app_db_module)

        # Step 5: Bulk sync global attributes
        synced = self._bulk_sync_attributes(entity_id, app_db_module)

        # Step 6: Seed default languages
        self._seed_default_languages(app_db_module)

        # Step 7: Seed default measurement units
        self._seed_default_units(app_db_module)

        result = {
            "entity_id": entity_id,
            "entity_name": entity.name,
            "db_name": db_name,
            "tables_created": len(pim_tables),
            "tiers_seeded": len(self.get_hierarchy_tiers(entity_id)),
            "attributes_synced": synced,
            "status": "initialized",
        }
        app_logger.info(f"PIM initialized: {result}")
        return result

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
            sync_service = PimAttributeSyncService(data_db)
            result = sync_service.sync_attribute(attr_data)
            data_db.commit()
            return result
        except Exception:
            data_db.rollback()
            raise
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

    def _create_local_db(self, db_name: str):
        """Create local PostgreSQL database if it doesn't exist."""
        from app.lakefusion_pim_service.config import data_sql_username, data_sql_password, data_sql_server
        from sqlalchemy import create_engine, text
        from urllib.parse import quote_plus

        admin_url = f"postgresql+psycopg2://{data_sql_username}:{quote_plus(data_sql_password)}@{data_sql_server}/postgres"
        admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
        with admin_engine.connect() as conn:
            exists = conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")).fetchone()
            if not exists:
                conn.execute(text(f"CREATE DATABASE {db_name}"))
                app_logger.info(f"Created local database: {db_name}")
            else:
                app_logger.info(f"Database already exists: {db_name}")
        admin_engine.dispose()

    def _create_lakebase_db(self, db_name: str):
        """Create a database on Lakebase if it doesn't exist."""
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

        # Connect to the default 'postgres' database to run CREATE DATABASE
        admin_url = URL.create(
            drivername="postgresql+psycopg2",
            username=username,
            password="",
            host=instance.read_write_dns,
            port=databricks_db_port,
            database="postgres",
        )
        admin_engine = create_engine(
            admin_url,
            isolation_level="AUTOCOMMIT",
            connect_args={
                "sslmode": "require",
                "password": cred.token,
            },
        )
        with admin_engine.connect() as conn:
            exists = conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")).fetchone()
            if not exists:
                conn.execute(text(f'CREATE DATABASE "{db_name}"'))
                app_logger.info(f"Created Lakebase database: {db_name}")
            else:
                app_logger.info(f"Lakebase database already exists: {db_name}")
        admin_engine.dispose()

    def _seed_hierarchy_tiers(self, entity_id: int, app_db_module=None):
        """Seed pim_entity_tier from entity_subtype."""
        if app_db_module is None:
            import app.lakefusion_pim_service.utils.app_db as app_db_module
        DataSessionLocal = app_db_module.DataSessionLocal
        from lakefusion_utility.models.pim import PimEntityTier

        tiers = self.get_hierarchy_tiers(entity_id)
        data_db = DataSessionLocal()
        try:
            existing = data_db.query(PimEntityTier).count()
            if existing > 0:
                app_logger.info("Hierarchy tiers already seeded, skipping")
                return

            parent_id = None
            for tier in tiers:
                t = PimEntityTier(
                    code=tier["code"],
                    label=tier["label"],
                    level=tier["level"],
                    parent_tier_id=parent_id,
                    is_leaf=tier["is_leaf"],
                )
                data_db.add(t)
                data_db.flush()
                parent_id = t.id

            data_db.commit()
            app_logger.info(f"Seeded {len(tiers)} hierarchy tiers")
        except Exception:
            data_db.rollback()
            raise
        finally:
            data_db.close()

    def _bulk_sync_attributes(self, entity_id: int, app_db_module=None) -> int:
        """Bulk sync all global attributes from MySQL to Lakebase."""
        from app.lakefusion_pim_service.services.pim_attribute_sync_service import PimAttributeSyncService
        if app_db_module is None:
            import app.lakefusion_pim_service.utils.app_db as app_db_module
        DataSessionLocal = app_db_module.DataSessionLocal

        global_attrs = self.get_global_attributes(entity_id)
        if not global_attrs:
            return 0

        data_db = DataSessionLocal()
        try:
            sync_service = PimAttributeSyncService(data_db)
            count = 0
            for attr in global_attrs:
                sync_service.sync_attribute(attr)
                count += 1
            data_db.commit()
            app_logger.info(f"Synced {count} global attributes to Lakebase")
            return count
        except Exception:
            data_db.rollback()
            raise
        finally:
            data_db.close()

    def _seed_default_languages(self, app_db_module=None):
        """Seed default languages if pim_language_ref is empty."""
        if app_db_module is None:
            import app.lakefusion_pim_service.utils.app_db as app_db_module
        DataSessionLocal = app_db_module.DataSessionLocal
        from lakefusion_utility.models.pim import PimLanguageRef

        DEFAULT_LANGS = [
            {"id": "en", "language_name": "English"},
            {"id": "fr", "language_name": "French"},
            {"id": "de", "language_name": "German"},
            {"id": "es", "language_name": "Spanish"},
            {"id": "it", "language_name": "Italian"},
            {"id": "pt", "language_name": "Portuguese"},
            {"id": "ja", "language_name": "Japanese"},
            {"id": "zh", "language_name": "Chinese"},
        ]

        data_db = DataSessionLocal()
        try:
            existing_count = data_db.query(PimLanguageRef).count()
            if existing_count > 0:
                return
            for lang in DEFAULT_LANGS:
                data_db.add(PimLanguageRef(id=lang["id"], language_name=lang["language_name"]))
            data_db.commit()
            app_logger.info(f"Seeded {len(DEFAULT_LANGS)} default languages")
        except Exception:
            data_db.rollback()
            raise
        finally:
            data_db.close()

    def _seed_default_units(self, app_db_module=None):
        """Seed default measurement units if pim_unit_ref is empty."""
        if app_db_module is None:
            import app.lakefusion_pim_service.utils.app_db as app_db_module
        DataSessionLocal = app_db_module.DataSessionLocal
        from lakefusion_utility.models.pim import PimUnitRef

        DEFAULT_UNITS = [
            # Weight
            {"name": "kg", "category": "weight", "description": "Kilogram", "base_unit": "kg", "to_base_factor": 1.0},
            {"name": "g", "category": "weight", "description": "Gram", "base_unit": "kg", "to_base_factor": 0.001},
            {"name": "lbs", "category": "weight", "description": "Pound", "base_unit": "kg", "to_base_factor": 0.453592},
            {"name": "oz", "category": "weight", "description": "Ounce", "base_unit": "kg", "to_base_factor": 0.0283495},
            # Length
            {"name": "m", "category": "length", "description": "Meter", "base_unit": "m", "to_base_factor": 1.0},
            {"name": "cm", "category": "length", "description": "Centimeter", "base_unit": "m", "to_base_factor": 0.01},
            {"name": "mm", "category": "length", "description": "Millimeter", "base_unit": "m", "to_base_factor": 0.001},
            {"name": "in", "category": "length", "description": "Inch", "base_unit": "m", "to_base_factor": 0.0254},
            {"name": "ft", "category": "length", "description": "Foot", "base_unit": "m", "to_base_factor": 0.3048},
            # Volume
            {"name": "L", "category": "volume", "description": "Liter", "base_unit": "L", "to_base_factor": 1.0},
            {"name": "mL", "category": "volume", "description": "Milliliter", "base_unit": "L", "to_base_factor": 0.001},
            {"name": "gal", "category": "volume", "description": "US Gallon", "base_unit": "L", "to_base_factor": 3.78541},
            {"name": "fl oz", "category": "volume", "description": "US Fluid Ounce", "base_unit": "L", "to_base_factor": 0.0295735},
        ]

        data_db = DataSessionLocal()
        try:
            existing_count = data_db.query(PimUnitRef).count()
            if existing_count > 0:
                return
            for unit in DEFAULT_UNITS:
                data_db.add(PimUnitRef(**unit))
            data_db.commit()
            app_logger.info(f"Seeded {len(DEFAULT_UNITS)} default measurement units")
        except Exception:
            data_db.rollback()
            raise
        finally:
            data_db.close()
