import traceback
from sqlalchemy.orm import Session
from fastapi import HTTPException
from app.lakefusion_pim_service.utils import pim_sql
from lakefusion_utility.models.pim import (
    PimSpecificationConfigCreate, PimSpecificationConfigUpdate,
    PimTaxonomyDefaultScalarCreate, PimTaxonomyDefaultRefCreate,
)
from lakefusion_utility.utils.app_db import db_commit_auto_rollback
from lakefusion_utility.utils.logging_utils import get_logger

app_logger = get_logger(__name__)

# SCRUM-1929 Phase 6.3 — converted from ORM to raw SQL. Synced tables have NO
# enforced FK cascades, so delete_config explicitly removes its children
# (default_scalar, default_refs, resolved_specification) in order before the
# config row — replacing the ORM cascade='all, delete-orphan'. entity_name is
# threaded into the constructor.

CONFIG_TBL = "pim_specification_config"
SCALAR_TBL = "pim_taxonomy_default_scalar"
REF_TBL = "pim_taxonomy_default_ref"
RESOLVED_TBL = "pim_resolved_specification"
NODE_TBL = "pim_taxonomy_node"
ATTR_TBL = "pim_attribute_definition"


def _rebuild_resolved_cache(db: Session, entity_name: str):
    """Rebuild the pim_resolved_specification cache after config changes."""
    try:
        from app.lakefusion_pim_service.services.pim_taxonomy_service import PimTaxonomyService
        PimTaxonomyService(db, entity_name).rebuild_resolved_cache()
    except Exception as e:
        app_logger.warning(f"Failed to rebuild resolved cache: {e}")


class PimConfigService:
    def __init__(self, db: Session, entity_name: str):
        self.db = db
        self.entity_name = entity_name
        self._config = pim_sql.pim_tbl(entity_name, CONFIG_TBL)
        self._scalar = pim_sql.pim_tbl(entity_name, SCALAR_TBL)
        self._ref = pim_sql.pim_tbl(entity_name, REF_TBL)
        self._resolved = pim_sql.pim_tbl(entity_name, RESOLVED_TBL)
        self._node = pim_sql.pim_tbl(entity_name, NODE_TBL)
        self._attr = pim_sql.pim_tbl(entity_name, ATTR_TBL)

    # ------------------------------------------------------------------
    # CREATE config
    # ------------------------------------------------------------------
    def create_config(self, data: PimSpecificationConfigCreate):
        try:
            node = pim_sql.fetch_one(
                self.db, f'SELECT "id" FROM {self._node} WHERE "id" = :id',
                {"id": data.taxonomy_node_id},
            )
            if not node:
                raise HTTPException(status_code=404, detail="Taxonomy node not found.")

            attr = pim_sql.fetch_one(
                self.db, f'SELECT "id" FROM {self._attr} WHERE "id" = :id',
                {"id": data.attribute_id},
            )
            if not attr:
                raise HTTPException(status_code=404, detail="Attribute definition not found.")

            existing = pim_sql.fetch_one(
                self.db,
                f'SELECT "id" FROM {self._config} '
                f'WHERE "taxonomy_node_id" = :nid AND "attribute_id" = :aid',
                {"nid": data.taxonomy_node_id, "aid": data.attribute_id},
            )
            if existing:
                raise HTTPException(
                    status_code=409,
                    detail="Config for this taxonomy node + attribute already exists.",
                )

            sql, params = pim_sql.build_insert(
                self.entity_name, CONFIG_TBL,
                {
                    "taxonomy_node_id": data.taxonomy_node_id,
                    "attribute_id": data.attribute_id,
                    "is_required": data.is_required or False,
                    "inherit_to_children": data.inherit_to_children if data.inherit_to_children is not None else True,
                    "override_allowed": data.override_allowed if data.override_allowed is not None else True,
                    "level_override": data.level_override,
                    "display_order": data.display_order or 0,
                },
            )
            pim_sql.execute(self.db, sql, params)
            db_commit_auto_rollback(db=self.db)
            _rebuild_resolved_cache(self.db, self.entity_name)
            return pim_sql.coerce_row(CONFIG_TBL, pim_sql.fetch_one(
                self.db, f'SELECT * FROM {self._config} WHERE "id" = :id', {"id": params["id"]}
            ))

        except HTTPException:
            raise
        except Exception as e:
            message = traceback.format_exc()
            app_logger.exception(f"Unable to create config. Reason - {message}")
            raise HTTPException(status_code=500, detail=f"Failed to create config: {str(e)}")

    # ------------------------------------------------------------------
    # READ – configs for a taxonomy node (direct)
    # ------------------------------------------------------------------
    def get_configs_for_node(self, node_id: str):
        try:
            node = pim_sql.fetch_one(
                self.db,
                f'SELECT "id" FROM {self._node} WHERE "id" = :id AND "is_active" = TRUE',
                {"id": node_id},
            )
            if not node:
                raise HTTPException(status_code=404, detail="Taxonomy node not found.")

            return pim_sql.coerce_rows(CONFIG_TBL, pim_sql.fetch_all(
                self.db,
                f'SELECT * FROM {self._config} WHERE "taxonomy_node_id" = :nid '
                f'ORDER BY "display_order"',
                {"nid": node_id},
            ))
        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Error fetching configs for node: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # READ – resolved attributes for a node (from cache, enriched)
    # ------------------------------------------------------------------
    def get_resolved_for_node(self, node_id: str):
        try:
            rows = pim_sql.fetch_all(
                self.db,
                f'''SELECT
                        r."id" AS id,
                        r."taxonomy_node_id" AS taxonomy_node_id,
                        r."attribute_id" AS attribute_id,
                        r."config_id" AS config_id,
                        r."source_node_id" AS source_node_id,
                        r."is_required" AS is_required,
                        r."display_order" AS display_order,
                        r."resolved_at" AS resolved_at,
                        a."code" AS attribute_code,
                        a."label" AS attribute_label,
                        a."data_type" AS attribute_data_type,
                        a."reference_entity_id" AS attribute_reference_entity_id,
                        a."level" AS attribute_level,
                        a."group" AS attribute_group,
                        sn."label" AS source_node_label,
                        s."default_text" AS default_text,
                        s."default_number" AS default_number,
                        s."default_boolean" AS default_boolean,
                        c."level_override" AS level_override,
                        c."override_allowed" AS override_allowed,
                        c."inherit_to_children" AS inherit_to_children
                    FROM {self._resolved} r
                    JOIN {self._attr} a ON r."attribute_id" = a."id"
                    JOIN {self._node} sn ON r."source_node_id" = sn."id"
                    LEFT JOIN {self._scalar} s ON s."config_id" = r."config_id"
                    LEFT JOIN {self._config} c ON c."id" = r."config_id"
                    WHERE r."taxonomy_node_id" = :nid
                    ORDER BY r."display_order"''',
                {"nid": node_id},
            )

            result = []
            for row in rows:
                default_value = None
                if row["default_text"] is not None:
                    default_value = row["default_text"]
                elif row["default_number"] is not None:
                    default_value = str(row["default_number"])
                elif row["default_boolean"] is not None:
                    default_value = str(row["default_boolean"]).lower()

                effective_level = (row["level_override"] or row["attribute_level"]) or "ALL"

                result.append({
                    "id": row["id"],
                    "taxonomy_node_id": row["taxonomy_node_id"],
                    "attribute_id": row["attribute_id"],
                    "config_id": row["config_id"],
                    "source_node_id": row["source_node_id"],
                    # synced tables don't carry NOT NULL/defaults — coerce.
                    "is_required": bool(row["is_required"]),
                    "display_order": row["display_order"] or 0,
                    "resolved_at": row["resolved_at"],
                    "attribute_code": row["attribute_code"],
                    "attribute_label": row["attribute_label"],
                    "attribute_data_type": row["attribute_data_type"],
                    "attribute_reference_entity_id": row["attribute_reference_entity_id"],
                    "attribute_level": effective_level,
                    "attribute_group": row["attribute_group"] or "Specifications",
                    "default_value": default_value,
                    "source_node_label": row["source_node_label"],
                    "override_allowed": row["override_allowed"] if row["override_allowed"] is not None else True,
                    "inherit_to_children": row["inherit_to_children"] if row["inherit_to_children"] is not None else True,
                })
            return result
        except Exception as e:
            app_logger.exception(f"Error fetching resolved config: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # UPDATE config
    # ------------------------------------------------------------------
    def update_config(self, config_id: str, data: PimSpecificationConfigUpdate):
        try:
            existing = pim_sql.fetch_one(
                self.db, f'SELECT "id" FROM {self._config} WHERE "id" = :id', {"id": config_id}
            )
            if not existing:
                raise HTTPException(status_code=404, detail="Config not found.")

            set_values = {}
            if data.is_required is not None:
                set_values["is_required"] = data.is_required
            if data.inherit_to_children is not None:
                set_values["inherit_to_children"] = data.inherit_to_children
            if data.override_allowed is not None:
                set_values["override_allowed"] = data.override_allowed
            if data.level_override is not None:
                set_values["level_override"] = data.level_override
            if data.display_order is not None:
                set_values["display_order"] = data.display_order

            if set_values:
                sql, params = pim_sql.build_update(
                    self.entity_name, CONFIG_TBL, set_values,
                    where='"id" = :id', where_params={"id": config_id},
                )
                pim_sql.execute(self.db, sql, params)
                db_commit_auto_rollback(db=self.db)
            _rebuild_resolved_cache(self.db, self.entity_name)
            return pim_sql.coerce_row(CONFIG_TBL, pim_sql.fetch_one(
                self.db, f'SELECT * FROM {self._config} WHERE "id" = :id', {"id": config_id}
            ))

        except HTTPException:
            raise
        except Exception as e:
            message = traceback.format_exc()
            app_logger.exception(f"Unable to update config. Reason - {message}")
            raise HTTPException(status_code=500, detail=f"Failed to update config: {str(e)}")

    # ------------------------------------------------------------------
    # DELETE config (explicitly cascades defaults + resolved — no DB FK cascade)
    # ------------------------------------------------------------------
    def delete_config(self, config_id: str):
        try:
            existing = pim_sql.fetch_one(
                self.db, f'SELECT "id" FROM {self._config} WHERE "id" = :id', {"id": config_id}
            )
            if not existing:
                raise HTTPException(status_code=404, detail="Config not found.")

            # Replicate ORM cascade='all, delete-orphan' + ondelete=CASCADE:
            # remove children that FK to this config before the config itself.
            p = {"cid": config_id}
            pim_sql.execute(self.db, f'DELETE FROM {self._scalar} WHERE "config_id" = :cid', p)
            pim_sql.execute(self.db, f'DELETE FROM {self._ref} WHERE "config_id" = :cid', p)
            pim_sql.execute(self.db, f'DELETE FROM {self._resolved} WHERE "config_id" = :cid', p)
            pim_sql.execute(self.db, f'DELETE FROM {self._config} WHERE "id" = :cid', p)
            db_commit_auto_rollback(db=self.db)
            _rebuild_resolved_cache(self.db, self.entity_name)
            return {"message": "Config deleted."}

        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Unable to delete config: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # SET scalar default (upsert)
    # ------------------------------------------------------------------
    def set_scalar_default(self, config_id: str, data: PimTaxonomyDefaultScalarCreate):
        try:
            config = pim_sql.fetch_one(
                self.db, f'SELECT "id" FROM {self._config} WHERE "id" = :id', {"id": config_id}
            )
            if not config:
                raise HTTPException(status_code=404, detail="Config not found.")

            existing = pim_sql.fetch_one(
                self.db, f'SELECT "id" FROM {self._scalar} WHERE "config_id" = :cid',
                {"cid": config_id},
            )

            scalar_vals = {
                "default_text": data.default_text,
                "default_number": data.default_number,
                "default_boolean": data.default_boolean,
                "default_date": data.default_date,
            }
            if existing:
                sql, params = pim_sql.build_update(
                    self.entity_name, SCALAR_TBL, scalar_vals,
                    where='"config_id" = :cid', where_params={"cid": config_id},
                )
                pim_sql.execute(self.db, sql, params)
            else:
                sql, params = pim_sql.build_insert(
                    self.entity_name, SCALAR_TBL, {"config_id": config_id, **scalar_vals},
                )
                pim_sql.execute(self.db, sql, params)

            db_commit_auto_rollback(db=self.db)
            return pim_sql.fetch_one(
                self.db, f'SELECT * FROM {self._scalar} WHERE "config_id" = :cid', {"cid": config_id}
            )

        except HTTPException:
            raise
        except Exception as e:
            message = traceback.format_exc()
            app_logger.exception(f"Unable to set scalar default. Reason - {message}")
            raise HTTPException(status_code=500, detail=f"Failed to set scalar default: {str(e)}")

    # ------------------------------------------------------------------
    # SET ref defaults (replace all)
    # ------------------------------------------------------------------
    def set_ref_defaults(self, config_id: str, data: PimTaxonomyDefaultRefCreate):
        try:
            config = pim_sql.fetch_one(
                self.db, f'SELECT "id" FROM {self._config} WHERE "id" = :id', {"id": config_id}
            )
            if not config:
                raise HTTPException(status_code=404, detail="Config not found.")

            # Delete existing refs, then insert the new set (replace-all)
            pim_sql.execute(
                self.db, f'DELETE FROM {self._ref} WHERE "config_id" = :cid', {"cid": config_id}
            )
            for key in data.ref_value_keys:
                sql, params = pim_sql.build_insert(
                    self.entity_name, REF_TBL, {"config_id": config_id, "ref_value_key": key},
                )
                pim_sql.execute(self.db, sql, params)

            db_commit_auto_rollback(db=self.db)
            return pim_sql.fetch_all(
                self.db, f'SELECT * FROM {self._ref} WHERE "config_id" = :cid', {"cid": config_id}
            )

        except HTTPException:
            raise
        except Exception as e:
            message = traceback.format_exc()
            app_logger.exception(f"Unable to set ref defaults. Reason - {message}")
            raise HTTPException(status_code=500, detail=f"Failed to set ref defaults: {str(e)}")
