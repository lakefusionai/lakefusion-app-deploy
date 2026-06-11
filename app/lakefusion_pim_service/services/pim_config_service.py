import traceback
from datetime import datetime
from sqlalchemy.orm import Session
from fastapi import HTTPException
from lakefusion_utility.models.pim import (
    PimSpecificationConfig, PimSpecificationConfigCreate, PimSpecificationConfigUpdate,
    PimTaxonomyDefaultScalar, PimTaxonomyDefaultScalarCreate,
    PimTaxonomyDefaultRef, PimTaxonomyDefaultRefCreate,
    PimResolvedSpecification,
    PimTaxonomyNode, PimAttributeDefinition,
)
from lakefusion_utility.utils.app_db import db_commit_auto_rollback
from lakefusion_utility.utils.logging_utils import get_logger

app_logger = get_logger(__name__)


def _rebuild_resolved_cache(db: Session):
    """Rebuild the pim_resolved_specification cache after config changes."""
    try:
        from app.lakefusion_pim_service.services.pim_taxonomy_service import PimTaxonomyService
        PimTaxonomyService(db).rebuild_resolved_cache()
    except Exception as e:
        app_logger.warning(f"Failed to rebuild resolved cache: {e}")


class PimConfigService:
    def __init__(self, db: Session):
        self.db = db

    # ------------------------------------------------------------------
    # CREATE config
    # ------------------------------------------------------------------
    def create_config(self, data: PimSpecificationConfigCreate):
        try:
            # Validate taxonomy node exists
            node = self.db.query(PimTaxonomyNode).filter(PimTaxonomyNode.id == data.taxonomy_node_id).first()
            if not node:
                raise HTTPException(status_code=404, detail="Taxonomy node not found.")

            # Validate attribute exists
            attr = self.db.query(PimAttributeDefinition).filter(PimAttributeDefinition.id == data.attribute_id).first()
            if not attr:
                raise HTTPException(status_code=404, detail="Attribute definition not found.")

            # Check for duplicate
            existing = (
                self.db.query(PimSpecificationConfig)
                .filter(
                    PimSpecificationConfig.taxonomy_node_id == data.taxonomy_node_id,
                    PimSpecificationConfig.attribute_id == data.attribute_id,
                )
                .first()
            )
            if existing:
                raise HTTPException(
                    status_code=409,
                    detail="Config for this taxonomy node + attribute already exists.",
                )

            config = PimSpecificationConfig(
                taxonomy_node_id=data.taxonomy_node_id,
                attribute_id=data.attribute_id,
                is_required=data.is_required or False,
                inherit_to_children=data.inherit_to_children if data.inherit_to_children is not None else True,
                override_allowed=data.override_allowed if data.override_allowed is not None else True,
                level_override=data.level_override,
                display_order=data.display_order or 0,
            )
            self.db.add(config)
            db_commit_auto_rollback(db=self.db)
            self.db.refresh(config)
            _rebuild_resolved_cache(self.db)
            return config

        except HTTPException:
            raise
        except Exception as e:
            message = traceback.format_exc()
            app_logger.exception(f"Unable to create config. Reason - {message}")
            raise HTTPException(status_code=500, detail=f"Failed to create config: {str(e)}")

    # ------------------------------------------------------------------
    # READ – configs for a taxonomy node (direct + inherited)
    # ------------------------------------------------------------------
    def get_configs_for_node(self, node_id: str):
        try:
            node = (
                self.db.query(PimTaxonomyNode)
                .filter(PimTaxonomyNode.id == node_id, PimTaxonomyNode.is_active == True)
                .first()
            )
            if not node:
                raise HTTPException(status_code=404, detail="Taxonomy node not found.")

            # Get all ancestor node IDs from materialized_path
            configs = (
                self.db.query(PimSpecificationConfig)
                .filter(PimSpecificationConfig.taxonomy_node_id == node_id)
                .order_by(PimSpecificationConfig.display_order)
                .all()
            )
            return configs
        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Error fetching configs for node: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # READ – resolved attributes for a node (from cache)
    # ------------------------------------------------------------------
    def get_resolved_for_node(self, node_id: str):
        try:
            from sqlalchemy.orm import aliased
            SourceNode = aliased(PimTaxonomyNode)

            rows = (
                self.db.query(
                    PimResolvedSpecification,
                    PimAttributeDefinition.code.label('attribute_code'),
                    PimAttributeDefinition.label.label('attribute_label'),
                    PimAttributeDefinition.data_type.label('attribute_data_type'),
                    PimAttributeDefinition.reference_entity_id.label('attribute_reference_entity_id'),
                    PimAttributeDefinition.level.label('attribute_level'),
                    PimAttributeDefinition.group.label('attribute_group'),
                    SourceNode.label.label('source_node_label'),
                    PimTaxonomyDefaultScalar.default_text.label('default_text'),
                    PimTaxonomyDefaultScalar.default_number.label('default_number'),
                    PimTaxonomyDefaultScalar.default_boolean.label('default_boolean'),
                )
                .join(PimAttributeDefinition, PimResolvedSpecification.attribute_id == PimAttributeDefinition.id)
                .join(SourceNode, PimResolvedSpecification.source_node_id == SourceNode.id)
                .outerjoin(PimTaxonomyDefaultScalar, PimTaxonomyDefaultScalar.config_id == PimResolvedSpecification.config_id)
                .filter(PimResolvedSpecification.taxonomy_node_id == node_id)
                .order_by(PimResolvedSpecification.display_order)
                .all()
            )

            result = []
            for rac, attr_code, attr_label, attr_data_type, attr_ref, attr_level, attr_group, source_label, default_text, default_number, default_boolean in rows:
                # Determine the effective default value
                default_value = None
                if default_text is not None:
                    default_value = default_text
                elif default_number is not None:
                    default_value = str(default_number)
                elif default_boolean is not None:
                    default_value = str(default_boolean).lower()

                # Use level_override from the config if set, otherwise attribute's level
                config = self.db.query(PimSpecificationConfig).filter(
                    PimSpecificationConfig.id == rac.config_id
                ).first()
                effective_level = (config.level_override if config and config.level_override else attr_level) or "ALL"

                result.append({
                    "id": rac.id,
                    "taxonomy_node_id": rac.taxonomy_node_id,
                    "attribute_id": rac.attribute_id,
                    "config_id": rac.config_id,
                    "source_node_id": rac.source_node_id,
                    "is_required": rac.is_required,
                    "display_order": rac.display_order,
                    "resolved_at": rac.resolved_at,
                    "attribute_code": attr_code,
                    "attribute_label": attr_label,
                    "attribute_data_type": attr_data_type,
                    "attribute_reference_entity_id": attr_ref,
                    "attribute_level": effective_level,
                    "attribute_group": attr_group or "Specifications",
                    "default_value": default_value,
                    "source_node_label": source_label,
                    "override_allowed": config.override_allowed if config else True,
                    "inherit_to_children": config.inherit_to_children if config else True,
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
            config = (
                self.db.query(PimSpecificationConfig)
                .filter(PimSpecificationConfig.id == config_id)
                .first()
            )
            if not config:
                raise HTTPException(status_code=404, detail="Config not found.")

            if data.is_required is not None:
                config.is_required = data.is_required
            if data.inherit_to_children is not None:
                config.inherit_to_children = data.inherit_to_children
            if data.override_allowed is not None:
                config.override_allowed = data.override_allowed
            if data.level_override is not None:
                config.level_override = data.level_override
            if data.display_order is not None:
                config.display_order = data.display_order

            config.updated_at = datetime.utcnow()
            db_commit_auto_rollback(db=self.db)
            self.db.refresh(config)
            _rebuild_resolved_cache(self.db)
            return config

        except HTTPException:
            raise
        except Exception as e:
            message = traceback.format_exc()
            app_logger.exception(f"Unable to update config. Reason - {message}")
            raise HTTPException(status_code=500, detail=f"Failed to update config: {str(e)}")

    # ------------------------------------------------------------------
    # DELETE config (cascade deletes defaults too)
    # ------------------------------------------------------------------
    def delete_config(self, config_id: str):
        try:
            config = (
                self.db.query(PimSpecificationConfig)
                .filter(PimSpecificationConfig.id == config_id)
                .first()
            )
            if not config:
                raise HTTPException(status_code=404, detail="Config not found.")

            self.db.delete(config)
            db_commit_auto_rollback(db=self.db)
            _rebuild_resolved_cache(self.db)
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
            config = (
                self.db.query(PimSpecificationConfig)
                .filter(PimSpecificationConfig.id == config_id)
                .first()
            )
            if not config:
                raise HTTPException(status_code=404, detail="Config not found.")

            existing = (
                self.db.query(PimTaxonomyDefaultScalar)
                .filter(PimTaxonomyDefaultScalar.config_id == config_id)
                .first()
            )

            if existing:
                existing.default_text = data.default_text
                existing.default_number = data.default_number
                existing.default_boolean = data.default_boolean
                existing.default_date = data.default_date
                existing.updated_at = datetime.utcnow()
            else:
                scalar = PimTaxonomyDefaultScalar(
                    config_id=config_id,
                    default_text=data.default_text,
                    default_number=data.default_number,
                    default_boolean=data.default_boolean,
                    default_date=data.default_date,
                )
                self.db.add(scalar)

            db_commit_auto_rollback(db=self.db)
            result = (
                self.db.query(PimTaxonomyDefaultScalar)
                .filter(PimTaxonomyDefaultScalar.config_id == config_id)
                .first()
            )
            return result

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
            config = (
                self.db.query(PimSpecificationConfig)
                .filter(PimSpecificationConfig.id == config_id)
                .first()
            )
            if not config:
                raise HTTPException(status_code=404, detail="Config not found.")

            # Delete existing refs
            self.db.query(PimTaxonomyDefaultRef).filter(
                PimTaxonomyDefaultRef.config_id == config_id
            ).delete()

            # Insert new refs
            for key in data.ref_value_keys:
                ref = PimTaxonomyDefaultRef(
                    config_id=config_id,
                    ref_value_key=key,
                )
                self.db.add(ref)

            db_commit_auto_rollback(db=self.db)
            refs = (
                self.db.query(PimTaxonomyDefaultRef)
                .filter(PimTaxonomyDefaultRef.config_id == config_id)
                .all()
            )
            return refs

        except HTTPException:
            raise
        except Exception as e:
            message = traceback.format_exc()
            app_logger.exception(f"Unable to set ref defaults. Reason - {message}")
            raise HTTPException(status_code=500, detail=f"Failed to set ref defaults: {str(e)}")
