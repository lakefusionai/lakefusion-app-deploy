import re
from sqlalchemy.orm import Session
from lakefusion_utility.models.pim import PimAttributeDefinition
from lakefusion_utility.utils.logging_utils import get_logger
from app.lakefusion_pim_service.services.pim_attribute_service import PimAttributeService

app_logger = get_logger(__name__)

# MySQL attribute type → PIM data_type
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


def _normalize_code(name: str) -> str:
    """Convert MySQL attribute name to a PIM attribute code.
    e.g. 'Brand Name' → 'brand_name', 'color' → 'color'
    """
    code = name.strip().lower()
    code = re.sub(r'[^a-z0-9]+', '_', code)
    code = code.strip('_')
    return code


class PimAttributeSyncService:
    """Syncs global attributes from MySQL entityattributes → Lakebase pim_attribute_definition."""

    def __init__(self, data_db: Session):
        self.data_db = data_db

    def sync_attribute(self, attr_data: dict) -> dict:
        """
        Sync a single attribute (create/update/delete).

        attr_data keys:
          - source_attr_id: int (MySQL entityattributes.id)
          - name: str (not present for deletes)
          - label: str
          - type: str (MySQL type)
          - type_config: dict | None
          - action: "delete" (optional — if present, soft-deletes the attribute)
        """
        # Handle delete action
        if attr_data.get("action") == "delete":
            source_id = attr_data.get("source_attr_id")
            if source_id:
                # Find by source_id in description or by iterating globals
                attrs = (
                    self.data_db.query(PimAttributeDefinition)
                    .filter(
                        PimAttributeDefinition.scope == "general",
                        PimAttributeDefinition.description.like(f"%source_id={source_id}%"),
                    )
                    .all()
                )
                for attr in attrs:
                    self.data_db.delete(attr)
                    app_logger.info(f"Deleted global attribute: {attr.code} (source_id={source_id})")
                if attrs:
                    return {"action": "deleted", "count": len(attrs)}
            return {"action": "delete_skipped", "reason": "attribute not found"}

        code = _normalize_code(attr_data["name"])
        pim_type = attr_data.get("pim_data_type") or TYPE_MAPPING.get(attr_data["type"], "TEXT")
        label = attr_data.get("label", attr_data["name"])

        # Check if attribute already exists by code
        existing = (
            self.data_db.query(PimAttributeDefinition)
            .filter(PimAttributeDefinition.code == code)
            .first()
        )

        if existing:
            # Update if type, label, or group changed
            type_config = attr_data.get("type_config") or {}
            new_group = type_config.get("group", "General") if isinstance(type_config, dict) else "General"
            new_display_order = type_config.get("display_order", 0) if isinstance(type_config, dict) else 0

            new_is_localizable = bool(type_config.get("is_localizable", False)) if isinstance(type_config, dict) else False
            new_level = type_config.get("level", None) if isinstance(type_config, dict) else None

            changed = False
            if existing.label != label:
                existing.label = label
                changed = True
            if existing.data_type != pim_type:
                existing.data_type = pim_type
                changed = True
            if existing.group != new_group:
                existing.group = new_group
                changed = True
            if existing.is_localizable != new_is_localizable:
                existing.is_localizable = new_is_localizable
                changed = True
            if existing.display_order != new_display_order:
                existing.display_order = new_display_order
                changed = True
            if new_level and existing.level != new_level:
                existing.level = new_level
                changed = True
            new_is_identifier = bool(attr_data.get("is_primary_key", False))
            new_is_label = bool(attr_data.get("is_label", False))
            if existing.is_identifier != new_is_identifier or existing.is_label != new_is_label:
                # Auto-swap: unset flag on previous holder before setting on this one
                attr_svc = PimAttributeService(self.data_db)
                attr_svc._auto_swap_identifier_label(
                    is_identifier=new_is_identifier,
                    is_label=new_is_label,
                    exclude_id=str(existing.id),
                )
            if existing.is_identifier != new_is_identifier:
                existing.is_identifier = new_is_identifier
                changed = True
            if existing.is_label != new_is_label:
                existing.is_label = new_is_label
                changed = True
            # Reconcile SELECT/MULTISELECT options from MDM (MDM is authoritative for synced attrs)
            if pim_type in ("SELECT", "MULTISELECT"):
                self._sync_options(existing.id, type_config)
            if changed:
                app_logger.info(f"Updated global attribute: {code} (type={pim_type}, group={new_group})")
            return {"code": code, "action": "updated" if changed else "unchanged", "id": existing.id}
        else:
            # Create new
            # Extract reference_entity_id from type_config if REFERENCE type
            ref_entity_id = None
            if attr_data.get("type_config") and attr_data["type"] == "REFERENCE_ENTITY":
                ref_entity_id = str(attr_data["type_config"].get("reference_entity_id", ""))

            # Read group, level, and display_order from type_config (set by SI in MDM portal)
            type_config = attr_data.get("type_config") or {}
            group = type_config.get("group", "General") if isinstance(type_config, dict) else "General"
            level = type_config.get("level", "PRODUCT") if isinstance(type_config, dict) else "PRODUCT"
            display_order = type_config.get("display_order", 0) if isinstance(type_config, dict) else 0

            # Map MDM flags to PIM flags
            is_identifier = bool(attr_data.get("is_primary_key", False))
            is_label_flag = bool(attr_data.get("is_label", False))

            # Auto-swap: unset flag on previous holder before setting on new attribute
            if is_identifier or is_label_flag:
                attr_svc = PimAttributeService(self.data_db)
                attr_svc._auto_swap_identifier_label(is_identifier, is_label_flag)

            new_attr = PimAttributeDefinition(
                code=code,
                label=label,
                data_type=pim_type,
                scope="general",
                level=level,
                is_system=False,
                is_localizable=bool(type_config.get("is_localizable", False)) if isinstance(type_config, dict) else False,
                is_identifier=is_identifier,
                is_label_attr=is_label_flag,
                reference_entity_id=ref_entity_id,
                group=group,
                display_order=display_order,
                description=f"Synced from entity attribute (source_id={attr_data.get('source_attr_id')})",
            )
            self.data_db.add(new_attr)
            self.data_db.flush()
            # Seed SELECT/MULTISELECT options from MDM type_config
            if pim_type in ("SELECT", "MULTISELECT"):
                self._sync_options(new_attr.id, type_config)
            app_logger.info(f"Created global attribute: {code} (type={pim_type}, id={new_attr.id})")
            return {"code": code, "action": "created", "id": new_attr.id}

    def _sync_options(self, attr_id, type_config):
        """Reconcile pim_attribute_option for a synced SELECT/MULTISELECT attribute to match
        MDM's type_config.options (MDM is authoritative). Each option may be a string label or
        a dict {value_key?, label, is_active?}. Active/Inactive (is_active) is mirrored from MDM.
        Removed options are deactivated if referenced by product values, else hard-deleted
        (handled by PimAttributeService._reconcile_options)."""
        from lakefusion_utility.models.pim import PimAttributeOptionCreate

        raw = type_config.get("options") if isinstance(type_config, dict) else None
        if raw is None:
            return  # MDM sent no options key — leave PIM options untouched
        opts = []
        for idx, item in enumerate(raw):
            if isinstance(item, dict):
                label = (item.get("label") or item.get("value_key") or "").strip()
                value_key = (item.get("value_key") or "").strip() or None
                # Default to active; only an explicit False deactivates.
                is_active = item.get("is_active", True) is not False
            else:
                label = str(item).strip()
                value_key = None
                is_active = True
            if not label:
                continue
            opts.append(PimAttributeOptionCreate(label=label, value_key=value_key, display_order=idx, is_active=is_active))
        PimAttributeService(self.data_db)._reconcile_options(str(attr_id), opts)

    def deactivate_removed(self, current_codes: set):
        """Soft-deactivate global attributes that are no longer in MySQL."""
        lakebase_globals = (
            self.data_db.query(PimAttributeDefinition)
            .filter(PimAttributeDefinition.scope == "general")
            .all()
        )
        deactivated = 0
        for attr in lakebase_globals:
            if attr.code not in current_codes:
                # Don't hard delete — values may reference this attribute
                app_logger.info(f"Global attribute '{attr.code}' no longer in entity — keeping but flagging")
                deactivated += 1
        return deactivated
