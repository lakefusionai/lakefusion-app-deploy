import re
from sqlalchemy.orm import Session
from app.lakefusion_pim_service.utils import pim_sql
from lakefusion_utility.models.pim import (
    PimAttributeDefinition, PimSpecificationConfig,
    PimValueText, PimValueNumber, PimValueBoolean,
    PimValueDate, PimValueSelect, PimValueMultiselect, PimValueReference,
    to_value_key,
)
from lakefusion_utility.utils.logging_utils import get_logger
from app.lakefusion_pim_service.services.pim_attribute_service import PimAttributeService

app_logger = get_logger(__name__)

# SCRUM-1929 Phase 6.3 — converted from ORM to raw SQL. Syncs global attributes
# from MySQL entityattributes → the Lakebase synced pim_attribute_definition table.
# Tightly coupled to PimAttributeService (auto-swap, option reconcile), so both
# convert together; entity_name is threaded into both.

ATTR_TBL = "pim_attribute_definition"
CONFIG_TBL = "pim_specification_config"
VALUE_TABLES = [
    "pim_value_text", "pim_value_number", "pim_value_boolean",
    "pim_value_date", "pim_value_select", "pim_value_multiselect", "pim_value_reference",
]

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
    code = name.strip().lower()
    code = re.sub(r'[^a-z0-9]+', '_', code)
    code = code.strip('_')
    return code


class PimAttributeSyncService:
    """Syncs global attributes from MySQL entityattributes → Lakebase pim_attribute_definition."""

    def __init__(self, data_db: Session, entity_name: str):
        self.data_db = data_db
        self.entity_name = entity_name
        self._attr = pim_sql.pim_tbl(entity_name, ATTR_TBL)
        self._config = pim_sql.pim_tbl(entity_name, CONFIG_TBL)
        self._attr_svc = PimAttributeService(data_db, entity_name)

    def _t(self, table):
        return pim_sql.pim_tbl(self.entity_name, table)

    def sync_attribute(self, attr_data: dict) -> dict:
        """Sync a single attribute (create/update/delete)."""
        # ---- DELETE ----
        if attr_data.get("action") == "delete":
            source_id = attr_data.get("source_attr_id")
            attr = None
            if source_id:
                attr = pim_sql.fetch_one(
                    self.data_db,
                    f'SELECT "id","code","scope" FROM {self._attr} WHERE "source_attr_id" = :sid',
                    {"sid": int(source_id)},
                )
            if attr:
                aid = attr["id"]
                # Clear values + configs referencing this attribute, then the attr
                # (synced tables have no FK cascade — explicit, ordered).
                for vt in VALUE_TABLES:
                    pim_sql.execute(self.data_db, f'DELETE FROM {self._t(vt)} WHERE "attribute_id" = :aid', {"aid": aid})
                pim_sql.execute(self.data_db, f'DELETE FROM {self._config} WHERE "attribute_id" = :aid', {"aid": aid})
                pim_sql.execute(self.data_db, f'DELETE FROM {self._attr} WHERE "id" = :aid', {"aid": aid})
                app_logger.info(f"Deleted attribute: {attr['code']} (source_attr_id={source_id}, scope={attr['scope']})")
                return {"action": "deleted", "count": 1}
            return {"action": "delete_skipped", "reason": "attribute not found"}

        code = _normalize_code(attr_data["name"])
        pim_type = attr_data.get("pim_data_type") or TYPE_MAPPING.get(attr_data["type"], "TEXT")
        label = attr_data.get("label", attr_data["name"])

        existing = pim_sql.fetch_one(
            self.data_db, f'SELECT * FROM {self._attr} WHERE "code" = :c', {"c": code}
        )

        if existing:
            # ---- UPDATE ----
            type_config = attr_data.get("type_config") or {}
            new_group = to_value_key(type_config.get("group") or "general") if isinstance(type_config, dict) else "general"
            new_display_order = type_config.get("display_order", 0) if isinstance(type_config, dict) else 0

            new_is_localizable = bool(type_config.get("is_localizable", False)) if isinstance(type_config, dict) else False
            new_level = type_config.get("level", None) if isinstance(type_config, dict) else None

            new_is_identifier = bool(attr_data.get("is_primary_key", False))
            new_is_label = bool(attr_data.get("is_label", False))

            set_values = {}
            if existing["label"] != label:
                set_values["label"] = label
            if existing["data_type"] != pim_type:
                set_values["data_type"] = pim_type
            if existing["group"] != new_group:
                set_values["group"] = new_group
            if existing["is_localizable"] != new_is_localizable:
                set_values["is_localizable"] = new_is_localizable
            if existing["display_order"] != new_display_order:
                set_values["display_order"] = new_display_order
            if new_level and existing["level"] != new_level:
                set_values["level"] = new_level

            if existing["is_identifier"] != new_is_identifier or existing["is_label"] != new_is_label:
                # Auto-swap previous holder before setting on this one
                self._attr_svc._auto_swap_identifier_label(
                    is_identifier=new_is_identifier,
                    is_label=new_is_label,
                    exclude_id=str(existing["id"]),
                )
            if existing["is_identifier"] != new_is_identifier:
                set_values["is_identifier"] = new_is_identifier
            if existing["is_label"] != new_is_label:
                set_values["is_label"] = new_is_label

            changed = bool(set_values)
            if changed:
                sql, params = pim_sql.build_update(
                    self.entity_name, ATTR_TBL, set_values,
                    where='"id" = :id', where_params={"id": existing["id"]},
                )
                pim_sql.execute(self.data_db, sql, params)

            # Reconcile options (MDM authoritative)
            if pim_type in ("SELECT", "MULTISELECT"):
                self._sync_options(existing["id"], type_config)

            if changed:
                app_logger.info(f"Updated global attribute: {code} (type={pim_type}, group={new_group})")
            return {"code": code, "action": "updated" if changed else "unchanged", "id": existing["id"]}

        # ---- CREATE ----
        ref_entity_id = None
        if attr_data.get("type_config") and attr_data["type"] == "REFERENCE_ENTITY":
            ref_entity_id = str(attr_data["type_config"].get("reference_entity_id", ""))

        # Read group, level, and display_order from type_config (set by SI in MDM portal)
        type_config = attr_data.get("type_config") or {}
        if not isinstance(type_config, dict):
            type_config = {}
        group = to_value_key(type_config.get("group") or "general")
        level = type_config.get("level", "PRODUCT")
        display_order = type_config.get("display_order", 0)

        # Map MDM flags to PIM flags
        is_identifier = bool(attr_data.get("is_primary_key", False))
        is_label_flag = bool(attr_data.get("is_label", False))

        if is_identifier or is_label_flag:
            self._attr_svc._auto_swap_identifier_label(is_identifier, is_label_flag)

        sql, params = pim_sql.build_insert(
            self.entity_name, ATTR_TBL,
            {
                "code": code,
                "label": label,
                "data_type": pim_type,
                "scope": "general",
                "level": level,
                "is_system": False,
                "is_localizable": bool(type_config.get("is_localizable", False)),
                "is_identifier": is_identifier,
                "is_label": is_label_flag,
                "reference_entity_id": ref_entity_id,
                "group": group,
                "display_order": display_order,
                "source_attr_id": attr_data.get("source_attr_id"),
                "description": attr_data.get("description") or None,
            },
        )
        pim_sql.execute(self.data_db, sql, params)
        new_id = params["id"]
        if pim_type in ("SELECT", "MULTISELECT"):
            self._sync_options(new_id, type_config)
        app_logger.info(f"Created global attribute: {code} (type={pim_type}, id={new_id})")
        return {"code": code, "action": "created", "id": new_id}

    def _sync_options(self, attr_id, type_config):
        """Reconcile pim_attribute_option for a synced SELECT/MULTISELECT attribute to match
        MDM's type_config.options (MDM authoritative). Delegates to the attribute service's
        _reconcile_options (now raw SQL)."""
        from lakefusion_utility.models.pim import PimAttributeOptionCreate

        raw = type_config.get("options") if isinstance(type_config, dict) else None
        if raw is None:
            return  # MDM sent no options key — leave PIM options untouched
        opts = []
        for idx, item in enumerate(raw):
            if isinstance(item, dict):
                label = (item.get("label") or item.get("value_key") or "").strip()
                value_key = (item.get("value_key") or "").strip() or None
                is_active = item.get("is_active", True) is not False
            else:
                label = str(item).strip()
                value_key = None
                is_active = True
            if not label:
                continue
            opts.append(PimAttributeOptionCreate(label=label, value_key=value_key, display_order=idx, is_active=is_active))
        self._attr_svc._reconcile_options(str(attr_id), opts)

    def deactivate_removed(self, current_codes: set):
        """Report global attributes no longer in MySQL (kept, not hard-deleted)."""
        lakebase_globals = pim_sql.fetch_all(
            self.data_db, f'SELECT "code" FROM {self._attr} WHERE "scope" = :s', {"s": "general"}
        )
        deactivated = 0
        for attr in lakebase_globals:
            if attr["code"] not in current_codes:
                app_logger.info(f"Global attribute '{attr['code']}' no longer in entity — keeping but flagging")
                deactivated += 1
        return deactivated
