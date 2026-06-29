import traceback
from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session
from fastapi import HTTPException
from app.lakefusion_pim_service.utils import pim_sql
from lakefusion_utility.models.pim import (
    PimAttributeDefinitionCreate, PimAttributeDefinitionUpdate,
    PimAttributeOptionUpdate,
    to_value_key,
)
from lakefusion_utility.utils.app_db import db_commit_auto_rollback
from lakefusion_utility.utils.logging_utils import get_logger

app_logger = get_logger(__name__)

# SCRUM-1929 Phase 6.3 — converted from ORM to raw SQL against the Delta-synced
# tables. Synced tables carry NO FK constraints, so every cascade the ORM relied
# on is replicated explicitly. delete_definition keeps the load-bearing order
# that the composite-FK RESTRICT (select/multiselect -> option) used to enforce.
# entity_name is threaded into the constructor.

# Logical pim table names (resolved per-entity via pim_sql.pim_tbl)
ATTR_TBL = "pim_attribute_definition"
OPTION_TBL = "pim_attribute_option"
CONFIG_TBL = "pim_specification_config"
RESOLVED_TBL = "pim_resolved_specification"
ENTITY_TBL = "pim_entity"
TIER_TBL = "pim_entity_tier"
NODE_TBL = "pim_taxonomy_node"
FLAT_TBL = "pim_entity_flat"
VALUE_TABLES = [
    "pim_value_text", "pim_value_number", "pim_value_boolean",
    "pim_value_date", "pim_value_select", "pim_value_multiselect", "pim_value_reference",
]
# Value tables that carry the option composite-FK (RESTRICT) — these reference
# pim_attribute_option(attribute_id, value_key) via ref_value_key.
OPTION_VALUE_TABLES = ["pim_value_select", "pim_value_multiselect"]

INELIGIBLE_ID_LABEL_TYPES = {"SELECT", "MULTISELECT", "REFERENCE"}


def _assert_eligible_id_label(data_type: str, is_identifier: bool, is_label: bool):
    """Reject identifier/display-name flags on types that cannot hold them."""
    if (is_identifier or is_label) and (data_type or "").upper() in INELIGIBLE_ID_LABEL_TYPES:
        flag_name = "identifier" if is_identifier else "display name"
        raise HTTPException(
            status_code=400,
            detail=f"Attribute type '{data_type}' cannot be the product {flag_name}.",
        )


class PimAttributeService:
    def __init__(self, db: Session, entity_name: str):
        self.db = db
        self.entity_name = entity_name
        self._attr = pim_sql.pim_tbl(entity_name, ATTR_TBL)
        self._opt = pim_sql.pim_tbl(entity_name, OPTION_TBL)
        self._config = pim_sql.pim_tbl(entity_name, CONFIG_TBL)
        self._resolved = pim_sql.pim_tbl(entity_name, RESOLVED_TBL)
        self._entity = pim_sql.pim_tbl(entity_name, ENTITY_TBL)
        self._tier = pim_sql.pim_tbl(entity_name, TIER_TBL)
        self._node = pim_sql.pim_tbl(entity_name, NODE_TBL)
        self._flat = pim_sql.pim_tbl(entity_name, FLAT_TBL)

    def _t(self, table):
        return pim_sql.pim_tbl(self.entity_name, table)

    def _fetch_in(self, sql, list_param, values, extra=None):
        # Empty IN-list → short-circuit (expanding [] renders a broken empty-IN that
        # errors on text columns; IN () matches nothing anyway).
        values = list(values)
        if not values:
            return []
        stmt = text(sql).bindparams(bindparam(list_param, expanding=True))
        params = {list_param: values}
        if extra:
            params.update(extra)
        return [dict(r) for r in self.db.execute(stmt, params).mappings().all()]

    # ------------------------------------------------------------------
    # AUTO-SWAP: ensure at most one is_identifier and one is_label
    # ------------------------------------------------------------------
    def _auto_swap_identifier_label(self, is_identifier: bool, is_label: bool, exclude_id: str = None):
        """Unset is_identifier/is_label on any existing attribute before setting it on a new one."""
        if is_identifier:
            sql = f'UPDATE {self._attr} SET "is_identifier" = FALSE, "updated_at" = :ts WHERE "is_identifier" = TRUE'
            params = {"ts": pim_sql.utcnow()}
            if exclude_id:
                sql += ' AND "id" != :eid'
                params["eid"] = exclude_id
            pim_sql.execute(self.db, sql, params)
        if is_label:
            sql = f'UPDATE {self._attr} SET "is_label" = FALSE, "updated_at" = :ts WHERE "is_label" = TRUE'
            params = {"ts": pim_sql.utcnow()}
            if exclude_id:
                sql += ' AND "id" != :eid'
                params["eid"] = exclude_id
            pim_sql.execute(self.db, sql, params)

    # ------------------------------------------------------------------
    # OPTIONS (SELECT / MULTISELECT controlled vocabulary)
    # ------------------------------------------------------------------
    def _seed_options(self, attr_id: str, labels):
        """Insert option rows for an attribute. `labels` is an iterable of either
        plain label strings or (label, value_key, display_order, is_active) tuples.
        Does NOT commit. Returns the created option dicts."""
        seen = pim_sql.fetch_all(
            self.db, f'SELECT "value_key" FROM {self._opt} WHERE "attribute_id" = :aid',
            {"aid": attr_id},
        )
        seen_keys = {r["value_key"] for r in seen}
        created = []
        next_order = 0
        for idx, item in enumerate(labels):
            if isinstance(item, tuple):
                label, value_key, display_order, is_active = item
            else:
                label, value_key, display_order, is_active = item, None, idx, True
            label = (label or '').strip()
            if not label:
                continue
            key = to_value_key(value_key) if (value_key or '').strip() else to_value_key(label)
            base = key
            n = 2
            while key in seen_keys:
                key = f"{base}_{n}"
                n += 1
            seen_keys.add(key)
            order = display_order if display_order is not None else next_order
            next_order = max(next_order, order) + 1
            sql, params = pim_sql.build_insert(
                self.entity_name, OPTION_TBL,
                {
                    "attribute_id": attr_id,
                    "value_key": key,
                    "label": label,
                    "display_order": order,
                    "is_active": is_active if is_active is not None else True,
                },
            )
            pim_sql.execute(self.db, sql, params)
            created.append({**params})
        return created

    def _option_usage(self, attr_id: str, value_key: str) -> int:
        """Count product values referencing (attribute_id, value_key) across the
        option value tables (select/multiselect)."""
        usage = 0
        for vt in OPTION_VALUE_TABLES:
            cnt = pim_sql.fetch_scalar(
                self.db,
                f'SELECT COUNT("id") FROM {self._t(vt)} '
                f'WHERE "attribute_id" = :aid AND "ref_value_key" = :vk',
                {"aid": attr_id, "vk": value_key},
            )
            usage += cnt or 0
        return usage

    def _reconcile_options(self, attr_id: str, options):
        """Sync the option set to the supplied list (PimAttributeOptionCreate items).
        Upsert matches by value_key; dropped options are deactivated if referenced by
        product values, else hard-deleted. Does NOT commit."""
        existing = {
            o["value_key"]: o for o in pim_sql.fetch_all(
                self.db, f'SELECT * FROM {self._opt} WHERE "attribute_id" = :aid', {"aid": attr_id}
            )
        }
        incoming_keys = set()
        for idx, o in enumerate(options):
            label = (o.label or '').strip()
            if not label:
                continue
            key = to_value_key(o.value_key) if (o.value_key or '').strip() else to_value_key(label)
            incoming_keys.add(key)
            disp = o.display_order if o.display_order is not None else idx
            active = o.is_active if o.is_active is not None else True
            if key in existing:
                sql, params = pim_sql.build_update(
                    self.entity_name, OPTION_TBL,
                    {"label": label, "display_order": disp, "is_active": active},
                    where='"id" = :id', where_params={"id": existing[key]["id"]},
                )
                pim_sql.execute(self.db, sql, params)
            else:
                sql, params = pim_sql.build_insert(
                    self.entity_name, OPTION_TBL,
                    {"attribute_id": attr_id, "value_key": key, "label": label,
                     "display_order": disp, "is_active": active},
                )
                pim_sql.execute(self.db, sql, params)
        # Handle options dropped from the list
        for key, row in existing.items():
            if key in incoming_keys:
                continue
            if self._option_usage(attr_id, key) > 0:
                sql, params = pim_sql.build_update(
                    self.entity_name, OPTION_TBL, {"is_active": False},
                    where='"id" = :id', where_params={"id": row["id"]},
                )
                pim_sql.execute(self.db, sql, params)
            else:
                pim_sql.execute(self.db, f'DELETE FROM {self._opt} WHERE "id" = :id', {"id": row["id"]})

    def list_options(self, attr_id: str):
        return pim_sql.coerce_rows(OPTION_TBL, pim_sql.fetch_all(
            self.db,
            f'SELECT * FROM {self._opt} WHERE "attribute_id" = :aid '
            f'ORDER BY "display_order", "label"',
            {"aid": attr_id},
        ))

    def add_option(self, attr_id: str, label: str):
        attr = pim_sql.fetch_one(
            self.db, f'SELECT "data_type" FROM {self._attr} WHERE "id" = :id', {"id": attr_id}
        )
        if not attr:
            raise HTTPException(status_code=404, detail="Attribute definition not found.")
        if attr["data_type"] not in ('SELECT', 'MULTISELECT'):
            raise HTTPException(
                status_code=400,
                detail=f"Options apply only to SELECT/MULTISELECT attributes (got '{attr['data_type']}').",
            )
        label = (label or '').strip()
        if not label:
            raise HTTPException(status_code=400, detail="Option label is required.")
        max_order = pim_sql.fetch_scalar(
            self.db, f'SELECT MAX("display_order") FROM {self._opt} WHERE "attribute_id" = :aid',
            {"aid": attr_id},
        )
        next_order = (max_order + 1) if max_order is not None else 0
        opt = self._seed_options(attr_id, [(label, None, next_order, True)])
        db_commit_auto_rollback(db=self.db, raise_exception=True)
        if not opt:
            return None
        return pim_sql.fetch_one(
            self.db, f'SELECT * FROM {self._opt} WHERE "id" = :id', {"id": opt[0]["id"]}
        )

    def update_option(self, option_id: str, data: PimAttributeOptionUpdate):
        opt = pim_sql.fetch_one(
            self.db, f'SELECT "id" FROM {self._opt} WHERE "id" = :id', {"id": option_id}
        )
        if not opt:
            raise HTTPException(status_code=404, detail="Option not found.")
        set_values = {}
        if data.label is not None:
            set_values["label"] = data.label.strip()
        if data.display_order is not None:
            set_values["display_order"] = data.display_order
        if data.is_active is not None:
            set_values["is_active"] = data.is_active
        if set_values:
            sql, params = pim_sql.build_update(
                self.entity_name, OPTION_TBL, set_values,
                where='"id" = :id', where_params={"id": option_id},
            )
            pim_sql.execute(self.db, sql, params)
            db_commit_auto_rollback(db=self.db, raise_exception=True)
        return pim_sql.fetch_one(
            self.db, f'SELECT * FROM {self._opt} WHERE "id" = :id', {"id": option_id}
        )

    def delete_option(self, option_id: str):
        """Delete an option. Pre-check product usage (FK ON DELETE RESTRICT backstop)."""
        opt = pim_sql.fetch_one(
            self.db, f'SELECT * FROM {self._opt} WHERE "id" = :id', {"id": option_id}
        )
        if not opt:
            raise HTTPException(status_code=404, detail="Option not found.")
        usage = self._option_usage(opt["attribute_id"], opt["value_key"])
        if usage > 0:
            raise HTTPException(
                status_code=400,
                detail=f"Cannot delete option '{opt['label']}': {usage} product value(s) use it.",
            )
        pim_sql.execute(self.db, f'DELETE FROM {self._opt} WHERE "id" = :id', {"id": option_id})
        db_commit_auto_rollback(db=self.db, raise_exception=True)
        return {"message": f"Option '{opt['label']}' deleted."}

    def reorder_options(self, attr_id: str, order: list):
        for idx, option_id in enumerate(order):
            sql, params = pim_sql.build_update(
                self.entity_name, OPTION_TBL, {"display_order": idx},
                where='"id" = :id AND "attribute_id" = :aid',
                where_params={"id": option_id, "aid": attr_id},
            )
            pim_sql.execute(self.db, sql, params)
        db_commit_auto_rollback(db=self.db, raise_exception=True)
        return self.list_options(attr_id)

    # ------------------------------------------------------------------
    # CREATE
    # ------------------------------------------------------------------
    def _valid_tier_codes(self):
        return {r["code"] for r in pim_sql.fetch_all(self.db, f'SELECT "code" FROM {self._tier}')}

    def create_definition(self, data: PimAttributeDefinitionCreate):
        try:
            code = to_value_key(data.code)
            existing = pim_sql.fetch_one(
                self.db, f'SELECT "id" FROM {self._attr} WHERE "code" = :c', {"c": code}
            )
            if existing:
                raise HTTPException(status_code=409, detail=f"Attribute with code '{code}' already exists.")

            if data.data_type == 'REFERENCE' and not data.reference_entity_id:
                raise HTTPException(status_code=400, detail="Attribute type 'REFERENCE' requires a reference_entity_id.")
            if data.data_type != 'REFERENCE' and data.reference_entity_id:
                raise HTTPException(status_code=400, detail=f"Attribute type '{data.data_type}' must not have a reference_entity_id.")

            if data.level and data.level != "ALL":
                valid_codes = self._valid_tier_codes()
                for lvl in data.level.split(","):
                    lvl = lvl.strip()
                    if lvl and lvl != "ALL" and lvl not in valid_codes:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Invalid level '{lvl}'. Valid values: {', '.join(sorted(valid_codes))}, ALL",
                        )

            id_flag = data.is_identifier or False
            label_flag = data.is_label or False
            if (id_flag or label_flag) and data.level and data.level != "ALL":
                flag_name = "Identifier" if id_flag else "Display Name"
                raise HTTPException(
                    status_code=400,
                    detail=f"{flag_name} can only be set on attributes with level 'ALL' (all tiers). Current level: '{data.level}'",
                )

            try:
                pim_sql.validate_data_type(data.data_type)
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))
            _assert_eligible_id_label(data.data_type, id_flag, label_flag)
            self._auto_swap_identifier_label(id_flag, label_flag)

            # group constructor logic: scope-derived default (mirrors ORM __init__)
            group = data.group or ('Specifications' if (data.scope or 'specifications') == 'specifications' else 'General')

            sql, params = pim_sql.build_insert(
                self.entity_name, ATTR_TBL,
                {
                    "code": code,
                    "label": data.label,
                    "data_type": data.data_type,
                    "reference_entity_id": data.reference_entity_id,
                    "description": data.description,
                    "is_localizable": data.is_localizable,
                    "level": data.level or "ALL",
                    "scope": data.scope or 'specifications',
                    "is_system": False,
                    "is_identifier": id_flag,
                    "is_label": label_flag,
                    "group": group,
                    "display_order": data.display_order or 0,
                },
            )
            pim_sql.execute(self.db, sql, params)
            attr_id = params["id"]

            if data.data_type in ('SELECT', 'MULTISELECT') and data.options:
                self._seed_options(attr_id, [
                    (o.label, o.value_key, o.display_order, o.is_active) for o in data.options
                ])

            db_commit_auto_rollback(db=self.db, raise_exception=True)
            return self._get_with_options(attr_id)

        except HTTPException:
            raise
        except Exception as e:
            message = traceback.format_exc()
            app_logger.exception(f"Unable to create attribute definition. Reason - {message}")
            raise HTTPException(status_code=500, detail=f"Failed to create attribute: {str(e)}")

    def _get_with_options(self, attr_id: str):
        attr = pim_sql.fetch_one(self.db, f'SELECT * FROM {self._attr} WHERE "id" = :id', {"id": attr_id})
        if attr is not None:
            # Synced tables don't carry the models' NOT NULL/default=False, so these
            # bool flags can come back NULL — coerce to False (NULL ≡ not-set).
            for _f in ("is_system", "is_identifier", "is_label", "is_localizable"):
                attr[_f] = bool(attr.get(_f))
            attr["options"] = self.list_options(attr_id)
        return attr

    # ------------------------------------------------------------------
    # BULK CREATE
    # ------------------------------------------------------------------
    def bulk_create_definitions(self, items):
        try:
            codes = [to_value_key(item.code) for item in items]
            if len(codes) != len(set(codes)):
                raise HTTPException(status_code=400, detail="Duplicate codes found in the request.")

            existing_rows = self._fetch_in(
                f'SELECT "code" FROM {self._attr} WHERE "code" IN :codes', "codes", codes
            )
            existing_codes = {r["code"] for r in existing_rows}
            if existing_codes:
                raise HTTPException(
                    status_code=409,
                    detail=f"Attributes already exist for codes: {', '.join(sorted(existing_codes))}",
                )

            valid_tier_codes = self._valid_tier_codes()
            created_ids = []
            errors = []
            for idx, data in enumerate(items):
                if data.level and data.level != "ALL":
                    invalid_levels = [l.strip() for l in data.level.split(",") if l.strip() and l.strip() != "ALL" and l.strip() not in valid_tier_codes]
                    if invalid_levels:
                        errors.append(f"Row {idx + 1} ({data.code}): invalid level '{','.join(invalid_levels)}'. Valid: {', '.join(sorted(valid_tier_codes))}, ALL")
                        continue
                if data.data_type == 'REFERENCE' and not data.reference_entity_id:
                    errors.append(f"Row {idx + 1} ({data.code}): type 'REFERENCE' requires a reference_entity_id.")
                    continue
                if data.data_type != 'REFERENCE' and data.reference_entity_id:
                    errors.append(f"Row {idx + 1} ({data.code}): type '{data.data_type}' must not have a reference_entity_id.")
                    continue
                id_flag = data.is_identifier or False
                label_flag = data.is_label or False
                if (id_flag or label_flag) and (data.data_type or "").upper() in INELIGIBLE_ID_LABEL_TYPES:
                    flag_name = "identifier" if id_flag else "display name"
                    errors.append(f"Row {idx + 1} ({data.code}): type '{data.data_type}' cannot be the product {flag_name}.")
                    continue
                if id_flag or label_flag:
                    self._auto_swap_identifier_label(id_flag, label_flag)

                group = data.group or ('Specifications' if (data.scope or 'specifications') == 'specifications' else 'General')
                sql, params = pim_sql.build_insert(
                    self.entity_name, ATTR_TBL,
                    {
                        "code": to_value_key(data.code), "label": data.label,
                        "data_type": data.data_type, "reference_entity_id": data.reference_entity_id,
                        "description": data.description, "is_localizable": data.is_localizable,
                        "level": data.level or "ALL", "scope": data.scope or 'specifications',
                        "is_system": False, "is_identifier": id_flag, "is_label": label_flag,
                        "group": group, "display_order": data.display_order or 0,
                    },
                )
                pim_sql.execute(self.db, sql, params)
                created_ids.append(params["id"])
                if data.data_type in ('SELECT', 'MULTISELECT') and data.options:
                    self._seed_options(params["id"], [
                        (o.label, o.value_key, o.display_order, o.is_active) for o in data.options
                    ])

            if errors:
                self.db.rollback()
                raise HTTPException(status_code=400, detail=errors)

            db_commit_auto_rollback(db=self.db, raise_exception=True)
            return {"created": len(created_ids), "attributes": [self._get_with_options(i) for i in created_ids]}

        except HTTPException:
            raise
        except Exception as e:
            message = traceback.format_exc()
            app_logger.exception(f"Unable to bulk create attribute definitions. Reason - {message}")
            raise HTTPException(status_code=500, detail=f"Failed to bulk create attributes: {str(e)}")

    # ------------------------------------------------------------------
    # BULK IMPORT (Specifications tab flat-file import)
    # ------------------------------------------------------------------
    def bulk_import_definitions(self, items):
        """Import attribute definitions; existing codes skipped, valid rows committed."""
        try:
            existing_codes = {r["code"] for r in pim_sql.fetch_all(self.db, f'SELECT "code" FROM {self._attr}')}
            valid_tier_codes = self._valid_tier_codes()

            created = 0
            skipped = 0
            errors = []
            for idx, data in enumerate(items):
                code = to_value_key(data.code)
                if not code:
                    errors.append(f"Row {idx + 1}: missing code.")
                    continue
                if code in existing_codes:
                    skipped += 1
                    continue
                level = data.level or 'ALL'
                if level != "ALL":
                    invalid_levels = [l.strip() for l in level.split(",") if l.strip() and l.strip() != "ALL" and l.strip() not in valid_tier_codes]
                    if invalid_levels:
                        errors.append(f"Row {idx + 1} ({code}): invalid level '{','.join(invalid_levels)}'. Valid: {', '.join(sorted(valid_tier_codes))}, ALL")
                        continue

                group = data.group or ('Specifications' if (data.scope or 'specifications') == 'specifications' else 'General')
                sql, params = pim_sql.build_insert(
                    self.entity_name, ATTR_TBL,
                    {
                        "code": code, "label": data.label, "data_type": data.data_type or 'TEXT',
                        "description": data.description, "is_localizable": data.is_localizable,
                        "level": level, "scope": data.scope or 'specifications',
                        "is_system": False, "is_identifier": False, "is_label": False,
                        "group": group, "display_order": data.display_order or 0,
                    },
                )
                pim_sql.execute(self.db, sql, params)
                existing_codes.add(code)
                created += 1
                if (data.data_type or '').upper() in ('SELECT', 'MULTISELECT') and data.options:
                    self._seed_options(params["id"], [
                        (o.label, o.value_key, o.display_order, o.is_active) for o in data.options
                    ])

            db_commit_auto_rollback(db=self.db, raise_exception=True)
            app_logger.info(f"Bulk attribute import: {created} created, {skipped} skipped, {len(errors)} errors")
            return {"created": created, "skipped": skipped, "errors": errors}

        except HTTPException:
            raise
        except Exception as e:
            self.db.rollback()
            app_logger.exception(f"Bulk attribute import failed, rolled back: {traceback.format_exc()}")
            return {"created": 0, "skipped": 0, "errors": [f"Import failed (rolled back): {str(e)}"]}

    # ------------------------------------------------------------------
    # LIST (enriched)
    # ------------------------------------------------------------------
    def list_definitions(self, include_inactive: bool = False):
        try:
            attrs = pim_sql.fetch_all(self.db, f'SELECT * FROM {self._attr} ORDER BY "code"')
            if not attrs:
                return []
            attr_ids = [a["id"] for a in attrs]

            # Categories count per attribute (distinct taxonomy nodes from configs)
            cat_rows = self._fetch_in(
                f'SELECT "attribute_id", COUNT(DISTINCT "taxonomy_node_id") AS cnt '
                f'FROM {self._config} WHERE "attribute_id" IN :ids GROUP BY "attribute_id"',
                "ids", attr_ids,
            )
            cat_counts = {r["attribute_id"]: r["cnt"] for r in cat_rows}

            # Products count per attribute (distinct product_id across all value tables)
            prod_counts = {}
            for vt in VALUE_TABLES:
                rows = self._fetch_in(
                    f'SELECT "attribute_id", COUNT(DISTINCT "product_id") AS cnt '
                    f'FROM {self._t(vt)} WHERE "attribute_id" IN :ids GROUP BY "attribute_id"',
                    "ids", attr_ids,
                )
                for r in rows:
                    prod_counts[r["attribute_id"]] = prod_counts.get(r["attribute_id"], 0) + r["cnt"]

            # Assigned products count (resolved config joined to active products)
            assigned_rows = self._fetch_in(
                f'SELECT r."attribute_id" AS attribute_id, COUNT(DISTINCT e."id") AS cnt '
                f'FROM {self._resolved} r JOIN {self._entity} e '
                f'ON e."taxonomy_node_id" = r."taxonomy_node_id" '
                f'WHERE r."attribute_id" IN :ids AND e."active" = TRUE '
                f'GROUP BY r."attribute_id"',
                "ids", attr_ids,
            )
            assigned_counts = {r["attribute_id"]: r["cnt"] for r in assigned_rows}

            # Options per attribute
            options_by_attr = {}
            opt_rows = self._fetch_in(
                f'SELECT "id","attribute_id","value_key","label","display_order","is_active" '
                f'FROM {self._opt} WHERE "attribute_id" IN :ids '
                f'ORDER BY "display_order", "label"',
                "ids", attr_ids,
            )
            for o in opt_rows:
                options_by_attr.setdefault(o["attribute_id"], []).append(o)

            result = []
            for a in attrs:
                cats = cat_counts.get(a["id"], 0)
                prods = prod_counts.get(a["id"], 0)
                assigned = assigned_counts.get(a["id"], 0)
                coverage = round((prods / assigned) * 100, 1) if assigned > 0 else 0.0
                result.append({
                    "id": a["id"], "code": a["code"], "label": a["label"],
                    "data_type": a["data_type"], "reference_entity_id": a["reference_entity_id"],
                    "description": a["description"],
                    "level": a["level"], "scope": a["scope"] or 'specifications',
                    "group": a["group"] or 'General', "display_order": a["display_order"] or 0,
                    # Synced tables don't carry the models' NOT NULL/default=False, so
                    # these bool flags can come back NULL — coerce to False (NULL ≡ not-set).
                    "is_localizable": bool(a["is_localizable"]),
                    "is_identifier": bool(a["is_identifier"]), "is_label": bool(a["is_label"]),
                    "is_system": bool(a["is_system"]), "options": options_by_attr.get(a["id"], []),
                    "created_at": a["created_at"], "updated_at": a["updated_at"],
                    "categories_count": cats, "products_count": prods,
                    "assigned_products_count": assigned, "coverage_pct": coverage,
                })
            return result

        except Exception as e:
            app_logger.exception(f"Error listing attribute definitions: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # GET
    # ------------------------------------------------------------------
    def get_definition(self, attr_id: str):
        try:
            attr = self._get_with_options(attr_id)
            if not attr:
                raise HTTPException(status_code=404, detail="Attribute definition not found.")
            return attr
        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Error fetching attribute definition: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # TOGGLE IDENTIFIER / LABEL
    # ------------------------------------------------------------------
    def toggle_identifier(self, attr_id: str):
        try:
            attr = pim_sql.fetch_one(
                self.db, f'SELECT "data_type","is_identifier" FROM {self._attr} WHERE "id" = :id',
                {"id": attr_id},
            )
            if not attr:
                raise HTTPException(status_code=404, detail="Attribute not found.")
            new_val = not attr["is_identifier"]
            set_values = {"is_identifier": new_val}
            if new_val:
                _assert_eligible_id_label(attr["data_type"], is_identifier=True, is_label=False)
                self._auto_swap_identifier_label(is_identifier=True, is_label=False, exclude_id=attr_id)
                set_values["is_label"] = False  # mutual exclusion
            sql, params = pim_sql.build_update(
                self.entity_name, ATTR_TBL, set_values, where='"id" = :id', where_params={"id": attr_id},
            )
            pim_sql.execute(self.db, sql, params)
            db_commit_auto_rollback(db=self.db)
            return self._get_with_options(attr_id)
        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Failed to toggle identifier: {traceback.format_exc()}")
            raise HTTPException(status_code=500, detail=str(e))

    def toggle_label(self, attr_id: str):
        try:
            attr = pim_sql.fetch_one(
                self.db, f'SELECT "data_type","is_label" FROM {self._attr} WHERE "id" = :id',
                {"id": attr_id},
            )
            if not attr:
                raise HTTPException(status_code=404, detail="Attribute not found.")
            new_val = not attr["is_label"]
            set_values = {"is_label": new_val}
            if new_val:
                _assert_eligible_id_label(attr["data_type"], is_identifier=False, is_label=True)
                self._auto_swap_identifier_label(is_identifier=False, is_label=True, exclude_id=attr_id)
                set_values["is_identifier"] = False  # mutual exclusion
            sql, params = pim_sql.build_update(
                self.entity_name, ATTR_TBL, set_values, where='"id" = :id', where_params={"id": attr_id},
            )
            pim_sql.execute(self.db, sql, params)
            db_commit_auto_rollback(db=self.db)
            return self._get_with_options(attr_id)
        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Failed to toggle label: {traceback.format_exc()}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # UPDATE
    # ------------------------------------------------------------------
    def update_definition(self, attr_id: str, data: PimAttributeDefinitionUpdate):
        try:
            attr = pim_sql.fetch_one(self.db, f'SELECT * FROM {self._attr} WHERE "id" = :id', {"id": attr_id})
            if not attr:
                raise HTTPException(status_code=404, detail="Attribute definition not found.")

            set_values = {}
            if data.code is not None:
                dup = pim_sql.fetch_one(
                    self.db, f'SELECT "id" FROM {self._attr} WHERE "code" = :c AND "id" != :id',
                    {"c": data.code, "id": attr_id},
                )
                if dup:
                    raise HTTPException(status_code=409, detail=f"Attribute code '{data.code}' already in use.")
                set_values["code"] = data.code
            if data.label is not None:
                set_values["label"] = data.label
            if data.data_type is not None:
                try:
                    pim_sql.validate_data_type(data.data_type)
                except ValueError as e:
                    raise HTTPException(status_code=400, detail=str(e))
                set_values["data_type"] = data.data_type
            if data.reference_entity_id is not None:
                set_values["reference_entity_id"] = data.reference_entity_id
            if data.description is not None:
                set_values["description"] = data.description
            if data.scope is not None:
                set_values["scope"] = data.scope
            if data.is_localizable is not None:
                set_values["is_localizable"] = data.is_localizable
            if data.level is not None:
                if data.level != "ALL":
                    valid_codes = self._valid_tier_codes()
                    for lvl in data.level.split(","):
                        lvl = lvl.strip()
                        if lvl and lvl != "ALL" and lvl not in valid_codes:
                            raise HTTPException(
                                status_code=400,
                                detail=f"Invalid level '{lvl}'. Valid values: {', '.join(sorted(valid_codes))}, ALL",
                            )
                set_values["level"] = data.level
            if data.is_system is not None:
                set_values["is_system"] = data.is_system
            if data.group is not None:
                set_values["group"] = data.group
            if data.display_order is not None:
                set_values["display_order"] = data.display_order

            effective_level = data.level if data.level is not None else attr["level"]
            if (data.is_identifier or data.is_label) and effective_level and effective_level != "ALL":
                flag_name = "Identifier" if data.is_identifier else "Display Name"
                raise HTTPException(
                    status_code=400,
                    detail=f"{flag_name} can only be set on attributes with level 'ALL' (all tiers). Current level: '{effective_level}'",
                )
            effective_type = data.data_type if data.data_type is not None else attr["data_type"]
            _assert_eligible_id_label(effective_type, bool(data.is_identifier), bool(data.is_label))
            if data.is_identifier is not None or data.is_label is not None:
                self._auto_swap_identifier_label(
                    is_identifier=bool(data.is_identifier) if data.is_identifier is not None else False,
                    is_label=bool(data.is_label) if data.is_label is not None else False,
                    exclude_id=attr_id,
                )
            if data.is_identifier is not None:
                set_values["is_identifier"] = data.is_identifier
            if data.is_label is not None:
                set_values["is_label"] = data.is_label

            if set_values:
                sql, params = pim_sql.build_update(
                    self.entity_name, ATTR_TBL, set_values, where='"id" = :id', where_params={"id": attr_id},
                )
                pim_sql.execute(self.db, sql, params)

            # Reconcile options for SELECT/MULTISELECT when an options list is supplied.
            if data.options is not None and effective_type in ('SELECT', 'MULTISELECT'):
                self._reconcile_options(attr_id, data.options)

            db_commit_auto_rollback(db=self.db)
            return self._get_with_options(attr_id)

        except HTTPException:
            raise
        except Exception as e:
            message = traceback.format_exc()
            app_logger.exception(f"Unable to update attribute definition. Reason - {message}")
            raise HTTPException(status_code=500, detail=f"Failed to update attribute: {str(e)}")

    # ------------------------------------------------------------------
    # USAGE + DELETE
    # ------------------------------------------------------------------
    def get_usage_stats(self, attr_id: str):
        cat_count = pim_sql.fetch_scalar(
            self.db, f'SELECT COUNT(DISTINCT "taxonomy_node_id") FROM {self._config} WHERE "attribute_id" = :aid',
            {"aid": attr_id},
        ) or 0
        prod_count = 0
        for vt in VALUE_TABLES:
            cnt = pim_sql.fetch_scalar(
                self.db, f'SELECT COUNT(DISTINCT "product_id") FROM {self._t(vt)} WHERE "attribute_id" = :aid',
                {"aid": attr_id},
            )
            prod_count += cnt or 0
        return {"categories_count": cat_count, "products_count": prod_count}

    def get_usage_details(self, attr_id: str):
        cat_rows = pim_sql.fetch_all(
            self.db,
            f'SELECT c."taxonomy_node_id" AS taxonomy_node_id, n."label" AS label, n."code" AS code, '
            f'n."materialized_path" AS materialized_path, c."is_required" AS is_required '
            f'FROM {self._config} c JOIN {self._node} n ON n."id" = c."taxonomy_node_id" '
            f'WHERE c."attribute_id" = :aid ORDER BY n."label"',
            {"aid": attr_id},
        )
        categories = [
            {"node_id": r["taxonomy_node_id"], "label": r["label"], "code": r["code"],
             "path": r["materialized_path"], "is_required": r["is_required"]}
            for r in cat_rows
        ]

        product_ids_with_values = set()
        for vt in VALUE_TABLES:
            rows = pim_sql.fetch_all(
                self.db, f'SELECT DISTINCT "product_id" FROM {self._t(vt)} WHERE "attribute_id" = :aid',
                {"aid": attr_id},
            )
            product_ids_with_values.update(r["product_id"] for r in rows)

        products = []
        if product_ids_with_values:
            prod_rows = self._fetch_in(
                f'SELECT "id","identifier","entity_type_id","status","taxonomy_node_id" '
                f'FROM {self._flat} WHERE "id" IN :ids AND "active" = TRUE '
                f'ORDER BY "identifier" LIMIT 100',
                "ids", list(product_ids_with_values),
            )
            products = [
                {"id": r["id"], "identifier": r["identifier"], "entity_type_id": r["entity_type_id"],
                 "status": r["status"], "taxonomy_node_id": r["taxonomy_node_id"]}
                for r in prod_rows
            ]

        assigned_count = pim_sql.fetch_scalar(
            self.db,
            f'SELECT COUNT(DISTINCT e."id") FROM {self._entity} e '
            f'JOIN {self._resolved} r ON e."taxonomy_node_id" = r."taxonomy_node_id" '
            f'WHERE r."attribute_id" = :aid AND e."active" = TRUE',
            {"aid": attr_id},
        ) or 0

        filled_count = len(product_ids_with_values)
        coverage_pct = round((filled_count / assigned_count) * 100, 1) if assigned_count > 0 else 0.0
        return {
            "attribute_id": attr_id, "categories": categories, "categories_count": len(categories),
            "products_with_values": products, "products_count": filled_count,
            "assigned_products_count": assigned_count, "coverage_pct": coverage_pct,
        }

    def delete_definition(self, attr_id: str):
        try:
            attr = pim_sql.fetch_one(
                self.db, f'SELECT "code","is_system" FROM {self._attr} WHERE "id" = :id', {"id": attr_id}
            )
            if not attr:
                raise HTTPException(status_code=404, detail="Attribute definition not found.")
            if attr["is_system"]:
                raise HTTPException(
                    status_code=400,
                    detail=f"Cannot delete system attribute '{attr['code']}'. System attributes are protected.",
                )

            # Synced tables have NO FK cascade — replicate the ORM cascade explicitly.
            # ORDER IS LOAD-BEARING (formerly enforced by fk_pim_vs_option /
            # fk_pim_vm_option ON DELETE RESTRICT): value rows referencing options MUST
            # go before the options; options before the attribute. We make ALL of it
            # explicit since no DB cascade exists on synced tables:
            #   1) all value rows for this attribute (clears the option RESTRICT)
            #   2) pim_attribute_option rows for this attribute (ORM had ON DELETE CASCADE)
            #   3) pim_resolved_specification (derived cache, no FK)
            #   4) pim_specification_config (ORM had ON DELETE CASCADE)
            #   5) the attribute row
            p = {"aid": attr_id}
            for vt in VALUE_TABLES:
                pim_sql.execute(self.db, f'DELETE FROM {self._t(vt)} WHERE "attribute_id" = :aid', p)
            pim_sql.execute(self.db, f'DELETE FROM {self._opt} WHERE "attribute_id" = :aid', p)
            pim_sql.execute(self.db, f'DELETE FROM {self._resolved} WHERE "attribute_id" = :aid', p)
            pim_sql.execute(self.db, f'DELETE FROM {self._config} WHERE "attribute_id" = :aid', p)
            pim_sql.execute(self.db, f'DELETE FROM {self._attr} WHERE "id" = :aid', p)
            db_commit_auto_rollback(db=self.db, raise_exception=True)
            return {"message": f"Attribute definition '{attr['code']}' deleted."}

        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Unable to delete attribute definition: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))
