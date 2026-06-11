import traceback
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import func
from fastapi import HTTPException
from lakefusion_utility.models.pim import (
    PimAttributeDefinition, PimAttributeDefinitionCreate, PimAttributeDefinitionUpdate,
    PimAttributeOption, PimAttributeOptionUpdate,
    PimSpecificationConfig, PimResolvedSpecification,
    PimEntity, PimEntityTier, PimTaxonomyNode,
    PimValueText, PimValueNumber, PimValueBoolean,
    PimValueDate, PimValueSelect, PimValueMultiselect, PimValueReference,
    to_value_key,
)
from lakefusion_utility.utils.app_db import db_commit_auto_rollback
from lakefusion_utility.utils.logging_utils import get_logger

VALUE_TABLES = [
    PimValueText, PimValueNumber, PimValueBoolean,
    PimValueDate, PimValueSelect, PimValueMultiselect, PimValueReference,
]

# Controlled-vocabulary / reference types are not stable text keys, so they
# cannot serve as the product identifier or display name.
INELIGIBLE_ID_LABEL_TYPES = {"SELECT", "MULTISELECT", "REFERENCE"}


def _assert_eligible_id_label(data_type: str, is_identifier: bool, is_label: bool):
    """Reject identifier/display-name flags on types that cannot hold them."""
    if (is_identifier or is_label) and (data_type or "").upper() in INELIGIBLE_ID_LABEL_TYPES:
        flag_name = "identifier" if is_identifier else "display name"
        raise HTTPException(
            status_code=400,
            detail=f"Attribute type '{data_type}' cannot be the product {flag_name}.",
        )

app_logger = get_logger(__name__)


class PimAttributeService:
    def __init__(self, db: Session):
        self.db = db

    # ------------------------------------------------------------------
    # AUTO-SWAP: ensure at most one is_identifier and one is_label
    # Standard PIM pattern: setting the flag on a new attribute automatically
    # clears it from the previous holder.
    # ------------------------------------------------------------------
    def _auto_swap_identifier_label(self, is_identifier: bool, is_label: bool, exclude_id: str = None):
        """Unset is_identifier/is_label on any existing attribute before setting it on a new one."""
        if is_identifier:
            q = self.db.query(PimAttributeDefinition).filter(PimAttributeDefinition.is_identifier == True)
            if exclude_id:
                q = q.filter(PimAttributeDefinition.id != exclude_id)
            for existing in q.all():
                existing.is_identifier = False
        if is_label:
            q = self.db.query(PimAttributeDefinition).filter(PimAttributeDefinition.is_label == True)
            if exclude_id:
                q = q.filter(PimAttributeDefinition.id != exclude_id)
            for existing in q.all():
                existing.is_label = False

    # ------------------------------------------------------------------
    # OPTIONS (SELECT / MULTISELECT controlled vocabulary)
    # ------------------------------------------------------------------
    def _seed_options(self, attr_id: str, labels):
        """Insert option rows for an attribute. `labels` is an iterable of either
        plain label strings or (label, value_key, display_order, is_active) tuples.
        value_key is derived from label when not supplied; collisions within the
        attribute are suffixed _2, _3, … Returns the created PimAttributeOption rows."""
        seen_keys = {
            r.value_key for r in
            self.db.query(PimAttributeOption.value_key)
            .filter(PimAttributeOption.attribute_id == attr_id).all()
        }
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
            key = (value_key or '').strip() or to_value_key(label)
            # de-dup key within this attribute
            base = key
            n = 2
            while key in seen_keys:
                key = f"{base}_{n}"
                n += 1
            seen_keys.add(key)
            order = display_order if display_order is not None else next_order
            next_order = max(next_order, order) + 1
            opt = PimAttributeOption(
                attribute_id=attr_id,
                value_key=key,
                label=label,
                display_order=order,
                is_active=is_active if is_active is not None else True,
            )
            self.db.add(opt)
            created.append(opt)
        return created

    def _reconcile_options(self, attr_id: str, options):
        """Sync the option set to the supplied list (PimAttributeOptionCreate items).
        - Match existing options by value_key (explicit or derived from label).
        - Upsert: update label/display_order/is_active for matches; insert new ones.
        - Options NOT in the new list are DEACTIVATED (is_active=False), not hard-deleted,
          so any product value already referencing them survives (FK ON DELETE RESTRICT).
          A removed option with zero product references is hard-deleted."""
        existing = {
            o.value_key: o for o in
            self.db.query(PimAttributeOption)
            .filter(PimAttributeOption.attribute_id == attr_id).all()
        }
        incoming_keys = set()
        for idx, o in enumerate(options):
            label = (o.label or '').strip()
            if not label:
                continue
            key = (o.value_key or '').strip() or to_value_key(label)
            incoming_keys.add(key)
            row = existing.get(key)
            if row:
                row.label = label
                row.display_order = o.display_order if o.display_order is not None else idx
                row.is_active = o.is_active if o.is_active is not None else True
                row.updated_at = datetime.utcnow()
            else:
                self.db.add(PimAttributeOption(
                    attribute_id=attr_id,
                    value_key=key,
                    label=label,
                    display_order=o.display_order if o.display_order is not None else idx,
                    is_active=o.is_active if o.is_active is not None else True,
                ))
        # Handle options dropped from the list
        for key, row in existing.items():
            if key in incoming_keys:
                continue
            usage = 0
            for VT in (PimValueSelect, PimValueMultiselect):
                usage += (
                    self.db.query(func.count(VT.id))
                    .filter(VT.attribute_id == attr_id, VT.ref_value_key == key)
                    .scalar() or 0
                )
            if usage > 0:
                row.is_active = False          # preserve referenced options
                row.updated_at = datetime.utcnow()
            else:
                self.db.delete(row)            # safe to remove — nothing references it

    def list_options(self, attr_id: str):
        return (
            self.db.query(PimAttributeOption)
            .filter(PimAttributeOption.attribute_id == attr_id)
            .order_by(PimAttributeOption.display_order, PimAttributeOption.label)
            .all()
        )

    def add_option(self, attr_id: str, label: str):
        attr = self.db.query(PimAttributeDefinition).filter(PimAttributeDefinition.id == attr_id).first()
        if not attr:
            raise HTTPException(status_code=404, detail="Attribute definition not found.")
        if attr.data_type not in ('SELECT', 'MULTISELECT'):
            raise HTTPException(
                status_code=400,
                detail=f"Options apply only to SELECT/MULTISELECT attributes (got '{attr.data_type}').",
            )
        label = (label or '').strip()
        if not label:
            raise HTTPException(status_code=400, detail="Option label is required.")
        max_order = (
            self.db.query(func.max(PimAttributeOption.display_order))
            .filter(PimAttributeOption.attribute_id == attr_id).scalar()
        )
        next_order = (max_order + 1) if max_order is not None else 0
        opt = self._seed_options(attr_id, [(label, None, next_order, True)])
        db_commit_auto_rollback(db=self.db, raise_exception=True)
        return opt[0] if opt else None

    def update_option(self, option_id: str, data: PimAttributeOptionUpdate):
        opt = self.db.query(PimAttributeOption).filter(PimAttributeOption.id == option_id).first()
        if not opt:
            raise HTTPException(status_code=404, detail="Option not found.")
        # value_key stays stable (UI edits label only). ON UPDATE CASCADE is the safety net
        # if a key is ever changed, but we do not expose key edits here.
        if data.label is not None:
            opt.label = data.label.strip()
        if data.display_order is not None:
            opt.display_order = data.display_order
        if data.is_active is not None:
            opt.is_active = data.is_active
        opt.updated_at = datetime.utcnow()
        db_commit_auto_rollback(db=self.db, raise_exception=True)
        self.db.refresh(opt)
        return opt

    def delete_option(self, option_id: str):
        """Delete an option. Pre-check product usage: if any product value references
        this option's (attribute_id, value_key), block with 400 + count (FK ON DELETE
        RESTRICT is the DB backstop). An option belongs to exactly one attribute, so the
        only thing that can reference it is a product value of that same attribute."""
        opt = self.db.query(PimAttributeOption).filter(PimAttributeOption.id == option_id).first()
        if not opt:
            raise HTTPException(status_code=404, detail="Option not found.")
        usage = 0
        for VT in (PimValueSelect, PimValueMultiselect):
            usage += (
                self.db.query(func.count(VT.id))
                .filter(VT.attribute_id == opt.attribute_id, VT.ref_value_key == opt.value_key)
                .scalar() or 0
            )
        if usage > 0:
            raise HTTPException(
                status_code=400,
                detail=f"Cannot delete option '{opt.label}': {usage} product value(s) use it.",
            )
        self.db.delete(opt)
        db_commit_auto_rollback(db=self.db, raise_exception=True)
        return {"message": f"Option '{opt.label}' deleted."}

    def reorder_options(self, attr_id: str, order: list):
        rows = {
            r.id: r for r in
            self.db.query(PimAttributeOption)
            .filter(PimAttributeOption.attribute_id == attr_id).all()
        }
        for idx, option_id in enumerate(order):
            r = rows.get(option_id)
            if r:
                r.display_order = idx
                r.updated_at = datetime.utcnow()
        db_commit_auto_rollback(db=self.db, raise_exception=True)
        return self.list_options(attr_id)

    # ------------------------------------------------------------------
    # CREATE
    # ------------------------------------------------------------------
    def create_definition(self, data: PimAttributeDefinitionCreate):
        try:
            existing = (
                self.db.query(PimAttributeDefinition)
                .filter(PimAttributeDefinition.code == data.code)
                .first()
            )
            if existing:
                raise HTTPException(
                    status_code=409,
                    detail=f"Attribute with code '{data.code}' already exists.",
                )

            # REFERENCE resolves to an RDM entity → requires reference_entity_id.
            # SELECT/MULTISELECT use the pim_attribute_option catalog (NOT reference_entity_id).
            # All other types must not carry a reference_entity_id.
            if data.data_type == 'REFERENCE' and not data.reference_entity_id:
                raise HTTPException(
                    status_code=400,
                    detail="Attribute type 'REFERENCE' requires a reference_entity_id.",
                )
            if data.data_type != 'REFERENCE' and data.reference_entity_id:
                raise HTTPException(
                    status_code=400,
                    detail=f"Attribute type '{data.data_type}' must not have a reference_entity_id.",
                )

            # Validate level against pim_entity_tier codes
            if data.level and data.level != "ALL":
                valid_codes = {t.code for t in self.db.query(PimEntityTier.code).all()}
                for lvl in data.level.split(","):
                    lvl = lvl.strip()
                    if lvl and lvl != "ALL" and lvl not in valid_codes:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Invalid level '{lvl}'. Valid values: {', '.join(sorted(valid_codes))}, ALL",
                        )

            id_flag = data.is_identifier or False
            label_flag = data.is_label or False

            # Identifier and Display Name can only be set on ALL-tier attributes
            if (id_flag or label_flag) and data.level and data.level != "ALL":
                flag_name = "Identifier" if id_flag else "Display Name"
                raise HTTPException(
                    status_code=400,
                    detail=f"{flag_name} can only be set on attributes with level 'ALL' (all tiers). Current level: '{data.level}'",
                )

            _assert_eligible_id_label(data.data_type, id_flag, label_flag)
            self._auto_swap_identifier_label(id_flag, label_flag)

            attr = PimAttributeDefinition(
                code=data.code,
                label=data.label,
                data_type=data.data_type,
                reference_entity_id=data.reference_entity_id,
                description=data.description,
                is_localizable=data.is_localizable,
                level=data.level,
                scope=data.scope or 'specifications',
                is_identifier=id_flag,
                is_label_attr=label_flag,
                group=data.group,
                display_order=data.display_order or 0,
            )
            self.db.add(attr)
            self.db.flush()  # need attr.id before adding options

            # Persist SELECT/MULTISELECT options (ignored for other types)
            if data.data_type in ('SELECT', 'MULTISELECT') and data.options:
                self._seed_options(attr.id, [
                    (o.label, o.value_key, o.display_order, o.is_active)
                    for o in data.options
                ])

            db_commit_auto_rollback(db=self.db, raise_exception=True)
            self.db.refresh(attr)
            return attr

        except HTTPException:
            raise
        except Exception as e:
            message = traceback.format_exc()
            app_logger.exception(f"Unable to create attribute definition. Reason - {message}")
            raise HTTPException(status_code=500, detail=f"Failed to create attribute: {str(e)}")

    # ------------------------------------------------------------------
    # BULK CREATE
    # ------------------------------------------------------------------
    def bulk_create_definitions(self, items: list[PimAttributeDefinitionCreate]):
        try:
            # Collect all codes to check for duplicates in one query
            codes = [item.code for item in items]
            if len(codes) != len(set(codes)):
                raise HTTPException(status_code=400, detail="Duplicate codes found in the request.")

            existing = (
                self.db.query(PimAttributeDefinition.code)
                .filter(PimAttributeDefinition.code.in_(codes))
                .all()
            )
            existing_codes = {row.code for row in existing}
            if existing_codes:
                raise HTTPException(
                    status_code=409,
                    detail=f"Attributes already exist for codes: {', '.join(sorted(existing_codes))}",
                )

            # Pre-fetch valid tier codes for level validation
            valid_tier_codes = {t.code for t in self.db.query(PimEntityTier.code).all()}

            created = []
            errors = []
            for idx, data in enumerate(items):
                # Validate level against pim_entity_tier codes
                if data.level and data.level != "ALL":
                    invalid_levels = [l.strip() for l in data.level.split(",") if l.strip() and l.strip() != "ALL" and l.strip() not in valid_tier_codes]
                    if invalid_levels:
                        errors.append(f"Row {idx + 1} ({data.code}): invalid level '{','.join(invalid_levels)}'. Valid: {', '.join(sorted(valid_tier_codes))}, ALL")
                        continue

                # REFERENCE requires reference_entity_id; SELECT/MULTISELECT use options (not ref).
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

                attr = PimAttributeDefinition(
                    code=data.code,
                    label=data.label,
                    data_type=data.data_type,
                    reference_entity_id=data.reference_entity_id,
                    description=data.description,
                    is_localizable=data.is_localizable,
                    level=data.level,
                    scope=data.scope or 'specifications',
                    is_identifier=id_flag,
                    is_label_attr=label_flag,
                    group=data.group,
                    display_order=data.display_order or 0,
                )
                self.db.add(attr)
                created.append(attr)

                if data.data_type in ('SELECT', 'MULTISELECT') and data.options:
                    self.db.flush()  # need attr.id
                    self._seed_options(attr.id, [
                        (o.label, o.value_key, o.display_order, o.is_active)
                        for o in data.options
                    ])

            if errors:
                self.db.rollback()
                raise HTTPException(status_code=400, detail=errors)

            db_commit_auto_rollback(db=self.db, raise_exception=True)
            for attr in created:
                self.db.refresh(attr)

            return {"created": len(created), "attributes": created}

        except HTTPException:
            raise
        except Exception as e:
            message = traceback.format_exc()
            app_logger.exception(f"Unable to bulk create attribute definitions. Reason - {message}")
            raise HTTPException(status_code=500, detail=f"Failed to bulk create attributes: {str(e)}")

    # ------------------------------------------------------------------
    # LIST
    # ------------------------------------------------------------------
    def list_definitions(self, include_inactive: bool = False):
        try:
            attrs = self.db.query(PimAttributeDefinition).order_by(PimAttributeDefinition.code).all()
            if not attrs:
                return []

            attr_ids = [a.id for a in attrs]

            # Batch: categories count per attribute (from taxonomy configs)
            cat_counts = dict(
                self.db.query(
                    PimSpecificationConfig.attribute_id,
                    func.count(func.distinct(PimSpecificationConfig.taxonomy_node_id)),
                )
                .filter(PimSpecificationConfig.attribute_id.in_(attr_ids))
                .group_by(PimSpecificationConfig.attribute_id)
                .all()
            )

            # Batch: products count per attribute (from all value tables)
            prod_counts = {}
            for VT in VALUE_TABLES:
                rows = (
                    self.db.query(VT.attribute_id, func.count(func.distinct(VT.product_id)))
                    .filter(VT.attribute_id.in_(attr_ids))
                    .group_by(VT.attribute_id)
                    .all()
                )
                for aid, cnt in rows:
                    prod_counts[aid] = prod_counts.get(aid, 0) + cnt

            # Batch: assigned products count per attribute (products in categories where attr is configured)
            # Uses resolved config (includes inherited attrs) joined to products via taxonomy_node_id
            assigned_counts = {}
            assigned_rows = (
                self.db.query(
                    PimResolvedSpecification.attribute_id,
                    func.count(func.distinct(PimEntity.id)),
                )
                .join(PimEntity, PimEntity.taxonomy_node_id == PimResolvedSpecification.taxonomy_node_id)
                .filter(PimResolvedSpecification.attribute_id.in_(attr_ids))
                .filter(PimEntity.active == True)
                .group_by(PimResolvedSpecification.attribute_id)
                .all()
            )
            for aid, cnt in assigned_rows:
                assigned_counts[aid] = cnt

            # Batch: options per attribute (SELECT/MULTISELECT only have any)
            options_by_attr = {}
            opt_rows = (
                self.db.query(PimAttributeOption)
                .filter(PimAttributeOption.attribute_id.in_(attr_ids))
                .order_by(PimAttributeOption.display_order, PimAttributeOption.label)
                .all()
            )
            for o in opt_rows:
                options_by_attr.setdefault(o.attribute_id, []).append({
                    "id": o.id,
                    "attribute_id": o.attribute_id,
                    "value_key": o.value_key,
                    "label": o.label,
                    "display_order": o.display_order,
                    "is_active": o.is_active,
                })

            # Enrich
            result = []
            for a in attrs:
                cats = cat_counts.get(a.id, 0)
                prods = prod_counts.get(a.id, 0)
                assigned = assigned_counts.get(a.id, 0)
                coverage = round((prods / assigned) * 100, 1) if assigned > 0 else 0.0

                d = {
                    "id": a.id,
                    "code": a.code,
                    "label": a.label,
                    "data_type": a.data_type,
                    "reference_entity_id": a.reference_entity_id,
                    "description": a.description,
                    "is_localizable": a.is_localizable,
                    "level": a.level,
                    "scope": a.scope or 'specifications',
                    "group": a.group or 'General',
                    "display_order": a.display_order or 0,
                    "is_identifier": a.is_identifier,
                    "is_label": a.is_label,
                    "is_system": a.is_system,
                    "options": options_by_attr.get(a.id, []),
                    "created_at": a.created_at,
                    "updated_at": a.updated_at,
                    "categories_count": cats,
                    "products_count": prods,
                    "assigned_products_count": assigned,
                    "coverage_pct": coverage,
                }
                result.append(d)
            return result

        except Exception as e:
            app_logger.exception(f"Error listing attribute definitions: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # GET
    # ------------------------------------------------------------------
    def get_definition(self, attr_id: str):
        try:
            attr = (
                self.db.query(PimAttributeDefinition)
                .filter(PimAttributeDefinition.id == attr_id)
                .first()
            )
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
        """Toggle is_identifier on the given attribute. If turning on, auto-swaps the previous identifier.
        Also enforces mutual exclusion: an attribute cannot be both identifier and label."""
        try:
            attr = self.db.query(PimAttributeDefinition).filter(PimAttributeDefinition.id == attr_id).first()
            if not attr:
                raise HTTPException(status_code=404, detail="Attribute not found.")
            new_val = not attr.is_identifier
            if new_val:
                _assert_eligible_id_label(attr.data_type, is_identifier=True, is_label=False)
                self._auto_swap_identifier_label(is_identifier=True, is_label=False, exclude_id=attr_id)
                attr.is_label = False  # mutual exclusion
            attr.is_identifier = new_val
            attr.updated_at = datetime.utcnow()
            db_commit_auto_rollback(db=self.db)
            self.db.refresh(attr)
            return attr
        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Failed to toggle identifier: {traceback.format_exc()}")
            raise HTTPException(status_code=500, detail=str(e))

    def toggle_label(self, attr_id: str):
        """Toggle is_label on the given attribute. If turning on, auto-swaps the previous label.
        Also enforces mutual exclusion: an attribute cannot be both identifier and label."""
        try:
            attr = self.db.query(PimAttributeDefinition).filter(PimAttributeDefinition.id == attr_id).first()
            if not attr:
                raise HTTPException(status_code=404, detail="Attribute not found.")
            new_val = not attr.is_label
            if new_val:
                _assert_eligible_id_label(attr.data_type, is_identifier=False, is_label=True)
                self._auto_swap_identifier_label(is_identifier=False, is_label=True, exclude_id=attr_id)
                attr.is_identifier = False  # mutual exclusion
            attr.is_label = new_val
            attr.updated_at = datetime.utcnow()
            db_commit_auto_rollback(db=self.db)
            self.db.refresh(attr)
            return attr
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
            attr = self.db.query(PimAttributeDefinition).filter(PimAttributeDefinition.id == attr_id).first()
            if not attr:
                raise HTTPException(status_code=404, detail="Attribute definition not found.")

            if data.code is not None:
                # Check uniqueness of new code
                dup = (
                    self.db.query(PimAttributeDefinition)
                    .filter(PimAttributeDefinition.code == data.code, PimAttributeDefinition.id != attr_id)
                    .first()
                )
                if dup:
                    raise HTTPException(status_code=409, detail=f"Attribute code '{data.code}' already in use.")
                attr.code = data.code
            if data.label is not None:
                attr.label = data.label
            if data.data_type is not None:
                attr.data_type = data.data_type
            if data.reference_entity_id is not None:
                attr.reference_entity_id = data.reference_entity_id
            if data.description is not None:
                attr.description = data.description
            if data.scope is not None:
                attr.scope = data.scope
            if data.is_localizable is not None:
                attr.is_localizable = data.is_localizable
            if data.level is not None:
                # Validate level against pim_entity_tier codes
                if data.level != "ALL":
                    valid_codes = {t.code for t in self.db.query(PimEntityTier.code).all()}
                    for lvl in data.level.split(","):
                        lvl = lvl.strip()
                        if lvl and lvl != "ALL" and lvl not in valid_codes:
                            raise HTTPException(
                                status_code=400,
                                detail=f"Invalid level '{lvl}'. Valid values: {', '.join(sorted(valid_codes))}, ALL",
                            )
                attr.level = data.level
            if data.is_system is not None:
                attr.is_system = data.is_system
            if data.group is not None:
                attr.group = data.group
            if data.display_order is not None:
                attr.display_order = data.display_order
            # Identifier and Display Name can only be set on ALL-tier attributes
            effective_level = data.level if data.level is not None else attr.level
            if (data.is_identifier or data.is_label) and effective_level and effective_level != "ALL":
                flag_name = "Identifier" if data.is_identifier else "Display Name"
                raise HTTPException(
                    status_code=400,
                    detail=f"{flag_name} can only be set on attributes with level 'ALL' (all tiers). Current level: '{effective_level}'",
                )
            # Validate against the effective type (attr.data_type already reflects
            # any type change applied above).
            _assert_eligible_id_label(attr.data_type, bool(data.is_identifier), bool(data.is_label))
            if data.is_identifier is not None or data.is_label is not None:
                self._auto_swap_identifier_label(
                    is_identifier=bool(data.is_identifier) if data.is_identifier is not None else False,
                    is_label=bool(data.is_label) if data.is_label is not None else False,
                    exclude_id=attr_id,
                )
            if data.is_identifier is not None:
                attr.is_identifier = data.is_identifier
            if data.is_label is not None:
                attr.is_label = data.is_label

            # Reconcile options for SELECT/MULTISELECT when an options list is supplied.
            effective_type = data.data_type if data.data_type is not None else attr.data_type
            if data.options is not None and effective_type in ('SELECT', 'MULTISELECT'):
                self._reconcile_options(attr_id, data.options)

            attr.updated_at = datetime.utcnow()
            db_commit_auto_rollback(db=self.db)
            self.db.refresh(attr)
            return attr

        except HTTPException:
            raise
        except Exception as e:
            message = traceback.format_exc()
            app_logger.exception(f"Unable to update attribute definition. Reason - {message}")
            raise HTTPException(status_code=500, detail=f"Failed to update attribute: {str(e)}")

    # ------------------------------------------------------------------
    # DELETE
    # ------------------------------------------------------------------
    def get_usage_stats(self, attr_id: str):
        """Get usage statistics for an attribute (for delete warning)."""
        cat_count = (
            self.db.query(func.count(func.distinct(PimSpecificationConfig.taxonomy_node_id)))
            .filter(PimSpecificationConfig.attribute_id == attr_id)
            .scalar() or 0
        )
        prod_count = 0
        for VT in VALUE_TABLES:
            cnt = (
                self.db.query(func.count(func.distinct(VT.product_id)))
                .filter(VT.attribute_id == attr_id)
                .scalar() or 0
            )
            prod_count += cnt
        return {"categories_count": cat_count, "products_count": prod_count}

    def get_usage_details(self, attr_id: str):
        """Get detailed usage breakdown: which categories and products use this attribute."""
        # Categories using this attribute (directly bound)
        cat_rows = (
            self.db.query(
                PimSpecificationConfig.taxonomy_node_id,
                PimTaxonomyNode.label,
                PimTaxonomyNode.code,
                PimTaxonomyNode.materialized_path,
                PimSpecificationConfig.is_required,
            )
            .join(PimTaxonomyNode, PimTaxonomyNode.id == PimSpecificationConfig.taxonomy_node_id)
            .filter(PimSpecificationConfig.attribute_id == attr_id)
            .order_by(PimTaxonomyNode.label)
            .all()
        )

        categories = [
            {
                "node_id": r.taxonomy_node_id,
                "label": r.label,
                "code": r.code,
                "path": r.materialized_path,
                "is_required": r.is_required,
            }
            for r in cat_rows
        ]

        # Products with values for this attribute (across all value tables)
        product_ids_with_values = set()
        for VT in VALUE_TABLES:
            rows = (
                self.db.query(VT.product_id)
                .filter(VT.attribute_id == attr_id)
                .distinct()
                .all()
            )
            product_ids_with_values.update(r.product_id for r in rows)

        # Get product details for those IDs
        products = []
        if product_ids_with_values:
            from lakefusion_utility.models.pim import PimEntityFlat
            prod_rows = (
                self.db.query(PimEntityFlat)
                .filter(PimEntityFlat.id.in_(list(product_ids_with_values)))
                .filter(PimEntityFlat.active == True)
                .order_by(PimEntityFlat.identifier)
                .limit(100)
                .all()
            )
            products = [
                {
                    "id": r.id,
                    "identifier": r.identifier,
                    "entity_type_id": r.entity_type_id,
                    "status": r.status,
                    "taxonomy_node_id": r.taxonomy_node_id,
                }
                for r in prod_rows
            ]

        # Assigned products count (products in categories where this attr applies)
        assigned_count = (
            self.db.query(func.count(func.distinct(PimEntity.id)))
            .join(PimResolvedSpecification, PimEntity.taxonomy_node_id == PimResolvedSpecification.taxonomy_node_id)
            .filter(PimResolvedSpecification.attribute_id == attr_id)
            .filter(PimEntity.active == True)
            .scalar() or 0
        )

        filled_count = len(product_ids_with_values)
        coverage_pct = round((filled_count / assigned_count) * 100, 1) if assigned_count > 0 else 0.0

        return {
            "attribute_id": attr_id,
            "categories": categories,
            "categories_count": len(categories),
            "products_with_values": products,
            "products_count": filled_count,
            "assigned_products_count": assigned_count,
            "coverage_pct": coverage_pct,
        }

    def delete_definition(self, attr_id: str):
        try:
            attr = self.db.query(PimAttributeDefinition).filter(PimAttributeDefinition.id == attr_id).first()
            if not attr:
                raise HTTPException(status_code=404, detail="Attribute definition not found.")

            if attr.is_system:
                raise HTTPException(
                    status_code=400,
                    detail=f"Cannot delete system attribute '{attr.code}'. System attributes are protected.",
                )

            # Cascade-delete the attribute's data: deleting an attribute removes it and its
            # values from all products; the UI confirms with a usage count first.
            # pim_value_* tables FK with ON DELETE NO ACTION, so their rows must be cleared
            # explicitly or the attribute delete fails. pim_specification_config FKs ON DELETE
            # CASCADE (auto-removed); pim_resolved_specification is a derived cache with no FK,
            # so clear its stale rows too. All in one transaction.
            # ORDER IS LOAD-BEARING under the option composite FK (fk_pim_vs_option /
            # fk_pim_vm_option, ON DELETE RESTRICT): select/multiselect VALUE rows MUST be
            # deleted BEFORE the attribute. Deleting the attribute cascades to
            # pim_attribute_option (ON DELETE CASCADE); that cascade only succeeds because the
            # referencing value rows are already gone. Do NOT move the attribute delete above
            # the value-table loop.
            for VT in VALUE_TABLES:
                self.db.query(VT).filter(VT.attribute_id == attr_id).delete(synchronize_session=False)
            self.db.query(PimResolvedSpecification).filter(
                PimResolvedSpecification.attribute_id == attr_id
            ).delete(synchronize_session=False)

            self.db.delete(attr)
            db_commit_auto_rollback(db=self.db, raise_exception=True)
            return {"message": f"Attribute definition '{attr.code}' deleted."}

        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Unable to delete attribute definition: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))
