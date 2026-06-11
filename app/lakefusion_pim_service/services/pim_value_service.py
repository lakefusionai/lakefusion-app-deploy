import traceback
from datetime import datetime, date
from sqlalchemy.orm import Session
from fastapi import HTTPException
from lakefusion_utility.models.pim import (
    PimEntity, PimAttributeDefinition, PimAttributeOption, PimSpecificationConfig,
    PimValueText, PimValueNumber, PimValueBoolean,
    PimValueDate, PimValueSelect, PimValueMultiselect,
    PimValueReference,
    PimBatchValueWriteRequest, PimResolvedSpecification,
)
from lakefusion_utility.utils.app_db import db_commit_auto_rollback
from lakefusion_utility.utils.logging_utils import get_logger

app_logger = get_logger(__name__)

# Route data_type → ORM class
VALUE_TABLE_MAP = {
    'TEXT': PimValueText,
    'NUMBER': PimValueNumber,
    'BOOLEAN': PimValueBoolean,
    'DATE': PimValueDate,
    'SELECT': PimValueSelect,
    'MULTISELECT': PimValueMultiselect,
    'REFERENCE': PimValueReference,
}


class PimValueService:
    def __init__(self, db: Session):
        self.db = db

    # ------------------------------------------------------------------
    # GET VALUES FOR PRODUCT  (SCRUM-1575)
    # ------------------------------------------------------------------
    def get_values_for_product(self, product_id: str):
        try:
            product = self.db.query(PimEntity).filter(
                PimEntity.id == product_id, PimEntity.active == True
            ).first()
            if not product:
                raise HTTPException(status_code=404, detail="Product not found.")

            # Pre-load all attribute definitions for label/code enrichment
            all_attrs = self.db.query(PimAttributeDefinition).all()
            attr_map = {str(a.id): a for a in all_attrs}

            # Pre-load option labels: (attribute_id, value_key) -> label, for SELECT/MULTISELECT display.
            # Only SELECT/MULTISELECT attributes carry options, so scope the query to those attribute
            # ids rather than scanning the whole pim_attribute_option table.
            option_attr_ids = [
                a.id for a in all_attrs if a.data_type.upper() in ("SELECT", "MULTISELECT")
            ]
            opt_label_map = {
                (o.attribute_id, o.value_key): o.label
                for o in (
                    self.db.query(PimAttributeOption)
                    .filter(PimAttributeOption.attribute_id.in_(option_attr_ids))
                    .all()
                    if option_attr_ids else []
                )
            }

            values = []

            # Text
            for row in self.db.query(PimValueText).filter(PimValueText.product_id == product_id).all():
                values.append(self._to_response(row, 'TEXT', value=str(row.value), attr_map=attr_map))

            # Number (includes price qualifiers)
            for row in self.db.query(PimValueNumber).filter(PimValueNumber.product_id == product_id).all():
                values.append(self._to_response(
                    row, 'NUMBER', value=str(row.value),
                    currency=row.currency, price_type=row.price_type,
                    territory=row.territory,
                    valid_from=str(row.valid_from) if row.valid_from else None,
                    valid_to=str(row.valid_to) if row.valid_to else None,
                    tax_rate=float(row.tax_rate) if row.tax_rate is not None else None,
                    attr_map=attr_map,
                ))

            # Boolean
            for row in self.db.query(PimValueBoolean).filter(PimValueBoolean.product_id == product_id).all():
                values.append(self._to_response(row, 'BOOLEAN', value=str(row.value), attr_map=attr_map))

            # Date
            for row in self.db.query(PimValueDate).filter(PimValueDate.product_id == product_id).all():
                values.append(self._to_response(row, 'DATE', value=str(row.value), attr_map=attr_map))

            # Select
            for row in self.db.query(PimValueSelect).filter(PimValueSelect.product_id == product_id).all():
                resp = self._to_response(row, 'SELECT', ref_value_key=row.ref_value_key, attr_map=attr_map)
                resp["ref_value_label"] = opt_label_map.get((row.attribute_id, row.ref_value_key))
                values.append(resp)

            # Multiselect — group by (attribute_id, locale)
            ms_rows = (
                self.db.query(PimValueMultiselect)
                .filter(PimValueMultiselect.product_id == product_id)
                .all()
            )
            ms_grouped = {}
            for row in ms_rows:
                group_key = (row.attribute_id, row.locale or '')
                if group_key not in ms_grouped:
                    attr = attr_map.get(str(row.attribute_id))
                    ms_grouped[group_key] = {
                        "id": row.id,
                        "product_id": row.product_id,
                        "attribute_id": row.attribute_id,
                        "attribute_label": attr.label if attr else None,
                        "attribute_code": attr.code if attr else None,
                        "data_type": "MULTISELECT",
                        "value": None,
                        "ref_value_key": None,
                        "ref_value_keys": [],
                        "ref_value_labels": [],
                        "locale": row.locale or '',
                        "currency": '',
                        "price_type": '',
                        "territory": '',
                        "valid_from": None,
                        "valid_to": None,
                        "source": row.source,
                        "version": row.version,
                        "changed_by": row.changed_by,
                    }
                ms_grouped[group_key]["ref_value_keys"].append(row.ref_value_key)
                ms_grouped[group_key]["ref_value_labels"].append(
                    opt_label_map.get((row.attribute_id, row.ref_value_key))
                )
            values.extend(ms_grouped.values())

            # Reference (links to RDM table rows)
            for row in self.db.query(PimValueReference).filter(PimValueReference.product_id == product_id).all():
                values.append(self._to_response(
                    row, 'REFERENCE',
                    value=row.ref_id,
                    ref_value_key=row.ref_table,
                    attr_map=attr_map,
                ))

            # Add inherited values from ancestors for multi-tier attributes
            own_attr_ids = {v["attribute_id"] for v in values}
            inherited_values = self._get_inherited_values(
                product_id, product.entity_type_id, own_attr_ids, attr_map
            )
            values.extend(inherited_values)

            # For ALL-level attrs where entity has its own value, check if parent also has a value
            # so frontend can show "Clear override" (overridden_from_tier/overridden_from_value)
            ancestors = self._get_ancestor_ids(product_id) if product.parent_id else []
            if ancestors:
                own_all_attr_ids = set()
                for v in values:
                    if v.get("inherited"):
                        continue
                    attr = attr_map.get(str(v["attribute_id"]))
                    if attr and (not attr.level or attr.level == "ALL"):
                        own_all_attr_ids.add(v["attribute_id"])
                if own_all_attr_ids:
                    # Find parent values for these attrs
                    for ancestor_id, ancestor_tier in ancestors:
                        if not own_all_attr_ids:
                            break
                        for VT in VALUE_TABLE_MAP.values():
                            rows = self.db.query(VT).filter(
                                VT.product_id == ancestor_id,
                                VT.attribute_id.in_(own_all_attr_ids)
                            ).all()
                            for row in rows:
                                if row.attribute_id in own_all_attr_ids:
                                    parent_val = str(row.value) if hasattr(row, 'value') else (row.ref_value_key if hasattr(row, 'ref_value_key') else None)
                                    # Annotate the own value
                                    for v in values:
                                        if v["attribute_id"] == row.attribute_id and not v.get("inherited"):
                                            v["overridden_from_tier"] = ancestor_tier
                                            v["overridden_from_value"] = parent_val
                                    own_all_attr_ids.discard(row.attribute_id)
                        break  # Only check nearest ancestor

            # Compute completeness — filtered by tier (matching _compute_completeness logic)
            required_rows = (
                self.db.query(
                    PimResolvedSpecification.attribute_id,
                    PimSpecificationConfig.level_override,
                    PimAttributeDefinition.level,
                )
                .join(PimSpecificationConfig, PimResolvedSpecification.config_id == PimSpecificationConfig.id)
                .join(PimAttributeDefinition, PimResolvedSpecification.attribute_id == PimAttributeDefinition.id)
                .filter(
                    PimResolvedSpecification.taxonomy_node_id == product.taxonomy_node_id,
                    PimResolvedSpecification.is_required == True,
                )
                .all()
            )
            # Filter by effective level matching product's tier
            required_ids = set()
            for row in required_rows:
                effective_level = (row.level_override if row.level_override else row.level) or "ALL"
                if effective_level == "ALL" or product.entity_type_id in [l.strip() for l in effective_level.split(",")]:
                    required_ids.add(str(row.attribute_id))

            total_required = len(required_ids)
            if total_required > 0:
                filled_ids = {str(v["attribute_id"]) for v in values if str(v["attribute_id"]) in required_ids and v.get("value")}
                completeness = round((len(filled_ids) / total_required) * 100, 2)
            else:
                completeness = 100.0

            return {
                "product_id": product_id,
                "values": values,
                "completeness": completeness,
            }

        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Error fetching values for product: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # BATCH WRITE VALUES  (SCRUM-1575)
    # ------------------------------------------------------------------
    def batch_write_values(self, product_id: str, data: PimBatchValueWriteRequest):
        try:
            product = self.db.query(PimEntity).filter(
                PimEntity.id == product_id, PimEntity.active == True
            ).first()
            if not product:
                raise HTTPException(status_code=404, detail="Product not found.")

            results = []

            for val in data.values:
                # Look up the attribute to determine data_type
                attr = self.db.query(PimAttributeDefinition).filter(
                    PimAttributeDefinition.id == val.attribute_id
                ).first()
                if not attr:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Attribute definition '{val.attribute_id}' not found.",
                    )

                # Enforce tier: attribute level must match product's entity_type_id
                if attr.level and attr.level != "ALL":
                    attr_levels = [l.strip() for l in attr.level.split(",")]
                    if product.entity_type_id not in attr_levels:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Cannot write '{attr.code}' (level={attr.level}) on {product.entity_type_id} entity.",
                        )

                data_type = attr.data_type.upper()

                if data_type == 'MULTISELECT':
                    results.extend(
                        self._upsert_multiselect(product_id, val, data_type)
                    )
                elif data_type == 'SELECT':
                    results.append(
                        self._upsert_select(product_id, val, data_type)
                    )
                elif data_type == 'REFERENCE':
                    results.append(
                        self._upsert_reference(product_id, val, attr)
                    )
                else:
                    results.append(
                        self._upsert_scalar(product_id, val, data_type)
                    )

            # Refresh the denormalized flat row so the catalog list reflects the edit.
            # batch_write_values only writes EAV (pim_value_*); the catalog list reads from
            # pim_entity_flat, which would otherwise stay stale until the next full rebuild.
            # refresh_flat_rows stages changes but does not commit, so it must run BEFORE the
            # commit below — both the value writes and the flat update persist in one transaction.
            try:
                from app.lakefusion_pim_service.services.pim_entity_service import PimEntityService
                PimEntityService(self.db).refresh_flat_rows([product_id])
            except Exception:
                app_logger.exception(f"Flat-row refresh failed after value write for product {product_id}")

            db_commit_auto_rollback(db=self.db)
            return {"product_id": product_id, "values": results, "completeness": None}

        except HTTPException:
            raise
        except Exception as e:
            message = traceback.format_exc()
            app_logger.exception(f"Unable to batch write values. Reason - {message}")
            raise HTTPException(status_code=500, detail=f"Failed to write values: {str(e)}")

    # ------------------------------------------------------------------
    # BULK UPDATE VALUES  (SCRUM-1576)
    # ------------------------------------------------------------------
    def bulk_update_values(self, data: dict):
        """
        Bulk update attribute values across multiple products.
        Expects: { product_ids: [...], attribute_id: str, value: str, mode: "fill_blanks"|"overwrite" }
        """
        try:
            product_ids = data.get("product_ids", [])
            attribute_id = data.get("attribute_id")
            value = data.get("value")
            mode = data.get("mode", "fill_blanks")

            if not product_ids or not attribute_id:
                raise HTTPException(status_code=400, detail="product_ids and attribute_id are required.")

            attr = self.db.query(PimAttributeDefinition).filter(
                PimAttributeDefinition.id == attribute_id
            ).first()
            if not attr:
                raise HTTPException(status_code=404, detail=f"Attribute '{attribute_id}' not found.")

            data_type = attr.data_type.upper()
            ValueTable = VALUE_TABLE_MAP.get(data_type)
            if not ValueTable:
                raise HTTPException(status_code=400, detail=f"Unsupported data type: {data_type}")

            # Pre-compute tier check for the attribute
            attr_levels = None
            if attr.level and attr.level != "ALL":
                attr_levels = [l.strip() for l in attr.level.split(",")]

            updated = 0
            skipped = 0

            for pid in product_ids:
                product = self.db.query(PimEntity).filter(
                    PimEntity.id == pid, PimEntity.active == True
                ).first()
                if not product:
                    skipped += 1
                    continue

                # Enforce tier: skip products whose tier doesn't match
                if attr_levels and product.entity_type_id not in attr_levels:
                    skipped += 1
                    continue

                existing = self.db.query(ValueTable).filter(
                    ValueTable.product_id == pid,
                    ValueTable.attribute_id == attribute_id,
                ).first()

                if mode == "fill_blanks" and existing:
                    skipped += 1
                    continue

                if existing:
                    if data_type in ('SELECT',):
                        existing.ref_value_key = value
                    elif data_type in ('MULTISELECT',):
                        pass  # multiselect handled separately
                    else:
                        existing.value = self._parse_value(value, data_type)
                    existing.source = 'USER_SET'
                    existing.version = existing.version + 1
                    updated += 1
                else:
                    new_row = ValueTable(
                        product_id=pid,
                        attribute_id=attribute_id,
                        source='USER_SET',
                        version=1,
                    )
                    if data_type in ('SELECT',):
                        new_row.ref_value_key = value
                    elif data_type in ('MULTISELECT',):
                        new_row.ref_value_key = value
                    else:
                        new_row.value = self._parse_value(value, data_type)
                    self.db.add(new_row)
                    updated += 1

            db_commit_auto_rollback(db=self.db)
            return {
                "message": f"Bulk update complete. {updated} updated, {skipped} skipped.",
                "updated": updated,
                "skipped": skipped,
            }

        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Bulk update failed: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Bulk update failed: {str(e)}")

    # ------------------------------------------------------------------
    # DELETE VALUE  (SCRUM-1575)
    # ------------------------------------------------------------------
    def delete_value(self, product_id: str, attribute_id: str):
        try:
            product = self.db.query(PimEntity).filter(
                PimEntity.id == product_id, PimEntity.active == True
            ).first()
            if not product:
                raise HTTPException(status_code=404, detail="Product not found.")

            attr = self.db.query(PimAttributeDefinition).filter(
                PimAttributeDefinition.id == attribute_id
            ).first()
            if not attr:
                raise HTTPException(status_code=404, detail="Attribute definition not found.")

            data_type = attr.data_type.upper()
            ValueTable = VALUE_TABLE_MAP.get(data_type)
            if not ValueTable:
                raise HTTPException(status_code=400, detail=f"Unknown data type: {data_type}")

            deleted = (
                self.db.query(ValueTable)
                .filter(
                    ValueTable.product_id == product_id,
                    ValueTable.attribute_id == attribute_id,
                )
                .delete(synchronize_session='fetch')
            )

            db_commit_auto_rollback(db=self.db)
            return {"message": f"Deleted {deleted} value(s) for attribute '{attribute_id}'."}

        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Unable to delete value: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    def delete_price_record(self, product_id: str, attribute_id: str, price_type: str, currency: str, territory: str):
        """Delete a specific price record by composite key."""
        try:
            deleted = (
                self.db.query(PimValueNumber)
                .filter(
                    PimValueNumber.product_id == product_id,
                    PimValueNumber.attribute_id == attribute_id,
                    PimValueNumber.price_type == price_type,
                    PimValueNumber.currency == currency,
                    PimValueNumber.territory == territory,
                )
                .delete(synchronize_session='fetch')
            )
            db_commit_auto_rollback(db=self.db)
            return {"message": f"Deleted {deleted} price record(s)."}
        except Exception as e:
            app_logger.exception(f"Unable to delete price record: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    def delete_locale_values(self, product_id: str, locale: str):
        """Delete all values for a product in a specific locale (across all value tables)."""
        try:
            total = 0
            for VT in VALUE_TABLE_MAP.values():
                if not hasattr(VT, 'locale'):
                    continue
                deleted = (
                    self.db.query(VT)
                    .filter(VT.product_id == product_id, VT.locale == locale)
                    .delete(synchronize_session='fetch')
                )
                total += deleted
            db_commit_auto_rollback(db=self.db)
            return {"message": f"Deleted {total} value(s) for locale '{locale}'."}
        except Exception as e:
            app_logger.exception(f"Unable to delete locale values: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # INTERNAL: upsert helpers
    # ------------------------------------------------------------------
    def _upsert_scalar(self, product_id, val, data_type):
        """Upsert a scalar value (TEXT, NUMBER, BOOLEAN, DATE).
        If value is None or empty string, delete the existing row (clear the value)."""
        ValueTable = VALUE_TABLE_MAP[data_type]
        locale = val.locale or ''

        # Build filter — unique key includes locale; NUMBER adds price qualifiers
        filters = [
            ValueTable.product_id == product_id,
            ValueTable.attribute_id == val.attribute_id,
            ValueTable.locale == locale,
        ]
        if data_type == 'NUMBER':
            filters.append(ValueTable.currency == (val.currency or ''))
            filters.append(ValueTable.price_type == (val.price_type or ''))
            filters.append(ValueTable.territory == (val.territory or ''))

        existing = self.db.query(ValueTable).filter(*filters).first()

        # Null/empty value = clear (delete the row)
        if val.value is None or val.value == '':
            if existing:
                self.db.delete(existing)
                self.db.flush()
            return {"attribute_id": val.attribute_id, "value": None, "status": "cleared"}

        parsed_value = self._parse_value(val.value, data_type)

        if existing:
            existing.value = parsed_value
            existing.source = val.source or existing.source
            existing.version = existing.version + 1
            existing.changed_by = val.changed_by
            existing.updated_at = datetime.utcnow()
            if data_type == 'NUMBER':
                existing.valid_from = self._parse_date_or_none(val.valid_from)
                existing.valid_to = self._parse_date_or_none(val.valid_to)
                if val.tax_rate is not None:
                    existing.tax_rate = val.tax_rate
            # Translation provenance (TEXT only)
            if data_type == 'TEXT' and hasattr(existing, 'translation_model'):
                if getattr(val, 'translation_model', None):
                    existing.translation_model = val.translation_model
                    existing.translation_source_locale = getattr(val, 'translation_source_locale', None)
                    existing.translated_at = datetime.utcnow()
                else:
                    # Manual edit clears provenance
                    existing.translation_model = None
                    existing.translation_source_locale = None
                    existing.translated_at = None
            row = existing
        else:
            kwargs = dict(
                product_id=product_id,
                attribute_id=val.attribute_id,
                locale=locale,
                source=val.source or 'USER_SET',
                changed_by=val.changed_by,
            )
            if data_type == 'NUMBER':
                kwargs.update(
                    currency=val.currency or '',
                    price_type=val.price_type or '',
                    territory=val.territory or '',
                    valid_from=self._parse_date_or_none(val.valid_from),
                    valid_to=self._parse_date_or_none(val.valid_to),
                    tax_rate=val.tax_rate,
                )
            # Translation provenance (TEXT only)
            if data_type == 'TEXT' and getattr(val, 'translation_model', None):
                kwargs['translation_model'] = val.translation_model
                kwargs['translation_source_locale'] = getattr(val, 'translation_source_locale', None)
                kwargs['translated_at'] = datetime.utcnow()
            row = ValueTable(**kwargs)
            row.value = parsed_value
            self.db.add(row)

        self.db.flush()

        # Build response with NUMBER-specific fields when applicable
        extra = {}
        if data_type == 'NUMBER':
            extra = dict(
                currency=row.currency, price_type=row.price_type,
                territory=row.territory,
                valid_from=str(row.valid_from) if row.valid_from else None,
                valid_to=str(row.valid_to) if row.valid_to else None,
            )
        return self._to_response(row, data_type, value=str(row.value), **extra)

    def _active_option_keys(self, attribute_id):
        """Return the set of active option value_keys for an attribute."""
        return {
            r.value_key for r in
            self.db.query(PimAttributeOption.value_key)
            .filter(
                PimAttributeOption.attribute_id == attribute_id,
                PimAttributeOption.is_active == True,
            ).all()
        }

    def _validate_option_key(self, attribute_id, key, valid_keys):
        """Strict validation: reject a select/multiselect value whose key has no defined,
        active option. Applies to ALL sources (UI/import/pipeline) — no auto-create. The
        composite FK (fk_pim_vs_option / fk_pim_vm_option) is the DB backstop; this is the
        friendly, named error surface."""
        if key in valid_keys:
            return
        attr = self.db.query(PimAttributeDefinition).filter(
            PimAttributeDefinition.id == attribute_id
        ).first()
        label = attr.label if attr else attribute_id
        code = attr.code if attr else attribute_id
        raise HTTPException(
            status_code=400,
            detail=f"'{key}' is not a valid option for attribute '{label}' ({code}). "
                   f"Define it as an option first.",
        )

    def _upsert_select(self, product_id, val, data_type):
        """Upsert a SELECT value."""
        locale = val.locale or ''
        self._validate_option_key(
            val.attribute_id, val.ref_value_key, self._active_option_keys(val.attribute_id)
        )
        existing = (
            self.db.query(PimValueSelect)
            .filter(
                PimValueSelect.product_id == product_id,
                PimValueSelect.attribute_id == val.attribute_id,
                PimValueSelect.locale == locale,
            )
            .first()
        )

        if existing:
            existing.ref_value_key = val.ref_value_key
            existing.source = val.source or existing.source
            existing.version = existing.version + 1
            existing.changed_by = val.changed_by
            existing.updated_at = datetime.utcnow()
            row = existing
        else:
            row = PimValueSelect(
                product_id=product_id,
                attribute_id=val.attribute_id,
                ref_value_key=val.ref_value_key,
                locale=locale,
                source=val.source or 'USER_SET',
                changed_by=val.changed_by,
            )
            self.db.add(row)

        self.db.flush()
        return self._to_response(row, data_type, ref_value_key=row.ref_value_key)

    def _upsert_multiselect(self, product_id, val, data_type):
        """Replace-all for MULTISELECT: delete existing, insert new keys."""
        locale = val.locale or ''
        keys = val.ref_value_keys or ([val.ref_value_key] if val.ref_value_key else [])
        # Validate every key BEFORE deleting existing rows (reject as a unit; no partial write).
        valid_keys = self._active_option_keys(val.attribute_id)
        for key in keys:
            self._validate_option_key(val.attribute_id, key, valid_keys)

        # Delete existing rows for this product+attribute+locale
        self.db.query(PimValueMultiselect).filter(
            PimValueMultiselect.product_id == product_id,
            PimValueMultiselect.attribute_id == val.attribute_id,
            PimValueMultiselect.locale == locale,
        ).delete(synchronize_session='fetch')

        results = []
        for key in keys:
            row = PimValueMultiselect(
                product_id=product_id,
                attribute_id=val.attribute_id,
                ref_value_key=key,
                locale=locale,
                source=val.source or 'USER_SET',
                changed_by=val.changed_by,
            )
            self.db.add(row)
            self.db.flush()
            results.append(self._to_response(row, data_type, ref_value_key=key))

        return results

    def _upsert_reference(self, product_id, val, attr):
        """Upsert a REFERENCE value (links to an RDM table row)."""
        locale = val.locale or ''
        ref_table = attr.reference_entity_id or ''
        ref_id = val.value or val.ref_value_key or ''

        existing = (
            self.db.query(PimValueReference)
            .filter(
                PimValueReference.product_id == product_id,
                PimValueReference.attribute_id == val.attribute_id,
                PimValueReference.locale == locale,
                PimValueReference.ref_table == ref_table,
                PimValueReference.ref_id == ref_id,
            )
            .first()
        )

        if existing:
            existing.source = val.source or existing.source
            existing.version = existing.version + 1
            existing.changed_by = val.changed_by
            existing.updated_at = datetime.utcnow()
            row = existing
        else:
            row = PimValueReference(
                product_id=product_id,
                attribute_id=val.attribute_id,
                ref_table=ref_table,
                ref_id=ref_id,
                locale=locale,
                source=val.source or 'USER_SET',
                changed_by=val.changed_by,
            )
            self.db.add(row)

        self.db.flush()
        return self._to_response(row, 'REFERENCE', value=row.ref_id, ref_value_key=row.ref_table)

    # ------------------------------------------------------------------
    # INTERNAL: parsing helpers
    # ------------------------------------------------------------------
    def _parse_value(self, value_str, data_type):
        """Parse a string value to the correct Python type."""
        if value_str is None:
            return None
        if data_type == 'NUMBER':
            try:
                return float(value_str)
            except (ValueError, TypeError):
                raise HTTPException(status_code=400, detail=f"Invalid number value: {value_str}")
        elif data_type == 'BOOLEAN':
            return value_str.lower() in ('true', '1', 'yes')
        elif data_type == 'DATE':
            try:
                return date.fromisoformat(value_str)
            except (ValueError, TypeError):
                raise HTTPException(status_code=400, detail=f"Invalid date value: {value_str}. Use YYYY-MM-DD.")
        return value_str  # TEXT

    def _parse_date_or_none(self, value_str):
        """Parse a date string or return None."""
        if not value_str:
            return None
        try:
            return date.fromisoformat(value_str)
        except (ValueError, TypeError):
            return None

    def _get_ancestor_ids(self, entity_id: str) -> list:
        """Walk up parent_id chain, return ordered list [(parent_id, tier), (grandparent_id, tier), ...]"""
        ancestors = []
        current_id = entity_id
        for _ in range(10):  # max depth safety
            entity = self.db.query(PimEntity.parent_id, PimEntity.entity_type_id).filter(PimEntity.id == current_id).first()
            if not entity or not entity.parent_id:
                break
            parent = self.db.query(PimEntity.id, PimEntity.entity_type_id).filter(PimEntity.id == entity.parent_id).first()
            if not parent:
                break
            ancestors.append((parent.id, parent.entity_type_id))
            current_id = parent.id
        return ancestors

    def _get_inherited_values(self, product_id: str, entity_type_id: str, own_attr_ids: set, attr_map: dict) -> list:
        """For multi-tier attributes where the entity has no own value, find the nearest ancestor's value."""
        # Identify multi-tier attributes that the entity doesn't have a value for
        multi_tier_attrs = {}
        for attr_id, attr in attr_map.items():
            if not attr.level or attr.level == "ALL":
                # ALL-level: inherits from parent, override available at any tier
                if attr_id not in own_attr_ids:
                    multi_tier_attrs[attr_id] = attr
            elif "," in attr.level:
                # Multi-tier: check if this entity's tier is in the list
                levels = [l.strip() for l in attr.level.split(",")]
                if entity_type_id in levels and attr_id not in own_attr_ids:
                    multi_tier_attrs[attr_id] = attr

        if not multi_tier_attrs:
            return []

        # Walk up ancestor chain
        ancestors = self._get_ancestor_ids(product_id)
        if not ancestors:
            return []

        inherited = []
        remaining_attrs = set(multi_tier_attrs.keys())

        for ancestor_id, ancestor_tier in ancestors:
            if not remaining_attrs:
                break
            # Batch query ancestor values for remaining attributes across typed value tables
            for data_type_key, VT in VALUE_TABLE_MAP.items():
                rows = (
                    self.db.query(VT)
                    .filter(VT.product_id == ancestor_id, VT.attribute_id.in_(remaining_attrs))
                    .all()
                )
                # Skip multiselect — handled separately below
                if data_type_key == 'MULTISELECT':
                    # Group multiselect rows by attribute_id
                    ms_grouped = {}
                    for row in rows:
                        if row.attribute_id not in remaining_attrs:
                            continue
                        if row.attribute_id not in ms_grouped:
                            ms_grouped[row.attribute_id] = []
                        ms_grouped[row.attribute_id].append(row.ref_value_key)
                    for attr_id, keys in ms_grouped.items():
                        attr = attr_map.get(str(attr_id))
                        inherited.append({
                            "id": None, "product_id": ancestor_id, "attribute_id": attr_id,
                            "attribute_label": attr.label if attr else None,
                            "attribute_code": attr.code if attr else None,
                            "data_type": "MULTISELECT", "value": None,
                            "ref_value_key": None, "ref_value_keys": keys,
                            "locale": "", "currency": "", "price_type": "", "territory": "",
                            "valid_from": None, "valid_to": None, "tax_rate": None,
                            "source": "INHERITED", "version": 1, "changed_by": None,
                            "inherited": True, "inherited_from_id": ancestor_id, "inherited_from_tier": ancestor_tier,
                        })
                        remaining_attrs.discard(attr_id)
                    continue

                for row in rows:
                    if row.attribute_id not in remaining_attrs:
                        continue
                    attr = attr_map.get(str(row.attribute_id))
                    # Extract value based on table type
                    if data_type_key == 'SELECT':
                        val = None
                        ref_key = row.ref_value_key
                    elif data_type_key == 'REFERENCE':
                        val = row.ref_id if hasattr(row, 'ref_id') else None
                        ref_key = row.ref_table if hasattr(row, 'ref_table') else None
                    else:
                        val = str(row.value) if row.value is not None else None
                        ref_key = None

                    resp = self._to_response(
                        row, data_type_key,
                        value=val,
                        ref_value_key=ref_key,
                        currency=getattr(row, 'currency', ''),
                        price_type=getattr(row, 'price_type', ''),
                        territory=getattr(row, 'territory', ''),
                        valid_from=str(row.valid_from) if hasattr(row, 'valid_from') and row.valid_from else None,
                        valid_to=str(row.valid_to) if hasattr(row, 'valid_to') and row.valid_to else None,
                        tax_rate=float(row.tax_rate) if hasattr(row, 'tax_rate') and row.tax_rate is not None else None,
                        attr_map=attr_map,
                    )
                    resp["inherited"] = True
                    resp["inherited_from_id"] = ancestor_id
                    resp["inherited_from_tier"] = ancestor_tier
                    inherited.append(resp)
                    remaining_attrs.discard(row.attribute_id)

        return inherited

    def _to_response(self, row, data_type, value=None, ref_value_key=None,
                     currency='', price_type='', territory='',
                     valid_from=None, valid_to=None, tax_rate=None, attr_map=None):
        """Convert a value table row to a unified response dict."""
        attr = (attr_map or {}).get(str(row.attribute_id))
        return {
            "id": row.id,
            "product_id": row.product_id,
            "attribute_id": row.attribute_id,
            "attribute_label": attr.label if attr else None,
            "attribute_code": attr.code if attr else None,
            "data_type": data_type,
            "value": value,
            "ref_value_key": ref_value_key,
            "ref_value_keys": None,
            "locale": getattr(row, 'locale', '') or '',
            "currency": currency,
            "price_type": price_type,
            "territory": territory,
            "valid_from": valid_from,
            "valid_to": valid_to,
            "tax_rate": tax_rate,
            "source": row.source,
            "version": row.version,
            "changed_by": row.changed_by,
            "inherited": False,
            "inherited_from_id": None,
            "inherited_from_tier": None,
        }
