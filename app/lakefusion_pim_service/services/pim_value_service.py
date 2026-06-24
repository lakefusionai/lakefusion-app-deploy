import traceback
from datetime import datetime, date
from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session
from fastapi import HTTPException
from app.lakefusion_pim_service.utils import pim_sql
from lakefusion_utility.models.pim import PimBatchValueWriteRequest
from lakefusion_utility.utils.app_db import db_commit_auto_rollback
from lakefusion_utility.utils.logging_utils import get_logger

app_logger = get_logger(__name__)

# SCRUM-1929 Phase 6.5 — converted from ORM to raw SQL against the Delta-synced
# tables. The 7 typed value tables are routed by data_type via VALUE_TABLE_MAP
# (logical pim_* names, resolved per-entity by pim_sql.pim_tbl). Rows are dicts
# from fetch_*; helpers build INSERT/UPDATE/DELETE explicitly. Synced tables have
# no FK cascade; the option composite-FK RESTRICT is enforced in app code via
# _validate_option_key. entity_name is threaded into the constructor.

# data_type -> logical value table name
VALUE_TABLE_MAP = {
    'TEXT': 'pim_value_text',
    'NUMBER': 'pim_value_number',
    'BOOLEAN': 'pim_value_boolean',
    'DATE': 'pim_value_date',
    'SELECT': 'pim_value_select',
    'MULTISELECT': 'pim_value_multiselect',
    'REFERENCE': 'pim_value_reference',
}
# Tables that store a scalar "value" column vs ref tables
SCALAR_TYPES = {'TEXT', 'NUMBER', 'BOOLEAN', 'DATE'}

ENTITY_TBL = "pim_entity"
ATTR_TBL = "pim_attribute_definition"
OPTION_TBL = "pim_attribute_option"
CONFIG_TBL = "pim_specification_config"
RESOLVED_TBL = "pim_resolved_specification"


class PimValueService:
    def __init__(self, db: Session, entity_name: str):
        self.db = db
        self.entity_name = entity_name
        self._entity = pim_sql.pim_tbl(entity_name, ENTITY_TBL)
        self._attr = pim_sql.pim_tbl(entity_name, ATTR_TBL)
        self._opt = pim_sql.pim_tbl(entity_name, OPTION_TBL)
        self._config = pim_sql.pim_tbl(entity_name, CONFIG_TBL)
        self._resolved = pim_sql.pim_tbl(entity_name, RESOLVED_TBL)

    def _vt(self, data_type):
        """Physical FQN of the value table for a data_type."""
        return pim_sql.pim_tbl(self.entity_name, VALUE_TABLE_MAP[data_type])

    def _fetch_in(self, sql, list_param, values, extra=None):
        # Empty IN-list: short-circuit. An expanding bindparam with [] renders a
        # broken "IN (SELECT CAST(NULL AS INTEGER)...)" that errors on text columns
        # ("operator does not exist: text = integer"). IN () matches nothing anyway.
        values = list(values)
        if not values:
            return []
        stmt = text(sql).bindparams(bindparam(list_param, expanding=True))
        params = {list_param: values}
        if extra:
            params.update(extra)
        return [dict(r) for r in self.db.execute(stmt, params).mappings().all()]

    def _get_product(self, product_id):
        return pim_sql.fetch_one(
            self.db, f'SELECT * FROM {self._entity} WHERE "id" = :id AND "active" = TRUE',
            {"id": product_id},
        )

    def _get_attr(self, attribute_id):
        return pim_sql.fetch_one(
            self.db, f'SELECT * FROM {self._attr} WHERE "id" = :id', {"id": attribute_id}
        )

    # ------------------------------------------------------------------
    # GET VALUES FOR PRODUCT
    # ------------------------------------------------------------------
    def get_values_for_product(self, product_id: str):
        try:
            product = self._get_product(product_id)
            if not product:
                raise HTTPException(status_code=404, detail="Product not found.")

            all_attrs = pim_sql.fetch_all(self.db, f'SELECT * FROM {self._attr}')
            attr_map = {str(a["id"]): a for a in all_attrs}

            option_attr_ids = [a["id"] for a in all_attrs if (a["data_type"] or "").upper() in ("SELECT", "MULTISELECT")]
            opt_label_map = {}
            if option_attr_ids:
                for o in self._fetch_in(
                    f'SELECT "attribute_id","value_key","label" FROM {self._opt} WHERE "attribute_id" IN :ids',
                    "ids", option_attr_ids,
                ):
                    opt_label_map[(o["attribute_id"], o["value_key"])] = o["label"]

            values = []

            for row in pim_sql.fetch_all(self.db, f'SELECT * FROM {self._vt("TEXT")} WHERE "product_id" = :pid', {"pid": product_id}):
                values.append(self._to_response(row, 'TEXT', value=str(row["value"]), attr_map=attr_map))

            for row in pim_sql.fetch_all(self.db, f'SELECT * FROM {self._vt("NUMBER")} WHERE "product_id" = :pid', {"pid": product_id}):
                values.append(self._to_response(
                    row, 'NUMBER', value=str(row["value"]),
                    currency=row["currency"], price_type=row["price_type"], territory=row["territory"],
                    valid_from=str(row["valid_from"]) if row["valid_from"] else None,
                    valid_to=str(row["valid_to"]) if row["valid_to"] else None,
                    tax_rate=float(row["tax_rate"]) if row["tax_rate"] is not None else None,
                    attr_map=attr_map,
                ))

            for row in pim_sql.fetch_all(self.db, f'SELECT * FROM {self._vt("BOOLEAN")} WHERE "product_id" = :pid', {"pid": product_id}):
                values.append(self._to_response(row, 'BOOLEAN', value=str(row["value"]), attr_map=attr_map))

            for row in pim_sql.fetch_all(self.db, f'SELECT * FROM {self._vt("DATE")} WHERE "product_id" = :pid', {"pid": product_id}):
                values.append(self._to_response(row, 'DATE', value=str(row["value"]), attr_map=attr_map))

            for row in pim_sql.fetch_all(self.db, f'SELECT * FROM {self._vt("SELECT")} WHERE "product_id" = :pid', {"pid": product_id}):
                resp = self._to_response(row, 'SELECT', ref_value_key=row["ref_value_key"], attr_map=attr_map)
                resp["ref_value_label"] = opt_label_map.get((row["attribute_id"], row["ref_value_key"]))
                values.append(resp)

            # Multiselect — group by (attribute_id, locale)
            ms_grouped = {}
            for row in pim_sql.fetch_all(self.db, f'SELECT * FROM {self._vt("MULTISELECT")} WHERE "product_id" = :pid', {"pid": product_id}):
                group_key = (row["attribute_id"], row["locale"] or '')
                if group_key not in ms_grouped:
                    attr = attr_map.get(str(row["attribute_id"]))
                    ms_grouped[group_key] = {
                        "id": row["id"], "product_id": row["product_id"], "attribute_id": row["attribute_id"],
                        "attribute_label": attr["label"] if attr else None,
                        "attribute_code": attr["code"] if attr else None,
                        "data_type": "MULTISELECT", "value": None, "ref_value_key": None,
                        "ref_value_keys": [], "ref_value_labels": [], "locale": row["locale"] or '',
                        "currency": '', "price_type": '', "territory": '', "valid_from": None, "valid_to": None,
                        "source": row["source"], "version": row["version"], "changed_by": row["changed_by"],
                    }
                ms_grouped[group_key]["ref_value_keys"].append(row["ref_value_key"])
                ms_grouped[group_key]["ref_value_labels"].append(opt_label_map.get((row["attribute_id"], row["ref_value_key"])))
            values.extend(ms_grouped.values())

            for row in pim_sql.fetch_all(self.db, f'SELECT * FROM {self._vt("REFERENCE")} WHERE "product_id" = :pid', {"pid": product_id}):
                values.append(self._to_response(row, 'REFERENCE', value=row["ref_id"], ref_value_key=row["ref_table"], attr_map=attr_map))

            # Inherited values from ancestors for multi-tier attributes
            own_attr_ids = {v["attribute_id"] for v in values}
            values.extend(self._get_inherited_values(product_id, product["entity_type_id"], own_attr_ids, attr_map))

            # Override annotation: nearest ancestor value for own ALL-level attrs
            ancestors = self._get_ancestor_ids(product_id) if product["parent_id"] else []
            if ancestors:
                own_all_attr_ids = set()
                for v in values:
                    if v.get("inherited"):
                        continue
                    attr = attr_map.get(str(v["attribute_id"]))
                    if attr and (not attr["level"] or attr["level"] == "ALL"):
                        own_all_attr_ids.add(v["attribute_id"])
                if own_all_attr_ids:
                    ancestor_id, ancestor_tier = ancestors[0]  # nearest only
                    for data_type_key in VALUE_TABLE_MAP:
                        rows = self._fetch_in(
                            f'SELECT * FROM {self._vt(data_type_key)} '
                            f'WHERE "product_id" = :pid AND "attribute_id" IN :ids',
                            "ids", list(own_all_attr_ids), {"pid": ancestor_id},
                        )
                        for row in rows:
                            if row["attribute_id"] not in own_all_attr_ids:
                                continue
                            parent_val = str(row["value"]) if "value" in row and row["value"] is not None else row.get("ref_value_key")
                            for v in values:
                                if v["attribute_id"] == row["attribute_id"] and not v.get("inherited"):
                                    v["overridden_from_tier"] = ancestor_tier
                                    v["overridden_from_value"] = parent_val
                            own_all_attr_ids.discard(row["attribute_id"])

            # Completeness — required resolved specs filtered by effective tier
            required_rows = pim_sql.fetch_all(
                self.db,
                f'SELECT r."attribute_id" AS attribute_id, c."level_override" AS level_override, '
                f'a."level" AS level '
                f'FROM {self._resolved} r '
                f'JOIN {self._config} c ON r."config_id" = c."id" '
                f'JOIN {self._attr} a ON r."attribute_id" = a."id" '
                f'WHERE r."taxonomy_node_id" = :nid AND r."is_required" = TRUE',
                {"nid": product["taxonomy_node_id"]},
            )
            required_ids = set()
            for row in required_rows:
                effective_level = (row["level_override"] or row["level"]) or "ALL"
                if effective_level == "ALL" or product["entity_type_id"] in [l.strip() for l in effective_level.split(",")]:
                    required_ids.add(str(row["attribute_id"]))

            total_required = len(required_ids)
            if total_required > 0:
                filled_ids = {str(v["attribute_id"]) for v in values if str(v["attribute_id"]) in required_ids and v.get("value")}
                completeness = round((len(filled_ids) / total_required) * 100, 2)
            else:
                completeness = 100.0

            return {"product_id": product_id, "values": values, "completeness": completeness}

        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Error fetching values for product: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # BATCH WRITE VALUES
    # ------------------------------------------------------------------
    def batch_write_values(self, product_id: str, data: PimBatchValueWriteRequest):
        try:
            product = self._get_product(product_id)
            if not product:
                raise HTTPException(status_code=404, detail="Product not found.")

            results = []
            for val in data.values:
                attr = self._get_attr(val.attribute_id)
                if not attr:
                    raise HTTPException(status_code=404, detail=f"Attribute definition '{val.attribute_id}' not found.")
                if attr["level"] and attr["level"] != "ALL":
                    attr_levels = [l.strip() for l in attr["level"].split(",")]
                    if product["entity_type_id"] not in attr_levels:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Cannot write '{attr['code']}' (level={attr['level']}) on {product['entity_type_id']} entity.",
                        )
                data_type = (attr["data_type"] or "").upper()
                if data_type == 'MULTISELECT':
                    results.extend(self._upsert_multiselect(product_id, val, data_type))
                elif data_type == 'SELECT':
                    results.append(self._upsert_select(product_id, val, data_type))
                elif data_type == 'REFERENCE':
                    results.append(self._upsert_reference(product_id, val, attr))
                else:
                    results.append(self._upsert_scalar(product_id, val, data_type))

            # Refresh denormalized flat row (stages, no commit) — must run before commit
            try:
                from app.lakefusion_pim_service.services.pim_entity_service import PimEntityService
                PimEntityService(self.db, self.entity_name).refresh_flat_rows([product_id])
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
    # BULK UPDATE VALUES
    # ------------------------------------------------------------------
    def bulk_update_values(self, data: dict):
        try:
            product_ids = data.get("product_ids", [])
            attribute_id = data.get("attribute_id")
            value = data.get("value")
            mode = data.get("mode", "fill_blanks")

            if not product_ids or not attribute_id:
                raise HTTPException(status_code=400, detail="product_ids and attribute_id are required.")

            attr = self._get_attr(attribute_id)
            if not attr:
                raise HTTPException(status_code=404, detail=f"Attribute '{attribute_id}' not found.")
            data_type = (attr["data_type"] or "").upper()
            if data_type not in VALUE_TABLE_MAP:
                raise HTTPException(status_code=400, detail=f"Unsupported data type: {data_type}")
            vt = self._vt(data_type)

            attr_levels = None
            if attr["level"] and attr["level"] != "ALL":
                attr_levels = [l.strip() for l in attr["level"].split(",")]

            updated = 0
            skipped = 0
            for pid in product_ids:
                product = self._get_product(pid)
                if not product:
                    skipped += 1
                    continue
                if attr_levels and product["entity_type_id"] not in attr_levels:
                    skipped += 1
                    continue

                existing = pim_sql.fetch_one(
                    self.db, f'SELECT "id","version" FROM {vt} WHERE "product_id" = :pid AND "attribute_id" = :aid',
                    {"pid": pid, "aid": attribute_id},
                )
                if mode == "fill_blanks" and existing:
                    skipped += 1
                    continue

                if existing:
                    set_values = {"source": "USER_SET", "version": (existing["version"] or 0) + 1}
                    if data_type in ('SELECT', 'MULTISELECT'):
                        set_values["ref_value_key"] = value
                    else:
                        set_values["value"] = self._parse_value(value, data_type)
                    sql, params = pim_sql.build_update(
                        self.entity_name, VALUE_TABLE_MAP[data_type], set_values,
                        where='"id" = :id', where_params={"id": existing["id"]},
                    )
                    pim_sql.execute(self.db, sql, params)
                else:
                    row_vals = {"product_id": pid, "attribute_id": attribute_id, "source": "USER_SET", "version": 1}
                    if data_type in ('SELECT', 'MULTISELECT'):
                        row_vals["ref_value_key"] = value
                    else:
                        row_vals["value"] = self._parse_value(value, data_type)
                    sql, params = pim_sql.build_insert(self.entity_name, VALUE_TABLE_MAP[data_type], row_vals)
                    pim_sql.execute(self.db, sql, params)
                updated += 1

            db_commit_auto_rollback(db=self.db)
            return {"message": f"Bulk update complete. {updated} updated, {skipped} skipped.", "updated": updated, "skipped": skipped}

        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Bulk update failed: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Bulk update failed: {str(e)}")

    # ------------------------------------------------------------------
    # DELETE VALUE
    # ------------------------------------------------------------------
    def delete_value(self, product_id: str, attribute_id: str):
        try:
            if not self._get_product(product_id):
                raise HTTPException(status_code=404, detail="Product not found.")
            attr = self._get_attr(attribute_id)
            if not attr:
                raise HTTPException(status_code=404, detail="Attribute definition not found.")
            data_type = (attr["data_type"] or "").upper()
            if data_type not in VALUE_TABLE_MAP:
                raise HTTPException(status_code=400, detail=f"Unknown data type: {data_type}")
            deleted = pim_sql.execute(
                self.db, f'DELETE FROM {self._vt(data_type)} WHERE "product_id" = :pid AND "attribute_id" = :aid',
                {"pid": product_id, "aid": attribute_id},
            )
            db_commit_auto_rollback(db=self.db)
            return {"message": f"Deleted {deleted} value(s) for attribute '{attribute_id}'."}
        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Unable to delete value: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    def delete_price_record(self, product_id: str, attribute_id: str, price_type: str, currency: str, territory: str):
        try:
            deleted = pim_sql.execute(
                self.db,
                f'DELETE FROM {self._vt("NUMBER")} WHERE "product_id" = :pid AND "attribute_id" = :aid '
                f'AND "price_type" = :pt AND "currency" = :cur AND "territory" = :ter',
                {"pid": product_id, "aid": attribute_id, "pt": price_type, "cur": currency, "ter": territory},
            )
            db_commit_auto_rollback(db=self.db)
            return {"message": f"Deleted {deleted} price record(s)."}
        except Exception as e:
            app_logger.exception(f"Unable to delete price record: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    def delete_locale_values(self, product_id: str, locale: str):
        try:
            total = 0
            for data_type in VALUE_TABLE_MAP:
                total += pim_sql.execute(
                    self.db, f'DELETE FROM {self._vt(data_type)} WHERE "product_id" = :pid AND "locale" = :loc',
                    {"pid": product_id, "loc": locale},
                )
            db_commit_auto_rollback(db=self.db)
            return {"message": f"Deleted {total} value(s) for locale '{locale}'."}
        except Exception as e:
            app_logger.exception(f"Unable to delete locale values: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # INTERNAL: upsert helpers (no commit — caller controls txn)
    # ------------------------------------------------------------------
    def _upsert_scalar(self, product_id, val, data_type):
        vt = self._vt(data_type)
        locale = val.locale or ''
        where = '"product_id" = :pid AND "attribute_id" = :aid AND "locale" = :loc'
        wp = {"pid": product_id, "aid": val.attribute_id, "loc": locale}
        if data_type == 'NUMBER':
            where += ' AND "currency" = :cur AND "price_type" = :pt AND "territory" = :ter'
            wp.update({"cur": val.currency or '', "pt": val.price_type or '', "ter": val.territory or ''})

        existing = pim_sql.fetch_one(self.db, f'SELECT * FROM {vt} WHERE {where}', wp)

        if val.value is None or val.value == '':
            if existing:
                pim_sql.execute(self.db, f'DELETE FROM {vt} WHERE "id" = :id', {"id": existing["id"]})
            return {"attribute_id": val.attribute_id, "value": None, "status": "cleared"}

        parsed_value = self._parse_value(val.value, data_type)

        if existing:
            set_values = {
                "value": parsed_value,
                "source": val.source or existing["source"],
                "version": (existing["version"] or 0) + 1,
                "changed_by": val.changed_by,
            }
            if data_type == 'NUMBER':
                set_values["valid_from"] = self._parse_date_or_none(val.valid_from)
                set_values["valid_to"] = self._parse_date_or_none(val.valid_to)
                if val.tax_rate is not None:
                    set_values["tax_rate"] = val.tax_rate
            if data_type == 'TEXT':
                if getattr(val, 'translation_model', None):
                    set_values["translation_model"] = val.translation_model
                    set_values["translation_source_locale"] = getattr(val, 'translation_source_locale', None)
                    set_values["translated_at"] = datetime.utcnow()
                else:
                    set_values["translation_model"] = None
                    set_values["translation_source_locale"] = None
                    set_values["translated_at"] = None
            sql, params = pim_sql.build_update(self.entity_name, VALUE_TABLE_MAP[data_type], set_values, where='"id" = :id', where_params={"id": existing["id"]})
            pim_sql.execute(self.db, sql, params)
            row = pim_sql.fetch_one(self.db, f'SELECT * FROM {vt} WHERE "id" = :id', {"id": existing["id"]})
        else:
            row_vals = {
                "product_id": product_id, "attribute_id": val.attribute_id, "locale": locale,
                "source": val.source or 'USER_SET', "changed_by": val.changed_by, "value": parsed_value,
            }
            if data_type == 'NUMBER':
                row_vals.update({
                    "currency": val.currency or '', "price_type": val.price_type or '', "territory": val.territory or '',
                    "valid_from": self._parse_date_or_none(val.valid_from),
                    "valid_to": self._parse_date_or_none(val.valid_to), "tax_rate": val.tax_rate,
                })
            if data_type == 'TEXT' and getattr(val, 'translation_model', None):
                row_vals["translation_model"] = val.translation_model
                row_vals["translation_source_locale"] = getattr(val, 'translation_source_locale', None)
                row_vals["translated_at"] = datetime.utcnow()
            sql, params = pim_sql.build_insert(self.entity_name, VALUE_TABLE_MAP[data_type], row_vals)
            pim_sql.execute(self.db, sql, params)
            row = pim_sql.fetch_one(self.db, f'SELECT * FROM {vt} WHERE "id" = :id', {"id": params["id"]})

        extra = {}
        if data_type == 'NUMBER':
            extra = dict(
                currency=row["currency"], price_type=row["price_type"], territory=row["territory"],
                valid_from=str(row["valid_from"]) if row["valid_from"] else None,
                valid_to=str(row["valid_to"]) if row["valid_to"] else None,
            )
        return self._to_response(row, data_type, value=str(row["value"]), **extra)

    def _active_option_keys(self, attribute_id):
        return {
            r["value_key"] for r in pim_sql.fetch_all(
                self.db, f'SELECT "value_key" FROM {self._opt} WHERE "attribute_id" = :aid AND "is_active" = TRUE',
                {"aid": attribute_id},
            )
        }

    def _validate_option_key(self, attribute_id, key, valid_keys):
        if key in valid_keys:
            return
        attr = self._get_attr(attribute_id)
        label = attr["label"] if attr else attribute_id
        code = attr["code"] if attr else attribute_id
        raise HTTPException(
            status_code=400,
            detail=f"'{key}' is not a valid option for attribute '{label}' ({code}). Define it as an option first.",
        )

    def _upsert_select(self, product_id, val, data_type):
        vt = self._vt('SELECT')
        locale = val.locale or ''
        self._validate_option_key(val.attribute_id, val.ref_value_key, self._active_option_keys(val.attribute_id))
        existing = pim_sql.fetch_one(
            self.db, f'SELECT * FROM {vt} WHERE "product_id" = :pid AND "attribute_id" = :aid AND "locale" = :loc',
            {"pid": product_id, "aid": val.attribute_id, "loc": locale},
        )
        if existing:
            sql, params = pim_sql.build_update(
                self.entity_name, 'pim_value_select',
                {"ref_value_key": val.ref_value_key, "source": val.source or existing["source"],
                 "version": (existing["version"] or 0) + 1, "changed_by": val.changed_by},
                where='"id" = :id', where_params={"id": existing["id"]},
            )
            pim_sql.execute(self.db, sql, params)
            row_id = existing["id"]
        else:
            sql, params = pim_sql.build_insert(
                self.entity_name, 'pim_value_select',
                {"product_id": product_id, "attribute_id": val.attribute_id, "ref_value_key": val.ref_value_key,
                 "locale": locale, "source": val.source or 'USER_SET', "changed_by": val.changed_by},
            )
            pim_sql.execute(self.db, sql, params)
            row_id = params["id"]
        row = pim_sql.fetch_one(self.db, f'SELECT * FROM {vt} WHERE "id" = :id', {"id": row_id})
        return self._to_response(row, data_type, ref_value_key=row["ref_value_key"])

    def _upsert_multiselect(self, product_id, val, data_type):
        vt = self._vt('MULTISELECT')
        locale = val.locale or ''
        keys = val.ref_value_keys or ([val.ref_value_key] if val.ref_value_key else [])
        valid_keys = self._active_option_keys(val.attribute_id)
        for key in keys:
            self._validate_option_key(val.attribute_id, key, valid_keys)

        pim_sql.execute(
            self.db, f'DELETE FROM {vt} WHERE "product_id" = :pid AND "attribute_id" = :aid AND "locale" = :loc',
            {"pid": product_id, "aid": val.attribute_id, "loc": locale},
        )
        results = []
        for key in keys:
            sql, params = pim_sql.build_insert(
                self.entity_name, 'pim_value_multiselect',
                {"product_id": product_id, "attribute_id": val.attribute_id, "ref_value_key": key,
                 "locale": locale, "source": val.source or 'USER_SET', "changed_by": val.changed_by},
            )
            pim_sql.execute(self.db, sql, params)
            row = pim_sql.fetch_one(self.db, f'SELECT * FROM {vt} WHERE "id" = :id', {"id": params["id"]})
            results.append(self._to_response(row, data_type, ref_value_key=key))
        return results

    def _upsert_reference(self, product_id, val, attr):
        vt = self._vt('REFERENCE')
        locale = val.locale or ''
        ref_table = attr["reference_entity_id"] or ''
        ref_id = val.value or val.ref_value_key or ''
        existing = pim_sql.fetch_one(
            self.db,
            f'SELECT * FROM {vt} WHERE "product_id" = :pid AND "attribute_id" = :aid AND "locale" = :loc '
            f'AND "ref_table" = :rt AND "ref_id" = :rid',
            {"pid": product_id, "aid": val.attribute_id, "loc": locale, "rt": ref_table, "rid": ref_id},
        )
        if existing:
            sql, params = pim_sql.build_update(
                self.entity_name, 'pim_value_reference',
                {"source": val.source or existing["source"], "version": (existing["version"] or 0) + 1,
                 "changed_by": val.changed_by},
                where='"id" = :id', where_params={"id": existing["id"]},
            )
            pim_sql.execute(self.db, sql, params)
            row_id = existing["id"]
        else:
            sql, params = pim_sql.build_insert(
                self.entity_name, 'pim_value_reference',
                {"product_id": product_id, "attribute_id": val.attribute_id, "ref_table": ref_table,
                 "ref_id": ref_id, "locale": locale, "source": val.source or 'USER_SET', "changed_by": val.changed_by},
            )
            pim_sql.execute(self.db, sql, params)
            row_id = params["id"]
        row = pim_sql.fetch_one(self.db, f'SELECT * FROM {vt} WHERE "id" = :id', {"id": row_id})
        return self._to_response(row, 'REFERENCE', value=row["ref_id"], ref_value_key=row["ref_table"])

    # ------------------------------------------------------------------
    # INTERNAL: parsing helpers
    # ------------------------------------------------------------------
    def _parse_value(self, value_str, data_type):
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
        return value_str

    def _parse_date_or_none(self, value_str):
        if not value_str:
            return None
        try:
            return date.fromisoformat(value_str)
        except (ValueError, TypeError):
            return None

    def _get_ancestor_ids(self, entity_id: str) -> list:
        """Walk up parent_id chain → ordered [(parent_id, tier), (grandparent_id, tier), ...]"""
        ancestors = []
        current_id = entity_id
        for _ in range(10):  # max depth safety
            entity = pim_sql.fetch_one(
                self.db, f'SELECT "parent_id" FROM {self._entity} WHERE "id" = :id', {"id": current_id}
            )
            if not entity or not entity["parent_id"]:
                break
            parent = pim_sql.fetch_one(
                self.db, f'SELECT "id","entity_type_id" FROM {self._entity} WHERE "id" = :id',
                {"id": entity["parent_id"]},
            )
            if not parent:
                break
            ancestors.append((parent["id"], parent["entity_type_id"]))
            current_id = parent["id"]
        return ancestors

    def _get_inherited_values(self, product_id: str, entity_type_id: str, own_attr_ids: set, attr_map: dict) -> list:
        multi_tier_attrs = {}
        for attr_id, attr in attr_map.items():
            if not attr["level"] or attr["level"] == "ALL":
                if attr_id not in own_attr_ids:
                    multi_tier_attrs[attr_id] = attr
            elif "," in attr["level"]:
                levels = [l.strip() for l in attr["level"].split(",")]
                if entity_type_id in levels and attr_id not in own_attr_ids:
                    multi_tier_attrs[attr_id] = attr

        if not multi_tier_attrs:
            return []

        ancestors = self._get_ancestor_ids(product_id)
        if not ancestors:
            return []

        inherited = []
        remaining_attrs = set(multi_tier_attrs.keys())
        for ancestor_id, ancestor_tier in ancestors:
            if not remaining_attrs:
                break
            for data_type_key in VALUE_TABLE_MAP:
                rows = self._fetch_in(
                    f'SELECT * FROM {self._vt(data_type_key)} '
                    f'WHERE "product_id" = :pid AND "attribute_id" IN :ids',
                    "ids", list(remaining_attrs), {"pid": ancestor_id},
                )
                if data_type_key == 'MULTISELECT':
                    ms_grouped = {}
                    for row in rows:
                        if row["attribute_id"] not in remaining_attrs:
                            continue
                        ms_grouped.setdefault(row["attribute_id"], []).append(row["ref_value_key"])
                    for attr_id, keys in ms_grouped.items():
                        attr = attr_map.get(str(attr_id))
                        inherited.append({
                            "id": None, "product_id": ancestor_id, "attribute_id": attr_id,
                            "attribute_label": attr["label"] if attr else None,
                            "attribute_code": attr["code"] if attr else None,
                            "data_type": "MULTISELECT", "value": None, "ref_value_key": None,
                            "ref_value_keys": keys, "locale": "", "currency": "", "price_type": "",
                            "territory": "", "valid_from": None, "valid_to": None, "tax_rate": None,
                            "source": "INHERITED", "version": 1, "changed_by": None,
                            "inherited": True, "inherited_from_id": ancestor_id, "inherited_from_tier": ancestor_tier,
                        })
                        remaining_attrs.discard(attr_id)
                    continue

                for row in rows:
                    if row["attribute_id"] not in remaining_attrs:
                        continue
                    if data_type_key == 'SELECT':
                        v_val = None
                        ref_key = row["ref_value_key"]
                    elif data_type_key == 'REFERENCE':
                        v_val = row.get("ref_id")
                        ref_key = row.get("ref_table")
                    else:
                        v_val = str(row["value"]) if row["value"] is not None else None
                        ref_key = None
                    resp = self._to_response(
                        row, data_type_key, value=v_val, ref_value_key=ref_key,
                        currency=row.get("currency", ''), price_type=row.get("price_type", ''),
                        territory=row.get("territory", ''),
                        valid_from=str(row["valid_from"]) if row.get("valid_from") else None,
                        valid_to=str(row["valid_to"]) if row.get("valid_to") else None,
                        tax_rate=float(row["tax_rate"]) if row.get("tax_rate") is not None else None,
                        attr_map=attr_map,
                    )
                    resp["inherited"] = True
                    resp["inherited_from_id"] = ancestor_id
                    resp["inherited_from_tier"] = ancestor_tier
                    inherited.append(resp)
                    remaining_attrs.discard(row["attribute_id"])
        return inherited

    def _to_response(self, row, data_type, value=None, ref_value_key=None,
                     currency='', price_type='', territory='',
                     valid_from=None, valid_to=None, tax_rate=None, attr_map=None):
        attr = (attr_map or {}).get(str(row["attribute_id"]))
        return {
            "id": row["id"], "product_id": row["product_id"], "attribute_id": row["attribute_id"],
            "attribute_label": attr["label"] if attr else None,
            "attribute_code": attr["code"] if attr else None,
            "data_type": data_type, "value": value, "ref_value_key": ref_value_key, "ref_value_keys": None,
            "locale": row.get("locale", '') or '', "currency": currency, "price_type": price_type,
            "territory": territory, "valid_from": valid_from, "valid_to": valid_to, "tax_rate": tax_rate,
            "source": row["source"], "version": row["version"], "changed_by": row["changed_by"],
            "inherited": False, "inherited_from_id": None, "inherited_from_tier": None,
        }
