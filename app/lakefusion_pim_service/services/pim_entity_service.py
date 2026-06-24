import traceback
import uuid
from datetime import datetime, date as _date
from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session
from fastapi import HTTPException
from app.lakefusion_pim_service.utils import pim_sql
from lakefusion_utility.models.pim import (
    PimEntityCreate, PimEntityUpdate,
    PimImportNode, PimBulkImportRequest, PimFlatImportRequest,
    to_value_key,
)
from lakefusion_utility.utils.app_db import db_commit_auto_rollback
from lakefusion_utility.utils.logging_utils import get_logger

app_logger = get_logger(__name__)

# SCRUM-1929 Phase 6.6 — converted from ORM to raw SQL against the Delta-synced
# tables. Largest PIM service. Synced tables have NO FK cascade — delete_product
# clears value rows + flat row explicitly. flat_import/bulk_import keep their
# chunked-insert shape but write via pim_sql; in-place value updates become
# explicit UPDATEs. entity_name threaded into the constructor.
#
# NOTE (perf): flat_import / bulk_import_hierarchy run row-wise in this service
# process. At million-row scale these should move to vectorized Spark (see PRD
# Scale Considerations); this port preserves behavior, not scale.

ENTITY_TBL = "pim_entity"
FLAT_TBL = "pim_entity_flat"
NODE_TBL = "pim_taxonomy_node"
ATTR_TBL = "pim_attribute_definition"
OPTION_TBL = "pim_attribute_option"
CONFIG_TBL = "pim_specification_config"
RESOLVED_TBL = "pim_resolved_specification"
TIER_TBL = "pim_entity_tier"

# data_type -> logical value table
VALUE_TABLE_BY_TYPE = {
    'TEXT': 'pim_value_text', 'NUMBER': 'pim_value_number', 'BOOLEAN': 'pim_value_boolean',
    'DATE': 'pim_value_date', 'SELECT': 'pim_value_select',
    'MULTISELECT': 'pim_value_multiselect', 'REFERENCE': 'pim_value_reference',
}
VALUE_TABLES = list(VALUE_TABLE_BY_TYPE.values())
VALUE_TEXT = 'pim_value_text'
VALUE_NUMBER = 'pim_value_number'


class PimEntityService:
    def __init__(self, db: Session, entity_name: str):
        self.db = db
        self.entity_name = entity_name
        self._entity = pim_sql.pim_tbl(entity_name, ENTITY_TBL)
        self._flat = pim_sql.pim_tbl(entity_name, FLAT_TBL)
        self._node = pim_sql.pim_tbl(entity_name, NODE_TBL)
        self._attr = pim_sql.pim_tbl(entity_name, ATTR_TBL)
        self._opt = pim_sql.pim_tbl(entity_name, OPTION_TBL)
        self._config = pim_sql.pim_tbl(entity_name, CONFIG_TBL)
        self._resolved = pim_sql.pim_tbl(entity_name, RESOLVED_TBL)
        self._tier = pim_sql.pim_tbl(entity_name, TIER_TBL)
        self._identifier_attr = None
        self._label_attr = None

    def _vt(self, data_type):
        return pim_sql.pim_tbl(self.entity_name, VALUE_TABLE_BY_TYPE[data_type])

    def _t(self, table):
        return pim_sql.pim_tbl(self.entity_name, table)

    def _fetch_in(self, sql, list_param, values, extra=None):
        # Empty IN-list → short-circuit (an expanding bindparam with [] renders a
        # broken empty-IN that errors on text columns; IN () matches nothing anyway).
        # Also correct for UPDATE/DELETE ... IN :ids RETURNING (affects no rows).
        values = list(values)
        if not values:
            return []
        stmt = text(sql).bindparams(bindparam(list_param, expanding=True))
        params = {list_param: values}
        if extra:
            params.update(extra)
        return [dict(r) for r in self.db.execute(stmt, params).mappings().all()]

    # ------------------------------------------------------------------
    # Identifier / label attribute helpers
    # ------------------------------------------------------------------
    def _get_identifier_attr(self):
        if self._identifier_attr is None:
            self._identifier_attr = pim_sql.fetch_one(
                self.db, f'SELECT * FROM {self._attr} WHERE "is_identifier" = TRUE'
            )
        return self._identifier_attr

    def _get_label_attr(self):
        if self._label_attr is None:
            self._label_attr = pim_sql.fetch_one(
                self.db, f'SELECT * FROM {self._attr} WHERE "is_label" = TRUE'
            )
        return self._label_attr

    def _get_text_value(self, entity_id, attr_id):
        if not attr_id:
            return None
        row = pim_sql.fetch_one(
            self.db,
            f'SELECT "value" FROM {self._vt("TEXT")} '
            f'WHERE "product_id" = :pid AND "attribute_id" = :aid AND "locale" = \'\'',
            {"pid": entity_id, "aid": attr_id},
        )
        return row["value"] if row else None

    def _get_identifier_value(self, entity_id):
        attr = self._get_identifier_attr()
        return self._get_text_value(entity_id, attr["id"]) if attr else None

    def _get_label_value(self, entity_id):
        attr = self._get_label_attr()
        return self._get_text_value(entity_id, attr["id"]) if attr else None

    def _write_text_value(self, entity_id, attr_id, value):
        """Upsert a plain-locale text value (used for identifier/label)."""
        if not attr_id or not value:
            return
        existing = pim_sql.fetch_one(
            self.db,
            f'SELECT "id" FROM {self._vt("TEXT")} '
            f'WHERE "product_id" = :pid AND "attribute_id" = :aid AND "locale" = \'\'',
            {"pid": entity_id, "aid": attr_id},
        )
        if existing:
            sql, params = pim_sql.build_update(
                self.entity_name, VALUE_TEXT, {"value": value},
                where='"id" = :id', where_params={"id": existing["id"]},
            )
            pim_sql.execute(self.db, sql, params)
        else:
            sql, params = pim_sql.build_insert(
                self.entity_name, VALUE_TEXT,
                {"product_id": entity_id, "attribute_id": attr_id, "value": value,
                 "locale": '', "source": 'USER_SET'},
            )
            pim_sql.execute(self.db, sql, params)

    def _write_identifier_value(self, entity_id, value):
        attr = self._get_identifier_attr()
        if attr:
            self._write_text_value(entity_id, attr["id"], value)

    def _write_label_value(self, entity_id, value):
        attr = self._get_label_attr()
        if attr:
            self._write_text_value(entity_id, attr["id"], value)

    def _check_identifier_unique(self, value, exclude_id=None):
        attr = self._get_identifier_attr()
        if not attr or not value:
            return
        sql = (f'SELECT "product_id" FROM {self._vt("TEXT")} '
               f'WHERE "attribute_id" = :aid AND "value" = :v AND "locale" = \'\'')
        params = {"aid": attr["id"], "v": value}
        if exclude_id:
            sql += ' AND "product_id" != :eid'
            params["eid"] = exclude_id
        if pim_sql.fetch_one(self.db, sql, params):
            raise HTTPException(status_code=409, detail=f"Product with identifier '{value}' already exists.")

    # ------------------------------------------------------------------
    # FLAT TABLE REFRESH
    # ------------------------------------------------------------------
    def refresh_flat_rows(self, entity_ids: list):
        """Refresh pim_entity_flat for specific entities (no commit).

        Set-based for scale (the reference-entity batching philosophy): a handful of
        bulk SELECTs into in-memory maps + one delete + one batched insert, instead
        of ~6 round-trips per entity. Each flat row is fully recomputed, so we
        delete-then-insert rather than per-row upsert."""
        if not entity_ids:
            return
        ids = list(dict.fromkeys(entity_ids))  # de-dup, preserve order
        id_attr = self._get_identifier_attr()
        label_attr = self._get_label_attr()
        id_attr_id = id_attr["id"] if id_attr else None
        label_attr_id = label_attr["id"] if label_attr else None

        # 1. Bulk-fetch the entities.
        entities = self._fetch_in(f'SELECT * FROM {self._entity} WHERE "id" IN :ids', "ids", ids)
        entity_by_id = {e["id"]: e for e in entities}

        # 2. Identifier + label text values for all entities (one query each).
        def _text_map(attr_id):
            if not attr_id:
                return {}
            rows = self._fetch_in(
                f'SELECT "product_id","value" FROM {self._vt("TEXT")} '
                f'WHERE "attribute_id" = :aid AND "locale" = \'\' AND "product_id" IN :ids',
                "ids", ids, {"aid": attr_id},
            )
            return {r["product_id"]: r["value"] for r in rows}
        id_vals = _text_map(id_attr_id)
        label_vals = _text_map(label_attr_id)

        # 3. Category (node) info for the entities' taxonomy nodes (one query).
        node_ids = list({e["taxonomy_node_id"] for e in entities if e["taxonomy_node_id"]})
        node_map = {}
        if node_ids:
            for n in self._fetch_in(
                f'SELECT "id","label","materialized_path" FROM {self._node} WHERE "id" IN :ids',
                "ids", node_ids,
            ):
                node_map[n["id"]] = (n["label"], n["materialized_path"])

        # 4. Child counts for all these entities as parents (one grouped query).
        child_counts = {}
        for r in self._fetch_in(
            f'SELECT "parent_id", COUNT("id") AS cnt FROM {self._entity} '
            f'WHERE "active" = TRUE AND "parent_id" IN :ids GROUP BY "parent_id"',
            "ids", ids,
        ):
            child_counts[r["parent_id"]] = r["cnt"]

        # 5. Replace the flat rows: delete all, then batch-insert the recomputed set.
        self._fetch_in(f'DELETE FROM {self._flat} WHERE "id" IN :ids RETURNING "id"', "ids", ids)

        flat_rows = []
        for eid in ids:
            entity = entity_by_id.get(eid)
            if not entity:
                continue  # gone → flat row already deleted above
            cat_label, cat_path = node_map.get(entity["taxonomy_node_id"], (None, None))
            flat_rows.append({
                "id": eid,
                "entity_type_id": entity["entity_type_id"], "taxonomy_node_id": entity["taxonomy_node_id"],
                "parent_id": entity["parent_id"], "status": entity["status"], "active": entity["active"],
                "promoted": entity["promoted"], "published_at": entity["published_at"],
                "identifier": id_vals.get(eid), "label": label_vals.get(eid),
                "category_label": cat_label, "category_path": cat_path,
                "child_count": child_counts.get(eid, 0),
                "created_at": entity["created_at"], "updated_at": entity["updated_at"],
            })
        if flat_rows:
            pim_sql.bulk_insert(self.db, self.entity_name, FLAT_TBL, flat_rows)

    # ------------------------------------------------------------------
    # CREATE PRODUCT
    # ------------------------------------------------------------------
    def create_product(self, data: PimEntityCreate):
        try:
            if data.identifier_value:
                self._check_identifier_unique(data.identifier_value)

            node = pim_sql.fetch_one(
                self.db, f'SELECT "id" FROM {self._node} WHERE "id" = :id AND "is_active" = TRUE',
                {"id": data.taxonomy_node_id},
            )
            if not node:
                raise HTTPException(status_code=404, detail="Taxonomy node not found.")

            if data.parent_id:
                parent = pim_sql.fetch_one(
                    self.db, f'SELECT "entity_type_id" FROM {self._entity} WHERE "id" = :id AND "active" = TRUE',
                    {"id": data.parent_id},
                )
                if not parent:
                    raise HTTPException(status_code=404, detail="Parent product not found.")
                parent_tier = pim_sql.fetch_one(
                    self.db, f'SELECT "label","is_leaf" FROM {self._tier} WHERE "code" = :c',
                    {"c": parent["entity_type_id"]},
                )
                if parent_tier and parent_tier["is_leaf"]:
                    raise HTTPException(status_code=400, detail=f"Cannot add children under a {parent_tier['label']} (leaf tier).")

            sql, params = pim_sql.build_insert(
                self.entity_name, ENTITY_TBL,
                {"entity_type_id": data.entity_type_id, "taxonomy_node_id": data.taxonomy_node_id,
                 "parent_id": data.parent_id, "status": data.status or 'Active',
                 "active": data.active if data.active is not None else True,
                 "promoted": False},
            )
            pim_sql.execute(self.db, sql, params)
            product_id = params["id"]

            if data.identifier_value:
                self._write_identifier_value(product_id, data.identifier_value)
            if data.label_value:
                self._write_label_value(product_id, data.label_value)

            db_commit_auto_rollback(db=self.db)
            self.refresh_flat_rows([product_id])
            db_commit_auto_rollback(db=self.db)
            return pim_sql.fetch_one(self.db, f'SELECT * FROM {self._flat} WHERE "id" = :id', {"id": product_id})

        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Unable to create product. Reason - {traceback.format_exc()}")
            raise HTTPException(status_code=500, detail=f"Failed to create product: {str(e)}")

    # ------------------------------------------------------------------
    # LIST PRODUCTS
    # ------------------------------------------------------------------
    _SORT_COLUMNS = {
        'identifier': 'identifier', 'label': 'label', 'sku': 'identifier',
        'status': 'status', 'entity_type_id': 'entity_type_id',
        'created_at': 'created_at', 'updated_at': 'updated_at',
    }

    def list_products(self, page: int = 1, page_size: int = 20,
                      entity_type_id: str = None, status: str = None,
                      taxonomy_node_id: str = None, search: str = None,
                      promoted: bool = None, sort_by: str = None, sort_dir: str = 'desc',
                      completeness_min: float = None, completeness_max: float = None):
        try:
            where = ['"active" = TRUE']
            params = {}
            if not entity_type_id:
                where.append('"parent_id" IS NULL')
            if entity_type_id:
                where.append('"entity_type_id" = :etype')
                params["etype"] = entity_type_id
            if status:
                statuses = [s.strip() for s in status.split(",")]
                if len(statuses) == 1:
                    where.append('"status" = :st')
                    params["st"] = statuses[0]
                else:
                    where.append('"status" = ANY(:sts)')
                    params["sts"] = statuses
            if taxonomy_node_id:
                node = pim_sql.fetch_one(self.db, f'SELECT "materialized_path" FROM {self._node} WHERE "id" = :id', {"id": taxonomy_node_id})
                if node:
                    desc = pim_sql.fetch_all(
                        self.db, f'SELECT "id" FROM {self._node} WHERE "materialized_path" LIKE :pat',
                        {"pat": f"{node['materialized_path']}%"},
                    )
                    desc_ids = [d["id"] for d in desc]
                    where.append('"taxonomy_node_id" = ANY(:tn)')
                    params["tn"] = desc_ids
                else:
                    where.append('"taxonomy_node_id" = :tnid')
                    params["tnid"] = taxonomy_node_id
            if promoted is not None:
                where.append('"promoted" = :promo')
                params["promo"] = promoted
            if search:
                where.append('("identifier" ILIKE :srch OR "label" ILIKE :srch)')
                params["srch"] = f"%{search}%"

            where_sql = " AND ".join(where)
            sort_col = self._SORT_COLUMNS.get(sort_by, 'created_at')
            order_sql = f'"{sort_col}" {"ASC" if sort_dir == "asc" else "DESC"}'

            if completeness_min is not None or completeness_max is not None:
                all_rows = pim_sql.fetch_all(self.db, f'SELECT * FROM {self._flat} WHERE {where_sql} ORDER BY {order_sql}', params)
                completeness_map = self._batch_compute_completeness([p["id"] for p in all_rows])
                filtered = []
                for p in all_rows:
                    c = completeness_map.get(p["id"], 100.0)
                    if completeness_min is not None and c < completeness_min:
                        continue
                    if completeness_max is not None and c > completeness_max:
                        continue
                    filtered.append((p, c))
                total = len(filtered)
                page_slice = filtered[(page - 1) * page_size: page * page_size]
                products = [p for p, _ in page_slice]
                pre_computed = {p["id"]: c for p, c in page_slice}
            else:
                total = pim_sql.fetch_scalar(self.db, f'SELECT COUNT(*) FROM {self._flat} WHERE {where_sql}', params) or 0
                products = pim_sql.fetch_all(
                    self.db,
                    f'SELECT * FROM {self._flat} WHERE {where_sql} ORDER BY {order_sql} '
                    f'LIMIT {int(page_size)} OFFSET {int((page - 1) * page_size)}',
                    params,
                )
                pre_computed = None

            if not products:
                return {"items": [], "total": total, "page": page, "page_size": page_size, "entity_types": []}

            product_ids = [p["id"] for p in products]
            completeness_map = pre_computed if pre_computed is not None else self._batch_compute_completeness(product_ids)

            # Prices for display + child price range
            price_map = {}
            if product_ids:
                for lp in self._fetch_in(
                    f'SELECT "product_id","value","currency" FROM {self._vt("NUMBER")} '
                    f'WHERE "product_id" IN :ids AND "price_type" = :pt',
                    "ids", product_ids, {"pt": "List"},
                ):
                    price_map[lp["product_id"]] = {"list_price": float(lp["value"]), "currency": lp["currency"] or "USD"}

                for cp in self._fetch_in(
                    f'SELECT e."parent_id" AS parent_id, MIN(n."value") AS price_min, MAX(n."value") AS price_max '
                    f'FROM {self._entity} e JOIN {self._vt("NUMBER")} n ON n."product_id" = e."id" '
                    f'WHERE e."parent_id" IN :ids AND e."active" = TRUE AND n."price_type" = :pt '
                    f'GROUP BY e."parent_id"',
                    "ids", product_ids, {"pt": "List"},
                ):
                    pm = price_map.setdefault(cp["parent_id"], {"list_price": None, "currency": "USD"})
                    pm["price_min"] = float(cp["price_min"]) if cp["price_min"] else None
                    pm["price_max"] = float(cp["price_max"]) if cp["price_max"] else None

            items = []
            for flat in products:
                price_info = price_map.get(flat["id"], {})
                items.append({
                    "id": flat["id"], "entity_type_id": flat["entity_type_id"],
                    "identifier": flat["identifier"], "label": flat["label"],
                    "taxonomy_node_id": flat["taxonomy_node_id"], "parent_id": flat["parent_id"],
                    "status": flat["status"], "active": flat["active"], "promoted": flat["promoted"],
                    "published_at": flat["published_at"], "created_at": flat["created_at"],
                    "updated_at": flat["updated_at"], "category_label": flat["category_label"],
                    "category_path": flat["category_path"], "child_count": flat["child_count"] or 0,
                    "completeness": completeness_map.get(flat["id"], 100.0),
                    "list_price": price_info.get("list_price"), "price_currency": price_info.get("currency"),
                    "price_min": price_info.get("price_min"), "price_max": price_info.get("price_max"),
                })

            entity_types = [
                r["entity_type_id"] for r in pim_sql.fetch_all(
                    self.db, f'SELECT DISTINCT "entity_type_id" FROM {self._flat} WHERE "active" = TRUE'
                )
            ]
            return {"items": items, "total": total, "page": page, "page_size": page_size, "entity_types": sorted(entity_types)}

        except Exception as e:
            app_logger.exception(f"Error listing products: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # GET PRODUCT DETAIL
    # ------------------------------------------------------------------
    def get_product(self, product_id: str):
        try:
            product = pim_sql.fetch_one(self.db, f'SELECT * FROM {self._entity} WHERE "id" = :id AND "active" = TRUE', {"id": product_id})
            if not product:
                raise HTTPException(status_code=404, detail="Product not found.")
            children = pim_sql.fetch_all(
                self.db, f'SELECT * FROM {self._flat} WHERE "parent_id" = :pid AND "active" = TRUE', {"pid": product_id}
            )
            completeness_info = self._compute_completeness(product_id)
            return {
                "id": product["id"], "entity_type_id": product["entity_type_id"],
                "identifier": self._get_identifier_value(product["id"]),
                "label": self._get_label_value(product["id"]),
                "taxonomy_node_id": product["taxonomy_node_id"], "parent_id": product["parent_id"],
                "status": product["status"], "active": product["active"], "promoted": product["promoted"],
                "published_at": product["published_at"], "created_at": product["created_at"],
                "updated_at": product["updated_at"], "children": children,
                "completeness": completeness_info["completeness"],
            }
        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Error fetching product: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # UPDATE PRODUCT
    # ------------------------------------------------------------------
    def update_product(self, product_id: str, data: PimEntityUpdate):
        try:
            product = pim_sql.fetch_one(self.db, f'SELECT * FROM {self._entity} WHERE "id" = :id AND "active" = TRUE', {"id": product_id})
            if not product:
                raise HTTPException(status_code=404, detail="Product not found.")
            old_taxonomy_node_id = product["taxonomy_node_id"]

            set_values = {}
            if data.taxonomy_node_id is not None:
                node = pim_sql.fetch_one(self.db, f'SELECT "id" FROM {self._node} WHERE "id" = :id AND "is_active" = TRUE', {"id": data.taxonomy_node_id})
                if not node:
                    raise HTTPException(status_code=404, detail="Taxonomy node not found.")
                set_values["taxonomy_node_id"] = data.taxonomy_node_id
            if data.status is not None:
                set_values["status"] = data.status
            if data.active is not None:
                set_values["active"] = data.active

            if set_values:
                sql, params = pim_sql.build_update(self.entity_name, ENTITY_TBL, set_values, where='"id" = :id', where_params={"id": product_id})
                pim_sql.execute(self.db, sql, params)
                db_commit_auto_rollback(db=self.db)

            if data.taxonomy_node_id and data.taxonomy_node_id != old_taxonomy_node_id:
                self._handle_category_reassignment(product_id, old_taxonomy_node_id, data.taxonomy_node_id)

            self.refresh_flat_rows([product_id])
            db_commit_auto_rollback(db=self.db)
            return pim_sql.fetch_one(self.db, f'SELECT * FROM {self._flat} WHERE "id" = :id', {"id": product_id})

        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Unable to update product. Reason - {traceback.format_exc()}")
            raise HTTPException(status_code=500, detail=f"Failed to update product: {str(e)}")

    # ------------------------------------------------------------------
    # DELETE PRODUCT (soft)
    # ------------------------------------------------------------------
    def delete_product(self, product_id: str):
        try:
            product = pim_sql.fetch_one(self.db, f'SELECT * FROM {self._entity} WHERE "id" = :id AND "active" = TRUE', {"id": product_id})
            if not product:
                raise HTTPException(status_code=404, detail="Product not found.")

            children = pim_sql.fetch_all(self.db, f'SELECT "id" FROM {self._entity} WHERE "parent_id" = :pid AND "active" = TRUE', {"pid": product_id})
            now = pim_sql.utcnow()
            pim_sql.execute(self.db, f'UPDATE {self._entity} SET "active" = FALSE, "updated_at" = :ts WHERE "id" = :id', {"ts": now, "id": product_id})
            if children:
                self._fetch_in(  # reuse expanding bind for the UPDATE
                    f'UPDATE {self._entity} SET "active" = FALSE, "updated_at" = :ts WHERE "id" IN :ids RETURNING "id"',
                    "ids", [c["id"] for c in children], {"ts": now},
                )
            db_commit_auto_rollback(db=self.db)

            all_ids = [product_id] + [c["id"] for c in children]
            self.refresh_flat_rows(all_ids)
            db_commit_auto_rollback(db=self.db)

            identifier = self._get_identifier_value(product_id)
            return {"message": f"Product '{identifier or product_id}' and {len(children)} child items soft-deleted."}

        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Unable to delete product: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # PUBLISH / UNPUBLISH
    # ------------------------------------------------------------------
    def _set_promoted(self, product_ids, promoted: bool, verb: str):
        if not product_ids:
            raise HTTPException(status_code=400, detail="product_ids list must not be empty.")
        products = self._fetch_in(
            f'SELECT "id","parent_id" FROM {self._entity} WHERE "id" IN :ids AND "active" = TRUE',
            "ids", product_ids,
        )
        found_ids = {str(p["id"]) for p in products}
        missing = [pid for pid in product_ids if pid not in found_ids]
        if missing:
            raise HTTPException(status_code=404, detail=f"Products not found: {', '.join(missing)}")
        child_products = [p for p in products if p["parent_id"] is not None]
        if child_products:
            child_ids = [self._get_identifier_value(p["id"]) or p["id"] for p in child_products]
            raise HTTPException(
                status_code=400,
                detail=f"Cannot {verb} child-level items directly. {verb.capitalize()} the parent product instead. Items: {', '.join(child_ids)}",
            )
        now = pim_sql.utcnow()
        published_at = now if promoted else None
        self._fetch_in(
            f'UPDATE {self._entity} SET "promoted" = :pr, "published_at" = :pa, "updated_at" = :ts '
            f'WHERE "id" IN :ids RETURNING "id"',
            "ids", product_ids, {"pr": promoted, "pa": published_at, "ts": now},
        )
        children = self._fetch_in(
            f'SELECT "id" FROM {self._entity} WHERE "parent_id" IN :ids AND "active" = TRUE',
            "ids", product_ids,
        )
        child_ids = [c["id"] for c in children]
        if child_ids:
            self._fetch_in(
                f'UPDATE {self._entity} SET "promoted" = :pr, "published_at" = :pa, "updated_at" = :ts '
                f'WHERE "id" IN :ids RETURNING "id"',
                "ids", child_ids, {"pr": promoted, "pa": published_at, "ts": now},
            )
        db_commit_auto_rollback(db=self.db)
        self.refresh_flat_rows(product_ids + child_ids)
        db_commit_auto_rollback(db=self.db)
        return len(products) + len(child_ids)

    def publish_products(self, product_ids: list):
        try:
            total = self._set_promoted(product_ids, True, "publish")
            return {"message": f"{len(product_ids)} product(s) published successfully.", "published_count": total}
        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Unable to publish products: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    def unpublish_products(self, product_ids: list):
        try:
            total = self._set_promoted(product_ids, False, "unpublish")
            return {"message": f"{len(product_ids)} product(s) unpublished.", "unpublished_count": total}
        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Unable to unpublish products: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # ITEMS (child products)
    # ------------------------------------------------------------------
    def create_item(self, parent_id: str, data: PimEntityCreate):
        try:
            parent = pim_sql.fetch_one(
                self.db, f'SELECT "entity_type_id","taxonomy_node_id" FROM {self._entity} WHERE "id" = :id AND "active" = TRUE',
                {"id": parent_id},
            )
            if not parent:
                raise HTTPException(status_code=404, detail="Parent product not found.")
            parent_tier = pim_sql.fetch_one(
                self.db, f'SELECT "id","label","is_leaf" FROM {self._tier} WHERE "code" = :c', {"c": parent["entity_type_id"]}
            )
            if parent_tier and parent_tier["is_leaf"]:
                raise HTTPException(status_code=400, detail=f"Cannot add children under a {parent_tier['label']} (leaf tier).")
            child_tier = None
            if parent_tier:
                child_tier = pim_sql.fetch_one(
                    self.db, f'SELECT "code" FROM {self._tier} WHERE "parent_tier_id" = :pid', {"pid": parent_tier["id"]}
                )
            if data.identifier_value:
                self._check_identifier_unique(data.identifier_value)

            sql, params = pim_sql.build_insert(
                self.entity_name, ENTITY_TBL,
                {"entity_type_id": child_tier["code"] if child_tier else data.entity_type_id,
                 "taxonomy_node_id": parent["taxonomy_node_id"], "parent_id": parent_id,
                 "status": data.status or 'Active',
                 "active": data.active if data.active is not None else True, "promoted": False},
            )
            pim_sql.execute(self.db, sql, params)
            item_id = params["id"]
            if data.identifier_value:
                self._write_identifier_value(item_id, data.identifier_value)
            if data.label_value:
                self._write_label_value(item_id, data.label_value)
            db_commit_auto_rollback(db=self.db)
            self.refresh_flat_rows([item_id, parent_id])
            db_commit_auto_rollback(db=self.db)
            return pim_sql.fetch_one(self.db, f'SELECT * FROM {self._flat} WHERE "id" = :id', {"id": item_id})

        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Unable to create item. Reason - {traceback.format_exc()}")
            raise HTTPException(status_code=500, detail=f"Failed to create item: {str(e)}")

    def list_items(self, parent_id: str):
        try:
            parent = pim_sql.fetch_one(self.db, f'SELECT "id" FROM {self._entity} WHERE "id" = :id AND "active" = TRUE', {"id": parent_id})
            if not parent:
                raise HTTPException(status_code=404, detail="Parent product not found.")
            return pim_sql.fetch_all(
                self.db, f'SELECT * FROM {self._flat} WHERE "parent_id" = :pid AND "active" = TRUE ORDER BY "created_at" DESC',
                {"pid": parent_id},
            )
        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Error listing items: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    def update_item(self, parent_id: str, item_id: str, data: PimEntityUpdate):
        try:
            item = pim_sql.fetch_one(
                self.db, f'SELECT "id" FROM {self._entity} WHERE "id" = :id AND "parent_id" = :pid AND "active" = TRUE',
                {"id": item_id, "pid": parent_id},
            )
            if not item:
                raise HTTPException(status_code=404, detail="Item not found under this parent.")
            set_values = {}
            if data.status is not None:
                set_values["status"] = data.status
            if data.active is not None:
                set_values["active"] = data.active
            if set_values:
                sql, params = pim_sql.build_update(self.entity_name, ENTITY_TBL, set_values, where='"id" = :id', where_params={"id": item_id})
                pim_sql.execute(self.db, sql, params)
                db_commit_auto_rollback(db=self.db)
            self.refresh_flat_rows([item_id])
            db_commit_auto_rollback(db=self.db)
            return pim_sql.fetch_one(self.db, f'SELECT * FROM {self._flat} WHERE "id" = :id', {"id": item_id})

        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Unable to update item. Reason - {traceback.format_exc()}")
            raise HTTPException(status_code=500, detail=f"Failed to update item: {str(e)}")

    def delete_item(self, parent_id: str, item_id: str):
        try:
            item = pim_sql.fetch_one(
                self.db, f'SELECT "id" FROM {self._entity} WHERE "id" = :id AND "parent_id" = :pid AND "active" = TRUE',
                {"id": item_id, "pid": parent_id},
            )
            if not item:
                raise HTTPException(status_code=404, detail="Item not found under this parent.")
            pim_sql.execute(self.db, f'UPDATE {self._entity} SET "active" = FALSE, "updated_at" = :ts WHERE "id" = :id', {"ts": pim_sql.utcnow(), "id": item_id})
            db_commit_auto_rollback(db=self.db)
            self.refresh_flat_rows([item_id, parent_id])
            db_commit_auto_rollback(db=self.db)
            identifier = self._get_identifier_value(item_id)
            return {"message": f"Item '{identifier or item_id}' soft-deleted."}
        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Unable to delete item: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # COMPLETENESS
    # ------------------------------------------------------------------
    def compute_completeness(self, product_id: str):
        try:
            product = pim_sql.fetch_one(self.db, f'SELECT "id" FROM {self._entity} WHERE "id" = :id AND "active" = TRUE', {"id": product_id})
            if not product:
                raise HTTPException(status_code=404, detail="Product not found.")
            return self._compute_completeness(product_id)
        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Error computing completeness: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    def _compute_completeness(self, product_id: str):
        product = pim_sql.fetch_one(self.db, f'SELECT "taxonomy_node_id","entity_type_id" FROM {self._entity} WHERE "id" = :id', {"id": product_id})
        if not product:
            return {"completeness": 0.0, "total_required": 0, "filled": 0, "missing": []}

        required_rows = pim_sql.fetch_all(
            self.db,
            f'SELECT r."attribute_id" AS attribute_id, c."level_override" AS level_override, a."level" AS level '
            f'FROM {self._resolved} r '
            f'JOIN {self._attr} a ON r."attribute_id" = a."id" '
            f'LEFT JOIN {self._config} c ON r."config_id" = c."id" '
            f'WHERE r."taxonomy_node_id" = :nid AND r."is_required" = TRUE',
            {"nid": product["taxonomy_node_id"]},
        )
        required_attrs = set()
        for row in required_rows:
            effective_level = (row["level_override"] or row["level"]) or "ALL"
            if effective_level == "ALL" or effective_level == product["entity_type_id"]:
                required_attrs.add(str(row["attribute_id"]))

        global_required_ids = {
            str(a["id"]) for a in pim_sql.fetch_all(
                self.db, f'SELECT "id" FROM {self._attr} WHERE "scope" = \'general\' AND "is_system" = TRUE'
            )
        }
        required_attr_ids = required_attrs | global_required_ids
        if not required_attr_ids:
            return {"completeness": 100.0, "total_required": 0, "filled": 0, "missing": []}

        filled_attr_ids = set()
        for vt in VALUE_TABLES:
            for r in pim_sql.fetch_all(self.db, f'SELECT DISTINCT "attribute_id" FROM {self._t(vt)} WHERE "product_id" = :pid', {"pid": product_id}):
                filled_attr_ids.add(str(r["attribute_id"]))

        filled_required = required_attr_ids & filled_attr_ids
        missing = list(required_attr_ids - filled_attr_ids)
        total = len(required_attr_ids)
        filled = len(filled_required)
        completeness = round((filled / total) * 100, 2) if total > 0 else 100.0
        return {"completeness": completeness, "total_required": total, "filled": filled, "missing": missing}

    def _batch_compute_completeness(self, product_ids: list) -> dict:
        if not product_ids:
            return {}
        products = self._fetch_in(
            f'SELECT "id","taxonomy_node_id","entity_type_id" FROM {self._entity} WHERE "id" IN :ids',
            "ids", product_ids,
        )
        product_node_map = {p["id"]: p["taxonomy_node_id"] for p in products}
        product_tier_map = {p["id"]: p["entity_type_id"] for p in products}

        node_ids = list({nid for nid in product_node_map.values() if nid})
        node_required_attrs = {}
        if node_ids:
            required_rows = self._fetch_in(
                f'SELECT r."taxonomy_node_id" AS taxonomy_node_id, r."attribute_id" AS attribute_id, '
                f'c."level_override" AS level_override, a."level" AS level '
                f'FROM {self._resolved} r '
                f'JOIN {self._attr} a ON r."attribute_id" = a."id" '
                f'LEFT JOIN {self._config} c ON r."config_id" = c."id" '
                f'WHERE r."taxonomy_node_id" IN :ids AND r."is_required" = TRUE',
                "ids", node_ids,
            )
            for row in required_rows:
                effective_level = (row["level_override"] or row["level"]) or "ALL"
                node_required_attrs.setdefault(row["taxonomy_node_id"], []).append((row["attribute_id"], effective_level))

        global_system_attrs = pim_sql.fetch_all(
            self.db, f'SELECT "id","level" FROM {self._attr} WHERE "scope" = \'general\' AND "is_system" = TRUE'
        )

        product_filled_map = {pid: set() for pid in product_ids}
        for vt in VALUE_TABLES:
            for row in self._fetch_in(
                f'SELECT "product_id","attribute_id" FROM {self._t(vt)} WHERE "product_id" IN :ids',
                "ids", product_ids,
            ):
                if row["product_id"] in product_filled_map:
                    product_filled_map[row["product_id"]].add(row["attribute_id"])

        def level_matches(level_str, tier_code):
            if not level_str or level_str == "ALL":
                return True
            return tier_code in [l.strip() for l in level_str.split(",")]

        result = {}
        for pid in product_ids:
            node_id = product_node_map.get(pid)
            tier = product_tier_map.get(pid, "PRODUCT")
            required = set()
            if node_id and node_id in node_required_attrs:
                for attr_id, eff_level in node_required_attrs[node_id]:
                    if level_matches(eff_level, tier):
                        required.add(attr_id)
            for ga in global_system_attrs:
                if level_matches(ga["level"], tier):
                    required.add(ga["id"])
            if not required:
                result[pid] = 100.0
            else:
                filled = len(required & product_filled_map.get(pid, set()))
                result[pid] = round((filled / len(required)) * 100, 2)
        return result

    # ------------------------------------------------------------------
    # DASHBOARD / HEALTH / EXPORT
    # ------------------------------------------------------------------
    def get_dashboard_stats(self):
        try:
            total_products = pim_sql.fetch_scalar(self.db, f'SELECT COUNT("id") FROM {self._entity} WHERE "active" = TRUE AND "parent_id" IS NULL') or 0
            total_items = pim_sql.fetch_scalar(self.db, f'SELECT COUNT("id") FROM {self._entity} WHERE "active" = TRUE AND "parent_id" IS NOT NULL') or 0
            all_pids = [r["id"] for r in pim_sql.fetch_all(self.db, f'SELECT "id" FROM {self._entity} WHERE "active" = TRUE AND "parent_id" IS NULL')]
            completeness_map = self._batch_compute_completeness(all_pids) if all_pids else {}
            incomplete_count = sum(1 for c in completeness_map.values() if c < 100)
            status_rows = pim_sql.fetch_all(self.db, f'SELECT "status", COUNT("id") AS cnt FROM {self._entity} WHERE "active" = TRUE AND "parent_id" IS NULL GROUP BY "status"')
            status_breakdown = {r["status"]: r["cnt"] for r in status_rows}
            published_count = pim_sql.fetch_scalar(self.db, f'SELECT COUNT("id") FROM {self._entity} WHERE "active" = TRUE AND "promoted" = TRUE AND "parent_id" IS NULL') or 0
            return {
                "total_products": total_products, "total_items": total_items,
                "incomplete_products": incomplete_count, "published_products": published_count,
                "status_breakdown": status_breakdown,
            }
        except Exception as e:
            app_logger.exception(f"Error computing dashboard stats: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    def get_category_health(self):
        try:
            nodes = pim_sql.fetch_all(self.db, f'SELECT * FROM {self._node} WHERE "is_active" = TRUE')
            if not nodes:
                return {"categories": []}
            products = pim_sql.fetch_all(self.db, f'SELECT "id","taxonomy_node_id" FROM {self._entity} WHERE "active" = TRUE')
            node_products = {}
            for p in products:
                if p["taxonomy_node_id"]:
                    node_products.setdefault(p["taxonomy_node_id"], []).append(p["id"])
            all_product_ids = [p["id"] for p in products]
            completeness_map = self._batch_compute_completeness(all_product_ids) if all_product_ids else {}
            categories = []
            for node in nodes:
                pids = node_products.get(node["id"], [])
                if not pids:
                    avg_completeness = None
                    product_count = 0
                else:
                    product_count = len(pids)
                    total_c = sum(completeness_map.get(pid, 100.0) for pid in pids)
                    avg_completeness = round(total_c / product_count, 2)
                categories.append({
                    "node_id": node["id"], "label": node["label"], "code": node["code"],
                    "level": node["level"], "materialized_path": node["materialized_path"],
                    "product_count": product_count, "avg_completeness": avg_completeness,
                })
            categories.sort(key=lambda c: (c["avg_completeness"] is None, c["avg_completeness"] or 0))
            return {"categories": categories}
        except Exception as e:
            app_logger.exception(f"Error computing category health: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    def export_products_csv(self, entity_type_id: str = None, status: str = None, taxonomy_node_id: str = None):
        try:
            where = ['"active" = TRUE']
            params = {}
            if entity_type_id:
                where.append('"entity_type_id" = :etype')
                params["etype"] = entity_type_id
            if status:
                statuses = [s.strip() for s in status.split(",")]
                if len(statuses) == 1:
                    where.append('"status" = :st')
                    params["st"] = statuses[0]
                else:
                    where.append('"status" = ANY(:sts)')
                    params["sts"] = statuses
            if taxonomy_node_id:
                node = pim_sql.fetch_one(self.db, f'SELECT "materialized_path" FROM {self._node} WHERE "id" = :id', {"id": taxonomy_node_id})
                if node:
                    desc = pim_sql.fetch_all(self.db, f'SELECT "id" FROM {self._node} WHERE "materialized_path" LIKE :pat', {"pat": f"{node['materialized_path']}%"})
                    where.append('"taxonomy_node_id" = ANY(:tn)')
                    params["tn"] = [d["id"] for d in desc]
            products = pim_sql.fetch_all(self.db, f'SELECT * FROM {self._flat} WHERE {" AND ".join(where)} ORDER BY "created_at" DESC', params)
            if not products:
                return {"rows": [], "columns": []}
            completeness_map = self._batch_compute_completeness([p["id"] for p in products])
            rows = []
            for p in products:
                rows.append({
                    "identifier": p["identifier"] or "", "label": p["label"] or "",
                    "entity_type_id": p["entity_type_id"], "status": p["status"],
                    "category": p["category_label"] or "", "category_path": p["category_path"] or "",
                    "completeness": completeness_map.get(p["id"], 100.0), "promoted": p["promoted"],
                    "published_at": str(p["published_at"]) if p["published_at"] else "",
                    "created_at": str(p["created_at"]) if p["created_at"] else "",
                })
            columns = list(rows[0].keys()) if rows else []
            return {"rows": rows, "columns": columns, "total": len(rows)}
        except Exception as e:
            app_logger.exception(f"Error exporting products: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # CATEGORY REASSIGNMENT HELPER
    # ------------------------------------------------------------------
    def _handle_category_reassignment(self, product_id: str, old_node_id: str, new_node_id: str):
        try:
            new_valid_attrs = {
                r["attribute_id"] for r in pim_sql.fetch_all(
                    self.db, f'SELECT "attribute_id" FROM {self._resolved} WHERE "taxonomy_node_id" = :nid', {"nid": new_node_id}
                )
            }
            global_attr_ids = {
                a["id"] for a in pim_sql.fetch_all(self.db, f'SELECT "id" FROM {self._attr} WHERE "scope" = \'general\'')
            }
            new_valid_attrs |= global_attr_ids
            for vt in VALUE_TABLES:
                if new_valid_attrs:
                    self._fetch_in(
                        f'DELETE FROM {self._t(vt)} WHERE "product_id" = :pid AND "attribute_id" NOT IN :ids RETURNING "id"',
                        "ids", list(new_valid_attrs), {"pid": product_id},
                    )
                else:
                    pim_sql.execute(self.db, f'DELETE FROM {self._t(vt)} WHERE "product_id" = :pid', {"pid": product_id})
            db_commit_auto_rollback(db=self.db)
            app_logger.info(f"Category reassignment: cleaned orphaned values for product {product_id} (old={old_node_id} -> new={new_node_id})")
        except Exception as e:
            app_logger.exception(f"Error during category reassignment cleanup: {str(e)}")

    # ------------------------------------------------------------------
    # BULK IMPORT HIERARCHY  (SCRUM-1790)
    # ------------------------------------------------------------------
    def _flag_attr(self, attr_id, field):
        """Set is_identifier / is_label on an attribute by id; refresh the cache."""
        pim_sql.execute(
            self.db, f'UPDATE {self._attr} SET "{field}" = TRUE, "updated_at" = :ts WHERE "id" = :id',
            {"ts": pim_sql.utcnow(), "id": attr_id},
        )
        row = pim_sql.fetch_one(self.db, f'SELECT * FROM {self._attr} WHERE "id" = :id', {"id": attr_id})
        if field == "is_identifier":
            self._identifier_attr = row
        else:
            self._label_attr = row
        return row

    def _bulk_insert_rows(self, table, rows):
        """Batched INSERT of fully-formed row dicts (id/timestamps already set).

        Uses pim_sql.bulk_insert (psycopg2 execute_values — the reference-entity
        pattern): a few multi-row statements instead of one round-trip per row.
        All rows for a table must share the same key set; normalize so a row that
        omits an optional column still aligns (missing -> None)."""
        if not rows:
            return
        # Union of keys across rows → consistent column set; fill gaps with None.
        cols = list({k for r in rows for k in r.keys()})
        norm = [{c: r.get(c) for c in cols} for r in rows]
        pim_sql.bulk_insert(self.db, self.entity_name, table, norm)

    def bulk_import_hierarchy(self, data: PimBulkImportRequest):
        """Bulk import a full N-tier hierarchy. Builds insert-lists then writes via raw SQL."""
        created = {}
        errors = []
        rejected_values = []
        products_to_insert = []
        values_to_insert = []

        attr_defs = {a["id"]: a for a in pim_sql.fetch_all(self.db, f'SELECT * FROM {self._attr}')}
        option_keys = {}
        for o in pim_sql.fetch_all(self.db, f'SELECT "attribute_id","value_key" FROM {self._opt} WHERE "is_active" = TRUE'):
            option_keys.setdefault(o["attribute_id"], set()).add(o["value_key"])

        id_attr = self._get_identifier_attr()
        label_attr = self._get_label_attr()
        if not id_attr and data.identifier_attribute_id:
            cand = pim_sql.fetch_one(self.db, f'SELECT "id" FROM {self._attr} WHERE "id" = :id', {"id": data.identifier_attribute_id})
            if cand:
                id_attr = self._flag_attr(cand["id"], "is_identifier")
        if not label_attr and data.label_attribute_id:
            cand = pim_sql.fetch_one(self.db, f'SELECT "id" FROM {self._attr} WHERE "id" = :id', {"id": data.label_attribute_id})
            if cand:
                label_attr = self._flag_attr(cand["id"], "is_label")
        if not id_attr:
            existing = pim_sql.fetch_one(self.db, f'SELECT "id" FROM {self._attr} WHERE "code" = \'sku\'')
            if existing:
                id_attr = self._flag_attr(existing["id"], "is_identifier")
        if not label_attr:
            existing = pim_sql.fetch_one(self.db, f'SELECT "id" FROM {self._attr} WHERE "code" = \'name\'')
            if existing:
                label_attr = self._flag_attr(existing["id"], "is_label")
        id_attr_id = id_attr["id"] if id_attr else None
        label_attr_id = label_attr["id"] if label_attr else None

        all_taxonomy_nodes = pim_sql.fetch_all(self.db, f'SELECT "id","label","code" FROM {self._node} WHERE "is_active" = TRUE')
        taxonomy_label_to_id = {n["label"].lower(): n["id"] for n in all_taxonomy_nodes}
        taxonomy_code_to_id = {n["code"].lower(): n["id"] for n in all_taxonomy_nodes}
        default_taxonomy_id = all_taxonomy_nodes[0]["id"] if all_taxonomy_nodes else None
        known_labels = set(taxonomy_label_to_id.keys())

        def resolve_taxonomy_id(value):
            if not value:
                return None
            if len(value) == 36 and '-' in value:
                return value
            val_lower = value.lower().strip()
            if val_lower in taxonomy_label_to_id:
                return taxonomy_label_to_id[val_lower]
            if val_lower in taxonomy_code_to_id:
                return taxonomy_code_to_id[val_lower]
            for known in known_labels:
                for pv in (f"{known}'s ", f"{known} "):
                    if val_lower.startswith(pv):
                        remainder = val_lower[len(pv):]
                        if remainder in taxonomy_label_to_id:
                            return taxonomy_label_to_id[remainder]
            best_match = None
            best_len = 0
            for label, nid in taxonomy_label_to_id.items():
                if len(label) > best_len and (label in val_lower or val_lower in label):
                    best_match = nid
                    best_len = len(label)
            return best_match

        def _uuid():
            return str(uuid.uuid4())

        def process_node(node: PimImportNode, parent_id=None, taxonomy_node_id=None):
            product_id = _uuid()
            now = datetime.utcnow()
            resolved_taxonomy = resolve_taxonomy_id(node.taxonomy_node_id) or taxonomy_node_id or default_taxonomy_id
            identifier_val = node.identifier_value or node.key
            label_val = node.label_value or identifier_val

            products_to_insert.append({
                'id': product_id, 'entity_type_id': node.tier_code, 'taxonomy_node_id': resolved_taxonomy,
                'parent_id': parent_id, 'status': node.status or 'Draft', 'active': True, 'promoted': False,
                'published_at': None, 'created_at': now, 'updated_at': now,
            })
            if id_attr_id and identifier_val:
                values_to_insert.append({'table': VALUE_TEXT, 'row': {
                    'id': _uuid(), 'product_id': product_id, 'attribute_id': id_attr_id, 'value': identifier_val,
                    'locale': '', 'source': 'IMPORT', 'version': 1, 'created_at': now, 'updated_at': now}})
            if label_attr_id and label_val:
                values_to_insert.append({'table': VALUE_TEXT, 'row': {
                    'id': _uuid(), 'product_id': product_id, 'attribute_id': label_attr_id, 'value': label_val,
                    'locale': '', 'source': 'IMPORT', 'version': 1, 'created_at': now, 'updated_at': now}})

            created[node.tier_code] = created.get(node.tier_code, 0) + 1

            for val in (node.values or []):
                if not val.value or not val.attribute_id:
                    continue
                attr_def = attr_defs.get(val.attribute_id)
                if not attr_def:
                    continue
                if attr_def["level"] and attr_def["level"] != "ALL":
                    if node.tier_code not in [l.strip() for l in attr_def["level"].split(",")]:
                        continue
                value_id = _uuid()
                data_type = attr_def["data_type"]
                base = {'id': value_id, 'product_id': product_id, 'attribute_id': val.attribute_id,
                        'locale': '', 'source': val.source or 'IMPORT', 'version': 1,
                        'created_at': now, 'updated_at': now}
                if data_type == 'NUMBER':
                    try:
                        nv = float(val.value)
                    except (ValueError, TypeError):
                        continue
                    values_to_insert.append({'table': VALUE_NUMBER, 'row': {**base, 'value': nv, 'currency': '', 'price_type': '', 'territory': ''}})
                elif data_type == 'BOOLEAN':
                    values_to_insert.append({'table': 'pim_value_boolean', 'row': {**base, 'value': val.value.lower() in ('true', '1', 'yes')}})
                elif data_type == 'SELECT':
                    key = to_value_key(str(val.value))
                    if key not in option_keys.get(val.attribute_id, set()):
                        rejected_values.append({'attribute_id': val.attribute_id, 'attribute_code': attr_def["code"], 'product_id': product_id, 'value': str(val.value), 'value_key': key, 'reason': 'option_not_defined'})
                        continue
                    values_to_insert.append({'table': 'pim_value_select', 'row': {**base, 'ref_value_key': key}})
                elif data_type == 'MULTISELECT':
                    raw_parts = [p.strip() for chunk in str(val.value).split(';') for p in chunk.split(',')]
                    valid_set = option_keys.get(val.attribute_id, set())
                    for part in [p for p in raw_parts if p]:
                        key = to_value_key(part)
                        if key not in valid_set:
                            rejected_values.append({'attribute_id': val.attribute_id, 'attribute_code': attr_def["code"], 'product_id': product_id, 'value': part, 'value_key': key, 'reason': 'option_not_defined'})
                            continue
                        values_to_insert.append({'table': 'pim_value_multiselect', 'row': {**base, 'id': _uuid(), 'ref_value_key': key}})
                elif data_type == 'REFERENCE':
                    values_to_insert.append({'table': 'pim_value_reference', 'row': {**base, 'ref_table': attr_def["reference_entity_id"] or '', 'ref_id': str(val.value)}})
                elif data_type == 'DATE':
                    try:
                        dv = _date.fromisoformat(str(val.value).strip())
                    except (ValueError, TypeError):
                        rejected_values.append({'attribute_id': val.attribute_id, 'attribute_code': attr_def["code"], 'product_id': product_id, 'value': str(val.value), 'value_key': None, 'reason': 'invalid_date'})
                        continue
                    values_to_insert.append({'table': 'pim_value_date', 'row': {**base, 'value': dv}})
                else:
                    values_to_insert.append({'table': VALUE_TEXT, 'row': {**base, 'value': str(val.value)}})

            for child in (node.children or []):
                process_node(child, parent_id=product_id, taxonomy_node_id=resolved_taxonomy)

        app_logger.info(f"Bulk import: processing {len(data.nodes)} root nodes...")
        for node in data.nodes:
            try:
                process_node(node)
            except Exception as e:
                errors.append(f"Node '{node.key}': {str(e)}")

        # Dedup by pim_entity unique columns. pim_entity has NO unique constraints
        # (verified in models), so this is a no-op guard kept for parity.
        # Remove existing products with matching identifier (clean re-import): match on
        # the identifier EAV value, since pim_entity itself has no natural unique key.
        if products_to_insert and id_attr_id:
            import_ids = [v['row']['value'] for v in values_to_insert
                          if v['table'] == VALUE_TEXT and v['row']['attribute_id'] == id_attr_id]
            if import_ids:
                existing_rows = self._fetch_in(
                    f'SELECT "product_id" FROM {self._vt("TEXT")} '
                    f'WHERE "attribute_id" = :aid AND "locale" = \'\' AND "value" IN :vals',
                    "vals", import_ids, {"aid": id_attr_id},
                )
                existing_ids = [r["product_id"] for r in existing_rows]
                if existing_ids:
                    app_logger.info(f"Bulk import: removing {len(existing_ids)} existing products for re-import...")
                    for vt in VALUE_TABLES:
                        self._fetch_in(f'DELETE FROM {self._t(vt)} WHERE "product_id" IN :ids RETURNING "id"', "ids", existing_ids)
                    self._fetch_in(f'DELETE FROM {self._entity} WHERE "id" IN :ids RETURNING "id"', "ids", existing_ids)

        try:
            app_logger.info(f"Bulk import: inserting {len(products_to_insert)} products...")
            self._bulk_insert_rows(ENTITY_TBL, products_to_insert)

            def dedup_values(rows):
                seen = set()
                result = []
                for r in rows:
                    key = (r['product_id'], r['attribute_id'], r.get('locale', ''))
                    if key not in seen:
                        seen.add(key)
                        result.append(r)
                return result

            for tbl in [VALUE_TEXT, VALUE_NUMBER, 'pim_value_boolean', 'pim_value_date', 'pim_value_select', 'pim_value_reference']:
                self._bulk_insert_rows(tbl, dedup_values([v['row'] for v in values_to_insert if v['table'] == tbl]))
            # Multiselect: dedup on full key incl. ref_value_key
            ms_seen = set()
            ms_rows = []
            for v in values_to_insert:
                if v['table'] != 'pim_value_multiselect':
                    continue
                r = v['row']
                k = (r['product_id'], r['attribute_id'], r.get('locale', ''), r['ref_value_key'])
                if k not in ms_seen:
                    ms_seen.add(k)
                    ms_rows.append(r)
            self._bulk_insert_rows('pim_value_multiselect', ms_rows)

            db_commit_auto_rollback(db=self.db)

            imported_ids = [p['id'] for p in products_to_insert]
            if imported_ids:
                self.refresh_flat_rows(imported_ids)
                db_commit_auto_rollback(db=self.db)

            app_logger.info(f"Bulk import complete: {sum(created.values())} products, {len(values_to_insert)} values")

        except Exception as e:
            self.db.rollback()
            app_logger.exception(f"Bulk import failed, rolled back: {str(e)}\n{traceback.format_exc()}")
            err_msg = str(e)[:500] + ("..." if len(str(e)) > 500 else "")
            errors.append(f"Bulk import failed: {err_msg}")
            created = {k: 0 for k in created}
            rejected_values = []

        if rejected_values:
            app_logger.warning(f"Bulk import: {len(rejected_values)} select/multiselect values rejected (option not defined).")
        return {"created": created, "errors": errors, "rejected_values": rejected_values}

    # =======================================================================
    # FLAT FILE IMPORT — SKU-keyed upsert with per-field Add/Overwrite modes
    # =======================================================================
    def flat_import(self, data: PimFlatImportRequest):
        """Item-grain flat-file import. SKU is the match key; per-field modes."""
        now = datetime.utcnow()
        inserted = 0
        updated = 0
        errors = []

        def _fail(row_number, reason):
            errors.append({'row_number': row_number, 'reason': reason})

        id_attr = self._get_identifier_attr()
        if not id_attr:
            raise HTTPException(status_code=400, detail="No identifier attribute (is_identifier) is defined — required to match rows by SKU.")
        label_attr = self._get_label_attr()
        id_attr_id = id_attr["id"]
        label_attr_id = label_attr["id"] if label_attr else None

        attr_defs = {a["id"]: a for a in pim_sql.fetch_all(self.db, f'SELECT * FROM {self._attr}')}
        option_keys = {}
        for o in pim_sql.fetch_all(self.db, f'SELECT "attribute_id","value_key" FROM {self._opt} WHERE "is_active" = TRUE'):
            option_keys.setdefault(o["attribute_id"], set()).add(o["value_key"])

        def _blank(v):
            return v is None or not str(v).strip()

        def _row_levels(row):
            """Ordered [(tier_code, level), …] top→leaf for either payload shape.

            Generic `levels` (n-tier) wins when present; otherwise fall back to the
            legacy product/variant/item using the request's *_tier_code fields."""
            if row.levels:
                return [(lvl.tier_code, lvl) for lvl in row.levels]
            out = []
            if row.product is not None:
                out.append((data.product_tier_code, row.product))
            if data.variant_tier_code and row.variant is not None and not _blank(row.variant.key):
                out.append((data.variant_tier_code, row.variant))
            if row.item is not None:
                out.append((data.item_tier_code, row.item))
            return out

        def _row_leaf(row):
            levels = _row_levels(row)
            return levels[-1][1] if levels else None

        def _row_root(row):
            levels = _row_levels(row)
            return levels[0][1] if levels else None

        def _validate_level(lvl):
            for v in (lvl.values or []):
                if _blank(v.value):
                    continue
                ad = attr_defs.get(v.attribute_id)
                if not ad:
                    return f"Unknown attribute '{v.attribute_id}'"
                if ad["data_type"] == 'NUMBER':
                    try:
                        float(str(v.value))
                    except (ValueError, TypeError):
                        return f"'{v.value}' is not a number for '{ad['label']}'"
                elif ad["data_type"] == 'DATE':
                    try:
                        _date.fromisoformat(str(v.value).strip())
                    except (ValueError, TypeError):
                        return f"'{v.value}' is not a valid date (YYYY-MM-DD) for '{ad['label']}'"
                elif ad["data_type"] == 'SELECT':
                    if to_value_key(str(v.value)) not in option_keys.get(ad["id"], set()):
                        return f"'{v.value}' is not a defined option of '{ad['label']}'"
                elif ad["data_type"] == 'MULTISELECT':
                    parts = [p.strip() for chunk in str(v.value).split(';') for p in chunk.split(',')]
                    for part in [p for p in parts if p]:
                        if to_value_key(part) not in option_keys.get(ad["id"], set()):
                            return f"'{part}' is not a defined option of '{ad['label']}'"
            return None

        valid_rows = []
        seen_skus = {}
        for row in data.rows:
            row_levels = _row_levels(row)
            leaf = _row_leaf(row)
            root = _row_root(row)
            if leaf is None or _blank(leaf.key):
                _fail(row.row_number, "Missing SKU (leaf-tier key)"); continue
            if root is None or _blank(root.key):
                _fail(row.row_number, "Missing Product ID (root-tier key)"); continue
            sku = str(leaf.key).strip()
            if sku in seen_skus:
                _fail(row.row_number, f"Duplicate SKU '{sku}' (first at row {seen_skus[sku]})"); continue
            reason = None
            for _tc, lvl in row_levels:
                if lvl is None:
                    continue
                reason = _validate_level(lvl)
                if reason:
                    break
            if reason:
                _fail(row.row_number, reason); continue
            seen_skus[sku] = row.row_number
            valid_rows.append(row)

        if not valid_rows:
            return {'inserted': 0, 'updated': 0, 'failed': len(errors), 'errors': errors}

        # Match existing entities by identifier value, per tier (all levels, all rows)
        all_keys = set()
        for row in valid_rows:
            for _tc, lvl in _row_levels(row):
                if lvl is not None and not _blank(lvl.key):
                    all_keys.add(str(lvl.key).strip())

        def _chunks(seq, size=1000):
            seq = list(seq)
            for i in range(0, len(seq), size):
                yield seq[i:i + size]

        existing_by_tier = {}  # (tier_code, identifier_value) -> entity dict
        for chunk in _chunks(all_keys):
            matched = self._fetch_in(
                f'SELECT e."id" AS id, e."entity_type_id" AS entity_type_id, e."status" AS status, '
                f'e."taxonomy_node_id" AS taxonomy_node_id, v."value" AS key '
                f'FROM {self._entity} e JOIN {self._vt("TEXT")} v ON v."product_id" = e."id" '
                f'WHERE v."attribute_id" = :aid AND v."locale" = \'\' AND v."value" IN :vals AND e."active" = TRUE',
                "vals", chunk, {"aid": id_attr_id},
            )
            for m in matched:
                existing_by_tier.setdefault((m["entity_type_id"], m["key"]), m)

        # Prefetch existing values of mapped attributes (as dicts keyed by (pid, attr))
        mapped_attr_ids = set()
        for row in valid_rows:
            for _tc, lvl in _row_levels(row):
                if lvl is None:
                    continue
                for v in (lvl.values or []):
                    mapped_attr_ids.add(v.attribute_id)
        if label_attr_id:
            mapped_attr_ids.add(label_attr_id)

        existing_ids = [e["id"] for e in existing_by_tier.values()]
        existing_vals = {dt: {} for dt in VALUE_TABLE_BY_TYPE}
        if existing_ids and mapped_attr_ids:
            for dt, tbl in VALUE_TABLE_BY_TYPE.items():
                for chunk in _chunks(existing_ids):
                    rows = self._fetch_two_in(
                        f'SELECT * FROM {self._t(tbl)} WHERE "product_id" IN :pids '
                        f'AND "attribute_id" IN :aids AND "locale" = \'\'',
                        "pids", chunk, "aids", list(mapped_attr_ids),
                    )
                    for r in rows:
                        existing_vals[dt].setdefault((r["product_id"], r["attribute_id"]), []).append(r)

        # Accumulators
        entities_to_insert = []
        values_to_insert = {dt: [] for dt in VALUE_TABLE_BY_TYPE}
        pending_value_updates = []   # (table, set_values, id)
        pending_value_deletes = []   # (table, id)
        pending_entity_updates = {}  # entity_id -> set_values (for existing entities)
        written = set()
        status_written = set()
        taxonomy_written = set()
        entity_cache = {}

        def _uuid():
            return str(uuid.uuid4())

        def _new_value_row(pid, attr_id, **extra):
            base = {'id': _uuid(), 'product_id': pid, 'attribute_id': attr_id, 'locale': '',
                    'source': 'IMPORT', 'version': 1, 'created_at': now, 'updated_at': now}
            base.update(extra)
            return base

        def _apply_value(pid, ad, raw, mode):
            if _blank(raw):
                return
            k = (pid, ad["id"])
            if k in written:
                return
            written.add(k)
            dt = ad["data_type"]

            if dt == 'MULTISELECT':
                rows = existing_vals['MULTISELECT'].get(k) or []
                if rows and mode == 'add_only':
                    return
                for r in rows:
                    pending_value_deletes.append(('pim_value_multiselect', r["id"]))
                existing_vals['MULTISELECT'][k] = []
                parts = [p.strip() for chunk in str(raw).split(';') for p in chunk.split(',')]
                for key in dict.fromkeys(to_value_key(p) for p in parts if p):
                    values_to_insert['MULTISELECT'].append(_new_value_row(pid, ad["id"], ref_value_key=key))
                return

            rows = existing_vals.get(dt, {}).get(k) or []
            existing = rows[0] if rows else None
            if existing is not None:
                is_empty = dt == 'TEXT' and _blank(existing.get("value"))
                if not is_empty and mode == 'add_only':
                    return
                set_values = {'source': 'IMPORT', 'version': (existing.get("version") or 1) + 1}
                if dt == 'NUMBER':
                    set_values['value'] = float(str(raw))
                elif dt == 'BOOLEAN':
                    set_values['value'] = str(raw).strip().lower() in ('true', '1', 'yes')
                elif dt == 'DATE':
                    set_values['value'] = _date.fromisoformat(str(raw).strip())
                elif dt == 'SELECT':
                    set_values['ref_value_key'] = to_value_key(str(raw))
                elif dt == 'REFERENCE':
                    set_values['ref_id'] = str(raw)
                else:
                    set_values['value'] = str(raw)
                pending_value_updates.append((VALUE_TABLE_BY_TYPE[dt], set_values, existing["id"]))
                return

            if dt == 'NUMBER':
                values_to_insert['NUMBER'].append(_new_value_row(pid, ad["id"], value=float(str(raw)), currency='', price_type='', territory=''))
            elif dt == 'BOOLEAN':
                values_to_insert['BOOLEAN'].append(_new_value_row(pid, ad["id"], value=str(raw).strip().lower() in ('true', '1', 'yes')))
            elif dt == 'DATE':
                values_to_insert['DATE'].append(_new_value_row(pid, ad["id"], value=_date.fromisoformat(str(raw).strip())))
            elif dt == 'SELECT':
                values_to_insert['SELECT'].append(_new_value_row(pid, ad["id"], ref_value_key=to_value_key(str(raw))))
            elif dt == 'REFERENCE':
                values_to_insert['REFERENCE'].append(_new_value_row(pid, ad["id"], ref_table=ad["reference_entity_id"] or '', ref_id=str(raw)))
            else:
                values_to_insert['TEXT'].append(_new_value_row(pid, ad["id"], value=str(raw)))

        def _entity_get(state, field):
            if state['is_new']:
                return state['insert_dict'][field]
            return pending_entity_updates.get(state['id'], {}).get(field, state['existing'][field])

        def _entity_set(state, field, value):
            if state['is_new']:
                state['insert_dict'][field] = value
                state['insert_dict']['updated_at'] = now
            else:
                pending_entity_updates.setdefault(state['id'], {})[field] = value

        def _apply_status(state, status, mode):
            if _blank(status):
                return
            if state['id'] in status_written:
                return
            status_written.add(state['id'])
            current = None if state.get('status_is_default') else _entity_get(state, 'status')
            if _blank(current) or mode == 'overwrite':
                _entity_set(state, 'status', str(status).strip())
                state['status_is_default'] = False

        def _apply_taxonomy(state, node_id, mode):
            if not node_id:
                return
            if state['id'] in taxonomy_written:
                return
            taxonomy_written.add(state['id'])
            if not _entity_get(state, 'taxonomy_node_id') or mode == 'overwrite':
                _entity_set(state, 'taxonomy_node_id', node_id)

        def _get_or_create_entity(tier_code, key, parent_id):
            ck = (tier_code, key)
            if ck in entity_cache:
                return entity_cache[ck]
            ent = existing_by_tier.get(ck)
            if ent is not None:
                state = {'id': ent["id"], 'existing': ent, 'is_new': False}
                entity_cache[ck] = state
                return state
            pid = _uuid()
            insert_dict = {'id': pid, 'entity_type_id': tier_code, 'taxonomy_node_id': None,
                           'parent_id': parent_id, 'status': 'Draft', 'active': True, 'promoted': False,
                           'published_at': None, 'created_at': now, 'updated_at': now}
            entities_to_insert.append(insert_dict)
            values_to_insert['TEXT'].append(_new_value_row(pid, id_attr_id, value=key))
            written.add((pid, id_attr_id))
            state = {'id': pid, 'insert_dict': insert_dict, 'is_new': True, 'status_is_default': True}
            entity_cache[ck] = state
            return state

        def _apply_level(state, lvl):
            if label_attr is not None and not _blank(lvl.label):
                _apply_value(state['id'], label_attr, lvl.label, lvl.label_mode or 'overwrite')
            _apply_status(state, lvl.status, lvl.status_mode or 'overwrite')
            _apply_taxonomy(state, lvl.taxonomy_node_id, lvl.taxonomy_mode or 'overwrite')
            for v in (lvl.values or []):
                ad = attr_defs.get(v.attribute_id)
                if ad is not None:
                    _apply_value(state['id'], ad, v.value, v.mode or 'overwrite')

        for row in valid_rows:
            # Walk levels top→leaf: create-or-reuse each, threading parent_id from
            # the previous non-blank level. Blank intermediate levels are skipped
            # (parent chain continues from the last present ancestor).
            row_levels = [(tc, lvl) for tc, lvl in _row_levels(row)
                          if lvl is not None and not _blank(lvl.key)]
            root_state = None
            parent_state = None
            leaf_state = None
            leaf_is_new = False
            for tier_code, lvl in row_levels:
                key = str(lvl.key).strip()
                ck = (tier_code, key)
                is_new = ck not in entity_cache and ck not in existing_by_tier
                state = _get_or_create_entity(tier_code, key, parent_state['id'] if parent_state else None)
                _apply_level(state, lvl)
                if root_state is None:
                    root_state = state
                parent_state = state
                leaf_state = state
                leaf_is_new = is_new  # last level wins → the leaf/item grain

            # Propagate the root level's taxonomy down to all descendants (parity
            # with the old product→variant/item taxonomy cascade).
            root_lvl = row_levels[0][1] if row_levels else None
            if root_lvl is not None and root_lvl.taxonomy_node_id and root_state is not None:
                root_taxonomy = _entity_get(root_state, 'taxonomy_node_id')
                if root_taxonomy:
                    taxonomy_mode = root_lvl.taxonomy_mode or 'overwrite'
                    for tier_code, lvl in row_levels[1:]:
                        st = entity_cache.get((tier_code, str(lvl.key).strip()))
                        if st is not None:
                            _apply_taxonomy(st, root_taxonomy, taxonomy_mode)

            if leaf_state is not None:
                if leaf_is_new:
                    inserted += 1
                else:
                    updated += 1

        # Persist in a single transaction: deletes → updates → inserts
        try:
            for tbl, rid in pending_value_deletes:
                pim_sql.execute(self.db, f'DELETE FROM {self._t(tbl)} WHERE "id" = :id', {"id": rid})
            for tbl, set_values, rid in pending_value_updates:
                sql, params = pim_sql.build_update(self.entity_name, tbl, set_values, where='"id" = :id', where_params={"id": rid})
                pim_sql.execute(self.db, sql, params)
            for eid, set_values in pending_entity_updates.items():
                sql, params = pim_sql.build_update(self.entity_name, ENTITY_TBL, set_values, where='"id" = :id', where_params={"id": eid})
                pim_sql.execute(self.db, sql, params)
            self._bulk_insert_rows(ENTITY_TBL, entities_to_insert)
            for dt, rows in values_to_insert.items():
                self._bulk_insert_rows(VALUE_TABLE_BY_TYPE[dt], rows)
            db_commit_auto_rollback(db=self.db)
        except Exception as e:
            self.db.rollback()
            app_logger.exception(f"Flat import failed, rolled back: {e}")
            err_msg = str(e)[:500] + ("..." if len(str(e)) > 500 else "")
            for row in valid_rows:
                _fail(row.row_number, f"Import failed: {err_msg}")
            return {'inserted': 0, 'updated': 0, 'failed': len(errors), 'errors': errors}

        affected_ids = {s['id'] for s in entity_cache.values()}
        affected_ids |= {e['parent_id'] for e in entities_to_insert if e.get('parent_id')}
        try:
            self.refresh_flat_rows(list(affected_ids))
            db_commit_auto_rollback(db=self.db)
        except Exception as e:
            app_logger.exception(f"Flat import: flat table refresh failed: {e}")

        app_logger.info(f"Flat import complete: {inserted} inserted, {updated} updated, {len(errors)} failed")
        return {'inserted': inserted, 'updated': updated, 'failed': len(errors), 'errors': errors}

    def _fetch_two_in(self, sql, p1, v1, p2, v2):
        """SELECT with two expanding IN params."""
        v1, v2 = list(v1), list(v2)
        if not v1 or not v2:  # either empty IN → no rows
            return []
        stmt = text(sql).bindparams(bindparam(p1, expanding=True), bindparam(p2, expanding=True))
        return [dict(r) for r in self.db.execute(stmt, {p1: v1, p2: v2}).mappings().all()]
