import traceback
import uuid
from datetime import datetime
from sqlalchemy.orm import Session
from fastapi import HTTPException
from app.lakefusion_pim_service.utils import pim_sql
from lakefusion_utility.models.pim import (
    PimTaxonomyNodeCreate, PimTaxonomyNodeUpdate,
    PimTaxonomyBulkImportRequest,
    to_value_key,
)
from lakefusion_utility.utils.app_db import db_commit_auto_rollback
from lakefusion_utility.utils.logging_utils import get_logger

app_logger = get_logger(__name__)

# SCRUM-1929 Phase 6.4 — converted from ORM to raw SQL against the Delta-synced
# tables. Ancestor walks use an in-memory map built from a flat SELECT of all
# active nodes (no recursive CTE needed); descendant updates use materialized_path
# LIKE; resolved-cache rebuild is the same closest-ancestor-wins algorithm over
# rows. Synced tables have no FK cascade — resolved rows are cleared explicitly.
# entity_name is threaded into the constructor.

NODE_TBL = "pim_taxonomy_node"
CONFIG_TBL = "pim_specification_config"
RESOLVED_TBL = "pim_resolved_specification"
ENTITY_TBL = "pim_entity"
ATTR_TBL = "pim_attribute_definition"


class PimTaxonomyService:
    def __init__(self, db: Session, entity_name: str):
        self.db = db
        self.entity_name = entity_name
        self._node = pim_sql.pim_tbl(entity_name, NODE_TBL)
        self._config = pim_sql.pim_tbl(entity_name, CONFIG_TBL)
        self._resolved = pim_sql.pim_tbl(entity_name, RESOLVED_TBL)
        self._entity = pim_sql.pim_tbl(entity_name, ENTITY_TBL)
        self._attr = pim_sql.pim_tbl(entity_name, ATTR_TBL)

    # ------------------------------------------------------------------
    # HELPER – resolve inherited attributes for a single node (no commit)
    # ------------------------------------------------------------------
    def _resolve_inherited_for_node(self, node: dict):
        """Compute + upsert resolved rows for one node by walking its ancestor
        chain and collecting configs with inherit_to_children=True (closest
        ancestor wins). Caller commits."""
        pim_sql.execute(
            self.db, f'DELETE FROM {self._resolved} WHERE "taxonomy_node_id" = :nid',
            {"nid": node["id"]},
        )

        all_nodes = pim_sql.fetch_all(
            self.db, f'SELECT "id","parent_id","level" FROM {self._node} WHERE "is_active" = TRUE'
        )
        ancestor_map = {n["id"]: n for n in all_nodes}

        current = node
        ancestors = []
        while current:
            ancestors.insert(0, current)
            current = ancestor_map.get(current.get("parent_id"))

        resolved = {}  # attribute_id -> (config_row, source_node)
        for ancestor in ancestors:
            configs = pim_sql.fetch_all(
                self.db, f'SELECT * FROM {self._config} WHERE "taxonomy_node_id" = :nid',
                {"nid": ancestor["id"]},
            )
            for cfg in configs:
                if ancestor["id"] == node["id"]:
                    resolved[cfg["attribute_id"]] = (cfg, ancestor)
                elif cfg["inherit_to_children"]:
                    cur = resolved.get(cfg["attribute_id"])
                    if cur is None or cur[1]["level"] < ancestor["level"]:
                        resolved[cfg["attribute_id"]] = (cfg, ancestor)

        for attr_id, (cfg, source_node) in resolved.items():
            sql, params = pim_sql.build_insert(
                self.entity_name, RESOLVED_TBL,
                {
                    "taxonomy_node_id": node["id"],
                    "attribute_id": attr_id,
                    "config_id": cfg["id"],
                    "source_node_id": source_node["id"],
                    "is_required": cfg["is_required"],
                    "display_order": cfg["display_order"],
                    "resolved_at": pim_sql.utcnow(),
                },
                # pim_resolved_specification has NO created_at/updated_at — it uses
                # resolved_at (set above). Disable both auto-timestamps (matches
                # rebuild_resolved_cache).
                auto_created_at=False, auto_updated_at=False,
            )
            pim_sql.execute(self.db, sql, params)

    # ------------------------------------------------------------------
    # BULK CREATE
    # ------------------------------------------------------------------
    def bulk_create_nodes(self, items):
        try:
            codes = [to_value_key(item.code) for item in items]
            if len(codes) != len(set(codes)):
                raise HTTPException(status_code=400, detail="Duplicate codes found in the request.")

            parent_ids = {item.parent_id for item in items}

            # Pre-check existing active codes under each parent
            for pid in parent_ids:
                if pid:
                    existing = pim_sql.fetch_all(
                        self.db,
                        f'SELECT "code" FROM {self._node} WHERE "parent_id" = :pid '
                        f'AND "is_active" = TRUE',
                        {"pid": pid},
                    )
                else:
                    existing = pim_sql.fetch_all(
                        self.db,
                        f'SELECT "code" FROM {self._node} WHERE "parent_id" IS NULL '
                        f'AND "is_active" = TRUE',
                    )
                existing_codes = {r["code"] for r in existing if r["code"] in codes}
                if existing_codes:
                    raise HTTPException(
                        status_code=409,
                        detail=f"Taxonomy nodes already exist for codes: {', '.join(sorted(existing_codes))}",
                    )

            # Resolve parent info per unique parent_id
            parent_map = {}  # parent_id -> (level, path)
            for pid in parent_ids:
                if pid:
                    parent = pim_sql.fetch_one(
                        self.db, f'SELECT "level","materialized_path" FROM {self._node} WHERE "id" = :id',
                        {"id": pid},
                    )
                    if not parent:
                        raise HTTPException(status_code=404, detail=f"Parent taxonomy node '{pid}' not found.")
                    parent_map[pid] = (parent["level"] + 1, parent["materialized_path"])
                else:
                    parent_map[None] = (0, "")

            created_ids = []
            errors = []
            for idx, data in enumerate(items):
                if not data.code or not data.label:
                    errors.append(f"Row {idx + 1}: code and label are required.")
                    continue
                code = to_value_key(data.code)
                level, parent_path = parent_map[data.parent_id]
                materialized_path = f"{parent_path}/{code}" if parent_path else code

                existing_inactive = pim_sql.fetch_one(
                    self.db,
                    f'SELECT "id" FROM {self._node} WHERE '
                    f'{"\"parent_id\" = :pid" if data.parent_id else "\"parent_id\" IS NULL"} '
                    f'AND "code" = :code AND "is_active" = FALSE',
                    ({"pid": data.parent_id, "code": code} if data.parent_id else {"code": code}),
                )
                if existing_inactive:
                    sql, params = pim_sql.build_update(
                        self.entity_name, NODE_TBL,
                        {"is_active": True, "label": data.label, "display_order": data.display_order or 0,
                         "materialized_path": materialized_path, "level": level},
                        where='"id" = :id', where_params={"id": existing_inactive["id"]},
                    )
                    pim_sql.execute(self.db, sql, params)
                    created_ids.append((existing_inactive["id"], data.parent_id))
                    continue

                sql, params = pim_sql.build_insert(
                    self.entity_name, NODE_TBL,
                    {"code": code, "label": data.label, "parent_id": data.parent_id,
                     "materialized_path": materialized_path, "level": level,
                     "display_order": data.display_order or 0},
                )
                pim_sql.execute(self.db, sql, params)
                created_ids.append((params["id"], data.parent_id))

            if errors:
                self.db.rollback()
                raise HTTPException(status_code=400, detail=errors)

            db_commit_auto_rollback(db=self.db, raise_exception=True)

            # Seed resolved cache for each created/reactivated node with a parent
            for node_id, pid in created_ids:
                if pid:
                    node = pim_sql.fetch_one(
                        self.db, f'SELECT "id","parent_id","level" FROM {self._node} WHERE "id" = :id',
                        {"id": node_id},
                    )
                    if node:
                        self._resolve_inherited_for_node(node)
            db_commit_auto_rollback(db=self.db, raise_exception=True)

            nodes = [
                pim_sql.coerce_row(NODE_TBL, pim_sql.fetch_one(self.db, f'SELECT * FROM {self._node} WHERE "id" = :id', {"id": nid}))
                for nid, _ in created_ids
            ]
            return {"created": len(created_ids), "nodes": nodes}

        except HTTPException:
            raise
        except Exception as e:
            message = traceback.format_exc()
            app_logger.exception(f"Unable to bulk create taxonomy nodes. Reason - {message}")
            raise HTTPException(status_code=500, detail=f"Failed to bulk create taxonomy nodes: {str(e)}")

    # ------------------------------------------------------------------
    # CREATE
    # ------------------------------------------------------------------
    def create_node(self, data: PimTaxonomyNodeCreate):
        try:
            code = to_value_key(data.code)
            if data.parent_id:
                active_dup = pim_sql.fetch_one(
                    self.db,
                    f'SELECT "id" FROM {self._node} WHERE "parent_id" = :pid AND "code" = :code '
                    f'AND "is_active" = TRUE',
                    {"pid": data.parent_id, "code": code},
                )
            else:
                active_dup = pim_sql.fetch_one(
                    self.db,
                    f'SELECT "id" FROM {self._node} WHERE "parent_id" IS NULL AND "code" = :code '
                    f'AND "is_active" = TRUE',
                    {"code": code},
                )
            if active_dup:
                raise HTTPException(
                    status_code=409,
                    detail=f"Taxonomy node with code '{code}' already exists under this parent.",
                )

            if data.parent_id:
                parent = pim_sql.fetch_one(
                    self.db, f'SELECT "level","materialized_path" FROM {self._node} WHERE "id" = :id',
                    {"id": data.parent_id},
                )
                if not parent:
                    raise HTTPException(status_code=404, detail="Parent taxonomy node not found.")
                level = parent["level"] + 1
                materialized_path = f"{parent['materialized_path']}/{code}"
            else:
                level = 0
                materialized_path = code

            # Reactivate a soft-deleted node with the same (parent_id, code)
            if data.parent_id:
                inactive = pim_sql.fetch_one(
                    self.db,
                    f'SELECT "id" FROM {self._node} WHERE "parent_id" = :pid AND "code" = :code '
                    f'AND "is_active" = FALSE',
                    {"pid": data.parent_id, "code": code},
                )
            else:
                inactive = pim_sql.fetch_one(
                    self.db,
                    f'SELECT "id" FROM {self._node} WHERE "parent_id" IS NULL AND "code" = :code '
                    f'AND "is_active" = FALSE',
                    {"code": code},
                )
            if inactive:
                sql, params = pim_sql.build_update(
                    self.entity_name, NODE_TBL,
                    {"is_active": True, "label": data.label, "display_order": data.display_order or 0,
                     "materialized_path": materialized_path, "level": level},
                    where='"id" = :id', where_params={"id": inactive["id"]},
                )
                pim_sql.execute(self.db, sql, params)
                db_commit_auto_rollback(db=self.db)
                node = pim_sql.coerce_row(NODE_TBL, pim_sql.fetch_one(self.db, f'SELECT * FROM {self._node} WHERE "id" = :id', {"id": inactive["id"]}))
                if node and node["parent_id"]:
                    self._resolve_inherited_for_node(node)
                    db_commit_auto_rollback(db=self.db)
                return node

            sql, params = pim_sql.build_insert(
                self.entity_name, NODE_TBL,
                {"code": code, "label": data.label, "parent_id": data.parent_id,
                 "materialized_path": materialized_path, "level": level,
                 "display_order": data.display_order or 0},
            )
            pim_sql.execute(self.db, sql, params)
            db_commit_auto_rollback(db=self.db)
            node = pim_sql.coerce_row(NODE_TBL, pim_sql.fetch_one(self.db, f'SELECT * FROM {self._node} WHERE "id" = :id', {"id": params["id"]}))
            if node and node["parent_id"]:
                self._resolve_inherited_for_node(node)
                db_commit_auto_rollback(db=self.db)
            return node

        except HTTPException:
            raise
        except Exception as e:
            message = traceback.format_exc()
            app_logger.exception(f"Unable to create taxonomy node. Reason - {message}")
            raise HTTPException(status_code=500, detail=f"Failed to create taxonomy node: {str(e)}")

    # ------------------------------------------------------------------
    # READ – full tree
    # ------------------------------------------------------------------
    def get_tree(self):
        try:
            nodes = pim_sql.coerce_rows(NODE_TBL, pim_sql.fetch_all(
                self.db,
                f'SELECT * FROM {self._node} WHERE "is_active" = TRUE '
                f'ORDER BY "level", "display_order"',
            ))
            counts = pim_sql.fetch_all(
                self.db,
                f'SELECT "taxonomy_node_id", COUNT("id") AS cnt FROM {self._entity} '
                f'WHERE "active" = TRUE GROUP BY "taxonomy_node_id"',
            )
            count_map = {r["taxonomy_node_id"]: r["cnt"] for r in counts}
            return self._build_tree(nodes, count_map)
        except Exception as e:
            app_logger.exception(f"Error fetching taxonomy tree: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    def _build_tree(self, nodes, count_map=None):
        if count_map is None:
            count_map = {}
        node_map = {n["id"]: {
            "id": n["id"], "code": n["code"], "label": n["label"],
            "parent_id": n["parent_id"], "materialized_path": n["materialized_path"],
            "level": n["level"], "display_order": n["display_order"],
            "product_count": count_map.get(n["id"], 0), "children": [],
        } for n in nodes}

        roots = []
        for n in nodes:
            if n["parent_id"] and n["parent_id"] in node_map:
                node_map[n["parent_id"]]["children"].append(node_map[n["id"]])
            else:
                roots.append(node_map[n["id"]])

        def _compute_totals(node):
            total = node["product_count"]
            for child in node["children"]:
                _compute_totals(child)
                total += child["total_product_count"]
            node["total_product_count"] = total

        for root in roots:
            _compute_totals(root)
        return roots

    # ------------------------------------------------------------------
    # READ – subtree
    # ------------------------------------------------------------------
    def get_subtree(self, node_id: str):
        try:
            node = pim_sql.fetch_one(
                self.db, f'SELECT "materialized_path" FROM {self._node} WHERE "id" = :id', {"id": node_id}
            )
            if not node:
                raise HTTPException(status_code=404, detail="Taxonomy node not found.")
            descendants = pim_sql.coerce_rows(NODE_TBL, pim_sql.fetch_all(
                self.db,
                f'SELECT * FROM {self._node} WHERE "materialized_path" LIKE :pat '
                f'AND "is_active" = TRUE ORDER BY "level", "display_order"',
                {"pat": f"{node['materialized_path']}%"},
            ))
            return self._build_tree(descendants)
        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Error fetching subtree: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # READ – single node
    # ------------------------------------------------------------------
    def get_node(self, node_id: str):
        try:
            node = pim_sql.coerce_row(NODE_TBL, pim_sql.fetch_one(
                self.db, f'SELECT * FROM {self._node} WHERE "id" = :id AND "is_active" = TRUE', {"id": node_id}
            ))
            if not node:
                raise HTTPException(status_code=404, detail="Taxonomy node not found.")
            return node
        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Error fetching taxonomy node: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # UPDATE
    # ------------------------------------------------------------------
    def update_node(self, node_id: str, data: PimTaxonomyNodeUpdate):
        try:
            node = pim_sql.fetch_one(self.db, f'SELECT * FROM {self._node} WHERE "id" = :id', {"id": node_id})
            if not node:
                raise HTTPException(status_code=404, detail="Taxonomy node not found.")

            new_code = to_value_key(data.code) if data.code is not None else node["code"]
            set_values = {}
            if data.code is not None:
                set_values["code"] = new_code
            if data.label is not None:
                set_values["label"] = data.label
            if data.display_order is not None:
                set_values["display_order"] = data.display_order
            if data.is_active is not None:
                set_values["is_active"] = data.is_active

            parent_changed = False
            new_parent_id = node["parent_id"]
            if data.parent_id is not None:
                candidate = data.parent_id if data.parent_id else None
                old_parent_id = str(node["parent_id"]) if node["parent_id"] else None
                if candidate != old_parent_id:
                    parent_changed = True
                    new_parent_id = candidate
                    set_values["parent_id"] = candidate

            old_path = node["materialized_path"]
            if parent_changed or data.code is not None:
                if new_parent_id:
                    parent = pim_sql.fetch_one(
                        self.db, f'SELECT "level","materialized_path" FROM {self._node} WHERE "id" = :id',
                        {"id": new_parent_id},
                    )
                    if parent:
                        set_values["level"] = parent["level"] + 1
                        set_values["materialized_path"] = f"{parent['materialized_path']}/{new_code}"
                    else:
                        set_values["level"] = 0
                        set_values["materialized_path"] = f"/{new_code}"
                else:
                    set_values["level"] = 0
                    set_values["materialized_path"] = f"/{new_code}"

            if set_values:
                sql, params = pim_sql.build_update(
                    self.entity_name, NODE_TBL, set_values, where='"id" = :id', where_params={"id": node_id},
                )
                pim_sql.execute(self.db, sql, params)

            # Update descendant paths + levels (path replace) if path changed
            if (parent_changed or data.code is not None) and "materialized_path" in set_values:
                new_path = set_values["materialized_path"]
                descendants = pim_sql.fetch_all(
                    self.db, f'SELECT "id","materialized_path" FROM {self._node} WHERE "materialized_path" LIKE :pat',
                    {"pat": f"{old_path}/%"},
                )
                for desc in descendants:
                    dpath = desc["materialized_path"].replace(old_path, new_path, 1)
                    dlevel = len(dpath.strip("/").split("/")) - 1
                    dsql, dparams = pim_sql.build_update(
                        self.entity_name, NODE_TBL, {"materialized_path": dpath, "level": dlevel},
                        where='"id" = :id', where_params={"id": desc["id"]},
                    )
                    pim_sql.execute(self.db, dsql, dparams)

            db_commit_auto_rollback(db=self.db)
            return pim_sql.fetch_one(self.db, f'SELECT * FROM {self._node} WHERE "id" = :id', {"id": node_id})

        except HTTPException:
            raise
        except Exception as e:
            message = traceback.format_exc()
            app_logger.exception(f"Unable to update taxonomy node. Reason - {message}")
            raise HTTPException(status_code=500, detail=f"Failed to update taxonomy node: {str(e)}")

    # ------------------------------------------------------------------
    # DELETE (soft) — node + all descendants
    # ------------------------------------------------------------------
    def delete_node(self, node_id: str):
        try:
            node = pim_sql.fetch_one(
                self.db, f'SELECT "code","materialized_path" FROM {self._node} WHERE "id" = :id', {"id": node_id}
            )
            if not node:
                raise HTTPException(status_code=404, detail="Taxonomy node not found.")

            descendants = pim_sql.fetch_all(
                self.db, f'SELECT "id" FROM {self._node} WHERE "materialized_path" LIKE :pat',
                {"pat": f"{node['materialized_path']}%"},
            )
            now = pim_sql.utcnow()
            pim_sql.execute(
                self.db,
                f'UPDATE {self._node} SET "is_active" = FALSE, "updated_at" = :ts '
                f'WHERE "materialized_path" LIKE :pat',
                {"ts": now, "pat": f"{node['materialized_path']}%"},
            )
            db_commit_auto_rollback(db=self.db)
            return {"message": f"Taxonomy node '{node['code']}' and {len(descendants) - 1} descendants soft-deleted."}

        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Unable to delete taxonomy node: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # DELETE CHILDREN (soft) — keeps the node, removes descendants
    # ------------------------------------------------------------------
    def delete_children(self, node_id: str):
        try:
            node = pim_sql.fetch_one(
                self.db, f'SELECT "code","materialized_path" FROM {self._node} WHERE "id" = :id', {"id": node_id}
            )
            if not node:
                raise HTTPException(status_code=404, detail="Taxonomy node not found.")

            pat = f"{node['materialized_path']}/%"
            descendants = pim_sql.fetch_all(
                self.db,
                f'SELECT "id" FROM {self._node} WHERE "materialized_path" LIKE :pat AND "is_active" = TRUE',
                {"pat": pat},
            )
            if not descendants:
                return {"message": f"Node '{node['code']}' has no active children to delete."}

            pim_sql.execute(
                self.db,
                f'UPDATE {self._node} SET "is_active" = FALSE, "updated_at" = :ts '
                f'WHERE "materialized_path" LIKE :pat AND "is_active" = TRUE',
                {"ts": pim_sql.utcnow(), "pat": pat},
            )
            db_commit_auto_rollback(db=self.db)
            return {"message": f"{len(descendants)} descendant(s) of '{node['code']}' soft-deleted."}

        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Unable to delete children of taxonomy node: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # REBUILD resolved attribute config cache
    # ------------------------------------------------------------------
    def rebuild_resolved_cache(self):
        try:
            pim_sql.execute(self.db, f'DELETE FROM {self._resolved}')

            nodes = pim_sql.fetch_all(
                self.db,
                f'SELECT "id","parent_id","level","materialized_path" FROM {self._node} '
                f'WHERE "is_active" = TRUE ORDER BY "level"',
            )
            ancestor_map = {n["id"]: n for n in nodes}

            # Pre-fetch all configs grouped by node for efficiency
            all_configs = pim_sql.fetch_all(self.db, f'SELECT * FROM {self._config}')
            configs_by_node = {}
            for c in all_configs:
                configs_by_node.setdefault(c["taxonomy_node_id"], []).append(c)

            for node in nodes:
                current = node
                ancestors = []
                while current:
                    ancestors.insert(0, current)
                    current = ancestor_map.get(current.get("parent_id"))

                resolved = {}  # attribute_id -> (config, source_node)
                for ancestor in ancestors:
                    for cfg in configs_by_node.get(ancestor["id"], []):
                        if ancestor["id"] == node["id"]:
                            resolved[cfg["attribute_id"]] = (cfg, ancestor)
                        elif cfg["inherit_to_children"]:
                            cur = resolved.get(cfg["attribute_id"])
                            if cur is None or cur[1]["level"] < ancestor["level"]:
                                resolved[cfg["attribute_id"]] = (cfg, ancestor)

                for attr_id, (cfg, source_node) in resolved.items():
                    sql, params = pim_sql.build_insert(
                        self.entity_name, RESOLVED_TBL,
                        {
                            "taxonomy_node_id": node["id"], "attribute_id": attr_id,
                            "config_id": cfg["id"], "source_node_id": source_node["id"],
                            "is_required": cfg["is_required"], "display_order": cfg["display_order"],
                            "resolved_at": pim_sql.utcnow(),
                        },
                        # pim_resolved_specification has NO created_at/updated_at — it
                        # uses resolved_at (set above). Disable both auto-timestamps.
                        auto_created_at=False, auto_updated_at=False,
                    )
                    pim_sql.execute(self.db, sql, params)

            db_commit_auto_rollback(db=self.db)
            count = pim_sql.fetch_scalar(self.db, f'SELECT COUNT(*) FROM {self._resolved}') or 0
            return {"message": f"Resolved attribute config cache rebuilt. {count} rows created."}

        except Exception as e:
            message = traceback.format_exc()
            app_logger.exception(f"Unable to rebuild resolved cache. Reason - {message}")
            raise HTTPException(status_code=500, detail=f"Failed to rebuild cache: {str(e)}")

    # ------------------------------------------------------------------
    # BULK TAXONOMY IMPORT  (SCRUM-1790)
    # ------------------------------------------------------------------
    def bulk_import_taxonomy(self, data: PimTaxonomyBulkImportRequest):
        """Bulk import taxonomy nodes + spec bindings in one transaction.
        Specs must already exist (imported via Specifications tab); unresolved
        codes are reported as errors."""
        errors = []
        nodes_created = 0
        bindings_created = 0

        def _uuid():
            return str(uuid.uuid4())

        app_logger.info(f"Bulk taxonomy import: {len(data.nodes)} nodes, {len(data.bindings)} bindings")

        parent_map = {n.code: n.parent_code for n in data.nodes}
        def get_depth(code):
            depth = 0
            cur = parent_map.get(code)
            visited = set()
            while cur and cur in parent_map and cur not in visited:
                visited.add(cur)
                depth += 1
                cur = parent_map.get(cur)
            return depth

        sorted_nodes = sorted(data.nodes, key=lambda n: get_depth(n.code))

        existing_rows = pim_sql.fetch_all(
            self.db, f'SELECT "code","id" FROM {self._node} WHERE "is_active" = TRUE'
        )
        existing_codes = {r["code"] for r in existing_rows}
        code_to_id = {r["code"]: r["id"] for r in existing_rows}

        nodes_to_insert = []
        now = datetime.utcnow()
        for node in sorted_nodes:
            code = to_value_key(node.code)
            if code in existing_codes:
                continue
            node_id = _uuid()
            parent_id = code_to_id.get(to_value_key(node.parent_code)) if node.parent_code else None
            level = 0
            path = code
            if parent_id and node.parent_code:
                parent_row = pim_sql.fetch_one(
                    self.db, f'SELECT "level","materialized_path" FROM {self._node} WHERE "id" = :id',
                    {"id": parent_id},
                )
                if parent_row:
                    level = parent_row["level"] + 1
                    path = f"{parent_row['materialized_path']}/{code}"
            nodes_to_insert.append({
                "id": node_id, "code": code, "label": node.label, "parent_id": parent_id,
                "level": level, "materialized_path": path, "display_order": 0,
                "is_active": True, "created_at": now, "updated_at": now,
            })
            code_to_id[node.code] = node_id
            code_to_id[code] = node_id
            existing_codes.add(code)
            nodes_created += 1

        try:
            # Batched insert (reference-entity pattern) — node dicts are fully built
            # (id/level/materialized_path computed above), safe to insert in one batch.
            if nodes_to_insert:
                cols = list({k for r in nodes_to_insert for k in r.keys()})
                norm = [{c: r.get(c) for c in cols} for r in nodes_to_insert]
                pim_sql.bulk_insert(self.db, self.entity_name, NODE_TBL, norm)
                app_logger.info(f"Bulk taxonomy: inserted {len(nodes_to_insert)} nodes")

            attr_rows = pim_sql.fetch_all(self.db, f'SELECT "code","id" FROM {self._attr}')
            attr_code_to_id = {r["code"]: r["id"] for r in attr_rows}

            existing_bindings = set()
            for r in pim_sql.fetch_all(
                self.db, f'SELECT "taxonomy_node_id","attribute_id" FROM {self._config}'
            ):
                existing_bindings.add(f"{r['taxonomy_node_id']}:{r['attribute_id']}")

            unknown_specs = set()
            bindings_to_insert = []
            for bind in (data.bindings or []):
                node_id = code_to_id.get(bind.category_code) or code_to_id.get(to_value_key(bind.category_code))
                attr_id = attr_code_to_id.get(bind.attribute_code) or attr_code_to_id.get(to_value_key(bind.attribute_code))
                if not attr_id:
                    unknown_specs.add(bind.attribute_code)
                    continue
                if not node_id:
                    continue
                binding_key = f"{node_id}:{attr_id}"
                if binding_key in existing_bindings:
                    continue
                bindings_to_insert.append({
                    "id": _uuid(), "taxonomy_node_id": node_id, "attribute_id": attr_id,
                    "is_required": bind.is_required or False,
                    "inherit_to_children": bind.inherit_to_children if bind.inherit_to_children is not None else True,
                    "override_allowed": True, "display_order": 0,
                    "created_at": now, "updated_at": now,
                })
                existing_bindings.add(binding_key)
                bindings_created += 1
            # Batched insert (reference-entity pattern).
            if bindings_to_insert:
                pim_sql.bulk_insert(self.db, self.entity_name, CONFIG_TBL, bindings_to_insert)

            if unknown_specs:
                errors.append(
                    f"Skipped bindings for {len(unknown_specs)} unknown spec code(s): "
                    f"{', '.join(sorted(unknown_specs))}. Import them via the Specifications tab first."
                )

            db_commit_auto_rollback(db=self.db)
            app_logger.info(f"Bulk taxonomy import complete: {nodes_created} nodes, {bindings_created} bindings")

        except Exception as e:
            self.db.rollback()
            app_logger.exception(f"Bulk taxonomy import failed, rolled back: {str(e)}")
            errors.append(f"Import failed (rolled back): {str(e)}")
            return {"nodes_created": 0, "bindings_created": 0, "errors": errors}

        if bindings_created > 0:
            try:
                self.rebuild_resolved_cache()
                app_logger.info("Resolved config cache rebuilt after bulk import")
            except Exception as e:
                errors.append(f"Cache rebuild failed (data saved, cache stale): {str(e)}")

        return {"nodes_created": nodes_created, "bindings_created": bindings_created, "errors": errors}
