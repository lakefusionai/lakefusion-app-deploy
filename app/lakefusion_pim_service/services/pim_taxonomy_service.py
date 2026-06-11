import traceback
from datetime import datetime
from sqlalchemy.orm import Session
from fastapi import HTTPException
from sqlalchemy import func
from lakefusion_utility.models.pim import (
    PimTaxonomyNode, PimTaxonomyNodeCreate, PimTaxonomyNodeUpdate,
    PimSpecificationConfig, PimResolvedSpecification, PimEntity,
    PimAttributeDefinition, PimAttributeOption, PimTaxonomyBulkImportRequest,
    to_value_key,
)
from lakefusion_utility.utils.app_db import db_commit_auto_rollback
from lakefusion_utility.utils.logging_utils import get_logger

app_logger = get_logger(__name__)


class PimTaxonomyService:
    def __init__(self, db: Session):
        self.db = db

    # ------------------------------------------------------------------
    # HELPER – resolve inherited attributes for a single node
    # ------------------------------------------------------------------
    def _resolve_inherited_for_node(self, node: PimTaxonomyNode):
        """
        Compute and upsert PimResolvedSpecification rows for a single node
        by walking its ancestor chain and collecting configs with inherit_to_children=True.
        Caller is responsible for committing after this returns.
        """
        # Clear any stale resolved rows for this node (handles reactivation)
        self.db.query(PimResolvedSpecification).filter(
            PimResolvedSpecification.taxonomy_node_id == node.id
        ).delete()

        # Build a map of all active nodes to walk the ancestor chain
        all_nodes = (
            self.db.query(PimTaxonomyNode)
            .filter(PimTaxonomyNode.is_active == True)
            .all()
        )
        ancestor_map = {n.id: n for n in all_nodes}

        # Walk from root down to this node
        current = node
        ancestors = []
        while current:
            ancestors.insert(0, current)
            current = ancestor_map.get(current.parent_id)

        # Collect resolved configs (closest ancestor wins for the same attribute)
        resolved = {}  # attribute_id -> (config, source_node)
        for ancestor in ancestors:
            configs = (
                self.db.query(PimSpecificationConfig)
                .filter(PimSpecificationConfig.taxonomy_node_id == ancestor.id)
                .all()
            )
            for cfg in configs:
                if ancestor.id == node.id:
                    resolved[cfg.attribute_id] = (cfg, ancestor)
                elif cfg.inherit_to_children:
                    if cfg.attribute_id not in resolved or resolved[cfg.attribute_id][1].level < ancestor.level:
                        resolved[cfg.attribute_id] = (cfg, ancestor)

        for attr_id, (cfg, source_node) in resolved.items():
            row = PimResolvedSpecification(
                taxonomy_node_id=node.id,
                attribute_id=attr_id,
                config_id=cfg.id,
                source_node_id=source_node.id,
                is_required=cfg.is_required,
                display_order=cfg.display_order,
            )
            self.db.add(row)

    # ------------------------------------------------------------------
    # BULK CREATE
    # ------------------------------------------------------------------
    def bulk_create_nodes(self, items: list[PimTaxonomyNodeCreate]):
        try:
            codes = [item.code for item in items]
            if len(codes) != len(set(codes)):
                raise HTTPException(status_code=400, detail="Duplicate codes found in the request.")

            # All rows share the same parent_id (set by frontend for the target node)
            parent_ids = {item.parent_id for item in items}

            # Pre-check existing codes under each parent
            for pid in parent_ids:
                existing = (
                    self.db.query(PimTaxonomyNode.code)
                    .filter(
                        PimTaxonomyNode.parent_id == pid,
                        PimTaxonomyNode.code.in_(codes),
                        PimTaxonomyNode.is_active == True,
                    )
                    .all()
                )
                existing_codes = {row.code for row in existing}
                if existing_codes:
                    raise HTTPException(
                        status_code=409,
                        detail=f"Taxonomy nodes already exist for codes: {', '.join(sorted(existing_codes))}",
                    )

            # Resolve parent info once per unique parent_id
            parent_map: dict[str | None, tuple[int, str]] = {}  # parent_id -> (level, path)
            for pid in parent_ids:
                if pid:
                    parent = self.db.query(PimTaxonomyNode).filter(PimTaxonomyNode.id == pid).first()
                    if not parent:
                        raise HTTPException(status_code=404, detail=f"Parent taxonomy node '{pid}' not found.")
                    parent_map[pid] = (parent.level + 1, parent.materialized_path)
                else:
                    parent_map[None] = (0, "")

            created = []
            errors = []
            for idx, data in enumerate(items):
                if not data.code or not data.label:
                    errors.append(f"Row {idx + 1}: code and label are required.")
                    continue

                level, parent_path = parent_map[data.parent_id]
                materialized_path = f"{parent_path}/{data.code}" if parent_path else data.code

                # Reactivate a previously soft-deleted node with the same (parent_id, code)
                existing_inactive = (
                    self.db.query(PimTaxonomyNode)
                    .filter(
                        PimTaxonomyNode.parent_id == data.parent_id,
                        PimTaxonomyNode.code == data.code,
                        PimTaxonomyNode.is_active == False,
                    )
                    .first()
                )
                if existing_inactive:
                    existing_inactive.is_active = True
                    existing_inactive.label = data.label
                    existing_inactive.display_order = data.display_order or 0
                    existing_inactive.materialized_path = materialized_path
                    existing_inactive.level = level
                    existing_inactive.updated_at = datetime.utcnow()
                    created.append(existing_inactive)
                    continue

                node = PimTaxonomyNode(
                    code=data.code,
                    label=data.label,
                    parent_id=data.parent_id,
                    materialized_path=materialized_path,
                    level=level,
                    display_order=data.display_order or 0,
                )
                self.db.add(node)
                created.append(node)

            if errors:
                self.db.rollback()
                raise HTTPException(status_code=400, detail=errors)

            db_commit_auto_rollback(db=self.db, raise_exception=True)
            for node in created:
                self.db.refresh(node)

            # Seed the resolved-attribute cache for every newly created/reactivated node
            for node in created:
                if node.parent_id:
                    self._resolve_inherited_for_node(node)
            db_commit_auto_rollback(db=self.db, raise_exception=True)

            return {"created": len(created), "nodes": created}

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
            # Check for duplicate code under same parent (active nodes)
            existing_active = (
                self.db.query(PimTaxonomyNode)
                .filter(
                    PimTaxonomyNode.parent_id == data.parent_id,
                    PimTaxonomyNode.code == data.code,
                    PimTaxonomyNode.is_active == True,
                )
                .first()
            )
            if existing_active:
                raise HTTPException(
                    status_code=409,
                    detail=f"Taxonomy node with code '{data.code}' already exists under this parent.",
                )

            # Compute level and materialized_path
            if data.parent_id:
                parent = self.db.query(PimTaxonomyNode).filter(PimTaxonomyNode.id == data.parent_id).first()
                if not parent:
                    raise HTTPException(status_code=404, detail="Parent taxonomy node not found.")
                level = parent.level + 1
                materialized_path = f"{parent.materialized_path}/{data.code}"
            else:
                level = 0
                materialized_path = data.code

            # Reactivate a previously soft-deleted node with the same (parent_id, code)
            existing_inactive = (
                self.db.query(PimTaxonomyNode)
                .filter(
                    PimTaxonomyNode.parent_id == data.parent_id,
                    PimTaxonomyNode.code == data.code,
                    PimTaxonomyNode.is_active == False,
                )
                .first()
            )
            if existing_inactive:
                existing_inactive.is_active = True
                existing_inactive.label = data.label
                existing_inactive.display_order = data.display_order or 0
                existing_inactive.materialized_path = materialized_path
                existing_inactive.level = level
                existing_inactive.updated_at = datetime.utcnow()
                db_commit_auto_rollback(db=self.db)
                self.db.refresh(existing_inactive)
                if existing_inactive.parent_id:
                    self._resolve_inherited_for_node(existing_inactive)
                    db_commit_auto_rollback(db=self.db)
                return existing_inactive

            node = PimTaxonomyNode(
                code=data.code,
                label=data.label,
                parent_id=data.parent_id,
                materialized_path=materialized_path,
                level=level,
                display_order=data.display_order or 0,
            )
            self.db.add(node)
            db_commit_auto_rollback(db=self.db)
            self.db.refresh(node)
            if node.parent_id:
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
            nodes = (
                self.db.query(PimTaxonomyNode)
                .filter(PimTaxonomyNode.is_active == True)
                .order_by(PimTaxonomyNode.level, PimTaxonomyNode.display_order)
                .all()
            )
            counts = (
                self.db.query(PimEntity.taxonomy_node_id, func.count(PimEntity.id).label("cnt"))
                .filter(PimEntity.active == True)
                .group_by(PimEntity.taxonomy_node_id)
                .all()
            )
            count_map = {row.taxonomy_node_id: row.cnt for row in counts}
            return self._build_tree(nodes, count_map)
        except Exception as e:
            app_logger.exception(f"Error fetching taxonomy tree: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    def _build_tree(self, nodes, count_map=None):
        if count_map is None:
            count_map = {}
        node_map = {n.id: {
            "id": n.id,
            "code": n.code,
            "label": n.label,
            "parent_id": n.parent_id,
            "materialized_path": n.materialized_path,
            "level": n.level,
            "display_order": n.display_order,
            "product_count": count_map.get(n.id, 0),
            "children": [],
        } for n in nodes}

        roots = []
        for n in nodes:
            if n.parent_id and n.parent_id in node_map:
                node_map[n.parent_id]["children"].append(node_map[n.id])
            else:
                roots.append(node_map[n.id])

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
            node = self.db.query(PimTaxonomyNode).filter(PimTaxonomyNode.id == node_id).first()
            if not node:
                raise HTTPException(status_code=404, detail="Taxonomy node not found.")

            descendants = (
                self.db.query(PimTaxonomyNode)
                .filter(
                    PimTaxonomyNode.materialized_path.like(f"{node.materialized_path}%"),
                    PimTaxonomyNode.is_active == True,
                )
                .order_by(PimTaxonomyNode.level, PimTaxonomyNode.display_order)
                .all()
            )
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
            node = (
                self.db.query(PimTaxonomyNode)
                .filter(PimTaxonomyNode.id == node_id, PimTaxonomyNode.is_active == True)
                .first()
            )
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
            node = self.db.query(PimTaxonomyNode).filter(PimTaxonomyNode.id == node_id).first()
            if not node:
                raise HTTPException(status_code=404, detail="Taxonomy node not found.")

            parent_changed = False
            if data.code is not None:
                node.code = data.code
            if data.label is not None:
                node.label = data.label
            if data.display_order is not None:
                node.display_order = data.display_order
            if data.is_active is not None:
                node.is_active = data.is_active

            # Handle parent_id change
            # FE sends parent_id as: UUID string (reparent), empty string (move to root), or omits it (no change)
            if data.parent_id is not None:
                new_parent_id = data.parent_id if data.parent_id else None
                old_parent_id = str(node.parent_id) if node.parent_id else None
                if new_parent_id != old_parent_id:
                    parent_changed = True
                    node.parent_id = new_parent_id

            # Recompute materialized_path and level if parent or code changed
            if parent_changed or data.code is not None:
                old_path = node.materialized_path
                if node.parent_id:
                    parent = self.db.query(PimTaxonomyNode).filter(PimTaxonomyNode.id == node.parent_id).first()
                    if parent:
                        node.level = parent.level + 1
                        node.materialized_path = f"{parent.materialized_path}/{node.code}"
                    else:
                        node.level = 0
                        node.materialized_path = f"/{node.code}"
                else:
                    node.level = 0
                    node.materialized_path = f"/{node.code}"

                # Update descendant paths and levels
                descendants = (
                    self.db.query(PimTaxonomyNode)
                    .filter(PimTaxonomyNode.materialized_path.like(f"{old_path}/%"))
                    .all()
                )
                for desc in descendants:
                    desc.materialized_path = desc.materialized_path.replace(old_path, node.materialized_path, 1)
                    desc.level = len(desc.materialized_path.strip("/").split("/")) - 1

            node.updated_at = datetime.utcnow()
            db_commit_auto_rollback(db=self.db)
            self.db.refresh(node)
            return node

        except HTTPException:
            raise
        except Exception as e:
            message = traceback.format_exc()
            app_logger.exception(f"Unable to update taxonomy node. Reason - {message}")
            raise HTTPException(status_code=500, detail=f"Failed to update taxonomy node: {str(e)}")

    # ------------------------------------------------------------------
    # DELETE (soft)
    # ------------------------------------------------------------------
    def delete_node(self, node_id: str):
        try:
            node = self.db.query(PimTaxonomyNode).filter(PimTaxonomyNode.id == node_id).first()
            if not node:
                raise HTTPException(status_code=404, detail="Taxonomy node not found.")

            # Soft-delete node and all descendants
            descendants = (
                self.db.query(PimTaxonomyNode)
                .filter(PimTaxonomyNode.materialized_path.like(f"{node.materialized_path}%"))
                .all()
            )
            for desc in descendants:
                desc.is_active = False
                desc.updated_at = datetime.utcnow()

            db_commit_auto_rollback(db=self.db)
            return {"message": f"Taxonomy node '{node.code}' and {len(descendants) - 1} descendants soft-deleted."}

        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Unable to delete taxonomy node: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # DELETE CHILDREN (soft) — keeps the node, removes all descendants
    # ------------------------------------------------------------------
    def delete_children(self, node_id: str):
        try:
            node = self.db.query(PimTaxonomyNode).filter(PimTaxonomyNode.id == node_id).first()
            if not node:
                raise HTTPException(status_code=404, detail="Taxonomy node not found.")

            # Soft-delete only strict descendants (path starts with node path + "/")
            descendants = (
                self.db.query(PimTaxonomyNode)
                .filter(
                    PimTaxonomyNode.materialized_path.like(f"{node.materialized_path}/%"),
                    PimTaxonomyNode.is_active == True,
                )
                .all()
            )

            if not descendants:
                return {"message": f"Node '{node.code}' has no active children to delete."}

            for desc in descendants:
                desc.is_active = False
                desc.updated_at = datetime.utcnow()

            db_commit_auto_rollback(db=self.db)
            return {"message": f"{len(descendants)} descendant(s) of '{node.code}' soft-deleted."}

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
            # Clear existing cache
            self.db.query(PimResolvedSpecification).delete()

            # Get all active nodes ordered by level (parents first)
            nodes = (
                self.db.query(PimTaxonomyNode)
                .filter(PimTaxonomyNode.is_active == True)
                .order_by(PimTaxonomyNode.level)
                .all()
            )

            # For each node, collect directly assigned + inherited configs
            for node in nodes:
                path_parts = node.materialized_path.split("/")

                # Walk from root to this node, collecting configs
                resolved = {}  # attribute_id -> (config, source_node)
                ancestor_nodes = (
                    self.db.query(PimTaxonomyNode)
                    .filter(PimTaxonomyNode.is_active == True)
                    .all()
                )
                ancestor_map = {n.id: n for n in ancestor_nodes}

                # Walk the path from root to this node
                current = node
                ancestors = []
                while current:
                    ancestors.insert(0, current)
                    current = ancestor_map.get(current.parent_id)

                for ancestor in ancestors:
                    configs = (
                        self.db.query(PimSpecificationConfig)
                        .filter(PimSpecificationConfig.taxonomy_node_id == ancestor.id)
                        .all()
                    )
                    for cfg in configs:
                        if ancestor.id == node.id:
                            # Direct config always applies
                            resolved[cfg.attribute_id] = (cfg, ancestor)
                        elif cfg.inherit_to_children:
                            # Inherited config — may be overridden by descendant
                            if cfg.attribute_id not in resolved or resolved[cfg.attribute_id][1].level < ancestor.level:
                                resolved[cfg.attribute_id] = (cfg, ancestor)

                # Insert resolved rows
                for attr_id, (cfg, source_node) in resolved.items():
                    row = PimResolvedSpecification(
                        taxonomy_node_id=node.id,
                        attribute_id=attr_id,
                        config_id=cfg.id,
                        source_node_id=source_node.id,
                        is_required=cfg.is_required,
                        display_order=cfg.display_order,
                    )
                    self.db.add(row)

            db_commit_auto_rollback(db=self.db)
            count = self.db.query(PimResolvedSpecification).count()
            return {"message": f"Resolved attribute config cache rebuilt. {count} rows created."}

        except Exception as e:
            message = traceback.format_exc()
            app_logger.exception(f"Unable to rebuild resolved cache. Reason - {message}")
            raise HTTPException(status_code=500, detail=f"Failed to rebuild cache: {str(e)}")

    # ------------------------------------------------------------------
    # BULK TAXONOMY IMPORT  (SCRUM-1790)
    # ------------------------------------------------------------------
    def bulk_import_taxonomy(self, data: PimTaxonomyBulkImportRequest):
        """
        Bulk import taxonomy nodes, attribute definitions, and bindings
        in a single transaction. Much faster than individual API calls.
        """
        import uuid

        errors = []
        nodes_created = 0
        attributes_created = 0
        bindings_created = 0

        def _uuid():
            return str(uuid.uuid4())

        # ── Phase 1: Create taxonomy nodes (ordered by depth) ──
        app_logger.info(f"Bulk taxonomy import: {len(data.nodes)} nodes, {len(data.attributes)} attributes, {len(data.bindings)} bindings")

        # Build parent-code → depth map for ordering
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

        # Check existing nodes
        existing_codes = set()
        existing_query = self.db.query(PimTaxonomyNode.code).filter(PimTaxonomyNode.is_active == True).all()
        for row in existing_query:
            existing_codes.add(row.code)

        code_to_id = {}
        # Seed existing code→id
        for row in self.db.query(PimTaxonomyNode.code, PimTaxonomyNode.id).filter(PimTaxonomyNode.is_active == True).all():
            code_to_id[row.code] = row.id

        nodes_to_insert = []
        now = datetime.utcnow()

        for node in sorted_nodes:
            code = node.code.lower().replace(" ", "_")
            if code in existing_codes:
                continue  # skip duplicates

            node_id = _uuid()
            parent_id = code_to_id.get(node.parent_code) if node.parent_code else None

            # Calculate level and path
            level = 0
            path = code
            if parent_id and node.parent_code:
                parent_row = self.db.query(PimTaxonomyNode).filter(PimTaxonomyNode.id == parent_id).first()
                if parent_row:
                    level = parent_row.level + 1
                    path = f"{parent_row.materialized_path}/{code}"

            nodes_to_insert.append({
                'id': node_id,
                'code': code,
                'label': node.label,
                'parent_id': parent_id,
                'level': level,
                'materialized_path': path,
                'display_order': 0,
                'is_active': True,
                'created_at': now,
                'updated_at': now,
            })
            code_to_id[node.code] = node_id
            code_to_id[code] = node_id  # also store lowercase version
            existing_codes.add(code)
            nodes_created += 1

        # ── Phases 2-4: All inserts in a single try block with rollback on failure ──
        try:
            # Phase 2a: Insert nodes
            if nodes_to_insert:
                self.db.execute(PimTaxonomyNode.__table__.insert(), nodes_to_insert)
                app_logger.info(f"Bulk taxonomy: inserted {len(nodes_to_insert)} nodes")

            # Phase 2b: Build and insert attribute definitions
            existing_attr_codes = set()
            for row in self.db.query(PimAttributeDefinition.code).all():
                existing_attr_codes.add(row.code)

            attr_code_to_id = {}
            for row in self.db.query(PimAttributeDefinition.code, PimAttributeDefinition.id).all():
                attr_code_to_id[row.code] = row.id

            # Pre-fetch valid tier codes for level validation
            from lakefusion_utility.models.pim import PimEntityTier
            valid_tier_codes = {t.code for t in self.db.query(PimEntityTier.code).all()}

            attrs_to_insert = []
            options_to_insert = []
            for attr in (data.attributes or []):
                code = attr.code.lower().replace(" ", "_")
                if code in existing_attr_codes:
                    continue

                attr_id = _uuid()
                scope = attr.scope or 'specifications'
                group = attr.group or ('Specifications' if scope == 'specifications' else 'General')
                level = attr.level or 'PRODUCT'
                # Validate level
                if level != 'ALL':
                    for lvl in level.split(","):
                        lvl = lvl.strip()
                        if lvl and lvl != 'ALL' and lvl not in valid_tier_codes:
                            raise HTTPException(status_code=400, detail=f"Invalid level '{lvl}' for attribute '{code}'. Valid: {', '.join(sorted(valid_tier_codes))}, ALL")
                dtype = attr.data_type or 'TEXT'
                attrs_to_insert.append({
                    'id': attr_id,
                    'code': code,
                    'label': attr.label,
                    'data_type': dtype,
                    'level': level,
                    'scope': scope,
                    'description': attr.description,
                    'reference_entity_id': attr.reference_entity_id if dtype == 'REFERENCE' else None,
                    'is_localizable': False,
                    'is_system': False,
                    'group': group,
                    'display_order': attr.display_order or 0,
                    'created_at': now,
                    'updated_at': now,
                })
                attr_code_to_id[attr.code] = attr_id
                attr_code_to_id[code] = attr_id
                existing_attr_codes.add(code)
                attributes_created += 1

                # Seed options for SELECT/MULTISELECT from the spec CSV "Values" column.
                if dtype in ('SELECT', 'MULTISELECT') and attr.options:
                    seen_keys = set()
                    for idx, o in enumerate(attr.options):
                        olabel = (o.label or '').strip()
                        if not olabel:
                            continue
                        okey = (o.value_key or '').strip() or to_value_key(olabel)
                        base, n = okey, 2
                        while okey in seen_keys:
                            okey = f"{base}_{n}"; n += 1
                        seen_keys.add(okey)
                        options_to_insert.append({
                            'id': _uuid(),
                            'attribute_id': attr_id,
                            'value_key': okey,
                            'label': olabel,
                            'display_order': o.display_order if o.display_order is not None else idx,
                            'is_active': o.is_active if o.is_active is not None else True,
                            'created_at': now,
                            'updated_at': now,
                        })

            if attrs_to_insert:
                self.db.execute(PimAttributeDefinition.__table__.insert(), attrs_to_insert)
                app_logger.info(f"Bulk taxonomy: inserted {len(attrs_to_insert)} attributes")
            if options_to_insert:
                self.db.execute(PimAttributeOption.__table__.insert(), options_to_insert)
                app_logger.info(f"Bulk taxonomy: inserted {len(options_to_insert)} attribute options")

            # Phase 3: Build and insert bindings
            existing_bindings = set()
            for row in self.db.query(
                PimSpecificationConfig.taxonomy_node_id,
                PimSpecificationConfig.attribute_id
            ).all():
                existing_bindings.add(f"{row.taxonomy_node_id}:{row.attribute_id}")

            bindings_to_insert = []
            for bind in (data.bindings or []):
                node_id = code_to_id.get(bind.category_code) or code_to_id.get(bind.category_code.lower().replace(" ", "_"))
                attr_id = attr_code_to_id.get(bind.attribute_code) or attr_code_to_id.get(bind.attribute_code.lower().replace(" ", "_"))

                if not node_id or not attr_id:
                    continue

                binding_key = f"{node_id}:{attr_id}"
                if binding_key in existing_bindings:
                    continue

                bindings_to_insert.append({
                    'id': _uuid(),
                    'taxonomy_node_id': node_id,
                    'attribute_id': attr_id,
                    'is_required': bind.is_required or False,
                    'inherit_to_children': bind.inherit_to_children if bind.inherit_to_children is not None else True,
                    'override_allowed': True,
                    'display_order': 0,
                    'created_at': now,
                    'updated_at': now,
                })
                existing_bindings.add(binding_key)
                bindings_created += 1

            if bindings_to_insert:
                self.db.execute(PimSpecificationConfig.__table__.insert(), bindings_to_insert)
                app_logger.info(f"Bulk taxonomy: inserted {len(bindings_to_insert)} bindings")

            # Phase 4: Single commit
            db_commit_auto_rollback(db=self.db)
            app_logger.info(f"Bulk taxonomy import complete: {nodes_created} nodes, {attributes_created} attrs, {bindings_created} bindings")

        except Exception as e:
            # Rollback entire transaction — DB stays clean
            self.db.rollback()
            app_logger.exception(f"Bulk taxonomy import failed, rolled back: {str(e)}")
            errors.append(f"Import failed (rolled back): {str(e)}")
            return {"nodes_created": 0, "attributes_created": 0, "bindings_created": 0, "errors": errors}

        # ── Phase 5: Rebuild resolved config cache (after successful commit) ──
        if bindings_created > 0:
            try:
                self.rebuild_resolved_cache()
                app_logger.info("Resolved config cache rebuilt after bulk import")
            except Exception as e:
                errors.append(f"Cache rebuild failed (data saved, cache stale): {str(e)}")

        return {
            "nodes_created": nodes_created,
            "attributes_created": attributes_created,
            "bindings_created": bindings_created,
            "errors": errors,
        }
