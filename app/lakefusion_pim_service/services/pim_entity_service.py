import traceback
from datetime import datetime, date as _date
from sqlalchemy.orm import Session
from sqlalchemy import text, func
from fastapi import HTTPException
from lakefusion_utility.models.pim import (
    PimEntity, PimEntityFlat, PimEntityCreate, PimEntityUpdate,
    PimTaxonomyNode, PimResolvedSpecification,
    PimAttributeDefinition, PimAttributeOption, PimSpecificationConfig,
    PimValueText, PimValueNumber, PimValueBoolean,
    PimValueDate, PimValueSelect, PimValueMultiselect,
    PimValueReference,
    PimImportNode, PimBulkImportRequest,
    to_value_key,
)
from lakefusion_utility.utils.app_db import db_commit_auto_rollback
from lakefusion_utility.utils.logging_utils import get_logger

app_logger = get_logger(__name__)

# Map of all typed value tables for completeness / cleanup queries
VALUE_TABLES = [
    PimValueText, PimValueNumber, PimValueBoolean,
    PimValueDate, PimValueSelect, PimValueMultiselect,
    PimValueReference,
]


class PimEntityService:
    def __init__(self, db: Session):
        self.db = db
        # Cache identifier and label attribute definitions (lazy loaded)
        self._identifier_attr = None
        self._label_attr = None

    def _get_identifier_attr(self):
        """Get the attribute definition marked as the identifier (is_identifier=True)."""
        if self._identifier_attr is None:
            self._identifier_attr = self.db.query(PimAttributeDefinition).filter(
                PimAttributeDefinition.is_identifier == True
            ).first()
        return self._identifier_attr

    def _get_label_attr(self):
        """Get the attribute definition marked as the label (is_label=True)."""
        if self._label_attr is None:
            self._label_attr = self.db.query(PimAttributeDefinition).filter(
                PimAttributeDefinition.is_label == True
            ).first()
        return self._label_attr

    def _get_identifier_value(self, entity_id: str) -> str | None:
        """Get the identifier value for an entity from EAV."""
        attr = self._get_identifier_attr()
        if not attr:
            return None
        val = self.db.query(PimValueText).filter(
            PimValueText.product_id == entity_id,
            PimValueText.attribute_id == attr.id,
            PimValueText.locale == '',
        ).first()
        return val.value if val else None

    def _get_label_value(self, entity_id: str) -> str | None:
        """Get the label value for an entity from EAV."""
        attr = self._get_label_attr()
        if not attr:
            return None
        val = self.db.query(PimValueText).filter(
            PimValueText.product_id == entity_id,
            PimValueText.attribute_id == attr.id,
            PimValueText.locale == '',
        ).first()
        return val.value if val else None

    def _write_identifier_value(self, entity_id: str, value: str):
        """Write the identifier value to EAV (upsert)."""
        attr = self._get_identifier_attr()
        if not attr or not value:
            return
        existing = self.db.query(PimValueText).filter(
            PimValueText.product_id == entity_id,
            PimValueText.attribute_id == attr.id,
            PimValueText.locale == '',
        ).first()
        if existing:
            existing.value = value
            existing.updated_at = datetime.utcnow()
        else:
            from lakefusion_utility.models.pim import _uuid
            self.db.add(PimValueText(
                id=_uuid(), product_id=entity_id, attribute_id=attr.id,
                value=value, locale='', source='USER_SET',
            ))

    def _write_label_value(self, entity_id: str, value: str):
        """Write the label value to EAV (upsert)."""
        attr = self._get_label_attr()
        if not attr or not value:
            return
        existing = self.db.query(PimValueText).filter(
            PimValueText.product_id == entity_id,
            PimValueText.attribute_id == attr.id,
            PimValueText.locale == '',
        ).first()
        if existing:
            existing.value = value
            existing.updated_at = datetime.utcnow()
        else:
            from lakefusion_utility.models.pim import _uuid
            self.db.add(PimValueText(
                id=_uuid(), product_id=entity_id, attribute_id=attr.id,
                value=value, locale='', source='USER_SET',
            ))

    def _check_identifier_unique(self, value: str, exclude_id: str = None):
        """Check identifier uniqueness across all entities via EAV."""
        attr = self._get_identifier_attr()
        if not attr or not value:
            return
        query = self.db.query(PimValueText).filter(
            PimValueText.attribute_id == attr.id,
            PimValueText.value == value,
            PimValueText.locale == '',
        )
        if exclude_id:
            query = query.filter(PimValueText.product_id != exclude_id)
        if query.first():
            raise HTTPException(
                status_code=409,
                detail=f"Product with identifier '{value}' already exists.",
            )

    def refresh_flat_rows(self, entity_ids: list):
        """Incrementally refresh pim_entity_flat for specific entities."""
        if not entity_ids:
            return
        id_attr = self._get_identifier_attr()
        label_attr = self._get_label_attr()
        id_attr_id = id_attr.id if id_attr else None
        label_attr_id = label_attr.id if label_attr else None

        for eid in entity_ids:
            entity = self.db.query(PimEntity).filter(PimEntity.id == eid).first()
            if not entity:
                # Entity was deleted — remove from flat table
                self.db.query(PimEntityFlat).filter(PimEntityFlat.id == eid).delete()
                continue

            # Get identifier and label from EAV
            id_val = None
            label_val = None
            if id_attr_id:
                row = self.db.query(PimValueText.value).filter(
                    PimValueText.product_id == eid,
                    PimValueText.attribute_id == id_attr_id,
                    PimValueText.locale == '',
                ).first()
                id_val = row[0] if row else None
            if label_attr_id:
                row = self.db.query(PimValueText.value).filter(
                    PimValueText.product_id == eid,
                    PimValueText.attribute_id == label_attr_id,
                    PimValueText.locale == '',
                ).first()
                label_val = row[0] if row else None

            # Get category info
            cat_label = None
            cat_path = None
            if entity.taxonomy_node_id:
                node = self.db.query(PimTaxonomyNode).filter(
                    PimTaxonomyNode.id == entity.taxonomy_node_id
                ).first()
                if node:
                    cat_label = node.label
                    cat_path = node.materialized_path

            # Child count
            child_count = self.db.query(func.count(PimEntity.id)).filter(
                PimEntity.parent_id == eid, PimEntity.active == True
            ).scalar() or 0

            # Upsert flat row
            flat = self.db.query(PimEntityFlat).filter(PimEntityFlat.id == eid).first()
            if flat:
                flat.entity_type_id = entity.entity_type_id
                flat.taxonomy_node_id = entity.taxonomy_node_id
                flat.parent_id = entity.parent_id
                flat.status = entity.status
                flat.active = entity.active
                flat.promoted = entity.promoted
                flat.published_at = entity.published_at
                flat.identifier = id_val
                flat.label = label_val
                flat.category_label = cat_label
                flat.category_path = cat_path
                flat.child_count = child_count
                flat.created_at = entity.created_at
                flat.updated_at = entity.updated_at
            else:
                self.db.add(PimEntityFlat(
                    id=eid, entity_type_id=entity.entity_type_id,
                    taxonomy_node_id=entity.taxonomy_node_id, parent_id=entity.parent_id,
                    status=entity.status, active=entity.active, promoted=entity.promoted,
                    published_at=entity.published_at,
                    identifier=id_val, label=label_val,
                    category_label=cat_label, category_path=cat_path,
                    child_count=child_count,
                    created_at=entity.created_at, updated_at=entity.updated_at,
                ))

    # ------------------------------------------------------------------
    # CREATE PRODUCT  (SCRUM-1569)
    # ------------------------------------------------------------------
    def create_product(self, data: PimEntityCreate):
        try:
            # Validate unique identifier
            if data.identifier_value:
                self._check_identifier_unique(data.identifier_value)

            # Validate taxonomy node exists
            node = self.db.query(PimTaxonomyNode).filter(
                PimTaxonomyNode.id == data.taxonomy_node_id,
                PimTaxonomyNode.is_active == True,
            ).first()
            if not node:
                raise HTTPException(status_code=404, detail="Taxonomy node not found.")

            # Validate parent if provided
            if data.parent_id:
                parent = self.db.query(PimEntity).filter(
                    PimEntity.id == data.parent_id, PimEntity.active == True
                ).first()
                if not parent:
                    raise HTTPException(status_code=404, detail="Parent product not found.")
                from lakefusion_utility.models.pim import PimEntityTier
                parent_tier = self.db.query(PimEntityTier).filter(
                    PimEntityTier.code == parent.entity_type_id
                ).first()
                if parent_tier and parent_tier.is_leaf:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Cannot add children under a {parent_tier.label} (leaf tier).",
                    )

            product = PimEntity(
                entity_type_id=data.entity_type_id,
                taxonomy_node_id=data.taxonomy_node_id,
                parent_id=data.parent_id,
                status=data.status or 'Active',
                active=data.active if data.active is not None else True,
            )
            self.db.add(product)
            self.db.flush()  # get the product.id

            # Write identifier and label as EAV values
            if data.identifier_value:
                self._write_identifier_value(product.id, data.identifier_value)
            if data.label_value:
                self._write_label_value(product.id, data.label_value)

            db_commit_auto_rollback(db=self.db)
            self.db.refresh(product)

            # Refresh flat table
            self.refresh_flat_rows([product.id])
            db_commit_auto_rollback(db=self.db)

            return self.db.query(PimEntityFlat).filter(PimEntityFlat.id == product.id).first()

        except HTTPException:
            raise
        except Exception as e:
            message = traceback.format_exc()
            app_logger.exception(f"Unable to create product. Reason - {message}")
            raise HTTPException(status_code=500, detail=f"Failed to create product: {str(e)}")

    # ------------------------------------------------------------------
    # LIST PRODUCTS  (SCRUM-1573)
    # ------------------------------------------------------------------
    # Sortable column mapping — uses flat table for fast reads
    SORT_COLUMNS = {
        'identifier': PimEntityFlat.identifier,
        'label': PimEntityFlat.label,
        'sku': PimEntityFlat.identifier,      # backward compat alias
        'status': PimEntityFlat.status,
        'entity_type_id': PimEntityFlat.entity_type_id,
        'created_at': PimEntityFlat.created_at,
        'updated_at': PimEntityFlat.updated_at,
    }

    def list_products(self, page: int = 1, page_size: int = 20,
                      entity_type_id: str = None, status: str = None,
                      taxonomy_node_id: str = None, search: str = None,
                      promoted: bool = None,
                      sort_by: str = None, sort_dir: str = 'desc',
                      completeness_min: float = None, completeness_max: float = None):
        try:
            # Read from flat table for fast catalog list views
            query = self.db.query(PimEntityFlat).filter(PimEntityFlat.active == True)

            if not entity_type_id:
                query = query.filter(PimEntityFlat.parent_id == None)

            if entity_type_id:
                query = query.filter(PimEntityFlat.entity_type_id == entity_type_id)
            if status:
                statuses = [s.strip() for s in status.split(",")]
                if len(statuses) == 1:
                    query = query.filter(PimEntityFlat.status == statuses[0])
                else:
                    query = query.filter(PimEntityFlat.status.in_(statuses))
            if taxonomy_node_id:
                node = self.db.query(PimTaxonomyNode).filter(PimTaxonomyNode.id == taxonomy_node_id).first()
                if node:
                    descendant_ids = [
                        n.id for n in self.db.query(PimTaxonomyNode.id)
                        .filter(PimTaxonomyNode.materialized_path.like(f"{node.materialized_path}%"))
                        .all()
                    ]
                    query = query.filter(PimEntityFlat.taxonomy_node_id.in_(descendant_ids))
                else:
                    query = query.filter(PimEntityFlat.taxonomy_node_id == taxonomy_node_id)
            if promoted is not None:
                query = query.filter(PimEntityFlat.promoted == promoted)
            if search:
                query = query.filter(
                    (PimEntityFlat.identifier.ilike(f"%{search}%")) |
                    (PimEntityFlat.label.ilike(f"%{search}%"))
                )

            # Apply sort
            sort_col = self.SORT_COLUMNS.get(sort_by, PimEntityFlat.created_at)
            order = sort_col.asc() if sort_dir == 'asc' else sort_col.desc()

            # If completeness filter requested, compute for all matching products
            if completeness_min is not None or completeness_max is not None:
                all_products = query.order_by(order).all()
                completeness_map = self._batch_compute_completeness([p.id for p in all_products])
                filtered = []
                for p in all_products:
                    c = completeness_map.get(p.id, 100.0)
                    if completeness_min is not None and c < completeness_min:
                        continue
                    if completeness_max is not None and c > completeness_max:
                        continue
                    filtered.append((p, c))
                total = len(filtered)
                page_slice = filtered[(page - 1) * page_size: page * page_size]
                products = [p for p, _ in page_slice]
                pre_computed_completeness = {p.id: c for p, c in page_slice}
            else:
                total = query.count()
                products = (
                    query.order_by(order)
                    .offset((page - 1) * page_size)
                    .limit(page_size)
                    .all()
                )
                pre_computed_completeness = None

            if not products:
                return {"items": [], "total": total, "page": page, "page_size": page_size, "entity_types": []}

            product_ids = [p.id for p in products]

            # Batch compute completeness for the page if not already done
            if pre_computed_completeness is None:
                completeness_map = self._batch_compute_completeness(product_ids)
            else:
                completeness_map = pre_computed_completeness

            # Enrich: batch-fetch List prices for display + price range across children
            price_map = {}
            if product_ids:
                list_prices = (
                    self.db.query(PimValueNumber)
                    .filter(PimValueNumber.product_id.in_(product_ids), PimValueNumber.price_type == "List")
                    .all()
                )
                for lp in list_prices:
                    price_map[lp.product_id] = {"list_price": float(lp.value), "currency": lp.currency or "USD"}

                child_prices = (
                    self.db.query(
                        PimEntity.parent_id,
                        func.min(PimValueNumber.value).label("price_min"),
                        func.max(PimValueNumber.value).label("price_max"),
                    )
                    .join(PimValueNumber, PimValueNumber.product_id == PimEntity.id)
                    .filter(PimEntity.parent_id.in_(product_ids), PimEntity.active == True, PimValueNumber.price_type == "List")
                    .group_by(PimEntity.parent_id)
                    .all()
                )
                for cp in child_prices:
                    if cp.parent_id in price_map:
                        price_map[cp.parent_id]["price_min"] = float(cp.price_min) if cp.price_min else None
                        price_map[cp.parent_id]["price_max"] = float(cp.price_max) if cp.price_max else None
                    else:
                        price_map[cp.parent_id] = {
                            "list_price": None, "currency": "USD",
                            "price_min": float(cp.price_min) if cp.price_min else None,
                            "price_max": float(cp.price_max) if cp.price_max else None,
                        }

            # Build response — flat table already has identifier, label, category info, child count
            items = []
            for flat in products:
                price_info = price_map.get(flat.id, {})
                items.append({
                    "id": flat.id,
                    "entity_type_id": flat.entity_type_id,
                    "identifier": flat.identifier,
                    "label": flat.label,
                    "taxonomy_node_id": flat.taxonomy_node_id,
                    "parent_id": flat.parent_id,
                    "status": flat.status,
                    "active": flat.active,
                    "promoted": flat.promoted,
                    "published_at": flat.published_at,
                    "created_at": flat.created_at,
                    "updated_at": flat.updated_at,
                    "category_label": flat.category_label,
                    "category_path": flat.category_path,
                    "child_count": flat.child_count or 0,
                    "completeness": completeness_map.get(flat.id, 100.0),
                    "list_price": price_info.get("list_price"),
                    "price_currency": price_info.get("currency"),
                    "price_min": price_info.get("price_min"),
                    "price_max": price_info.get("price_max"),
                })

            entity_types = [
                row[0] for row in
                self.db.query(PimEntityFlat.entity_type_id)
                .filter(PimEntityFlat.active == True)
                .distinct()
                .all()
            ]

            return {
                "items": items, "total": total, "page": page, "page_size": page_size,
                "entity_types": sorted(entity_types),
            }

        except Exception as e:
            app_logger.exception(f"Error listing products: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # GET PRODUCT DETAIL  (SCRUM-1573)
    # ------------------------------------------------------------------
    def get_product(self, product_id: str):
        try:
            product = (
                self.db.query(PimEntity)
                .filter(PimEntity.id == product_id, PimEntity.active == True)
                .first()
            )
            if not product:
                raise HTTPException(status_code=404, detail="Product not found.")

            # Build detail dict with children + completeness
            children = (
                self.db.query(PimEntityFlat)
                .filter(PimEntityFlat.parent_id == product_id, PimEntityFlat.active == True)
                .all()
            )
            completeness_info = self._compute_completeness(product_id)

            id_val = self._get_identifier_value(product.id)
            label_val = self._get_label_value(product.id)
            return {
                "id": product.id,
                "entity_type_id": product.entity_type_id,
                "identifier": id_val,
                "label": label_val,
                "taxonomy_node_id": product.taxonomy_node_id,
                "parent_id": product.parent_id,
                "status": product.status,
                "active": product.active,
                "promoted": product.promoted,
                "published_at": product.published_at,
                "created_at": product.created_at,
                "updated_at": product.updated_at,
                "children": children,
                "completeness": completeness_info["completeness"],
            }

        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Error fetching product: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # UPDATE PRODUCT  (SCRUM-1573)
    # ------------------------------------------------------------------
    def update_product(self, product_id: str, data: PimEntityUpdate):
        try:
            product = self.db.query(PimEntity).filter(
                PimEntity.id == product_id, PimEntity.active == True
            ).first()
            if not product:
                raise HTTPException(status_code=404, detail="Product not found.")

            old_taxonomy_node_id = product.taxonomy_node_id

            if data.taxonomy_node_id is not None:
                node = self.db.query(PimTaxonomyNode).filter(
                    PimTaxonomyNode.id == data.taxonomy_node_id,
                    PimTaxonomyNode.is_active == True,
                ).first()
                if not node:
                    raise HTTPException(status_code=404, detail="Taxonomy node not found.")
                product.taxonomy_node_id = data.taxonomy_node_id
            if data.status is not None:
                product.status = data.status
            if data.active is not None:
                product.active = data.active

            product.updated_at = datetime.utcnow()
            db_commit_auto_rollback(db=self.db)

            # Handle category reassignment — delete orphaned values
            if data.taxonomy_node_id and data.taxonomy_node_id != old_taxonomy_node_id:
                self._handle_category_reassignment(product, old_taxonomy_node_id, data.taxonomy_node_id)

            self.db.refresh(product)

            # Refresh flat table
            self.refresh_flat_rows([product.id])
            db_commit_auto_rollback(db=self.db)

            return self.db.query(PimEntityFlat).filter(PimEntityFlat.id == product.id).first()

        except HTTPException:
            raise
        except Exception as e:
            message = traceback.format_exc()
            app_logger.exception(f"Unable to update product. Reason - {message}")
            raise HTTPException(status_code=500, detail=f"Failed to update product: {str(e)}")

    # ------------------------------------------------------------------
    # DELETE PRODUCT (soft-delete)  (SCRUM-1573)
    # ------------------------------------------------------------------
    def delete_product(self, product_id: str):
        try:
            product = self.db.query(PimEntity).filter(
                PimEntity.id == product_id, PimEntity.active == True
            ).first()
            if not product:
                raise HTTPException(status_code=404, detail="Product not found.")

            # Soft-delete product and all children (SKUs)
            product.active = False
            product.updated_at = datetime.utcnow()
            children = (
                self.db.query(PimEntity)
                .filter(PimEntity.parent_id == product_id, PimEntity.active == True)
                .all()
            )
            for child in children:
                child.active = False
                child.updated_at = datetime.utcnow()

            db_commit_auto_rollback(db=self.db)

            # Refresh flat table for deleted entities
            all_ids = [product.id] + [c.id for c in children]
            self.refresh_flat_rows(all_ids)
            db_commit_auto_rollback(db=self.db)

            identifier = self._get_identifier_value(product.id)
            return {
                "message": f"Product '{identifier or product.id}' and {len(children)} child items soft-deleted."
            }

        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Unable to delete product: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # PUBLISH PRODUCTS  (SCRUM-1572)
    # ------------------------------------------------------------------
    def publish_products(self, product_ids: list):
        try:
            if not product_ids:
                raise HTTPException(status_code=400, detail="product_ids list must not be empty.")

            products = (
                self.db.query(PimEntity)
                .filter(PimEntity.id.in_(product_ids), PimEntity.active == True)
                .all()
            )
            found_ids = {str(p.id) for p in products}
            missing = [pid for pid in product_ids if pid not in found_ids]
            if missing:
                raise HTTPException(
                    status_code=404,
                    detail=f"Products not found: {', '.join(missing)}",
                )

            child_products = [p for p in products if p.parent_id is not None]
            if child_products:
                child_ids = [self._get_identifier_value(p.id) or p.id for p in child_products]
                raise HTTPException(
                    status_code=400,
                    detail=f"Cannot publish child-level items directly. Publish the parent product instead. Items: {', '.join(child_ids)}",
                )

            now = datetime.utcnow()
            for p in products:
                p.promoted = True
                p.published_at = now
                p.updated_at = now

            # Cascade to children
            children = (
                self.db.query(PimEntity)
                .filter(PimEntity.parent_id.in_(product_ids), PimEntity.active == True)
                .all()
            )
            for child in children:
                child.promoted = True
                child.published_at = now
                child.updated_at = now

            db_commit_auto_rollback(db=self.db)

            # Refresh flat table
            all_ids = product_ids + [c.id for c in children]
            self.refresh_flat_rows(all_ids)
            db_commit_auto_rollback(db=self.db)

            total = len(products) + len(children)
            return {
                "message": f"{len(products)} product(s) published successfully.",
                "published_count": total,
            }

        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Unable to publish products: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # UNPUBLISH PRODUCTS  (SCRUM-1572)
    # ------------------------------------------------------------------
    def unpublish_products(self, product_ids: list):
        try:
            if not product_ids:
                raise HTTPException(status_code=400, detail="product_ids list must not be empty.")

            products = (
                self.db.query(PimEntity)
                .filter(PimEntity.id.in_(product_ids), PimEntity.active == True)
                .all()
            )
            found_ids = {str(p.id) for p in products}
            missing = [pid for pid in product_ids if pid not in found_ids]
            if missing:
                raise HTTPException(
                    status_code=404,
                    detail=f"Products not found: {', '.join(missing)}",
                )

            child_products = [p for p in products if p.parent_id is not None]
            if child_products:
                child_ids = [self._get_identifier_value(p.id) or p.id for p in child_products]
                raise HTTPException(
                    status_code=400,
                    detail=f"Cannot unpublish child-level items directly. Unpublish the parent product instead. Items: {', '.join(child_ids)}",
                )

            now = datetime.utcnow()
            for p in products:
                p.promoted = False
                p.published_at = None
                p.updated_at = now

            # Cascade to children
            children = (
                self.db.query(PimEntity)
                .filter(PimEntity.parent_id.in_(product_ids), PimEntity.active == True)
                .all()
            )
            for child in children:
                child.promoted = False
                child.published_at = None
                child.updated_at = now

            db_commit_auto_rollback(db=self.db)

            # Refresh flat table
            all_ids = product_ids + [c.id for c in children]
            self.refresh_flat_rows(all_ids)
            db_commit_auto_rollback(db=self.db)
            total = len(products) + len(children)
            return {
                "message": f"{len(products)} product(s) unpublished.",
                "unpublished_count": total,
            }

        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Unable to unpublish products: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # CREATE CHILD (add a child product under a parent — supports N-tier)  (SCRUM-1574)
    # ------------------------------------------------------------------
    def create_item(self, parent_id: str, data: PimEntityCreate):
        try:
            parent = self.db.query(PimEntity).filter(
                PimEntity.id == parent_id, PimEntity.active == True
            ).first()
            if not parent:
                raise HTTPException(status_code=404, detail="Parent product not found.")

            # Validate parent tier can have children
            from lakefusion_utility.models.pim import PimEntityTier
            parent_tier = self.db.query(PimEntityTier).filter(
                PimEntityTier.code == parent.entity_type_id
            ).first()
            if parent_tier and parent_tier.is_leaf:
                raise HTTPException(
                    status_code=400,
                    detail=f"Cannot add children under a {parent_tier.label} (leaf tier).",
                )

            # Determine child tier — next level down from parent
            child_tier = self.db.query(PimEntityTier).filter(
                PimEntityTier.parent_tier_id == parent_tier.id
            ).first() if parent_tier else None
            # Validate unique identifier
            if data.identifier_value:
                self._check_identifier_unique(data.identifier_value)

            item = PimEntity(
                entity_type_id=child_tier.code if child_tier else data.entity_type_id,
                taxonomy_node_id=parent.taxonomy_node_id,
                parent_id=parent_id,
                status=data.status or 'Active',
                active=data.active if data.active is not None else True,
            )
            self.db.add(item)
            self.db.flush()

            # Write identifier and label as EAV values
            if data.identifier_value:
                self._write_identifier_value(item.id, data.identifier_value)
            if data.label_value:
                self._write_label_value(item.id, data.label_value)

            db_commit_auto_rollback(db=self.db)
            self.db.refresh(item)

            # Refresh flat table for item and parent (parent child_count changes)
            self.refresh_flat_rows([item.id, parent_id])
            db_commit_auto_rollback(db=self.db)

            return self.db.query(PimEntityFlat).filter(PimEntityFlat.id == item.id).first()

        except HTTPException:
            raise
        except Exception as e:
            message = traceback.format_exc()
            app_logger.exception(f"Unable to create item. Reason - {message}")
            raise HTTPException(status_code=500, detail=f"Failed to create item: {str(e)}")

    # ------------------------------------------------------------------
    # LIST ITEMS  (SCRUM-1574)
    # ------------------------------------------------------------------
    def list_items(self, parent_id: str):
        try:
            parent = self.db.query(PimEntity).filter(
                PimEntity.id == parent_id, PimEntity.active == True
            ).first()
            if not parent:
                raise HTTPException(status_code=404, detail="Parent product not found.")

            items = (
                self.db.query(PimEntityFlat)
                .filter(PimEntityFlat.parent_id == parent_id, PimEntityFlat.active == True)
                .order_by(PimEntityFlat.created_at.desc())
                .all()
            )
            return items

        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Error listing items: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # UPDATE ITEM  (SCRUM-1574)
    # ------------------------------------------------------------------
    def update_item(self, parent_id: str, item_id: str, data: PimEntityUpdate):
        try:
            item = self.db.query(PimEntity).filter(
                PimEntity.id == item_id,
                PimEntity.parent_id == parent_id,
                PimEntity.active == True,
            ).first()
            if not item:
                raise HTTPException(status_code=404, detail="Item not found under this parent.")

            if data.status is not None:
                item.status = data.status
            if data.active is not None:
                item.active = data.active
            # taxonomy_node_id for items is inherited from parent — ignore if passed

            item.updated_at = datetime.utcnow()
            db_commit_auto_rollback(db=self.db)
            self.db.refresh(item)

            # Refresh flat table
            self.refresh_flat_rows([item.id])
            db_commit_auto_rollback(db=self.db)

            return self.db.query(PimEntityFlat).filter(PimEntityFlat.id == item.id).first()

        except HTTPException:
            raise
        except Exception as e:
            message = traceback.format_exc()
            app_logger.exception(f"Unable to update item. Reason - {message}")
            raise HTTPException(status_code=500, detail=f"Failed to update item: {str(e)}")

    # ------------------------------------------------------------------
    # DELETE ITEM  (SCRUM-1574)
    # ------------------------------------------------------------------
    def delete_item(self, parent_id: str, item_id: str):
        try:
            item = self.db.query(PimEntity).filter(
                PimEntity.id == item_id,
                PimEntity.parent_id == parent_id,
                PimEntity.active == True,
            ).first()
            if not item:
                raise HTTPException(status_code=404, detail="Item not found under this parent.")

            item.active = False
            item.updated_at = datetime.utcnow()
            db_commit_auto_rollback(db=self.db)

            # Refresh flat table for item and parent (parent child_count changes)
            self.refresh_flat_rows([item.id, parent_id])
            db_commit_auto_rollback(db=self.db)

            identifier = self._get_identifier_value(item.id)
            return {"message": f"Item '{identifier or item.id}' soft-deleted."}

        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Unable to delete item: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # COMPLETENESS  (SCRUM-1569)
    # ------------------------------------------------------------------
    def compute_completeness(self, product_id: str):
        try:
            product = self.db.query(PimEntity).filter(
                PimEntity.id == product_id, PimEntity.active == True
            ).first()
            if not product:
                raise HTTPException(status_code=404, detail="Product not found.")

            return self._compute_completeness(product_id)

        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Error computing completeness: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    def _compute_completeness(self, product_id: str):
        """Compute completeness as % of required attributes that have values."""
        # Get product's taxonomy node
        product = self.db.query(PimEntity).filter(PimEntity.id == product_id).first()
        if not product:
            return {"completeness": 0.0, "total_required": 0, "filled": 0, "missing": []}

        # Get all required attributes from resolved config, filtered by level
        required_configs = (
            self.db.query(PimResolvedSpecification, PimAttributeDefinition.level)
            .join(PimAttributeDefinition, PimResolvedSpecification.attribute_id == PimAttributeDefinition.id)
            .filter(
                PimResolvedSpecification.taxonomy_node_id == product.taxonomy_node_id,
                PimResolvedSpecification.is_required == True,
            )
            .all()
        )

        # Also check level_override from the config
        required_attrs = []
        for rac, attr_level in required_configs:
            config = self.db.query(PimSpecificationConfig).filter(
                PimSpecificationConfig.id == rac.config_id
            ).first()
            effective_level = (config.level_override if config and config.level_override else attr_level) or "ALL"
            # Only include if level matches product's tier or is ALL
            if effective_level == "ALL" or effective_level == product.entity_type_id:
                required_attrs.append(rac)

        # Also include global attributes that are required (scope=global)
        global_required = (
            self.db.query(PimAttributeDefinition)
            .filter(
                PimAttributeDefinition.scope == "general",
                PimAttributeDefinition.is_system == True,
            )
            .all()
        )
        global_required_ids = {a.id for a in global_required}

        if not required_attrs and not global_required_ids:
            return {"completeness": 100.0, "total_required": 0, "filled": 0, "missing": []}

        required_attr_ids = {r.attribute_id for r in required_attrs} | global_required_ids

        # Collect filled attribute_ids from all 6 value tables using UNION ALL
        filled_attr_ids = set()
        for ValueTable in VALUE_TABLES:
            rows = (
                self.db.query(ValueTable.attribute_id)
                .filter(ValueTable.product_id == product_id)
                .all()
            )
            filled_attr_ids.update(r[0] for r in rows)

        filled_required = required_attr_ids & filled_attr_ids
        missing = list(required_attr_ids - filled_attr_ids)
        total = len(required_attr_ids)
        filled = len(filled_required)
        completeness = round((filled / total) * 100, 2) if total > 0 else 100.0

        return {
            "completeness": completeness,
            "total_required": total,
            "filled": filled,
            "missing": missing,
        }

    def _batch_compute_completeness(self, product_ids: list) -> dict:
        """Batch compute completeness for multiple products. Returns {product_id: percentage}.
        Filters required attributes by tier level (matching single-product completeness logic)."""
        if not product_ids:
            return {}

        # Get all products with their taxonomy nodes and entity_type_id
        products = (
            self.db.query(PimEntity.id, PimEntity.taxonomy_node_id, PimEntity.entity_type_id)
            .filter(PimEntity.id.in_(product_ids))
            .all()
        )
        product_node_map = {p.id: p.taxonomy_node_id for p in products}
        product_tier_map = {p.id: p.entity_type_id for p in products}

        # Get distinct taxonomy nodes and batch-fetch required attrs with level info
        node_ids = list(set(nid for nid in product_node_map.values() if nid))
        # node_id -> list of (attribute_id, effective_level)
        node_required_attrs = {}
        if node_ids:
            required_rows = (
                self.db.query(
                    PimResolvedSpecification.taxonomy_node_id,
                    PimResolvedSpecification.attribute_id,
                    PimSpecificationConfig.level_override,
                    PimAttributeDefinition.level,
                )
                .join(PimSpecificationConfig, PimResolvedSpecification.config_id == PimSpecificationConfig.id)
                .join(PimAttributeDefinition, PimResolvedSpecification.attribute_id == PimAttributeDefinition.id)
                .filter(
                    PimResolvedSpecification.taxonomy_node_id.in_(node_ids),
                    PimResolvedSpecification.is_required == True,
                )
                .all()
            )
            for row in required_rows:
                effective_level = (row.level_override if row.level_override else row.level) or "ALL"
                node_required_attrs.setdefault(row.taxonomy_node_id, []).append(
                    (row.attribute_id, effective_level)
                )

        # Also include global system attributes (same as single-product method)
        global_system_attrs = (
            self.db.query(PimAttributeDefinition.id, PimAttributeDefinition.level)
            .filter(
                PimAttributeDefinition.scope == "general",
                PimAttributeDefinition.is_system == True,
            )
            .all()
        )

        # Batch-fetch all filled attribute_ids per product across all value tables
        product_filled_map = {pid: set() for pid in product_ids}
        for ValueTable in VALUE_TABLES:
            rows = (
                self.db.query(ValueTable.product_id, ValueTable.attribute_id)
                .filter(ValueTable.product_id.in_(product_ids))
                .all()
            )
            for row in rows:
                if row.product_id in product_filled_map:
                    product_filled_map[row.product_id].add(row.attribute_id)

        # Helper: check if a level matches a tier
        def level_matches(level_str, tier_code):
            if not level_str or level_str == "ALL":
                return True
            return tier_code in [l.strip() for l in level_str.split(",")]

        # Compute completeness per product (tier-filtered)
        result = {}
        for pid in product_ids:
            node_id = product_node_map.get(pid)
            tier = product_tier_map.get(pid, "PRODUCT")

            # Filter spec required attrs by tier
            required = set()
            if node_id and node_id in node_required_attrs:
                for attr_id, eff_level in node_required_attrs[node_id]:
                    if level_matches(eff_level, tier):
                        required.add(attr_id)

            # Add global system attrs matching this tier
            for ga in global_system_attrs:
                if level_matches(ga.level, tier):
                    required.add(ga.id)

            if not required:
                result[pid] = 100.0
            else:
                filled = len(required & product_filled_map.get(pid, set()))
                result[pid] = round((filled / len(required)) * 100, 2)
        return result

    # ------------------------------------------------------------------
    # DASHBOARD STATS  (SCRUM-1585)
    # ------------------------------------------------------------------
    def get_dashboard_stats(self):
        """Return summary stats for dashboard highlights."""
        try:
            total_products = (
                self.db.query(func.count(PimEntity.id))
                .filter(PimEntity.active == True, PimEntity.parent_id == None)
                .scalar() or 0
            )

            total_items = (
                self.db.query(func.count(PimEntity.id))
                .filter(PimEntity.active == True, PimEntity.parent_id != None)
                .scalar() or 0
            )

            # Products with completeness < 100%
            all_pids = [
                r[0] for r in
                self.db.query(PimEntity.id)
                .filter(PimEntity.active == True, PimEntity.parent_id == None)
                .all()
            ]
            completeness_map = self._batch_compute_completeness(all_pids) if all_pids else {}
            incomplete_count = sum(1 for c in completeness_map.values() if c < 100)

            # Status breakdown
            status_rows = (
                self.db.query(PimEntity.status, func.count(PimEntity.id))
                .filter(PimEntity.active == True, PimEntity.parent_id == None)
                .group_by(PimEntity.status)
                .all()
            )
            status_breakdown = {r[0]: r[1] for r in status_rows}

            # Published count
            published_count = (
                self.db.query(func.count(PimEntity.id))
                .filter(PimEntity.active == True, PimEntity.promoted == True, PimEntity.parent_id == None)
                .scalar() or 0
            )

            return {
                "total_products": total_products,
                "total_items": total_items,
                "incomplete_products": incomplete_count,
                "published_products": published_count,
                "status_breakdown": status_breakdown,
            }

        except Exception as e:
            app_logger.exception(f"Error computing dashboard stats: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # CATEGORY HEALTH DASHBOARD  (SCRUM-1563)
    # ------------------------------------------------------------------
    def get_category_health(self):
        """Return per-category aggregate completeness for dashboard."""
        try:
            # Get all active taxonomy nodes
            nodes = self.db.query(PimTaxonomyNode).filter(PimTaxonomyNode.is_active == True).all()
            if not nodes:
                return {"categories": []}

            # Get all active products grouped by taxonomy_node_id
            products = self.db.query(PimEntity).filter(PimEntity.active == True).all()
            node_products = {}
            for p in products:
                if p.taxonomy_node_id:
                    node_products.setdefault(p.taxonomy_node_id, []).append(p.id)

            # Batch compute completeness for all products
            all_product_ids = [p.id for p in products]
            completeness_map = self._batch_compute_completeness(all_product_ids) if all_product_ids else {}

            # Aggregate per category
            categories = []
            for node in nodes:
                pids = node_products.get(node.id, [])
                if not pids:
                    avg_completeness = None
                    product_count = 0
                else:
                    product_count = len(pids)
                    total_c = sum(completeness_map.get(pid, 100.0) for pid in pids)
                    avg_completeness = round(total_c / product_count, 2)

                categories.append({
                    "node_id": node.id,
                    "label": node.label,
                    "code": node.code,
                    "level": node.level,
                    "materialized_path": node.materialized_path,
                    "product_count": product_count,
                    "avg_completeness": avg_completeness,
                })

            # Sort by completeness ascending (lowest first for triage)
            categories.sort(key=lambda c: (c["avg_completeness"] is None, c["avg_completeness"] or 0))
            return {"categories": categories}

        except Exception as e:
            app_logger.exception(f"Error computing category health: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # CSV EXPORT  (SCRUM-1585)
    # ------------------------------------------------------------------
    def export_products_csv(self, entity_type_id: str = None, status: str = None,
                            taxonomy_node_id: str = None):
        """Export products as a list of flat dicts suitable for CSV conversion."""
        try:
            query = self.db.query(PimEntityFlat).filter(PimEntityFlat.active == True)
            if entity_type_id:
                query = query.filter(PimEntityFlat.entity_type_id == entity_type_id)
            if status:
                statuses = [s.strip() for s in status.split(",")]
                if len(statuses) == 1:
                    query = query.filter(PimEntityFlat.status == statuses[0])
                else:
                    query = query.filter(PimEntityFlat.status.in_(statuses))
            if taxonomy_node_id:
                node = self.db.query(PimTaxonomyNode).filter(PimTaxonomyNode.id == taxonomy_node_id).first()
                if node:
                    descendant_ids = [
                        n.id for n in self.db.query(PimTaxonomyNode.id)
                        .filter(PimTaxonomyNode.materialized_path.like(f"{node.materialized_path}%"))
                        .all()
                    ]
                    query = query.filter(PimEntityFlat.taxonomy_node_id.in_(descendant_ids))

            products = query.order_by(PimEntityFlat.created_at.desc()).all()
            if not products:
                return {"rows": [], "columns": []}

            product_ids = [p.id for p in products]
            completeness_map = self._batch_compute_completeness(product_ids)

            rows = []
            for p in products:
                rows.append({
                    "identifier": p.identifier or "",
                    "label": p.label or "",
                    "entity_type_id": p.entity_type_id,
                    "status": p.status,
                    "category": p.category_label or "",
                    "category_path": p.category_path or "",
                    "completeness": completeness_map.get(p.id, 100.0),
                    "promoted": p.promoted,
                    "published_at": str(p.published_at) if p.published_at else "",
                    "created_at": str(p.created_at) if p.created_at else "",
                })

            columns = list(rows[0].keys()) if rows else []
            return {"rows": rows, "columns": columns, "total": len(rows)}

        except Exception as e:
            app_logger.exception(f"Error exporting products: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # CATEGORY REASSIGNMENT HELPER  (SCRUM-1573)
    # ------------------------------------------------------------------
    def _handle_category_reassignment(self, product, old_node_id: str, new_node_id: str):
        """Delete attribute values that are no longer valid after category change."""
        try:
            # Get attribute IDs valid in the new category
            new_valid_attrs = {
                r.attribute_id
                for r in self.db.query(PimResolvedSpecification)
                .filter(PimResolvedSpecification.taxonomy_node_id == new_node_id)
                .all()
            }

            # Also preserve global attribute values (scope=global applies to all categories)
            global_attr_ids = {
                a.id for a in self.db.query(PimAttributeDefinition.id)
                .filter(PimAttributeDefinition.scope == "general")
                .all()
            }
            new_valid_attrs |= global_attr_ids

            # Delete values whose attribute_id is NOT in the new category
            for ValueTable in VALUE_TABLES:
                orphaned = (
                    self.db.query(ValueTable)
                    .filter(
                        ValueTable.product_id == product.id,
                        ~ValueTable.attribute_id.in_(new_valid_attrs) if new_valid_attrs else True,
                    )
                    .all()
                )
                for row in orphaned:
                    self.db.delete(row)

            db_commit_auto_rollback(db=self.db)
            app_logger.info(
                f"Category reassignment: cleaned orphaned values for product {product.id} "
                f"(old_node={old_node_id} → new_node={new_node_id})"
            )

        except Exception as e:
            app_logger.exception(f"Error during category reassignment cleanup: {str(e)}")

    # ------------------------------------------------------------------
    # BULK IMPORT HIERARCHY  (SCRUM-1790)
    # ------------------------------------------------------------------
    def bulk_import_hierarchy(self, data: PimBulkImportRequest):
        """
        Bulk import a full N-tier product hierarchy in a single transaction.
        Receives pre-mapped nodes from the frontend and inserts them directly
        using bulk operations — no per-record HTTP overhead.
        """
        import uuid
        from datetime import datetime

        created = {}
        errors = []
        rejected_values = []   # select/multiselect values whose option isn't defined (strict, no auto-create)
        products_to_insert = []
        values_to_insert = []

        # Pre-fetch attribute definitions for data type lookup
        attr_defs = {
            a.id: a for a in self.db.query(PimAttributeDefinition).all()
        }

        # Pre-fetch active option keys per attribute (strict validation — options must be
        # pre-seeded by the spec/taxonomy load; the importer never auto-creates them).
        option_keys = {}   # attribute_id -> set(value_key)
        for o in self.db.query(PimAttributeOption).filter(PimAttributeOption.is_active == True).all():
            option_keys.setdefault(o.attribute_id, set()).add(o.value_key)

        # Get identifier and label attribute IDs for EAV writes.
        # If the frontend sent explicit attribute IDs, flag them now.
        # If no attribute is flagged yet, auto-create system attributes.
        id_attr = self._get_identifier_attr()
        label_attr = self._get_label_attr()

        if not id_attr and data.identifier_attribute_id:
            # Frontend told us which attribute should be the identifier — flag it
            candidate = self.db.query(PimAttributeDefinition).filter(
                PimAttributeDefinition.id == data.identifier_attribute_id
            ).first()
            if candidate:
                candidate.is_identifier = True
                self.db.flush()
                self._identifier_attr = candidate
                id_attr = candidate

        if not label_attr and data.label_attribute_id:
            candidate = self.db.query(PimAttributeDefinition).filter(
                PimAttributeDefinition.id == data.label_attribute_id
            ).first()
            if candidate:
                candidate.is_label = True
                self.db.flush()
                self._label_attr = candidate
                label_attr = candidate

        # Identifier/label are OPTIONAL for products. If no attribute is flagged and
        # the frontend didn't designate one, flag a pre-existing sku/name attr if it
        # happens to exist — but never auto-CREATE system attrs (that would force an
        # "Identity" group/tab onto products that intentionally have neither).
        if not id_attr:
            existing = self.db.query(PimAttributeDefinition).filter(
                PimAttributeDefinition.code == 'sku'
            ).first()
            if existing:
                existing.is_identifier = True
                self.db.flush()
                self._identifier_attr = existing
                id_attr = existing

        if not label_attr:
            existing = self.db.query(PimAttributeDefinition).filter(
                PimAttributeDefinition.code == 'name'
            ).first()
            if existing:
                existing.is_label = True
                self.db.flush()
                self._label_attr = existing
                label_attr = existing

        id_attr_id = id_attr.id if id_attr else None
        label_attr_id = label_attr.id if label_attr else None

        # Pre-fetch taxonomy nodes for fallback resolution
        from lakefusion_utility.models.pim import PimTaxonomyNode
        all_taxonomy_nodes = self.db.query(PimTaxonomyNode).filter(PimTaxonomyNode.is_active == True).all()
        taxonomy_label_to_id = {n.label.lower(): n.id for n in all_taxonomy_nodes}
        taxonomy_code_to_id = {n.code.lower(): n.id for n in all_taxonomy_nodes}
        # Default: first root node if nothing matches
        default_taxonomy_id = all_taxonomy_nodes[0].id if all_taxonomy_nodes else None

        # Build a set of known taxonomy labels for prefix-stripping (dynamic, not hardcoded)
        known_labels = set(n.label.lower() for n in all_taxonomy_nodes)

        def resolve_taxonomy_id(value):
            """Resolve a category label/code/ID to a taxonomy node ID with dynamic fuzzy matching."""
            if not value:
                return None
            # Direct UUID match
            if len(value) == 36 and '-' in value:
                return value
            val_lower = value.lower().strip()
            # 1. Exact label match
            if val_lower in taxonomy_label_to_id:
                return taxonomy_label_to_id[val_lower]
            # 2. Exact code match
            if val_lower in taxonomy_code_to_id:
                return taxonomy_code_to_id[val_lower]
            # 3. Dynamic prefix stripping: try removing each known taxonomy label as a prefix
            #    e.g., "Women's Lifestyle Sneakers" → remove "Women" prefix → "Lifestyle Sneakers"
            for known in known_labels:
                prefix_variations = [f"{known}'s ", f"{known} "]
                for pv in prefix_variations:
                    if val_lower.startswith(pv):
                        remainder = val_lower[len(pv):]
                        if remainder in taxonomy_label_to_id:
                            return taxonomy_label_to_id[remainder]
            # 4. Substring containment: find a taxonomy node whose label is contained in the value or vice versa
            #    Prefer longer matches (more specific)
            best_match = None
            best_len = 0
            for label, nid in taxonomy_label_to_id.items():
                if len(label) > best_len and (label in val_lower or val_lower in label):
                    best_match = nid
                    best_len = len(label)
            if best_match:
                return best_match
            return None

        def _uuid():
            return str(uuid.uuid4())

        def process_node(node: PimImportNode, parent_id: str = None, taxonomy_node_id: str = None):
            """Recursively build insert lists for a node and its children."""
            product_id = _uuid()
            now = datetime.utcnow()

            # Resolve taxonomy: try provided value, then inherit from parent, then default
            resolved_taxonomy = resolve_taxonomy_id(node.taxonomy_node_id)
            if not resolved_taxonomy:
                resolved_taxonomy = taxonomy_node_id  # inherit from parent
            if not resolved_taxonomy:
                resolved_taxonomy = default_taxonomy_id  # fallback

            identifier_val = node.identifier_value or node.key
            label_val = node.label_value or identifier_val

            products_to_insert.append({
                'id': product_id,
                'entity_type_id': node.tier_code,
                'taxonomy_node_id': resolved_taxonomy,
                'parent_id': parent_id,
                'status': node.status or 'Draft',
                'active': True,
                'promoted': False,
                'published_at': None,
                'created_at': now,
                'updated_at': now,
            })

            # Write identifier and label as EAV text values
            if id_attr_id and identifier_val:
                values_to_insert.append({
                    'table': 'pim_value_text',
                    'row': {
                        'id': _uuid(), 'product_id': product_id, 'attribute_id': id_attr_id,
                        'value': identifier_val, 'locale': '', 'source': 'IMPORT',
                        'version': 1, 'created_at': now, 'updated_at': now,
                    }
                })
            if label_attr_id and label_val:
                values_to_insert.append({
                    'table': 'pim_value_text',
                    'row': {
                        'id': _uuid(), 'product_id': product_id, 'attribute_id': label_attr_id,
                        'value': label_val, 'locale': '', 'source': 'IMPORT',
                        'version': 1, 'created_at': now, 'updated_at': now,
                    }
                })

            # Track count per tier
            created[node.tier_code] = created.get(node.tier_code, 0) + 1

            # Build value inserts
            for val in (node.values or []):
                if not val.value or not val.attribute_id:
                    continue
                attr_def = attr_defs.get(val.attribute_id)
                if not attr_def:
                    continue

                # Enforce tier: skip values whose attribute level doesn't match this node's tier
                if attr_def.level and attr_def.level != "ALL":
                    attr_levels = [l.strip() for l in attr_def.level.split(",")]
                    if node.tier_code not in attr_levels:
                        continue

                value_id = _uuid()
                data_type = attr_def.data_type

                if data_type == 'NUMBER':
                    try:
                        numeric_val = float(val.value)
                    except (ValueError, TypeError):
                        continue
                    values_to_insert.append({
                        'table': 'pim_value_number',
                        'row': {
                            'id': value_id,
                            'product_id': product_id,
                            'attribute_id': val.attribute_id,
                            'value': numeric_val,
                            'locale': '',
                            'currency': '',
                            'price_type': '',
                            'territory': '',
                            'source': val.source or 'IMPORT',
                            'version': 1,
                            'created_at': now,
                            'updated_at': now,
                        }
                    })
                elif data_type == 'BOOLEAN':
                    bool_val = val.value.lower() in ('true', '1', 'yes')
                    values_to_insert.append({
                        'table': 'pim_value_boolean',
                        'row': {
                            'id': value_id,
                            'product_id': product_id,
                            'attribute_id': val.attribute_id,
                            'value': bool_val,
                            'locale': '',
                            'source': val.source or 'IMPORT',
                            'version': 1,
                            'created_at': now,
                            'updated_at': now,
                        }
                    })
                elif data_type == 'SELECT':
                    key = to_value_key(str(val.value))
                    if key not in option_keys.get(val.attribute_id, set()):
                        rejected_values.append({
                            'attribute_id': val.attribute_id,
                            'attribute_code': attr_def.code,
                            'product_id': product_id,
                            'value': str(val.value),
                            'value_key': key,
                            'reason': 'option_not_defined',
                        })
                        continue
                    values_to_insert.append({
                        'table': 'pim_value_select',
                        'row': {
                            'id': value_id,
                            'product_id': product_id,
                            'attribute_id': val.attribute_id,
                            'ref_value_key': key,
                            'locale': '',
                            'source': val.source or 'IMPORT',
                            'version': 1,
                            'created_at': now,
                            'updated_at': now,
                        }
                    })
                elif data_type == 'MULTISELECT':
                    # split on ';' (spec CSV) or ',', one row per key
                    raw_parts = [p.strip() for chunk in str(val.value).split(';') for p in chunk.split(',')]
                    parts = [p for p in raw_parts if p]
                    valid_set = option_keys.get(val.attribute_id, set())
                    for part in parts:
                        key = to_value_key(part)
                        if key not in valid_set:
                            rejected_values.append({
                                'attribute_id': val.attribute_id,
                                'attribute_code': attr_def.code,
                                'product_id': product_id,
                                'value': part,
                                'value_key': key,
                                'reason': 'option_not_defined',
                            })
                            continue
                        values_to_insert.append({
                            'table': 'pim_value_multiselect',
                            'row': {
                                'id': _uuid(),
                                'product_id': product_id,
                                'attribute_id': val.attribute_id,
                                'ref_value_key': key,
                                'locale': '',
                                'source': val.source or 'IMPORT',
                                'version': 1,
                                'created_at': now,
                                'updated_at': now,
                            }
                        })
                elif data_type == 'REFERENCE':
                    values_to_insert.append({
                        'table': 'pim_value_reference',
                        'row': {
                            'id': value_id,
                            'product_id': product_id,
                            'attribute_id': val.attribute_id,
                            'ref_table': attr_def.reference_entity_id or '',
                            'ref_id': str(val.value),
                            'locale': '',
                            'source': val.source or 'IMPORT',
                            'version': 1,
                            'created_at': now,
                            'updated_at': now,
                        }
                    })
                elif data_type == 'DATE':
                    try:
                        date_val = _date.fromisoformat(str(val.value).strip())
                    except (ValueError, TypeError):
                        rejected_values.append({
                            'attribute_id': val.attribute_id,
                            'attribute_code': attr_def.code,
                            'product_id': product_id,
                            'value': str(val.value),
                            'value_key': None,
                            'reason': 'invalid_date',
                        })
                        continue
                    values_to_insert.append({
                        'table': 'pim_value_date',
                        'row': {
                            'id': value_id,
                            'product_id': product_id,
                            'attribute_id': val.attribute_id,
                            'value': date_val,
                            'locale': '',
                            'source': val.source or 'IMPORT',
                            'version': 1,
                            'created_at': now,
                            'updated_at': now,
                        }
                    })
                else:
                    # TEXT
                    values_to_insert.append({
                        'table': 'pim_value_text',
                        'row': {
                            'id': value_id,
                            'product_id': product_id,
                            'attribute_id': val.attribute_id,
                            'value': str(val.value),
                            'locale': '',
                            'source': val.source or 'IMPORT',
                            'version': 1,
                            'created_at': now,
                            'updated_at': now,
                        }
                    })

            # Recurse into children
            for child in (node.children or []):
                process_node(child, parent_id=product_id, taxonomy_node_id=resolved_taxonomy)

        # Phase 1: Walk the entire tree and build insert lists
        app_logger.info(f"Bulk import: processing {len(data.nodes)} root nodes...")
        for node in data.nodes:
            try:
                process_node(node)
            except Exception as e:
                errors.append(f"Node '{node.key}': {str(e)}")

        # Phase 2: Deduplicate by unique constraint columns (read from model, not hardcoded)
        unique_cols = []
        for constraint in PimEntity.__table__.constraints:
            if hasattr(constraint, 'columns') and constraint.__class__.__name__ == 'UniqueConstraint':
                unique_cols = [col.name for col in constraint.columns]
                break

        if unique_cols:
            seen = set()
            deduped = []
            removed_ids = set()
            for p in products_to_insert:
                key = tuple(p.get(col) for col in unique_cols)
                if key not in seen:
                    seen.add(key)
                    deduped.append(p)
                else:
                    removed_ids.add(p['id'])
            dup_count = len(products_to_insert) - len(deduped)
            if dup_count > 0:
                app_logger.info(f"Bulk import: skipped {dup_count} duplicates on {unique_cols}")
                # Also remove values that reference the removed product IDs
                values_to_insert = [v for v in values_to_insert if v['row']['product_id'] not in removed_ids]
            products_to_insert = deduped

        # Remove existing products with matching unique keys (clean re-import)
        if unique_cols and products_to_insert:
            unique_col_name = unique_cols[0]
            unique_model_col = getattr(PimEntity, unique_col_name, None)
            if unique_model_col is not None:
                all_keys = [p[unique_col_name] for p in products_to_insert if p.get(unique_col_name)]
                if all_keys:
                    existing_keys = set(
                        getattr(row, unique_col_name) for row in
                        self.db.query(unique_model_col).filter(unique_model_col.in_(all_keys)).all()
                    )
                    if existing_keys:
                        app_logger.info(f"Bulk import: removing {len(existing_keys)} existing products for re-import...")
                        existing_ids = [
                            row.id for row in
                            self.db.query(PimEntity.id).filter(unique_model_col.in_(existing_keys)).all()
                        ]
                        if existing_ids:
                            for ValueTable in VALUE_TABLES:
                                self.db.query(ValueTable).filter(ValueTable.product_id.in_(existing_ids)).delete(synchronize_session=False)
                            self.db.query(PimEntity).filter(PimEntity.id.in_(existing_ids)).delete(synchronize_session=False)
                            self.db.flush()

        # Phase 3-5: All inserts in a single try block with rollback on any failure
        try:
            # Phase 3: Bulk insert products
            app_logger.info(f"Bulk import: inserting {len(products_to_insert)} products...")
            if products_to_insert:
                self.db.execute(
                    PimEntity.__table__.insert(),
                    products_to_insert
                )

            # Phase 4: Bulk insert values grouped by table
            # Deduplicate values by unique constraint (product_id, attribute_id, locale)
            # CSV rows repeat product-level attributes for every item row — keep first occurrence only
            def dedup_values(rows):
                seen = set()
                result = []
                for r in rows:
                    key = (r['product_id'], r['attribute_id'], r.get('locale', ''))
                    if key not in seen:
                        seen.add(key)
                        result.append(r)
                return result

            text_values = dedup_values([v['row'] for v in values_to_insert if v['table'] == 'pim_value_text'])
            number_values = dedup_values([v['row'] for v in values_to_insert if v['table'] == 'pim_value_number'])
            boolean_values = dedup_values([v['row'] for v in values_to_insert if v['table'] == 'pim_value_boolean'])
            date_values = dedup_values([v['row'] for v in values_to_insert if v['table'] == 'pim_value_date'])
            select_values = dedup_values([v['row'] for v in values_to_insert if v['table'] == 'pim_value_select'])
            reference_values = dedup_values([v['row'] for v in values_to_insert if v['table'] == 'pim_value_reference'])
            # Multiselect is multi-row per (product, attr, locale); dedup on the full key incl. ref_value_key
            ms_seen = set()
            multiselect_values = []
            for v in values_to_insert:
                if v['table'] != 'pim_value_multiselect':
                    continue
                r = v['row']
                k = (r['product_id'], r['attribute_id'], r.get('locale', ''), r['ref_value_key'])
                if k not in ms_seen:
                    ms_seen.add(k)
                    multiselect_values.append(r)

            app_logger.info(
                f"Bulk import: inserting {len(text_values)} text, {len(number_values)} number, "
                f"{len(boolean_values)} boolean, {len(date_values)} date, {len(select_values)} select, "
                f"{len(multiselect_values)} multiselect, {len(reference_values)} reference values..."
            )

            if text_values:
                self.db.execute(PimValueText.__table__.insert(), text_values)
            if number_values:
                self.db.execute(PimValueNumber.__table__.insert(), number_values)
            if boolean_values:
                self.db.execute(PimValueBoolean.__table__.insert(), boolean_values)
            if date_values:
                self.db.execute(PimValueDate.__table__.insert(), date_values)
            if select_values:
                self.db.execute(PimValueSelect.__table__.insert(), select_values)
            if multiselect_values:
                self.db.execute(PimValueMultiselect.__table__.insert(), multiselect_values)
            if reference_values:
                self.db.execute(PimValueReference.__table__.insert(), reference_values)

            # Phase 5: Single commit
            db_commit_auto_rollback(db=self.db)

            # Phase 6: Bulk refresh flat table (single SQL, not per-row)
            imported_ids = [p['id'] for p in products_to_insert]
            if imported_ids:
                app_logger.info(f"Bulk import: refreshing flat table for {len(imported_ids)} products...")
                id_attr = self._get_identifier_attr()
                label_attr = self._get_label_attr()
                id_attr_id = id_attr.id if id_attr else None
                label_attr_id = label_attr.id if label_attr else None

                # Build flat rows from already-inserted data using batch queries
                from sqlalchemy import case as sa_case
                # Fetch all identifier values in one query
                id_map = {}
                if id_attr_id:
                    for row in self.db.query(PimValueText.product_id, PimValueText.value).filter(
                        PimValueText.product_id.in_(imported_ids),
                        PimValueText.attribute_id == id_attr_id,
                        PimValueText.locale == '',
                    ).all():
                        id_map[row[0]] = row[1]

                # Fetch all label values in one query
                label_map = {}
                if label_attr_id:
                    for row in self.db.query(PimValueText.product_id, PimValueText.value).filter(
                        PimValueText.product_id.in_(imported_ids),
                        PimValueText.attribute_id == label_attr_id,
                        PimValueText.locale == '',
                    ).all():
                        label_map[row[0]] = row[1]

                # Fetch all taxonomy nodes in one query
                node_map = {}
                tax_ids = set(p.get('taxonomy_node_id') for p in products_to_insert if p.get('taxonomy_node_id'))
                if tax_ids:
                    for node in self.db.query(PimTaxonomyNode).filter(PimTaxonomyNode.id.in_(tax_ids)).all():
                        node_map[node.id] = (node.label, node.materialized_path)

                # Count children per parent in one query
                from sqlalchemy import func as sa_func
                child_counts = {}
                parent_ids = set(p.get('parent_id') for p in products_to_insert if p.get('parent_id'))
                # Also include imported_ids since they could be parents of other imported entities
                count_targets = set(imported_ids) | parent_ids
                if count_targets:
                    for row in self.db.query(PimEntity.parent_id, sa_func.count(PimEntity.id)).filter(
                        PimEntity.parent_id.in_(count_targets),
                        PimEntity.active == True,
                    ).group_by(PimEntity.parent_id).all():
                        child_counts[row[0]] = row[1]

                # Delete existing flat rows for these IDs and bulk insert new ones
                self.db.query(PimEntityFlat).filter(PimEntityFlat.id.in_(imported_ids)).delete(synchronize_session=False)
                flat_rows = []
                for p in products_to_insert:
                    pid = p['id']
                    tn_id = p.get('taxonomy_node_id')
                    cat_label, cat_path = node_map.get(tn_id, (None, None)) if tn_id else (None, None)
                    flat_rows.append({
                        'id': pid,
                        'entity_type_id': p['entity_type_id'],
                        'taxonomy_node_id': tn_id,
                        'parent_id': p.get('parent_id'),
                        'status': p.get('status', 'Draft'),
                        'active': p.get('active', True),
                        'promoted': p.get('promoted', False),
                        'published_at': p.get('published_at'),
                        'identifier': id_map.get(pid),
                        'label': label_map.get(pid),
                        'category_label': cat_label,
                        'category_path': cat_path,
                        'child_count': child_counts.get(pid, 0),
                        'created_at': p.get('created_at'),
                        'updated_at': p.get('updated_at'),
                    })
                if flat_rows:
                    self.db.execute(PimEntityFlat.__table__.insert(), flat_rows)
                db_commit_auto_rollback(db=self.db)

            app_logger.info(
                f"Bulk import complete: {sum(created.values())} products, "
                f"{len(values_to_insert)} values"
            )

        except Exception as e:
            # Rollback entire transaction — DB stays clean
            self.db.rollback()
            import traceback
            tb = traceback.format_exc()
            app_logger.exception(f"Bulk import failed, rolled back: {str(e)}\n{tb}")
            # Include a truncated but useful error message for the UI
            err_msg = str(e)
            if len(err_msg) > 500:
                err_msg = err_msg[:500] + "..."
            errors.append(f"Bulk import failed: {err_msg}")
            created = {k: 0 for k in created}  # reset counts since nothing was committed
            rejected_values = []  # nothing committed

        if rejected_values:
            app_logger.warning(
                f"Bulk import: {len(rejected_values)} select/multiselect values rejected "
                f"(option not defined). They were NOT written."
            )
        return {"created": created, "errors": errors, "rejected_values": rejected_values}
