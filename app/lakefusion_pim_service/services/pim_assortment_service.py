import traceback
from sqlalchemy.orm import Session
from sqlalchemy import func
from fastapi import HTTPException
from lakefusion_utility.models.pim import (
    PimAssortment, PimAssortmentCreate, PimAssortmentUpdate,
    PimEntity, PimTaxonomyNode,
)
from lakefusion_utility.utils.app_db import db_commit_auto_rollback
from lakefusion_utility.utils.logging_utils import get_logger

app_logger = get_logger(__name__)


class PimAssortmentService:
    def __init__(self, db: Session):
        self.db = db

    # ------------------------------------------------------------------
    # CREATE
    # ------------------------------------------------------------------
    def create_assortment(self, data: PimAssortmentCreate):
        try:
            # Validate taxonomy node exists
            node = self.db.query(PimTaxonomyNode).filter(PimTaxonomyNode.id == data.taxonomy_node_id).first()
            if not node:
                raise HTTPException(status_code=404, detail="Taxonomy node not found")

            # Check if assortment already exists for this node + steward
            existing = (
                self.db.query(PimAssortment)
                .filter(
                    PimAssortment.taxonomy_node_id == data.taxonomy_node_id,
                    PimAssortment.steward_user_id == data.steward_user_id,
                )
                .first()
            )
            if existing:
                raise HTTPException(
                    status_code=409,
                    detail=f"Assortment already exists for this category and steward"
                )

            assortment = PimAssortment(
                taxonomy_node_id=data.taxonomy_node_id,
                steward_user_id=data.steward_user_id,
                steward_display_name=data.steward_display_name,
                name=data.name or node.label,
                description=data.description,
            )
            self.db.add(assortment)
            db_commit_auto_rollback(db=self.db)
            self.db.refresh(assortment)
            return assortment

        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Error creating assortment: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # LIST
    # ------------------------------------------------------------------
    def list_assortments(self, steward_user_id: str = None):
        try:
            query = self.db.query(PimAssortment)
            if steward_user_id:
                query = query.filter(PimAssortment.steward_user_id == steward_user_id)
            return query.order_by(PimAssortment.created_at.desc()).all()

        except Exception as e:
            app_logger.exception(f"Error listing assortments: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # GET
    # ------------------------------------------------------------------
    def get_assortment(self, assortment_id: str):
        try:
            assortment = self.db.query(PimAssortment).filter(PimAssortment.id == assortment_id).first()
            if not assortment:
                raise HTTPException(status_code=404, detail="Assortment not found")
            return assortment

        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Error getting assortment: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # UPDATE
    # ------------------------------------------------------------------
    def update_assortment(self, assortment_id: str, data: PimAssortmentUpdate):
        try:
            assortment = self.db.query(PimAssortment).filter(PimAssortment.id == assortment_id).first()
            if not assortment:
                raise HTTPException(status_code=404, detail="Assortment not found")

            if data.name is not None:
                assortment.name = data.name
            if data.description is not None:
                assortment.description = data.description
            if data.steward_user_id is not None:
                assortment.steward_user_id = data.steward_user_id
            if data.steward_display_name is not None:
                assortment.steward_display_name = data.steward_display_name

            db_commit_auto_rollback(db=self.db)
            self.db.refresh(assortment)
            return assortment

        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Error updating assortment: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # DELETE
    # ------------------------------------------------------------------
    def delete_assortment(self, assortment_id: str):
        try:
            assortment = self.db.query(PimAssortment).filter(PimAssortment.id == assortment_id).first()
            if not assortment:
                raise HTTPException(status_code=404, detail="Assortment not found")

            self.db.delete(assortment)
            db_commit_auto_rollback(db=self.db)
            return {"message": f"Assortment '{assortment.name}' deleted."}

        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Error deleting assortment: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # DASHBOARD: My Assortments (enriched with category + completeness)
    # ------------------------------------------------------------------
    def get_my_assortments(self, steward_user_id: str, product_service):
        """Return enriched assortment cards for the dashboard."""
        try:
            assortments = (
                self.db.query(PimAssortment)
                .filter(PimAssortment.steward_user_id == steward_user_id)
                .order_by(PimAssortment.created_at.desc())
                .all()
            )
            if not assortments:
                return []

            # Batch: get taxonomy node details
            node_ids = [a.taxonomy_node_id for a in assortments]
            nodes = self.db.query(PimTaxonomyNode).filter(PimTaxonomyNode.id.in_(node_ids)).all()
            node_map = {n.id: n for n in nodes}

            # Batch: get product counts per node
            prod_counts = dict(
                self.db.query(PimEntity.taxonomy_node_id, func.count(PimEntity.id))
                .filter(PimEntity.taxonomy_node_id.in_(node_ids))
                .filter(PimEntity.active == True)
                .filter(PimEntity.parent_id == None)
                .group_by(PimEntity.taxonomy_node_id)
                .all()
            )

            # Batch: get product IDs for completeness
            all_products = (
                self.db.query(PimEntity.id, PimEntity.taxonomy_node_id)
                .filter(PimEntity.taxonomy_node_id.in_(node_ids))
                .filter(PimEntity.active == True)
                .filter(PimEntity.parent_id == None)
                .all()
            )

            all_pids = [p.id for p in all_products]
            completeness_map = product_service._batch_compute_completeness(all_pids) if all_pids else {}

            # Group completeness by node
            node_completeness = {}
            for p in all_products:
                node_completeness.setdefault(p.taxonomy_node_id, []).append(
                    completeness_map.get(p.id, 100.0)
                )

            result = []
            for a in assortments:
                node = node_map.get(a.taxonomy_node_id)
                pcount = prod_counts.get(a.taxonomy_node_id, 0)
                c_list = node_completeness.get(a.taxonomy_node_id, [])
                avg_c = round(sum(c_list) / len(c_list), 1) if c_list else 0.0

                result.append({
                    "id": a.id,
                    "taxonomy_node_id": a.taxonomy_node_id,
                    "steward_user_id": a.steward_user_id,
                    "steward_display_name": a.steward_display_name,
                    "name": a.name,
                    "description": a.description,
                    "category_label": node.label if node else None,
                    "category_path": node.materialized_path if node else None,
                    "product_count": pcount,
                    "avg_completeness": avg_c,
                    "created_at": a.created_at,
                    "updated_at": a.updated_at,
                })

            return result

        except Exception as e:
            app_logger.exception(f"Error getting my assortments: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # DASHBOARD: Unassigned Products
    # ------------------------------------------------------------------
    def get_unassigned_products(self, product_service):
        """Return products in categories that have no assortment assigned."""
        try:
            # Get all taxonomy_node_ids that have assortments
            assigned_node_ids = [
                r[0] for r in
                self.db.query(PimAssortment.taxonomy_node_id).distinct().all()
            ]

            # Get products NOT in any assigned category
            query = (
                self.db.query(PimEntity)
                .filter(PimEntity.active == True)
                .filter(PimEntity.parent_id == None)
            )
            if assigned_node_ids:
                query = query.filter(~PimEntity.taxonomy_node_id.in_(assigned_node_ids))

            products = query.order_by(PimEntity.created_at.desc()).limit(200).all()
            if not products:
                return []

            # Get taxonomy labels
            node_ids = list(set(p.taxonomy_node_id for p in products))
            nodes = self.db.query(PimTaxonomyNode).filter(PimTaxonomyNode.id.in_(node_ids)).all()
            node_map = {n.id: n for n in nodes}

            # Batch completeness
            pids = [p.id for p in products]
            completeness_map = product_service._batch_compute_completeness(pids) if pids else {}

            result = []
            for p in products:
                node = node_map.get(p.taxonomy_node_id)
                result.append({
                    "id": p.id,
                    "identifier": product_service._get_identifier_value(p.id),
                    "entity_type_id": p.entity_type_id,
                    "status": p.status,
                    "taxonomy_node_id": p.taxonomy_node_id,
                    "category_label": node.label if node else None,
                    "completeness": completeness_map.get(p.id, 0.0),
                    "created_at": p.created_at,
                })

            return result

        except Exception as e:
            app_logger.exception(f"Error getting unassigned products: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))
