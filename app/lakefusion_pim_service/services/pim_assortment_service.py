from sqlalchemy.orm import Session
from sqlalchemy import bindparam, text
from fastapi import HTTPException
from app.lakefusion_pim_service.utils import pim_sql
from lakefusion_utility.models.pim import PimAssortmentCreate, PimAssortmentUpdate
from lakefusion_utility.utils.app_db import db_commit_auto_rollback
from lakefusion_utility.utils.logging_utils import get_logger

app_logger = get_logger(__name__)

# SCRUM-1929 Phase 6.2 — converted from ORM to raw SQL against the Delta-synced
# tables (gold."{entity}_pim_assortment_prod_synced", "..._pim_taxonomy_node_...",
# "..._pim_entity_..."). pim_assortment has a UUID id + updated_at (auto-handled).
# Completeness/identifier still delegate to product_service (converted in 6.6).
# IN (...) clauses use SQLAlchemy expanding bind params for text().

ASSORT_TBL = "pim_assortment"
NODE_TBL = "pim_taxonomy_node"
ENTITY_TBL = "pim_entity"


class PimAssortmentService:
    def __init__(self, db: Session, entity_name: str):
        self.db = db
        self.entity_name = entity_name
        self._assort = pim_sql.pim_tbl(entity_name, ASSORT_TBL)
        self._node = pim_sql.pim_tbl(entity_name, NODE_TBL)
        self._entity = pim_sql.pim_tbl(entity_name, ENTITY_TBL)

    def _fetch_in(self, sql: str, list_param: str, values: list, extra: dict = None):
        """Run a SELECT with an expanding IN list param over the per-entity engine."""
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
    # CREATE
    # ------------------------------------------------------------------
    def create_assortment(self, data: PimAssortmentCreate):
        try:
            node = pim_sql.fetch_one(
                self.db, f'SELECT "label" FROM {self._node} WHERE "id" = :id',
                {"id": data.taxonomy_node_id},
            )
            if not node:
                raise HTTPException(status_code=404, detail="Taxonomy node not found")

            existing = pim_sql.fetch_one(
                self.db,
                f'SELECT "id" FROM {self._assort} '
                f'WHERE "taxonomy_node_id" = :nid AND "steward_user_id" = :sid',
                {"nid": data.taxonomy_node_id, "sid": data.steward_user_id},
            )
            if existing:
                raise HTTPException(
                    status_code=409,
                    detail="Assortment already exists for this category and steward",
                )

            sql, params = pim_sql.build_insert(
                self.entity_name, ASSORT_TBL,
                {
                    "taxonomy_node_id": data.taxonomy_node_id,
                    "steward_user_id": data.steward_user_id,
                    "steward_display_name": data.steward_display_name,
                    "name": data.name or node["label"],
                    "description": data.description,
                },
            )
            pim_sql.execute(self.db, sql, params)
            db_commit_auto_rollback(db=self.db)
            return pim_sql.fetch_one(
                self.db, f'SELECT * FROM {self._assort} WHERE "id" = :id', {"id": params["id"]}
            )

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
            if steward_user_id:
                return pim_sql.fetch_all(
                    self.db,
                    f'SELECT * FROM {self._assort} WHERE "steward_user_id" = :sid '
                    f'ORDER BY "created_at" DESC',
                    {"sid": steward_user_id},
                )
            return pim_sql.fetch_all(
                self.db, f'SELECT * FROM {self._assort} ORDER BY "created_at" DESC'
            )
        except Exception as e:
            app_logger.exception(f"Error listing assortments: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------
    # GET
    # ------------------------------------------------------------------
    def get_assortment(self, assortment_id: str):
        try:
            assortment = pim_sql.fetch_one(
                self.db, f'SELECT * FROM {self._assort} WHERE "id" = :id', {"id": assortment_id}
            )
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
            existing = pim_sql.fetch_one(
                self.db, f'SELECT "id" FROM {self._assort} WHERE "id" = :id', {"id": assortment_id}
            )
            if not existing:
                raise HTTPException(status_code=404, detail="Assortment not found")

            set_values = {}
            if data.name is not None:
                set_values["name"] = data.name
            if data.description is not None:
                set_values["description"] = data.description
            if data.steward_user_id is not None:
                set_values["steward_user_id"] = data.steward_user_id
            if data.steward_display_name is not None:
                set_values["steward_display_name"] = data.steward_display_name

            if set_values:
                sql, params = pim_sql.build_update(
                    self.entity_name, ASSORT_TBL, set_values,
                    where='"id" = :id', where_params={"id": assortment_id},
                )
                pim_sql.execute(self.db, sql, params)
                db_commit_auto_rollback(db=self.db)
            return pim_sql.fetch_one(
                self.db, f'SELECT * FROM {self._assort} WHERE "id" = :id', {"id": assortment_id}
            )
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
            assortment = pim_sql.fetch_one(
                self.db, f'SELECT "name" FROM {self._assort} WHERE "id" = :id', {"id": assortment_id}
            )
            if not assortment:
                raise HTTPException(status_code=404, detail="Assortment not found")
            pim_sql.execute(self.db, f'DELETE FROM {self._assort} WHERE "id" = :id', {"id": assortment_id})
            db_commit_auto_rollback(db=self.db)
            return {"message": f"Assortment '{assortment['name']}' deleted."}
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
            assortments = pim_sql.fetch_all(
                self.db,
                f'SELECT * FROM {self._assort} WHERE "steward_user_id" = :sid '
                f'ORDER BY "created_at" DESC',
                {"sid": steward_user_id},
            )
            if not assortments:
                return []

            node_ids = [a["taxonomy_node_id"] for a in assortments]
            nodes = self._fetch_in(
                f'SELECT "id", "label", "materialized_path" FROM {self._node} WHERE "id" IN :ids',
                "ids", node_ids,
            )
            node_map = {n["id"]: n for n in nodes}

            # Product counts per node (active, top-level)
            count_rows = self._fetch_in(
                f'SELECT "taxonomy_node_id", COUNT("id") AS cnt FROM {self._entity} '
                f'WHERE "taxonomy_node_id" IN :ids AND "active" = TRUE AND "parent_id" IS NULL '
                f'GROUP BY "taxonomy_node_id"',
                "ids", node_ids,
            )
            prod_counts = {r["taxonomy_node_id"]: r["cnt"] for r in count_rows}

            # Product ids for completeness
            all_products = self._fetch_in(
                f'SELECT "id", "taxonomy_node_id" FROM {self._entity} '
                f'WHERE "taxonomy_node_id" IN :ids AND "active" = TRUE AND "parent_id" IS NULL',
                "ids", node_ids,
            )
            all_pids = [p["id"] for p in all_products]
            completeness_map = product_service._batch_compute_completeness(all_pids) if all_pids else {}

            node_completeness = {}
            for p in all_products:
                node_completeness.setdefault(p["taxonomy_node_id"], []).append(
                    completeness_map.get(p["id"], 100.0)
                )

            result = []
            for a in assortments:
                node = node_map.get(a["taxonomy_node_id"])
                pcount = prod_counts.get(a["taxonomy_node_id"], 0)
                c_list = node_completeness.get(a["taxonomy_node_id"], [])
                avg_c = round(sum(c_list) / len(c_list), 1) if c_list else 0.0
                result.append({
                    "id": a["id"],
                    "taxonomy_node_id": a["taxonomy_node_id"],
                    "steward_user_id": a["steward_user_id"],
                    "steward_display_name": a["steward_display_name"],
                    "name": a["name"],
                    "description": a["description"],
                    "category_label": node["label"] if node else None,
                    "category_path": node["materialized_path"] if node else None,
                    "product_count": pcount,
                    "avg_completeness": avg_c,
                    "created_at": a["created_at"],
                    "updated_at": a["updated_at"],
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
            assigned_node_ids = [
                r["taxonomy_node_id"] for r in pim_sql.fetch_all(
                    self.db, f'SELECT DISTINCT "taxonomy_node_id" FROM {self._assort}'
                )
            ]

            if assigned_node_ids:
                products = self._fetch_in(
                    f'SELECT * FROM {self._entity} '
                    f'WHERE "active" = TRUE AND "parent_id" IS NULL '
                    f'AND "taxonomy_node_id" NOT IN :ids '
                    f'ORDER BY "created_at" DESC LIMIT 200',
                    "ids", assigned_node_ids,
                )
            else:
                products = pim_sql.fetch_all(
                    self.db,
                    f'SELECT * FROM {self._entity} '
                    f'WHERE "active" = TRUE AND "parent_id" IS NULL '
                    f'ORDER BY "created_at" DESC LIMIT 200',
                )
            if not products:
                return []

            node_ids = list({p["taxonomy_node_id"] for p in products})
            nodes = self._fetch_in(
                f'SELECT "id", "label" FROM {self._node} WHERE "id" IN :ids', "ids", node_ids,
            )
            node_map = {n["id"]: n for n in nodes}

            pids = [p["id"] for p in products]
            completeness_map = product_service._batch_compute_completeness(pids) if pids else {}

            result = []
            for p in products:
                node = node_map.get(p["taxonomy_node_id"])
                result.append({
                    "id": p["id"],
                    "identifier": product_service._get_identifier_value(p["id"]),
                    "entity_type_id": p["entity_type_id"],
                    "status": p["status"],
                    "taxonomy_node_id": p["taxonomy_node_id"],
                    "category_label": node["label"] if node else None,
                    "completeness": completeness_map.get(p["id"], 0.0),
                    "created_at": p["created_at"],
                })
            return result

        except Exception as e:
            app_logger.exception(f"Error getting unassigned products: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))
