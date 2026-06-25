from sqlalchemy.orm import Session
from fastapi import HTTPException
from app.lakefusion_pim_service.utils import pim_sql
from lakefusion_utility.models.pim import PimEntityTierCreate
from lakefusion_utility.utils.logging_utils import get_logger

app_logger = get_logger(__name__)

# SCRUM-1929 Phase 6.2 — converted from ORM to raw SQL against the Delta-synced
# tables gold."{entity}_pim_entity_tier_prod_synced". pim_entity_tier has a UUID
# id (auto-injected) and updated_at (auto-set on update). Delete cross-checks the
# pim_entity table (products) and self (child tiers). entity_name is threaded
# from the route's /{entity_name} prefix into the service constructor.

TIER_TBL = "pim_entity_tier"


class PimEntityTierService:
    def __init__(self, db: Session, entity_name: str):
        self.db = db
        self.entity_name = entity_name
        self._tier = pim_sql.pim_tbl(entity_name, TIER_TBL)

    def list_tiers(self):
        """Return all tiers ordered by level (top-tier first)."""
        try:
            return pim_sql.coerce_rows(
                TIER_TBL,
                pim_sql.fetch_all(self.db, f'SELECT * FROM {self._tier} ORDER BY "level"'),
            )
        except Exception as e:
            app_logger.error(f"Error listing entity tiers: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    def get_tier(self, tier_id: str):
        tier = pim_sql.coerce_row(TIER_TBL, pim_sql.fetch_one(
            self.db, f'SELECT * FROM {self._tier} WHERE "id" = :id', {"id": tier_id}
        ))
        if not tier:
            raise HTTPException(status_code=404, detail="Entity tier not found.")
        return tier

    def create_tier(self, data: PimEntityTierCreate):
        """Add a new tier. Validates code/level uniqueness and parent exists."""
        try:
            # Check code uniqueness
            existing = pim_sql.fetch_one(
                self.db, f'SELECT "id" FROM {self._tier} WHERE "code" = :c', {"c": data.code.upper()}
            )
            if existing:
                raise HTTPException(status_code=409, detail=f"Tier with code '{data.code}' already exists.")

            # Check level uniqueness
            existing_level = pim_sql.fetch_one(
                self.db, f'SELECT "code" FROM {self._tier} WHERE "level" = :lvl', {"lvl": data.level}
            )
            if existing_level:
                raise HTTPException(
                    status_code=409,
                    detail=f"Level {data.level} is already used by tier '{existing_level['code']}'. "
                           "Reorder existing tiers first.",
                )

            # Validate parent tier exists if provided
            if data.parent_tier_id:
                parent = pim_sql.fetch_one(
                    self.db, f'SELECT "id" FROM {self._tier} WHERE "id" = :id',
                    {"id": data.parent_tier_id},
                )
                if not parent:
                    raise HTTPException(status_code=404, detail="Parent tier not found.")

            sql, params = pim_sql.build_insert(
                self.entity_name, TIER_TBL,
                {
                    "code": data.code.upper(),
                    "label": data.label,
                    "level": data.level,
                    "parent_tier_id": data.parent_tier_id,
                    "is_leaf": data.is_leaf,
                },
            )
            pim_sql.execute(self.db, sql, params)
            self.db.commit()
            app_logger.info(f"Entity tier created: {params['code']} (level={params['level']})")
            return pim_sql.fetch_one(
                self.db, f'SELECT * FROM {self._tier} WHERE "id" = :id', {"id": params["id"]}
            )

        except HTTPException:
            self.db.rollback()
            raise
        except Exception as e:
            self.db.rollback()
            app_logger.error(f"Error creating entity tier: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    def update_tier(self, tier_id: str, data: dict):
        """Update tier label or is_leaf flag. Code and level are immutable after creation."""
        try:
            tier = pim_sql.fetch_one(
                self.db, f'SELECT "code" FROM {self._tier} WHERE "id" = :id', {"id": tier_id}
            )
            if not tier:
                raise HTTPException(status_code=404, detail="Entity tier not found.")

            set_values = {}
            if "label" in data:
                set_values["label"] = data["label"]
            if "is_leaf" in data:
                set_values["is_leaf"] = data["is_leaf"]

            if set_values:
                sql, params = pim_sql.build_update(
                    self.entity_name, TIER_TBL, set_values,
                    where='"id" = :id', where_params={"id": tier_id},
                )
                pim_sql.execute(self.db, sql, params)
                self.db.commit()
            app_logger.info(f"Entity tier updated: {tier['code']}")
            return pim_sql.fetch_one(
                self.db, f'SELECT * FROM {self._tier} WHERE "id" = :id', {"id": tier_id}
            )

        except HTTPException:
            self.db.rollback()
            raise
        except Exception as e:
            self.db.rollback()
            app_logger.error(f"Error updating entity tier: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    def delete_tier(self, tier_id: str):
        """Delete a tier. Fails if any products use this tier's code or child tiers reference it."""
        try:
            tier = pim_sql.fetch_one(
                self.db, f'SELECT "code" FROM {self._tier} WHERE "id" = :id', {"id": tier_id}
            )
            if not tier:
                raise HTTPException(status_code=404, detail="Entity tier not found.")
            tier_code = tier["code"]

            # Check if any products use this tier code (FK guard — no DB FK on synced tables)
            entity_tbl = pim_sql.pim_tbl(self.entity_name, "pim_entity")
            product_count = pim_sql.fetch_scalar(
                self.db, f'SELECT COUNT(*) FROM {entity_tbl} WHERE "entity_type_id" = :c',
                {"c": tier_code},
            )
            if product_count:
                raise HTTPException(
                    status_code=409,
                    detail=f"Cannot delete tier '{tier_code}' — {product_count} product(s) still use it. "
                           "Reassign or delete those products first.",
                )

            # Check if child tiers reference this as parent
            child_count = pim_sql.fetch_scalar(
                self.db, f'SELECT COUNT(*) FROM {self._tier} WHERE "parent_tier_id" = :id',
                {"id": tier_id},
            )
            if child_count:
                raise HTTPException(
                    status_code=409,
                    detail=f"Cannot delete tier '{tier_code}' — {child_count} child tier(s) reference it. "
                           "Delete or reparent child tiers first.",
                )

            pim_sql.execute(self.db, f'DELETE FROM {self._tier} WHERE "id" = :id', {"id": tier_id})
            self.db.commit()
            app_logger.info(f"Entity tier deleted: {tier_code}")

        except HTTPException:
            self.db.rollback()
            raise
        except Exception as e:
            self.db.rollback()
            app_logger.error(f"Error deleting entity tier: {e}")
            raise HTTPException(status_code=500, detail=str(e))
