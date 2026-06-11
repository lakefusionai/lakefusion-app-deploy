from sqlalchemy.orm import Session
from fastapi import HTTPException
from lakefusion_utility.models.pim import PimEntityTier, PimEntityTierCreate
from lakefusion_utility.utils.logging_utils import get_logger

app_logger = get_logger(__name__)


class PimEntityTierService:
    def __init__(self, db: Session):
        self.db = db

    def list_tiers(self):
        """Return all tiers ordered by level (top-tier first)."""
        try:
            return self.db.query(PimEntityTier).order_by(PimEntityTier.level).all()
        except Exception as e:
            app_logger.error(f"Error listing entity tiers: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    def get_tier(self, tier_id: str):
        tier = self.db.query(PimEntityTier).filter(PimEntityTier.id == tier_id).first()
        if not tier:
            raise HTTPException(status_code=404, detail="Entity tier not found.")
        return tier

    def create_tier(self, data: PimEntityTierCreate):
        """Add a new tier. Validates level uniqueness and parent exists."""
        try:
            # Check code uniqueness
            existing = self.db.query(PimEntityTier).filter(PimEntityTier.code == data.code).first()
            if existing:
                raise HTTPException(status_code=409, detail=f"Tier with code '{data.code}' already exists.")

            # Check level uniqueness
            existing_level = self.db.query(PimEntityTier).filter(PimEntityTier.level == data.level).first()
            if existing_level:
                raise HTTPException(
                    status_code=409,
                    detail=f"Level {data.level} is already used by tier '{existing_level.code}'. "
                           "Reorder existing tiers first.",
                )

            # Validate parent tier exists if provided
            if data.parent_tier_id:
                parent = self.db.query(PimEntityTier).filter(PimEntityTier.id == data.parent_tier_id).first()
                if not parent:
                    raise HTTPException(status_code=404, detail="Parent tier not found.")

            tier = PimEntityTier(
                code=data.code.upper(),
                label=data.label,
                level=data.level,
                parent_tier_id=data.parent_tier_id,
                is_leaf=data.is_leaf,
            )
            self.db.add(tier)
            self.db.commit()
            self.db.refresh(tier)
            app_logger.info(f"Entity tier created: {tier.code} (level={tier.level})")
            return tier

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
            tier = self.db.query(PimEntityTier).filter(PimEntityTier.id == tier_id).first()
            if not tier:
                raise HTTPException(status_code=404, detail="Entity tier not found.")

            if "label" in data:
                tier.label = data["label"]
            if "is_leaf" in data:
                tier.is_leaf = data["is_leaf"]

            self.db.commit()
            self.db.refresh(tier)
            app_logger.info(f"Entity tier updated: {tier.code}")
            return tier

        except HTTPException:
            self.db.rollback()
            raise
        except Exception as e:
            self.db.rollback()
            app_logger.error(f"Error updating entity tier: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    def delete_tier(self, tier_id: str):
        """Delete a tier. Fails if any products use this tier's code."""
        try:
            tier = self.db.query(PimEntityTier).filter(PimEntityTier.id == tier_id).first()
            if not tier:
                raise HTTPException(status_code=404, detail="Entity tier not found.")

            # Check if any products use this tier code
            from lakefusion_utility.models.pim import PimEntity
            product_count = self.db.query(PimEntity).filter(
                PimEntity.entity_type_id == tier.code
            ).count()
            if product_count > 0:
                raise HTTPException(
                    status_code=409,
                    detail=f"Cannot delete tier '{tier.code}' — {product_count} product(s) still use it. "
                           "Reassign or delete those products first.",
                )

            # Check if child tiers reference this as parent
            child_count = self.db.query(PimEntityTier).filter(
                PimEntityTier.parent_tier_id == tier_id
            ).count()
            if child_count > 0:
                raise HTTPException(
                    status_code=409,
                    detail=f"Cannot delete tier '{tier.code}' — {child_count} child tier(s) reference it. "
                           "Delete or reparent child tiers first.",
                )

            self.db.delete(tier)
            self.db.commit()
            app_logger.info(f"Entity tier deleted: {tier.code}")

        except HTTPException:
            self.db.rollback()
            raise
        except Exception as e:
            self.db.rollback()
            app_logger.error(f"Error deleting entity tier: {e}")
            raise HTTPException(status_code=500, detail=str(e))
