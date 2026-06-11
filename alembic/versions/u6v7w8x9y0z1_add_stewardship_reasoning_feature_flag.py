"""add stewardship reasoning feature flag

Revision ID: u6v7w8x9y0z1
Revises: t5u6v7w8x9y0
Create Date: 2026-05-05

"""
from typing import Sequence, Union
from datetime import datetime
from alembic import op
import sqlalchemy as sa

from sqlalchemy.sql import table, column
from sqlalchemy import String, Enum, Text, DateTime


# revision identifiers, used by Alembic.
revision: str = 'u6v7w8x9y0z1'
down_revision: Union[str, None] = 't5u6v7w8x9y0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    feature_flags = table(
        'feature_flags',
        column('name', String),
        column('status', Enum),
        column('description', Text),
        column('owner_team', String),
        column('created_at', DateTime),
        column('updated_at', DateTime),
        column('expires_at', DateTime),
    )

    op.bulk_insert(
        feature_flags,
        [
            {
                "name": "ENABLE_STEWARDSHIP_REASONING",
                "status": "ACTIVE",
                "description": "Enables Adaptive Match Learning (AML) features: steward reason capture, stewardship history tab, and Add Reason button on potential matches and golden dedup",
                "owner_team": "LakeFusion",
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "expires_at": None,
            }
        ]
    )


def downgrade() -> None:
    op.execute(
        "DELETE FROM feature_flags WHERE name='ENABLE_STEWARDSHIP_REASONING';"
    )
