"""lakefusion-tags-feature-flag

Revision ID: a34962cf3f69
Revises: 0f387b4197ef
Create Date: 2025-11-05 17:41:15.724937

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import table, column
from sqlalchemy import String, Enum, Text, DateTime
from datetime import datetime
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision: str = 'a34962cf3f69'
down_revision: Union[str, None] = '0f387b4197ef'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ✅ Insert LakeFusion Tags feature flag
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
                "name": "ENABLE_LAKEFUSION_TAG_USAGE",
                "status": "INACTIVE",
                "description": "Enables dedicated LakeFusion tag update endpoint and workflow",
                "owner_team": "LakeFusion",
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "expires_at": None,
            }
        ]
    )


def downgrade() -> None:
    # Remove feature flag
    op.execute(
        "DELETE FROM feature_flags WHERE name='ENABLE_LAKEFUSION_TAG_USAGE';"
    )
