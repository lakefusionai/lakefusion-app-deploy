"""File Entity Matching Under Feature Flag

Revision ID: f6fb868870a0
Revises: 4baa643b7fe5
Create Date: 2025-11-15 01:46:11.241564

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import table, column
from sqlalchemy import String, Enum, Text, DateTime
from datetime import datetime
from sqlalchemy.dialects import mysql


# revision identifiers, used by Alembic.
revision: str = 'f6fb868870a0'
down_revision: Union[str, None] = '4baa643b7fe5'
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
                "name": "ENABLE_FILE_ENTITY_MATCHING_MODULE",
                "status": "INACTIVE",
                "description": "Enables File Entity Matching module in console and Side Menu",
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
        "DELETE FROM feature_flags WHERE name='ENABLE_FILE_ENTITY_MATCHING_MODULE';"
    )
