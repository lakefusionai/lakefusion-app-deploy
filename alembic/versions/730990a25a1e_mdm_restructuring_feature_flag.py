"""mdm_restructuring_feature_flag

Revision ID: 730990a25a1e
Revises: fbdf42f72c40
Create Date: 2025-11-12 20:00:12.816479

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import table, column
from sqlalchemy import String, Enum, Text, DateTime
from datetime import datetime
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision: str = '730990a25a1e'
down_revision: Union[str, None] = 'fbdf42f72c40'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ✅ Insert Base Prompt feature flag
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
                "name": "MDM_RESTRUCTURING",
                "status": "INACTIVE",
                "description": "Enable the new Integration Hub pipeline flow with initial/incremental load support",
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
        "DELETE FROM feature_flags WHERE name='MDM_RESTRUCTURING';"
    )