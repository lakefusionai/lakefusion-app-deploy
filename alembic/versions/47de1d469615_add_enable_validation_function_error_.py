"""add_ENABLE_VALIDATION_FUNCTION_ERROR_FEATURE

Revision ID: 47de1d469615
Revises: d9f7dc0339b8
Create Date: 2026-02-02 12:36:07.109792

"""
from typing import Sequence, Union

from alembic import op
from sqlalchemy.sql import table, column
from sqlalchemy import String, Enum, Text, DateTime
from datetime import datetime


# revision identifiers, used by Alembic.
revision: str = '47de1d469615'
down_revision: Union[str, None] = 'd9f7dc0339b8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ✅ Insert ENABLE_VALIDATION_FUNCTION_ERROR_FEATURE feature flag
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
                "name": "ENABLE_VALIDATION_FUNCTION_ERROR_FEATURE",
                "status": "INACTIVE",
                "description": "Enables validation function error feature in the application",
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
        "DELETE FROM feature_flags WHERE name='ENABLE_VALIDATION_FUNCTION_ERROR_FEATURE';"
    )
