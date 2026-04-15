"""add_enableReferenceDataManagement_ff
Revision ID: c948a3a6cb29
Revises: k7l8m9n0o1p2
Create Date: 2026-01-15 16:39:30.122933
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import table, column
from sqlalchemy import String, Enum, Text, DateTime
from datetime import datetime

# revision identifiers, used by Alembic.
revision: str = 'c948a3a6cb29'
down_revision: Union[str, None] = 'k7l8m9n0o1p2'
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

   conn = op.get_bind()
   existing = conn.execute(
        sa.text("SELECT 1 FROM feature_flags WHERE name = 'ENABLE_REFERENCE_DATA_MANAGEMENT'")
    ).fetchone()

   if not existing:
        op.bulk_insert(
            feature_flags,
            [
                {
                    "name": "ENABLE_REFERENCE_DATA_MANAGEMENT",
                    "status": "INACTIVE",
                    "description": "Enables reference entity feature in the application",
                    "owner_team": "LakeFusion",
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                    "expires_at": None,
                }
            ]
        )


def downgrade() -> None:
    op.execute("DELETE FROM feature_flags WHERE name = 'ENABLE_REFERENCE_DATA_MANAGEMENT';")
