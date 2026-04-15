"""enable_documentation_hub_feature_flag

Revision ID: 2a8b9c1d3e4f
Revises: cf6e48b3940e
Create Date: 2026-01-30

Enables the Documentation Hub feature in the Self-Service Portal:
1. Adds ENABLE_DOCUMENTATION_HUB feature flag
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import table, column
from sqlalchemy import String, Enum, Text, DateTime
from datetime import datetime

# revision identifiers, used by Alembic.
revision: str = '2a8b9c1d3e4f'
down_revision: Union[str, None] = 'cf6e48b3940e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Insert ENABLE_DOCUMENTATION_HUB flag
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
                "name": "ENABLE_DOCUMENTATION_HUB",
                "status": "INACTIVE",
                "description": "Enables the Documentation Hub feature in the Self-Service Portal for accessing API docs, user guides, and developer documentation",
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
        "DELETE FROM feature_flags WHERE name='ENABLE_DOCUMENTATION_HUB';"
    )
