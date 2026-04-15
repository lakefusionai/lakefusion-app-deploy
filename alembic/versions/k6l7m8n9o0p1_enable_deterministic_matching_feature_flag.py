"""enable_deterministic_matching_feature_flag

Revision ID: k6l7m8n9o0p1
Revises: j5k6l7m8n9o0
Create Date: 2026-02-17

Adds ENABLE_DETERMINISTIC_MATCHING_FEATURE flag (INACTIVE by default)
to gate the Match Rules tab in the Add/Edit Model dialog.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import table, column
from sqlalchemy import String, Enum, Text, DateTime
from datetime import datetime

# revision identifiers, used by Alembic.
revision: str = 'k6l7m8n9o0p1'
down_revision: Union[str, None] = 'j5k6l7m8n9o0'
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
                "name": "ENABLE_DETERMINISTIC_MATCHING_FEATURE",
                "status": "INACTIVE",
                "description": "Enables the Match Rules tab in the Add/Edit Model dialog for MatchMaven",
                "owner_team": "LakeFusion",
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "expires_at": None,
            }
        ]
    )


def downgrade() -> None:
    op.execute(
        "DELETE FROM feature_flags WHERE name='ENABLE_DETERMINISTIC_MATCHING_FEATURE';"
    )
