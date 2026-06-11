"""feature_flag_for_sdk_changes

Revision ID: 9ac38a58bd93
Revises: d1e2f3g4h5i6
Create Date: 2026-02-05 10:00:38.877553

Adds SDK_CHANGES feature flag to toggle between:
- INACTIVE: Use production notebooks (integration-core, maven-core, dnb_integration)
- ACTIVE: Use SDK revamped notebooks (integration-core_revamped, maven-core_revamped, dnb_integration_revamped)
"""
from typing import Sequence, Union

from alembic import op
from sqlalchemy.sql import table, column
from sqlalchemy import String, Enum, Text, DateTime
from datetime import datetime, timezone


# revision identifiers, used by Alembic.
revision: str = '9ac38a58bd93'
down_revision: Union[str, None] = 'd1e2f3g4h5i6'
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
                "name": "SDK_CHANGES",
                "status": "INACTIVE",
                "description": "Enable SDK revamped notebooks with thin wrappers for integration-core, maven-core, and dnb_integration",
                "owner_team": "LakeFusion",
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
                "expires_at": None,
            }
        ]
    )


def downgrade() -> None:
    op.execute("DELETE FROM feature_flags WHERE name='SDK_CHANGES';")
