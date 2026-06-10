"""disable_sdk_changes_flag

Revision ID: f2b436f4c2d5
Revises: abd52774154a
Create Date: 2026-03-16 15:33:44.107119

Sets SDK_CHANGES feature flag to INACTIVE to use production notebooks
(integration-core, maven-core, dnb_integration) instead of SDK revamped versions.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f2b436f4c2d5'
down_revision: Union[str, None] = 'abd52774154a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("UPDATE feature_flags SET status = 'INACTIVE' WHERE name = 'SDK_CHANGES';")


def downgrade() -> None:
    op.execute("UPDATE feature_flags SET status = 'ACTIVE' WHERE name = 'SDK_CHANGES';")
