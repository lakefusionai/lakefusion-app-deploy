"""sdk_changes_active_by_default

Revision ID: 77aefbdb6538
Revises: 76cc3702342f
Create Date: 2026-02-12 07:52:52.540310

Sets SDK_CHANGES feature flag to ACTIVE by default.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '77aefbdb6538'
down_revision: Union[str, None] = '76cc3702342f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("UPDATE feature_flags SET status = 'ACTIVE' WHERE name = 'SDK_CHANGES';")


def downgrade() -> None:
    op.execute("UPDATE feature_flags SET status = 'INACTIVE' WHERE name = 'SDK_CHANGES';")
