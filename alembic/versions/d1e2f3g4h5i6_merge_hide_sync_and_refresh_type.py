"""merge hide_sync_table_from_ui and add_refresh_type_column

Revision ID: d1e2f3g4h5i6
Revises: 5d6eb922da06, 672634ea4156
Create Date: 2026-02-04 00:00:00.000000

Merges two independent migration branches:
- 5d6eb922da06: hide_sync_table_from_ui
- 672634ea4156: add_refresh_type_column
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd1e2f3g4h5i6'
down_revision: Union[str, Sequence[str], None] = ('5d6eb922da06', '672634ea4156')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
