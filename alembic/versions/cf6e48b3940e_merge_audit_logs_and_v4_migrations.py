"""merge_audit_logs_and_v4_migrations

Revision ID: cf6e48b3940e
Revises: 8f737e01bcd2, d9f7dc0339b8
Create Date: 2026-01-28 13:26:15.569281

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'cf6e48b3940e'
down_revision: Union[str, None] = ('8f737e01bcd2', 'd9f7dc0339b8')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
