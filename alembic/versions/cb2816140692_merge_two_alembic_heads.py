"""merge two alembic heads

Revision ID: cb2816140692
Revises: b2c3d4e5f6a7
Create Date: 2026-05-21 11:24:42.992489

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'cb2816140692'
down_revision: Union[str, None] = ('b2c3d4e5f6a7')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
