"""merge alembic heads: complex data types and embedding mode

Revision ID: d33c14388fed
Revises: a2b3c4d5e6f7
Create Date: 2026-06-04 01:00:00.287226

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd33c14388fed'
down_revision: Union[str, None] = ('a2b3c4d5e6f7')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
