"""merge heads

Revision ID: 4baa643b7fe5
Revises: 51ff99157296, 730990a25a1e
Create Date: 2025-11-15 01:26:16.651425

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4baa643b7fe5'
down_revision: Union[str, None] = ('51ff99157296', '730990a25a1e')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
