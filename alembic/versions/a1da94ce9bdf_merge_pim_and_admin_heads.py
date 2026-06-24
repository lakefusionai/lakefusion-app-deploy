"""merge_pim_and_admin_heads

Revision ID: a1da94ce9bdf
Revises: 8e72450b9d02
Create Date: 2026-04-24 13:30:38.772574

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a1da94ce9bdf'
down_revision: Union[str, None] = ('8e72450b9d02')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
