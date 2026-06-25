"""merge struct_definition_scope and lakegraph_url heads

Revision ID: f44bc0f2f539
Revises: s7t8r9u0c1t2
Create Date: 2026-06-24 15:29:02.839631

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f44bc0f2f539'
down_revision: Union[str, None] = ('s7t8r9u0c1t2')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
