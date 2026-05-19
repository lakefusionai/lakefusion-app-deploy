"""merge sdk change and validation

Revision ID: 64a5cc16b57e
Revises: 77aefbdb6538, a84402de100f
Create Date: 2026-02-12 17:31:53.801197

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '64a5cc16b57e'
down_revision: Union[str, None] = ('77aefbdb6538', 'a84402de100f')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
