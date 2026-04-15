"""merge pipeline_mode and entity_attribute_datatype changes

Revision ID: 5bb4ce57d8e9
Revises: dbfafff755c8, f91a68dac8bc
Create Date: 2026-01-12 07:51:14.822442

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5bb4ce57d8e9'
down_revision: Union[str, None] = ('dbfafff755c8', 'f91a68dac8bc')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
