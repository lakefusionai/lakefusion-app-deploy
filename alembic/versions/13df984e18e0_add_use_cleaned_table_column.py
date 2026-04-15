"""add_use_cleaned_table_column

Revision ID: 13df984e18e0
Revises: e5f6a7b8c9d0
Create Date: 2026-01-19 21:59:25.040823

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision: str = '13df984e18e0'
down_revision: Union[str, None] = 'e5f6a7b8c9d0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('entitydatasetmapping', sa.Column('use_cleaned_table', sa.Boolean(), nullable=False, server_default='0'))


def downgrade() -> None:
    op.drop_column('entitydatasetmapping', 'use_cleaned_table')
