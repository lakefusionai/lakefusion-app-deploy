"""add steward reason columns to entity_search_databricks_job

Revision ID: t5u6v7w8x9y0
Revises: s4t5u6v7w8x9
Create Date: 2026-05-01

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 't5u6v7w8x9y0'
down_revision: Union[str, None] = 's4t5u6v7w8x9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('entity_search_databricks_job', sa.Column('steward_reason', sa.Text(), nullable=True))
    op.add_column('entity_search_databricks_job', sa.Column('steward_reason_category', sa.String(255), nullable=True))


def downgrade() -> None:
    op.drop_column('entity_search_databricks_job', 'steward_reason_category')
    op.drop_column('entity_search_databricks_job', 'steward_reason')
