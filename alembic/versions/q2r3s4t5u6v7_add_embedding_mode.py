"""add embedding_mode column to model_experiments

Revision ID: q2r3s4t5u6v7
Revises: p1q2r3s4t5u6
Create Date: 2026-04-24 12:00:00.000000

Adds embedding_mode column to model_experiments table.
Values: 'managed' (default, existing behavior) or 'precomputed' (new path).
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'q2r3s4t5u6v7'
down_revision: Union[str, None] = 'p1q2r3s4t5u6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'model_experiments',
        sa.Column('embedding_mode', sa.String(20), nullable=True, server_default='managed')
    )


def downgrade() -> None:
    op.drop_column('model_experiments', 'embedding_mode')
