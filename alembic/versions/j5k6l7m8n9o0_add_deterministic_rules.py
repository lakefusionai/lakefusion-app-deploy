"""add deterministic_rules column to model_experiments

Revision ID: j5k6l7m8n9o0
Revises: i4j5k6l7m8n9
Create Date: 2026-02-17 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'j5k6l7m8n9o0'
down_revision: Union[str, None] = 'i4j5k6l7m8n9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add nullable JSON column deterministic_rules to model_experiments."""
    op.add_column(
        'model_experiments',
        sa.Column('deterministic_rules', sa.JSON(), nullable=True)
    )


def downgrade() -> None:
    """Remove deterministic_rules column from model_experiments."""
    op.drop_column('model_experiments', 'deterministic_rules')
