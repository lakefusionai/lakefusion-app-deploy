"""pipeline_mode

Revision ID: f91a68dac8bc
Revises: a1b2c3d4e5f6
Create Date: 2025-12-30 00:39:04.630009

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f91a68dac8bc'
down_revision: Union[str, None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add pipeline_mode column to integration_hub table
    op.add_column(
        'integration_hub',
        sa.Column('pipeline_mode', sa.String(50), nullable=True, server_default='separate')
    )
    
    # Add pipeline_mode to backup table for tracking
    op.add_column(
        'integration_hub_backup',
        sa.Column('pipeline_mode', sa.String(50), nullable=True)
    )
    
    # Update existing records to have default pipeline_mode = 'separate'
    op.execute("""
        UPDATE integration_hub 
        SET pipeline_mode = 'separate' 
        WHERE pipeline_mode IS NULL
    """)


def downgrade() -> None:
    # Drop columns
    op.drop_column('integration_hub_backup', 'pipeline_mode')
    op.drop_column('integration_hub', 'pipeline_mode')
