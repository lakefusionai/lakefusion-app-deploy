"""add_refresh_type_column

Revision ID: 672634ea4156
Revises: 2a8b9c1d3e4f
Create Date: 2026-01-27 12:02:35.271953

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.orm import Session
from sqlalchemy import text
import logging

# revision identifiers, used by Alembic.
revision: str = '672634ea4156'
down_revision: Union[str, None] = '2a8b9c1d3e4f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

logger = logging.getLogger(__name__)


def upgrade() -> None:
    # Add refresh_type column to profiling_tasks
    op.add_column('profiling_tasks', sa.Column('refresh_type', sa.String(length=255), nullable=True))
    op.add_column('profiling_tasks', sa.Column('refresh_info', sa.JSON, nullable=True))
    
    # Update existing records with refresh type "onSchedule"
    bind = op.get_bind()
    session = Session(bind=bind)
    
    try:
        session.execute(text("UPDATE profiling_tasks SET refresh_type = 'onSchedule' WHERE refresh_type IS NULL"))
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"failed to update refresh_type column: {e}")
    finally:
        session.close()


def downgrade() -> None:
    op.drop_column('profiling_tasks', 'refresh_type')
    op.drop_column('profiling_tasks', 'refresh_info')