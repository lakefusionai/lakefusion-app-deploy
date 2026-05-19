"""activate validation function warning feature flag

Revision ID: c3d4e5f6a7b8
Revises: d9f7dc0339b8
Create Date: 2026-02-03 18:45:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c3d4e5f6a7b8'
down_revision: Union[str, None] = 'd9f7dc0339b8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        UPDATE feature_flags
        SET status = 'ACTIVE'
        WHERE name = 'ENABLE_VALIDATION_FUNCTION_WARNING_FEATURE';
    """)


def downgrade() -> None:
    op.execute("""
        UPDATE feature_flags
        SET status = 'INACTIVE'
        WHERE name = 'ENABLE_VALIDATION_FUNCTION_WARNING_FEATURE';
    """)
