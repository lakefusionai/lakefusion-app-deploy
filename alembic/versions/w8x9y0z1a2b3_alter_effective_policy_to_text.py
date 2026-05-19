"""alter effective_policy column from String(50) to Text

The effective_policy column in notebook_sync_audit_log was defined as
String(50), which is too short for force-sync messages that include
the user email and version info (e.g. "Force sync by user@domain.com
— version upgrade: none -> 4.2.0").

Revision ID: w8x9y0z1a2b3
Revises: v7w8x9y0z1a2
Create Date: 2026-05-12
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = 'w8x9y0z1a2b3'
down_revision: Union[str, None] = 'v7w8x9y0z1a2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    try:
        op.alter_column(
            'notebook_sync_audit_log',
            'effective_policy',
            existing_type=sa.String(50),
            type_=sa.Text(),
            existing_nullable=True,
        )
    except Exception:
        pass


def downgrade() -> None:
    try:
        op.alter_column(
            'notebook_sync_audit_log',
            'effective_policy',
            existing_type=sa.Text(),
            type_=sa.String(50),
            existing_nullable=True,
        )
    except Exception:
        pass
