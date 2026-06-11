"""add enrichment_policies to entity_dnb

Revision ID: n9o0p1q2r3s4
Revises: l7m8n9o0p1q2
Create Date: 2026-03-05

Add enrichment_policies JSON column to entity_dnb table.
Stores per-entity D&B enrichment configuration (e.g. hierarchy, companyinfo).
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = 'n9o0p1q2r3s4'
down_revision: Union[str, None] = 'l7m8n9o0p1q2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _column_exists() -> bool:
    """Check if enrichment_policies column already exists."""
    conn = op.get_bind()
    dialect = conn.dialect.name

    if dialect == 'mysql':
        result = conn.execute(text(
            "SELECT COLUMN_NAME "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = DATABASE() "
            "AND TABLE_NAME = 'entity_dnb' "
            "AND COLUMN_NAME = 'enrichment_policies'"
        )).fetchone()
    else:
        result = conn.execute(text(
            "SELECT column_name "
            "FROM information_schema.columns "
            "WHERE table_catalog = current_database() "
            "AND table_name = 'entity_dnb' "
            "AND column_name = 'enrichment_policies'"
        )).fetchone()
    return result is not None


def upgrade() -> None:
    if _column_exists():
        return

    op.add_column('entity_dnb', sa.Column('enrichment_policies', sa.JSON(), nullable=True))


def downgrade() -> None:
    if not _column_exists():
        return

    op.drop_column('entity_dnb', 'enrichment_policies')
