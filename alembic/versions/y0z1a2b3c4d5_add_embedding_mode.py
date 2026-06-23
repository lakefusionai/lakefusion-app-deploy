"""add embedding_mode column to model_experiments

Revision ID: y0z1a2b3c4d5
Revises: x9y0z1a2b3c4
Create Date: 2026-04-24 12:00:00.000000

Adds embedding_mode column to model_experiments table.
Values: 'managed' (default, existing behavior) or 'precomputed' (new path).

NOTE: Originally created with revision id 'q2r3s4t5u6v7' which collided with
another migration on a parallel branch (alter_effective_policy_to_text).
Renamed to 'y0z1a2b3c4d5' to break the duplicate. If this migration was
already applied on an environment under the old id, run on that DB:
    UPDATE alembic_version SET version_num = 'y0z1a2b3c4d5'
    WHERE version_num = 'q2r3s4t5u6v7';
(only if the applied row was THIS migration, not the alter_effective_policy one).
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'y0z1a2b3c4d5'
down_revision: Union[str, None] = 'x9y0z1a2b3c4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _column_exists(conn, table_name: str, column_name: str) -> bool:
    dialect = conn.dialect.name
    if dialect == 'mysql':
        result = conn.execute(
            sa.text(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_schema = DATABASE() AND table_name = :table AND column_name = :column"
            ),
            {"table": table_name, "column": column_name},
        ).fetchone()
    else:
        result = conn.execute(
            sa.text(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_catalog = current_database() AND table_name = :table AND column_name = :column"
            ),
            {"table": table_name, "column": column_name},
        ).fetchone()
    return result is not None


def upgrade() -> None:
    bind = op.get_bind()
    if not _column_exists(bind, 'model_experiments', 'embedding_mode'):
        op.add_column(
            'model_experiments',
            sa.Column('embedding_mode', sa.String(20), nullable=True, server_default='managed')
        )


def downgrade() -> None:
    op.drop_column('model_experiments', 'embedding_mode')
