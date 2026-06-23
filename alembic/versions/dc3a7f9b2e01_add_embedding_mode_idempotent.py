"""idempotent catch-up for skipped migrations

Revision ID: dc3a7f9b2e01
Revises: cb2816140692
Create Date: 2026-05-28 18:00:00.000000

Re-applies columns from revisions that were skipped in some environments:
  - y0z1a2b3c4d5: embedding_mode on model_experiments
  - b2c3d4e5f6a7: task_type on integration_hub + PIM sentinel row
Idempotent — skips if already present.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'dc3a7f9b2e01'
down_revision: Union[str, None] = 'cb2816140692'
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

    # From y0z1a2b3c4d5: embedding_mode on model_experiments
    if not _column_exists(bind, 'model_experiments', 'embedding_mode'):
        op.add_column(
            'model_experiments',
            sa.Column('embedding_mode', sa.String(20), nullable=True, server_default='managed')
        )

    # From b2c3d4e5f6a7: task_type on integration_hub
    if not _column_exists(bind, 'integration_hub', 'task_type'):
        op.add_column(
            'integration_hub',
            sa.Column('task_type', sa.String(50), nullable=False, server_default='mdm'),
        )

    # NOTE: The PIM_PLACEHOLDER sentinel (model_experiments id=-1) is NOT seeded here.
    # It has a NOT NULL FK to entity.id, but a fresh install has no entities yet, so an
    # install-time insert violates the FK. The sentinel is created lazily in
    # PimEntityBridgeService.initialize_pim() instead — the first point at which a real
    # entity is guaranteed to exist, for both fresh installs and existing customers.


def downgrade() -> None:
    bind = op.get_bind()
    if _column_exists(bind, 'model_experiments', 'embedding_mode'):
        op.drop_column('model_experiments', 'embedding_mode')
    if _column_exists(bind, 'integration_hub', 'task_type'):
        op.drop_column('integration_hub', 'task_type')
