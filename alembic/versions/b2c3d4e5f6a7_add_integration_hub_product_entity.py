"""unify integration_hub — add task_type, seed PIM sentinel model

Revision ID: b2c3d4e5f6a7
Revises: a1da94ce9bdf
Create Date: 2026-04-27 10:00:00.000000

PIM tasks use the main integration_hub table with task_type='pim'
and modelid=-1 (PIM_PLACEHOLDER sentinel) instead of NULL.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'b2c3d4e5f6a7'
down_revision: Union[str, None] = 'a1da94ce9bdf'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    conn = op.get_bind()

    # Add task_type column (skip if a prior partial run already added it).
    # Use SQLAlchemy inspect() so the check is dialect-agnostic (no DATABASE()).
    columns = [c["name"] for c in sa.inspect(conn).get_columns("integration_hub")]
    if "task_type" not in columns:
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
    op.drop_column('integration_hub', 'task_type')
