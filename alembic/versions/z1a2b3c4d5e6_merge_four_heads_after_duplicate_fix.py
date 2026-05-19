"""merge four heads after duplicate-revision fix (with self-healing backfill)

Revision ID: z1a2b3c4d5e6
Revises: e8f9a0b1c2d3, w8x9y0z1a2b3, x9y0z1a2b3c4, y0z1a2b3c4d5
Create Date: 2026-05-18 00:00:00.000000

After renaming the two duplicate-id orphan migrations (p1q2r3s4t5u6 →
x9y0z1a2b3c4 and q2r3s4t5u6v7 → y0z1a2b3c4d5), the migration graph had
four parallel heads that `alembic upgrade head` couldn't disambiguate:

    e8f9a0b1c2d3 — bootstrap_lakefusion_admin_group
    w8x9y0z1a2b3 — alter_effective_policy_to_text
    x9y0z1a2b3c4 — add_execution_type_to_entity_search_job
    y0z1a2b3c4d5 — add_embedding_mode

This merge revision has all four as parents, becoming the single new head.

Self-healing backfill
---------------------
While the duplicate-id condition existed, alembic's load order picked ONE of
each duplicate pair and silently orphaned the other. Environments that ran
migrations during that window applied the "winner" DDL only and skipped the
"loser" DDL — yet `alembic_version` was advanced past the revision id as if
both had run. After the rename, alembic treats those revisions as already
applied (because they're on the current branch's ancestry), so it never
retries the orphaned DDL.

This block re-applies the DDL/data from the orphaned branch with idempotent
guards. On clean DBs that walked the correct branch, every check returns
False/exists and the block is a no-op. On DBs that took the wrong duplicate
path, the missing columns/rows are filled in.

Orphaned items recovered:
    - entity.entity_type, entity_subtype, storage_type (from entity_type_subtype)
    - entityattributes.type_config                     (from entity_type_subtype)
    - feature_flags row ENABLE_REFERENCE_DATA_MANAGEMENT (from c948a3a6cb29)
"""
from typing import Sequence, Union
from datetime import datetime

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "z1a2b3c4d5e6"
down_revision: Union[str, Sequence[str], None] = (
    "e8f9a0b1c2d3",
    "w8x9y0z1a2b3",
    "x9y0z1a2b3c4",
    "y0z1a2b3c4d5",
)
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
    conn = op.get_bind()

    # entity.entity_type
    if not _column_exists(conn, 'entity', 'entity_type'):
        op.add_column(
            'entity',
            sa.Column('entity_type', sa.String(50), nullable=False, server_default='master'),
        )

    # entity.entity_subtype
    if not _column_exists(conn, 'entity', 'entity_subtype'):
        op.add_column(
            'entity',
            sa.Column('entity_subtype', sa.String(50), nullable=True),
        )

    # entity.storage_type
    if not _column_exists(conn, 'entity', 'storage_type'):
        op.add_column(
            'entity',
            sa.Column('storage_type', sa.String(50), nullable=False, server_default='delta'),
        )

    # entityattributes.type_config
    if not _column_exists(conn, 'entityattributes', 'type_config'):
        op.add_column(
            'entityattributes',
            sa.Column('type_config', sa.JSON(), nullable=True),
        )

    # feature_flags row: ENABLE_REFERENCE_DATA_MANAGEMENT
    try:
        existing = conn.execute(
            sa.text(
                "SELECT 1 FROM feature_flags "
                "WHERE name = 'ENABLE_REFERENCE_DATA_MANAGEMENT'"
            )
        ).fetchone()
        if not existing:
            conn.execute(
                sa.text(
                    "INSERT INTO feature_flags "
                    "(name, status, description, owner_team, created_at, updated_at, expires_at) "
                    "VALUES (:name, :status, :description, 'LakeFusion', :now, :now, NULL)"
                ),
                {
                    "name": "ENABLE_REFERENCE_DATA_MANAGEMENT",
                    "status": "INACTIVE",
                    "description": "Enables reference entity feature in the application",
                    "now": datetime.utcnow(),
                },
            )
    except Exception:
        pass


def downgrade() -> None:
    pass
