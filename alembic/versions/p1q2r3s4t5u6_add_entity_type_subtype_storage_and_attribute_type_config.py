"""add entity_type, entity_subtype, storage_type to entity and type_config to entityattributes
Revision ID: p1q2r3s4t5u6
Revises: c948a3a6cb29
Create Date: 2026-03-17 00:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'p1q2r3s4t5u6'
down_revision: Union[str, None] = 'c948a3a6cb29'
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

    # -- entity table --

    # entity_type: master | product | reference
    # All existing rows default to 'master' -- no data migration needed.
    if not _column_exists(conn, 'entity', 'entity_type'):
        op.add_column(
            'entity',
            sa.Column(
                'entity_type',
                sa.String(50),
                nullable=False,
                server_default='master',
            )
        )

    # entity_subtype: flat | hierarchical
    # Only applicable when entity_type = 'reference'. NULL for all other types.
    if not _column_exists(conn, 'entity', 'entity_subtype'):
        op.add_column(
            'entity',
            sa.Column(
                'entity_subtype',
                sa.String(50),
                nullable=True,
            )
        )

    # storage_type: delta | lakebase
    # All existing rows default to 'delta' -- no data migration needed.
    if not _column_exists(conn, 'entity', 'storage_type'):
        op.add_column(
            'entity',
            sa.Column(
                'storage_type',
                sa.String(50),
                nullable=False,
                server_default='delta',
            )
        )

    # -- entityattributes table --

    # type_config: stores type-specific metadata as JSON.
    # Used by REFERENCE_ENTITY (reference_entity_id, reference_entity_output_id)
    # and any future complex attribute types. NULL for scalar types.
    if not _column_exists(conn, 'entityattributes', 'type_config'):
        op.add_column(
            'entityattributes',
            sa.Column(
                'type_config',
                sa.JSON(),
                nullable=True,
            )
        )


def downgrade() -> None:
    op.drop_column('entityattributes', 'type_config')
    op.drop_column('entity', 'storage_type')
    op.drop_column('entity', 'entity_subtype')
    op.drop_column('entity', 'entity_type')
