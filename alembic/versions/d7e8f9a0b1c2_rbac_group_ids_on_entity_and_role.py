"""Add RBAC group id columns to entity and role

Revision ID: d7e8f9a0b1c2
Revises: f5a6b7c8d9e0
Create Date: 2026-04-23 10:00:00.000000

Adds:
  entity.developer_group_id  — Databricks SCIM group id for LAKEFUSION_DEVELOPER_<ENTITY>
  entity.steward_group_id    — Databricks SCIM group id for LAKEFUSION_DATA_STEWARD_<ENTITY>
  roles.dbx_group_id         — Databricks SCIM group id for LAKEFUSION_ADMIN

Populated by RBACProvisioningService at entity-create time. Nullable so existing
entities continue to work; a separate backfill migration (or pipeline-time executor)
populates rows for pre-existing entities.
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "d7e8f9a0b1c2"
down_revision: Union[str, None] = "f5a6b7c8d9e0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _column_exists(conn, table: str, column: str) -> bool:
    inspector = sa.inspect(conn)
    return column in {c["name"] for c in inspector.get_columns(table)}


def upgrade() -> None:
    conn = op.get_bind()

    if not _column_exists(conn, "entity", "developer_group_id"):
        op.add_column(
            "entity",
            sa.Column("developer_group_id", sa.String(255), nullable=True),
        )
    if not _column_exists(conn, "entity", "steward_group_id"):
        op.add_column(
            "entity",
            sa.Column("steward_group_id", sa.String(255), nullable=True),
        )
    if not _column_exists(conn, "roles", "dbx_group_id"):
        op.add_column(
            "roles",
            sa.Column("dbx_group_id", sa.String(255), nullable=True),
        )


def downgrade() -> None:
    conn = op.get_bind()
    if _column_exists(conn, "entity", "developer_group_id"):
        op.drop_column("entity", "developer_group_id")
    if _column_exists(conn, "entity", "steward_group_id"):
        op.drop_column("entity", "steward_group_id")
    if _column_exists(conn, "roles", "dbx_group_id"):
        op.drop_column("roles", "dbx_group_id")
