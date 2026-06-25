"""add display_order column to entityattributes

Revision ID: d1s2p3o4r5d6
Revises: f44bc0f2f539
Create Date: 2026-06-23

Adds display_order INT column to entityattributes table for user-defined
attribute ordering. Backfills existing rows with their id value to preserve
current insertion order.

All DDL guarded with existence checks (idempotent on retry / partial apply).
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "d1s2p3o4r5d6"
down_revision: Union[str, None] = "f44bc0f2f539"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _column_exists(conn, table_name: str, column_name: str) -> bool:
    dialect = conn.dialect.name
    if dialect == "mysql":
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
    if not _column_exists(conn, "entityattributes", "display_order"):
        op.add_column("entityattributes", sa.Column("display_order", sa.Integer(), nullable=False, server_default="0"))
        # Backfill: set display_order = id so existing attributes keep their current order
        op.execute("UPDATE entityattributes SET display_order = id")


def downgrade() -> None:
    conn = op.get_bind()
    if _column_exists(conn, "entityattributes", "display_order"):
        op.drop_column("entityattributes", "display_order")
