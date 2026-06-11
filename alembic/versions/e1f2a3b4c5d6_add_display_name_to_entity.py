"""add display_name column to entity table and backfill from name

Revision ID: e1f2a3b4c5d6
Revises: dc3a7f9b2e01
Create Date: 2026-06-08 00:00:00.000000

Adds display_name (String(255), nullable) to the entity table.
Backfills existing rows with the current name value so display_name
is populated for all pre-existing entities.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "e1f2a3b4c5d6"
down_revision: Union[str, None] = "dc3a7f9b2e01"
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

    if not _column_exists(conn, "entity", "display_name"):
        op.add_column(
            "entity",
            sa.Column("display_name", sa.String(255), nullable=True),
        )

    # Backfill: set display_name = name for all existing rows where display_name is NULL
    conn.execute(
        sa.text("UPDATE entity SET display_name = name WHERE display_name IS NULL")
    )


def downgrade() -> None:
    conn = op.get_bind()
    if _column_exists(conn, "entity", "display_name"):
        op.drop_column("entity", "display_name")
