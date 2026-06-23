"""add complex types to schema_evolution_edits: is_array + struct_definition_id

Revision ID: se1c2o3m4p5x
Revises: i7s8c9h0e1m2
Create Date: 2026-06-23 00:00:00.000000

Extends schema evolution to support ARRAY / STRUCT / ARRAY<STRUCT> attributes,
mirroring entityattributes.is_array + struct_definition_id.

- schema_evolution_edits.is_array (bool, default false): orthogonal flag,
  combines with attribute_type to express ARRAY<scalar> / ARRAY<STRUCT>.
- schema_evolution_edits.struct_definition_id (FK -> struct_definition.id,
  nullable, ON DELETE RESTRICT): set when attribute_type='STRUCT'.

All DDL guarded with existence checks (idempotent on retry / partial apply).
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "se1c2o3m4p5x"
down_revision: Union[str, None] = "i7s8c9h0e1m2"
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


def _constraint_exists(conn, table_name: str, constraint_name: str) -> bool:
    dialect = conn.dialect.name
    if dialect == "mysql":
        result = conn.execute(
            sa.text(
                "SELECT 1 FROM information_schema.table_constraints "
                "WHERE table_schema = DATABASE() AND table_name = :table AND constraint_name = :constraint"
            ),
            {"table": table_name, "constraint": constraint_name},
        ).fetchone()
    else:
        result = conn.execute(
            sa.text(
                "SELECT 1 FROM information_schema.table_constraints "
                "WHERE table_catalog = current_database() AND table_name = :table AND constraint_name = :constraint"
            ),
            {"table": table_name, "constraint": constraint_name},
        ).fetchone()
    return result is not None


def upgrade() -> None:
    conn = op.get_bind()

    # is_array (bool, default false)
    if not _column_exists(conn, "schema_evolution_edits", "is_array"):
        op.add_column(
            "schema_evolution_edits",
            sa.Column("is_array", sa.Boolean, nullable=False, server_default=sa.false()),
        )

    # struct_definition_id (FK -> struct_definition.id, nullable, RESTRICT)
    if not _column_exists(conn, "schema_evolution_edits", "struct_definition_id"):
        op.add_column(
            "schema_evolution_edits",
            sa.Column("struct_definition_id", sa.Integer, nullable=True),
        )
    if not _constraint_exists(
        conn, "schema_evolution_edits", "fk_schema_evolution_edits_struct_definition_id"
    ):
        op.create_foreign_key(
            "fk_schema_evolution_edits_struct_definition_id",
            "schema_evolution_edits",
            "struct_definition",
            ["struct_definition_id"],
            ["id"],
            ondelete="RESTRICT",
        )


def downgrade() -> None:
    conn = op.get_bind()

    if _constraint_exists(
        conn, "schema_evolution_edits", "fk_schema_evolution_edits_struct_definition_id"
    ):
        op.drop_constraint(
            "fk_schema_evolution_edits_struct_definition_id",
            "schema_evolution_edits",
            type_="foreignkey",
        )
    if _column_exists(conn, "schema_evolution_edits", "struct_definition_id"):
        op.drop_column("schema_evolution_edits", "struct_definition_id")
    if _column_exists(conn, "schema_evolution_edits", "is_array"):
        op.drop_column("schema_evolution_edits", "is_array")
