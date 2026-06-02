"""add complex data types schema: struct_definition, struct_field, entityattributes.is_array + struct_definition_id, ENABLE_COMPLEX_DATA_TYPES flag

Revision ID: a2b3c4d5e6f7
Revises: p1q2r3s4t5u6
Create Date: 2026-05-19 00:00:00.000000

Foundation schema for ARRAY / STRUCT / ARRAY<STRUCT> attribute support
(Epic , story ).

- struct_definition: reusable struct schema (name, description, is_active).
- struct_field: scalar fields per struct_definition, ordered by ordinal_position.
- entityattributes.is_array (bool, default false): orthogonal flag, combines
  with `type` to express ARRAY<scalar> or ARRAY<STRUCT>.
- entityattributes.struct_definition_id (FK, nullable, ON DELETE RESTRICT):
  references struct_definition when type='STRUCT'.
- feature_flags row ENABLE_COMPLEX_DATA_TYPES (INACTIVE by default).

All DDL guarded with existence checks (idempotent on retry / partial apply).
"""
from typing import Sequence, Union
from datetime import datetime

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "a2b3c4d5e6f7"
down_revision: Union[str, None] = "p1q2r3s4t5u6"
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


def _table_exists(conn, table_name: str) -> bool:
    dialect = conn.dialect.name
    if dialect == "mysql":
        result = conn.execute(
            sa.text(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = :table"
            ),
            {"table": table_name},
        ).fetchone()
    else:
        result = conn.execute(
            sa.text(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_catalog = current_database() AND table_name = :table"
            ),
            {"table": table_name},
        ).fetchone()
    return result is not None


def _constraint_exists(conn, table_name: str, constraint_name: str) -> bool:
    dialect = conn.dialect.name
    if dialect == "mysql":
        result = conn.execute(
            sa.text(
                "SELECT 1 FROM information_schema.table_constraints "
                "WHERE table_schema = DATABASE() AND table_name = :table AND constraint_name = :name"
            ),
            {"table": table_name, "name": constraint_name},
        ).fetchone()
    else:
        result = conn.execute(
            sa.text(
                "SELECT 1 FROM information_schema.table_constraints "
                "WHERE table_catalog = current_database() AND table_name = :table AND constraint_name = :name"
            ),
            {"table": table_name, "name": constraint_name},
        ).fetchone()
    return result is not None


def upgrade() -> None:
    conn = op.get_bind()

    # 1. struct_definition table
    if not _table_exists(conn, "struct_definition"):
        op.create_table(
            "struct_definition",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("name", sa.String(255), nullable=False),
            sa.Column("description", sa.String(1024), nullable=True),
            sa.Column("is_active", sa.Boolean, nullable=False, server_default=sa.true()),
            sa.Column("created_by", sa.String(255), nullable=True),
            sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
            sa.Column("updated_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
            sa.UniqueConstraint("name", name="uq_struct_definition_name"),
        )
    else:
        # Heal partial pre-existing table — add any missing cols.
        if not _column_exists(conn, "struct_definition", "description"):
            op.add_column("struct_definition", sa.Column("description", sa.String(1024), nullable=True))
        if not _column_exists(conn, "struct_definition", "is_active"):
            op.add_column("struct_definition", sa.Column("is_active", sa.Boolean, nullable=False, server_default=sa.true()))
        if not _column_exists(conn, "struct_definition", "created_by"):
            op.add_column("struct_definition", sa.Column("created_by", sa.String(255), nullable=True))
        if not _column_exists(conn, "struct_definition", "created_at"):
            op.add_column("struct_definition", sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()))
        if not _column_exists(conn, "struct_definition", "updated_at"):
            op.add_column("struct_definition", sa.Column("updated_at", sa.DateTime, nullable=False, server_default=sa.func.now()))

    # 2. struct_field table
    if not _table_exists(conn, "struct_field"):
        op.create_table(
            "struct_field",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column(
                "struct_definition_id",
                sa.Integer,
                sa.ForeignKey("struct_definition.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("name", sa.String(255), nullable=False),
            sa.Column("label", sa.String(255), nullable=True),
            sa.Column("data_type", sa.String(64), nullable=False),
            sa.Column("ordinal_position", sa.Integer, nullable=False, server_default="0"),
            sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
            sa.Column("updated_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
            sa.UniqueConstraint(
                "struct_definition_id", "name", name="uq_struct_field_def_name"
            ),
        )
        op.create_index(
            "ix_struct_field_struct_definition_id",
            "struct_field",
            ["struct_definition_id"],
        )
    else:
        if not _column_exists(conn, "struct_field", "label"):
            op.add_column("struct_field", sa.Column("label", sa.String(255), nullable=True))
        if not _column_exists(conn, "struct_field", "ordinal_position"):
            op.add_column("struct_field", sa.Column("ordinal_position", sa.Integer, nullable=False, server_default="0"))
        if not _column_exists(conn, "struct_field", "created_at"):
            op.add_column("struct_field", sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()))
        if not _column_exists(conn, "struct_field", "updated_at"):
            op.add_column("struct_field", sa.Column("updated_at", sa.DateTime, nullable=False, server_default=sa.func.now()))

    # 3. entityattributes.is_array
    if not _column_exists(conn, "entityattributes", "is_array"):
        op.add_column(
            "entityattributes",
            sa.Column(
                "is_array",
                sa.Boolean,
                nullable=False,
                server_default=sa.false(),
            ),
        )

    # 4. entityattributes.struct_definition_id (FK, nullable, RESTRICT on delete)
    if not _column_exists(conn, "entityattributes", "struct_definition_id"):
        op.add_column(
            "entityattributes",
            sa.Column("struct_definition_id", sa.Integer, nullable=True),
        )
    if not _constraint_exists(
        conn, "entityattributes", "fk_entityattributes_struct_definition_id"
    ):
        op.create_foreign_key(
            "fk_entityattributes_struct_definition_id",
            "entityattributes",
            "struct_definition",
            ["struct_definition_id"],
            ["id"],
            ondelete="RESTRICT",
        )
    op.create_index(
        "ix_entityattributes_struct_definition_id",
        "entityattributes",
        ["struct_definition_id"],
    )

    # 5. ENABLE_COMPLEX_DATA_TYPES feature flag (INACTIVE)
    existing = conn.execute(
        sa.text("SELECT 1 FROM feature_flags WHERE name = :name"),
        {"name": "ENABLE_COMPLEX_DATA_TYPES"},
    ).fetchone()
    if not existing:
        conn.execute(
            sa.text(
                "INSERT INTO feature_flags "
                "(name, status, description, owner_team, created_at, updated_at, expires_at) "
                "VALUES (:name, :status, :description, 'LakeFusion', :now, :now, NULL)"
            ),
            {
                "name": "ENABLE_COMPLEX_DATA_TYPES",
                "status": "INACTIVE",
                "description": "Enables ARRAY / STRUCT / ARRAY<STRUCT> attribute types and the related Struct Definition management UI.",
                "now": datetime.utcnow(),
            },
        )


def downgrade() -> None:
    conn = op.get_bind()

    # Reverse order of upgrade.

    # 5. Feature flag
    conn.execute(
        sa.text("DELETE FROM feature_flags WHERE name = :name"),
        {"name": "ENABLE_COMPLEX_DATA_TYPES"},
    )

    # 4. entityattributes.struct_definition_id (drop FK + index + col)
    op.drop_index(
        "ix_entityattributes_struct_definition_id",
        table_name="entityattributes",
    )
    if _constraint_exists(
        conn, "entityattributes", "fk_entityattributes_struct_definition_id"
    ):
        op.drop_constraint(
            "fk_entityattributes_struct_definition_id",
            "entityattributes",
            type_="foreignkey",
        )
    if _column_exists(conn, "entityattributes", "struct_definition_id"):
        op.drop_column("entityattributes", "struct_definition_id")

    # 3. entityattributes.is_array
    if _column_exists(conn, "entityattributes", "is_array"):
        op.drop_column("entityattributes", "is_array")

    # 2. struct_field
    if _table_exists(conn, "struct_field"):
        op.drop_index(
            "ix_struct_field_struct_definition_id",
            table_name="struct_field",
        )
        op.drop_table("struct_field")

    # 1. struct_definition
    if _table_exists(conn, "struct_definition"):
        op.drop_table("struct_definition")
