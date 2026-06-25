"""scope struct_definition to an owning entity

Revision ID: s7t8r9u0c1t2
Revises: lg1u2r3l4c5g
Create Date: 2026-06-17 00:00:00.000000

Make struct definitions exclusive to a single entity so editing one entity's
struct can never affect another's.

- struct_definition.entity_id (FK -> entity.id, nullable, ON DELETE CASCADE,
  indexed).
- Backfill entity_id from the referencing attributes when a struct is
  referenced by exactly one entity (unambiguous). Structs with no references,
  or referenced by more than one entity, are left NULL.
- Replace the global unique(name) with a per-entity unique(entity_id, name).

All DDL guarded with existence checks (idempotent on retry / partial apply).
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "s7t8r9u0c1t2"
down_revision: Union[str, None] = "lg1u2r3l4c5g"
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

    # 1. entity_id column (nullable, indexed)
    if not _column_exists(conn, "struct_definition", "entity_id"):
        op.add_column(
            "struct_definition",
            sa.Column("entity_id", sa.Integer, nullable=True),
        )
        op.create_index(
            "ix_struct_definition_entity_id",
            "struct_definition",
            ["entity_id"],
        )

    # 2. FK -> entity.id (cascade so an entity's structs drop with it)
    if not _constraint_exists(
        conn, "struct_definition", "fk_struct_definition_entity_id"
    ):
        op.create_foreign_key(
            "fk_struct_definition_entity_id",
            "struct_definition",
            "entity",
            ["entity_id"],
            ["id"],
            ondelete="CASCADE",
        )

    # 3. Backfill entity_id from referencing attributes when unambiguous
    #    (exactly one distinct entity references the struct).
    conn.execute(
        sa.text(
            """
            UPDATE struct_definition AS sd
            SET entity_id = (
                SELECT MIN(ea.entity_id)
                FROM entityattributes AS ea
                WHERE ea.struct_definition_id = sd.id
            )
            WHERE sd.entity_id IS NULL
              AND (
                SELECT COUNT(DISTINCT ea.entity_id)
                FROM entityattributes AS ea
                WHERE ea.struct_definition_id = sd.id
              ) = 1
            """
        )
    )

    # 4. Swap global unique(name) for per-entity unique(entity_id, name).
    if _constraint_exists(conn, "struct_definition", "uq_struct_definition_name"):
        op.drop_constraint(
            "uq_struct_definition_name", "struct_definition", type_="unique"
        )
    if not _constraint_exists(
        conn, "struct_definition", "uq_struct_definition_entity_name"
    ):
        op.create_unique_constraint(
            "uq_struct_definition_entity_name",
            "struct_definition",
            ["entity_id", "name"],
        )


def downgrade() -> None:
    conn = op.get_bind()

    # Reverse order of upgrade.
    if _constraint_exists(
        conn, "struct_definition", "uq_struct_definition_entity_name"
    ):
        op.drop_constraint(
            "uq_struct_definition_entity_name", "struct_definition", type_="unique"
        )
    if not _constraint_exists(conn, "struct_definition", "uq_struct_definition_name"):
        op.create_unique_constraint(
            "uq_struct_definition_name", "struct_definition", ["name"]
        )

    if _constraint_exists(conn, "struct_definition", "fk_struct_definition_entity_id"):
        op.drop_constraint(
            "fk_struct_definition_entity_id", "struct_definition", type_="foreignkey"
        )

    if _column_exists(conn, "struct_definition", "entity_id"):
        op.drop_index(
            "ix_struct_definition_entity_id", table_name="struct_definition"
        )
        op.drop_column("struct_definition", "entity_id")
