"""add relationship tables and ENABLE_RELATIONSHIPS feature flag

Revision ID: r1l2a3t4i5o6
Revises: e1f2a3b4c5d6
Create Date: 2026-06-02

Story 1 — configuration foundation. Creates three Postgres/MySQL tables that
back the Designer surface for first-class Relationships and seeds the feature
flag (INACTIVE by default so the UI stays hidden until ops flips it on).

Database-agnostic:
- Generic sa.* column types only.
- Boolean literals via sa.text('true' / 'false') in server defaults.
- Idempotent ops handled by the project's env.py wrapper.
"""

from typing import Sequence, Union
from datetime import datetime

from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import column, table
from sqlalchemy import String, Enum as SAEnum, Text, DateTime


# revision identifiers, used by Alembic.
revision: str = "r1l2a3t4i5o6"
down_revision: Union[str, None] = "e1f2a3b4c5d6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ---------- relationship ----------
    op.create_table(
        "relationship",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("name", sa.String(64), nullable=False),
        sa.Column("label_forward", sa.String(100), nullable=False),
        sa.Column("label_reverse", sa.String(100), nullable=True),
        sa.Column("source_entity_id", sa.Integer(), sa.ForeignKey("entity.id"), nullable=False),
        sa.Column("target_entity_id", sa.Integer(), sa.ForeignKey("entity.id"), nullable=False),
        sa.Column("cardinality", sa.String(8), nullable=False),
        sa.Column(
            "is_bidirectional",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("status", sa.String(20), server_default="draft", nullable=False),
        sa.Column("developer_group_id", sa.String(255), nullable=True),
        sa.Column("steward_group_id", sa.String(255), nullable=True),
        sa.Column("created_by", sa.String(255), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.func.current_timestamp(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.func.current_timestamp(),
            nullable=False,
        ),
        sa.Column(
            "is_active",
            sa.Boolean(),
            server_default=sa.text("true"),
            nullable=False,
        ),
        sa.UniqueConstraint("name", name="uix_relationship_name"),
    )
    op.create_index("ix_rel_src_tgt", "relationship", ["source_entity_id", "target_entity_id"])
    op.create_index("ix_rel_status_active", "relationship", ["status", "is_active"])

    # ---------- relationship_attribute ----------
    op.create_table(
        "relationship_attribute",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column(
            "relationship_id",
            sa.Integer(),
            sa.ForeignKey("relationship.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("name", sa.String(64), nullable=False),
        sa.Column("label", sa.String(100), nullable=False),
        sa.Column("data_type", sa.String(20), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("ordinal", sa.SmallInteger(), server_default="0", nullable=False),
        sa.UniqueConstraint("relationship_id", "name", name="uix_relattr_rel_name"),
    )

    # ---------- relationship_dataset_mapping ----------
    op.create_table(
        "relationship_dataset_mapping",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column(
            "relationship_id",
            sa.Integer(),
            sa.ForeignKey("relationship.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("dataset_id", sa.Integer(), sa.ForeignKey("datasets.id"), nullable=False),
        sa.Column("source_pk_column", sa.String(255), nullable=False),
        sa.Column("target_pk_column", sa.String(255), nullable=False),
        sa.Column("start_date_column", sa.String(255), nullable=True),
        sa.Column("end_date_column", sa.String(255), nullable=True),
        sa.Column("status_column", sa.String(255), nullable=True),
        sa.Column("source_system_tag", sa.String(50), nullable=False),
        sa.Column("priority", sa.SmallInteger(), server_default="100", nullable=False),
        sa.Column("attribute_mappings", sa.JSON(), nullable=False),
        sa.Column("created_by", sa.String(255), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.func.current_timestamp(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.func.current_timestamp(),
            nullable=False,
        ),
        sa.Column(
            "is_active",
            sa.Boolean(),
            server_default=sa.text("true"),
            nullable=False,
        ),
    )
    op.create_index(
        "ix_relmap_rel_active",
        "relationship_dataset_mapping",
        ["relationship_id", "is_active"],
    )

    # ---------- feature flag (INACTIVE so UI hidden by default) ----------
    feature_flags = table(
        "feature_flags",
        column("name", String),
        column("status", SAEnum),
        column("description", Text),
        column("owner_team", String),
        column("created_at", DateTime),
        column("updated_at", DateTime),
        column("expires_at", DateTime),
    )
    op.bulk_insert(
        feature_flags,
        [
            {
                "name": "ENABLE_RELATIONSHIPS",
                "status": "INACTIVE",
                "description": (
                    "Enables first-class Relationships in LakeFusion MDM (Designer "
                    "surface, sidebar item, embedded tab on Entity detail). When "
                    "INACTIVE, no Relationships UI is visible."
                ),
                "owner_team": "LakeFusion",
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "expires_at": None,
            }
        ],
    )


def downgrade() -> None:
    op.execute("DELETE FROM feature_flags WHERE name = 'ENABLE_RELATIONSHIPS'")
    op.drop_index("ix_relmap_rel_active", table_name="relationship_dataset_mapping")
    op.drop_table("relationship_dataset_mapping")
    op.drop_table("relationship_attribute")
    op.drop_index("ix_rel_status_active", table_name="relationship")
    op.drop_index("ix_rel_src_tgt", table_name="relationship")
    op.drop_table("relationship")
