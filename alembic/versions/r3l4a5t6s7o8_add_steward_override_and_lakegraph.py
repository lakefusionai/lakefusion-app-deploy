"""add relationship_steward_override + lakegraph_deployment tables

Revision ID: r3l4a5t6s7o8
Revises: r2l3a4t5b6c7
Create Date: 2026-06-02

Story 3 — Steward override audit/overlay table + LakeGraph deployment-state
table. Generic SQLAlchemy types only. MySQL+PostgreSQL+Lakebase compatible.
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "r3l4a5t6s7o8"
down_revision: Union[str, None] = "r2l3a4t5b6c7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ---------- relationship_steward_override ----------
    op.create_table(
        "relationship_steward_override",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column(
            "relationship_id",
            sa.Integer(),
            sa.ForeignKey("relationship.id"),
            nullable=False,
        ),
        sa.Column("src_node_id", sa.String(255), nullable=False),
        sa.Column("dst_node_id", sa.String(255), nullable=False),
        sa.Column("attribute_name", sa.String(64), nullable=False),
        sa.Column("override_action", sa.String(20), nullable=False),
        sa.Column("override_value", sa.Text(), nullable=True),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("steward_id", sa.String(255), nullable=False),
        sa.Column(
            "created_at",
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
        sa.UniqueConstraint(
            "relationship_id",
            "src_node_id",
            "dst_node_id",
            "attribute_name",
            name="uix_relovr_full",
        ),
    )
    op.create_index(
        "ix_relovr_rel_active",
        "relationship_steward_override",
        ["relationship_id", "is_active"],
    )
    op.create_index(
        "ix_relovr_src",
        "relationship_steward_override",
        ["relationship_id", "src_node_id"],
    )

    # ---------- lakegraph_deployment ----------
    op.create_table(
        "lakegraph_deployment",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("source_system_id", sa.String(255), nullable=False),
        sa.Column("graph_config_id", sa.BigInteger(), nullable=True),
        sa.Column("graph_name", sa.String(255), nullable=True),
        sa.Column("deploy_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("last_status", sa.String(20), nullable=True),
        sa.Column("deployment_hash", sa.String(64), nullable=True),
        sa.Column("last_deployed_at", sa.DateTime(), nullable=True),
        sa.Column("redirect_url", sa.Text(), nullable=True),
        sa.Column("created_by", sa.String(255), nullable=True),
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
        sa.UniqueConstraint("source_system_id", name="uix_lgdep_ssid"),
    )


def downgrade() -> None:
    op.drop_table("lakegraph_deployment")
    op.drop_index("ix_relovr_src", table_name="relationship_steward_override")
    op.drop_index("ix_relovr_rel_active", table_name="relationship_steward_override")
    op.drop_table("relationship_steward_override")
