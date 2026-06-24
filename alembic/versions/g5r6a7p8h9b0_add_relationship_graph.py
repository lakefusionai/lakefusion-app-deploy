"""add relationship_graph + member tables and lakegraph_deployment.graph_id

Revision ID: g5r6a7p8h9b0
Revises: r4l5a6t7m8c9
Create Date: 2026-06-08

A LakeGraph graph is a named bundle of relationships; nodes are auto-derived from
the endpoints of the selected relationships. Each graph deploys to its own
LakeGraph graph_config (one lakegraph_deployment row per graph).

Database-agnostic: generic sa.* types only. Booleans via sa.text('true'/'false').
Idempotent ops handled by the project's env.py wrapper.
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "g5r6a7p8h9b0"
down_revision: Union[str, None] = "r4l5a6t7m8c9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "relationship_graph",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("slug", sa.String(255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("created_by", sa.String(255), nullable=True),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.current_timestamp(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.current_timestamp(), nullable=False),
        sa.UniqueConstraint("slug", name="uix_relgraph_slug"),
    )

    op.create_table(
        "relationship_graph_member",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column(
            "graph_id",
            sa.Integer(),
            sa.ForeignKey("relationship_graph.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "relationship_id",
            sa.Integer(),
            sa.ForeignKey("relationship.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.UniqueConstraint("graph_id", "relationship_id", name="uix_relgraphmem_g_r"),
    )
    op.create_index(
        "ix_relgraphmem_graph", "relationship_graph_member", ["graph_id"]
    )

    # Plain nullable column (no DB-level FK for cross-dialect portability; the ORM
    # carries the relationship). One deployment row per graph going forward.
    op.add_column(
        "lakegraph_deployment",
        sa.Column("graph_id", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("lakegraph_deployment", "graph_id")
    op.drop_index("ix_relgraphmem_graph", table_name="relationship_graph_member")
    op.drop_table("relationship_graph_member")
    op.drop_table("relationship_graph")
