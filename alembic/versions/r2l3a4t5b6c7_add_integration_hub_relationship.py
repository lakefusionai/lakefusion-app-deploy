"""add integration_hub_relationship table

Revision ID: r2l3a4t5b6c7
Revises: r1l2a3t4i5o6
Create Date: 2026-06-02

Story 2 — IntHub task type for Relationship sync pipelines. Adds the
``integration_hub_relationship`` table that drives the Materialize Edges
Databricks job per task.

Database-agnostic: generic sa.* types only. Booleans via sa.text('true'/
'false'). JSON via sa.JSON().
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "r2l3a4t5b6c7"
down_revision: Union[str, None] = "r1l2a3t4i5o6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "integration_hub_relationship",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("task_name", sa.String(255), nullable=False),
        sa.Column("entity_id", sa.Integer(), sa.ForeignKey("entity.id"), nullable=False),
        # [{ "relationship_id": int, "mapping_ids": [int, ...] }]
        sa.Column("relationship_ids", sa.JSON(), nullable=False),
        sa.Column("merge_config", sa.JSON(), nullable=False),
        sa.Column("cron_expression", sa.String(45), server_default="0 0 2 * * ?", nullable=False),
        sa.Column("timezone_id", sa.String(255), server_default="UTC", nullable=True),
        sa.Column("to_emails", sa.JSON(), nullable=False),
        sa.Column("job_id", sa.String(255), nullable=True),
        sa.Column("version", sa.String(255), nullable=True),
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
        sa.UniqueConstraint("task_name", name="uix_inthubrel_task_name"),
    )
    op.create_index(
        "ix_inthubrel_entity_active",
        "integration_hub_relationship",
        ["entity_id", "is_active"],
    )


def downgrade() -> None:
    op.drop_index("ix_inthubrel_entity_active", table_name="integration_hub_relationship")
    op.drop_table("integration_hub_relationship")
