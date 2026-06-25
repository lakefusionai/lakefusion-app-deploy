"""drop merge_config from integration_hub_relationship

Revision ID: r4l5a6t7m8c9
Revises: r3l4a5t6s7o8
Create Date: 2026-06-08

Merge Behaviour was removed from the Relationship Sync task (it was added to the
UI by mistake). The relationship materialize-edges pipeline now uses a fixed
default merge behaviour (source_wins / auto_insert) baked into Materialize_Edges,
so the per-task ``merge_config`` column is no longer needed.

Reference Entity sync keeps its own merge_config — that table is untouched.

Database-agnostic: generic sa.* types only. Idempotent ops handled by the
project's env.py wrapper.
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "r4l5a6t7m8c9"
down_revision: Union[str, None] = "r3l4a5t6s7o8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_column("integration_hub_relationship", "merge_config")


def downgrade() -> None:
    # Re-add as nullable; a cross-dialect NOT NULL JSON default is not portable
    # and the column is no longer written by the application.
    op.add_column(
        "integration_hub_relationship",
        sa.Column("merge_config", sa.JSON(), nullable=True),
    )
