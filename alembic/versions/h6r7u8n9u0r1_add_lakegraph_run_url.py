"""add run_id + run_url to lakegraph_deployment

Revision ID: h6r7u8n9u0r1
Revises: g5r6a7p8h9b0
Create Date: 2026-06-08

LakeGraph's deploy/redeploy response carries deployment.run_id + run_url (the
Databricks job-run link). Persist them so the Graphs UI can offer a "view job
run" link alongside the LakeGraph redirect.

Database-agnostic: generic sa.* types only. Idempotent ops handled by env.py.
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "h6r7u8n9u0r1"
down_revision: Union[str, None] = "g5r6a7p8h9b0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("lakegraph_deployment", sa.Column("run_id", sa.String(255), nullable=True))
    op.add_column("lakegraph_deployment", sa.Column("run_url", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("lakegraph_deployment", "run_url")
    op.drop_column("lakegraph_deployment", "run_id")
