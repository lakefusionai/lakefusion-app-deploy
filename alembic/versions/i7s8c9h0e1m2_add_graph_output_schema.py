"""add output_schema to relationship_graph

Revision ID: i7s8c9h0e1m2
Revises: h6r7u8n9u0r1
Create Date: 2026-06-08

The user selects the target UC catalog.schema for the deployed graph tables in
the New Graph dialog (LakeGraph requires output_schema to be an existing
catalog.schema; it does not create catalogs). Persist it per graph.

Database-agnostic: generic sa.* types only. Idempotent ops handled by env.py.
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "i7s8c9h0e1m2"
down_revision: Union[str, None] = "h6r7u8n9u0r1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "relationship_graph", sa.Column("output_schema", sa.String(255), nullable=True)
    )


def downgrade() -> None:
    op.drop_column("relationship_graph", "output_schema")
