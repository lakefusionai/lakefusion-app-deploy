"""enable_pim_feature_flag

Revision ID: 8e72450b9d02
Revises: d33c14388fed
Create Date: 2026-04-17 16:03:21.154592

Adds ENABLE_PIM feature flag to gate PIM (Product Information Management) endpoints.
- ACTIVE (default): PIM module is accessible
- INACTIVE: All PIM endpoints return 404
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8e72450b9d02'
down_revision: Union[str, None] = 'd33c14388fed'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Idempotent insert: skip if the flag already exists (e.g. seeded manually
    # or by a prior partial run) to avoid a duplicate-key violation.
    # Dialect-agnostic: Python-level existence check + standard SQL.
    conn = op.get_bind()
    exists = conn.execute(sa.text(
        "SELECT 1 FROM feature_flags WHERE name = 'ENABLE_PIM'"
    )).scalar()
    if not exists:
        conn.execute(sa.text(
            "INSERT INTO feature_flags "
            "(name, status, description, owner_team, created_at, updated_at, expires_at) "
            "VALUES ('ENABLE_PIM', 'ACTIVE', :desc, 'LakeFusion', "
            "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL)"
        ), {"desc": "Enables PIM (Product Information Management) module — taxonomy, attributes, products, and pricing endpoints"})


def downgrade() -> None:
    op.execute("DELETE FROM feature_flags WHERE name='ENABLE_PIM';")
