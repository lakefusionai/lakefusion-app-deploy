"""enable_advanced_model_sources_feature_flag

Revision ID: m8n9o0p1q2r3
Revises: d199631e4fc0
Create Date: 2026-03-03 10:00:00.000000

Adds ENABLE_ADVANCED_MODEL_SOURCES feature flag to toggle visibility of:
- INACTIVE (default): LLM shows only Foundation tab; Embedding shows Foundation + Custom (HuggingFace only)
- ACTIVE: All model source tabs visible (Custom, External, UC Model)
"""
from typing import Sequence, Union

from alembic import op
from sqlalchemy.sql import table, column
from sqlalchemy import String, Enum, Text, DateTime
from datetime import datetime, timezone


# revision identifiers, used by Alembic.
revision: str = 'm8n9o0p1q2r3'
down_revision: Union[str, None] = 'd199631e4fc0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    feature_flags = table(
        'feature_flags',
        column('name', String),
        column('status', Enum),
        column('description', Text),
        column('owner_team', String),
        column('created_at', DateTime),
        column('updated_at', DateTime),
        column('expires_at', DateTime),
    )

    op.bulk_insert(
        feature_flags,
        [
            {
                "name": "ENABLE_ADVANCED_MODEL_SOURCES",
                "status": "INACTIVE",
                "description": "Enable advanced model source options: LLM Custom, External Model API, and Embedding UC Model tabs in the model configuration UI",
                "owner_team": "LakeFusion",
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
                "expires_at": None,
            }
        ]
    )


def downgrade() -> None:
    op.execute("DELETE FROM feature_flags WHERE name='ENABLE_ADVANCED_MODEL_SOURCES';")
