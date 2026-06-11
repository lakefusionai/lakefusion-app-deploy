"""add_ENABLE_BATCH_IMPORT_FOR_BUSINESS_GLOSSARY

Revision ID: 76cc3702342f
Revises: 13df984e18e0
Create Date: 2026-02-10 01:12:53.966238

"""
from typing import Sequence, Union

from alembic import op
from sqlalchemy.sql import table, column
from sqlalchemy import String, Enum, Text, DateTime
from datetime import datetime


# revision identifiers, used by Alembic.
revision: str = '76cc3702342f'
down_revision: Union[str, None] = '13df984e18e0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ✅ Insert Base Prompt feature flag
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
                "name": "ENABLE_BATCH_IMPORT_FOR_BUSINESS_GLOSSARY",
                "status": "INACTIVE",
                "description": "Enable batch import for business glossary to import multiple glossary terms at once.",
                "owner_team": "LakeFusion",
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "expires_at": None,
            }
        ]
    )


def downgrade() -> None:
    # Remove feature flag
    op.execute(
        "DELETE FROM feature_flags WHERE name='ENABLE_BATCH_IMPORT_FOR_BUSINESS_GLOSSARY';"
    )
