"""align_feature_flags_for_fresh_install

Revision ID: 15e4f262c71e
Revises: 1ace0c7b0781
Create Date: 2026-01-20 16:04:39.148024

This migration aligns feature flags for fresh installations:
- ENABLE_LAKEFUSION_TAG_USAGE: INACTIVE -> ACTIVE
- ENABLE_PLAYGROUND_FEATURE: INACTIVE -> ACTIVE
- ENABLE_USER_MANAGEMENT: Insert as INACTIVE (was missing)

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import table, column
from sqlalchemy import String, Enum, Text, DateTime
from datetime import datetime


# revision identifiers, used by Alembic.
revision: str = '15e4f262c71e'
down_revision: Union[str, None] = '1ace0c7b0781'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    dialect = op.get_bind().dialect.name

    # Update ENABLE_LAKEFUSION_TAG_USAGE to ACTIVE
    op.execute("""
        UPDATE feature_flags
        SET status = 'ACTIVE', updated_at = CURRENT_TIMESTAMP
        WHERE name = 'ENABLE_LAKEFUSION_TAG_USAGE';
    """)

    # Update ENABLE_PLAYGROUND_FEATURE to ACTIVE
    op.execute("""
        UPDATE feature_flags
        SET status = 'ACTIVE', updated_at = CURRENT_TIMESTAMP
        WHERE name = 'ENABLE_PLAYGROUND_FEATURE';
    """)

    # Insert ENABLE_USER_MANAGEMENT as INACTIVE (if not exists)
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

    # Use dialect-specific INSERT to avoid duplicate key errors if flag already exists
    if dialect == 'mysql':
        op.execute("""
            INSERT IGNORE INTO feature_flags (name, status, description, owner_team, created_at, updated_at, expires_at)
            VALUES (
                'ENABLE_USER_MANAGEMENT',
                'INACTIVE',
                'Enables user management feature in the application',
                'LakeFusion',
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP,
                NULL
            );
        """)
    else:
        op.execute("""
            INSERT INTO feature_flags (name, status, description, owner_team, created_at, updated_at, expires_at)
            VALUES (
                'ENABLE_USER_MANAGEMENT',
                'INACTIVE',
                'Enables user management feature in the application',
                'LakeFusion',
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP,
                NULL
            )
            ON CONFLICT DO NOTHING;
        """)


def downgrade() -> None:
    # Revert ENABLE_LAKEFUSION_TAG_USAGE to INACTIVE
    op.execute("""
        UPDATE feature_flags
        SET status = 'INACTIVE', updated_at = CURRENT_TIMESTAMP
        WHERE name = 'ENABLE_LAKEFUSION_TAG_USAGE';
    """)

    # Revert ENABLE_PLAYGROUND_FEATURE to INACTIVE
    op.execute("""
        UPDATE feature_flags
        SET status = 'INACTIVE', updated_at = CURRENT_TIMESTAMP
        WHERE name = 'ENABLE_PLAYGROUND_FEATURE';
    """)

    # Remove ENABLE_USER_MANAGEMENT
    op.execute("""
        DELETE FROM feature_flags
        WHERE name = 'ENABLE_USER_MANAGEMENT';
    """)
