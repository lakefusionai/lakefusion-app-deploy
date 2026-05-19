"""alter base_prompt content to mediumtext with utf8mb4

Revision ID: l7m8n9o0p1q2
Revises: d4e5f6a7b8c9
Create Date: 2026-02-26

Alter base_prompts.content from TEXT (~64KB) to MEDIUMTEXT (~16MB)
and set charset to utf8mb4 to support special characters (arrows, emojis, etc.).
Only applies if the column is not already MEDIUMTEXT with utf8mb4.
On PostgreSQL, TEXT is already unlimited so this migration is a no-op.
"""
from typing import Sequence, Union

from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision: str = 'l7m8n9o0p1q2'
down_revision: Union[str, None] = 'd4e5f6a7b8c9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _needs_upgrade() -> bool:
    """Check if content column needs to be altered (MySQL only)."""
    conn = op.get_bind()

    # PostgreSQL TEXT is already unlimited — no upgrade needed
    if conn.dialect.name != 'mysql':
        return False

    result = conn.execute(text(
        "SELECT COLUMN_TYPE, CHARACTER_SET_NAME "
        "FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() "
        "AND TABLE_NAME = 'base_prompts' "
        "AND COLUMN_NAME = 'content'"
    )).fetchone()

    if result is None:
        return False

    col_type, charset = result
    # Skip if already mediumtext with utf8mb4
    if col_type.lower() == 'mediumtext' and charset == 'utf8mb4':
        return False

    return True


def upgrade() -> None:
    if not _needs_upgrade():
        return

    op.alter_column(
        'base_prompts',
        'content',
        existing_type=mysql.TEXT(),
        type_=mysql.MEDIUMTEXT(charset='utf8mb4', collation='utf8mb4_unicode_ci'),
        existing_nullable=False,
    )


def downgrade() -> None:
    dialect = op.get_bind().dialect.name
    if dialect != 'mysql':
        return

    op.alter_column(
        'base_prompts',
        'content',
        existing_type=mysql.MEDIUMTEXT(),
        type_=mysql.TEXT(),
        existing_nullable=False,
    )
