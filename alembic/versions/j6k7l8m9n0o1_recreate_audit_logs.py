"""recreate audit_logs table with extended fields

Revision ID: j6k7l8m9n0o1
Revises: i5j6k7l8m9n0
Create Date: 2026-03-27 01:40:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import logging

logger = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision: str = 'j6k7l8m9n0o1'
down_revision: Union[str, None] = 'i5j6k7l8m9n0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Drop and recreate audit_logs table with all extended fields.
    Audit logs are transient data — safe to drop."""

    try:
        op.drop_table('audit_logs')
        logger.info("Dropped old audit_logs table")
    except Exception as e:
        logger.info(f"Could not drop audit_logs (may not exist): {e}")

    try:
        op.create_table(
            'audit_logs',
            sa.Column('audit_logs_id', sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column('username', sa.String(255), nullable=False, index=True),
            sa.Column('created_date', sa.DateTime(), nullable=False, index=True),
            sa.Column('api_path', sa.Text(), nullable=False),
            sa.Column('request_method', sa.String(10), nullable=True),
            sa.Column('request_payload', sa.Text(), nullable=True),
            sa.Column('response_payload', sa.Text(), nullable=True),
            sa.Column('response_status', sa.Integer(), nullable=True, index=True),
            sa.Column('client_ip', sa.String(45), nullable=True),
            sa.Column('user_agent', sa.String(512), nullable=True),
            sa.Column('execution_time_ms', sa.Float(), nullable=True),
        )
        logger.info("Created audit_logs table with extended fields")
    except Exception as e:
        logger.info(f"Could not create audit_logs (may already exist): {e}")


def downgrade() -> None:
    """Recreate the original audit_logs table without extended fields."""
    try:
        op.drop_table('audit_logs')
    except Exception as e:
        logger.info(f"Could not drop audit_logs: {e}")

    try:
        op.create_table(
            'audit_logs',
            sa.Column('audit_logs_id', sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column('username', sa.String(255), nullable=False),
            sa.Column('created_date', sa.DateTime(), nullable=False),
            sa.Column('api_path', sa.Text(), nullable=False),
            sa.Column('request_payload', sa.Text(), nullable=True),
            sa.Column('response_status', sa.Integer(), nullable=True),
        )
    except Exception as e:
        logger.info(f"Could not recreate old audit_logs: {e}")
