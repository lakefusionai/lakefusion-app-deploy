"""add_audit_logs_extended_fields

Revision ID: 8f737e01bcd2
Revises: o0p1q2r3s4t5
Create Date: 2026-01-20 16:25:09.133457

Adds new columns to audit_logs table for comprehensive request tracking:
- request_method: HTTP method (GET, POST, etc.)
- response_payload: Masked response body for errors
- client_ip: Client IP address
- user_agent: Client user agent string
- execution_time_ms: Request duration in milliseconds

Also adds indexes for better query performance.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8f737e01bcd2'
down_revision: Union[str, None] = 'o0p1q2r3s4t5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add new columns to audit_logs table
    op.add_column('audit_logs', sa.Column('request_method', sa.String(10), nullable=True))
    op.add_column('audit_logs', sa.Column('response_payload', sa.Text(), nullable=True))
    op.add_column('audit_logs', sa.Column('client_ip', sa.String(45), nullable=True))
    op.add_column('audit_logs', sa.Column('user_agent', sa.String(512), nullable=True))
    op.add_column('audit_logs', sa.Column('execution_time_ms', sa.Float(), nullable=True))

    # Add indexes for better query performance
    op.create_index('ix_audit_logs_username', 'audit_logs', ['username'])
    op.create_index('ix_audit_logs_created_date', 'audit_logs', ['created_date'])
    op.create_index('ix_audit_logs_response_status', 'audit_logs', ['response_status'])


def downgrade() -> None:
    # Remove indexes
    op.drop_index('ix_audit_logs_response_status', table_name='audit_logs')
    op.drop_index('ix_audit_logs_created_date', table_name='audit_logs')
    op.drop_index('ix_audit_logs_username', table_name='audit_logs')

    # Remove columns
    op.drop_column('audit_logs', 'execution_time_ms')
    op.drop_column('audit_logs', 'user_agent')
    op.drop_column('audit_logs', 'client_ip')
    op.drop_column('audit_logs', 'response_payload')
    op.drop_column('audit_logs', 'request_method')
