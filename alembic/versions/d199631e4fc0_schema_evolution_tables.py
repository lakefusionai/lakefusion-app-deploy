"""schema evolution tables

Revision ID: d199631e4fc0
Revises: k6l7m8n9o0p1
Create Date: 2026-03-02 00:13:18.271369

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.orm import Session
from sqlalchemy import text
import logging

# revision identifiers, used by Alembic.
revision: str = 'd199631e4fc0'
down_revision: Union[str, None] = 'k6l7m8n9o0p1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    # Add unique index on integration_hub.id so it can be used as a FK target.
    # integration_hub has a composite PK (id, entity_id, modelid), but
    # schema_evolution_jobs.integration_hub_id references integration_hub.id alone.
    # MySQL requires a unique index on the referenced column for FK constraints.
    op.create_index('uix_integration_hub_id', 'integration_hub', ['id'], unique=True)

    # Table 1: schema_evolution_jobs
    op.create_table(
        'schema_evolution_jobs',
        sa.Column('id', sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column('entity_id', sa.Integer(), sa.ForeignKey('entity.id'), nullable=False),
        sa.Column('integration_hub_id', sa.Integer(), sa.ForeignKey('integration_hub.id'), nullable=False),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('status', sa.String(50), nullable=False, server_default='DRAFT'),
        sa.Column('databricks_job_id', sa.String(255), nullable=True),
        sa.Column('databricks_run_id', sa.String(255), nullable=True),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('completed_at', sa.DateTime(), nullable=True),
        sa.Column('created_by', sa.String(255), nullable=False),
    )

    # Table 2: schema_evolution_edits
    op.create_table(
        'schema_evolution_edits',
        sa.Column('id', sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column('job_id', sa.Integer(), sa.ForeignKey('schema_evolution_jobs.id', ondelete='CASCADE'), nullable=False),
        sa.Column('action_type', sa.String(50), nullable=False),
        sa.Column('attribute_name', sa.String(255), nullable=False),
        sa.Column('attribute_label', sa.String(255), nullable=True),
        sa.Column('attribute_description', sa.Text(), nullable=True),
        sa.Column('attribute_type', sa.String(50), nullable=True),
        sa.Column('is_match_attribute', sa.Boolean(), server_default='0'),
        sa.Column('source_mappings', sa.JSON(), nullable=True),
        sa.Column('survivorship_rule', sa.JSON(), nullable=True),
        sa.Column('validation_function', sa.JSON(), nullable=True),
        sa.Column('new_attribute_name', sa.String(255), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )

    # Add schema_evolution_enabled column to entity table
    op.add_column(
        'entity',
        sa.Column('schema_evolution_enabled', sa.Boolean(), server_default='0', nullable=False)
    )

    # Insert SCHEMA_EVOLUTION feature flag
    bind = op.get_bind()
    session = Session(bind=bind)
    try:
        session.execute(
            text("""
                INSERT INTO feature_flags (name, description, status, owner_team, created_at, updated_at)
                VALUES ('SCHEMA_EVOLUTION', 'Enable schema evolution for entities', 'ACTIVE', 'LakeFusion', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """)
        )
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Feature flag SCHEMA_EVOLUTION may already exist: {e}")
    finally:
        session.close()


def downgrade() -> None:
    # Remove feature flag
    bind = op.get_bind()
    session = Session(bind=bind)
    try:
        session.execute(text("DELETE FROM feature_flags WHERE name = 'SCHEMA_EVOLUTION'"))
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Failed to remove SCHEMA_EVOLUTION feature flag: {e}")
    finally:
        session.close()

    op.drop_column('entity', 'schema_evolution_enabled')
    op.drop_table('schema_evolution_edits')
    op.drop_table('schema_evolution_jobs')
    op.drop_index('uix_integration_hub_id', table_name='integration_hub')
