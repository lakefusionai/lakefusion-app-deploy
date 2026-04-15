"""create and seed pt_models_config with JSON schema

Revision ID: i4j5k6l7m8n9
Revises: 76cc3702342f
Create Date: 2026-02-12 16:45:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import json
from datetime import datetime


# revision identifiers, used by Alembic.
revision: str = 'i4j5k6l7m8n9'
down_revision: Union[str, None] = '64a5cc16b57e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create pt_models_config table (if not exists) and seed with known PT-supported models."""

    dialect = op.get_bind().dialect.name

    # Create table -- idempotency patch in env.py skips if table already exists
    # For updated_at ON UPDATE CURRENT_TIMESTAMP: MySQL-only feature, PG uses triggers
    if dialect == 'mysql':
        updated_at_default = sa.text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP')
    else:
        updated_at_default = sa.text('CURRENT_TIMESTAMP')

    op.create_table(
        'pt_models_config',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('entity_name', sa.String(255), nullable=False, unique=True),
        sa.Column('entity_version', sa.String(10), nullable=False, server_default='1'),
        sa.Column('pt_type', sa.Enum('model_units', 'legacy', name='pttype'), nullable=False),
        sa.Column('model_category', sa.Enum('llm', 'embedding', name='modelcategory'), nullable=False),
        sa.Column('pt_config', sa.JSON(), nullable=False),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default='1'),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=updated_at_default),
    )

    # Create indexes
    op.create_index('ix_pt_models_config_entity_name', 'pt_models_config', ['entity_name'])
    op.create_index('ix_pt_models_config_is_active', 'pt_models_config', ['is_active'])

    # Seed with known PT-supported models using JSON format
    now = datetime.utcnow()

    pt_models_table = sa.table(
        'pt_models_config',
        sa.column('entity_name', sa.String),
        sa.column('entity_version', sa.String),
        sa.column('pt_type', sa.String),
        sa.column('model_category', sa.String),
        sa.column('pt_config', sa.JSON),
        sa.column('is_active', sa.Boolean),
        sa.column('created_at', sa.DateTime),
        sa.column('updated_at', sa.DateTime),
    )

    # Define seed data with JSON configs
    # Entity names use Databricks native format (underscores) to match Unity Catalog API responses
    seed_data = [
        # Model Units PT models (new SDK >=0.55.0)
        {
            'entity_name': 'system.ai.gpt-oss-120b',
            'entity_version': '1',
            'pt_type': 'model_units',
            'model_category': 'llm',
            'pt_config': json.dumps({"chunk_size": 50, "provisioned_model_units": 50}),
            'is_active': True,
            'created_at': now,
            'updated_at': now,
        },
        {
            'entity_name': 'system.ai.gpt-oss-20b',
            'entity_version': '1',
            'pt_type': 'model_units',
            'model_category': 'llm',
            'pt_config': json.dumps({"chunk_size": 50, "provisioned_model_units": 50}),
            'is_active': True,
            'created_at': now,
            'updated_at': now,
        },
        {
            'entity_name': 'system.ai.gemma-3-12b-it',
            'entity_version': '1',
            'pt_type': 'model_units',
            'model_category': 'llm',
            'pt_config': json.dumps({"chunk_size": 50, "provisioned_model_units": 50}),
            'is_active': True,
            'created_at': now,
            'updated_at': now,
        },
        {
            'entity_name': 'system.ai.llama-4-maverick',
            'entity_version': '1',
            'pt_type': 'model_units',
            'model_category': 'llm',
            'pt_config': json.dumps({"chunk_size": 50, "provisioned_model_units": 50}),
            'is_active': True,
            'created_at': now,
            'updated_at': now,
        },
        # Legacy PT models (tokens/sec)
        {
            'entity_name': 'system.ai.llama_v3_3_70b_instruct',
            'entity_version': '1',
            'pt_type': 'legacy',
            'model_category': 'llm',
            'pt_config': json.dumps({
                "chunk_size": 1580,
                "min_provisioned_throughput": 0,
                "max_provisioned_throughput": 3160
            }),
            'is_active': True,
            'created_at': now,
            'updated_at': now,
        },
        {
            'entity_name': 'system.ai.gte_large_en_v1_5',
            'entity_version': '1',
            'pt_type': 'legacy',
            'model_category': 'embedding',
            'pt_config': json.dumps({
                "chunk_size": 19000,
                "min_provisioned_throughput": 0,
                "max_provisioned_throughput": 38000
            }),
            'is_active': True,
            'created_at': now,
            'updated_at': now,
        },
    ]

    # Bulk insert seed data
    op.bulk_insert(pt_models_table, seed_data)


def downgrade() -> None:
    """Drop pt_models_config table."""
    op.drop_index('ix_pt_models_config_is_active', table_name='pt_models_config')
    op.drop_index('ix_pt_models_config_entity_name', table_name='pt_models_config')
    op.drop_table('pt_models_config')
