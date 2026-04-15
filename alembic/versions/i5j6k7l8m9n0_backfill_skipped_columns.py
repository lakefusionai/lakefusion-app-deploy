"""backfill columns and seed data skipped by parallel branch merges

Revision ID: i5j6k7l8m9n0
Revises: h4i5j6k7l8m9
Create Date: 2026-03-27 01:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import json
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision: str = 'i5j6k7l8m9n0'
down_revision: Union[str, None] = 'h4i5j6k7l8m9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Backfill columns and seed data that were skipped when parallel alembic
    branches got stamped but their DDL never executed. Each operation is
    wrapped in try/except so environments where these already exist continue
    without error."""

    bind = op.get_bind()

    # --- Columns from migration 672634ea4156 (add_refresh_type_column) ---
    try:
        op.add_column('profiling_tasks', sa.Column('refresh_type', sa.String(255), nullable=True))
        logger.info("Added profiling_tasks.refresh_type")
    except Exception as e:
        logger.info(f"profiling_tasks.refresh_type already exists or skipped: {e}")

    try:
        op.add_column('profiling_tasks', sa.Column('refresh_info', sa.JSON(), nullable=True))
        logger.info("Added profiling_tasks.refresh_info")
    except Exception as e:
        logger.info(f"profiling_tasks.refresh_info already exists or skipped: {e}")

    try:
        bind.execute(sa.text("UPDATE profiling_tasks SET refresh_type = 'onSchedule' WHERE refresh_type IS NULL"))
        logger.info("Set default refresh_type for existing profiling_tasks rows")
    except Exception as e:
        logger.info(f"Could not update profiling_tasks.refresh_type defaults: {e}")

    # --- Column from migration j5k6l7m8n9o0 (add_deterministic_rules) ---
    try:
        op.add_column('model_experiments', sa.Column('deterministic_rules', sa.JSON(), nullable=True))
        logger.info("Added model_experiments.deterministic_rules")
    except Exception as e:
        logger.info(f"model_experiments.deterministic_rules already exists or skipped: {e}")

    # --- Column from migration 13df984e18e0 (add_use_cleaned_table_column) ---
    try:
        op.add_column('entitydatasetmapping', sa.Column('use_cleaned_table', sa.Boolean(), nullable=True))
        logger.info("Added entitydatasetmapping.use_cleaned_table")
    except Exception as e:
        logger.info(f"entitydatasetmapping.use_cleaned_table already exists or skipped: {e}")

    # --- Seed data from migration i4j5k6l7m8n9 (pt_models_config) ---
    try:
        result = bind.execute(sa.text("SELECT COUNT(*) FROM pt_models_config"))
        count = result.scalar()
        if count == 0:
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
            op.bulk_insert(pt_models_table, [
                {
                    'entity_name': 'system.ai.gpt-oss-120b', 'entity_version': '1',
                    'pt_type': 'model_units', 'model_category': 'llm',
                    'pt_config': json.dumps({"chunk_size": 50, "provisioned_model_units": 50}),
                    'is_active': True, 'created_at': now, 'updated_at': now,
                },
                {
                    'entity_name': 'system.ai.gpt-oss-20b', 'entity_version': '1',
                    'pt_type': 'model_units', 'model_category': 'llm',
                    'pt_config': json.dumps({"chunk_size": 50, "provisioned_model_units": 50}),
                    'is_active': True, 'created_at': now, 'updated_at': now,
                },
                {
                    'entity_name': 'system.ai.gemma-3-12b-it', 'entity_version': '1',
                    'pt_type': 'model_units', 'model_category': 'llm',
                    'pt_config': json.dumps({"chunk_size": 50, "provisioned_model_units": 50}),
                    'is_active': True, 'created_at': now, 'updated_at': now,
                },
                {
                    'entity_name': 'system.ai.llama-4-maverick', 'entity_version': '1',
                    'pt_type': 'model_units', 'model_category': 'llm',
                    'pt_config': json.dumps({"chunk_size": 50, "provisioned_model_units": 50}),
                    'is_active': True, 'created_at': now, 'updated_at': now,
                },
                {
                    'entity_name': 'system.ai.llama_v3_3_70b_instruct', 'entity_version': '1',
                    'pt_type': 'legacy', 'model_category': 'llm',
                    'pt_config': json.dumps({"chunk_size": 1580, "min_provisioned_throughput": 0, "max_provisioned_throughput": 3160}),
                    'is_active': True, 'created_at': now, 'updated_at': now,
                },
                {
                    'entity_name': 'system.ai.gte_large_en_v1_5', 'entity_version': '1',
                    'pt_type': 'legacy', 'model_category': 'embedding',
                    'pt_config': json.dumps({"chunk_size": 19000, "min_provisioned_throughput": 0, "max_provisioned_throughput": 38000}),
                    'is_active': True, 'created_at': now, 'updated_at': now,
                },
            ])
            logger.info("Seeded pt_models_config with 6 default models")
        else:
            logger.info(f"pt_models_config already has {count} rows, skipping seed")
    except Exception as e:
        logger.info(f"Could not seed pt_models_config: {e}")


def downgrade() -> None:
    try:
        op.drop_column('entitydatasetmapping', 'use_cleaned_table')
    except Exception as e:
        logger.info(f"Could not drop entitydatasetmapping.use_cleaned_table: {e}")

    try:
        op.drop_column('model_experiments', 'deterministic_rules')
    except Exception as e:
        logger.info(f"Could not drop model_experiments.deterministic_rules: {e}")

    try:
        op.drop_column('profiling_tasks', 'refresh_info')
    except Exception as e:
        logger.info(f"Could not drop profiling_tasks.refresh_info: {e}")

    try:
        op.drop_column('profiling_tasks', 'refresh_type')
    except Exception as e:
        logger.info(f"Could not drop profiling_tasks.refresh_type: {e}")

    try:
        op.execute("DELETE FROM pt_models_config")
    except Exception as e:
        logger.info(f"Could not clear pt_models_config: {e}")
