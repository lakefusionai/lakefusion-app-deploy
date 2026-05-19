"""add execution_type column, ENABLE_SQL_STEWARDSHIP flag, and standardize job statuses

Revision ID: x9y0z1a2b3c4
Revises: w8x9y0z1a2b3
Create Date: 2026-04-16

NOTE: Originally created with revision id 'p1q2r3s4t5u6' which collided with
another migration on a parallel branch (add_entity_type_subtype_storage...).
Renamed to 'x9y0z1a2b3c4' to break the duplicate. If this migration was
already applied on an environment under the old id, run on that DB:
    UPDATE alembic_version SET version_num = 'x9y0z1a2b3c4'
    WHERE version_num = 'p1q2r3s4t5u6';
(only if the applied row was THIS migration, not the entity_type_subtype one).

1. Adds execution_type column to entity_search_databricks_job.
2. Inserts ENABLE_SQL_STEWARDSHIP feature flag (INACTIVE by default).
3. Migrates legacy status values to standardized uppercase enum:
   prepare → SUBMITTED, match_merge_pending → PENDING,
   match_merge_successed → COMPLETED, match_merge_failed → FAILED
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision: str = 'x9y0z1a2b3c4'
down_revision: Union[str, None] = 'w8x9y0z1a2b3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

STATUS_MIGRATION = {
    'prepare': 'SUBMITTED',
    'match_merge_pending': 'PENDING',
    'match_merge_successed': 'COMPLETED',
    'match_merge_failed': 'FAILED',
}


def _column_exists(conn, table_name: str, column_name: str) -> bool:
    dialect = conn.dialect.name
    if dialect == 'mysql':
        result = conn.execute(
            sa.text(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_schema = DATABASE() AND table_name = :table AND column_name = :column"
            ),
            {"table": table_name, "column": column_name},
        ).fetchone()
    else:
        result = conn.execute(
            sa.text(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_catalog = current_database() AND table_name = :table AND column_name = :column"
            ),
            {"table": table_name, "column": column_name},
        ).fetchone()
    return result is not None


def upgrade() -> None:
    bind = op.get_bind()

    # 1. Add execution_type and query_id columns, make job_run_id nullable.
    # Guarded with column-exists checks because some envs applied this DDL
    # under the old duplicate revision id 'p1q2r3s4t5u6' before the rename.
    if not _column_exists(bind, 'entity_search_databricks_job', 'execution_type'):
        op.add_column(
            'entity_search_databricks_job',
            sa.Column('execution_type', sa.String(50), nullable=True, server_default='job')
        )
    if not _column_exists(bind, 'entity_search_databricks_job', 'query_id'):
        op.add_column(
            'entity_search_databricks_job',
            sa.Column('query_id', sa.String(255), nullable=True)
        )
    try:
        op.alter_column(
            'entity_search_databricks_job',
            'job_run_id',
            existing_type=sa.String(255),
            nullable=True
        )
    except Exception as e:
        logger.info(f"alter_column job_run_id skipped (likely already nullable): {e}")

    # 2. Insert feature flag (if not exists)
    try:
        result = bind.execute(
            sa.text("SELECT COUNT(*) FROM feature_flags WHERE name = :name"),
            {"name": "ENABLE_SQL_STEWARDSHIP"}
        )
        if result.scalar() == 0:
            bind.execute(
                sa.text("""
                    INSERT INTO feature_flags (name, status, description, owner_team, created_at, updated_at)
                    VALUES (:name, :status, :description, 'LakeFusion', :now, :now)
                """),
                {
                    "name": "ENABLE_SQL_STEWARDSHIP",
                    "status": "INACTIVE",
                    "description": "Enable SQL warehouse-based stewardship operations (merge, not-a-match) instead of notebook jobs. ~10x faster.",
                    "now": datetime.utcnow()
                }
            )
            logger.info("Inserted feature flag: ENABLE_SQL_STEWARDSHIP (INACTIVE)")
    except Exception as e:
        logger.info(f"Could not insert feature flag: {e}")

    # 3. Migrate legacy status values to standardized uppercase
    for old_status, new_status in STATUS_MIGRATION.items():
        try:
            result = bind.execute(
                sa.text("UPDATE entity_search_databricks_job SET status = :new WHERE status = :old"),
                {"old": old_status, "new": new_status}
            )
            count = result.rowcount
            if count > 0:
                logger.info(f"Migrated {count} rows: '{old_status}' → '{new_status}'")
        except Exception as e:
            logger.info(f"Could not migrate status '{old_status}': {e}")


def downgrade() -> None:
    # Revert status values
    bind = op.get_bind()
    for old_status, new_status in STATUS_MIGRATION.items():
        try:
            bind.execute(
                sa.text("UPDATE entity_search_databricks_job SET status = :old WHERE status = :new"),
                {"old": old_status, "new": new_status}
            )
        except Exception:
            pass

    op.drop_column('entity_search_databricks_job', 'query_id')
    op.drop_column('entity_search_databricks_job', 'execution_type')
