"""add version column to model_experiments

Revision ID: 8d593a92e3c6
Revises: ce1c99df9f70
Create Date: 2025-12-10 00:25:01.014247

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.orm import Session
from sqlalchemy import text
import os
import logging

# revision identifiers, used by Alembic.
revision: str = '8d593a92e3c6'
down_revision: Union[str, None] = 'ce1c99df9f70'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

logger = logging.getLogger(__name__)


def sync_model_experiment_versions(session: Session):
    """
    Sync version for all Model Experiment records by checking associated job_id.
    Extracts version tag from Databricks job definitions and updates DB.
    """
    from lakefusion_utility.utils.databricks_util import DatabricksJobManager
    
    token = os.environ.get('LAKEFUSION_DATABRICKS_DAPI', '')
    if not token:
        logger.warning("No Databricks token found, setting all versions to 1.0.0")
        session.execute(text("UPDATE model_experiments SET version = '1.0.0' WHERE version IS NULL"))
        return
    
    try:
        databricks_job = DatabricksJobManager(token)
        
        # Get all model experiments with their associated job info
        # Using raw SQL to avoid ORM dependency issues during migration
        result = session.execute(text("""
            SELECT DISTINCT me.id as model_id, mjj.job_id
            FROM model_experiments me
            LEFT JOIN match_maven_job mjj ON me.id = mjj.modelid AND me.entity_id = mjj.entity_id
            WHERE me.version IS NULL
        """))
        
        rows = result.fetchall()
        updated_count = 0
        
        for row in rows:
            model_id = row[0]
            job_id = row[1]
            version = "1.0.0"  # Default version for legacy experiments
            
            if job_id:
                try:
                    job = databricks_job.get_job_info(job_id=job_id)
                    if job.settings and job.settings.tags:
                        version = job.settings.tags.get("version", "1.0.0")
                except Exception as ex:
                    logger.warning(f"Failed to get job info for job_id={job_id}, model_id={model_id}: {ex}")
            
            # Update the model experiment with the version
            session.execute(
                text("UPDATE model_experiments SET version = :version WHERE id = :model_id"),
                {"version": version, "model_id": model_id}
            )
            updated_count += 1
        
        logger.info(f"Version sync completed for model_experiments. Updated {updated_count} rows.")
        
    except Exception as e:
        logger.error(f"Error during version sync: {e}")
        # Fallback: set all NULL versions to 1.0.0
        session.execute(text("UPDATE model_experiments SET version = '1.0.0' WHERE version IS NULL"))


def upgrade() -> None:
    # Add version column to model_experiments
    op.add_column('model_experiments', sa.Column('version', sa.String(length=255), nullable=True))
    
    # Populate existing records with version from associated Databricks jobs
    bind = op.get_bind()
    session = Session(bind=bind)
    
    try:
        sync_model_experiment_versions(session)
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Migration failed: {e}")
        # Ensure all records have at least a default version
        session.execute(text("UPDATE model_experiments SET version = '1.0.0' WHERE version IS NULL"))
        session.commit()
    finally:
        session.close()


def downgrade() -> None:
    op.drop_column('model_experiments', 'version')
