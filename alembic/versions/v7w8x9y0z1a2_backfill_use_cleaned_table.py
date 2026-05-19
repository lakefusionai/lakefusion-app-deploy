"""backfill use_cleaned_table for datasets with quality tasks
   and re-upload entity/model metadata json for every integration hub

For any dataset that has a quality_task entry, set use_cleaned_table = TRUE
on every entitydatasetmapping row referencing that dataset. This corrects
entities created before the use_cleaned_table feature was introduced, where
the pre-feature pipeline auto-appended `_cleaned` to dataset paths whenever
a DQ task existed. Without this backfill, post-upgrade entity.json contains
paths without `_cleaned`, while existing unified table records still carry
`source_path` with `_cleaned` — breaking SourceSystemStrategy exact-match
lookups in survivorship.

After the DB backfill, re-upload entity.json and model.json to the
Databricks metadata volume for every active integration hub so that the
running pipelines pick up the corrected paths. Reuses the same logic that
backs the "Upload Metadata Files" button on the Integration Hub job page
(Integration_HubService.upload_metadata_files).

Revision ID: v7w8x9y0z1a2
Revises: u6v7w8x9y0z1
Create Date: 2026-04-29 12:00:00.000000

"""
from typing import Sequence, Union
import logging
import os
import traceback

from alembic import op
import sqlalchemy as sa
from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)

revision: str = 'v7w8x9y0z1a2'
down_revision: Union[str, None] = 'u6v7w8x9y0z1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    dialect = bind.dialect.name

    select_sql = sa.text("""
        SELECT edm.entity_id, edm.dataset_id, edm.use_cleaned_table
        FROM entitydatasetmapping edm
        WHERE edm.dataset_id IN (
            SELECT DISTINCT qt.dataset_id
            FROM quality_tasks qt
            WHERE qt.dataset_id IS NOT NULL
        )
    """)
    rows = bind.execute(select_sql).fetchall()

    total = len(rows)
    already_true = sum(1 for r in rows if r.use_cleaned_table)
    to_update = total - already_true

    logger.info(
        "use_cleaned_table backfill — matches: %d, already TRUE: %d, to update: %d",
        total, already_true, to_update,
    )

    # MySQL-only: capture and disable safe-update mode so the WHERE-without-PK
    # update works regardless of server / client default. PostgreSQL has no
    # equivalent session variable.
    prior_safe_updates = None
    if dialect == "mysql":
        try:
            prior_safe_updates = bind.execute(
                sa.text("SELECT @@SESSION.sql_safe_updates")
            ).scalar()
        except Exception as e:
            logger.info("Could not read sql_safe_updates: %s", e)

    try:
        if dialect == "mysql":
            bind.execute(sa.text("SET SESSION sql_safe_updates = 0"))

        # Portable subquery form — works on MySQL 8 and PostgreSQL. The inner
        # derived-table alias `AS dq` is needed for MySQL's restriction on
        # self-referencing UPDATE targets.
        update_sql = sa.text("""
            UPDATE entitydatasetmapping
            SET use_cleaned_table = TRUE
            WHERE dataset_id IN (
                SELECT dataset_id FROM (
                    SELECT DISTINCT dataset_id
                    FROM quality_tasks
                    WHERE dataset_id IS NOT NULL
                ) AS dq
            )
              AND (use_cleaned_table = FALSE OR use_cleaned_table IS NULL)
        """)
        result = bind.execute(update_sql)
        logger.info("use_cleaned_table backfill — rows updated: %s", result.rowcount)
    finally:
        if dialect == "mysql" and prior_safe_updates is not None:
            try:
                bind.execute(sa.text(f"SET SESSION sql_safe_updates = {int(prior_safe_updates)}"))
            except Exception as e:
                logger.info("Could not restore sql_safe_updates: %s", e)

    _reupload_metadata_for_all_integration_hubs(bind)


def _reupload_metadata_for_all_integration_hubs(bind) -> None:
    """Iterate every active integration hub and re-upload entity.json /
    model.json by invoking the same service used by the UI button."""

    databricks_token = os.environ.get('LAKEFUSION_DATABRICKS_DAPI')
    if not databricks_token:
        logger.warning(
            "LAKEFUSION_DATABRICKS_DAPI not set — skipping metadata re-upload. "
            "Set the env var and re-run if needed."
        )
        return

    try:
        from lakefusion_utility.services.integration_hub_service import Integration_HubService
    except ImportError as e:
        logger.warning("Could not import Integration_HubService: %s", e)
        return

    session = Session(bind=bind)
    try:
        rows = session.execute(text("""
            SELECT id, entity_id
            FROM integration_hub
            WHERE is_active = TRUE
            ORDER BY id
        """)).fetchall()

        logger.info("Re-uploading metadata for %d active integration hubs", len(rows))

        ih_service = Integration_HubService(session)

        success = 0
        failures = []
        for row in rows:
            ih_id = row.id
            entity_id = row.entity_id
            try:
                ih_service.upload_metadata_files(ih_id, databricks_token)
                logger.info("Uploaded metadata for integration_hub_id=%s entity_id=%s", ih_id, entity_id)
                success += 1
            except Exception as e:
                logger.warning(
                    "Failed metadata upload for integration_hub_id=%s entity_id=%s: %s",
                    ih_id, entity_id, e,
                )
                failures.append((ih_id, entity_id, str(e)))

        logger.info(
            "Metadata re-upload complete — success: %d, failures: %d", success, len(failures)
        )
        if failures:
            for ih_id, entity_id, err in failures:
                logger.warning("  failed ih=%s entity=%s: %s", ih_id, entity_id, err)
    except Exception:
        logger.warning(
            "Metadata re-upload pass errored: %s", traceback.format_exc()
        )
    finally:
        session.close()


def downgrade() -> None:
    # No-op. Reverting use_cleaned_table flips would re-break entities relying
    # on the corrected value. If a true rollback is required, restore from
    # backup or set the column manually.
    logger.info("Downgrade is a no-op for use_cleaned_table backfill.")
