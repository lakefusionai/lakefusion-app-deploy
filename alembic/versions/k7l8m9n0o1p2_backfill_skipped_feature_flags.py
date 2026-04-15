"""backfill feature flags skipped by parallel branch merges

Revision ID: k7l8m9n0o1p2
Revises: j6k7l8m9n0o1
Create Date: 2026-03-27 02:00:00.000000

Four feature flags were never inserted on staging/prod because they lived
in parallel alembic branches that got stamped as applied but whose DDL
never executed. This migration inserts them if missing.

Original migrations:
- 2a8b9c1d3e4f: ENABLE_DOCUMENTATION_HUB
- 9ac38a58bd93: SDK_CHANGES (toggled by later migrations, final state = INACTIVE)
- 76cc3702342f: ENABLE_BATCH_IMPORT_FOR_BUSINESS_GLOSSARY
- k6l7m8n9o0p1: ENABLE_DETERMINISTIC_MATCHING_FEATURE
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision: str = 'k7l8m9n0o1p2'
down_revision: Union[str, None] = 'j6k7l8m9n0o1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _insert_flag_if_missing(bind, name, status, description):
    """Insert a feature flag only if it doesn't already exist."""
    try:
        result = bind.execute(
            sa.text("SELECT COUNT(*) FROM feature_flags WHERE name = :name"),
            {"name": name}
        )
        count = result.scalar()
        if count == 0:
            bind.execute(
                sa.text("""
                    INSERT INTO feature_flags (name, status, description, owner_team, created_at, updated_at)
                    VALUES (:name, :status, :description, 'LakeFusion', :now, :now)
                """),
                {"name": name, "status": status, "description": description, "now": datetime.utcnow()}
            )
            logger.info(f"Inserted feature flag: {name} ({status})")
        else:
            logger.info(f"Feature flag {name} already exists, skipping")
    except Exception as e:
        logger.info(f"Could not insert feature flag {name}: {e}")


def upgrade() -> None:
    """Backfill 4 feature flags that were skipped by parallel branch merges."""

    bind = op.get_bind()

    _insert_flag_if_missing(
        bind,
        name="ENABLE_DOCUMENTATION_HUB",
        status="INACTIVE",
        description="Enables the Documentation Hub feature in the Self-Service Portal for accessing API docs, user guides, and developer documentation"
    )

    _insert_flag_if_missing(
        bind,
        name="SDK_CHANGES",
        status="INACTIVE",
        description="Enable SDK revamped notebooks with thin wrappers for integration-core, maven-core, and dnb_integration"
    )

    _insert_flag_if_missing(
        bind,
        name="ENABLE_BATCH_IMPORT_FOR_BUSINESS_GLOSSARY",
        status="INACTIVE",
        description="Enable batch import for business glossary to import multiple glossary terms at once."
    )

    _insert_flag_if_missing(
        bind,
        name="ENABLE_DETERMINISTIC_MATCHING_FEATURE",
        status="ACTIVE",
        description="Enables the Match Rules tab in the Add/Edit Model dialog for MatchMaven"
    )


def downgrade() -> None:
    bind = op.get_bind()

    flags = [
        "ENABLE_DOCUMENTATION_HUB",
        "SDK_CHANGES",
        "ENABLE_BATCH_IMPORT_FOR_BUSINESS_GLOSSARY",
        "ENABLE_DETERMINISTIC_MATCHING_FEATURE",
    ]

    for flag in flags:
        try:
            bind.execute(
                sa.text("DELETE FROM feature_flags WHERE name = :name"),
                {"name": flag}
            )
            logger.info(f"Deleted feature flag: {flag}")
        except Exception as e:
            logger.info(f"Could not delete feature flag {flag}: {e}")
