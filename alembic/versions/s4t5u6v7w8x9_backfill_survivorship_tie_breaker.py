"""backfill tieBreaker on every existing survivorship rule

For every active row in ``survivorshiprule``, ensure each Source System
rule in the JSON ``rules`` column has an explicit ``tieBreaker`` field
set to ``"latest"`` (the engine default). Non-Source-System rules are
left alone — the field is meaningless there and the service strips it
on save.

After the DB pass, re-upload ``entity.json`` / ``model.json`` to the
Databricks metadata volume for every active integration hub so that the
running pipelines pick up the now-explicit field on next run. Reuses
the same logic backing the "Upload Metadata Files" button on the
Integration Hub job page (``Integration_HubService.upload_metadata_files``).

The engine already defaults ``tieBreaker`` to ``"latest"`` when missing,
so this migration is purely a data-cleanup pass — there is no runtime
behavioural change for entities that haven't been touched in the UI.

Revision ID: s4t5u6v7w8x9
Revises: k7l8m9n0o1p2
Create Date: 2026-05-11 00:00:00.000000

"""
from typing import Sequence, Union
import json
import logging
import os
import traceback

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

revision: str = 's4t5u6v7w8x9'
down_revision: Union[str, None] = 'k7l8m9n0o1p2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


VALID_TIE_BREAKERS = {"latest", "earliest"}


def upgrade() -> None:
    bind = op.get_bind()

    rows = bind.execute(text("""
        SELECT survivorship_id, entity_id, rules
        FROM survivorshiprule
        WHERE is_active = TRUE
    """)).fetchall()

    logger.info("tieBreaker backfill — scanning %d active rule groups", len(rows))

    patched_rules_total = 0
    patched_groups = 0
    untouched_groups = 0
    bad_payload_groups = 0

    for row in rows:
        rules_raw = row.rules

        # ``rules`` column is JSON; depending on the DB driver it may arrive
        # as a Python list/dict already or as a string that still needs to
        # be parsed. Cover both cases.
        try:
            if isinstance(rules_raw, (list, dict)):
                rules = rules_raw
            elif rules_raw is None:
                rules = []
            else:
                rules = json.loads(rules_raw)
        except (TypeError, ValueError) as e:
            logger.warning(
                "Skipping survivorship_id=%s entity_id=%s — rules JSON unparseable: %s",
                row.survivorship_id, row.entity_id, e,
            )
            bad_payload_groups += 1
            continue

        if not isinstance(rules, list):
            logger.warning(
                "Skipping survivorship_id=%s entity_id=%s — rules is not a list (type=%s)",
                row.survivorship_id, row.entity_id, type(rules).__name__,
            )
            bad_payload_groups += 1
            continue

        changed = False
        for rule in rules:
            if not isinstance(rule, dict):
                continue
            if rule.get("strategy") != "Source System":
                continue

            current = str(rule.get("tieBreaker") or "").lower()
            if current in VALID_TIE_BREAKERS:
                continue

            rule["tieBreaker"] = "latest"
            changed = True
            patched_rules_total += 1

        if changed:
            bind.execute(
                text(
                    "UPDATE survivorshiprule "
                    "SET rules = :rules "
                    "WHERE survivorship_id = :sid"
                ),
                {"rules": json.dumps(rules), "sid": row.survivorship_id},
            )
            patched_groups += 1
        else:
            untouched_groups += 1

    logger.info(
        "tieBreaker backfill DB pass complete — "
        "groups patched: %d, groups untouched: %d, groups skipped (bad payload): %d, "
        "rules patched: %d",
        patched_groups, untouched_groups, bad_payload_groups, patched_rules_total,
    )

    _reupload_metadata_for_all_integration_hubs(bind)


def _reupload_metadata_for_all_integration_hubs(bind) -> None:
    """Iterate every active integration hub and re-upload entity.json /
    model.json by invoking the same service used by the UI button.

    Mirrors the volume-pass logic from migration
    ``r3s4t5u6v7w8_backfill_use_cleaned_table``. Skipped (with a
    warning) when ``LAKEFUSION_DATABRICKS_DAPI`` is unset so local /
    CI runs without the token still complete the DB pass.
    """
    databricks_token = os.environ.get('LAKEFUSION_DATABRICKS_DAPI')
    if not databricks_token:
        logger.warning(
            "LAKEFUSION_DATABRICKS_DAPI not set — skipping metadata re-upload. "
            "Set the env var and re-run, OR rely on the next user-triggered "
            "save / integration job to regenerate per-entity metadata."
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
                logger.info(
                    "Uploaded metadata for integration_hub_id=%s entity_id=%s",
                    ih_id, entity_id,
                )
                success += 1
            except Exception as e:
                logger.warning(
                    "Failed metadata upload for integration_hub_id=%s entity_id=%s: %s",
                    ih_id, entity_id, e,
                )
                failures.append((ih_id, entity_id, str(e)))

        logger.info(
            "Metadata re-upload complete — success: %d, failures: %d",
            success, len(failures),
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
    # No-op. Stripping the field would just return the rules to the same
    # ambiguous state — the engine still defaults to "latest" regardless.
    logger.info("Downgrade is a no-op for tieBreaker backfill.")
