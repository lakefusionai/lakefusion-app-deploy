"""RBAC backfill for existing entities

Revision ID: f5a6b7c8d9e0
Revises: d1s2p3o4r5d6
Create Date: 2026-03-16 12:00:00.000000

Provisions Databricks account-level groups, writes group IDs back onto
``entity`` rows, adds the entity owner as a member, and applies every
``applies_to='always'`` grant from ``rbac_permissions.json`` for each active
entity that doesn't already have both group IDs populated.

Delegates to :class:`RBACProvisioningService` so this one-time backfill takes
the exact same code path as entity-create-time provisioning — the flat config
is the single source of truth for group templates and grants. Previous
revisions of this migration hardcoded privilege maps inline; they drifted from
``rbac_permissions.json`` over time (missing ``USE_CATALOG``, outdated table
list, stale job privileges). That inline logic has been removed.

The provisioning service now lives in ``lakefusion_utility`` and has no
``lakefusion_core_engine`` dependency, so this migration runs in any
service venv that has ``lakefusion_utility`` installed.
"""

import os
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "f5a6b7c8d9e0"
down_revision: Union[str, None] = "d1s2p3o4r5d6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _required_tables_exist(conn) -> bool:
    inspector = sa.inspect(conn)
    tables = set(inspector.get_table_names())
    required = {"roles", "rbac_groups", "group_permissions", "group_members", "entity"}
    return required.issubset(tables)


def upgrade() -> None:
    conn = op.get_bind()

    if not _required_tables_exist(conn):
        print("  -> Required RBAC tables do not exist, skipping backfill")
        return

    databricks_host = os.environ.get("DATABRICKS_HOST", "")
    databricks_token = os.environ.get("LAKEFUSION_DATABRICKS_DAPI", "")
    if not databricks_host or not databricks_token:
        print("  -> DATABRICKS_HOST / LAKEFUSION_DATABRICKS_DAPI not set; skipping backfill")
        return

    # catalog_name must be configured in db_config_properties before RBAC
    # provisioning can run (the service uses it to qualify Unity Catalog
    # grants). On a fresh deploy it may not exist yet — skip cleanly instead
    # of letting RBACProvisioningService.__init__ raise.
    catalog_row = conn.execute(
        sa.text("SELECT config_value FROM db_config_properties WHERE config_key = 'catalog_name'")
    ).fetchone()
    if not catalog_row or not catalog_row[0]:
        print("  -> catalog_name not configured in db_config_properties; skipping backfill")
        return

    from sqlalchemy.orm import Session as OrmSession
    from lakefusion_utility.models.entity import Entity
    from lakefusion_utility.services.rbac_provisioning_service import (
        RBACProvisioningService,
    )

    session = OrmSession(bind=conn)
    try:
        service = RBACProvisioningService(db=session, token=databricks_token)

        entities = (
            session.query(Entity)
            .filter(Entity.is_active == True)  # noqa: E712
            .order_by(Entity.id)
            .all()
        )

        if not entities:
            print("  -> No active entities found; nothing to backfill")
            return

        print(f"  -> Backfilling RBAC provisioning for {len(entities)} active entities")

        for entity in entities:
            if entity.developer_group_id and entity.steward_group_id:
                print(f"    [SKIP] {entity.name} — groups already provisioned")
                continue
            sp = session.begin_nested()
            try:
                result = service.provision_entity(
                    entity,
                    creator_email=entity.created_by,
                    applies_to=("always", "prod"),
                    pipeline_type="integration",
                )
                session.flush()
                sp.commit()
                print(
                    f"    [OK]   {entity.name} — "
                    f"dev={result['developer_group_id']} steward={result['steward_group_id']} "
                    f"grants={result['grants_applied']} skipped={result['grants_skipped']}"
                )
            except Exception as e:
                sp.rollback()
                print(f"    [WARN] {entity.name} — provisioning failed: {e}")

        print("  -> RBAC backfill complete")
    finally:
        session.close()


def downgrade() -> None:
    # Backfill is forward-only — there's no safe way to reverse the provisioning
    # without risking the removal of Databricks groups that other running
    # services (middlelayer, pipeline) may have since extended with members or
    # grants. Leave the artifacts in place; operators who truly need to revert
    # should run a targeted cleanup script.
    pass
