"""Bootstrap LAKEFUSION_ADMIN group (singular, canonical) and apply admin grants

Revision ID: e8f9a0b1c2d3
Revises: d7e8f9a0b1c2
Create Date: 2026-04-23 10:10:00.000000

Creates the account-level SCIM group ``LAKEFUSION_ADMIN`` (the canonical name
used by the flat RBAC config, replacing the prior ``LAKEFUSION_ADMINS_<WS>``),
stores its id on ``roles.dbx_group_id`` where ``name='Admin'``, and applies all
admin-level grants whose ``applies_to='always'`` (workspace-wide schemas and
shared volumes).

Existing per-workspace admin groups (``LAKEFUSION_ADMINS_<WORKSPACE_ID>``)
created by ``e4f5a6b7c8d9_initial_rbac_admin_sync`` are left untouched so
running installs don't lose admin access mid-deploy. The RBAC admin sync cron
should be updated separately to point at the new canonical name.
"""

import os
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "e8f9a0b1c2d3"
down_revision: Union[str, None] = "d7e8f9a0b1c2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()

    databricks_host = os.environ.get("DATABRICKS_HOST", "")
    databricks_token = os.environ.get("LAKEFUSION_DATABRICKS_DAPI", "")
    if not databricks_host or not databricks_token:
        print("  -> Skipping admin bootstrap: DATABRICKS_HOST / LAKEFUSION_DATABRICKS_DAPI not set")
        return

    from databricks.sdk.service import catalog
    from lakefusion_utility.config.rbac_config_loader import load_grants
    from lakefusion_utility.utils.databricks_groups import (
        ensure_account_group,
        grant_uc_object_permissions,
    )
    from lakefusion_utility.utils.databricks_util import _create_workspace_client

    # _create_workspace_client handles Databricks Apps OAuth, PAT fallback,
    # and DATABRICKS_HOST https:// prefix normalization in one place.
    w = _create_workspace_client(databricks_token)
    api = w.api_client

    group_id = ensure_account_group(api, "LAKEFUSION_ADMIN")
    print(f"  -> LAKEFUSION_ADMIN group id={group_id}")

    conn.execute(
        sa.text("UPDATE roles SET dbx_group_id = :gid WHERE name = :rname"),
        {"gid": str(group_id), "rname": "Admin"},
    )
    print("  -> roles.dbx_group_id set for 'Admin'")

    catalog_row = conn.execute(
        sa.text(
            "SELECT config_value FROM db_config_properties WHERE config_key = 'catalog_name'"
        )
    ).fetchone()
    if not catalog_row:
        print("  -> catalog_name not configured; skipping admin grant application")
        return
    catalog_name = catalog_row[0]

    rows = load_grants(
        entity="__bootstrap__",
        experiment_id="prod",
        catalog=catalog_name,
        pipeline_type=None,
        applies_to={"always"},
        roles=["admin"],
    )

    securable_map = {
        "catalog": catalog.SecurableType.CATALOG,
        "schema": catalog.SecurableType.SCHEMA,
        "volume": catalog.SecurableType.VOLUME,
        "table": catalog.SecurableType.TABLE,
    }
    granted = 0
    for row in rows:
        securable = securable_map.get(row.resource_type)
        if securable is None:
            continue
        result = grant_uc_object_permissions(
            w, row.full_name, securable,
            {row.group_name: row.privileges},
        )
        granted += len(result["results"])

    print(f"  -> Admin grants applied: {granted} rows")


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(
        sa.text("UPDATE roles SET dbx_group_id = NULL WHERE name = :rname"),
        {"rname": "Admin"},
    )
