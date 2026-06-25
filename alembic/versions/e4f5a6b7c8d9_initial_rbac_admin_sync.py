"""Ensure RBAC group-id columns exist on ``roles`` and ``entity``

Revision ID: e4f5a6b7c8d9
Revises: d3e4f5a6b7c8
Create Date: 2026-02-24 10:00:00.000000

Adds ``roles.dbx_group_id``, ``entity.developer_group_id``, and
``entity.steward_group_id`` if they don't already exist. These columns are
referenced by the ORM ``Role`` and ``Entity`` models (extended for RBAC
provisioning in ``d7e8f9a0b1c2``); adding them early keeps any subsequent
migration or service startup that queries those models from failing with
``Unknown column`` before the later migration has run.

History: this revision previously also created a legacy account-level group
``LAKEFUSION_ADMINS_<WORKSPACE_ID>`` and a DB group "LakeFusion Admins
(Auto-Synced)". Both were superseded by ``e8f9a0b1c2d3_bootstrap_lakefusion_admin_group``
(the canonical ``LAKEFUSION_ADMIN`` singular group) and by the hourly
``sync_admin_users`` cron job. The legacy code path has been removed so fresh
installs don't create the now-unused artifacts. Environments that already ran
the old version keep the extra group harmlessly — no code references it.
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "e4f5a6b7c8d9"
down_revision: Union[str, None] = "d3e4f5a6b7c8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _ensure_rbac_columns(conn) -> None:
    inspector = sa.inspect(conn)
    table_names = set(inspector.get_table_names())

    if "roles" in table_names:
        roles_cols = {c["name"] for c in inspector.get_columns("roles")}
        if "dbx_group_id" not in roles_cols:
            op.add_column("roles", sa.Column("dbx_group_id", sa.String(length=255), nullable=True))

    if "entity" in table_names:
        entity_cols = {c["name"] for c in inspector.get_columns("entity")}
        if "developer_group_id" not in entity_cols:
            op.add_column("entity", sa.Column("developer_group_id", sa.String(length=255), nullable=True))
        if "steward_group_id" not in entity_cols:
            op.add_column("entity", sa.Column("steward_group_id", sa.String(length=255), nullable=True))


def upgrade() -> None:
    conn = op.get_bind()
    _ensure_rbac_columns(conn)


def downgrade() -> None:
    # Columns are owned by ``d7e8f9a0b1c2``; that migration drops them on downgrade.
    pass
