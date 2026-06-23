"""seed lakegraph_url into db_config_properties (Settings → LakeGraph)

Revision ID: lg1u2r3l4c5g
Revises: se1c2o3m4p5x
Create Date: 2026-06-09

The LakeGraph base URL is per-client (only known after the client provisions their
LakeGraph instance), so it's configured in-app via Settings → LakeGraph rather than
a build-time env var. Seed an empty row; the integration stays cleanly disabled
(deploy → 503, UI hides the action) until a value is set.

Database-agnostic: generic sa.* types, op.bulk_insert. Idempotent ops handled by env.py.
"""

from typing import Sequence, Union
from datetime import datetime

from alembic import op
from sqlalchemy.sql import column, table
from sqlalchemy import String, Text, SmallInteger, DateTime


# revision identifiers, used by Alembic.
revision: str = "lg1u2r3l4c5g"
down_revision: Union[str, None] = "se1c2o3m4p5x"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    cfg = table(
        "db_config_properties",
        column("config_key", String),
        column("config_value", Text),
        column("config_value_type", String),
        column("config_label", String),
        column("config_desc", Text),
        column("config_category", String),
        column("config_show", SmallInteger),
        column("updated_by", String),
        column("updated_at", DateTime),
    )
    op.bulk_insert(
        cfg,
        [
            {
                "config_key": "lakegraph_url",
                "config_value": "",
                "config_value_type": "string",
                "config_label": "LakeGraph URL",
                "config_desc": (
                    "Base URL of the LakeGraph instance for graph deploy / redeploy / "
                    "status (e.g. https://lakegraph-<workspace>.azure.databricksapps.com). "
                    "Leave blank until the client's LakeGraph is provisioned."
                ),
                "config_category": "LAKEGRAPH",
                "config_show": 1,
                "updated_by": "system",
                "updated_at": datetime.utcnow(),
            }
        ],
    )


def downgrade() -> None:
    op.execute("DELETE FROM db_config_properties WHERE config_key = 'lakegraph_url'")
