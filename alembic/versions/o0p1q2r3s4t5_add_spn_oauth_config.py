"""add spn oauth config

Revision ID: o0p1q2r3s4t5
Revises: n9o0p1q2r3s4
Create Date: 2026-03-13

Move lakefusion_spn to SPN category with updated label/description.
The new lakefusion_spn_secret config is handled by config_defaults seeding.
"""
from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = 'o0p1q2r3s4t5'
down_revision: Union[str, None] = 'n9o0p1q2r3s4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()

    # Update lakefusion_spn: move to SPN category, update label and description
    conn.execute(text("""
        UPDATE db_config_properties
        SET config_label = 'Service Principal Client ID',
            config_desc = 'The Application (Client) ID of the Service Principal used for secure authentication between LakeFusion and Databricks, including job execution (run_as) and workflow OAuth flows such as the Vector Search network-optimized route.',
            config_category = 'SPN',
            updated_at = CURRENT_TIMESTAMP
        WHERE config_key = 'lakefusion_spn'
    """))


def downgrade() -> None:
    conn = op.get_bind()

    # Move lakefusion_spn back to GENERAL category with original label/description
    conn.execute(text("""
        UPDATE db_config_properties
        SET config_label = 'Lakefusion SPN',
            config_desc = 'Represents the Service Principal Name (SPN) used for secure authentication between LakeFusion and Databricks.',
            config_category = 'GENERAL',
            updated_at = CURRENT_TIMESTAMP
        WHERE config_key = 'lakefusion_spn'
    """))
