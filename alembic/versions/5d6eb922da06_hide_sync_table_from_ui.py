"""hide_sync_table_from_ui

Revision ID: 5d6eb922da06
Revises: 2a8b9c1d3e4f
Create Date: 2026-02-02 23:40:29.873561

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5d6eb922da06'
down_revision: Union[str, None] = '2a8b9c1d3e4f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    #Hide sync_table column from UI by setting config_show to 0 in the db_config_properties table
    op.execute("""
        UPDATE db_config_properties 
        SET config_show = 0 
        WHERE config_key = 'sync_tables';
    """)



def downgrade() -> None:
    # Revert the change by setting config_show back to 1
    op.execute("""
        UPDATE db_config_properties 
        SET config_show = 1 
        WHERE config_key = 'sync_tables';
    """)
    
