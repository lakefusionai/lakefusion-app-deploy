"""merge validation feature heads

Revision ID: d4e5f6a7b8c9
Revises: 47de1d469615, c3d4e5f6a7b8
Create Date: 2026-02-04 12:30:00.000000

Merges the two validation function feature flag migrations:
- 47de1d469615: ENABLE_VALIDATION_FUNCTION_ERROR_FEATURE
- c3d4e5f6a7b8: ENABLE_VALIDATION_FUNCTION_WARNING_FEATURE

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd4e5f6a7b8c9'
down_revision: Union[str, Sequence[str], None] = ('47de1d469615', 'c3d4e5f6a7b8')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
