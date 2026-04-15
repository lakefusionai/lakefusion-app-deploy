"""added base_prompt_id in model experiment

Revision ID: 51ff99157296
Revises: a34962cf3f69
Create Date: 2025-11-13 20:47:39.888587

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '51ff99157296'
down_revision: Union[str, None] = 'a34962cf3f69'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Auto-generated command
    op.alter_column('db_config_properties', 'config_show',
               existing_type=sa.SmallInteger(),
               nullable=False)

    # Manual migration: Add base_prompt_id to model_experiments
    op.add_column('model_experiments',
                  sa.Column('base_prompt_id', sa.Integer(), nullable=True))

    op.create_index('fk_model_experiments_base_prompt_id_base_prompts',
                    'model_experiments',
                    ['base_prompt_id'])

    op.create_foreign_key(
        'fk_model_experiments_base_prompt_id_base_prompts',
        'model_experiments',
        'base_prompts',
        ['base_prompt_id'],
        ['id'],
        ondelete='SET NULL',
        onupdate='CASCADE'
    )


def downgrade() -> None:
    # Drop foreign key constraint
    op.drop_constraint(
        'fk_model_experiments_base_prompt_id_base_prompts',
        'model_experiments',
        type_='foreignkey'
    )

    # Drop index
    op.drop_index('fk_model_experiments_base_prompt_id_base_prompts',
                  'model_experiments')

    # Drop column
    op.drop_column('model_experiments', 'base_prompt_id')

    # Revert db_config_properties change
    op.alter_column('db_config_properties', 'config_show',
               existing_type=sa.SmallInteger(),
               nullable=True)
