"""db_config_schema_update

Revision ID: 4f3b398e6460
Revises: 6ad578c4345f
Create Date: 2025-10-07 13:17:02.054666

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

# revision identifiers, used by Alembic.
revision: str = '4f3b398e6460'
down_revision: Union[str, None] = '8cd7011307b7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    # Get database connection and inspector
    conn = op.get_bind()
    dialect = conn.dialect.name
    inspector = inspect(conn)

    # Get existing columns in the table
    columns = [col['name'] for col in inspector.get_columns('db_config_properties')]

    # Check if we have the old schema (configkey, configvalue)
    has_old_schema = 'configkey' in columns and 'configvalue' in columns
    has_new_schema = 'config_key' in columns and 'config_value' in columns

    if has_old_schema and not has_new_schema:
        # Case 2: Existing database with old schema - perform migration
        print("Detected old schema. Running migration to update db_config_properties...")

        # Rename existing columns (must specify the existing type for MySQL)
        op.alter_column('db_config_properties', 'configkey',
                        new_column_name='config_key',
                        existing_type=sa.String(length=255),
                        existing_nullable=False)
        op.alter_column('db_config_properties', 'configvalue',
                        new_column_name='config_value',
                        existing_type=sa.Text(),
                        existing_nullable=False)

        # Add new columns as nullable first (to allow existing rows)
        op.add_column('db_config_properties', sa.Column('config_value_type', sa.String(length=50), nullable=True))
        op.add_column('db_config_properties', sa.Column('config_label', sa.String(length=255), nullable=True))
        op.add_column('db_config_properties', sa.Column('config_desc', sa.Text(), nullable=True))
        op.add_column('db_config_properties', sa.Column('extended_values', sa.Text(), nullable=True))
        op.add_column('db_config_properties', sa.Column('config_category', sa.String(length=255), nullable=True))
        op.add_column('db_config_properties', sa.Column('config_show', sa.SmallInteger(), nullable=True))
        op.add_column('db_config_properties', sa.Column('updated_by', sa.String(length=255), nullable=True))
        op.add_column('db_config_properties', sa.Column('updated_at', sa.DateTime(), nullable=True))

        # Update existing rows with default values
        op.execute("""
            UPDATE db_config_properties
            SET
                config_value_type = '',
                config_label = '',
                config_desc = '',
                config_category = '',
                updated_by = 'system',
                updated_at = CURRENT_TIMESTAMP
            WHERE config_label IS NULL
        """)

        # Now alter columns to NOT NULL (except extended_values and updated_by which should remain nullable)
        op.alter_column('db_config_properties', 'config_value_type',
                        existing_type=sa.String(length=50),
                        nullable=False)
        op.alter_column('db_config_properties', 'config_label',
                        existing_type=sa.String(length=255),
                        nullable=False)
        op.alter_column('db_config_properties', 'config_desc',
                        existing_type=sa.Text(),
                        nullable=False)
        op.alter_column('db_config_properties', 'config_category',
                        existing_type=sa.String(length=255),
                        nullable=False)
        op.alter_column('db_config_properties', 'updated_at',
                        existing_type=sa.DateTime(),
                        nullable=False)

        # Add auto-update trigger for updated_at column
        if dialect == 'mysql':
            op.execute("""
                ALTER TABLE db_config_properties
                MODIFY COLUMN updated_at DATETIME NOT NULL
                DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP
            """)
        else:
            op.execute("""
                ALTER TABLE db_config_properties
                ALTER COLUMN updated_at SET DEFAULT CURRENT_TIMESTAMP
            """)

        print("Migration completed successfully.")

    elif has_new_schema:
        # Case 1: Fresh database or already migrated - skip migration
        print("Detected new schema. Skipping migration as db_config_properties is already up-to-date.")

    else:
        # Edge case: table doesn't exist or has unexpected schema
        print("Warning: Unexpected table schema detected. Please verify db_config_properties structure.")


def downgrade() -> None:
    # Get database connection and inspector
    conn = op.get_bind()
    inspector = inspect(conn)

    # Get existing columns in the table
    columns = [col['name'] for col in inspector.get_columns('db_config_properties')]

    # Only downgrade if new schema exists
    has_new_schema = 'config_key' in columns and 'config_value' in columns

    if has_new_schema:
        print("Reverting to old schema...")

        # Drop new columns
        op.drop_column('db_config_properties', 'updated_at')
        op.drop_column('db_config_properties', 'updated_by')
        op.drop_column('db_config_properties', 'config_category')
        op.drop_column('db_config_properties', 'extended_values')
        op.drop_column('db_config_properties', 'config_desc')
        op.drop_column('db_config_properties', 'config_label')
        op.drop_column('db_config_properties', 'config_value_type')
        op.drop_column('db_config_properties', 'config_show')

        # Rename columns back to original names (must specify the existing type for MySQL)
        op.alter_column('db_config_properties', 'config_value',
                        new_column_name='configvalue',
                        existing_type=sa.Text(),
                        existing_nullable=False)
        op.alter_column('db_config_properties', 'config_key',
                        new_column_name='configkey',
                        existing_type=sa.String(length=255),
                        existing_nullable=False)

        print("Downgrade completed successfully.")
    else:
        print("Old schema already exists. Skipping downgrade.")
