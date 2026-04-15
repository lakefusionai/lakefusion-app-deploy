"""update datatypes of entity attributes

Revision ID: dbfafff755c8
Revises: a1b2c3d4e5f6
Create Date: 2025-12-29 15:34:23.218626

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text



# revision identifiers, used by Alembic.
revision: str = 'dbfafff755c8'
down_revision: Union[str, None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

TYPE_MAPPINGS = {
    # Standard PostgreSQL types -> Spark types
    'bigint': 'BIGINT',
    'boolean': 'BOOLEAN',
    'char': 'STRING',
    'varchar': 'STRING',
    'date': 'DATE',
    'double precision': 'DOUBLE',
    'integer': 'INT',
    'numeric': 'FLOAT',
    'real': 'FLOAT',
    'smallint': 'SMALLINT',
    'text': 'STRING',
    'timestamp': 'TIMESTAMP',
    'timestamp without time zone': 'TIMESTAMP',
    'timestamp with time zone': 'TIMESTAMP',
    
    'string': 'STRING',
    'int': 'INT',
    'long': 'BIGINT',
    'double': 'DOUBLE',
    'float': 'FLOAT',
    'decimal': 'DOUBLE',
    'tinyint': 'TINYINT',
}

REVERSE_TYPE_MAPPINGS = {
    'BIGINT': 'bigint',
    'BOOLEAN': 'boolean',
    'DATE': 'date',
    'DOUBLE': 'double precision',
    'FLOAT': 'real',
    'INT': 'integer',
    'SMALLINT': 'smallint',
    'STRING': 'varchar',
    'TINYINT': 'smallint',
    'TIMESTAMP': 'timestamp',
}


def upgrade() -> None:
    """
    Upgrade: Convert PostgreSQL-style types to Spark-compatible uppercase types.
    
    This migration:
    1. Uses case-insensitive matching (LOWER function) to find rows to update
    2. Updates each type to its Spark-compatible equivalent
    3. Is idempotent - safe to run multiple times
    """
    connection = op.get_bind()
    
    # Log current state before migration
    result = connection.execute(
        text("SELECT type, COUNT(*) as cnt FROM entityattributes GROUP BY type ORDER BY type")
    )
    print("\n" + "=" * 60)
    print("BEFORE MIGRATION - Current type distribution:")
    print("=" * 60)
    for row in result:
        print(f"  {row.type}: {row.cnt}")
    print("=" * 60 + "\n")
    
    # Apply each type mapping
    total_updated = 0
    
    for old_type, new_type in TYPE_MAPPINGS.items():
        # Use parameterized query for safety
        # Case-insensitive match using LOWER()
        result = connection.execute(
            text("""
                UPDATE entityattributes 
                SET type = :new_type 
                WHERE LOWER(type) = LOWER(:old_type)
                AND type != :new_type
            """),
            {"old_type": old_type, "new_type": new_type}
        )
        
        if result.rowcount > 0:
            print(f"  Updated {result.rowcount} rows: '{old_type}' -> '{new_type}'")
            total_updated += result.rowcount
    
    # Log final state after migration
    result = connection.execute(
        text("SELECT type, COUNT(*) as cnt FROM entityattributes GROUP BY type ORDER BY type")
    )
    print("\n" + "=" * 60)
    print("AFTER MIGRATION - Updated type distribution:")
    print("=" * 60)
    for row in result:
        print(f"  {row.type}: {row.cnt}")
    print("=" * 60)
    print(f"\nTotal rows updated: {total_updated}")
    print("=" * 60 + "\n")


def downgrade() -> None:
    """
    Downgrade: Revert Spark-compatible types back to PostgreSQL-style types.
    
    This allows rolling back the migration if needed.
    """
    connection = op.get_bind()
    
    # Log current state before downgrade
    result = connection.execute(
        text("SELECT type, COUNT(*) as cnt FROM entityattributes GROUP BY type ORDER BY type")
    )
    
    # Apply reverse mappings
    total_updated = 0
    
    for spark_type, pg_type in REVERSE_TYPE_MAPPINGS.items():
        # Exact match for uppercase Spark types
        result = connection.execute(
            text("""
                UPDATE entityattributes 
                SET type = :pg_type 
                WHERE type = :spark_type
            """),
            {"spark_type": spark_type, "pg_type": pg_type}
        )
        
        if result.rowcount > 0:
            print(f"  Reverted {result.rowcount} rows: '{spark_type}' -> '{pg_type}'")
            total_updated += result.rowcount
    
    # Log final state after downgrade
    result = connection.execute(
        text("SELECT type, COUNT(*) as cnt FROM entityattributes GROUP BY type ORDER BY type")
    )
    print("\n" + "=" * 60)
    print("AFTER DOWNGRADE - Reverted type distribution:")
    print("=" * 60)
    for row in result:
        print(f"  {row.type}: {row.cnt}")
    print("=" * 60)
    print(f"\nTotal rows reverted: {total_updated}")
    print("=" * 60 + "\n")

