"""migrate_survivorship_rules_to_new_format

Revision ID: 1ace0c7b0781
Revises: 5bb4ce57d8e9
Create Date: 2026-01-13 13:32:40.210191

This migration:
1. Converts existing survivorship rules to the new format in the database
2. Uploads updated survivorship JSON to Databricks volumes for all entities
"""
from typing import Sequence, Union
import os

from alembic import op
import sqlalchemy as sa
from sqlalchemy.orm import Session
from sqlalchemy import text
import json


# revision identifiers, used by Alembic.
revision: str = '1ace0c7b0781'
down_revision: Union[str, None] = '5bb4ce57d8e9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Mapping of attribute types to default aggregation functions
AGGREGATION_DEFAULTS_BY_TYPE = {
    # Numeric types
    'BIGINT': 'avg',
    'INT': 'avg',
    'INTEGER': 'avg',
    'SMALLINT': 'avg',
    'TINYINT': 'avg',
    'FLOAT': 'avg',
    'DOUBLE': 'avg',
    'DECIMAL': 'avg',
    'LONG': 'avg',
    'NUMBER': 'avg',
    # String types
    'STRING': 'concat',
    'VARCHAR': 'concat',
    'TEXT': 'concat',
    'CHAR': 'concat',
    # Date/Timestamp types
    'DATE': 'earliest',
    'TIMESTAMP': 'earliest',
    'DATETIME': 'earliest',
    # Boolean
    'BOOLEAN': 'any_true',
    'BOOL': 'any_true',
}

DEFAULT_AGGREGATION_FUNCTION = 'concat'  # Fallback for unknown types


def get_default_aggregation_function(attribute_type: str) -> str:
    """Get the default aggregation function for an attribute type."""
    if not attribute_type:
        return DEFAULT_AGGREGATION_FUNCTION
    normalized_type = attribute_type.upper()
    return AGGREGATION_DEFAULTS_BY_TYPE.get(normalized_type, DEFAULT_AGGREGATION_FUNCTION)


def is_new_aggregation_format(strategy_rule) -> bool:
    """Check if the strategyRule is already in the new format for Aggregation."""
    if isinstance(strategy_rule, dict) and 'function' in strategy_rule:
        return True
    return False


def is_new_frequency_format(strategy_rule) -> bool:
    """Check if the strategyRule is already in the new format for Frequency."""
    return strategy_rule in ('most_frequent', 'least_frequent')


def migrate_rules(rules: list, attribute_types: dict) -> tuple[list, bool]:
    """
    Migrate rules to the new format.
    Returns (migrated_rules, was_modified)
    """
    migrated_rules = []
    was_modified = False
    
    for rule in rules:
        new_rule = rule.copy()
        strategy = rule.get('strategy', '')
        attribute = rule.get('attribute', '')
        strategy_rule = rule.get('strategyRule')
        
        if strategy == 'Aggregation':
            if not is_new_aggregation_format(strategy_rule):
                # Get attribute type and determine default function
                attr_type = attribute_types.get(attribute, 'STRING')
                default_function = get_default_aggregation_function(attr_type)
                new_rule['strategyRule'] = {'function': default_function}
                was_modified = True
                print(f"  Migrating Aggregation rule for '{attribute}' (type: {attr_type}): "
                      f"strategyRule -> {{'function': '{default_function}'}}")
        
        elif strategy == 'Frequency':
            if not is_new_frequency_format(strategy_rule):
                new_rule['strategyRule'] = 'most_frequent'
                was_modified = True
                print(f"  Migrating Frequency rule for '{attribute}': "
                      f"strategyRule -> 'most_frequent'")
        
        # Ensure ignoreNull exists
        if 'ignoreNull' not in new_rule:
            new_rule['ignoreNull'] = False
            was_modified = True
        
        # Ensure aggregateUniqueOnly exists for Aggregation
        if strategy == 'Aggregation' and 'aggregateUniqueOnly' not in new_rule:
            new_rule['aggregateUniqueOnly'] = False
            was_modified = True
        
        migrated_rules.append(new_rule)
    
    return migrated_rules, was_modified


def get_entity_survivorship_data(session: Session, entity_id: int) -> list:
    """
    Get all survivorship groups for an entity.
    
    Returns list of survivorship group dictionaries.
    """
    query = text("""
        SELECT 
            survivorship_id,
            entity_id,
            group_name,
            description,
            is_default,
            rules,
            created_at,
            created_by,
            is_active
        FROM survivorshiprule
        WHERE entity_id = :entity_id AND is_active = true
    """)
    
    result = session.execute(query, {'entity_id': entity_id}).fetchall()
    
    survivorship_groups = []
    for row in result:
        rules = row.rules
        if isinstance(rules, str):
            rules = json.loads(rules)
        
        survivorship_groups.append({
            'survivorship_id': row.survivorship_id,
            'entity_id': row.entity_id,
            'group_name': row.group_name,
            'description': row.description,
            'is_default': row.is_default,
            'rules': rules,
            'created_at': str(row.created_at) if row.created_at else None,
            'created_by': row.created_by,
            'is_active': row.is_active
        })
    
    return survivorship_groups


def upload_to_databricks(session: Session, entity_id: int, survivorship_data: list) -> bool:
    """
    Upload survivorship data to Databricks volume.
    
    Returns True if successful, False otherwise.
    """
    try:
        from lakefusion_utility.utils.databricks_util import _create_workspace_client
        from io import BytesIO

        # Get Databricks configuration
        databricks_token = os.environ.get('DATABRICKS_TOKEN')

        # Get catalog name from config
        catalog_query = text("""
            SELECT config_value
            FROM config_properties
            WHERE config_key = 'catalog_name'
        """)
        catalog_result = session.execute(catalog_query).fetchone()

        if not catalog_result:
            print(f"  Warning: catalog_name not found in config. Skipping Databricks upload.")
            return False

        catalog_name = catalog_result.config_value

        # Initialize Databricks client
        w = _create_workspace_client(databricks_token)
        
        # Build volume path for production survivorship
        volume_path = f"/Volumes/{catalog_name}/metadata/metadata_files"
        folder_path = f"{volume_path}/entity_{entity_id}_prod"
        file_path = f"{folder_path}_survivorship.json"
        
        # Convert to JSON and upload
        json_data = json.dumps(survivorship_data, default=str)
        file_content = BytesIO(json_data.encode('utf-8'))
        
        w.dbfs.upload(file_path, file_content, overwrite=True)
        print(f"  Uploaded survivorship.json to {file_path}")
        
        return True
        
    except ImportError:
        print(f"  Warning: databricks-sdk not installed. Skipping Databricks upload.")
        return False
    except Exception as e:
        print(f"  Warning: Failed to upload to Databricks: {e}")
        return False


def upgrade() -> None:
    """Migrate survivorship rules to the new format and upload to Databricks."""
    bind = op.get_bind()
    session = Session(bind=bind)
    
    try:
        # ============================================
        # STEP 1: Migrate survivorship rules in database
        # ============================================
        print("\n" + "="*60)
        print("STEP 1: Migrating survivorship rules in database")
        print("="*60)
        
        survivorship_query = text("""
            SELECT 
                s.survivorship_id,
                s.entity_id,
                s.group_name,
                s.rules
            FROM survivorshiprule s
            WHERE s.is_active = true
        """)
        
        survivorship_groups = session.execute(survivorship_query).fetchall()
        print(f"Found {len(survivorship_groups)} survivorship groups to check")
        
        # Track which entities were updated
        updated_entities = set()
        
        for group in survivorship_groups:
            survivorship_id = group.survivorship_id
            entity_id = group.entity_id
            group_name = group.group_name
            rules = group.rules
            
            # Parse rules if it's a string
            if isinstance(rules, str):
                rules = json.loads(rules)
            
            if not rules:
                continue
            
            print(f"\nProcessing group '{group_name}' (ID: {survivorship_id})")
            
            # Get attribute types for this entity
            attributes_query = text("""
                SELECT name, type
                FROM entityattributes
                WHERE entity_id = :entity_id AND is_active = true
            """)
            attributes_result = session.execute(
                attributes_query, 
                {'entity_id': entity_id}
            ).fetchall()
            
            attribute_types = {attr.name: attr.type for attr in attributes_result}
            
            # Migrate rules
            migrated_rules, was_modified = migrate_rules(rules, attribute_types)
            
            if was_modified:
                # Update the survivorship group with migrated rules
                update_query = text("""
                    UPDATE survivorshiprule
                    SET rules = :rules
                    WHERE survivorship_id = :survivorship_id
                """)
                session.execute(
                    update_query,
                    {
                        'rules': json.dumps(migrated_rules),
                        'survivorship_id': survivorship_id
                    }
                )
                print(f"  Updated group '{group_name}'")
                updated_entities.add(entity_id)
            else:
                print(f"  No migration needed for group '{group_name}'")
        
        # Commit database changes
        session.commit()
        print("\nDatabase migration completed successfully!")
        
        # ============================================
        # STEP 2: Upload survivorship JSON to Databricks for ALL entities
        # ============================================
        print("\n" + "="*60)
        print("STEP 2: Uploading survivorship JSON to Databricks")
        print("="*60)
        
        # Get ALL active entities
        entities_query = text("""
            SELECT DISTINCT id, name
            FROM entity
            WHERE is_active = true
        """)
        all_entities = session.execute(entities_query).fetchall()
        print(f"Found {len(all_entities)} active entities to sync to Databricks")
        
        successful_uploads = 0
        failed_uploads = 0
        
        for entity in all_entities:
            entity_id = entity.id
            entity_name = entity.name
            
            print(f"\nProcessing entity '{entity_name}' (ID: {entity_id})")
            
            # Get survivorship data for this entity
            survivorship_data = get_entity_survivorship_data(session, entity_id)
            
            if not survivorship_data:
                print(f"  No survivorship groups found for entity '{entity_name}'")
                continue
            
            # Upload to Databricks
            if upload_to_databricks(session, entity_id, survivorship_data):
                successful_uploads += 1
            else:
                failed_uploads += 1
        
        print("\n" + "="*60)
        print("MIGRATION SUMMARY")
        print("="*60)
        print(f"Database migration: Completed")
        print(f"Databricks uploads: {successful_uploads} successful, {failed_uploads} failed/skipped")
        
    except Exception as e:
        session.rollback()
        print(f"Migration failed: {e}")
        raise
    finally:
        session.close()


def downgrade() -> None:
    """
    Downgrade survivorship rules to the old format.
    Note: This will lose information about specific aggregation functions.
    """
    bind = op.get_bind()
    session = Session(bind=bind)
    
    try:
        survivorship_query = text("""
            SELECT 
                survivorship_id,
                group_name,
                rules
            FROM survivorshiprule
            WHERE is_active = true
        """)
        
        survivorship_groups = session.execute(survivorship_query).fetchall()
        print(f"Found {len(survivorship_groups)} survivorship groups to downgrade")
        
        for group in survivorship_groups:
            survivorship_id = group.survivorship_id
            group_name = group.group_name
            rules = group.rules
            
            if isinstance(rules, str):
                rules = json.loads(rules)
            
            if not rules:
                continue
            
            print(f"\nDowngrading group '{group_name}' (ID: {survivorship_id})")
            
            downgraded_rules = []
            was_modified = False
            
            for rule in rules:
                new_rule = rule.copy()
                strategy = rule.get('strategy', '')
                strategy_rule = rule.get('strategyRule')
                
                if strategy == 'Aggregation':
                    if isinstance(strategy_rule, dict) and 'function' in strategy_rule:
                        # Revert to old format (null or empty)
                        new_rule['strategyRule'] = None
                        was_modified = True
                
                elif strategy == 'Frequency':
                    if strategy_rule in ('most_frequent', 'least_frequent'):
                        # Revert to old format (null or empty)
                        new_rule['strategyRule'] = None
                        was_modified = True
                
                downgraded_rules.append(new_rule)
            
            if was_modified:
                update_query = text("""
                    UPDATE survivorshiprule
                    SET rules = :rules
                    WHERE survivorship_id = :survivorship_id
                """)
                session.execute(
                    update_query,
                    {
                        'rules': json.dumps(downgraded_rules),
                        'survivorship_id': survivorship_id
                    }
                )
                print(f"  Downgraded group '{group_name}'")
        
        session.commit()
        print("\nDowngrade completed!")
        
    except Exception as e:
        session.rollback()
        print(f"Downgrade failed: {e}")
        raise
    finally:
        session.close()