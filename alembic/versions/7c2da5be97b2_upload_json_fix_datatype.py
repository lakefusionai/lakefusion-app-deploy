"""upload_json_fix_datatype

Revision ID: 7c2da5be97b2
Revises: 15e4f262c71e
Create Date: 2026-01-22 13:21:40.003696

"""
from typing import Sequence, Union
import os
import traceback

from alembic import op
import sqlalchemy as sa
from sqlalchemy.orm import Session
from sqlalchemy import text
import json
from io import BytesIO


# revision identifiers, used by Alembic.
revision: str = '7c2da5be97b2'
down_revision: Union[str, None] = '15e4f262c71e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Complete type mappings - lowercase to uppercase Spark types
TYPE_MAPPINGS = {
    # Integer types
    'bigint': 'BIGINT',
    'int': 'INT',
    'integer': 'INT',
    'smallint': 'SMALLINT',
    'tinyint': 'TINYINT',
    'long': 'BIGINT',
    
    # Floating point types
    'float': 'FLOAT',
    'double': 'DOUBLE',
    'double precision': 'DOUBLE',
    'real': 'FLOAT',
    'decimal': 'DECIMAL',
    'numeric': 'DECIMAL',
    'number': 'DOUBLE',
    
    # String types
    'string': 'STRING',
    'varchar': 'STRING',
    'text': 'STRING',
    'char': 'STRING',
    
    # Date/Time types
    'date': 'DATE',
    'timestamp': 'TIMESTAMP',
    'timestamp without time zone': 'TIMESTAMP',
    'timestamp with time zone': 'TIMESTAMP',
    'datetime': 'TIMESTAMP',
    
    # Boolean
    'boolean': 'BOOLEAN',
    'bool': 'BOOLEAN',
}

REVERSE_TYPE_MAPPINGS = {
    'BIGINT': 'bigint',
    'INT': 'integer',
    'SMALLINT': 'smallint',
    'TINYINT': 'tinyint',
    'FLOAT': 'float',
    'DOUBLE': 'double',
    'DECIMAL': 'decimal',
    'STRING': 'varchar',
    'DATE': 'date',
    'TIMESTAMP': 'timestamp',
    'BOOLEAN': 'boolean',
}


def upload_json_to_databricks_volume(catalog_name: str, databricks_host: str, databricks_token: str, 
                                      entity_id: int, file_name: str, data: dict) -> bool:
    """
    Upload JSON data to Databricks volume.
    
    Path format: /Volumes/{catalog}/metadata/metadata_files/entity_{id}_prod_{filename}
    """
    try:
        from databricks.sdk import WorkspaceClient
        
        # Initialize Databricks client with explicit host and token
        w = WorkspaceClient(
            host=databricks_host,
            token=databricks_token
        )
        
        # Build file path: /Volumes/{catalog}/metadata/metadata_files/entity_{id}_prod_{filename}
        file_path = f"/Volumes/{catalog_name}/metadata/metadata_files/entity_{entity_id}_prod_{file_name}"
        
        # Convert to JSON
        json_data = json.dumps(data, default=str)
        file_content = BytesIO(json_data.encode('utf-8'))
        
        # Upload using files API for Unity Catalog Volumes
        w.files.upload(file_path, file_content, overwrite=True)
        print(f"    Uploaded {file_name} to {file_path}")
        
        return True
        
    except Exception as e:
        print(f"    Warning: Failed to upload {file_name}: {e}")
        return False


def upgrade() -> None:
    """
    1. Fix entity attribute types - convert lowercase to uppercase Spark types
    2. Upload entity.json and model.json to Databricks for all active entities with integration hubs
    """
    bind = op.get_bind()
    session = Session(bind=bind)
    
    try:
        # ============================================
        # STEP 1: Fix entity attribute types
        # ============================================
        print("\n" + "=" * 70)
        print("STEP 1: Fixing entity attribute types (lowercase -> uppercase)")
        print("=" * 70)
        
        # Log current state before migration
        result = session.execute(
            text("SELECT type, COUNT(*) as cnt FROM entityattributes GROUP BY type ORDER BY type")
        )
        print("\nBEFORE - Current type distribution:")
        for row in result:
            print(f"  {row.type}: {row.cnt}")
        
        # Apply each type mapping
        total_updated = 0
        
        for old_type, new_type in TYPE_MAPPINGS.items():
            # Use LOWER and TRIM for robust matching
            result = session.execute(
                text("""
                    UPDATE entityattributes 
                    SET type = :new_type 
                    WHERE LOWER(TRIM(type)) = LOWER(TRIM(:old_type))
                """),
                {"old_type": old_type, "new_type": new_type}
            )
            
            if result.rowcount > 0:
                print(f"  Updated {result.rowcount} rows: '{old_type}' -> '{new_type}'")
                total_updated += result.rowcount
        
        # Commit the type updates
        session.commit()
        
        # Log final state after migration
        result = session.execute(
            text("SELECT type, COUNT(*) as cnt FROM entityattributes GROUP BY type ORDER BY type")
        )
        print("\nAFTER - Updated type distribution:")
        for row in result:
            print(f"  {row.type}: {row.cnt}")
        print(f"\nTotal rows updated: {total_updated}")
        
        # ============================================
        # STEP 2: Upload entity.json and model.json to Databricks
        # ============================================
        print("\n" + "=" * 70)
        print("STEP 2: Uploading entity.json and model.json to Databricks")
        print("=" * 70)
        
        # Get Databricks credentials from environment (same as databricks_sync_service.py)
        databricks_host = os.environ.get('DATABRICKS_HOST')
        databricks_token = os.environ.get('LAKEFUSION_DATABRICKS_DAPI')
        
        if not databricks_host:
            print("\nWarning: DATABRICKS_HOST not set. Skipping Databricks upload.")
            return
            
        if not databricks_token:
            print("\nWarning: LAKEFUSION_DATABRICKS_DAPI not set. Skipping Databricks upload.")
            print("Set LAKEFUSION_DATABRICKS_DAPI environment variable and re-run if needed.")
            return
        
        print(f"\nUsing Databricks host: {databricks_host}")
        
        # Get catalog name from db_config_properties
        catalog_query = text("""
            SELECT config_value 
            FROM db_config_properties 
            WHERE config_key = 'catalog_name'
        """)
        catalog_result = session.execute(catalog_query).fetchone()
        
        if not catalog_result:
            print("\nWarning: catalog_name not found in db_config_properties. Skipping upload.")
            return
        
        catalog_name = catalog_result.config_value
        print(f"Using catalog: {catalog_name}")
        
        # Import services
        try:
            from lakefusion_utility.services.model_experiment_service import ExperimentService
            from lakefusion_utility.services.entity_service import EntityService
        except ImportError as e:
            print(f"\nWarning: Could not import required services: {e}")
            print("Skipping Databricks upload.")
            return
        
        # Get all active entities that have an active integration hub
        entities_query = text("""
            SELECT DISTINCT 
                e.id as entity_id, 
                e.name as entity_name,
                ih.modelid
            FROM entity e
            JOIN integration_hub ih ON e.id = ih.entity_id
            WHERE e.is_active = true AND ih.is_active = true
            ORDER BY e.id
        """)
        entities = session.execute(entities_query).fetchall()
        print(f"Found {len(entities)} active entities with integration hubs\n")
        
        if len(entities) == 0:
            print("No entities to process. Skipping upload.")
            return
        
        # Initialize services - reuse the alembic session
        entity_service = EntityService(session)
        experiment_service = ExperimentService(session)
        
        entity_uploads = 0
        model_uploads = 0
        failed_entities = []
        
        for row in entities:
            entity_id = row.entity_id
            entity_name = row.entity_name
            model_id = row.modelid
            
            print(f"Processing entity '{entity_name}' (ID: {entity_id}, Model ID: {model_id})")
            
            try:
                # Get entity and model data using existing services
                entity_data = entity_service.get_full_entity_by_id(entity_id).dict()
                model_data = experiment_service.get_model(model_id).dict()
                
                # Upload files
                if upload_json_to_databricks_volume(
                    catalog_name, databricks_host, databricks_token,
                    entity_id, 'entity.json', entity_data
                ):
                    entity_uploads += 1
                
                if upload_json_to_databricks_volume(
                    catalog_name, databricks_host, databricks_token,
                    entity_id, 'model.json', model_data
                ):
                    model_uploads += 1
                    
            except Exception as e:
                print(f"    Error processing entity: {e}")
                failed_entities.append((entity_id, entity_name, str(e)))
        
        print("\n" + "=" * 70)
        print("UPLOAD SUMMARY")
        print("=" * 70)
        print(f"Total entities processed: {len(entities)}")
        print(f"Entity JSON files uploaded: {entity_uploads}")
        print(f"Model JSON files uploaded: {model_uploads}")
        
        if failed_entities:
            print(f"\nFailed entities ({len(failed_entities)}):")
            for eid, ename, err in failed_entities:
                print(f"  - {ename} (ID: {eid}): {err}")
        
        print("\n" + "=" * 70)
        print("MIGRATION COMPLETED SUCCESSFULLY")
        print("=" * 70 + "\n")
        
    except Exception as e:
        session.rollback()
        print(f"\nMigration failed: {e}")
        print(traceback.format_exc())
        raise
    finally:
        session.close()


def downgrade() -> None:
    """
    Downgrade: Revert Spark-compatible types back to lowercase types.
    Note: This doesn't remove uploaded files from Databricks.
    """
    bind = op.get_bind()
    session = Session(bind=bind)
    
    try:
        print("\n" + "=" * 70)
        print("Downgrading entity attribute types (uppercase -> lowercase)")
        print("=" * 70)
        
        total_updated = 0
        
        for spark_type, pg_type in REVERSE_TYPE_MAPPINGS.items():
            result = session.execute(
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
        
        session.commit()
        
        # Log final state
        result = session.execute(
            text("SELECT type, COUNT(*) as cnt FROM entityattributes GROUP BY type ORDER BY type")
        )
        print("\nAFTER DOWNGRADE - Type distribution:")
        for row in result:
            print(f"  {row.type}: {row.cnt}")
        
        print(f"\nTotal rows reverted: {total_updated}")
        print("\nNote: Files uploaded to Databricks were not removed.")
        
    except Exception as e:
        session.rollback()
        print(f"Downgrade failed: {e}")
        raise
    finally:
        session.close()
