"""update_metadata_json

Re-upload entity.json and model.json for all active entities with integration hubs.

This migration is needed because the Pydantic response schemas were updated:
- SurvivorshipRuleSchema now includes ignoreNull and aggregateUniqueOnly fields
- DatasetMappingSchema now includes use_cleaned_table field

Existing metadata JSON files on Databricks Volumes were serialized with the old schemas
and are missing these fields. This re-generates and re-uploads them.

Revision ID: 97b56221dd50
Revises: m8n9o0p1q2r3
Create Date: 2026-03-06 15:49:52.954322

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
revision: str = '97b56221dd50'
down_revision: Union[str, None] = 'm8n9o0p1q2r3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upload_json_to_databricks_volume(catalog_name: str, databricks_host: str, databricks_token: str,
                                      entity_id: int, file_name: str, data: dict) -> bool:
    """
    Upload JSON data to Databricks volume.

    Path format: /Volumes/{catalog}/metadata/metadata_files/entity_{id}_prod_{filename}
    """
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient(
            host=databricks_host,
            token=databricks_token
        )

        file_path = f"/Volumes/{catalog_name}/metadata/metadata_files/entity_{entity_id}_prod_{file_name}"

        json_data = json.dumps(data, default=str)
        file_content = BytesIO(json_data.encode('utf-8'))

        w.files.upload(file_path, file_content, overwrite=True)
        print(f"    Uploaded {file_name} to {file_path}")

        return True

    except Exception as e:
        print(f"    Warning: Failed to upload {file_name}: {e}")
        return False


def upgrade() -> None:
    """
    Re-upload entity.json and model.json to Databricks for all active entities
    with integration hubs, so metadata files reflect updated Pydantic schemas
    (ignoreNull, aggregateUniqueOnly, use_cleaned_table).
    """
    bind = op.get_bind()
    session = Session(bind=bind)

    try:
        print("\n" + "=" * 70)
        print("Uploading updated entity.json and model.json to Databricks")
        print("=" * 70)

        # Get Databricks credentials from environment
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
            from lakefusion_utility.services.entity_service import EntityService
            from lakefusion_utility.services.model_experiment_service import ExperimentService
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
                # entity_data now includes ignoreNull, aggregateUniqueOnly, use_cleaned_table
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
    No-op downgrade. Uploaded files on Databricks are not reverted.
    The old metadata files (without ignoreNull, aggregateUniqueOnly, use_cleaned_table)
    would need to be regenerated from the previous schema versions if a true rollback is needed.
    """
    print("Note: Metadata files uploaded to Databricks were not reverted.")
    print("Re-run the previous migration's upload step if a rollback is needed.")
