# Databricks notebook source
# MAGIC %md
# MAGIC # Version Generator
# MAGIC Creates a new migration version with metadata and template notebook

# COMMAND ----------

dbutils.widgets.text("title", "", "Migration Title")
dbutils.widgets.text("description", "", "Migration Description")

# COMMAND ----------

# MAGIC %run ../utils/execute_utils

# COMMAND ----------

setup_lakefusion_engine()

# COMMAND ----------

from lakefusion_core_engine.services.dbx_migration_service import *

# COMMAND ----------

def create_metadata_json(revision: str, down_revision: str | None, description: str, author: str) -> dict:
    """
    Create metadata dictionary
    """
    return {
        "revision": revision,
        "down_revision": down_revision,
        "description": description,
        "created_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "author": author
    }

# COMMAND ----------

def create_migration_notebook_content(revision: str, description: str) -> str:
    """
    Create migration notebook template content
    """
    return f'''# Databricks notebook source
dbutils.widgets.dropdown("function", "upgrade", ["upgrade", "downgrade"], "Function")
function = dbutils.widgets.get("function")

"""
Migration: {description}
Revision: {revision}
"""

def upgrade():
    """
    {description}
    """
    print(f"Applying migration: {description}")
    print("-" * 80)
    
    print(f"Custom Upgrade Logic")
    
    print("-" * 80)
    print(f"✓ Migration applied successfully")

def downgrade():
    """
    Revert {description}
    """
    print(f"Reverting migration: {description}")
    print("-" * 80)
    
    print(f"Custom Downgrade Logic")
    
    print("-" * 80)
    print(f"✓ Migration reverted successfully")
    if function == "downgrade":
    downgrade()
else:
    upgrade()
'''

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language

# COMMAND ----------

# Get widget values
title = dbutils.widgets.get("title").strip()
description = dbutils.widgets.get("description").strip()

# COMMAND ----------

# Validate inputs
if not title:
    raise ValueError("Migration title is required")
if not description:
    raise ValueError("Migration description is required")

print(f"Title: {title}")
print(f"Description: {description}")

# COMMAND ----------

# Initialize components
w = WorkspaceClient()
current_user = spark.sql("SELECT current_user() as user").collect()[0]['user']
print(f"Author: {current_user}")

# COMMAND ----------

# Find migrations base path and initialize utilities
migrations_base_path = find_migrations_path()
print(f"Migrations base path: {migrations_base_path}")

migration_utils = MigrationUtils(migrations_base_path)

# COMMAND ----------

# Get latest version (will be the down_revision)
latest_version_info = migration_utils.get_latest_version()
latest_version = latest_version_info['version'] if latest_version_info else None

if latest_version:
    print(f"Latest version (will be set as down_revision): {latest_version}")
else:
    print("No previous migrations found - this will be the first migration")

# COMMAND ----------

# Generate new revision ID
revision_id = generate_revision_id(title)
print(f"Generated revision ID: {revision_id}")

# COMMAND ----------

# Create version folder path
version_folder = migrations_base_path / revision_id
version_folder_str = str(version_folder)
print(f"Version folder: {version_folder_str}")

# COMMAND ----------

# Create metadata
metadata = create_metadata_json(revision_id, latest_version, description, current_user)
print("Metadata:")
print(json.dumps(metadata, indent=2))

# COMMAND ----------

migration_content = create_migration_notebook_content(revision=revision_id, description=description)
print("Migration content:")
print(migration_content)

print("Migration notebook content created")
print(f"Content length: {len(migration_content)} characters")

# COMMAND ----------

import base64

try:
    # Create version folder using local filesystem
    print(f"Creating version folder: {version_folder_str}")
    version_folder.mkdir(parents=True, exist_ok=True)
    print("✓ Version folder created")
    
    # Write metadata.json
    metadata_path = version_folder / "metadata.json"
    print(f"Writing metadata.json to: {metadata_path}")
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    print("✓ metadata.json created")
    
    # Write migration notebook using Workspace Client
    # Convert local path to workspace path
    workspace_path = version_folder_str.replace("/Workspace", "")
    migration_notebook_path = f"{workspace_path}/migration"
    
    print(f"Creating migration notebook at: {migration_notebook_path}")
    
    # Encode content as base64
    content_base64 = base64.b64encode(migration_content.encode('utf-8')).decode('utf-8')
    
    w.workspace.import_(
        path=migration_notebook_path,
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        content=content_base64,
        overwrite=True
    )
    print("✓ migration notebook created")
    
    print("\n" + "=" * 80)
    print("SUCCESS: New migration version created!")
    print("=" * 80)
    print(f"Revision: {revision_id}")
    print(f"Down Revision: {latest_version or 'None (first migration)'}")
    print(f"Description: {description}")
    print(f"Author: {current_user}")
    print(f"Location: {version_folder_str}")
    print(f"Notebook: {migration_notebook_path}")
    print("=" * 80)
    
except Exception as e:
    print(f"\n✗ ERROR: Failed to create migration version")
    print(f"Error: {str(e)}")
    import traceback
    traceback.print_exc()
    raise
