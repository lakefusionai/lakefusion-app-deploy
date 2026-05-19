# Migration Utilities

A robust migration management system for Databricks that provides version control for database schema changes, similar to Alembic but designed specifically for Databricks environments.

## Overview

This migration system consists of three main components:

1. **VersionGenerator**: Creates new migration versions with metadata and template notebooks
2. **Run Migrations**: Executes pending migrations for entities (manual or automated)
3. **MigrationRunner**: Core execution engine with fault tolerance and rollback support
4. **MigrationUtils**: Utility functions for discovering and managing available migrations

All core classes are packaged in the `lakefusion_core_engine` wheel package at:
```
lakefusion_core_engine/lakefusion_core_engine/services/dbx_migration_service.py
```

## Directory Structure
```
dbx_pipeline_artifacts/
├── src/
│   └── migration-utilities/
│       ├── README.md (this file)
│       ├── VersionGenerator (notebook)      - Create new migrations
│       └── Run Migrations (notebook)         - Execute pending migrations
├── versions/                                 # Your migration versions go here
│   ├── <revision_id>_<title>/
│   │   ├── metadata.json
│   │   └── migration (notebook)
│   └── ...
└── sample_versions/                          # Sample migration examples for reference
    ├── example_create_table/
    ├── example_add_column/
    └── example_data_transformation/
```

## Installation

The migration utilities are available through the `lakefusion_core_engine` wheel package:
```python
# Install the wheel package (if not already installed)
%pip install /path/to/lakefusion_core_engine-*.whl

# Import the classes
from lakefusion_core_engine.services.dbx_migration_service import (
    MigrationRunner,
    MigrationUtils,
    find_migrations_path,
    generate_revision_id
)
```

## Getting Started

### 1. Generate a New Migration Version

Use the **VersionGenerator** notebook to create a new migration:

1. Open `src/migration-utilities/VersionGenerator` notebook
2. Fill in the widgets:
   - **Migration Title**: Short name (e.g., "add_customer_email_column")
   - **Migration Description**: Detailed description (e.g., "Add email column to customers table in silver schema")
3. Run all cells

The generator will:
- Generate a unique revision ID (format: `<hash>_<sanitized_title>`)
- Create a new folder under `versions/`
- Create `metadata.json` with revision tracking
- Create a template `migration` notebook with upgrade/downgrade functions
- Automatically link to the previous migration (down_revision)

**Example output:**
```
SUCCESS: New migration version created!
================================================================================
Revision: 69f98523486c_add_customer_email_column
Down Revision: 0ea5a919a9ee_test_migration
Description: Add email column to customers table in silver schema
Author: nbharadwaj@lakefusion.ai
Location: /Workspace/.../versions/69f98523486c_add_customer_email_column
Notebook: /Users/.../versions/69f98523486c_add_customer_email_column/migration
================================================================================
```

### 2. Implement Migration Logic

After generation, edit the migration notebook to implement your actual migration logic:
```python
def upgrade():
    """
    Add email column to customers table in silver schema
    """
    print(f"Applying migration: Add email column to customers table")
    print("-" * 80)
    
    # Your custom upgrade logic
    spark.sql("""
        ALTER TABLE lakefusion_ai.silver.customers 
        ADD COLUMN email STRING COMMENT 'Customer email address'
    """)
    
    print("-" * 80)
    print(f"✓ Migration applied successfully")

def downgrade():
    """
    Revert: Remove email column from customers table
    """
    print(f"Reverting migration: Remove email column")
    print("-" * 80)
    
    # Your custom downgrade logic
    spark.sql("""
        ALTER TABLE lakefusion_ai.silver.customers 
        DROP COLUMN email
    """)
    
    print("-" * 80)
    print(f"✓ Migration reverted successfully")
```

### 3. Run Migrations

You have two options for running migrations:

#### Option A: Using the Run Migrations Notebook (Recommended)

**For Manual Execution:**

1. Open the [Run Migrations notebook](https://github.com/lakefusionai/lakefusion-universe/blob/develop/4.0.0/dbx_pipeline_artifacts/src/migration-utilities/Run%20Migrations.ipynb)
2. Set the required widgets:
   - `catalog_name`: Your catalog (e.g., "lakefusion_ai")
   - `entity_id`: Entity identifier (e.g., "109")
   - `entity`: Entity name (e.g., "test_separate_dedup_pipeline")
   - `experiment_id`: Environment (e.g., "prod")
3. Run all cells

The notebook will:
- Auto-discover all pending migrations
- Execute each migration with automatic rollback on failure
- Track migration history in the `alembic_versions` table
- Provide detailed logging and error reporting

**For Integration Pipeline:**

Add the Run Migrations notebook as a task in your Databricks workflow:
```yaml
tasks:
  - task_key: parse_entity_model
    notebook_task:
      notebook_path: /src/integration-core/Parse Entity & Model JSON
    # ... other config
  
  - task_key: run_migrations
    depends_on:
      - task_key: parse_entity_model
    notebook_task:
      notebook_path: /src/migration-utilities/Run Migrations
    base_parameters:
      catalog_name: "{{job.parameters.catalog_name}}"
      entity_id: "{{job.parameters.entity_id}}"
      experiment_id: "{{job.parameters.experiment_id}}"
    # ... other config
  
  - task_key: continue_pipeline
    depends_on:
      - task_key: run_migrations
    # ... rest of pipeline
```

**How it works:**

The Run Migrations notebook:
1. Creates widgets for all task values and job parameters
2. Retrieves task values from upstream tasks (e.g., `Parse_Entity_Model_JSON`)
3. Collects all parameters (job params + task values) into a `params` dict
4. Initializes `MigrationRunner` with the collected parameters
5. Discovers pending migrations for the entity
6. Executes each pending migration with `run_upgrade_with_rollback()`
7. All parameters are automatically forwarded to individual migration notebooks

**Key Features:**
- ✅ Automatic parameter collection from job and upstream tasks
- ✅ All 27 task value keys supported via `TaskValueKey` enum
- ✅ Automatic rollback on migration failure
- ✅ Works in both manual and automated pipeline contexts
- ✅ No code changes needed - just add as a task

#### Option B: Using MigrationRunner Programmatically

For custom workflows or testing, you can use MigrationRunner directly:
```python
# Databricks notebook source
from lakefusion_core_engine.services.dbx_migration_service import MigrationRunner

# COMMAND ----------

# Prepare parameters to pass to migration notebooks
params = {
    "catalog_name": "lakefusion_ai",
    "entity_id": "109",
    "entity": "test_separate_dedup_pipeline",
    "experiment_id": "prod",
    # Add any other parameters your migrations need
}

# Initialize runner for a specific entity
runner = MigrationRunner(
    spark=spark,
    catalog="lakefusion_ai",
    entity_id="109",
    params=params,      # Parameters passed to migration notebooks
    dbutils=dbutils     # Pass dbutils from notebook context
)

# COMMAND ----------

# Check what migrations need to be applied
pending = runner.get_pending_migrations()
print(f"Found {len(pending)} pending migrations")

# COMMAND ----------

# Apply all pending migrations
for migration in pending:
    print(f"\nApplying: {migration['version']}")
    result = runner.run_upgrade_with_rollback(
        version_folder=migration['folder_path']
    )
    
    if result['upgrade_success']:
        print(f"✓ Success in {result['upgrade_time']:.2f}s")
    elif result['rollback_success']:
        print(f"⚠ Migration failed but rollback succeeded")
        break
    else:
        print(f"✗ Migration and rollback both failed")
        break
```

## Sample Migrations

Check the `sample_versions/` directory for reference implementations:

- **example_create_table**: Shows how to create and drop tables
- **example_add_column**: Demonstrates adding/removing columns
- **example_data_transformation**: Complex data transformations and migrations

Each sample includes:
- Complete `metadata.json` structure
- Working upgrade and downgrade functions
- Best practices and patterns

## Migration Metadata Structure

Each migration version contains a `metadata.json` file:
```json
{
  "revision": "69f98523486c_add_customer_email_column",
  "down_revision": "0ea5a919a9ee_test_migration",
  "description": "Add email column to customers table in silver schema",
  "created_at": "2024-11-20T10:00:00Z",
  "author": "nbharadwaj@lakefusion.ai"
}
```

- **revision**: Unique identifier for this migration
- **down_revision**: Previous migration in the chain (null for first migration)
- **description**: Human-readable description
- **created_at**: Timestamp when migration was created
- **author**: User who created the migration

## Version Tracking

Migrations are tracked per entity in the `<catalog>.metadata.alembic_versions` table:

| Column | Type | Description |
|--------|------|-------------|
| entity_id | STRING | Entity identifier |
| version_num | STRING | Migration revision ID |
| applied_at | TIMESTAMP | When migration was applied |
| applied_by | STRING | User who applied migration |
| execution_time_seconds | DOUBLE | Migration execution time |
| migration_name | STRING | Migration description |

## Advanced Usage

### Check Migration Status
```python
from lakefusion_core_engine.services.dbx_migration_service import MigrationRunner

runner = MigrationRunner(spark, "lakefusion_ai", "109", params, dbutils)

# Get currently applied versions for an entity
applied = runner.get_applied_versions()
print(f"Applied versions: {applied}")

# Get latest available migration
latest = runner.utils.get_latest_version()
print(f"Latest migration: {latest['version']}")

# Check if entity is up to date
if latest['version'] in applied:
    print("✓ Entity is up to date")
else:
    print("⚠ Entity needs migration")
```

### Run Specific Migration
```python
# Run a specific migration by folder path
result = runner.run_upgrade(
    version_folder="/Workspace/.../versions/69f98523486c_add_email",
    record_success=True,
    timeout_seconds=1200  # 20 minutes
)
```

### Downgrade Migration
```python
# Revert a specific migration
result = runner.run_downgrade(
    version_folder="/Workspace/.../versions/69f98523486c_add_email",
    remove_record=True
)
```

### List All Available Migrations
```python
from lakefusion_core_engine.services.dbx_migration_service import (
    MigrationUtils,
    find_migrations_path
)

migrations_path = find_migrations_path()
utils = MigrationUtils(migrations_path)

all_migrations = utils.get_available_migrations()
for m in all_migrations:
    print(f"{m['version']}: {m['description']}")
```

### Passing Parameters to Migrations

The `params` dictionary passed to `MigrationRunner` is forwarded to all migration notebooks:
```python
params = {
    "catalog_name": "lakefusion_ai",
    "entity_id": "109",
    "entity": "test_separate_dedup_pipeline",
    "experiment_id": "prod",
    "is_integration_hub": "1",
    "processed_records": "1000",
    # Custom parameters for your migrations
    "source_table": "raw.customers",
    "target_table": "silver.customers"
}

runner = MigrationRunner(spark, catalog, entity_id, params, dbutils)
```

These parameters are accessible in your migration notebooks via widgets:
```python
# In your migration notebook
catalog_name = dbutils.widgets.get("catalog_name")
entity = dbutils.widgets.get("entity")
source_table = dbutils.widgets.get("source_table")
target_table = dbutils.widgets.get("target_table")
```

**Note:** The Run Migrations notebook automatically collects and forwards all job parameters and task values from upstream tasks, so you don't need to manually configure parameters when using it in a pipeline.

## Integration Pipeline Workflow
```
┌──────────────────────┐
│ Parse Entity Model   │  Parse entity configuration, set task values
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│  Run Migrations      │  Auto-collect params, execute pending migrations
│  (New Task)          │  • Reads task values from upstream
└──────────┬───────────┘  • Forwards all params to migrations
           │              • Applies with auto-rollback
           ▼
┌──────────────────────┐
│ Continue Pipeline    │  Rest of entity processing
│ (Existing Tasks)     │
└──────────────────────┘
```

**Key Points:**
- Run Migrations task should be added **after** entity parsing/configuration tasks
- Run Migrations task should be added **before** main entity processing tasks
- All task values and job parameters are automatically propagated
- Migration failures will halt the pipeline (by design, for safety)

## Best Practices

### 1. Migration Design
- **Keep migrations small and focused**: One migration = one logical change
- **Always implement both upgrade and downgrade**: Ensure rollback capability
- **Test migrations thoroughly**: Run upgrade followed by downgrade in dev
- **Use idempotent operations**: Migrations should be safe to run multiple times

### 2. Migration Content
- **Add explicit error handling**: Catch and report errors clearly
- **Log important steps**: Use print statements for debugging
- **Validate preconditions**: Check that tables/columns exist before operating
- **Handle data carefully**: Back up data before destructive operations

### 3. Version Management
- **Never modify applied migrations**: Create new migrations instead
- **Keep migration history linear**: Avoid branching when possible
- **Document complex migrations**: Add comments explaining the "why"
- **Use descriptive titles**: Make it easy to understand what each migration does

### 4. Testing Strategy
```python
# Test in development first
dev_params = {
    "catalog_name": "lakefusion_ai_dev",
    "entity_id": "109",
    "entity": "test_entity",
    # ... other params
}

dev_runner = MigrationRunner(spark, "lakefusion_ai_dev", "109", dev_params, dbutils)

# Apply migration
result = dev_runner.run_upgrade(migration_path)

# Verify results
spark.sql("DESCRIBE lakefusion_ai_dev.silver.customers").show()

# Test rollback
result = dev_runner.run_downgrade(migration_path)

# Verify rollback worked
spark.sql("DESCRIBE lakefusion_ai_dev.silver.customers").show()
```

### 5. Pipeline Integration
- **Use Run Migrations notebook**: Don't write custom migration execution code
- **Place after configuration tasks**: Ensure task values are set upstream
- **Place before processing tasks**: Apply schema changes before data operations
- **Monitor execution**: Check pipeline logs for migration status

## Core Components

### Run Migrations Notebook

**Purpose:** Centralized migration execution for both manual and automated workflows

**Features:**
- Automatic widget creation for all parameters
- Task value retrieval from upstream tasks
- Parameter collection and forwarding
- Pending migration discovery
- Automatic rollback on failure
- Comprehensive logging

**Usage Contexts:**
1. **Manual Testing**: Open notebook, set widgets, run cells
2. **Pipeline Integration**: Add as workflow task with job parameters
3. **Development**: Test migrations before production deployment

**Links:**
- [Run Migrations Notebook](https://github.com/lakefusionai/lakefusion-universe/blob/develop/4.0.0/dbx_pipeline_artifacts/src/migration-utilities/Run%20Migrations.ipynb)

### MigrationRunner

Main class for executing migrations on specific entities.

**Constructor:**
```python
MigrationRunner(
    spark: SparkSession,
    catalog: str,
    entity_id: str,
    params: dict,
    dbutils: any
)
```

**Key Methods:**
- `get_applied_versions()` → set: Get applied versions for entity
- `get_pending_migrations()` → List[Dict]: Get migrations to apply
- `run_upgrade(version_folder, record_success, timeout_seconds)` → Dict: Execute upgrade
- `run_downgrade(version_folder, remove_record, timeout_seconds)` → Dict: Execute downgrade
- `run_upgrade_with_rollback(version_folder, timeout_seconds)` → Dict: Upgrade with auto-rollback

### MigrationUtils

Utility class for migration discovery and management.

**Constructor:**
```python
MigrationUtils(migrations_base_path: Path)
```

**Key Methods:**
- `get_available_migrations()` → List[Dict]: Scan and return all migrations
- `get_latest_version()` → Optional[Dict]: Find the head revision

### Helper Functions

- `find_migrations_path()` → Path: Auto-discover versions directory
- `generate_revision_id(title: str)` → str: Generate unique revision ID

## Troubleshooting

### Migration Fails During Execution

1. Check the error message in the result dictionary
2. Verify the migration logic is correct
3. Ensure entity has necessary permissions
4. Check if tables/schemas exist
```python
result = runner.run_upgrade(migration_path)
if not result['success']:
    print(f"Error: {result['error']}")
    print(f"Failed after {result['execution_time']:.2f}s")
```

### Run Migrations Notebook Fails to Get Task Values

If task values are not being retrieved in the pipeline:

1. Verify upstream task name matches: `"Parse_Entity_Model_JSON"`
2. Check that task values are being set with `TaskValueKey` enum
3. Ensure Run Migrations task depends on the parsing task
4. Check job parameters are configured correctly

### Migration Shows as Pending But Was Already Applied

This can happen if the tracking table record is missing:
```python
# Manually record migration (use with caution)
metadata = {'revision': 'your_revision_id', 'description': 'Your description'}
runner._record_success(metadata, execution_time=0.0)
```

### Cannot Find Migrations Path

Ensure you're running from within the correct directory structure:
```python
from lakefusion_core_engine.services.dbx_migration_service import find_migrations_path
import os

print(f"Current directory: {os.getcwd()}")
try:
    path = find_migrations_path()
    print(f"Found migrations at: {path}")
except FileNotFoundError as e:
    print(f"Error: {e}")
```

### dbutils Not Defined Error

Make sure to pass `dbutils` when creating MigrationRunner:
```python
# WRONG - will fail
runner = MigrationRunner(spark, catalog, entity_id, params)

# CORRECT - pass dbutils from notebook
runner = MigrationRunner(spark, catalog, entity_id, params, dbutils)
```

### Rollback Failed

If automatic rollback fails:

1. Review the downgrade function implementation
2. Manually run the downgrade in a notebook
3. Clean up tracking table if needed:
```python
# Manually remove failed migration record
spark.sql(f"""
    DELETE FROM lakefusion_ai.metadata.alembic_versions
    WHERE entity_id = '109' AND version_num = 'failed_revision_id'
""")
```

## Migration Workflow Example

Complete workflow from creation to application:
```python
# 1. Create new migration
# Open VersionGenerator notebook
# Enter: title="add_status_column", description="Add status column to orders"
# Run all cells

# 2. Implement migration logic
# Edit generated migration notebook
# Implement upgrade() and downgrade() functions

# 3. Test manually using Run Migrations notebook
# Open Run Migrations notebook
# Set widgets: catalog_name, entity_id, entity, experiment_id
# Run all cells - verify success

# 4. Test rollback
# Manually run downgrade for the migration
# Verify changes were reverted

# 5. Add to pipeline
# Add Run Migrations task after Parse Entity Model task
# Configure job parameters
# Deploy pipeline

# 6. Monitor production
# Check alembic_versions table for applied migrations
# Review pipeline logs for migration execution
```

## Support

For issues or questions:
1. Check `sample_versions/` for working examples
2. Review the [Run Migrations notebook](https://github.com/lakefusionai/lakefusion-universe/blob/develop/4.0.0/dbx_pipeline_artifacts/src/migration-utilities/Run%20Migrations.ipynb) for usage patterns
3. Review migration logs for error details
4. Verify entity permissions and catalog access
5. Check the wheel package installation
6. Contact the data engineering team

## Future Enhancements

Planned features:
- [ ] Migration dependency visualization
- [ ] Batch migration application across multiple entities
- [ ] Migration dry-run mode
- [ ] Migration performance metrics
- [ ] Automated migration testing framework
- [ ] Migration history and audit reports
- [ ] Integration with CI/CD pipelines
