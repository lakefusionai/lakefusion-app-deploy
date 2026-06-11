# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 0
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

# MAGIC %run ../utils/taskvalues_enum

# COMMAND ----------

# Define job parameter keys
job_params = [
    "catalog_name",
    "entity_id", 
    "experiment_id",
    "is_integration_hub",
    "processed_records",
    "to_emails"
]

# COMMAND ----------

for key in TaskValueKey:
    dbutils.widgets.text(key.value, "")

for key in job_params:
    dbutils.widgets.text(key, "")

# COMMAND ----------

# Create parameters dictionary
params = {
}

# Add job parameters (these are already available as widgets from the job)
for param in job_params:
    try:
        params[param] = dbutils.widgets.get(param)
    except Exception as e:
        print(f"Warning: Could not get job parameter {param}: {e}")
        params[param] = ""

# Add all task value keys as individual parameters
for key in TaskValueKey:
    params[key.value] = dbutils.widgets.get(key.value)

# COMMAND ----------

for key in TaskValueKey:
    debug_value = dbutils.widgets.get(key.value)
    try:
        params[key.value] = dbutils.jobs.taskValues.get(
            "Parse_Entity_Model_JSON", 
            key.value, 
            debugValue=debug_value
        )
    except Exception as e:
        print(f"Warning: Could not retrieve task value for {key.value}: {e}")
        params[key.value] = debug_value

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")

# COMMAND ----------
catalog_name = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "catalog_name", debugValue=dbutils.widgets.get("catalog_name"))
experiment_id = dbutils.widgets.get("experiment_id")


# COMMAND ----------

# Get is_initial_load parameter
is_initial_load = dbutils.jobs.taskValues.get(
    taskKey="Check_Master_Exists",
    key=TaskValueKey.IS_INITIAL_LOAD.value,
    default="false"
)

print(f"Is initial load: {is_initial_load}")

# COMMAND ----------

# MAGIC %run ../utils/execute_utils



# COMMAND ----------

from lakefusion_core_engine.services.dbx_migration_service import *

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Create runner
runner = MigrationRunner(spark, params.get('catalog_name'), params.get('entity_id'), params, dbutils)

# COMMAND ----------

latest_version_info = runner.utils.get_latest_version()

# COMMAND ----------

if is_initial_load:
    print("[INFO] Initial load mode detected")
    print("[INFO] Will create alembic initial load entry without executing migrations")

    # Get the latest version
    latest_version_info = runner.utils.get_latest_version()

    if latest_version_info:
        print(f"\n[INFO] Latest version found: {latest_version_info['version']}")
        print(f"[INFO] Description: {latest_version_info['description']}")

        # Create alembic initial load entry
        result = runner.create_alembic_initial_load_entry(latest_version_info)

        if result['success']:
            print(f"\n✓ Alembic initial load entry created - entity marked at version: {result['revision']}")
        else:
            print(f"\n✗ Failed to create alembic initial load entry: {result['error']}")
            raise Exception(f"Initial load entry creation failed: {result['error']}")
    else:
        print("[WARNING] No migrations found to record")

else:
    print("[INFO] Regular migration mode")
    print("[INFO] Will execute all pending migrations")

    # Get pending migrations
    pending_migrations = runner.get_pending_migrations()

    if not pending_migrations:
        print("\n✓ No pending migrations - entity is up to date")
    else:
        # Run each pending migration
        for pending_migration in pending_migrations:
            version_folder = pending_migration.get("folder_path")
            print(f"\n[INFO] Running migration: {version_folder}")

            result = runner.run_upgrade_with_rollback(version_folder)

            if not result['upgrade_success']:
                print(f"\n✗ Migration failed, stopping execution")
                raise Exception(f"Migration failed: {result['error']}")

        print(f"\n✓ All migrations completed successfully")
