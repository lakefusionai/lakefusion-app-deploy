"""add_incremental_validation

Revision ID: a84402de100f
Revises: e5f6a7b8c9d0
Create Date: 2026-02-12 16:48:42.973514

"""
from typing import Sequence, Union
import os
import traceback

from alembic import op
from sqlalchemy.orm import Session
from sqlalchemy import text


# revision identifiers, used by Alembic.
revision: str = 'a84402de100f'
down_revision: Union[str, None] = 'e5f6a7b8c9d0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def compare_versions(version1: str, version2: str) -> int:
    """
    Compare two version strings.
    Returns:
        -1 if version1 < version2
         0 if version1 == version2
         1 if version1 > version2
    """
    if not version1:
        version1 = "1.0.0"
    if not version2:
        version2 = "1.0.0"

    v1_parts = [int(x) for x in version1.split('.')]
    v2_parts = [int(x) for x in version2.split('.')]

    while len(v1_parts) < len(v2_parts):
        v1_parts.append(0)
    while len(v2_parts) < len(v1_parts):
        v2_parts.append(0)

    for i in range(len(v1_parts)):
        if v1_parts[i] < v2_parts[i]:
            return -1
        elif v1_parts[i] > v2_parts[i]:
            return 1
    return 0


def add_validate_incremental_load_to_job(job_manager, job_id: str) -> bool:
    """
    Add Validate_Incremental_Load task to an existing Databricks job.

    Inserts the task between Check_Increments_Exists and Has_Inserts_Check,
    updating the dependency chain accordingly.

    Returns True if updated, False if skipped.
    """
    from databricks.sdk.service.jobs import (
        Task, NotebookTask, RunIf, Source, TaskDependency,
        JobSettings, JobAccessControlRequest, JobPermissionLevel
    )

    job_details = job_manager.get_job_info(job_id)

    if not job_details or not job_details.settings or not job_details.settings.tasks:
        print(f"    Warning: Job {job_id} has no tasks or settings")
        return False

    current_tasks = list(job_details.settings.tasks)
    current_task_keys = {task.task_key for task in current_tasks}

    # Skip if already has Validate_Incremental_Load
    if "Validate_Incremental_Load" in current_task_keys:
        print(f"    Skipping job {job_id}: Already has Validate_Incremental_Load")
        return False

    # Skip if no incremental tasks present
    if "Check_Increments_Exists" not in current_task_keys:
        print(f"    Skipping job {job_id}: No Check_Increments_Exists task (not an incremental pipeline)")
        return False

    # Skip if Has_Inserts_Check is missing
    if "Has_Inserts_Check" not in current_task_keys:
        print(f"    Skipping job {job_id}: No Has_Inserts_Check task found")
        return False

    # Derive the notebook path from the existing Check_Increments_Exists task
    check_increments_task = next(t for t in current_tasks if t.task_key == "Check_Increments_Exists")
    check_increments_notebook_path = check_increments_task.notebook_task.notebook_path

    # Replace the suffix to get Validate_Incremental_Load path
    # e.g. ".../incremental_load/Check_Increment_Exists" -> ".../incremental_load/Validate_Incremental_Load"
    validate_notebook_path = check_increments_notebook_path.rsplit("/", 1)[0] + "/Validate_Incremental_Load"

    # Copy base_parameters from the existing Check_Increments_Exists task
    base_parameters = check_increments_task.notebook_task.base_parameters

    # Create the new Validate_Incremental_Load task
    validate_task = Task(
        task_key="Validate_Incremental_Load",
        depends_on=[TaskDependency(task_key="Check_Increments_Exists")],
        run_if=RunIf.ALL_SUCCESS,
        notebook_task=NotebookTask(
            notebook_path=validate_notebook_path,
            base_parameters=base_parameters,
            source=Source.WORKSPACE,
        ),
        timeout_seconds=0,
    )

    # Build new task list:
    # 1. Insert Validate_Incremental_Load after Check_Increments_Exists
    # 2. Update Has_Inserts_Check to depend on Validate_Incremental_Load instead of Check_Increments_Exists
    new_tasks = []
    for task in current_tasks:
        if task.task_key == "Has_Inserts_Check":
            # Update Has_Inserts_Check dependency: Check_Increments_Exists -> Validate_Incremental_Load
            updated_depends_on = []
            if task.depends_on:
                for dep in task.depends_on:
                    if dep.task_key == "Check_Increments_Exists":
                        updated_depends_on.append(TaskDependency(task_key="Validate_Incremental_Load"))
                    else:
                        updated_depends_on.append(dep)
            else:
                updated_depends_on = [TaskDependency(task_key="Validate_Incremental_Load")]

            updated_has_inserts = Task(
                task_key=task.task_key,
                depends_on=updated_depends_on,
                condition_task=task.condition_task,
                timeout_seconds=task.timeout_seconds,
            )
            new_tasks.append(updated_has_inserts)
        else:
            new_tasks.append(task)

    # Insert Validate_Incremental_Load right after Check_Increments_Exists
    final_tasks = []
    for task in new_tasks:
        final_tasks.append(task)
        if task.task_key == "Check_Increments_Exists":
            final_tasks.append(validate_task)

    # Get resource tags
    try:
        from lakefusion_utility.utils.databricks_util import get_resource_tags
        tags = get_resource_tags()
        if tags is None:
            tags = {}
    except Exception:
        tags = {}

    # Create new job settings preserving all existing settings
    new_settings = JobSettings(
        name=job_details.settings.name,
        tasks=final_tasks,
        parameters=job_details.settings.parameters,
        schedule=job_details.settings.schedule,
        email_notifications=job_details.settings.email_notifications,
        run_as=job_details.settings.run_as,
        max_concurrent_runs=job_details.settings.max_concurrent_runs or 3,
        tags=tags
    )

    # Reset the job with new settings
    job_manager.w.jobs.reset(job_id=job_id, new_settings=new_settings)

    # Update permissions
    permissions = [
        JobAccessControlRequest(
            permission_level=JobPermissionLevel.CAN_MANAGE,
            group_name="users"
        )
    ]
    job_manager.w.jobs.update_permissions(job_id=job_id, access_control_list=permissions)

    print(f"    Successfully added Validate_Incremental_Load to job {job_id}")
    return True


def upgrade() -> None:
    """
    Add Validate_Incremental_Load task to all v4+ Integration Hub pipelines.

    This migration:
    1. Finds all active Integration Hub records with version >= 4.0.0
    2. For both merged and separate pipeline modes, updates the Databricks job (job_id_1)
       to include Validate_Incremental_Load between Check_Increments_Exists and Has_Inserts_Check
    """
    bind = op.get_bind()
    session = Session(bind=bind)

    try:
        print("\n" + "=" * 70)
        print("MIGRATION: Add Validate_Incremental_Load to Integration Hub Pipelines")
        print("=" * 70)

        # ============================================
        # STEP 1: Get Databricks credentials
        # ============================================

        databricks_host = os.environ.get('DATABRICKS_HOST')
        databricks_token = os.environ.get('LAKEFUSION_DATABRICKS_DAPI')

        if not databricks_host:
            print("\nError: DATABRICKS_HOST not set. Cannot proceed with migration.")
            print("Set DATABRICKS_HOST environment variable and re-run.")
            return

        if not databricks_token:
            print("\nError: LAKEFUSION_DATABRICKS_DAPI not set. Cannot proceed with migration.")
            print("Set LAKEFUSION_DATABRICKS_DAPI environment variable and re-run.")
            return

        print(f"\nUsing Databricks host: {databricks_host}")

        # ============================================
        # STEP 2: Initialize Databricks Job Manager
        # ============================================

        try:
            from lakefusion_utility.utils.databricks_util import DatabricksJobManager
            job_manager = DatabricksJobManager(databricks_token)
        except ImportError as e:
            print(f"\nError: Could not import DatabricksJobManager: {e}")
            return

        # ============================================
        # STEP 3: Query all active Integration Hubs
        # ============================================

        print("\n" + "-" * 50)
        print("Finding Integration Hubs with version >= 4.0.0")
        print("-" * 50)

        hubs_query = text("""
            SELECT
                id, task_name, entity_id, job_id_1, version, pipeline_mode
            FROM integration_hub
            WHERE is_active = true
            ORDER BY id
        """)

        hubs = session.execute(hubs_query).fetchall()
        print(f"Found {len(hubs)} active Integration Hubs")

        # Filter to v4+ only
        v4_hubs = []
        for hub in hubs:
            version = hub.version or "1.0.0"
            if compare_versions(version, "4.0.0") >= 0:
                v4_hubs.append(hub)
            else:
                print(f"  Skipping hub {hub.id} ({hub.task_name}): version {version} < 4.0.0")

        print(f"\nFound {len(v4_hubs)} Integration Hubs with version >= 4.0.0")

        if len(v4_hubs) == 0:
            print("No v4+ Integration Hubs to migrate.")
            return

        # ============================================
        # STEP 4: Process each v4+ hub
        # ============================================

        print("\n" + "-" * 50)
        print("Adding Validate_Incremental_Load to v4+ Pipelines")
        print("-" * 50)

        successful = 0
        failed = 0
        skipped = 0
        failed_hubs = []

        for hub in v4_hubs:
            hub_id = hub.id
            task_name = hub.task_name
            job_id_1 = hub.job_id_1
            version = hub.version
            pipeline_mode = hub.pipeline_mode or "separate"

            print(f"\nProcessing hub {hub_id}: {task_name}")
            print(f"  Version: {version}, Mode: {pipeline_mode}")
            print(f"  Job ID 1: {job_id_1}")

            if not job_id_1:
                print(f"  Skipping: No job_id_1")
                skipped += 1
                continue

            # Update job_id_1 (main pipeline for both merged and separate modes)
            try:
                success = add_validate_incremental_load_to_job(
                    job_manager=job_manager,
                    job_id=job_id_1,
                )

                if success:
                    successful += 1
                else:
                    skipped += 1

            except Exception as e:
                print(f"  Error processing hub {hub_id}: {e}")
                traceback.print_exc()
                failed += 1
                failed_hubs.append((hub_id, task_name, str(e)))

        # ============================================
        # STEP 5: Summary
        # ============================================

        print("\n" + "=" * 70)
        print("MIGRATION SUMMARY")
        print("=" * 70)
        print(f"Total v4+ Integration Hubs: {len(v4_hubs)}")
        print(f"Successfully updated: {successful}")
        print(f"Skipped (already has task or not applicable): {skipped}")
        print(f"Failed: {failed}")

        if failed_hubs:
            print(f"\nFailed hubs:")
            for hub_id, task_name, error in failed_hubs:
                print(f"  - Hub {hub_id} ({task_name}): {error}")

        print("\n" + "=" * 70)
        print("MIGRATION COMPLETED")
        print("=" * 70 + "\n")

    except Exception as e:
        session.rollback()
        print(f"\nMigration failed: {e}")
        traceback.print_exc()
        raise
    finally:
        session.close()


def downgrade() -> None:
    """
    Downgrade is not supported for this migration.

    Removing Validate_Incremental_Load from live pipelines requires
    manual intervention to ensure data integrity.
    """
    print("\n" + "=" * 70)
    print("DOWNGRADE NOTE")
    print("=" * 70)
    print("Automatic downgrade is not supported for this migration.")
    print("Removing Validate_Incremental_Load from pipelines requires manual intervention.")
    print("=" * 70 + "\n")
