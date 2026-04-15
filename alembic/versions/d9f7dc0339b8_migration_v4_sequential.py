"""migration_v4_sequential

Revision ID: d9f7dc0339b8
Revises: 7c2da5be97b2
Create Date: 2026-01-28 08:38:18.305467

"""
from typing import Sequence, Union
import os
import traceback
from datetime import datetime

from alembic import op
import sqlalchemy as sa
from sqlalchemy.orm import Session
from sqlalchemy import text


# revision identifiers, used by Alembic.
revision: str = 'd9f7dc0339b8'
down_revision: Union[str, None] = '7c2da5be97b2'
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
    
    # Pad shorter version with zeros
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


def build_sequential_incremental_tasks(notebook_path: str, parameters: dict):
    """
    Build the sequential incremental tasks for the integration/merged pipeline.
    Returns a list of Task objects.
    """
    from databricks.sdk.service.jobs import Task, NotebookTask, RunIf, Source, TaskDependency, ConditionTask, ConditionTaskOp
    
    return [
        Task(
            task_key="Check_Increments_Exists",
            depends_on=[TaskDependency(task_key="Is_Initial_Load_Check", outcome=False)],
            run_if=RunIf.ALL_SUCCESS,
            notebook_task=NotebookTask(
                notebook_path=f"{notebook_path}/src/integration-core/incremental_load/Check_Increment_Exists",
                base_parameters=parameters,
                source=Source.WORKSPACE,
            ),
            timeout_seconds=0,
        ),

        # ==================== INSERTS (Step 1) ====================
        Task(
            task_key="Has_Inserts_Check",
            depends_on=[TaskDependency(task_key="Check_Increments_Exists")],
            condition_task=ConditionTask(
                op=ConditionTaskOp.EQUAL_TO,
                left="{{tasks.Check_Increments_Exists.values.has_inserts}}",
                right="true",
            ),
            timeout_seconds=0,
        ),

        Task(
            task_key="Process_Incremental_Inserts",
            depends_on=[TaskDependency(task_key="Has_Inserts_Check", outcome=True)],
            run_if=RunIf.ALL_SUCCESS,
            notebook_task=NotebookTask(
                notebook_path=f"{notebook_path}/src/integration-core/incremental_load/Increment_Inserts_To_Unified",
                base_parameters=parameters,
                source=Source.WORKSPACE,
            ),
            timeout_seconds=0,
        ),

        # ==================== UPDATES (Step 2 - Sequential after Inserts) ====================
        Task(
            task_key="Has_Updates_Check",
            depends_on=[
                TaskDependency(task_key="Has_Inserts_Check", outcome=False),
                TaskDependency(task_key="Process_Incremental_Inserts"),
            ],
            run_if=RunIf.NONE_FAILED,
            condition_task=ConditionTask(
                op=ConditionTaskOp.EQUAL_TO,
                left="{{tasks.Check_Increments_Exists.values.has_updates}}",
                right="true",
            ),
            timeout_seconds=0,
        ),

        Task(
            task_key="Process_Incremental_Updates",
            depends_on=[TaskDependency(task_key="Has_Updates_Check", outcome=True)],
            run_if=RunIf.ALL_SUCCESS,
            notebook_task=NotebookTask(
                notebook_path=f"{notebook_path}/src/integration-core/incremental_load/Increment_Updates_To_Unified",
                base_parameters=parameters,
                source=Source.WORKSPACE,
            ),
            timeout_seconds=0,
        ),

        # ==================== DELETES (Step 3 - Sequential after Updates) ====================
        Task(
            task_key="Has_Deletes_Check",
            depends_on=[
                TaskDependency(task_key="Has_Updates_Check", outcome=False),
                TaskDependency(task_key="Process_Incremental_Updates"),
            ],
            run_if=RunIf.NONE_FAILED,
            condition_task=ConditionTask(
                op=ConditionTaskOp.EQUAL_TO,
                left="{{tasks.Check_Increments_Exists.values.has_deletes}}",
                right="true",
            ),
            timeout_seconds=0,
        ),

        Task(
            task_key="Process_Incremental_Deletes",
            depends_on=[TaskDependency(task_key="Has_Deletes_Check", outcome=True)],
            run_if=RunIf.ALL_SUCCESS,
            notebook_task=NotebookTask(
                notebook_path=f"{notebook_path}/src/integration-core/incremental_load/Increment_Deletes_To_Unified",
                base_parameters=parameters,
                source=Source.WORKSPACE,
            ),
            timeout_seconds=0,
        ),
    ]


def build_sequential_endpoints_mapping_task(notebook_path: str, parameters: dict):
    """
    Build the Endpoints_Mapping task with sequential dependencies.
    """
    from databricks.sdk.service.jobs import Task, NotebookTask, RunIf, Source, TaskDependency
    
    return Task(
        task_key="Endpoints_Mapping",
        depends_on=[
            TaskDependency(task_key="Load_Secondary_Sources"),
            TaskDependency(task_key="Has_Deletes_Check", outcome=False),
            TaskDependency(task_key="Process_Incremental_Deletes"),
        ],
        run_if=RunIf.NONE_FAILED,
        notebook_task=NotebookTask(
            notebook_path=f"{notebook_path}/src/integration-core/Endpoints_Mapping",
            base_parameters=parameters,
            source=Source.WORKSPACE,
        ),
        timeout_seconds=0,
    )


# Task keys that need to be replaced (the parallel incremental tasks)
PARALLEL_INCREMENTAL_TASK_KEYS = [
    "Check_Increments_Exists",
    "Has_Inserts_Check",
    "Process_Incremental_Inserts",
    "Has_Updates_Check",
    "Process_Incremental_Updates",
    "Has_Deletes_Check",
    "Process_Incremental_Deletes",
    "Endpoints_Mapping",
]


def update_job_to_sequential(job_manager, job_id: str, notebook_path: str, catalog_name: str, 
                              entity_id: int, to_emails: list) -> bool:
    """
    Update a Databricks job to use sequential incremental processing.
    
    Returns True if successful, False if skipped, raises exception on error.
    """
    from databricks.sdk.service.jobs import JobSettings, JobAccessControlRequest, JobPermissionLevel
    
    # Get current job details
    job_details = job_manager.get_job_info(job_id)
    
    if not job_details or not job_details.settings or not job_details.settings.tasks:
        print(f"    Warning: Job {job_id} has no tasks or settings")
        return False
    
    current_tasks = list(job_details.settings.tasks)
    current_task_keys = {task.task_key for task in current_tasks}
    
    # Check if Check_Increments_Exists exists (indicates v4 integration pipeline)
    if "Check_Increments_Exists" not in current_task_keys:
        print(f"    Skipping job {job_id}: No incremental tasks found (not a v4 integration pipeline)")
        return False
    
    # Check if already sequential by looking at Has_Updates_Check dependencies
    has_updates_task = next((t for t in current_tasks if t.task_key == "Has_Updates_Check"), None)
    if has_updates_task and has_updates_task.depends_on:
        depends_on_keys = [d.task_key for d in has_updates_task.depends_on]
        if "Has_Inserts_Check" in depends_on_keys or "Process_Incremental_Inserts" in depends_on_keys:
            print(f"    Skipping job {job_id}: Already using sequential incremental processing")
            return False
    
    # Build parameters
    parameters = {
        "entity_id": str(entity_id),
        "experiment_id": "prod",
        "to_emails": ','.join(to_emails) if to_emails else '',
        "processed_records": "",
        "is_integration_hub": "1",
        "catalog_name": str(catalog_name),
    }
    
    # Remove the old parallel incremental tasks
    new_tasks = [task for task in current_tasks if task.task_key not in PARALLEL_INCREMENTAL_TASK_KEYS]
    
    # Add the new sequential incremental tasks
    sequential_tasks = build_sequential_incremental_tasks(notebook_path, parameters)
    new_tasks.extend(sequential_tasks)
    
    # Add the new Endpoints_Mapping task
    endpoints_task = build_sequential_endpoints_mapping_task(notebook_path, parameters)
    new_tasks.append(endpoints_task)
    
    # Get resource tags
    try:
        from lakefusion_utility.utils.databricks_util import get_resource_tags
        tags = get_resource_tags()
        if tags is None:
            tags = {}
    except:
        tags = {}
    
    # Create new job settings preserving existing settings
    new_settings = JobSettings(
        name=job_details.settings.name,
        tasks=new_tasks,
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
    
    print(f"    Successfully updated job {job_id} to sequential incremental processing")
    return True


def upgrade() -> None:
    """
    Migrate all v4+ Integration Hub pipelines from parallel to sequential incremental processing.
    
    This migration:
    1. Finds all Integration Hub records with version >= 4.0.0
    2. Updates their Databricks jobs (job_id_1) to use sequential incremental processing
    """
    bind = op.get_bind()
    session = Session(bind=bind)
    
    try:
        print("\n" + "=" * 70)
        print("MIGRATION: V4 Pipelines - Parallel to Sequential Incremental Processing")
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
        # STEP 2: Get required config values
        # ============================================
        
        # Get catalog name
        catalog_result = session.execute(
            text("SELECT config_value FROM db_config_properties WHERE config_key = 'catalog_name'")
        ).fetchone()
        
        if not catalog_result:
            print("\nError: catalog_name not found in db_config_properties.")
            return
        
        catalog_name = catalog_result.config_value
        print(f"Using catalog: {catalog_name}")
        
        # Get notebook path
        notebook_result = session.execute(
            text("SELECT config_value FROM db_config_properties WHERE config_key = 'notebook_path'")
        ).fetchone()
        
        if not notebook_result:
            print("\nError: notebook_path not found in db_config_properties.")
            return
        
        notebook_path = notebook_result.config_value
        print(f"Using notebook path: {notebook_path}")
        
        # ============================================
        # STEP 3: Initialize Databricks Job Manager
        # ============================================
        
        try:
            from lakefusion_utility.utils.databricks_util import DatabricksJobManager
            job_manager = DatabricksJobManager(databricks_token)
        except ImportError as e:
            print(f"\nError: Could not import DatabricksJobManager: {e}")
            return
        
        # ============================================
        # STEP 4: Query all v4+ Integration Hubs
        # ============================================
        
        print("\n" + "-" * 50)
        print("Finding Integration Hubs with version >= 4.0.0")
        print("-" * 50)
        
        # Query all active integration hubs
        hubs_query = text("""
            SELECT 
                id, task_name, entity_id, modelid, to_emails,
                job_id_1, job_id_2, version, pipeline_mode,
                is_active
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
        # STEP 5: Process each v4+ hub
        # ============================================
        
        print("\n" + "-" * 50)
        print("Processing v4+ Integration Hubs")
        print("-" * 50)
        
        successful = 0
        failed = 0
        skipped = 0
        failed_hubs = []
        
        for hub in v4_hubs:
            hub_id = hub.id
            task_name = hub.task_name
            entity_id = hub.entity_id
            job_id_1 = hub.job_id_1
            job_id_2 = hub.job_id_2
            version = hub.version
            pipeline_mode = hub.pipeline_mode or "separate"
            to_emails = hub.to_emails if hub.to_emails else []
            
            # Parse to_emails if it's a string
            if isinstance(to_emails, str):
                try:
                    import json
                    to_emails = json.loads(to_emails)
                except:
                    to_emails = []
            
            print(f"\nProcessing hub {hub_id}: {task_name}")
            print(f"  Version: {version}, Mode: {pipeline_mode}")
            print(f"  Job ID 1: {job_id_1}, Job ID 2: {job_id_2}")
            
            # Skip if no job_id_1
            if not job_id_1:
                print(f"  Skipping: No job_id_1")
                skipped += 1
                continue
            
            # Update job_id_1 (main pipeline - either separate integration or merged)
            try:
                success = update_job_to_sequential(
                    job_manager=job_manager,
                    job_id=job_id_1,
                    notebook_path=notebook_path,
                    catalog_name=catalog_name,
                    entity_id=entity_id,
                    to_emails=to_emails
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
        # STEP 6: Summary
        # ============================================
        
        print("\n" + "=" * 70)
        print("MIGRATION SUMMARY")
        print("=" * 70)
        print(f"Total v4+ Integration Hubs: {len(v4_hubs)}")
        print(f"Successfully migrated: {successful}")
        print(f"Skipped (already sequential or no incremental tasks): {skipped}")
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
    
    Reverting to parallel incremental processing is NOT recommended
    as it causes concurrency issues with Delta Lake.
    """
    print("\n" + "=" * 70)
    print("DOWNGRADE NOTE")
    print("=" * 70)
    print("Automatic downgrade is not supported for this migration.")
    print("Reverting to parallel processing is NOT recommended")
    print("as it causes concurrency issues with Delta Lake.")
    print("=" * 70 + "\n")
