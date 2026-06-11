"""inject_compute_embeddings_into_43_jobs

For every active Integration Hub that was created on a pre-4.3.0 release and
is now upgrading into 4.3.0, surgically inject the new ``Compute_Embeddings``
task between ``Endpoints_Mapping`` and ``Entity_Matching_Vector_Search``.

For merged-mode pipelines, also inject ``Compute_Embeddings_Golden`` between
``Process_Unified_Dedup_Table`` and ``Golden_Entity_Matching_Vector_Search``,
and rewrite the golden VS task to depend on both compute-embeddings tasks
(mirroring the live builder at integration_hub_service.py:1158-1159).

Mirrors the surgical-patch pattern from a84402de100f_add_incremental_validation:
``jobs.reset(job_id, new_settings)`` keeps the same ``job_id`` (no orphans, no
DB writes), and is idempotent on re-run. All live ``JobSettings`` fields are
copied through to the new settings (schedule, emails, run_as, tags,
job_clusters, git_source, webhook_notifications, trigger, etc.); only
``tasks`` is mutated and ``tags`` overlaid with the current resource-tag set.

Without this migration, customers upgrading from 4.2.x to 4.3.0 hit a runtime
crash in Entity_Matching_Vector_Search when they switch to a precomputed
model_experiment, because the notebook reads ``embedding_dim`` from a
Compute_Embeddings task that doesn't exist in their pre-4.3.0 DAG.

Revision ID: s5t6u7v8w9x0
Revises: r3s4t5u6v7w8
Create Date: 2026-05-20 14:00:00.000000

"""
from typing import Sequence, Union
import os
import re
import traceback

from alembic import op
from sqlalchemy.orm import Session
from sqlalchemy import text


revision: str = 's5t6u7v8w9x0'
down_revision: Union[str, None] = 'r3s4t5u6v7w8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def compare_versions(version1: str, version2: str) -> int:
    """Compare two version strings. Returns -1, 0, or 1.

    Tolerates pre-release suffixes like '4.2.0-beta1' or '4.3.0-rc2' by taking
    only the leading digits of each dot-segment — we don't try to order
    pre-releases relative to their base. For cohort-filtering purposes here
    '4.2.0-rc2' compares equal to '4.2.0', which is fine.
    """
    if not version1:
        version1 = "1.0.0"
    if not version2:
        version2 = "1.0.0"

    def _coerce_part(part: str) -> int:
        m = re.match(r"\d+", part)
        return int(m.group(0)) if m else 0

    v1_parts = [_coerce_part(x) for x in version1.split('.')]
    v2_parts = [_coerce_part(x) for x in version2.split('.')]

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


def _clone_task(task, **overrides):
    """Recreate a Task object preserving fields and overriding selected ones.

    databricks.sdk's Task dataclass doesn't have a convenient ``replace`` —
    rebuild explicitly and drop None values that aren't valid kwargs.
    """
    from databricks.sdk.service.jobs import Task
    fields = {
        "task_key": task.task_key,
        "depends_on": task.depends_on,
        "run_if": task.run_if,
        "notebook_task": task.notebook_task,
        "condition_task": task.condition_task,
        "timeout_seconds": task.timeout_seconds,
    }
    fields.update(overrides)
    return Task(**{k: v for k, v in fields.items() if v is not None})


def add_compute_embeddings_to_job(job_manager, job_id: str) -> bool:
    """Inject Compute_Embeddings (and optionally Compute_Embeddings_Golden) into
    an existing pre-4.3.0 Integration Hub job.

    Returns True if patched, False if skipped (idempotent / not applicable).
    Raises on unexpected error.
    """
    from databricks.sdk.service.jobs import (
        Task, NotebookTask, RunIf, Source, TaskDependency,
        JobSettings, JobAccessControlRequest, JobPermissionLevel,
    )

    job_details = job_manager.get_job_info(job_id)
    if not job_details or not job_details.settings or not job_details.settings.tasks:
        print(f"    Warning: Job {job_id} has no tasks or settings")
        return False

    current_tasks = list(job_details.settings.tasks)
    current_task_keys = {t.task_key for t in current_tasks}

    # Idempotency guard 1: not an integration-core pipeline.
    if "Entity_Matching_Vector_Search" not in current_task_keys:
        print(f"    Skipping job {job_id}: No Entity_Matching_Vector_Search task")
        return False
    if "Endpoints_Mapping" not in current_task_keys:
        print(f"    Skipping job {job_id}: No Endpoints_Mapping anchor task")
        return False

    # Idempotency guard 2: legacy maven-core jobs use a notebook that doesn't
    # support precomputed mode, so injecting Compute_Embeddings would be
    # pointless and might confuse downstream tasks.
    normal_vs_task = next(t for t in current_tasks if t.task_key == "Entity_Matching_Vector_Search")
    vs_notebook_path = normal_vs_task.notebook_task.notebook_path if normal_vs_task.notebook_task else ""
    if "/maven-core/" in vs_notebook_path or "/integration-core/" not in vs_notebook_path:
        print(f"    Skipping job {job_id}: Not an integration-core pipeline (vs_notebook={vs_notebook_path})")
        return False

    # Shape detection: merged-mode runs both normal AND golden dedup in one job.
    has_golden_vs = "Golden_Entity_Matching_Vector_Search" in current_task_keys
    has_unified_dedup = "Process_Unified_Dedup_Table" in current_task_keys
    is_merged_shape = has_golden_vs and has_unified_dedup

    # Idempotency guard 3: already patched?
    needs_normal = "Compute_Embeddings" not in current_task_keys
    needs_golden = is_merged_shape and "Compute_Embeddings_Golden" not in current_task_keys
    if not needs_normal and not needs_golden:
        print(f"    Skipping job {job_id}: Compute_Embeddings already present")
        return False

    # Derive the new task's notebook path from a sibling. Endpoints_Mapping
    # lives directly under the integration-core notebook folder — same level as
    # Compute_Embeddings — so a simple suffix swap gives the right path
    # regardless of the customer's workspace prefix.
    endpoints_task = next(t for t in current_tasks if t.task_key == "Endpoints_Mapping")
    endpoints_notebook_path = endpoints_task.notebook_task.notebook_path
    compute_embeddings_path = endpoints_notebook_path.rsplit("/", 1)[0] + "/Compute_Embeddings"
    base_parameters = endpoints_task.notebook_task.base_parameters

    # Build the new task(s).
    new_compute_normal_task = None
    new_compute_golden_task = None

    if needs_normal:
        new_compute_normal_task = Task(
            task_key="Compute_Embeddings",
            depends_on=[TaskDependency(task_key="Endpoints_Mapping")],
            run_if=RunIf.NONE_FAILED,
            notebook_task=NotebookTask(
                notebook_path=compute_embeddings_path,
                base_parameters=base_parameters,
                source=Source.WORKSPACE,
            ),
            timeout_seconds=0,
        )

    if needs_golden:
        new_compute_golden_task = Task(
            task_key="Compute_Embeddings_Golden",
            depends_on=[TaskDependency(task_key="Process_Unified_Dedup_Table")],
            run_if=RunIf.NONE_FAILED,
            notebook_task=NotebookTask(
                notebook_path=compute_embeddings_path,
                base_parameters=base_parameters,
                source=Source.WORKSPACE,
            ),
            timeout_seconds=0,
        )

    # Walk current tasks. Rewrite VS-task dependencies and insert new tasks
    # right after their anchors. Preserve original task ordering for everything
    # else so the workspace UI rendering stays familiar.
    new_tasks = []
    for task in current_tasks:
        if task.task_key == "Entity_Matching_Vector_Search" and needs_normal:
            new_tasks.append(_clone_task(
                task,
                depends_on=[TaskDependency(task_key="Compute_Embeddings")],
            ))
        elif task.task_key == "Golden_Entity_Matching_Vector_Search" and needs_golden:
            # In the merged pipeline the golden VS task waits for BOTH compute
            # tasks. Matches the live builder at integration_hub_service.py:1158-1159.
            new_tasks.append(_clone_task(
                task,
                depends_on=[
                    TaskDependency(task_key="Compute_Embeddings_Golden"),
                    TaskDependency(task_key="Compute_Embeddings"),
                ],
            ))
        else:
            new_tasks.append(task)

        if task.task_key == "Endpoints_Mapping" and new_compute_normal_task is not None:
            new_tasks.append(new_compute_normal_task)
        if task.task_key == "Process_Unified_Dedup_Table" and new_compute_golden_task is not None:
            new_tasks.append(new_compute_golden_task)

    # Resolve resource tags before reset. Fall back to whatever the live job had.
    try:
        from lakefusion_utility.utils.databricks_util import get_resource_tags
        tags = get_resource_tags() or job_details.settings.tags or {}
    except Exception:
        tags = job_details.settings.tags or {}

    # Copy every live JobSettings field and overlay only what we want to change.
    # Enumerating fields explicitly (rather than name-by-name) avoids silently
    # dropping settings like git_source, job_clusters, webhook_notifications,
    # trigger, continuous, queue, timeout_seconds, etc. when jobs.reset replaces
    # the entire settings object.
    import dataclasses
    live_fields = {
        f.name: getattr(job_details.settings, f.name)
        for f in dataclasses.fields(job_details.settings)
    }
    live_fields["tasks"] = new_tasks
    live_fields["tags"] = tags
    if not live_fields.get("max_concurrent_runs"):
        live_fields["max_concurrent_runs"] = 3
    # Drop None values — the SDK rejects unexpected None kwargs on some fields.
    new_settings = JobSettings(**{k: v for k, v in live_fields.items() if v is not None})

    job_manager.w.jobs.reset(job_id=job_id, new_settings=new_settings)

    permissions = [JobAccessControlRequest(
        permission_level=JobPermissionLevel.CAN_MANAGE,
        group_name="users",
    )]
    job_manager.w.jobs.update_permissions(job_id=job_id, access_control_list=permissions)

    patched_what = []
    if needs_normal:
        patched_what.append("Compute_Embeddings")
    if needs_golden:
        patched_what.append("Compute_Embeddings_Golden")
    print(f"    Successfully injected {', '.join(patched_what)} into job {job_id}")
    return True


def upgrade() -> None:
    """Inject Compute_Embeddings into existing 4.0.0 <= version < 4.3.0 Integration Hub jobs."""
    bind = op.get_bind()
    session = Session(bind=bind)

    try:
        print("\n" + "=" * 70)
        print("MIGRATION: Inject Compute_Embeddings into pre-4.3.0 Integration Hub jobs")
        print("=" * 70)

        databricks_host = os.environ.get('DATABRICKS_HOST')
        databricks_token = os.environ.get('LAKEFUSION_DATABRICKS_DAPI')

        if not databricks_host:
            print("\nDATABRICKS_HOST not set — skipping Databricks DAG injection.")
            print("The schema portion of this migration is a no-op; existing customers will")
            print("hit the Compute_Embeddings error if they upgrade to precomputed mode.")
            print("Re-run with DATABRICKS_HOST + LAKEFUSION_DATABRICKS_DAPI set, OR have")
            print("customers click the per-task 'Update Job' button in the Integration Hub UI.")
            return

        if not databricks_token:
            print("\nLAKEFUSION_DATABRICKS_DAPI not set — skipping Databricks DAG injection.")
            return

        print(f"\nUsing Databricks host: {databricks_host}")

        try:
            from lakefusion_utility.utils.databricks_util import DatabricksJobManager
            job_manager = DatabricksJobManager(databricks_token)
        except ImportError as e:
            print(f"\nError: Could not import DatabricksJobManager: {e}")
            return

        print("\n" + "-" * 50)
        print("Finding pre-4.3.0 Integration Hubs")
        print("-" * 50)

        hubs_query = text("""
            SELECT
                id, task_name, entity_id, job_id_1, job_id_2, version, pipeline_mode
            FROM integration_hub
            WHERE is_active = TRUE
            ORDER BY id
        """)
        hubs = session.execute(hubs_query).fetchall()
        print(f"Found {len(hubs)} active Integration Hubs")

        # Filter to the cohort that needs patching: 4.0.0 <= version < 4.3.0.
        # Pre-4.0.0 hubs use the maven-core notebook (no precomputed mode anyway).
        # 4.3.0+ hubs were built on current code (already have Compute_Embeddings).
        target_hubs = []
        for hub in hubs:
            version = hub.version or "1.0.0"
            if compare_versions(version, "4.0.0") < 0:
                print(f"  Skipping hub {hub.id} ({hub.task_name}): version {version} < 4.0.0 (maven-core pipeline)")
                continue
            if compare_versions(version, "4.3.0") >= 0:
                print(f"  Skipping hub {hub.id} ({hub.task_name}): version {version} >= 4.3.0 (DAG already current)")
                continue
            target_hubs.append(hub)

        print(f"\nFound {len(target_hubs)} Integration Hubs in the 4.0.0 <= version < 4.3.0 cohort")

        if len(target_hubs) == 0:
            print("Nothing to patch.")
            return

        print("\n" + "-" * 50)
        print("Injecting Compute_Embeddings")
        print("-" * 50)

        successful = 0
        skipped = 0
        failed = 0
        failed_hubs = []

        for hub in target_hubs:
            hub_id = hub.id
            task_name = hub.task_name
            job_ids = [
                (label, job_id)
                for label, job_id in (("job_id_1", hub.job_id_1), ("job_id_2", hub.job_id_2))
                if job_id
            ]

            print(f"\nProcessing hub {hub_id}: {task_name} (version={hub.version}, mode={hub.pipeline_mode})")
            if not job_ids:
                print(f"  Skipping: no job_id_1 or job_id_2 set")
                skipped += 1
                continue

            for label, job_id in job_ids:
                print(f"  {label} = {job_id}")
                try:
                    if add_compute_embeddings_to_job(job_manager=job_manager, job_id=str(job_id)):
                        successful += 1
                    else:
                        skipped += 1
                except Exception as e:
                    print(f"    Error patching {label}={job_id}: {e}")
                    traceback.print_exc()
                    failed += 1
                    failed_hubs.append((hub_id, task_name, label, job_id, str(e)))

        print("\n" + "=" * 70)
        print("MIGRATION SUMMARY")
        print("=" * 70)
        print(f"Target Integration Hubs: {len(target_hubs)}")
        print(f"Jobs successfully injected: {successful}")
        print(f"Jobs skipped (already patched / not applicable): {skipped}")
        print(f"Jobs failed: {failed}")

        if failed_hubs:
            print("\nFailed jobs (customers can click the 'Update Job' button to recover):")
            for hub_id, task_name, label, job_id, err in failed_hubs:
                print(f"  - Hub {hub_id} ({task_name}) {label}={job_id}: {err}")

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
    """Automatic downgrade is not supported.

    Removing Compute_Embeddings from running pipelines would re-introduce the
    crash this migration fixes (for precomputed mode) without restoring the
    pre-4.3.0 VS notebook that would tolerate its absence. Manual intervention
    via the Integration Hub UI is the recovery path.
    """
    print("\n" + "=" * 70)
    print("DOWNGRADE NOTE")
    print("=" * 70)
    print("Automatic downgrade is not supported for this migration.")
    print("Removing Compute_Embeddings from live pipelines requires manual intervention.")
    print("=" * 70 + "\n")
