from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timezone
from sqlalchemy.exc import DBAPIError, OperationalError

from app.lakefusion_cron_service.utils.app_db import db_context
from lakefusion_utility.utils.logging_utils import get_logger

logger = get_logger(__name__)

CRON_HTTP_TIMEOUT_SECONDS = 60
SUMMARY_KEYS = ("polled", "updated", "unchanged", "errored")


def _ms_to_iso(ms: int | None) -> str | None:
    """Convert a millisecond UTC epoch to an ISO-8601 string, or None."""
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def _create_timed_workspace_client(token: str = ""):
    from lakefusion_utility.utils.databricks_util import _create_workspace_client

    # Delegate auth entirely to the canonical helper so PAT-vs-App-SP logic
    # stays in one place and we never trigger the "multiple auth methods" error
    # on Databricks Apps.  We then inject the cron-specific HTTP timeout on the
    # already-constructed config object.
    client = _create_workspace_client(token or None)
    client.config.http_timeout_seconds = CRON_HTTP_TIMEOUT_SECONDS
    return client


def _monitoring_service(token: str):
    from lakefusion_utility.services.profiling_tasks import MonitoringService

    service = MonitoringService(token=token)
    # Override .w with a timed client.  MonitoringService.__init__ sets self.w
    # once via _create_workspace_client and never reassigns it internally —
    # verified against lakefusion_utility wheel v1.0.0 (SQLAlchemy 2.0.34).
    # The assertion below acts as an upgrade-time canary: if a future wheel
    # version adds lazy re-init of self.w, this will raise immediately rather
    # than silently losing the timeout.
    service.w = _create_timed_workspace_client(token)
    assert service.w is not None, (
        "MonitoringService.w was not accepted — re-verify wheel compatibility "
        "after a lakefusion_utility upgrade (see _monitoring_service in cron_jobs.py)."
    )
    return service


def _databricks_job_manager(token: str):
    from lakefusion_utility.utils.databricks_util import DatabricksJobManager

    manager = DatabricksJobManager(token)
    # Same pattern as _monitoring_service — DatabricksJobManager sets self.w
    # once in __init__ and never reassigns it.  Assertion is the upgrade canary.
    manager.w = _create_timed_workspace_client(token)
    assert manager.w is not None, (
        "DatabricksJobManager.w was not accepted — re-verify wheel compatibility "
        "after a lakefusion_utility upgrade (see _databricks_job_manager in cron_jobs.py)."
    )
    return manager


def _format_summary(result):
    if isinstance(result, dict):
        if all(key in result for key in SUMMARY_KEYS):
            return (
                f"{result['polled']} polled, {result['updated']} updated, "
                f"{result['unchanged']} unchanged, {result['errored']} errored"
            )
        return ", ".join(f"{key}={value}" for key, value in result.items())
    if isinstance(result, (str, int, float)):
        return result
    return None


def _job_summary(polled=0, updated=0, unchanged=0, errored=0):
    return {
        "polled": polled,
        "updated": updated,
        "unchanged": unchanged,
        "errored": errored,
    }


def _log_item_failure(job_id: str, item_id: str, exc: Exception):
    logger.error(
        f"job {job_id} item {item_id} failed: {type(exc).__name__}: {exc}",
        exc_info=True,
    )


def check_monitor_status_job(db):
    from lakefusion_utility.services.profiling_tasks import (
        ProfilingTaskService,
        lakefusion_databricks_dapi,
    )

    task_service = ProfilingTaskService(db)
    pending_tasks = task_service.fetch_tasks_with_pending_status()
    summary = _job_summary(polled=len(pending_tasks))

    # Hoist client construction outside the loop — WorkspaceClient is expensive
    # to instantiate and holds an HTTP session that should be reused per tick.
    monitoring_service = _monitoring_service(lakefusion_databricks_dapi)

    for task in pending_tasks:
        before_status = (task.monitor_info or {}).get("status")
        try:
            latest_monitor_info = monitoring_service.get_monitor(task.dataset.path)
            latest_monitor_dict = latest_monitor_info.as_dict()
            task_service.update_monitor_info(task, latest_monitor_dict)
            after_status = (latest_monitor_dict or {}).get("status")
            if after_status != before_status:
                summary["updated"] += 1
            else:
                summary["unchanged"] += 1
        except Exception as exc:
            summary["errored"] += 1
            _log_item_failure("check_monitor_status", f"task_id={task.id}", exc)

    return summary


def check_refresh_status_job(db):
    from lakefusion_utility.services.profiling_tasks import (
        ProfilingTaskService,
        lakefusion_databricks_dapi,
    )

    task_service = ProfilingTaskService(db)
    pending_tasks = task_service.fetch_tasks_with_pending_refresh()
    summary = _job_summary(polled=len(pending_tasks))

    # Hoist client construction outside the loop — see check_monitor_status_job.
    monitoring_service = _monitoring_service(lakefusion_databricks_dapi)

    for task in pending_tasks:
        before_status = (task.refresh_info or {}).get("status")
        try:
            latest_refresh_info = monitoring_service.get_refresh_status(task.dataset.path)
            refresh_info = {
                "refresh_id": latest_refresh_info.refresh_id,
                # Guard against state=None (e.g. a refresh still pending SDK-side):
                # hasattr(None, "value") is False, so the unguarded original code
                # would produce str(None) == "None" as the status string.
                "status": (
                    latest_refresh_info.state.value
                    if latest_refresh_info.state and hasattr(latest_refresh_info.state, "value")
                    else (str(latest_refresh_info.state) if latest_refresh_info.state else None)
                ),
                "started_at": _ms_to_iso(latest_refresh_info.start_time_ms),
                "completed_at": _ms_to_iso(latest_refresh_info.end_time_ms),
                "message": latest_refresh_info.message,
            }
            task_service.update_refresh_info(task, refresh_info)
            after_status = refresh_info.get("status")
            if after_status != before_status:
                summary["updated"] += 1
            else:
                summary["unchanged"] += 1
        except Exception as exc:
            summary["errored"] += 1
            _log_item_failure("check_refresh_status", f"task_id={task.id}", exc)

    return summary


def check_job_monitor_status_job(db):
    from lakefusion_utility.services.databricks_model_run import (
        Monitor_Model_Databricks_Job,
        lakefusion_databricks_dapi,
    )

    monitor_service = Monitor_Model_Databricks_Job(db)
    pending_job_run_tasks = monitor_service.get_pending_tasks()
    unique_job_runs = {}
    for job_run in pending_job_run_tasks:
        unique_job_runs.setdefault(job_run.job_run_id, job_run)

    summary = _job_summary(polled=len(unique_job_runs))

    # Hoist client construction outside the loop — WorkspaceClient is expensive
    # to instantiate and holds an HTTP session that should be reused per tick.
    job_status = _databricks_job_manager(lakefusion_databricks_dapi)

    for job_run_id, job_run in unique_job_runs.items():
        before_status = getattr(job_run, "experiment_status", None)
        try:
            latest_job_run_info = job_status.get_job_run_status(job_run_id)
            new_experiment_status = monitor_service.get_experiment_status(latest_job_run_info, job_run_id)
            monitor_service.update_job_run_status(job_run_id, latest_job_run_info, new_experiment_status)
            if new_experiment_status != before_status:
                summary["updated"] += 1
            else:
                summary["unchanged"] += 1
        except Exception as exc:
            summary["errored"] += 1
            _log_item_failure("check_job_monitor_status", f"job_run_id={job_run_id}", exc)

    return summary


def check_entity_search_monitor_status_job(db):
    from lakefusion_utility.services.entity_search_service import (
        Monitor_Entity_Search_Databricks_Job,
        lakefusion_databricks_dapi,
    )

    monitor_service = Monitor_Entity_Search_Databricks_Job(db)
    pending_job_run_tasks = monitor_service.get_pending_tasks()
    unique_job_runs = {}
    for job_run in pending_job_run_tasks:
        unique_job_runs.setdefault(job_run.job_run_id, job_run)

    summary = _job_summary(polled=len(unique_job_runs))

    # Hoist client construction outside the loop — see check_job_monitor_status_job.
    job_status = _databricks_job_manager(lakefusion_databricks_dapi)

    for job_run_id, job_run in unique_job_runs.items():
        before_status = getattr(job_run, "status", None)
        try:
            latest_job_run_info = job_status.get_job_run_status(job_run_id)
            if latest_job_run_info is None:
                latest_job_run_info = job_run.job_run_status
            new_status = monitor_service.get_experiment_status(latest_job_run_info, job_run_id)
            monitor_service.update_job_run_status(job_run_id, latest_job_run_info, new_status)
            if new_status != before_status:
                summary["updated"] += 1
            else:
                summary["unchanged"] += 1
        except Exception as exc:
            summary["errored"] += 1
            _log_item_failure("check_entity_search_monitor_status", f"job_run_id={job_run_id}", exc)

    return summary


def process_query_completed_stewardship_job(db):
    from lakefusion_utility.services.entity_search_service import (
        EntitySearchService,
        Model_Databricks_Entity_Search_Job,
        StewardshipJobStatus,
    )
    from lakefusion_utility.utils.databricks_util import generate_pat, get_app_sp_token

    # NOTE: raw query lives here rather than in EntitySearchService because the
    # external wheel does not expose a `get_query_completed_records()` method.
    # TODO: move into the service layer once the wheel is updated.
    query_completed_records = db.query(Model_Databricks_Entity_Search_Job).filter(
        Model_Databricks_Entity_Search_Job.status == StewardshipJobStatus.QUERY_COMPLETED.value,
        Model_Databricks_Entity_Search_Job.execution_type == "query",
    ).all()
    summary = _job_summary(polled=len(query_completed_records))

    if not query_completed_records:
        return summary

    entity_groups = {}
    for record in query_completed_records:
        entity_groups.setdefault(record.entity_id, []).append(record)

    token = get_app_sp_token() or generate_pat()
    if not token:
        summary["unchanged"] = len(query_completed_records)
        logger.warning("No Databricks token available (SP or PAT) - cannot submit stewardship jobs")
        return summary

    service = EntitySearchService(db)
    # Hoist client construction outside the loop — see check_monitor_status_job.
    job_service = _databricks_job_manager(token)

    for entity_id, records in entity_groups.items():
        try:
            # Two-entry job_parameters mirrors the original utility wheel's
            # process_query_completed_stewardship function (entity_search_service.py).
            # The first dict targets the per-entity crosswalk task; the second is a
            # catalog-wide sweep task that intentionally omits entity_id.
            job_parameters = [
                {"entity_id": str(entity_id), "experiment_id": "prod", "catalog_name": service.catalog_name},
                {"catalog_name": service.catalog_name, "experiment_id": "prod"},
            ]
            # _create_pm_crosswalk_tasks is a private method; tracked for
            # promotion to a public API once the wheel is updated (see TODO above).
            tasks_list = service._create_pm_crosswalk_tasks(job_parameters)
            job_create_response = job_service.create_and_submit_job("Stewardship_PM_Crosswalk", tasks_list)
            run_id = job_create_response.run_id

            for record in records:
                record.job_run_id = str(run_id)
                record.status = StewardshipJobStatus.SUBMITTED.value

            db.commit()
            summary["updated"] += len(records)
        except Exception as exc:
            db.rollback()
            summary["errored"] += len(records)
            _log_item_failure("process_query_completed_stewardship", f"entity_id={entity_id}", exc)

    return summary


def check_schema_evolution_status_job(db):
    from lakefusion_utility.models.schema_evolution import SchemaEvolutionJob
    from lakefusion_utility.services.schema_evolution_service import SchemaEvolutionService
    # Import from profiling_tasks — the canonical source used by all other polling
    # jobs in this module.  All service modules define this from the same env var
    # (LAKEFUSION_DATABRICKS_DAPI) so the value is identical regardless of origin.
    from lakefusion_utility.services.profiling_tasks import lakefusion_databricks_dapi

    running_jobs = db.query(SchemaEvolutionJob).filter(
        SchemaEvolutionJob.status == "RUNNING"
    ).all()
    summary = _job_summary(polled=len(running_jobs))

    if not running_jobs:
        return summary

    service = SchemaEvolutionService(db)
    # Hoist client construction outside the loop — see check_monitor_status_job.
    timed_client = _create_timed_workspace_client(lakefusion_databricks_dapi)
    for job in running_jobs:
        before_status = job.status  # always "RUNNING" — filter above guarantees it
        try:
            # before_status is always "RUNNING" (see filter); only skip polling
            # when there is no Databricks run ID to look up.
            if not job.databricks_run_id:
                result = {
                    "status": before_status,
                    "databricks_run_id": job.databricks_run_id,
                    "life_cycle_state": None,
                    "result_state": None,
                }
            else:
                run = timed_client.jobs.get_run(run_id=int(job.databricks_run_id))
                life_cycle_state = run.state.life_cycle_state.value if run.state.life_cycle_state else None
                result_state = run.state.result_state.value if run.state.result_state else None
                if life_cycle_state in {"TERMINATED", "INTERNAL_ERROR", "SKIPPED"}:
                    if life_cycle_state == "TERMINATED" and result_state == "SUCCESS":
                        service.complete_job(job.id, "COMPLETED", None, lakefusion_databricks_dapi)
                    else:
                        error_msg = run.state.state_message or f"Job failed with result: {result_state} (lifecycle: {life_cycle_state})"
                        service.complete_job(job.id, "FAILED", error_msg, lakefusion_databricks_dapi)
                    job = service.get_job(job.id)
                result = {
                    "status": job.status,
                    "databricks_run_id": job.databricks_run_id,
                    "life_cycle_state": life_cycle_state,
                    "result_state": result_state,
                }
            after_status = result.get("status", before_status)
            if after_status != before_status:
                summary["updated"] += 1
            else:
                summary["unchanged"] += 1
        except Exception as exc:
            summary["errored"] += 1
            _log_item_failure("check_schema_evolution_status", f"job_id={job.id}", exc)

    return summary

def job_wrapper(job_id: str, job_func, *args, **kwargs):
    """Wrap every scheduled job with start, success, failure and rollback handling."""
    start_time = datetime.now(timezone.utc)
    logger.info(f"job {job_id} started")
    try:
        with db_context() as db:
            result = job_func(db, *args, **kwargs)
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        summary = _format_summary(result)
        if summary is not None:
            logger.info(f"job {job_id} finished in {duration:.3f}s: {summary}")
        else:
            logger.info(f"job {job_id} finished in {duration:.3f}s")
    except (OperationalError, DBAPIError) as exc:
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        logger.error(
            f"cron DB connection failed for job {job_id} after {duration:.3f}s: {type(exc).__name__}: {exc}",
            exc_info=True,
        )
    except Exception as exc:
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        logger.error(
            f"job {job_id} failed after {duration:.3f}s: {type(exc).__name__}: {exc}",
            exc_info=True,
        )
        # Intentional swallow: cron jobs must not crash the scheduler process.
        # Job-level failures are observable via the structured log line above.
        # APScheduler-level failures (thread-pool exhaustion, misfire resolution
        # errors) bypass job_wrapper entirely and are caught by the
        # EVENT_JOB_ERROR listener in main.py.


def get_scheduler_jobs(scheduler: BackgroundScheduler):
    from app.lakefusion_cron_service.config import deployment_env
    from apscheduler.triggers.cron import CronTrigger
    from pytz import utc
    from lakefusion_utility.services.audit_log_service import purge_old_audit_logs
    from lakefusion_utility.services.rbac_admin_sync import sync_admin_users
    from app.lakefusion_cron_service.services.notebook_sync_service import purge_old_notebook_sync_audit_logs

    # max_instances=1 + coalesce=True makes hung jobs visible via EVENT_JOB_MAX_INSTANCES.
    single_instance_job_kwargs = {
        'max_instances': 1,
        'replace_existing': True,
        'coalesce': True,
    }

    # Audit log purge job - runs daily at 2 AM UTC for all environments
    scheduler.add_job(
        func=lambda: job_wrapper('purge_old_audit_logs', purge_old_audit_logs),
        id='purge_old_audit_logs',
        trigger=CronTrigger.from_crontab('0 2 * * *', timezone=utc),  # Daily at 2 AM UTC
        **single_instance_job_kwargs,
    )

    # Notebook sync audit log purge - runs daily at 3 AM UTC for all environments
    scheduler.add_job(
        func=lambda: job_wrapper('purge_old_notebook_sync_audit_logs', purge_old_notebook_sync_audit_logs),
        id='purge_old_notebook_sync_audit_logs',
        trigger=CronTrigger.from_crontab('0 3 * * *', timezone=utc),  # Daily at 3 AM UTC
        **single_instance_job_kwargs,
    )

    # RBAC admin sync - runs hourly for all environments
    scheduler.add_job(
        func=lambda: job_wrapper('sync_admin_users', sync_admin_users),
        id='sync_admin_users',
        trigger=CronTrigger.from_crontab('0 * * * *', timezone=utc),  # Every hour
        **single_instance_job_kwargs,
    )

    if deployment_env == 'local':
        """ Add local jobs here """
        pass

    elif deployment_env == 'dev':
        """ Add development jobs here """
        scheduler.add_job(
            func=lambda: job_wrapper('check_monitor_status', check_monitor_status_job),
            id='check_monitor_status',
            trigger=CronTrigger.from_crontab('*/10 * * * *', timezone=utc),
            **single_instance_job_kwargs,
        )
        scheduler.add_job(
            func=lambda: job_wrapper('check_refresh_status', check_refresh_status_job),
            id='check_refresh_status',
            trigger=CronTrigger.from_crontab('*/5 * * * *', timezone=utc),
            **single_instance_job_kwargs,
        )
        scheduler.add_job(
            func=lambda: job_wrapper('check_entity_search_monitor_status', check_entity_search_monitor_status_job),
            id='check_entity_search_monitor_status',
            trigger=CronTrigger.from_crontab('*/2 * * * *', timezone=utc),
            **single_instance_job_kwargs,
        )
        scheduler.add_job(
            func=lambda: job_wrapper('check_job_monitor_status', check_job_monitor_status_job),
            id='check_job_monitor_status',
            trigger=CronTrigger.from_crontab('*/2 * * * *', timezone=utc),
            **single_instance_job_kwargs,
        )
        scheduler.add_job(
            func=lambda: job_wrapper('check_schema_evolution_status', check_schema_evolution_status_job),
            id='check_schema_evolution_status',
            trigger=CronTrigger.from_crontab('*/2 * * * *', timezone=utc),
            **single_instance_job_kwargs,
        )
        scheduler.add_job(
            func=lambda: job_wrapper('process_query_completed_stewardship', process_query_completed_stewardship_job),
            id='process_query_completed_stewardship',
            trigger=CronTrigger.from_crontab('*/5 * * * *', timezone=utc),
            **single_instance_job_kwargs,
        )

    else:
        """ Add production jobs here """
        scheduler.add_job(
            func=lambda: job_wrapper('check_monitor_status', check_monitor_status_job),
            id='check_monitor_status',
            trigger=CronTrigger.from_crontab('*/10 * * * *', timezone=utc),
            **single_instance_job_kwargs,
        )
        scheduler.add_job(
            func=lambda: job_wrapper('check_refresh_status', check_refresh_status_job),
            id='check_refresh_status',
            trigger=CronTrigger.from_crontab('*/10 * * * *', timezone=utc),
            **single_instance_job_kwargs,
        )
        scheduler.add_job(
            func=lambda: job_wrapper('check_job_monitor_status', check_job_monitor_status_job),
            id='check_job_monitor_status',
            trigger=CronTrigger.from_crontab('*/2 * * * *', timezone=utc),
            **single_instance_job_kwargs,
        )
        scheduler.add_job(
            func=lambda: job_wrapper('check_entity_search_monitor_status', check_entity_search_monitor_status_job),
            id='check_entity_search_monitor_status',
            trigger=CronTrigger.from_crontab('*/2 * * * *', timezone=utc),
            **single_instance_job_kwargs,
        )
        scheduler.add_job(
            func=lambda: job_wrapper('check_schema_evolution_status', check_schema_evolution_status_job),
            id='check_schema_evolution_status',
            trigger=CronTrigger.from_crontab('*/2 * * * *', timezone=utc),
            **single_instance_job_kwargs,
        )
        scheduler.add_job(
            func=lambda: job_wrapper('process_query_completed_stewardship', process_query_completed_stewardship_job),
            id='process_query_completed_stewardship',
            trigger=CronTrigger.from_crontab('*/5 * * * *', timezone=utc),
            **single_instance_job_kwargs,
        )

    return scheduler
