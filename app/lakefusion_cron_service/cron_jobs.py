from apscheduler.schedulers.background import BackgroundScheduler

def get_scheduler_jobs(scheduler: BackgroundScheduler):
    from app.lakefusion_cron_service.config import deployment_env
    from app.lakefusion_cron_service.utils.app_db import get_db
    from sqlalchemy.orm import Session
    from apscheduler.triggers.cron import CronTrigger
    from pytz import utc
    from app.lakefusion_cron_service.utils.app_db import db_context
    from lakefusion_utility.services.profiling_tasks import check_monitor_status, check_refresh_status
    from lakefusion_utility.services.databricks_model_run import check_job_monitor_status
    from lakefusion_utility.services.entity_search_service import check_entity_search_monitor_status
    from lakefusion_utility.services.audit_log_service import purge_old_audit_logs
    from lakefusion_utility.services.schema_evolution_service import check_schema_evolution_status
    from app.lakefusion_cron_service.services.notebook_sync_service import purge_old_notebook_sync_audit_logs

    def job_wrapper(job_func, *args, **kwargs):
        with db_context() as db:
            job_func(db, *args, **kwargs)

    # Audit log purge job - runs daily at 2 AM UTC for all environments
    scheduler.add_job(
        func=lambda: job_wrapper(purge_old_audit_logs),
        id='purge_old_audit_logs',
        trigger=CronTrigger.from_crontab('0 2 * * *', timezone=utc),  # Daily at 2 AM UTC
        max_instances=1,
        replace_existing=True,
        coalesce=True
    )

    # Notebook sync audit log purge - runs daily at 3 AM UTC for all environments
    scheduler.add_job(
        func=lambda: job_wrapper(purge_old_notebook_sync_audit_logs),
        id='purge_old_notebook_sync_audit_logs',
        trigger=CronTrigger.from_crontab('0 3 * * *', timezone=utc),  # Daily at 3 AM UTC
        max_instances=1,
        replace_existing=True,
        coalesce=True
    )

    if deployment_env == 'local':
        """ Add local jobs here """
        pass

    elif deployment_env == 'dev':
        """ Add development jobs here """
        scheduler.add_job(
            func=lambda: job_wrapper(check_monitor_status),
            id='check_monitor_status',
            trigger=CronTrigger.from_crontab('*/10 * * * *', timezone=utc),
            max_instances=1,
            replace_existing=True,
            coalesce=True
        )
        scheduler.add_job(
            func=lambda: job_wrapper(check_refresh_status),
            id='check_refresh_status',
            trigger=CronTrigger.from_crontab('*/5 * * * *', timezone=utc),
            max_instances=1,
            replace_existing=True,
            coalesce=True
        )
        scheduler.add_job(
            func=lambda: job_wrapper(check_entity_search_monitor_status),
            id='check_entity_search_monitor_status',
            trigger=CronTrigger.from_crontab('*/2 * * * *', timezone=utc),
            max_instances=1,
            replace_existing=True,
            coalesce=True
        )
        scheduler.add_job(
            func=lambda: job_wrapper(check_job_monitor_status),
            id='check_job_monitor_status',
            trigger=CronTrigger.from_crontab('*/2 * * * *', timezone=utc),
            max_instances=1,
            replace_existing=True,
            coalesce=True
        )
        scheduler.add_job(
            func=lambda: job_wrapper(check_schema_evolution_status),
            id='check_schema_evolution_status',
            trigger=CronTrigger.from_crontab('*/2 * * * *', timezone=utc),
            max_instances=1,
            replace_existing=True,
            coalesce=True
        )

    else:
        """ Add production jobs here """
        scheduler.add_job(
            func=lambda: job_wrapper(check_monitor_status),
            id='check_monitor_status',
            trigger=CronTrigger.from_crontab('*/10 * * * *', timezone=utc),
            max_instances=1,
            replace_existing=True,
            coalesce=True
        )
        scheduler.add_job(
            func=lambda: job_wrapper(check_refresh_status),
            id='check_refresh_status',
            trigger=CronTrigger.from_crontab('*/10 * * * *', timezone=utc),
            max_instances=1,
            replace_existing=True,
            coalesce=True
        )
        scheduler.add_job(
            func=lambda: job_wrapper(check_job_monitor_status),
            id='check_job_monitor_status',
            trigger=CronTrigger.from_crontab('*/2 * * * *', timezone=utc),
            max_instances=1,
            replace_existing=True,
            coalesce=True
        )
        scheduler.add_job(
            func=lambda: job_wrapper(check_entity_search_monitor_status),
            id='check_entity_search_monitor_status',
            trigger=CronTrigger.from_crontab('*/2 * * * *', timezone=utc),
            max_instances=1,
            replace_existing=True,
            coalesce=True
        )
        scheduler.add_job(
            func=lambda: job_wrapper(check_schema_evolution_status),
            id='check_schema_evolution_status',
            trigger=CronTrigger.from_crontab('*/2 * * * *', timezone=utc),
            max_instances=1,
            replace_existing=True,
            coalesce=True
        )

    return scheduler
