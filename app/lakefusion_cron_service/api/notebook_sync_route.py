import threading
import uuid
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.lakefusion_cron_service.utils.app_db import get_db, db_context, token_required_wrapper
from lakefusion_utility.models.notebook_sync import ForceSyncRequest
from lakefusion_utility.utils.logging_utils import get_logger

app_logger = get_logger(__name__)

notebook_sync_router = APIRouter(tags=["Notebook Sync"], prefix='/notebook-sync')

# Track running sync jobs
_sync_status = {}


def _run_sync_in_background(sync_id: str, unlock_ids: list, user: str):
    """Run force sync in a background thread with its own DB session."""
    _sync_status[sync_id] = {"status": "running", "result": None}
    try:
        with db_context() as db:
            from app.lakefusion_cron_service.services.notebook_sync_executor import execute_sync
            result = execute_sync(db=db, force=True, unlock_ids=unlock_ids, user=user, trigger="force_sync")
            _sync_status[sync_id] = {"status": "completed", "result": result}
            app_logger.info(f"Force sync {sync_id} completed: {result}")
    except Exception as e:
        _sync_status[sync_id] = {"status": "failed", "result": str(e)}
        app_logger.error(f"Force sync {sync_id} failed: {e}")


@notebook_sync_router.post("/force-sync")
def force_sync(
    data: ForceSyncRequest,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    """
    Trigger force sync asynchronously. Returns a sync_id to poll for status.
    Only cron service can do this — it has access to dbx_pipeline_artifacts.
    """
    decoded = check.get("decoded", {}) if isinstance(check, dict) else {}
    user = decoded.get("sub", "unknown") if isinstance(decoded, dict) else "unknown"

    sync_id = str(uuid.uuid4())
    thread = threading.Thread(
        target=_run_sync_in_background,
        args=(sync_id, data.unlock_ids, user),
        daemon=True,
    )
    thread.start()

    return {"sync_id": sync_id, "status": "started", "message": "Force sync started in background"}


@notebook_sync_router.get("/force-sync/{sync_id}")
def get_sync_status(
    sync_id: str,
    check: dict = Depends(token_required_wrapper),
):
    """Poll the status of a force sync job."""
    status = _sync_status.get(sync_id)
    if not status:
        raise HTTPException(status_code=404, detail="Sync job not found")
    return {"sync_id": sync_id, **status}
