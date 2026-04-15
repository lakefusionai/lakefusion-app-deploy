from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from app.lakefusion_middlelayer_service.utils.app_db import get_db, token_required_wrapper
import uuid
from lakefusion_utility.models.notebook_sync import (
    NotebookSyncRegistry,
    NotebookSyncRegistryResponse, NotebookSyncRegistryUpdate,
    NotebookSyncAuditLog, NotebookSyncAuditLogResponse,
)
from lakefusion_utility.utils.logging_utils import get_logger
from datetime import datetime, timezone

app_logger = get_logger(__name__)

notebook_sync_router = APIRouter(tags=["Notebook Sync"], prefix='/notebook-sync')


@notebook_sync_router.get("/registry", response_model=List[NotebookSyncRegistryResponse])
def get_registry(
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    """Get all active items in the notebook sync registry."""
    return (
        db.query(NotebookSyncRegistry)
        .filter(NotebookSyncRegistry.status == 'ACTIVE')
        .order_by(NotebookSyncRegistry.artifact_path)
        .all()
    )


@notebook_sync_router.get("/registry/locked", response_model=List[NotebookSyncRegistryResponse])
def get_locked_items(
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    """Get all items with LOCKED override_policy (direct or on a parent folder)."""
    all_active = (
        db.query(NotebookSyncRegistry)
        .filter(NotebookSyncRegistry.status == 'ACTIVE')
        .all()
    )

    def resolve_policy(item):
        if item.override_policy:
            return item.override_policy
        parent_path = item.parent_path
        while parent_path:
            parent = next((i for i in all_active if i.artifact_path == parent_path), None)
            if parent and parent.override_policy:
                return parent.override_policy
            parent_path = parent.parent_path if parent else None
        return "SYNC_ON_UPGRADE"

    return [item for item in all_active if resolve_policy(item) == "LOCKED"]


@notebook_sync_router.put("/registry/{item_id}", response_model=NotebookSyncRegistryResponse)
def update_registry_item(
    item_id: str,
    data: NotebookSyncRegistryUpdate,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    """Update sync policy for a notebook or folder."""
    item = db.query(NotebookSyncRegistry).filter(NotebookSyncRegistry.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Registry item not found")

    decoded = check.get("decoded", {}) if isinstance(check, dict) else {}
    user = decoded.get("sub", "unknown") if isinstance(decoded, dict) else "unknown"

    old_policy = item.override_policy or "INHERIT"
    new_policy = data.override_policy or "INHERIT"

    item.override_policy = data.override_policy
    item.updated_at = datetime.now(timezone.utc)

    if data.override_policy == "LOCKED":
        item.locked_by = data.locked_by or user
        item.locked_at = datetime.now(timezone.utc)
        item.lock_reason = data.lock_reason
    else:
        item.locked_by = None
        item.locked_at = None
        item.lock_reason = None

    # Log policy change
    if old_policy != new_policy:
        policy_labels = {"SYNC_ON_UPGRADE": "Sync on Upgrade", "LOCKED": "Locked", "INHERIT": "Inherit"}
        reason = f"{policy_labels.get(old_policy, old_policy)} → {policy_labels.get(new_policy, new_policy)} by {user}"
        if data.lock_reason:
            reason += f" — {data.lock_reason}"
        audit = NotebookSyncAuditLog(
            id=str(uuid.uuid4()),
            registry_id=item.id,
            artifact_path=item.artifact_path,
            action="POLICY_CHANGED",
            from_version=item.deployed_version,
            to_version=item.available_version,
            effective_policy=new_policy,
            reason=reason,
        )
        db.add(audit)

    db.commit()
    db.refresh(item)
    return item


@notebook_sync_router.put("/registry/{item_id}/apply-to-children")
def apply_policy_to_children(
    item_id: str,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    """Reset all children of a folder to inherit from parent."""
    item = db.query(NotebookSyncRegistry).filter_by(id=item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Registry item not found")
    if item.item_type != 'FOLDER':
        raise HTTPException(status_code=400, detail="Can only apply to children of a folder")

    children = (
        db.query(NotebookSyncRegistry)
        .filter(
            NotebookSyncRegistry.parent_path.like(f"{item.artifact_path}%"),
            NotebookSyncRegistry.status == 'ACTIVE',
        )
        .all()
    )
    for child in children:
        child.override_policy = None
        child.locked_by = None
        child.locked_at = None
        child.lock_reason = None
        child.updated_at = datetime.now(timezone.utc)
    db.commit()
    return {"message": f"Reset {len(children)} children to inherit policy"}


@notebook_sync_router.get("/audit-log", response_model=List[NotebookSyncAuditLogResponse])
def get_audit_log(
    limit: int = Query(default=50, le=500),
    offset: int = Query(default=0),
    action: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    """Get sync audit log entries."""
    query = db.query(NotebookSyncAuditLog).order_by(NotebookSyncAuditLog.created_at.desc())
    if action:
        query = query.filter(NotebookSyncAuditLog.action == action)
    return query.offset(offset).limit(limit).all()
