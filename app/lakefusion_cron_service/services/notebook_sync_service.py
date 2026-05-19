"""
Notebook Sync Service — manages the sync registry and controls which notebooks get imported.

Responsibilities:
1. Auto-discover notebooks/folders from dbx_pipeline_artifacts/
2. Populate/update notebook_sync_registry
3. Resolve effective policy (walk parent chain for inheritance)
4. Decide import/skip per notebook based on policy + version
5. Log every decision to notebook_sync_audit_log
"""

import hashlib
import os
import uuid
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Tuple
from sqlalchemy.orm import Session
from lakefusion_utility.models.notebook_sync import (
    NotebookSyncRegistry, NotebookSyncAuditLog,
)
from lakefusion_utility.utils.logging_utils import get_logger

app_logger = get_logger(__name__)

# Extensions treated as notebooks — imported via workspace.import_() (extension removed)
NOTEBOOK_EXTENSIONS = {'.py', '.sql', '.scala', '.r', '.ipynb'}
# Extensions treated as regular files — uploaded via import_single_file (extension preserved)
REGULAR_FILE_EXTENSIONS = {'.whl', '.jar', '.egg', '.json', '.sh', '.bash', '.md',
                           '.txt', '.yaml', '.yml', '.conf', '.properties', '.xml',
                           '.csv', '.tsv', '.parquet', '.avro', '.orc', '.pyi', '.example'}
ALL_TRACKED_EXTENSIONS = NOTEBOOK_EXTENSIONS | REGULAR_FILE_EXTENSIONS
SKIP_DIRS = {'__pycache__', 'node_modules', '.git', '__MACOSX'}

DEFAULT_POLICY = 'SYNC_ON_UPGRADE'


def _file_hash(file_path: str) -> str:
    """Return MD5 hex digest of a file's contents (32-char string)."""
    with open(file_path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()


def _resolve_file(artifacts_dir: str, artifact_path: str) -> Optional[str]:
    """Find the actual file path for an artifact.
    For notebooks (no extension in artifact_path): tries each NOTEBOOK_EXTENSION.
    For regular files (extension in artifact_path): checks the path directly.
    """
    full_path = os.path.join(artifacts_dir, artifact_path)
    # Regular file — artifact_path already includes extension
    if os.path.exists(full_path):
        return full_path
    # Notebook — try each extension
    for ext in NOTEBOOK_EXTENSIONS:
        candidate = full_path + ext
        if os.path.exists(candidate):
            return candidate
    return None


class NotebookSyncService:
    def __init__(self, db: Session):
        self.db = db

    # ------------------------------------------------------------------
    # 1. SCAN & REGISTER — auto-discover from artifacts directory
    # ------------------------------------------------------------------
    def scan_and_register(self, artifacts_dir: str, app_version: str, workspace_base_path: str) -> Dict[str, int]:
        """
        Walk dbx_pipeline_artifacts/, register new items, archive removed ones.
        Returns counts: {registered: N, reactivated: N, archived: N}
        """
        counts = {"registered": 0, "reactivated": 0, "archived": 0}
        discovered_paths = set()

        if not os.path.exists(artifacts_dir):
            app_logger.warning(f"Artifacts directory not found: {artifacts_dir}")
            return counts

        for root, dirs, files in os.walk(artifacts_dir):
            dirs[:] = [d for d in dirs if not d.startswith('.') and d not in SKIP_DIRS]

            # Register folder
            relative_dir = os.path.relpath(root, artifacts_dir)
            if relative_dir != ".":
                parent_path = os.path.dirname(relative_dir) if os.path.dirname(relative_dir) != "." else None
                folder_ws_path = os.path.join(workspace_base_path, relative_dir).replace("\\", "/")
                self._register_item(
                    artifact_path=relative_dir,
                    item_type="FOLDER",
                    parent_path=parent_path if parent_path else None,
                    display_name=os.path.basename(relative_dir),
                    workspace_path=folder_ws_path,
                    app_version=app_version,
                    counts=counts,
                )
                discovered_paths.add(relative_dir)

            # Register notebooks and regular files
            for file_name in files:
                if file_name.startswith('.'):
                    continue

                file_ext = os.path.splitext(file_name)[1].lower()
                is_notebook = file_ext in NOTEBOOK_EXTENSIONS
                is_regular = file_ext in REGULAR_FILE_EXTENSIONS

                if not is_notebook and not is_regular:
                    continue  # Unknown extension — skip

                if is_notebook:
                    # Notebooks: strip extension from artifact_path (Databricks convention)
                    display = os.path.splitext(file_name)[0]
                    item_type = "NOTEBOOK"
                else:
                    # Regular files: keep extension in artifact_path
                    display = file_name
                    item_type = "FILE"

                if relative_dir == ".":
                    artifact_path = display
                    parent_path = None
                else:
                    artifact_path = os.path.join(relative_dir, display).replace("\\", "/")
                    parent_path = relative_dir

                ws_path = os.path.join(workspace_base_path, artifact_path).replace("\\", "/")
                self._register_item(
                    artifact_path=artifact_path,
                    item_type=item_type,
                    parent_path=parent_path,
                    display_name=display,
                    workspace_path=ws_path,
                    app_version=app_version,
                    counts=counts,
                )
                discovered_paths.add(artifact_path)

        # Archive items no longer in artifacts
        active_items = (
            self.db.query(NotebookSyncRegistry)
            .filter(NotebookSyncRegistry.status == 'ACTIVE')
            .all()
        )
        for item in active_items:
            if item.artifact_path not in discovered_paths:
                item.status = 'ARCHIVED'
                item.updated_at = datetime.now(timezone.utc)
                counts["archived"] += 1

        self.db.commit()
        app_logger.info(
            f"Registry scan complete: {counts['registered']} registered, "
            f"{counts['reactivated']} reactivated, {counts['archived']} archived"
        )
        return counts

    def _register_item(self, artifact_path: str, item_type: str, parent_path: Optional[str],
                       display_name: str, workspace_path: str, app_version: str, counts: dict):
        existing = (
            self.db.query(NotebookSyncRegistry)
            .filter(NotebookSyncRegistry.artifact_path == artifact_path)
            .first()
        )
        if existing:
            existing.available_version = app_version
            existing.workspace_path = workspace_path
            if existing.status == 'ARCHIVED':
                existing.status = 'ACTIVE'
                existing.updated_at = datetime.now(timezone.utc)
                counts["reactivated"] += 1
        else:
            item = NotebookSyncRegistry(
                id=str(uuid.uuid4()),
                artifact_path=artifact_path,
                item_type=item_type,
                parent_path=parent_path,
                display_name=display_name,
                workspace_path=workspace_path,
                override_policy=None,  # inherit by default
                available_version=app_version,
                status='ACTIVE',
            )
            self.db.add(item)
            counts["registered"] += 1

    # ------------------------------------------------------------------
    # 2. RESOLVE EFFECTIVE POLICY — walk parent chain
    # ------------------------------------------------------------------
    def resolve_effective_policy(self, item: NotebookSyncRegistry) -> str:
        """Walk up parent_path chain to find the first non-NULL override_policy."""
        if item.override_policy:
            return item.override_policy

        # Walk up parent chain
        current_parent = item.parent_path
        while current_parent:
            parent = (
                self.db.query(NotebookSyncRegistry)
                .filter(
                    NotebookSyncRegistry.artifact_path == current_parent,
                    NotebookSyncRegistry.status == 'ACTIVE',
                )
                .first()
            )
            if parent and parent.override_policy:
                return parent.override_policy
            current_parent = parent.parent_path if parent else None

        return DEFAULT_POLICY

    # ------------------------------------------------------------------
    # 3. GET NOTEBOOKS TO SYNC — returns list of (registry_item, decision)
    # ------------------------------------------------------------------
    def get_sync_decisions(self, sync_run_id: str) -> List[Tuple[NotebookSyncRegistry, str, str]]:
        """
        Returns list of (item, action, reason) for all active notebooks.
        Actions: 'IMPORT' or 'SKIP'
        """
        notebooks = (
            self.db.query(NotebookSyncRegistry)
            .filter(
                NotebookSyncRegistry.item_type.in_(['NOTEBOOK', 'FILE']),
                NotebookSyncRegistry.status == 'ACTIVE',
            )
            .order_by(NotebookSyncRegistry.artifact_path)
            .all()
        )

        decisions = []
        for nb in notebooks:
            policy = self.resolve_effective_policy(nb)

            if policy == 'LOCKED':
                locked_by = nb.locked_by or self._find_locking_ancestor(nb)
                decisions.append((nb, 'SKIP', f"Locked by {locked_by or 'unknown'}"))
            elif policy == 'SYNC_ON_UPGRADE':
                if nb.available_version and nb.available_version != nb.deployed_version:
                    decisions.append((nb, 'IMPORT', f"Version upgrade: {nb.deployed_version or 'none'} -> {nb.available_version}"))
                else:
                    decisions.append((nb, 'SKIP', "Same version — no upgrade needed"))
            else:
                decisions.append((nb, 'SKIP', f"Unknown policy: {policy}"))

        return decisions

    # ------------------------------------------------------------------
    # 3b. STARTUP SYNC — import all non-locked, with descriptive reasons
    # ------------------------------------------------------------------
    def get_startup_sync_decisions(self, trigger: str = "startup",
                                    artifacts_dir: str = "dbx_pipeline_artifacts") -> List[Tuple[NotebookSyncRegistry, str, str]]:
        """
        Returns decisions for startup/restart sync.
        Compares file hash to decide — only imports notebooks whose content changed.
        """
        notebooks = (
            self.db.query(NotebookSyncRegistry)
            .filter(
                NotebookSyncRegistry.item_type.in_(['NOTEBOOK', 'FILE']),
                NotebookSyncRegistry.status == 'ACTIVE',
            )
            .order_by(NotebookSyncRegistry.artifact_path)
            .all()
        )

        trigger_label = "Service startup" if trigger == "startup" else f"Trigger: {trigger}"
        decisions = []
        for nb in notebooks:
            policy = self.resolve_effective_policy(nb)

            if policy == 'LOCKED':
                locked_by = nb.locked_by or self._find_locking_ancestor(nb)
                decisions.append((nb, 'SKIP', f"Locked by {locked_by or 'unknown'}"))
                continue

            # Compute current file hash
            file_path = _resolve_file(artifacts_dir, nb.artifact_path)
            if not file_path:
                decisions.append((nb, 'SKIP', "File not found in artifacts directory"))
                continue

            current_hash = _file_hash(file_path)

            if nb.available_version and nb.available_version != nb.deployed_version:
                decisions.append((nb, 'IMPORT', f"Version upgrade: {nb.deployed_version or 'none'} -> {nb.available_version}"))
            elif current_hash != nb.deployed_hash:
                decisions.append((nb, 'IMPORT', f"{trigger_label} sync — file content changed"))
            else:
                decisions.append((nb, 'SKIP', "File unchanged (hash match)"))

        return decisions

    def _find_locking_ancestor(self, item: NotebookSyncRegistry) -> Optional[str]:
        """Find which ancestor folder has the LOCKED policy and return its locked_by."""
        current_parent = item.parent_path
        while current_parent:
            parent = (
                self.db.query(NotebookSyncRegistry)
                .filter(NotebookSyncRegistry.artifact_path == current_parent)
                .first()
            )
            if parent and parent.override_policy == 'LOCKED':
                return parent.locked_by
            current_parent = parent.parent_path if parent else None
        return None

    # ------------------------------------------------------------------
    # 4. LOG AUDIT
    # ------------------------------------------------------------------
    def log_audit(self, registry_id: Optional[str], artifact_path: str, action: str,
                  from_version: Optional[str], to_version: Optional[str],
                  effective_policy: str, reason: str, sync_run_id: str):
        log = NotebookSyncAuditLog(
            id=str(uuid.uuid4()),
            registry_id=registry_id,
            artifact_path=artifact_path,
            action=action,
            from_version=from_version,
            to_version=to_version,
            effective_policy=effective_policy,
            reason=reason,
            sync_run_id=sync_run_id,
        )
        self.db.add(log)

    def mark_imported(self, item: NotebookSyncRegistry, file_hash: Optional[str] = None):
        """Update registry after successful import."""
        item.deployed_version = item.available_version
        if file_hash:
            item.deployed_hash = file_hash
        item.last_synced_at = datetime.now(timezone.utc)
        item.updated_at = datetime.now(timezone.utc)

    # ------------------------------------------------------------------
    # 5. FORCE SYNC — unlock specified items and mark all for import
    # ------------------------------------------------------------------
    def prepare_force_sync(self, unlock_ids: List[str], user: str,
                           artifacts_dir: str = "dbx_pipeline_artifacts") -> List[Tuple[NotebookSyncRegistry, str, str]]:
        """
        Unlock specified items, then return decisions for ALL active notebooks (force import all non-locked).
        """
        # Unlock requested items
        for item_id in unlock_ids:
            item = self.db.query(NotebookSyncRegistry).filter(NotebookSyncRegistry.id == item_id).first()
            if item:
                item.override_policy = None  # reset to inherit
                item.locked_by = None
                item.locked_at = None
                item.lock_reason = None
                item.updated_at = datetime.now(timezone.utc)
                app_logger.info(f"Unlocked {item.artifact_path} for force sync (by {user})")

        self.db.flush()

        # Now get decisions — all non-locked notebooks get IMPORT
        notebooks = (
            self.db.query(NotebookSyncRegistry)
            .filter(
                NotebookSyncRegistry.item_type.in_(['NOTEBOOK', 'FILE']),
                NotebookSyncRegistry.status == 'ACTIVE',
            )
            .order_by(NotebookSyncRegistry.artifact_path)
            .all()
        )

        decisions = []
        for nb in notebooks:
            policy = self.resolve_effective_policy(nb)
            if policy == 'LOCKED':
                locked_by = nb.locked_by or self._find_locking_ancestor(nb)
                decisions.append((nb, 'SKIP', f"Kept locked during force sync (by {locked_by or 'unknown'})"))
            elif nb.available_version and nb.available_version != nb.deployed_version:
                decisions.append((nb, 'IMPORT', f"Force sync by {user} — version upgrade: {nb.deployed_version or 'none'} -> {nb.available_version}"))
            else:
                decisions.append((nb, 'IMPORT', f"Force sync by {user}"))

        return decisions

    # ------------------------------------------------------------------
    # 6. READ helpers for the UI
    # ------------------------------------------------------------------
    def get_all_active(self) -> List[NotebookSyncRegistry]:
        return (
            self.db.query(NotebookSyncRegistry)
            .filter(NotebookSyncRegistry.status == 'ACTIVE')
            .order_by(NotebookSyncRegistry.artifact_path)
            .all()
        )

    def get_locked_items(self) -> List[NotebookSyncRegistry]:
        """Get all items with effective LOCKED policy (own or inherited)."""
        active = self.get_all_active()
        locked = []
        for item in active:
            if self.resolve_effective_policy(item) == 'LOCKED':
                locked.append(item)
        return locked

    def get_audit_logs(self, limit: int = 100, offset: int = 0,
                       action: Optional[str] = None) -> List[NotebookSyncAuditLog]:
        query = self.db.query(NotebookSyncAuditLog).order_by(NotebookSyncAuditLog.created_at.desc())
        if action:
            query = query.filter(NotebookSyncAuditLog.action == action)
        return query.offset(offset).limit(limit).all()

    def update_policy(self, item_id: str, override_policy: Optional[str],
                      locked_by: Optional[str] = None, lock_reason: Optional[str] = None):
        item = self.db.query(NotebookSyncRegistry).filter(NotebookSyncRegistry.id == item_id).first()
        if not item:
            return None

        item.override_policy = override_policy
        item.updated_at = datetime.now(timezone.utc)

        if override_policy == 'LOCKED':
            item.locked_by = locked_by
            item.locked_at = datetime.now(timezone.utc)
            item.lock_reason = lock_reason
        else:
            item.locked_by = None
            item.locked_at = None
            item.lock_reason = None

        self.db.commit()
        self.db.refresh(item)
        return item

    def apply_policy_to_children(self, parent_path: str):
        """Reset all children of a folder to inherit (override_policy = NULL)."""
        children = (
            self.db.query(NotebookSyncRegistry)
            .filter(
                NotebookSyncRegistry.parent_path.like(f"{parent_path}%"),
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
        self.db.commit()
        return len(children)

    # ------------------------------------------------------------------
    # 8. PURGE OLD AUDIT LOGS
    # ------------------------------------------------------------------
    def purge_old_audit_logs(self, retention_days: int = 15) -> int:
        """Delete audit log records older than retention_days."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
        count = (
            self.db.query(NotebookSyncAuditLog)
            .filter(NotebookSyncAuditLog.created_at < cutoff)
            .delete(synchronize_session=False)
        )
        self.db.commit()
        app_logger.info(f"Purged {count} notebook sync audit log records older than {retention_days} days")
        return count


def purge_old_notebook_sync_audit_logs(db, retention_days: int = 15):
    """Standalone entry point for the cron job scheduler."""
    service = NotebookSyncService(db)
    service.purge_old_audit_logs(retention_days=retention_days)
