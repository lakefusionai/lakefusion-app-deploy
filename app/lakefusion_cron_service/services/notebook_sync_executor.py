"""
Notebook Sync Executor — registry-aware wrapper around the artifact import logic.

Flow:
1. Scan artifacts and update registry
2. Resolve all policies (single pass)
3. Determine which files need sync (hash comparison)
4. Import only changed files to Databricks
5. Update DB + commit once
"""

import os
import uuid
import base64
from typing import List, Optional, Dict
from sqlalchemy.orm import Session
from app.lakefusion_cron_service.services.notebook_sync_service import (
    NotebookSyncService, NOTEBOOK_EXTENSIONS, _file_hash, _resolve_file,
)
from lakefusion_utility.services.databricks_sync_service import NotebookService, lakefusion_databricks_dapi
from lakefusion_utility.utils.db_config_utility import DBConfigPropertiesService
from lakefusion_utility.utils.logging_utils import get_logger
from databricks.sdk.service.workspace import ImportFormat, Language, ObjectInfo
from databricks.sdk.service.iam import PermissionLevel

app_logger = get_logger(__name__)

NOTEBOOK_LANG_MAP = {
    '.py': (Language.PYTHON, ImportFormat.SOURCE),
    '.sql': (Language.SQL, ImportFormat.SOURCE),
    '.scala': (Language.SCALA, ImportFormat.SOURCE),
    '.r': (Language.R, ImportFormat.SOURCE),
    '.ipynb': (Language.PYTHON, ImportFormat.JUPYTER),
}

from lakefusion_utility.utils.lf_utils import get_version_info

APP_VERSION = (get_version_info() or {}).get("productVersion", "0.0.0")


def execute_sync(db: Session, force: bool = False,
                 unlock_ids: Optional[List[str]] = None,
                 user: str = "system",
                 trigger: str = "startup") -> Dict:
    sync_run_id = str(uuid.uuid4())
    service = NotebookSyncService(db)

    db_config_service = DBConfigPropertiesService(db=db)
    target_path = db_config_service.getDBConfigProperties('notebook_path', required=True)
    lakefusion_spn = db_config_service.getDBConfigProperties('lakefusion_spn', required=True)
    artifacts_dir = "dbx_pipeline_artifacts"

    app_logger.info(f"Notebook sync started (run_id={sync_run_id}, trigger={trigger}, force={force})")

    # ── 1. Scan artifacts → update registry ──
    scan_counts = service.scan_and_register(
        artifacts_dir=artifacts_dir,
        app_version=APP_VERSION,
        workspace_base_path=target_path,
    )

    # ── 2. Resolve all policies in one pass ──
    if force:
        decisions = service.prepare_force_sync(
            unlock_ids=unlock_ids or [], user=user, artifacts_dir=artifacts_dir,
        )
    else:
        decisions = service.get_startup_sync_decisions(
            trigger=trigger, artifacts_dir=artifacts_dir,
        )

    # Split into imports vs skips
    to_import = [(item, reason) for item, action, reason in decisions if action == 'IMPORT']
    to_skip = [(item, reason) for item, action, reason in decisions if action == 'SKIP']

    app_logger.info(f"Sync decisions: {len(to_import)} to import, {len(to_skip)} to skip")

    # ── 3. Import changed files to Databricks ──
    notebook_service = NotebookService(token=lakefusion_databricks_dapi)
    imported_count = 0
    error_count = 0

    if to_import:
        # Pre-create workspace directories once
        dirs_to_create = set()
        for item, _ in to_import:
            if item.workspace_path:
                ws_parent = os.path.dirname(item.workspace_path)
                if ws_parent:
                    dirs_to_create.add(ws_parent)
        for ws_dir in sorted(dirs_to_create):
            try:
                notebook_service.w.workspace.mkdirs(ws_dir)
            except Exception as e:
                app_logger.warning(f"Failed to create workspace dir {ws_dir}: {e}")

        # Import each file
        for item, reason in to_import:
            try:
                file_path = _resolve_file(artifacts_dir, item.artifact_path)
                if not file_path:
                    raise FileNotFoundError(f"No file found for {item.artifact_path}")

                _import_file(notebook_service, file_path, item.workspace_path)

                # Stage DB updates (no commit yet)
                file_hash = _file_hash(file_path)
                service.mark_imported(item, file_hash=file_hash)
                service.log_audit(
                    registry_id=item.id, artifact_path=item.artifact_path,
                    action='IMPORTED', from_version=item.deployed_version,
                    to_version=item.available_version,
                    effective_policy=reason, reason=reason,
                    sync_run_id=sync_run_id,
                )
                imported_count += 1
            except Exception as e:
                app_logger.error(f"Failed to import {item.artifact_path}: {e}")
                service.log_audit(
                    registry_id=item.id, artifact_path=item.artifact_path,
                    action='SKIPPED', from_version=item.deployed_version,
                    to_version=item.available_version,
                    effective_policy=reason, reason=f"Import error: {str(e)}",
                    sync_run_id=sync_run_id,
                )
                error_count += 1

    # Log skipped items
    for item, reason in to_skip:
        service.log_audit(
            registry_id=item.id, artifact_path=item.artifact_path,
            action='SKIPPED', from_version=item.deployed_version,
            to_version=item.available_version,
            effective_policy=reason, reason=reason,
            sync_run_id=sync_run_id,
        )

    # ── 4. Grant SPN permissions on the notebook folder ──
    if imported_count > 0:
        try:
            info: ObjectInfo = notebook_service.get_object_info(target_path)
            notebook_service.set_permissions(
                workspace_object=info,
                principal_id=lakefusion_spn,
                permission=PermissionLevel.CAN_MANAGE,
                is_service_principal=True,
            )
            app_logger.info(f"Granted CAN_MANAGE to SPN {lakefusion_spn} on {target_path}")
        except Exception as e:
            app_logger.warning(f"Failed to set SPN permissions: {e}")

    # ── 5. Single commit ──
    db.commit()

    summary = {
        "sync_run_id": sync_run_id,
        "trigger": trigger,
        "force": force,
        "registered": scan_counts.get("registered", 0),
        "imported": imported_count,
        "skipped": len(to_skip),
        "errors": error_count,
    }
    app_logger.info(f"Notebook sync complete: {summary}")
    return summary


def _import_file(notebook_service: NotebookService, file_path: str, workspace_path: str):
    """Import a notebook or upload a regular file to the Databricks workspace."""
    file_ext = os.path.splitext(file_path)[1].lower()

    if file_ext in NOTEBOOK_LANG_MAP:
        # Notebook — import via workspace.import_() (extension stripped from workspace_path)
        language, import_format = NOTEBOOK_LANG_MAP[file_ext]
        with open(file_path, "rb") as f:
            content = base64.b64encode(f.read()).decode("utf-8")
        notebook_service.w.workspace.import_(
            path=workspace_path,
            content=content,
            format=import_format,
            language=language,
            overwrite=True,
        )
    else:
        # Regular file — upload via import_single_file (extension preserved)
        notebook_service.import_single_file(
            source_file_path=file_path,
            target_path=workspace_path,
            overwrite=True,
        )

    app_logger.info(f"Imported: {file_path} -> {workspace_path}")
