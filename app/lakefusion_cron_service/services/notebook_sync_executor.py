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

    # ── 6. Sync derived UDFs (survivorship) if source changed ──
    udf_synced = _sync_survivorship_udf(
        db=db, service=service, db_config_service=db_config_service,
        sync_run_id=sync_run_id, force=force,
    )

    summary = {
        "sync_run_id": sync_run_id,
        "trigger": trigger,
        "force": force,
        "registered": scan_counts.get("registered", 0),
        "imported": imported_count,
        "skipped": len(to_skip),
        "errors": error_count,
        "udf_synced": udf_synced,
    }
    app_logger.info(f"Notebook sync complete: {summary}")
    return summary


IS_DATABRICKS_APPS = bool(os.environ.get('DATABRICKS_APP_URL'))


def _import_file(notebook_service: NotebookService, file_path: str, workspace_path: str):
    """Import a notebook or upload a regular file to the Databricks workspace."""
    file_ext = os.path.splitext(file_path)[1].lower()

    if file_ext in NOTEBOOK_LANG_MAP:
        # Notebook with extension (K8s) — import via workspace.import_()
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
    elif IS_DATABRICKS_APPS and file_ext == '':
        # Databricks Apps strips .py from notebooks — extensionless file is a Python notebook
        with open(file_path, "rb") as f:
            content = base64.b64encode(f.read()).decode("utf-8")
        notebook_service.w.workspace.import_(
            path=workspace_path,
            content=content,
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
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


# ===========================================================================
# Derived UDF sync — survivorship
# ===========================================================================

UDF_ARTIFACT_PATH = "survivorship_udf"
UDF_DISPLAY_NAME = "Survivorship UDF (auto-generated)"

def _compute_survivorship_hash() -> str:
    """Compute MD5 hash of all survivorship SDK source files combined."""
    import hashlib
    import inspect
    try:
        from lakefusion_core_engine.survivorship import engine as engine_mod
        from lakefusion_core_engine.survivorship.utils import helpers as helpers_mod
        from lakefusion_core_engine.survivorship.models import config as config_mod
        from lakefusion_core_engine.survivorship.models import result as result_mod
        from lakefusion_core_engine.survivorship.strategies import base as base_mod
        from lakefusion_core_engine.survivorship.strategies import recency_strategy as recency_mod
        from lakefusion_core_engine.survivorship.strategies import source_system_strategy as source_mod
        from lakefusion_core_engine.survivorship.strategies import aggregation_strategy as agg_mod
        from lakefusion_core_engine.survivorship.strategies import frequency_strategy as freq_mod

        modules = [helpers_mod, config_mod, result_mod, base_mod,
                   recency_mod, source_mod, agg_mod, freq_mod, engine_mod]
        combined = "".join(inspect.getsource(mod) for mod in modules)
        return hashlib.md5(combined.encode()).hexdigest()
    except Exception as e:
        app_logger.warning(f"Could not compute survivorship SDK hash: {e}")
        return ""


def _sync_survivorship_udf(
    db: Session,
    service: NotebookSyncService,
    db_config_service: DBConfigPropertiesService,
    sync_run_id: str,
    force: bool = False,
) -> bool:
    """
    Sync the survivorship UDF if SDK source has changed.
    Registers a single entry in notebook_sync_registry with item_type='UDF'.
    """
    from lakefusion_utility.models.notebook_sync import NotebookSyncRegistry

    try:
        # Compute current hash of survivorship SDK source
        current_hash = _compute_survivorship_hash()
        if not current_hash:
            app_logger.info("Survivorship SDK not available — skipping UDF sync")
            return False

        # Find or create registry entry
        registry_item = db.query(NotebookSyncRegistry).filter(
            NotebookSyncRegistry.artifact_path == UDF_ARTIFACT_PATH
        ).first()

        if not registry_item:
            registry_item = NotebookSyncRegistry(
                artifact_path=UDF_ARTIFACT_PATH,
                item_type='UDF',
                display_name=UDF_DISPLAY_NAME,
                available_version=APP_VERSION,
                status='ACTIVE',
            )
            db.add(registry_item)
            db.flush()
            app_logger.info(f"Registered UDF artifact in sync registry: {UDF_ARTIFACT_PATH}")

        # Update available version
        registry_item.available_version = APP_VERSION

        # Check if sync needed
        if not force and registry_item.deployed_hash == current_hash:
            app_logger.info(f"Survivorship UDF up to date (hash: {current_hash[:12]}...). Skipping.")
            service.log_audit(
                registry_id=registry_item.id, artifact_path=UDF_ARTIFACT_PATH,
                action='SKIPPED', from_version=registry_item.deployed_version,
                to_version=APP_VERSION, effective_policy='SYNC_ON_UPGRADE',
                reason='hash_match', sync_run_id=sync_run_id,
            )
            db.commit()
            return False

        # Hash changed or force — re-register UDF
        app_logger.info(f"Survivorship UDF needs update (deployed: {(registry_item.deployed_hash or 'none')[:12]}..., current: {current_hash[:12]}...)")

        catalog_name = db_config_service.getDBConfigProperties(key="catalog_name")
        warehouse_id = db_config_service.getDBConfigProperties(key="cron_warehouse_id")

        if not catalog_name or not warehouse_id:
            app_logger.warning("catalog_name or cron_warehouse_id not configured — skipping UDF registration")
            return False

        from lakefusion_utility.utils.databricks_util import get_app_sp_token, generate_pat
        token = get_app_sp_token() or generate_pat()
        if not token:
            app_logger.warning("No token available for UDF registration — skipping")
            return False

        from lakefusion_utility.services.catalog_setup_service import CatalogSetupService
        svc = CatalogSetupService(
            token=token,
            warehouse_id=warehouse_id,
            catalog_name=catalog_name,
        )

        # Run with timeout to prevent blocking app startup
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(svc.register_survivorship_udf)
            try:
                success = future.result(timeout=60)
            except concurrent.futures.TimeoutError:
                app_logger.warning("Survivorship UDF registration timed out after 60s — skipping (will retry next sync)")
                return False

        if success:
            service.mark_imported(registry_item, file_hash=current_hash)
            service.log_audit(
                registry_id=registry_item.id, artifact_path=UDF_ARTIFACT_PATH,
                action='IMPORTED', from_version=registry_item.deployed_version,
                to_version=APP_VERSION, effective_policy='SYNC_ON_UPGRADE',
                reason='hash_changed' if not force else 'force_sync',
                sync_run_id=sync_run_id,
            )
            db.commit()
            app_logger.info(f"Survivorship UDF registered successfully (hash: {current_hash[:12]}...)")
        else:
            service.log_audit(
                registry_id=registry_item.id, artifact_path=UDF_ARTIFACT_PATH,
                action='SKIPPED', from_version=registry_item.deployed_version,
                to_version=APP_VERSION, effective_policy='SYNC_ON_UPGRADE',
                reason='registration_failed', sync_run_id=sync_run_id,
            )
            db.commit()
            app_logger.warning("Survivorship UDF registration failed")

        return success

    except Exception as e:
        app_logger.warning(f"Survivorship UDF sync failed (non-blocking): {e}")
        return False
