# Databricks notebook source
# MAGIC %run ./parse_utils

# COMMAND ----------

version_info = get_version_info()

# COMMAND ----------

print(version_info)

# COMMAND ----------

# MAGIC
# MAGIC %run ./pipeline_attributes

# COMMAND ----------

import os
import sys
import glob
import importlib

def setup_lakefusion_engine():
    """
    Configure imports for lakefusion_core_engine.
    - Works from any nested notebook under dbx_pipeline_artifacts/src/**
    - Local dev: adds ../../lakefusion_core_engine to sys.path
    - Prod: installs latest wheel on ALL cluster nodes using %pip
    - Sets up external override path for pre/post execute customization
    """

    # Determine environment
    environment = version_info.get('environment', 'production')
    IS_LOCAL_DEV = environment == "local"

    # Step 1. Find the nearest `src` folder in the current path
    current_path = os.getcwd()
    path_parts = current_path.split(os.sep)

    src_index = None
    for i in range(len(path_parts) - 1, -1, -1):
        if path_parts[i] == "src":
            src_index = i
            break

    if src_index is None:
        raise RuntimeError("❌ Could not find 'src' folder in current path. "
                           "Make sure your notebook is under dbx_pipeline_artifacts/src.")

    src_path = os.sep.join(path_parts[:src_index + 1])  # path to .../dbx_pipeline_artifacts/src
    project_root = os.path.dirname(src_path)            # path to .../dbx_pipeline_artifacts

    print("project_root--", project_root)

    if IS_LOCAL_DEV:
        # --- Local Development Mode ---
        # Navigate two levels up to lakefusion_core_engine
        local_engine_path = os.path.abspath(os.path.join(project_root, "..", "lakefusion_core_engine"))

        if not os.path.exists(local_engine_path):
            raise FileNotFoundError(
                f"❌ Local lakefusion_core_engine folder not found at: {local_engine_path}"
            )

        if local_engine_path not in sys.path:
            sys.path.insert(0, local_engine_path)
        # Evict stale namespace from prior runs and refresh finder cache so
        # `import lakefusion_core_engine` re-resolves to the nested package.
        for _mod in [m for m in list(sys.modules) if m == "lakefusion_core_engine" or m.startswith("lakefusion_core_engine.")]:
            del sys.modules[_mod]
        importlib.invalidate_caches()
        print(f"✅ Using local lakefusion_core_engine from: {local_engine_path}")

    else:
        # --- Production Mode ---
        wheel_dir = os.path.join(project_root, "wheel")
        wheel_files = glob.glob(os.path.join(wheel_dir, "lakefusion_core_engine-*.whl"))

        if not wheel_files:
            raise FileNotFoundError(f"❌ No lakefusion_core_engine wheel found under: {wheel_dir}")

        wheel_files.sort(reverse=True)
        wheel_path = wheel_files[0]

        print(f"📦 Installing lakefusion_core_engine from: {os.path.basename(wheel_path)}")
        
        # CRITICAL: Use IPython magic command to install on all nodes
        try:
            from IPython import get_ipython
            ipython = get_ipython()
            
            if ipython is not None:
                # Use %pip magic - works with Spark Connect
                print("Using %pip magic command to install on all cluster nodes...")
                # --force-reinstall: the wheel version is pinned at 1.0.0, so a
                # plain --upgrade is a no-op when the cluster already has 1.0.0
                # installed — newly added submodules (e.g. write_ops) would never
                # land. Force the reinstall; --no-deps keeps it fast (no declared deps).
                ipython.run_line_magic('pip', f'install --upgrade --force-reinstall --no-deps {wheel_path}')
                print("✅ Installed wheel on all cluster nodes using %pip")
            else:
                raise RuntimeError("IPython not available")
                
        except Exception as e:
            print(f"❌ Failed to install using %pip: {e}")
            print("⚠️  This installation is driver-only and UDFs will fail!")
            import subprocess
            subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "--force-reinstall", "--no-deps", wheel_path])

    # --- Configure External Override Path ---
    # This enables auto-discovery of customer overrides from external/overrides/
    # Overrides mirror the folder structure of executors/tasks/
    override_path = os.path.join(project_root, "external", "overrides")
    try:
        from lakefusion_core_engine.executors import OverrideLoader
        OverrideLoader.set_override_path(override_path)
        if os.path.exists(override_path):
            print(f"✅ External overrides path configured: {override_path}")
        else:
            print(f"ℹ️  External overrides path (create to enable): {override_path}")
    except ImportError:
        # OverrideLoader not available in older versions - skip silently
        pass

    print("✅ lakefusion_core_engine setup complete.")


# COMMAND ----------

setup_lakefusion_engine()

# COMMAND ----------

import importlib

from lakefusion_core_engine.utils.logger_setup import init_logger

# COMMAND ----------

try:
    pipeline_type = get_pipeline_type(experiment_id)
except Exception:
    pipeline_type = "Integration_Hub"

try:
    job_id, task_run_id, task_key, run_id, task_name = get_pipeline_attributes()
except Exception as e:
    print(f"Could not retrieve pipeline attributes: {e}")
    print(f"Running in interactive/standalone mode")
    job_id, task_run_id, task_key, run_id, task_name = None, None, None, None, "interactive"

print(f"job_id = {job_id}, task_run_id ={task_run_id}, run_id ={run_id}, task_name ={task_name}")

# COMMAND ----------

class _PrintLogger:
    """Used when ENABLE_INTERACTIVE=True or when DatabricksLogger is unavailable."""
    def info(self, msg, *args, **kwargs):    print(f"[INFO]  {msg}")
    def warning(self, msg, *args, **kwargs): print(f"[WARN]  {msg}")
    def error(self, msg, *args, **kwargs):   print(f"[ERROR] {msg}")
    def debug(self, msg, *args, **kwargs):   print(f"[DEBUG] {msg}")

class _PrintLoggerInstance:
    """Stub logger_instance when DatabricksLogger is unavailable. Provides shutdown() and check_health() no-ops."""
    def shutdown(self):   pass
    def check_health(self): pass
    def get_logger(self): return _PrintLogger()

# COMMAND ----------

try:
    logger_instance, logger = init_logger(
        task_name=task_name or "interactive",
        run_id=run_id,
        task_run_id=task_run_id,
        pipeline_type=pipeline_type,
        job_id=job_id,
        catalog_name=catalog_name if 'catalog_name' in dir() and catalog_name else None
    )
except Exception as e:
    print(f"Could not initialize DatabricksLogger: {e}")
    print(f"Falling back to PrintLogger for console output")
    logger_instance = _PrintLoggerInstance()
    logger = _PrintLogger()

# COMMAND ----------

logger.info(f"Starting the logger for task {task_name}.......")
logger.info(
    f"Pipeline type={pipeline_type}, job_id={job_id}, run_id={run_id}, "
    f"task_name={task_name}, task_run_id={task_run_id}, task_key={task_key if 'task_key' in dir() else 'N/A'}"
)

# COMMAND ----------

def get_scope_name(default="lakefusion"):
    """Get the secret scope name from job parameters, falling back to default.
    
    In Databricks Apps, the scope is set to the app name (DATABRICKS_APP_NAME).
    In K8s/standalone, it defaults to 'lakefusion'.
    """
    try:
        scope = dbutils.widgets.get("secret_scope_name")
        if scope and scope.strip():
            return scope.strip()
    except Exception:
        pass
    return default

SECRET_SCOPE_NAME = get_scope_name()
logger.info(f"Secret scope: {SECRET_SCOPE_NAME}")