# Databricks notebook source
import json

# COMMAND ----------

# Widget definitions
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("experiment_id", "", "Integration Hub Experiment ID")
dbutils.widgets.text("embedding_model", "", "Embedding Model")
dbutils.widgets.text("llm_model", "", "LLM Model")
dbutils.widgets.text("llm_provisionless", "", "LLM Provisionless")

# COMMAND ----------

# Parameter extraction
catalog_name = dbutils.widgets.get("catalog_name")
entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity")
experiment_id = dbutils.widgets.get("experiment_id")
experiment_id = experiment_id.replace("-", "")
embedding_model = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "embedding_model")
llm_model = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "llm_model")
embedding_model_parsed = embedding_model.replace("/", "-").lower()
llm_provisionless = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "llm_provisionless")
llm_model_parsed = "lakefusion-" + llm_model if not llm_provisionless else None
rbac_owner_emails_raw = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "rbac_owner_emails", debugValue="[]")
rbac_owner_emails = json.loads(rbac_owner_emails_raw) if isinstance(rbac_owner_emails_raw, str) else rbac_owner_emails_raw

# COMMAND ----------

print("entity", entity)
print("experiment_id", experiment_id)
print("llm_model", llm_model)
print("llm_model_parsed", llm_model_parsed)
print("embedding_model", embedding_model)
print("embedding_model_parsed", embedding_model_parsed)
print("rbac_owner_emails", rbac_owner_emails)

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

setup_lakefusion_engine()

# COMMAND ----------

# DBTITLE 1,Configuration
ENABLE_INTERACTIVE = False  # Set to True for step-by-step debugging

# COMMAND ----------

from lakefusion_core_engine.executors import TaskContext, create_task_executor

# COMMAND ----------

# Build pipeline-specific params from centralized config
from lakefusion_core_engine.config.rbac_config_loader import build_params
import re as _re

# Extract workspace ID from DATABRICKS_HOST for admin group name resolution
_dbx_host = spark.conf.get("spark.databricks.workspaceUrl", "")
_ws_match = _re.search(r"adb-(\d+)\.", _dbx_host)
workspace_id = _ws_match.group(1) if _ws_match else ""

params = build_params(
    pipeline_type="integration",
    entity=entity,
    experiment_id=experiment_id,
    catalog_name=catalog_name,
    llm_model_parsed=llm_model_parsed,
    embedding_model_parsed=embedding_model_parsed,
    creator_emails=rbac_owner_emails,
    workspace_id=workspace_id,
)

# COMMAND ----------

context = TaskContext(
    task_name="RBAC_Permissions",
    entity=entity,
    experiment_id=experiment_id if experiment_id else None,
    catalog_name=catalog_name,
    dataset_objects=[],
    spark=spark,
    dbutils=dbutils,
    params=params,
)

# COMMAND ----------

# DBTITLE 1,Execute task
executor = create_task_executor('integration_core.rbac_permissions', context)

if not ENABLE_INTERACTIVE:
    result = executor.run()
else:
    executor.interactive()
    # In subsequent cells, use:
    # executor.print_steps()
    # executor.run_step(1)
    # executor.run_step(2)
    # ...

# COMMAND ----------

# DBTITLE 1,Display result
if not ENABLE_INTERACTIVE:
    print(f"Status: {result.status.value}")
    print(f"Message: {result.message}")
    if result.metrics:
        print(f"Metrics: {result.metrics}")

# COMMAND ----------

# Exit with result
if not ENABLE_INTERACTIVE:
    if result.is_failed:
        dbutils.notebook.exit(json.dumps({
            "status": "failed",
            "message": result.message,
            "errors": result.errors
        }))
    else:
        dbutils.notebook.exit(json.dumps({
            "status": "success",
            "message": result.message,
            "metrics": result.metrics
        }))
