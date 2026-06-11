# Databricks notebook source
# MAGIC %pip install --upgrade "databricks-sdk>=0.89.0"
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

# Final stage of the relationship sync DAG. Logs the per-relationship summary
# emitted by Materialize_Edges so the IntHub UI run-page can show counts at a
# glance.

import json

# COMMAND ----------

dbutils.widgets.text("integration_hub_relationship_id", "", "IntHub Relationship Task ID")
dbutils.widgets.text("entity_id", "", "Driving Entity ID")
dbutils.widgets.text("experiment_id", "prod", "Experiment ID")
dbutils.widgets.text("catalog_name", "", "catalog name")

integration_hub_relationship_id = dbutils.widgets.get("integration_hub_relationship_id")
entity_id = dbutils.widgets.get("entity_id")
experiment_id = dbutils.widgets.get("experiment_id") or "prod"
catalog_name = dbutils.widgets.get("catalog_name")

task_name = dbutils.jobs.taskValues.get(
    taskKey="Parse_Relationship_JSON",
    key="task_name",
    debugValue="",
)

summary_json = dbutils.jobs.taskValues.get(
    taskKey="Materialize_Edges",
    key="materialize_summary",
    debugValue="{}",
)
summary = json.loads(summary_json) if summary_json else {}

# COMMAND ----------

logger.info("=" * 60)
logger.info(f"RELATIONSHIP SYNC SUMMARY  ·  task={task_name or '(unknown)'}")
logger.info(f"  integration_hub_relationship_id: {integration_hub_relationship_id}")
logger.info(f"  entity_id:     {entity_id}")
logger.info(f"  experiment_id: {experiment_id}")
logger.info(f"  catalog_name:  {catalog_name}")
logger.info("-" * 60)
relationships = summary.get("relationships", []) or []
logger.info(f"  relationships processed : {len(relationships)}")
logger.info(f"  rows promoted           : {summary.get('rows_promoted', 0)}")
logger.info(f"  rows DLQ                : {summary.get('rows_dlq', 0)}")
logger.info("-" * 60)

for rel in relationships:
    name      = rel.get("relationship", "?")
    promoted  = rel.get("promoted", 0)
    dlq       = rel.get("dlq", 0)
    err       = rel.get("error")
    if err:
        logger.warning(f"  ✗ {name:30s} promoted={promoted:>6d}  dlq={dlq:>6d}  ERROR={err}")
    else:
        logger.info(f"  ✓ {name:30s} promoted={promoted:>6d}  dlq={dlq:>6d}")

logger.info("=" * 60)

# COMMAND ----------

# Re-publish the summary for downstream consumers / job-run history.
dbutils.jobs.taskValues.set(key="materialize_summary", value=json.dumps(summary))
