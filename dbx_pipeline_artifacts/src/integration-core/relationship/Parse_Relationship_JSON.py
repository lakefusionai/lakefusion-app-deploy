# Databricks notebook source
# MAGIC %pip install --upgrade "databricks-sdk>=0.89.0"
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

# Parse the relationship_sync.json snapshot uploaded by the middlelayer when
# the IntHub Relationship task was created. Hands the config off to the
# downstream Materialize_Edges notebook via Databricks task values.

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

# COMMAND ----------

if not catalog_name:
    raise ValueError("catalog_name widget is empty")
if not entity_id:
    raise ValueError("entity_id widget is empty")

snapshot_path = (
    f"/Volumes/{catalog_name}/metadata/metadata_files/"
    f"entity_{entity_id}_{experiment_id}_relationship_sync.json"
)
logger.info(f"Reading config from {snapshot_path}")

# COMMAND ----------

# Read the snapshot from the UC Volume. spark.read.text(wholetext=True) returns
# the entire file content in a single row's `value` column.
df = spark.read.text(snapshot_path, wholetext=True)
raw = df.collect()[0]["value"]
config = json.loads(raw)

if not config.get("relationships"):
    raise ValueError(
        f"relationship_sync.json at {snapshot_path} contains zero relationships"
    )

logger.info(f"Loaded {len(config['relationships'])} relationship(s) for task "
      f"'{config.get('task_name')}'")

# COMMAND ----------

# Pass the full config to the next notebook via task values. Materialize_Edges
# reads it back without needing to re-touch the UC Volume.
dbutils.jobs.taskValues.set(key="relationship_config", value=json.dumps(config))
dbutils.jobs.taskValues.set(
    key="relationship_count", value=str(len(config["relationships"]))
)
dbutils.jobs.taskValues.set(
    key="task_name", value=config.get("task_name", "")
)
logger.info("Task values set: relationship_config, relationship_count, task_name")
