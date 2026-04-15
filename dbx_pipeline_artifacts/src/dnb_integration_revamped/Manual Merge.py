# Databricks notebook source
# MAGIC %md
# MAGIC # D&B Manual Merge (SDK Version)
# MAGIC
# MAGIC Allows manual selection and merging of D&B match results when
# MAGIC automatic merge is not possible due to multiple high-confidence matches.
# MAGIC
# MAGIC **Customization Points:**
# MAGIC - `PRE-EXECUTE`: Add custom logic before manual merge
# MAGIC - `POST-EXECUTE`: Add custom logic after manual merge

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Required Libraries

# COMMAND ----------

# MAGIC %pip install requests

# COMMAND ----------

import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("entity_name", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("id_value", "", "Primary Key Value")
dbutils.widgets.text("dnb_duns_value", "", "D-U-N-S Value")
dbutils.widgets.text("api_key", "", "D&B API Key")
dbutils.widgets.text("api_secret", "", "D&B API Secret")
dbutils.widgets.text("registration_id", "", "Monitoring Registration ID")
dbutils.widgets.dropdown("add_to_monitoring", "false", ["true", "false"], "Add to Monitoring")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameter Extraction

# COMMAND ----------

entity_name = dbutils.widgets.get("entity_name")
catalog_name = dbutils.widgets.get("catalog_name")
id_value = dbutils.widgets.get("id_value")
dnb_duns_value = dbutils.widgets.get("dnb_duns_value")
api_key = dbutils.widgets.get("api_key")
api_secret = dbutils.widgets.get("api_secret")
registration_id = dbutils.widgets.get("registration_id") or None
add_to_monitoring = dbutils.widgets.get("add_to_monitoring") == "true"

# COMMAND ----------

print("="*60)
print("D&B MANUAL MERGE (SDK VERSION)")
print("="*60)
print(f"Entity: {entity_name}")
print(f"Catalog: {catalog_name}")
print(f"ID Value: {id_value}")
print(f"D-U-N-S Value: {dnb_duns_value}")
print("="*60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup LakeFusion Core Engine

# COMMAND ----------

# MAGIC %run ../utils/execute_utils

# COMMAND ----------

setup_lakefusion_engine()

# COMMAND ----------

# DBTITLE 1,Configuration
ENABLE_INTERACTIVE = False  # Set to True for step-by-step debugging

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task Setup

# COMMAND ----------

# Use qualified imports for namespace clarity
from lakefusion_core_engine.executors import TaskContext
from lakefusion_core_engine.executors.tasks.dnb_integration import DNBManualMergeExecutor

# Create task context
context = TaskContext(
    task_name="dnb_manual_merge",
    entity=entity_name,
    experiment_id=None,
    catalog_name=catalog_name,
    spark=spark,
    dbutils=dbutils,
    params={
        "entity_name": entity_name,
        "id_value": id_value,
        "dnb_duns_value": dnb_duns_value,
        "api_key": api_key,
        "api_secret": api_secret,
        "registration_id": registration_id,
        "add_to_monitoring": add_to_monitoring
    }
)

# Create task instance
executor = DNBManualMergeExecutor(context)

print(f"Master Table: {executor.master_table}")
print(f"DNB DUNS Table: {executor.dnb_duns_table}")

# COMMAND ----------

# DBTITLE 1,Execute task
try:
    if not ENABLE_INTERACTIVE:
        result = executor.run()
    else:
        executor.interactive()
except Exception as e:
    print(f"Error running executor: {e}")
    raise

# COMMAND ----------

if not ENABLE_INTERACTIVE:
    # Display final status
    print("\n" + "=" * 80)
    print("D&B MANUAL MERGE COMPLETE")
    print("=" * 80)

    print(f"\nFinal Status: {result.status}")
    print(f"Message: {result.message}")

    if result.metrics:
        print(f"\nMetrics:")
        for key, value in result.metrics.items():
            print(f"  {key}: {value}")

    if result.task_values:
        print(f"\nTask Values (for downstream):")
        for key, value in result.task_values.items():
            print(f"  {key}: {value}")

