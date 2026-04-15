# Databricks notebook source
# MAGIC %md
# MAGIC # D&B Match and Monitoring Accelerator (SDK v4.2.0)
# MAGIC
# MAGIC Integrates with Dun & Bradstreet Company Match API to match company data
# MAGIC and register for monitoring updates.
# MAGIC
# MAGIC **Features:**
# MAGIC - **Incremental Processing**: Only processes new/unprocessed records
# MAGIC - **Distributed API Calls**: Uses mapInPandas for parallel processing (~4 TPS)
# MAGIC - **Graceful Error Handling**: HTTP 401/429+00050 errors save progress before exit
# MAGIC - **Smart Scoring**: AUTO_MERGED, IN_REVIEW, NOT_A_MATCH with proper low-confidence handling
# MAGIC - **Deduplication**: MERGE INTO on lakefusion_id + dnb_duns
# MAGIC - **Hierarchy Enrichment**: Optional corporate hierarchy data fetch
# MAGIC
# MAGIC **Customization Points:**
# MAGIC - `PRE-EXECUTE`: Add custom logic before D&B matching
# MAGIC - `POST-EXECUTE`: Add custom logic after D&B matching

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

dbutils.widgets.text("entity_dnb_settings", "", "Entity DNB Settings")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("api_key", "", "D&B API Key")
dbutils.widgets.text("api_secret", "", "D&B API Secret")
dbutils.widgets.text("limit_records", "10", "Limit Records to Process")
dbutils.widgets.text("registration_id", "", "Monitoring Registration ID")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameter Extraction

# COMMAND ----------

entity_dnb_settings = dbutils.widgets.get("entity_dnb_settings")
entity = dbutils.widgets.get("entity")
catalog_name = dbutils.widgets.get("catalog_name")
api_key = dbutils.widgets.get("api_key")
api_secret = dbutils.widgets.get("api_secret")
limit_records_str = dbutils.widgets.get("limit_records")
limit_records = int(limit_records_str) if limit_records_str and limit_records_str.strip() else None  # None = process all
registration_id = dbutils.widgets.get("registration_id") or None

# COMMAND ----------

# Get task values from upstream
entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
entity_dnb_settings = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_dnb_settings", debugValue=entity_dnb_settings)

# COMMAND ----------

print("="*60)
print("D&B MATCH AND MONITORING ACCELERATOR (SDK v4.2.0)")
print("="*60)
print(f"Entity: {entity}")
print(f"Catalog: {catalog_name}")
print(f"Limit Records: {limit_records if limit_records else 'All (incremental)'}")
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
from lakefusion_core_engine.executors.tasks.dnb_integration import DNBMatchMonitoringExecutor

# Create task context
context = TaskContext(
    task_name="dnb_match_monitoring",
    entity=entity,
    experiment_id=None,
    catalog_name=catalog_name,
    spark=spark,
    dbutils=dbutils,
    params={
        "entity_dnb_settings": entity_dnb_settings,
        "api_key": api_key,
        "api_secret": api_secret,
        "limit_records": limit_records,
        "registration_id": registration_id
    }
)

# Create task instance
executor = DNBMatchMonitoringExecutor(context)

print(f"Master Table: {executor.master_table}")
print(f"DNB DUNS Table (gold): {executor.dnb_duns_table}")
print(f"DNB Silver Table: {executor.dnb_silver_table}")
print(f"DNB Error Table: {executor.dnb_error_table}")
if executor.hierarchy_enabled:
    print(f"DNB Enrichment Table: {executor.dnb_enrichment_table}")
print(f"Hierarchy Enrichment: {'Enabled' if executor.hierarchy_enabled else 'Disabled'}")

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
    print("D&B MATCH AND MONITORING COMPLETE")
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

    # Check for API blocking and exit gracefully if needed
    if result.task_values and result.task_values.get("api_blocked"):
        print("\n" + "=" * 80)
        print("WARNING: API was blocked during execution")
        print("All successful results have been saved.")
        print("Request a new API token and rerun for remaining records.")
        print("=" * 80)
        dbutils.notebook.exit("API_BLOCKED: All successful results saved. Request a new API token and rerun.")

