# Databricks notebook source
import json

# COMMAND ----------

# Widget definitions
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("entity_id", "", "Entity ID")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("lakebase_instance_id", "", "Lakebase Instance ID")
dbutils.widgets.text("lakebase_endpoint_id", "", "Lakebase Endpoint ID")
dbutils.widgets.text("lakebase_branch_id", "", "Lakebase Branch ID")
dbutils.widgets.text("write_mode", "", "Write Mode")

# COMMAND ----------

# Parameter extraction
catalog_name = dbutils.widgets.get("catalog_name")
entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity")
experiment_id = dbutils.widgets.get("experiment_id")
entity_type = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_type", default="reference", debugValue="reference")
storage_type = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "storage_type", default="delta", debugValue="delta")
rbac_owner_emails_raw = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "rbac_owner_emails", default="[]", debugValue="[]")
rbac_owner_emails = json.loads(rbac_owner_emails_raw) if isinstance(rbac_owner_emails_raw, str) else rbac_owner_emails_raw
lakebase_instance_id = dbutils.widgets.get("lakebase_instance_id")

# COMMAND ----------

print("=" * 80)
print("RBAC PERMISSIONS — REFERENCE ENTITY")
print("=" * 80)
print(f"  Entity: {entity}")
print(f"  Entity type: {entity_type}")
print(f"  Storage type: {storage_type}")
print(f"  Catalog: {catalog_name}")
print(f"  Lakebase instance: {lakebase_instance_id}")
print("=" * 80)

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

setup_lakefusion_engine()

# COMMAND ----------

# DBTITLE 1,Setup
from collections import defaultdict

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog

from lakefusion_core_engine.config.rbac_config_loader import (
    load_grants,
    entity_group_names,
)
from lakefusion_core_engine.utils.databricks_groups import (
    ensure_account_group,
    grant_uc_object_permissions,
    add_group_members_by_email,
    grant_lakebase_project_access,
    grant_postgres_table_permissions,
)
from lakefusion_core_engine.lakebase_client import LakebasePgClient

PIPELINE_TYPE = "integration"

_RESOURCE_TYPE_TO_SECURABLE = {
    "catalog": catalog.SecurableType.CATALOG,
    "schema": catalog.SecurableType.SCHEMA,
    "table": catalog.SecurableType.TABLE,
    "volume": catalog.SecurableType.VOLUME,
}

w = WorkspaceClient()
api = w.api_client

groups = entity_group_names(entity)
dev_group = groups["developer"]
steward_group = groups["data_steward"]
group_list = [dev_group, steward_group]

# COMMAND ----------

# DBTITLE 1,Step 1 — Ensure entity groups + add creators
for grp_name in groups.values():
    ensure_account_group(api, grp_name)

if rbac_owner_emails:
    print(f"  Adding creator emails to groups: {rbac_owner_emails}")
    for grp_name in groups.values():
        add_group_members_by_email(api, w, grp_name, rbac_owner_emails)

# COMMAND ----------

# DBTITLE 1,Step 2 — Apply UC grants on Delta reference tables
rows = load_grants(
    entity=entity,
    experiment_id=experiment_id or "",
    catalog=catalog_name,
    pipeline_type=PIPELINE_TYPE,
    applies_to={"always", "prod"},
)

uc_counts = defaultdict(lambda: {"granted": 0, "skipped": 0})
for resource_type in ("catalog", "schema", "table", "volume"):
    resource_rows = [r for r in rows if r.resource_type == resource_type]
    securable = _RESOURCE_TYPE_TO_SECURABLE.get(resource_type)
    if not securable:
        continue
    for row in resource_rows:
        result = grant_uc_object_permissions(
            w, row.full_name, securable, {row.group_name: row.privileges},
        )
        uc_counts[resource_type]["granted"] += len(result["results"])
        uc_counts[resource_type]["skipped"] += len(result["skipped"])
    print(f"  {resource_type}: {uc_counts[resource_type]['granted']} granted, {uc_counts[resource_type]['skipped']} skipped")

# COMMAND ----------

# DBTITLE 1,Step 3 — Lakebase project + Postgres grants (Lakebase storage only)
if storage_type == "lakebase":
    print("  Lakebase storage — applying project + Postgres grants")

    # Grant CAN_USE on Lakebase project
    if lakebase_instance_id:
        try:
            grant_lakebase_project_access(w, f"projects/{lakebase_instance_id}", group_list)
        except Exception as e:
            print(f"  Failed granting Lakebase project access: {e}")
    else:
        print("  Skipping Lakebase project grant — lakebase_instance_id not configured")

    # Postgres table grants
    entity_snake = entity.lower().replace(" ", "_")
    pg_schema = "gold"
    synced_tables = [
        f"{entity_snake}_reference_prod_synced",
        f"{entity_snake}_reference_audit_prod_synced",
        f"{entity_snake}_reference_conflict_queue_prod_synced",
    ]
    try:
        pg = LakebasePgClient(
            instance_id=lakebase_instance_id,
            database="databricks_postgres",
            workspace_client=w,
        )
        with pg:
            grant_postgres_table_permissions(pg, pg_schema, synced_tables, group_list)
        print(f"  Postgres grants applied for {len(synced_tables)} tables")
    except Exception as e:
        print(f"  Failed applying Postgres grants: {e}")
else:
    print("  Delta storage — UC grants applied in Step 2, no Postgres grants needed")

# COMMAND ----------

# DBTITLE 1,Done
print("=" * 80)
print("RBAC PERMISSIONS — REFERENCE ENTITY COMPLETE")
print("=" * 80)
print(f"  Entity: {entity}")
print(f"  Storage type: {storage_type}")

dbutils.notebook.exit(json.dumps({
    "status": "success",
    "message": f"RBAC permissions granted for reference entity '{entity}'",
    "metrics": {"entity": entity, "entity_type": entity_type, "storage_type": storage_type},
}))
