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
rbac_owner_emails_raw = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "rbac_owner_emails", default="[]", debugValue="[]")
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

# DBTITLE 1,Inlined RBAC apply (no executor)
from collections import defaultdict

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog

from lakefusion_core_engine.config.rbac_config_loader import (
    load_grants,
    entity_group_names,
    JOB_SENTINEL,
    MODEL_ENDPOINT_SENTINEL,
    VECTOR_ENDPOINT_SENTINEL,
)
from lakefusion_core_engine.utils.databricks_groups import (
    ensure_account_group,
    get_job_owner_id,
    grant_uc_object_permissions,
    set_job_permissions_with_owner,
    grant_serving_endpoint_permissions,
    grant_vector_search_endpoint_permissions,
    grant_uc_model_privileges,
    add_group_members_by_email,
)

PIPELINE_TYPE = "integration"
VS_ENDPOINT = None

_RESOURCE_TYPE_TO_SECURABLE = {
    "catalog": catalog.SecurableType.CATALOG,
    "schema": catalog.SecurableType.SCHEMA,
    "table": catalog.SecurableType.TABLE,
    "index": catalog.SecurableType.TABLE,
    "volume": catalog.SecurableType.VOLUME,
}
_VIEW_WRITE_PRIVILEGES = {"UPDATE", "INSERT", "DELETE", "MERGE", "MODIFY"}

print("=" * 80)
print("RBAC PERMISSIONS")
print("=" * 80)
print(f"  Entity: {entity}")
print(f"  Experiment ID: {experiment_id or 'prod'}")
print(f"  Catalog: {catalog_name}")
print(f"  Pipeline type: {PIPELINE_TYPE}")
print("=" * 80)

w = WorkspaceClient()
api = w.api_client

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
job_id = ctx.jobId().get() if ctx.jobId().isDefined() else None
print(f"  Job ID: {job_id}")

# COMMAND ----------

# DBTITLE 1,Load + group grants
rows = load_grants(
    entity=entity,
    experiment_id=experiment_id or "",
    catalog=catalog_name,
    pipeline_type=PIPELINE_TYPE,
    applies_to={"always", "prod", "experiment"},
    llm_model=llm_model_parsed,
    embedding_model=embedding_model_parsed,
)
is_prod = (experiment_id or "prod") == "prod"
filtered = []
for r in rows:
    if r.applies_to == "prod" and not is_prod:
        continue
    if r.applies_to == "experiment" and is_prod:
        continue
    filtered.append(r)

grants_by_resource = defaultdict(list)
for r in filtered:
    grants_by_resource[r.resource_type].append(r)
grants_by_resource = dict(grants_by_resource)

# COMMAND ----------

# DBTITLE 1,Pre-flight (skip if already in place)
def _group_exists(group_name):
    resp = api.do(
        "GET", "/api/2.0/account/scim/v2/Groups",
        query={"filter": f'displayName eq "{group_name}"'},
    )
    return bool(resp.get("Resources"))

def _check_uc_grants_for_principal(full_name, securable_type, principal, required_privileges):
    try:
        grants = w.grants.get(full_name=full_name, securable_type=securable_type)
        for assignment in (grants.privilege_assignments or []):
            if assignment.principal == principal:
                existing = {p.privilege.value for p in (assignment.privileges or [])}
                if set(required_privileges).issubset(existing):
                    return True
        return False
    except Exception:
        return False

groups = entity_group_names(entity)
dev_group = groups["developer"]
steward_group = groups["data_steward"]

skip_run = False
if _group_exists(dev_group) and _group_exists(steward_group):
    print("  Pre-flight: Both entity groups already exist")
    member_ok = True
    if rbac_owner_emails:
        resp = api.do(
            "GET", "/api/2.0/account/scim/v2/Groups",
            query={"filter": f'displayName eq "{dev_group}"'},
        )
        resources = resp.get("Resources", [])
        member_count = len(resources[0].get("members", [])) if resources else 0
        if member_count == 0:
            print(f"  Pre-flight: Group '{dev_group}' has no members — will add creators")
            member_ok = False
    if member_ok:
        schema_rows = grants_by_resource.get("schema", [])
        if schema_rows:
            probe = schema_rows[0]
            if _check_uc_grants_for_principal(
                probe.full_name, catalog.SecurableType.SCHEMA,
                probe.group_name, probe.privileges,
            ):
                print("  Pre-flight: All RBAC permissions already in place")
                skip_run = True

if skip_run:
    dbutils.notebook.exit(json.dumps({
        "status": "success",
        "message": f"RBAC permissions already in place for entity '{entity}'",
        "metrics": {"entity": entity, "experiment_id": experiment_id, "already_in_place": True},
    }))

# COMMAND ----------

# DBTITLE 1,Step 1 — Ensure entity groups + add creators
owner_user_id = None
if job_id:
    owner_user_id, owner_name = get_job_owner_id(w, job_id)
    if owner_user_id:
        print(f"  Job owner: {owner_name} (id={owner_user_id})")

for grp_name in groups.values():
    ensure_account_group(api, grp_name, owner_user_id=owner_user_id)

if rbac_owner_emails:
    print(f"  Adding creator emails to groups: {rbac_owner_emails}")
    for grp_name in groups.values():
        add_group_members_by_email(api, w, grp_name, rbac_owner_emails)

# COMMAND ----------

# DBTITLE 1,Step 2 — Apply UC grants (schema/table/index/volume)
def _effective_privileges(row):
    if row.resource_type != "table":
        return row.privileges
    try:
        info = w.tables.get(row.full_name)
        if getattr(info, "table_type", None) and str(info.table_type).upper().endswith("VIEW"):
            return [p for p in row.privileges if p not in _VIEW_WRITE_PRIVILEGES]
    except Exception:
        pass
    return row.privileges

uc_counts = defaultdict(lambda: {"granted": 0, "skipped": 0})
for resource_type in ("schema", "table", "index", "volume"):
    rows = grants_by_resource.get(resource_type, [])
    securable = _RESOURCE_TYPE_TO_SECURABLE[resource_type]
    for row in rows:
        privileges = _effective_privileges(row)
        result = grant_uc_object_permissions(
            w, row.full_name, securable, {row.group_name: privileges},
        )
        uc_counts[resource_type]["granted"] += len(result["results"])
        uc_counts[resource_type]["skipped"] += len(result["skipped"])
    print(f"  {resource_type}: {uc_counts[resource_type]['granted']} granted, {uc_counts[resource_type]['skipped']} skipped")

# COMMAND ----------

# DBTITLE 1,Step 3 — Grant job permissions
job_rows = grants_by_resource.get("job", [])
if job_id and job_rows:
    group_perms = {}
    for row in job_rows:
        if row.full_name != JOB_SENTINEL:
            continue
        group_perms.setdefault(row.group_name, []).extend(row.privileges)
    if group_perms:
        set_job_permissions_with_owner(w, job_id, group_perms)
        print(f"  Granted job perms to groups: {list(group_perms.keys())}")
else:
    print("  No job_id or job rows — skipping")

# COMMAND ----------

# DBTITLE 1,Step 4 — Grant model endpoint permissions
endpoint_rows = grants_by_resource.get("endpoint", [])
if endpoint_rows:
    group_perms = {}
    for row in endpoint_rows:
        if row.full_name != MODEL_ENDPOINT_SENTINEL:
            continue
        group_perms.setdefault(row.group_name, []).extend(row.privileges)
    for model_name in [m for m in (llm_model_parsed, embedding_model_parsed) if m]:
        grant_serving_endpoint_permissions(w, model_name, group_perms)
        print(f"  Granted endpoint perms on: {model_name}")

# COMMAND ----------

# DBTITLE 1,Step 4b — Grant vector endpoint permissions
vector_rows = grants_by_resource.get("vector_endpoint", [])
if vector_rows and VS_ENDPOINT:
    group_perms = {}
    for row in vector_rows:
        if row.full_name != VECTOR_ENDPOINT_SENTINEL:
            continue
        group_perms.setdefault(row.group_name, []).extend(row.privileges)
    if group_perms:
        grant_vector_search_endpoint_permissions(w, VS_ENDPOINT, group_perms)
        print(f"  Granted vector endpoint perms on: {VS_ENDPOINT}")

# COMMAND ----------

# DBTITLE 1,Step 5 — Grant UC model permissions
model_rows = grants_by_resource.get("uc_model", [])
for row in model_rows:
    grant_uc_model_privileges(spark, row.full_name, row.group_name, row.privileges)
    print(f"  Granted UC model perms: {row.full_name} -> {row.group_name}")

# COMMAND ----------

# DBTITLE 1,Done
print("=" * 80)
print("RBAC PERMISSIONS — COMPLETE")
print("=" * 80)
print(f"  Entity: {entity}")
print(f"  Experiment ID: {experiment_id or 'prod'}")

dbutils.notebook.exit(json.dumps({
    "status": "success",
    "message": f"RBAC permissions granted for entity '{entity}'",
    "metrics": {"entity": entity, "experiment_id": experiment_id},
}))
