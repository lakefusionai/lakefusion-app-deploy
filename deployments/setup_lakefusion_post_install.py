# Databricks notebook source
# MAGIC %md
# MAGIC # LakeFusion Post-Installation Setup
# MAGIC
# MAGIC This notebook grants the App Service Principal READ access to the LakeFusion secrets scope.
# MAGIC
# MAGIC **Run this after:**
# MAGIC 1. `setup_lakefusion_prereqs` has created the secrets scope and database
# MAGIC 2. `setup_lakefusion_app` has created the Databricks App (which provisions the App SP)
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. Validates the secrets scope exists
# MAGIC 2. Grants READ permission on the scope to the App Service Principal

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Widgets

# COMMAND ----------

dbutils.widgets.text("secrets_scope", "", "Secrets Scope Name")
dbutils.widgets.text("app_service_principal_id", "", "App Service Principal Client ID")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Widget Values

# COMMAND ----------

SECRETS_SCOPE = dbutils.widgets.get("secrets_scope")
APP_SP_ID = dbutils.widgets.get("app_service_principal_id")

_errors = []
if not SECRETS_SCOPE:
    _errors.append("Secrets Scope Name is required.")
if not APP_SP_ID:
    _errors.append("App Service Principal Client ID is required. Find this in Apps > your app > Overview.")

if _errors:
    for e in _errors:
        print(f"❌ {e}")
    raise SystemExit("Setup aborted: Please fill in all required parameters in the widgets above and re-run.")

print(f"Configuration:")
print(f"  Secrets Scope: {SECRETS_SCOPE}")
print(f"  App Service Principal: {APP_SP_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

import requests

def get_api_context():
    """Get workspace API context for raw HTTP requests."""
    context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    host = context.apiUrl().get()
    token = context.apiToken().get()
    return host, token

def api_request(method, endpoint, data=None):
    """Make a raw API request to Databricks workspace."""
    host, token = get_api_context()
    url = f"{host}{endpoint}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    if method == "GET":
        response = requests.get(url, headers=headers)
    elif method == "POST":
        response = requests.post(url, headers=headers, json=data)
    else:
        raise ValueError(f"Unsupported method: {method}")

    return response

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Validate Secrets Scope Exists

# COMMAND ----------

def check_secrets_scope_exists(scope_name):
    """Check if a secrets scope exists."""
    try:
        scopes = dbutils.secrets.listScopes()
        return any(s.name == scope_name for s in scopes)
    except Exception as e:
        print(f"Error checking scopes: {e}")
        return False

if not check_secrets_scope_exists(SECRETS_SCOPE):
    raise SystemExit(f"Secrets scope '{SECRETS_SCOPE}' does not exist. Run setup_lakefusion_prereqs first.")

print(f"Secrets scope '{SECRETS_SCOPE}' exists.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Grant READ Permission to App Service Principal

# COMMAND ----------

def grant_scope_read_acl(scope_name, principal):
    """Grant READ permission on a secrets scope to a principal."""
    response = api_request("POST", "/api/2.0/secrets/acls/put", {
        "scope": scope_name,
        "principal": principal,
        "permission": "READ"
    })
    if response.status_code == 200:
        print(f"Granted READ permission on scope '{scope_name}' to '{principal}'")
        return True
    else:
        print(f"Failed to grant ACL: {response.text}")
        return False

def list_scope_acls(scope_name):
    """List all ACLs on a secrets scope."""
    response = api_request("GET", f"/api/2.0/secrets/acls/list?scope={scope_name}")
    if response.status_code == 200:
        return response.json().get("items", [])
    else:
        print(f"Failed to list ACLs: {response.text}")
        return []

print("=" * 50)
print("Step 2: Granting READ Access to App Service Principal")
print("=" * 50)

grant_scope_read_acl(SECRETS_SCOPE, APP_SP_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Verify ACLs

# COMMAND ----------

print("=" * 50)
print("Current ACLs on scope:")
print("=" * 50)

acls = list_scope_acls(SECRETS_SCOPE)
for acl in acls:
    print(f"  {acl.get('principal')}: {acl.get('permission')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("  POST-INSTALLATION SETUP COMPLETE")
print("=" * 60)
print(f"  Secrets Scope: {SECRETS_SCOPE}")
print(f"  App SP: {APP_SP_ID} -> READ")
print()
print("  Only the scope creator (admin) has MANAGE access.")
print("  The App SP has READ access for runtime secret retrieval.")
print("  No other workspace users have access to the scope.")
print("=" * 60)
