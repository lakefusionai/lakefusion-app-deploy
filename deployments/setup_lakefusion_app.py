# Databricks notebook source
# MAGIC %md
# MAGIC # LakeFusion App Setup Notebook
# MAGIC
# MAGIC This notebook helps you set up the LakeFusion Databricks App with all required configurations:
# MAGIC 1. Create Databricks App (if not exists)
# MAGIC 2. Create Lakebase OLTP Database (if not exists)
# MAGIC 3. Set up secrets scope and secrets
# MAGIC 4. Configure app resources (database, secrets)
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Databricks workspace admin access
# MAGIC - Service principal for OIDC authentication (optional, for SSO)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Dependencies

# COMMAND ----------

# MAGIC %pip install databricks-sdk>=0.77.0 --upgrade

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Widgets

# COMMAND ----------

# Widget definitions
dbutils.widgets.text("app_name", "lakefusionai", "App Name")
dbutils.widgets.text("app_description", "LakeFusion AI", "App Description")
dbutils.widgets.text("source_code_path", "/Workspace/Users/{user}/lakefusion-app-deploy", "Source Code Path")
dbutils.widgets.text("database_name", "lakefusion-db", "Lakebase Instance Name")
dbutils.widgets.text("internal_db_name", "lakefusion_transactional_db", "PostgreSQL Database Name")
dbutils.widgets.text("secrets_scope", "lakefusion", "Secrets Scope Name")
dbutils.widgets.text("oidc_client_id", "", "OIDC Client ID (for SSO)")
dbutils.widgets.text("oidc_client_secret", "", "OIDC Client Secret (for SSO)")
dbutils.widgets.text("databricks_dapi", "", "Databricks DAPI Token")
dbutils.widgets.dropdown("create_database", "true", ["true", "false"], "Create Lakebase Database")
dbutils.widgets.dropdown("create_secrets", "true", ["true", "false"], "Create Secrets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Widget Values

# COMMAND ----------

# Get configuration values
APP_NAME = dbutils.widgets.get("app_name")
APP_DESCRIPTION = dbutils.widgets.get("app_description")
SOURCE_CODE_PATH = dbutils.widgets.get("source_code_path")
DATABASE_NAME = dbutils.widgets.get("database_name")
INTERNAL_DB_NAME = dbutils.widgets.get("internal_db_name")
SECRETS_SCOPE = dbutils.widgets.get("secrets_scope")
OIDC_CLIENT_ID = dbutils.widgets.get("oidc_client_id")
OIDC_CLIENT_SECRET = dbutils.widgets.get("oidc_client_secret")
DATABRICKS_DAPI = dbutils.widgets.get("databricks_dapi")
CREATE_DATABASE = dbutils.widgets.get("create_database") == "true"
CREATE_SECRETS = dbutils.widgets.get("create_secrets") == "true"

# Replace {user} placeholder in source code path
current_user = spark.sql("SELECT current_user()").collect()[0][0]
if "{user}" in SOURCE_CODE_PATH:
    SOURCE_CODE_PATH = SOURCE_CODE_PATH.replace("{user}", current_user)

print(f"Configuration:")
print(f"  App Name: {APP_NAME}")
print(f"  App Description: {APP_DESCRIPTION}")
print(f"  Source Code Path: {SOURCE_CODE_PATH}")
print(f"  Lakebase Instance Name: {DATABASE_NAME}")
print(f"  PostgreSQL Database Name: {INTERNAL_DB_NAME}")
print(f"  Secrets Scope: {SECRETS_SCOPE}")
print(f"  Create Database: {CREATE_DATABASE}")
print(f"  Create Secrets: {CREATE_SECRETS}")
print(f"  OIDC Client ID: {'[SET]' if OIDC_CLIENT_ID else '[NOT SET]'}")
print(f"  OIDC Client Secret: {'[SET]' if OIDC_CLIENT_SECRET else '[NOT SET]'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

import requests
import time
import uuid
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import DatabaseInstance

# Initialize the Databricks SDK WorkspaceClient
# When running in a notebook, it automatically uses the notebook context for authentication
w = WorkspaceClient()

def get_api_context():
    """Get workspace API context for raw HTTP requests (used for secrets API)."""
    context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    host = context.apiUrl().get()
    token = context.apiToken().get()
    return host, token

def api_request(method, endpoint, data=None):
    """Make a raw API request to Databricks workspace (for secrets API)."""
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
    elif method == "PUT":
        response = requests.put(url, headers=headers, json=data)
    elif method == "PATCH":
        response = requests.patch(url, headers=headers, json=data)
    elif method == "DELETE":
        response = requests.delete(url, headers=headers)
    else:
        raise ValueError(f"Unsupported method: {method}")

    return response

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Check/Create Secrets Scope

# COMMAND ----------

def check_secrets_scope_exists(scope_name):
    """Check if a secrets scope exists."""
    try:
        scopes = dbutils.secrets.listScopes()
        return any(s.name == scope_name for s in scopes)
    except Exception as e:
        print(f"Error checking scopes: {e}")
        return False

def create_secrets_scope(scope_name):
    """Create a secrets scope."""
    response = api_request("POST", "/api/2.0/secrets/scopes/create", {
        "scope": scope_name,
        "initial_manage_principal": "users"
    })
    if response.status_code == 200:
        print(f"Created secrets scope: {scope_name}")
        return True
    elif response.status_code == 400 and "SCOPE_ALREADY_EXISTS" in response.text:
        print(f"Secrets scope already exists: {scope_name}")
        return True
    else:
        print(f"Failed to create secrets scope: {response.text}")
        return False

def put_secret(scope_name, key, value):
    """Put a secret in a scope."""
    if not value:
        print(f"Skipping secret {key} - no value provided")
        return True

    response = api_request("POST", "/api/2.0/secrets/put", {
        "scope": scope_name,
        "key": key,
        "string_value": value
    })
    if response.status_code == 200:
        print(f"Set secret: {scope_name}/{key}")
        return True
    else:
        print(f"Failed to set secret {key}: {response.text}")
        return False

# Check and create secrets scope
if CREATE_SECRETS:
    print("=" * 50)
    print("Step 1: Setting up Secrets Scope")
    print("=" * 50)

    if not check_secrets_scope_exists(SECRETS_SCOPE):
        create_secrets_scope(SECRETS_SCOPE)
    else:
        print(f"Secrets scope already exists: {SECRETS_SCOPE}")

    # Set OIDC secrets if provided
    if OIDC_CLIENT_ID:
        put_secret(SECRETS_SCOPE, "DATABRICKS_OIDC_CLIENT_ID", OIDC_CLIENT_ID)
    if OIDC_CLIENT_SECRET:
        put_secret(SECRETS_SCOPE, "DATABRICKS_OIDC_CLIENT_SECRET", OIDC_CLIENT_SECRET)
    if DATABRICKS_DAPI:
        put_secret(SECRETS_SCOPE, "LAKEFUSION_DATABRICKS_DAPI", DATABRICKS_DAPI)
else:
    print("Skipping secrets setup (CREATE_SECRETS=false)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Check/Create Lakebase Database Instance

# COMMAND ----------

def list_lakebase_databases():
    """List all Lakebase OLTP database instances using SDK."""
    try:
        instances = list(w.database.list_database_instances())
        return instances
    except Exception as e:
        print(f"Failed to list database instances: {e}")
        return []

def check_lakebase_database_exists(db_name):
    """Check if a Lakebase database instance exists."""
    databases = list_lakebase_databases()
    return any(db.name == db_name for db in databases)

def create_lakebase_database(db_name):
    """Create a Lakebase OLTP database instance using SDK."""
    try:
        result = w.database.create_database_instance(
            DatabaseInstance(
                name=db_name,
                capacity="CU_1",
                retention_window_in_days=14,
                enable_pg_native_login=True
            )
        )
        print(f"Created Lakebase database instance: {db_name}")
        return result
    except Exception as e:
        error_msg = str(e)
        if "ALREADY_EXISTS" in error_msg or "409" in error_msg:
            print(f"Lakebase database instance already exists: {db_name}")
            return get_lakebase_database(db_name)
        else:
            print(f"Failed to create database instance: {e}")
            return None

def enable_native_pg_login(instance_name):
    """Enable native PostgreSQL login on an existing instance using SDK."""
    try:
        w.database.update_database_instance(
            name=instance_name,
            enable_pg_native_login=True
        )
        print(f"Enabled native PostgreSQL login for: {instance_name}")
        return True
    except Exception as e:
        print(f"Failed to enable native pg login: {e}")
        return False

def get_lakebase_database(db_name):
    """Get Lakebase database instance details using SDK."""
    try:
        return w.database.get_database_instance(name=db_name)
    except Exception:
        return None

if CREATE_DATABASE:
    print("=" * 50)
    print("Step 2: Setting up Lakebase Database Instance")
    print("=" * 50)

    db_info = get_lakebase_database(DATABASE_NAME)
    if db_info:
        print(f"Lakebase database instance already exists: {DATABASE_NAME}")
        print(f"Connection endpoint: {db_info.read_write_dns or 'N/A'}")
    else:
        print(f"Creating Lakebase database instance: {DATABASE_NAME}")
        db_info = create_lakebase_database(DATABASE_NAME)
        if db_info:
            print("Database instance created successfully")
else:
    db_info = None
    print("Skipping database setup (CREATE_DATABASE=false)")

# COMMAND ----------

db_info = get_lakebase_database(DATABASE_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2b: Create PostgreSQL Database in Instance

# COMMAND ----------

def start_lakebase_instance(instance_name):
    """Start a Lakebase database instance using SDK."""
    try:
        w.database.start_database_instance(name=instance_name)
        print(f"Starting instance: {instance_name}")
        return True
    except Exception as e:
        print(f"Failed to start instance: {e}")
        return False

def wait_for_instance_running(instance_name, timeout=300):
    """Wait for instance to be in RUNNING/AVAILABLE state."""
    print(f"Waiting for instance {instance_name} to be RUNNING/AVAILABLE...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        db_info = get_lakebase_database(instance_name)
        if db_info:
            state = str(db_info.state) if db_info.state else None
            print(f"  Current state: {state}")
            if state in ["RUNNING", "AVAILABLE", "State.RUNNING", "State.AVAILABLE"]:
                return True
            elif state in ["FAILED", "DELETED", "State.FAILED", "State.DELETED"]:
                print(f"Instance in terminal state: {state}")
                return False
        time.sleep(10)
    print("Timeout waiting for instance to start")
    return False

def get_postgres_credentials(instance_name):
    """Get credentials for PostgreSQL connection using Databricks SDK.

    Uses generate_database_credential() to get a short-lived OAuth token
    for Lakebase OLTP authentication from notebooks.
    """
    # Get current user for username
    context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    username = context.userName().get()

    # Generate database credential (short-lived OAuth token)
    cred = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[instance_name]
    )

    return username, cred.token

def check_postgres_database_exists(instance_name, db_name):
    """Check if a PostgreSQL database exists within the Lakebase instance."""
    import psycopg2

    db_info = get_lakebase_database(instance_name)
    if not db_info:
        return False

    host = db_info.read_write_dns
    username, token = get_postgres_credentials(instance_name)

    try:
        conn = psycopg2.connect(
            host=host,
            port=5432,
            database="postgres",
            user=username,
            password=token,
            sslmode="require"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
        exists = cursor.fetchone() is not None
        cursor.close()
        conn.close()
        return exists
    except Exception as e:
        print(f"Failed to check database: {e}")
        return False

def create_postgres_database(instance_name, db_name):
    """Create a PostgreSQL database within the Lakebase instance."""
    import psycopg2
    from psycopg2 import sql

    db_info = get_lakebase_database(instance_name)
    if not db_info:
        print(f"Could not get instance info for {instance_name}")
        return False

    host = db_info.read_write_dns
    username, token = get_postgres_credentials(instance_name)

    try:
        conn = psycopg2.connect(
            host=host,
            port=5432,
            database="postgres",
            user=username,
            password=token,
            sslmode="require"
        )
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
        print(f"Created PostgreSQL database: {db_name}")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Failed to create PostgreSQL database: {e}")
        return False

# Execute Step 2b: Create PostgreSQL database in the instance
if CREATE_DATABASE and db_info:
    print("=" * 50)
    print("Step 2b: Creating PostgreSQL Database in Instance")
    print("=" * 50)

    # Check if instance is running, start if needed
    current_state = str(db_info.state) if db_info.state else None

    if current_state in ["STOPPED", "State.STOPPED"]:
        print(f"Instance is stopped, starting...")
        if start_lakebase_instance(DATABASE_NAME):
            if not wait_for_instance_running(DATABASE_NAME):
                print("Failed to start instance, skipping database creation")
    elif current_state not in ["RUNNING", "AVAILABLE", "State.RUNNING", "State.AVAILABLE", "DatabaseInstanceState.AVAILABLE"]:
        print(f"Instance state is {current_state}, waiting for RUNNING/AVAILABLE...")
        if not wait_for_instance_running(DATABASE_NAME):
            print("Instance not ready, skipping database creation")

    # Refresh instance info
    db_info = get_lakebase_database(DATABASE_NAME)

    # Enable native PostgreSQL login if not already enabled
    if db_info and not getattr(db_info, "effective_enable_pg_native_login", False):
        print("Enabling native PostgreSQL login...")
        enable_native_pg_login(DATABASE_NAME)
        # Wait a bit for the setting to take effect
        time.sleep(5)

    current_state = str(db_info.state) if db_info and db_info.state else None
    if db_info and current_state in ["RUNNING", "AVAILABLE", "State.RUNNING", "State.AVAILABLE", "DatabaseInstanceState.AVAILABLE"]:
        if check_postgres_database_exists(DATABASE_NAME, INTERNAL_DB_NAME):
            print(f"PostgreSQL database already exists: {INTERNAL_DB_NAME}")
        else:
            print(f"Creating PostgreSQL database: {INTERNAL_DB_NAME}")
            create_postgres_database(DATABASE_NAME, INTERNAL_DB_NAME)
    else:
        print("Instance not running/available, cannot create PostgreSQL database")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2c: Create Database and Register as Unity Catalog
# MAGIC
# MAGIC Create a new PostgreSQL database and register it as a Unity Catalog catalog in one step.

# COMMAND ----------

from databricks.sdk.service.database import DatabaseCatalog

# Widget for catalog name
dbutils.widgets.text("catalog_name", "lakefusion_ai", "Unity Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

def create_database_and_catalog(instance_name, db_name, catalog_name):
    """Create a new PostgreSQL database and register it as a Unity Catalog catalog."""
    try:
        catalog = w.database.create_database_catalog(
            DatabaseCatalog(
                name=catalog_name,
                database_instance_name=instance_name,
                database_name=db_name,
                create_database_if_not_exists=True
            )
        )
        print(f"Created database and catalog successfully!")
        print(f"  Catalog Name: {catalog.name}")
        print(f"  Database Name: {db_name}")
        print(f"  Instance: {instance_name}")
        return catalog
    except Exception as e:
        error_msg = str(e)
        if "ALREADY_EXISTS" in error_msg or "already exists" in error_msg.lower():
            print(f"Catalog already exists: {catalog_name}")
            return {"name": catalog_name, "database_name": db_name}
        else:
            print(f"Failed to create database catalog: {e}")
            return None

def grant_catalog_privileges(instance_name, db_name):
    """Grant privileges on the public schema to allow table creation."""
    import psycopg2

    db_info = get_lakebase_database(instance_name)
    if not db_info:
        print(f"Could not get instance info for {instance_name}")
        return False

    host = db_info.read_write_dns
    username, token = get_postgres_credentials(instance_name)

    try:
        conn = psycopg2.connect(
            host=host,
            port=5432,
            database=db_name,
            user=username,
            password=token,
            sslmode="require"
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Grant privileges on public schema
        cursor.execute("GRANT ALL ON SCHEMA public TO PUBLIC;")
        cursor.execute("GRANT CREATE ON SCHEMA public TO PUBLIC;")
        cursor.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO PUBLIC;")
        cursor.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO PUBLIC;")
        print(f"Granted schema privileges on database: {db_name}")

        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Failed to grant privileges: {e}")
        return False

# Execute Step 2c: Create database and catalog
if CREATE_DATABASE and db_info:
    print("=" * 50)
    print("Step 2c: Creating Database and Unity Catalog")
    print("=" * 50)

    print(f"Catalog Name: {CATALOG_NAME}")
    print(f"PostgreSQL Database: {INTERNAL_DB_NAME}")
    print(f"Lakebase Instance: {DATABASE_NAME}")

    current_state = str(db_info.state) if db_info and db_info.state else None
    if db_info and current_state in ["RUNNING", "AVAILABLE", "State.RUNNING", "State.AVAILABLE", "DatabaseInstanceState.AVAILABLE"]:
        # Create database and register as catalog
        print(f"\nCreating database '{INTERNAL_DB_NAME}' and catalog '{CATALOG_NAME}'...")
        catalog = create_database_and_catalog(DATABASE_NAME, INTERNAL_DB_NAME, CATALOG_NAME)

        if catalog:
            # Grant privileges for table creation
            print("\nGranting schema privileges...")
            grant_catalog_privileges(DATABASE_NAME, INTERNAL_DB_NAME)

            print(f"\n" + "=" * 50)
            print(f"Catalog '{CATALOG_NAME}' is ready for use!")
            print(f"=" * 50)
            print(f"Access via Unity Catalog: {CATALOG_NAME}.public.<table_name>")
    else:
        print("Instance not running/available, cannot create catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Check/Create Databricks App

# COMMAND ----------

def get_app(app_name):
    """Get app details using raw API."""
    response = api_request("GET", f"/api/2.0/apps/{app_name}")
    if response.status_code == 200:
        return response.json()
    elif response.status_code == 404:
        return None
    else:
        print(f"Error getting app: {response.text}")
        return None

def create_app(app_name, description, source_code_path):
    """Create a Databricks App using raw API."""
    response = api_request("POST", "/api/2.0/apps", {
        "name": app_name,
        "description": description,
        "default_source_code_path": source_code_path
    })
    if response.status_code == 200:
        print(f"Created app: {app_name}")
        return response.json()
    else:
        print(f"Failed to create app: {response.text}")
        return None

def update_app_resources(app_name, resources):
    """Update app resources (database, secrets) using raw API."""
    # Use raw API for app resources as SDK types may vary by version
    response = api_request("PATCH", f"/api/2.0/apps/{app_name}", {
        "resources": resources
    })
    if response.status_code == 200:
        print(f"Updated app resources for: {app_name}")
        return response.json()
    else:
        print(f"Failed to update app resources: {response.text}")
        return None

print("=" * 50)
print("Step 3: Setting up Databricks App")
print("=" * 50)

app_info = get_app(APP_NAME)
if app_info:
    print(f"App already exists: {APP_NAME}")
    print(f"App URL: {app_info.get('url', 'N/A')}")
    print(f"App Status: {app_info.get('app_status', {}).get('state', 'N/A')}")
    print(f"Compute Status: {app_info.get('compute_status', {}).get('state', 'N/A')}")
else:
    print(f"Creating app: {APP_NAME}")
    app_info = create_app(APP_NAME, APP_DESCRIPTION, SOURCE_CODE_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Configure App Resources

# COMMAND ----------

def wait_for_app_ready(app_name, timeout=600):
    """Wait for app compute to be in ACTIVE or STOPPED state."""
    print(f"Checking app compute state...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        app_info = get_app(app_name)
        if app_info:
            compute_state = app_info.get("compute_status", {}).get("state", "UNKNOWN")
            print(f"  Compute state: {compute_state}")
            if compute_state in ["ACTIVE", "STOPPED", "RUNNING"]:
                return True
            elif compute_state in ["STARTING", "STOPPING", "PENDING"]:
                print(f"  Waiting for compute to be ready...")
                time.sleep(20)
            else:
                # Unknown state, try anyway
                return True
        else:
            return False
    print("Timeout waiting for app compute to be ready")
    return False

print("=" * 50)
print("Step 4: Configuring App Resources")
print("=" * 50)

# Wait for app to be ready before updating resources
if not wait_for_app_ready(APP_NAME):
    print("Warning: Could not verify app state, attempting update anyway...")

# Build resources list
resources = []

# Add Lakebase OLTP database resource
if CREATE_DATABASE:
    resources.append({
        "name": "lakefusion-db",
        "database": {
            "instance_name": DATABASE_NAME,        # Lakebase instance name (e.g., "lakefusion-db")
            "database_name": INTERNAL_DB_NAME,     # PostgreSQL database name (e.g., "lakefusion_transactional_db")
            "permission": "CAN_CONNECT_AND_CREATE"
        }
    })

# Add secret resources
if CREATE_SECRETS and OIDC_CLIENT_ID:
    resources.append({
        "name": "DATABRICKS_OIDC_CLIENT_ID",
        "secret": {
            "scope": SECRETS_SCOPE,
            "key": "DATABRICKS_OIDC_CLIENT_ID",
            "permission": "READ"
        }
    })

if CREATE_SECRETS and OIDC_CLIENT_SECRET:
    resources.append({
        "name": "DATABRICKS_OIDC_CLIENT_SECRET",
        "secret": {
            "scope": SECRETS_SCOPE,
            "key": "DATABRICKS_OIDC_CLIENT_SECRET",
            "permission": "READ"
        }
    })

if CREATE_SECRETS and DATABRICKS_DAPI:
    resources.append({
        "name": "LAKEFUSION_DATABRICKS_DAPI",
        "secret": {
            "scope": SECRETS_SCOPE,
            "key": "LAKEFUSION_DATABRICKS_DAPI",
            "permission": "READ"
        }
    })

if resources:
    print(f"Configuring {len(resources)} resources:")
    for r in resources:
        print(f"  - {r['name']}")

    result = update_app_resources(APP_NAME, resources)
    if result:
        print("Resources configured successfully!")
else:
    print("No resources to configure")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Summary

# COMMAND ----------

print("=" * 50)
print("Setup Summary")
print("=" * 50)

# Get final app state
final_app_info = get_app(APP_NAME)
if final_app_info:
    print(f"\nApp Name: {final_app_info.get('name')}")
    print(f"App URL: {final_app_info.get('url', 'N/A')}")
    print(f"Source Code Path: {final_app_info.get('default_source_code_path', 'N/A')}")
    print(f"App Status: {final_app_info.get('app_status', {}).get('state', 'N/A')}")
    print(f"Compute Status: {final_app_info.get('compute_status', {}).get('state', 'N/A')}")

    print(f"\nConfigured Resources:")
    for resource in final_app_info.get('resources', []):
        print(f"  - {resource.get('name')}")

    print(f"\nService Principal: {final_app_info.get('service_principal_name', 'N/A')}")

    print("\n" + "=" * 50)
    print("Next Steps:")
    print("=" * 50)
    print("1. Sync your app code to the source code path")
    print("2. Start the app compute if stopped")
    print("3. Deploy the app")
    print(f"\nApp URL: {final_app_info.get('url', 'N/A')}")
else:
    print("Failed to retrieve app information")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Sync App Files to Workspace

# COMMAND ----------

# import base64
# import os

# def sync_file_to_workspace(local_content, workspace_path):
#     """Upload a file to the Databricks workspace."""
#     encoded_content = base64.b64encode(local_content.encode('utf-8')).decode('utf-8')
#     response = api_request("POST", "/api/2.0/workspace/import", {
#         "path": workspace_path,
#         "format": "AUTO",
#         "content": encoded_content,
#         "overwrite": True
#     })
#     if response.status_code == 200:
#         return True
#     else:
#         print(f"Failed to upload {workspace_path}: {response.text}")
#         return False

# def ensure_workspace_directory(path):
#     """Ensure a directory exists in the workspace."""
#     response = api_request("POST", "/api/2.0/workspace/mkdirs", {
#         "path": path
#     })
#     return response.status_code == 200

# def list_workspace_files(path):
#     """List files in a workspace directory."""
#     response = api_request("GET", f"/api/2.0/workspace/list?path={path}")
#     if response.status_code == 200:
#         return response.json().get("objects", [])
#     return []

# # Widget for sync control
# dbutils.widgets.dropdown("sync_files", "false", ["true", "false"], "Sync App Files")
# dbutils.widgets.text("local_app_path", "/Workspace/Repos/lakefusion-app", "Local App Source Path")

# SYNC_FILES = dbutils.widgets.get("sync_files") == "true"
# LOCAL_APP_PATH = dbutils.widgets.get("local_app_path")

# print("=" * 50)
# print("Step 6: Sync App Files")
# print("=" * 50)

# if SYNC_FILES:
#     print(f"Syncing files from: {LOCAL_APP_PATH}")
#     print(f"Syncing files to: {SOURCE_CODE_PATH}")

#     # Ensure target directory exists
#     ensure_workspace_directory(SOURCE_CODE_PATH)

#     # List source files and copy them
#     source_files = list_workspace_files(LOCAL_APP_PATH)
#     if source_files:
#         for obj in source_files:
#             src_path = obj.get("path")
#             file_name = src_path.split("/")[-1]
#             dest_path = f"{SOURCE_CODE_PATH}/{file_name}"

#             if obj.get("object_type") == "FILE":
#                 # Read and copy file
#                 read_response = api_request("GET", f"/api/2.0/workspace/export?path={src_path}&format=AUTO")
#                 if read_response.status_code == 200:
#                     content = read_response.json().get("content", "")
#                     # Re-upload to destination
#                     upload_response = api_request("POST", "/api/2.0/workspace/import", {
#                         "path": dest_path,
#                         "format": "AUTO",
#                         "content": content,
#                         "overwrite": True
#                     })
#                     if upload_response.status_code == 200:
#                         print(f"  Synced: {file_name}")
#                     else:
#                         print(f"  Failed to sync: {file_name}")
#             elif obj.get("object_type") == "DIRECTORY":
#                 print(f"  Skipping directory: {file_name} (recursive sync not implemented)")

#         print("File sync completed!")
#     else:
#         print(f"No files found in {LOCAL_APP_PATH}")
#         print("Make sure the source path exists and contains your app files.")
# else:
#     print("File sync skipped (SYNC_FILES=false)")
#     print(f"To sync files, set 'Sync App Files' widget to 'true'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Deploy App

# COMMAND ----------

dbutils.widgets.dropdown("deploy_app", "true", ["true", "false"], "Deploy App")
DEPLOY_APP = dbutils.widgets.get("deploy_app") == "true"

def start_app_compute(app_name):
    """Start app compute using raw API."""
    response = api_request("POST", f"/api/2.0/apps/{app_name}/start")
    if response.status_code == 200:
        print(f"Started app compute: {app_name}")
        return True
    else:
        print(f"Failed to start app compute: {response.text}")
        return False

def deploy_app(app_name, source_code_path):
    """Deploy app from source code path using raw API."""
    response = api_request("POST", f"/api/2.0/apps/{app_name}/deployments", {
        "source_code_path": source_code_path
    })
    if response.status_code == 200:
        print(f"Deployment started for: {app_name}")
        return response.json()
    else:
        print(f"Failed to deploy app: {response.text}")
        return None

def get_deployment_status(app_name, deployment_id):
    """Get deployment status."""
    response = api_request("GET", f"/api/2.0/apps/{app_name}/deployments/{deployment_id}")
    if response.status_code == 200:
        return response.json()
    return None

def wait_for_deployment(app_name, deployment_id, timeout=600):
    """Wait for deployment to complete."""
    print(f"Waiting for deployment {deployment_id} to complete...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        status = get_deployment_status(app_name, deployment_id)
        if status:
            state = status.get("status", {}).get("state", "UNKNOWN")
            print(f"  Deployment state: {state}")
            if state == "SUCCEEDED":
                return True
            elif state in ["FAILED", "CANCELLED"]:
                print(f"Deployment failed: {status.get('status', {}).get('message', 'Unknown error')}")
                return False
        time.sleep(15)
    print("Deployment timed out")
    return False

print("=" * 50)
print("Step 7: Deploy App")
print("=" * 50)

if DEPLOY_APP:
    # Check current app status
    app_info = get_app(APP_NAME)
    if app_info:
        compute_state = app_info.get("compute_status", {}).get("state", "UNKNOWN")
        print(f"Current compute state: {compute_state}")

        # Start compute if stopped
        if compute_state == "STOPPED":
            print("Starting app compute...")
            start_app_compute(APP_NAME)
            # Wait for compute to start
            print("Waiting for compute to start...")
            time.sleep(30)

        # Deploy the app
        print(f"Deploying app from: {SOURCE_CODE_PATH}")
        deployment = deploy_app(APP_NAME, SOURCE_CODE_PATH)

        if deployment:
            deployment_id = deployment.get("deployment_id")
            print(f"Deployment ID: {deployment_id}")

            # Wait for deployment to complete
            if wait_for_deployment(APP_NAME, deployment_id):
                print("\n" + "=" * 50)
                print("DEPLOYMENT SUCCESSFUL!")
                print("=" * 50)
                print(f"App URL: {app_info.get('url', 'N/A')}")
            else:
                print("\nDeployment did not complete successfully. Check the app logs for details.")
    else:
        print(f"App {APP_NAME} not found. Please create the app first.")
else:
    print("Deployment skipped (DEPLOY_APP=false)")
    print("To deploy the app, set 'Deploy App' widget to 'true'")
