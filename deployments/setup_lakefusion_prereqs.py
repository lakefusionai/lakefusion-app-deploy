# Databricks notebook source
# MAGIC %md
# MAGIC # LakeFusion Prerequisites Setup
# MAGIC
# MAGIC This notebook sets up the infrastructure prerequisites for LakeFusion.
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. Create Secrets Scope and store OIDC credentials
# MAGIC 2. Create Lakebase Database Instance
# MAGIC 3. Create PostgreSQL Database in the Instance
# MAGIC 4. Register Database as Unity Catalog
# MAGIC
# MAGIC **Note:** After app creation, run `setup_lakefusion_post_install` to grant the App SP READ access to the secrets scope.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Dependencies

# COMMAND ----------

# MAGIC %pip install databricks-sdk>=0.77.0 --upgrade

# COMMAND ----------

# MAGIC %pip install PyYAML

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Widgets

# COMMAND ----------

# Widget definitions — prerequisites only (no app-specific widgets)
dbutils.widgets.text("database_name", "", "App DB — Lakebase Instance Name")
dbutils.widgets.text("internal_db_name", "", "App DB — Lakebase Database Name")
dbutils.widgets.text("secrets_scope", "", "Secrets Scope Name")
dbutils.widgets.text("oidc_client_id", "", "OIDC Client ID (for SSO)")
dbutils.widgets.text("oidc_client_secret", "", "OIDC Client Secret (for SSO)")
dbutils.widgets.text("catalog_name", "", "Database Unity Catalog Name")
dbutils.widgets.dropdown("create_database", "true", ["true", "false"], "Create Lakebase Database")
dbutils.widgets.dropdown("create_secrets", "true", ["true", "false"], "Create Secrets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Widget Values

# COMMAND ----------

# Get configuration values
DATABASE_NAME = dbutils.widgets.get("database_name")
INTERNAL_DB_NAME = dbutils.widgets.get("internal_db_name")
SECRETS_SCOPE = dbutils.widgets.get("secrets_scope")
OIDC_CLIENT_ID = dbutils.widgets.get("oidc_client_id")
OIDC_CLIENT_SECRET = dbutils.widgets.get("oidc_client_secret")
CATALOG_NAME = dbutils.widgets.get("catalog_name")
CREATE_DATABASE = dbutils.widgets.get("create_database") == "true"
CREATE_SECRETS = dbutils.widgets.get("create_secrets") == "true"

# --- Validate required parameters ---
_errors = []

# OIDC credentials are critical — check first
if not OIDC_CLIENT_ID:
    _errors.append("OIDC Client ID is required. Register an OAuth app in Account Console > Settings > App connections.")
if not OIDC_CLIENT_SECRET:
    _errors.append("OIDC Client Secret is required. Register an OAuth app in Account Console > Settings > App connections.")

if _errors:
    for e in _errors:
        print(f"❌ {e}")
    raise SystemExit("Setup aborted: OIDC credentials are required. Please configure them in the widgets above and re-run.")

# Other required parameters
if not DATABASE_NAME:
    _errors.append("Lakebase Instance Name is required.")
if not INTERNAL_DB_NAME:
    _errors.append("PostgreSQL Database Name is required.")
if not SECRETS_SCOPE:
    _errors.append("Secrets Scope Name is required.")
if not CATALOG_NAME:
    _errors.append("Database Unity Catalog Name is required.")

if _errors:
    for e in _errors:
        print(f"❌ {e}")
    raise SystemExit("Setup aborted: Please fill in all required parameters in the widgets above and re-run.")

print(f"Configuration:")
print(f"  App DB — Lakebase Instance: {DATABASE_NAME}")
print(f"  App DB — Database Name: {INTERNAL_DB_NAME}")
print(f"  Secrets Scope: {SECRETS_SCOPE}")
print(f"  Database Unity Catalog Name: {CATALOG_NAME}")
print(f"  Create Database: {CREATE_DATABASE}")
print(f"  Create Secrets: {CREATE_SECRETS}")
print(f"  OIDC Client ID: [SET]")
print(f"  OIDC Client Secret: [SET]")

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
        "scope": scope_name
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
    """Get credentials for PostgreSQL connection using Databricks SDK."""
    context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    username = context.userName().get()

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

# COMMAND ----------

from databricks.sdk.service.database import DatabaseCatalog

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
        print(f"\nCreating database '{INTERNAL_DB_NAME}' and catalog '{CATALOG_NAME}'...")
        catalog = create_database_and_catalog(DATABASE_NAME, INTERNAL_DB_NAME, CATALOG_NAME)

        if catalog:
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
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("  PREREQUISITES SETUP COMPLETE")
print("=" * 60)

if CREATE_SECRETS:
    print(f"  ✅ Secrets Scope: {SECRETS_SCOPE}")
    if OIDC_CLIENT_ID:
        print(f"     - DATABRICKS_OIDC_CLIENT_ID: [SET]")
    if OIDC_CLIENT_SECRET:
        print(f"     - DATABRICKS_OIDC_CLIENT_SECRET: [SET]")

if CREATE_DATABASE:
    db_info = get_lakebase_database(DATABASE_NAME)
    if db_info:
        print(f"  ✅ Lakebase Instance: {DATABASE_NAME}")
        print(f"     Endpoint: {db_info.read_write_dns or 'N/A'}")
        print(f"     State: {db_info.state}")
    print(f"  ✅ PostgreSQL Database: {INTERNAL_DB_NAME}")
    print(f"  ✅ Unity Catalog: {CATALOG_NAME}")

print("=" * 60)
