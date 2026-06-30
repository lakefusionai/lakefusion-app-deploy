# LakeFusion - Marketplace Installation Guide

## Step 1: Register OIDC Client (Required for SSO)

Before installing LakeFusion, you must register an OAuth application in your Databricks Account Console:

1. Go to **Account Console** > **Settings** > **App connections**
2. Click **Add connection**
3. Configure:
   - **Name**: `lakefusion` (or your preferred name)
   - **Redirect URLs**:
     - `https://<your-app-name>-<workspace-id>.<region>.databricksapps.com/auth/callback`
     - `https://<your-app-name>-<workspace-id>.<region>.databricksapps.com/auth/code`
   - **Scopes**: `all-apis`, `offline_access`, `openid`, `profile`
4. Save and note the **Client ID** and **Client Secret** — you'll need these during setup

> The redirect URL follows the Databricks Apps URL pattern. Replace `<your-app-name>`, `<workspace-id>`, and `<region>` with your actual values. You can find these after installing the app in Step 3.

---

## Step 2: Run Prerequisites Setup

Run `setup_lakefusion_prereqs.py` in a Databricks notebook to provision infrastructure:

**What it does:**
1. Creates Secrets Scope and sets OIDC secrets
2. Creates Lakebase Database Instance
3. Creates PostgreSQL Database in the Instance
4. Registers Database as Unity Catalog

**Widgets:**

| Widget | Default | Description |
|--------|---------|-------------|
| `database_name` | `lakefusion-db` | Lakebase instance name |
| `internal_db_name` | `lakefusion_transactional_db` | PostgreSQL database name |
| `secrets_scope` | `lakefusion` | Secrets scope name |
| `oidc_client_id` | _(empty)_ | OIDC Client ID from Step 1 |
| `oidc_client_secret` | _(empty)_ | OIDC Client Secret from Step 1 |
| `catalog_name` | `lakefusion_ai` | Database Unity Catalog name (where processed data is stored) |
| `create_database` | `true` | Whether to create the Lakebase database |
| `create_secrets` | `true` | Whether to create secrets scope |

---

## Step 3: Install from Databricks Marketplace

1. Go to **Marketplace** in your Databricks workspace
2. Search for **LakeFusion**
3. Click **Install** (or **Request Access** if access-gated)
4. Configure the app resources when prompted:
   - **Database**: Select the Lakebase instance created in Step 2
   - **Secrets**: Select the secrets scope created in Step 2
5. Wait for the app to deploy and start

---

## Step 4: Grant App SP Access to Secrets Scope

After the app is created, run `setup_lakefusion_post_install.py` to grant the App Service Principal READ access to the secrets scope.

**Widgets:**

| Widget | Description |
|--------|-------------|
| `secrets_scope` | The secrets scope name used in Step 2 (e.g., `lakefusion`) |
| `app_service_principal_id` | The SP application ID from **Apps** > your app > **Overview** |

> **Why this step?** The secrets scope is created with admin-only access by default. The App SP needs READ access to retrieve OIDC credentials and other secrets at runtime. No other workspace users are granted access.

---

## Step 5: Post-Installation Configuration

After the app is running, configure LakeFusion settings:

1. Open the LakeFusion app URL
2. Navigate to **Settings** > **General Settings** (`/settings/general-settings`)
3. Update the following parameters:

| Setting | Description | Example |
|---------|-------------|---------|
| **Catalog Name** | The Unity Catalog name where processed master data is stored. This catalog serves as the central repository for organizing and managing all entity datasets. | `lakefusion_ai` |
| **Cron Warehouse ID** | The SQL warehouse ID for scheduled jobs and synchronization tasks. Find this in your workspace under **SQL Warehouses**. | `5433afee246b68c2` |
| **DBX Cloud** | The Databricks cloud environment where workloads are deployed. | `azure`, `aws`, or `gcp` |
| **Notebook Path** | The workspace path where LakeFusion pipeline notebooks are stored. | `/Workspace/Lakefusion_Notebooks/match-maven-notebooks-universe` |

---

## Step 6: Set Up Data Catalog

The App Service Principal needs write access to the data catalog. Run the following SQL to create the required schemas and volumes:

```sql
-- Create schemas
CREATE SCHEMA IF NOT EXISTS <catalog_name>.default;
CREATE SCHEMA IF NOT EXISTS <catalog_name>.silver;
CREATE SCHEMA IF NOT EXISTS <catalog_name>.gold;
CREATE SCHEMA IF NOT EXISTS <catalog_name>.embedding;
CREATE SCHEMA IF NOT EXISTS <catalog_name>.llm;
CREATE SCHEMA IF NOT EXISTS <catalog_name>.metadata;

-- Create required volumes
CREATE VOLUME IF NOT EXISTS <catalog_name>.metadata.metadata_files;
CREATE VOLUME IF NOT EXISTS <catalog_name>.default.pipeline_logs;
```

Replace `<catalog_name>` with the catalog name you configured in Step 5 (e.g., `lakefusion_ai`).

**Grant permissions to the App Service Principal:**

```sql
-- Grant the App SP full access to the data catalog
GRANT USE CATALOG ON CATALOG <catalog_name> TO `<app-sp-client-id>`;
GRANT USE SCHEMA ON CATALOG <catalog_name> TO `<app-sp-client-id>`;
GRANT CREATE TABLE ON CATALOG <catalog_name> TO `<app-sp-client-id>`;
GRANT CREATE VOLUME ON CATALOG <catalog_name> TO `<app-sp-client-id>`;
GRANT SELECT, MODIFY ON CATALOG <catalog_name> TO `<app-sp-client-id>`;
GRANT READ VOLUME, WRITE VOLUME ON CATALOG <catalog_name> TO `<app-sp-client-id>`;
```

> The `<app-sp-client-id>` is the service principal automatically created for your app. Find it in **Apps** > your app > **Overview**.

**Grant workspace permissions:**

The App SP also needs **CAN MANAGE** permission on the notebooks root folder (`/Workspace/Lakefusion_Notebooks/`). Set this via the Databricks workspace UI:
1. Go to **Workspace** in the left nav
2. Navigate to or create `/Workspace/Lakefusion_Notebooks/`
3. Right-click the folder > **Permissions**
4. Add the App Service Principal with **CAN MANAGE**

---

## Step 7: Configure User Authorization Scopes

Configure user authorization scopes in the Databricks UI:

1. Go to **Apps** > your app > **Authorization**
2. Add the following scopes:

| Scope | Purpose |
|-------|---------|
| `sql` | SQL warehouse queries |
| `catalog.catalogs:read` | Unity Catalog access |
| `catalog.schemas:read` | Schema browsing |
| `catalog.tables:read` | Table access |
| `model-serving` | Model serving endpoints |
| `vector-search` | Vector search |
| `files` | Files and volumes |
| `workspace.workspace` | Workspace operations |
| `ai-gateway` | AI Gateway access |

---

## Step 8: Grant App Permissions

Grant **CAN USE** permission to users/groups who need access:

1. Go to **Apps** > your app > **Permissions**
2. Add users or groups with **CAN USE** permission

For M2M API access, the calling service principal also needs **CAN USE**.

---

## M2M API Access

To call LakeFusion APIs programmatically (without a browser):

1. Generate a token from Databricks OIDC:
   ```bash
   curl -X POST "https://<workspace-host>/oidc/v1/token" \
     -d "client_id=<sp-client-id>" \
     -d "client_secret=<sp-client-secret>" \
     -d "grant_type=client_credentials" \
     -d "scope=all-apis"
   ```

2. Call LakeFusion APIs:
   ```bash
   curl "https://<app-url>/api/middle-layer/dataset/" \
     -H "Authorization: Bearer <token>"
   ```

> The calling service principal must have **CAN USE** permission on the app.

---

## Support

For support, contact: support@lakefusion.ai
