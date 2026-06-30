# LakeFusion - Setup Guide

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

> The redirect URL follows the Databricks Apps URL pattern. Replace `<your-app-name>`, `<workspace-id>`, and `<region>` with your actual values. You can find these after creating the app in Step 3.

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

## Step 3: Run Full App Setup

Run `setup_lakefusion_app.py` to create and configure the Databricks App:

**What it does:**
1. Creates the Databricks App (if not exists)
2. Runs prerequisites (if not already done)
3. Configures app resources (database, secrets)
4. Grants READ permission on secrets scope to the App SP
5. Deploys the app

**Additional Widgets (beyond prereqs):**

| Widget | Default | Description |
|--------|---------|-------------|
| `app_name` | `lakefusionai` | Databricks App name |
| `app_description` | `LakeFusion AI` | Description |
| `source_code_path` | `/Workspace/Users/{user}/lakefusion-app-deploy` | Source code path |
| `lakegraph_url` | _(empty)_ | LakeGraph URL (optional) |
| `deploy_app` | `true` | Whether to deploy after setup |

---

## Step 3a: Post-Installation (Standalone)

If you ran `setup_lakefusion_prereqs.py` and created the app separately (e.g., via Marketplace), run `setup_lakefusion_post_install.py` to grant the App SP READ access to the secrets scope.

**Widgets:**

| Widget | Description |
|--------|-------------|
| `secrets_scope` | The secrets scope name from Step 2 |
| `app_service_principal_id` | The SP application ID from **Apps** > your app > **Overview** |

> **Note:** This step is not needed if you used `setup_lakefusion_app.py` — it handles ACL grants automatically.

---

## Step 4: Set Up Data Catalog

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

Replace `<catalog_name>` with the catalog name you configured in Step 2 (e.g., `lakefusion_ai`).

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

**Grant workspace permissions:**

The App SP also needs **CAN MANAGE** permission on the notebooks root folder (`/Workspace/Lakefusion_Notebooks/`). Set this via the Databricks workspace UI under **Workspace** > right-click the folder > **Permissions**.

---

## Step 5: Configure User Authorization Scopes

After the app is created, configure user authorization scopes in the Databricks UI:

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

## Step 6: Grant App Permissions

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

## Re-provisioning

Both notebooks are idempotent — existing resources are detected and skipped. Set `create_database` / `create_secrets` to `false` to skip specific steps.

---

## Support

For support, contact: support@lakefusion.ai
