# LakeFusion Utilities

Setup notebooks for provisioning LakeFusion infrastructure on Databricks.

## Notebooks

### `setup_lakefusion_prereqs.py` — Prerequisites Setup

Sets up the infrastructure for LakeFusion: secrets, databases, and Unity Catalog.

**Steps:**
1. Creates Secrets Scope + sets OIDC and DAPI secrets
2. Creates Lakebase Database Instance
3. Creates PostgreSQL Database in the Instance
4. Registers Database as Unity Catalog

**Widgets:**

| Widget | Default | Description |
|--------|---------|-------------|
| `database_name` | `lakefusion-db` | Lakebase instance name |
| `internal_db_name` | `lakefusion_transactional_db` | PostgreSQL database name |
| `secrets_scope` | `lakefusion` | Secrets scope name |
| `oidc_client_id` | _(empty)_ | OIDC Client ID for SSO |
| `oidc_client_secret` | _(empty)_ | OIDC Client Secret for SSO |
| `databricks_dapi` | _(empty)_ | Databricks DAPI token |
| `catalog_name` | `lakefusion_ai` | Unity Catalog name |
| `create_database` | `true` | Whether to create the Lakebase database |
| `create_secrets` | `true` | Whether to create secrets scope |

---

### `setup_lakefusion_app.py` — Full Setup

End-to-end setup: prerequisites + Databricks App creation + configuration + deployment.

**Additional Widgets (beyond prereqs):**

| Widget | Default | Description |
|--------|---------|-------------|
| `app_name` | `lakefusionai` | Databricks App name |
| `app_description` | `LakeFusion AI` | Description |
| `source_code_path` | `/Workspace/Users/{user}/lakefusion-app-deploy` | Source code path |
| `lakegraph_url` | _(empty)_ | LakeGraph URL |
| `data_db_type` | `lakebase` | Data DB / PIM DB type |
| `data_db_instance` | `lakefusion-db` | Data DB / PIM DB Lakebase instance |
| `data_db_search_path` | `public` | Data DB / PIM DB search path |
| `deploy_app` | `true` | Whether to deploy after setup |

---

## Usage

### First-time setup
1. Upload notebooks to your Databricks workspace
2. Run `setup_lakefusion_prereqs.py` to provision secrets + database
3. Run `setup_lakefusion_app.py` for full deployment

### Re-provisioning
- Both notebooks are idempotent — existing resources are detected and skipped
- Set `create_database` / `create_secrets` to `false` to skip specific steps
