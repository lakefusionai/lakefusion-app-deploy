# LakeFusion App Deploy

Auto-generated deploy artifacts for running LakeFusion as a single Databricks App.

> **Do not edit this repo directly.** All changes flow from [lakefusion-universe](https://github.com/lakefusionai/lakefusion-universe) via GitHub Actions.

## What is this?

LakeFusion is a Databricks-native AI-powered MDM (Master Data Management) platform. In production, it runs as 6 independent FastAPI microservices + 4 single-spa UI portals. This repo contains the **unified build** — all services and UI assembled into a single deployable Databricks App.

## How it works

1. A push to `lakefusion-universe` triggers the [build-and-deploy]( https://github.com/lakefusionai/lakefusion-universe/actions) GitHub Actions workflow
2. The workflow runs `scripts/build_lakefusion_app.sh` which:
   - Builds `lakefusion-utility` as a Python wheel
   - Copies each service's code as a subpackage with rewritten imports
   - Builds all 4 UI portals and assembles static files
   - Generates `app.yaml`, `config.js`, `importmap.json`, and merged `requirements.txt`
   - Copies alembic migrations and Databricks pipeline artifacts
3. The generated output is pushed to **this repo** on the matching branch

## Directory Structure

```
├── app/                          # Unified FastAPI application
│   ├── main.py                   # Single entry point (all 40+ routers)
│   ├── config.py                 # Database config (PG*/Lakebase/MySQL)
│   ├── utils/app_db.py           # Shim for utility wheel imports
│   ├── lakefusion_auth_service/
│   ├── lakefusion_middlelayer_service/
│   ├── lakefusion_databricks_service/
│   ├── lakefusion_cron_service/
│   ├── lakefusion_matchmaven_service/
│   └── lakefusion_mcp_service/
├── static/                       # UI (single-spa microfrontends)
│   ├── index.html                # Root portal entry
│   ├── config.js                 # Runtime API config (relative URLs)
│   ├── importmap.json            # SystemJS module map
│   ├── lakefusion-root-config.js
│   ├── lakefusion-main.js
│   ├── lakefusion-utility.js
│   └── lakefusion-selfservice.js
├── wheels/                       # lakefusion-utility wheel
├── alembic/                      # Database migrations
├── dbx_pipeline_artifacts/       # Databricks notebooks for workspace sync
├── app.yaml                      # Databricks App deployment config
├── alembic.ini
├── requirements.txt
└── version.json
```

## Deploying to Databricks

```bash
# Clone this repo
git clone https://github.com/lakefusionai/lakefusion-app-deploy.git
cd lakefusion-app-deploy

# Deploy to Databricks Apps
databricks apps deploy lakefusion-app --source-code-path /Workspace/Users/<your-email>/lakefusion-app-deploy
```

Or sync via the Databricks workspace repos UI.

## Routing

| Path | Destination |
|------|-------------|
| `/api/auth/*` | Auth Service |
| `/api/middle-layer/*` | Middlelayer Service |
| `/api/databricks/*` | Databricks Service |
| `/api/cron/*` | Cron Service |
| `/api/match-maven/*` | MatchMaven Service |
| `/api/mcp/*` | MCP Service |
| `/docs` | Swagger API Docs |
| `/*` (everything else) | UI (SPA catch-all → index.html) |

## Database Support

- **Deployed (Databricks App):** Lakebase (PostgreSQL-compatible, OAuth token refresh)
- **Local dev:** PostgreSQL via `PG*` environment variables
- **Legacy:** MySQL via `SQL_*` environment variables

## Branch Mapping

| Source Branch (lakefusion-universe) | Deploy Branch (this repo) |
|-------------------------------------|---------------------------|
| `develop/5.0.0` | `develop/5.0.0` |
| `staging/4.2.0` | `staging/4.2.0` |
| `main` | `main` |
| `release/*` | `release/*` |

## Source Repository

All source code lives in [lakefusionai/lakefusion-universe](https://github.com/lakefusionai/lakefusion-universe). To make changes, submit PRs there — this repo is auto-generated.
