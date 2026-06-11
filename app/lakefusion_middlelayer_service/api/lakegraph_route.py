"""
LakeGraph proxy + Relationship Graph routes.

A graph is a named bundle of relationships (nodes auto-derived from endpoints).
CRUD lives here; deploy/redeploy/status proxy to LakeGraph's external API, scoped
per graph. The user's JWT is forwarded verbatim; `X-Lakefusion-User-Id` carries
the human email for LakeGraph's audit trail.
"""

from typing import List, Optional
import os

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.lakefusion_middlelayer_service.utils.app_db import get_db, token_required_wrapper
from lakefusion_utility.models.relationship import (
    LakeGraphDeployRequest,
    LakeGraphDeploymentResponse,
    RelationshipGraphCreate,
    RelationshipGraphUpdate,
    RelationshipGraphResponse,
)
from lakefusion_utility.services.lakegraph_service import LakeGraphService
from lakefusion_utility.services.relationship_graph_service import (
    RelationshipGraphService,
)


lakegraph_router = APIRouter(tags=["LakeGraph API"], prefix="/lakegraph")


def _user_email(check: dict) -> str:
    decoded = check.get("decoded", {}) or {}
    return decoded.get("email") or decoded.get("sub") or ""


@lakegraph_router.get("/config")
def get_config(
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    """Tells the UI whether the LakeGraph integration is configured + the base URL
    (for the in-new-tab redirect after deploy)."""
    url = os.environ.get("LAKEGRAPH_URL", "")
    if url and not url.startswith(("http://", "https://")):
        url = f"https://{url}"
    return {"enabled": bool(url), "lakegraph_url": url}


# ──────────────────────────────────────────────────────────────────────────────
# Relationship Graph CRUD
# ──────────────────────────────────────────────────────────────────────────────

@lakegraph_router.get("/graphs", response_model=List[RelationshipGraphResponse])
def list_graphs(
    is_active: bool = True,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    return RelationshipGraphService(db).read_all(is_active)


@lakegraph_router.post("/graphs", response_model=RelationshipGraphResponse)
def create_graph(
    payload: RelationshipGraphCreate,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    payload.created_by = check.get("decoded", {}).get("sub", "")
    return RelationshipGraphService(db).create(payload)


@lakegraph_router.get("/graphs/{graph_id}", response_model=RelationshipGraphResponse)
def get_graph(
    graph_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    return RelationshipGraphService(db).get(graph_id)


@lakegraph_router.patch("/graphs/{graph_id}", response_model=RelationshipGraphResponse)
def update_graph(
    graph_id: int,
    payload: RelationshipGraphUpdate,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    return RelationshipGraphService(db).update(graph_id, payload)


@lakegraph_router.delete("/graphs/{graph_id}")
def delete_graph(
    graph_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    return RelationshipGraphService(db).delete(graph_id)


# ──────────────────────────────────────────────────────────────────────────────
# Per-graph deploy / state / status (LakeGraph proxy)
# ──────────────────────────────────────────────────────────────────────────────

@lakegraph_router.get(
    "/graphs/{graph_id}/state", response_model=Optional[LakeGraphDeploymentResponse]
)
def get_state(
    graph_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    return LakeGraphService(db).get_state(graph_id)


@lakegraph_router.get("/graphs/{graph_id}/live-hash")
def get_live_hash(
    graph_id: int,
    warehouse_id: str = Query(...),
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    # Soft-fail: feature off / no config / not-ready yields an empty hash instead
    # of a 500, so the UI just stays on Create rather than blocking the page.
    try:
        return {"hash": LakeGraphService(db).get_live_hash(graph_id, warehouse_id)}
    except Exception:  # noqa: BLE001
        return {"hash": ""}


@lakegraph_router.post("/graphs/{graph_id}/deploy")
def deploy(
    graph_id: int,
    payload: LakeGraphDeployRequest,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    token = check.get("token", "")
    steward_id = check.get("decoded", {}).get("sub", "")
    return LakeGraphService(db).deploy(
        graph_id, payload, token, steward_id, _user_email(check)
    )


@lakegraph_router.get("/graphs/{graph_id}/status")
def status_proxy(
    graph_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    token = check.get("token", "")
    return LakeGraphService(db).status(graph_id, token, _user_email(check))
