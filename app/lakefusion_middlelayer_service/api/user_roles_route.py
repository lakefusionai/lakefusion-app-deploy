import json
import re

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Optional
from app.lakefusion_middlelayer_service.utils.app_db import get_db, token_required_wrapper
from lakefusion_utility.services.rbac_service import RBACService
from lakefusion_utility.models.rbac import RoleResponse
from lakefusion_utility.utils.logging_utils import get_logger

logger = get_logger(__name__)

user_roles_router = APIRouter(tags=["User Roles API"], prefix="/user-roles")


@user_roles_router.get("/roles", response_model=List[RoleResponse])
def list_roles(
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    """List all available roles."""
    try:
        service = RBACService(db)
        roles = service.list_roles()
        return [RoleResponse.model_validate(r) for r in roles]
    except Exception as e:
        logger.exception("Failed to list roles")
        raise HTTPException(status_code=500, detail=str(e))


@user_roles_router.get("/roles/{role_id}", response_model=RoleResponse)
def get_role(
    role_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    """Get a single role by ID."""
    service = RBACService(db)
    role = service.get_role_by_id(role_id)
    if not role:
        raise HTTPException(status_code=404, detail="Role not found")
    return RoleResponse.model_validate(role)


# ── RBAC Audit Log helpers ──────────────────────────────────────────────

# API paths that represent RBAC-related actions
_RBAC_PATH_PATTERNS = [
    r"/rbac-groups",
    r"/user-roles",
    r"/users.*groups",
]
_RBAC_REGEX = re.compile("|".join(_RBAC_PATH_PATTERNS), re.IGNORECASE)

# Map HTTP method + path fragment → human-readable action
_ACTION_MAP = [
    ("POST",   "/rbac-groups/assign",           "assign_role"),
    ("POST",   "/rbac-groups/unassign",          "unassign_role"),
    ("POST",   "/rbac-groups",                   "create_group"),
    ("DELETE",  "/rbac-groups",                  "delete_group"),
    ("PUT",    "/rbac-groups",                   "update_group"),
    ("PATCH",  "/rbac-groups",                   "update_group"),
    ("POST",   "/members",                       "add_member"),
    ("DELETE",  "/members",                      "remove_member"),
    ("POST",   "/permissions",                   "add_permission"),
    ("DELETE",  "/permissions",                  "remove_permission"),
]


def _derive_action(method: str, path: str) -> str:
    """Derive a human-readable action from HTTP method + path."""
    method_upper = (method or "").upper()
    path_lower = (path or "").lower()
    for m, fragment, action in _ACTION_MAP:
        if method_upper == m and fragment in path_lower:
            return action
    # Fallback
    method_action = {
        "POST": "create", "PUT": "update", "PATCH": "update",
        "DELETE": "delete", "GET": "view",
    }
    return method_action.get(method_upper, method_upper.lower())


def _derive_target(path: str, payload_str: str) -> dict:
    """Extract target_type, target_id, target_name from path and payload."""
    target = {"target_type": "rbac", "target_id": "", "target_name": ""}

    # Try to extract group name or ID from payload
    if payload_str:
        try:
            payload = json.loads(payload_str) if isinstance(payload_str, str) else {}
        except (json.JSONDecodeError, TypeError):
            payload = {}
    else:
        payload = {}

    if "group" in (path or "").lower():
        target["target_type"] = "group"
        target["target_name"] = payload.get("group_name") or payload.get("name", "")
        target["target_id"] = str(payload.get("group_id", ""))
    elif "assign" in (path or "").lower():
        target["target_type"] = "role_assignment"
        target["target_name"] = payload.get("user_email") or payload.get("user_id", "")
        target["target_id"] = str(payload.get("entity_id", ""))
    elif "member" in (path or "").lower():
        target["target_type"] = "group_member"
        target["target_name"] = payload.get("user_id") or payload.get("email", "")
    elif "permission" in (path or "").lower():
        target["target_type"] = "permission"
        target["target_name"] = payload.get("role_name", "")
        target["target_id"] = str(payload.get("entity_id", ""))

    return target


PAGE_SIZE = 50


@user_roles_router.get("/audit")
def get_audit_logs(
    cursor: Optional[str] = Query(None, description="Pagination cursor (last seen ID)"),
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    """Return RBAC-related audit logs from the general audit_logs table.

    Filters for API paths containing rbac-groups, user-roles, etc.
    Maps general audit fields to the RBAC-specific AuditLog shape expected
    by the frontend.
    """
    try:
        # Build query filtering for RBAC-related paths
        where_clause = (
            "(api_path LIKE '%rbac-groups%' "
            "OR api_path LIKE '%user-roles%' "
            "OR api_path LIKE '%/users%groups%')"
        )
        # Exclude GET requests (only show mutations)
        where_clause += " AND request_method != 'GET'"
        # Exclude OPTIONS preflight
        where_clause += " AND request_method != 'OPTIONS'"

        params = {"limit": PAGE_SIZE + 1}

        if cursor:
            where_clause += " AND audit_logs_id < :cursor"
            params["cursor"] = int(cursor)

        query = text(
            f"SELECT audit_logs_id, username, created_date, api_path, "
            f"request_method, request_payload, response_status "
            f"FROM audit_logs "
            f"WHERE {where_clause} "
            f"ORDER BY audit_logs_id DESC "
            f"LIMIT :limit"
        )

        rows = db.execute(query, params).fetchall()

        has_more = len(rows) > PAGE_SIZE
        if has_more:
            rows = rows[:PAGE_SIZE]

        logs = []
        for row in rows:
            row_id, username, created_date, api_path, method, payload, status = row
            action = _derive_action(method, api_path)
            target = _derive_target(api_path, payload)

            details = {}
            if status:
                details["status_code"] = status
            if method:
                details["method"] = method
            if api_path:
                details["path"] = api_path

            logs.append({
                "id": str(row_id),
                "action": action,
                "actor": username or "unknown",
                "target_type": target["target_type"],
                "target_id": target["target_id"],
                "target_name": target["target_name"],
                "details": details,
                "created_at": str(created_date) if created_date else "",
            })

        next_cursor = str(rows[-1][0]) if rows and has_more else None

        return {
            "data": logs,
            "has_more": has_more,
            "next_cursor": next_cursor,
        }
    except Exception as e:
        logger.exception("Failed to fetch audit logs")
        raise HTTPException(status_code=500, detail=str(e))
