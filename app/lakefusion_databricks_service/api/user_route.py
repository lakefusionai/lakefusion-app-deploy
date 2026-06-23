"""
User management routes for the Databricks service.

Provides endpoints for listing workspace users and groups via the
Databricks SCIM API.
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.lakefusion_databricks_service.utils.app_db import get_db
from app.lakefusion_databricks_service.utils.rbac_auth import require_user_management
from lakefusion_utility.utils.databricks_util import User_Management
from lakefusion_utility.utils.logging_utils import get_logger

logger = get_logger(__name__)

user_router = APIRouter(tags=["User Management API"], prefix="/users")


@user_router.get("/list")
def list_users(
    check: dict = Depends(require_user_management),
    db: Session = Depends(get_db),
):
    """List all users in the Databricks workspace."""
    try:
        svc = User_Management(token=check.get("token"))
        users = svc.list_users()
        return [
            {
                "id": u.id,
                "userName": u.user_name,
                "displayName": u.display_name,
                "active": u.active,
                "emails": [
                    {"value": e.value, "primary": getattr(e, "primary", False)}
                    for e in (u.emails or [])
                ],
                "groups": [
                    {"value": g.value, "display": g.display}
                    for g in (u.groups or [])
                ],
            }
            for u in users
        ]
    except Exception as e:
        logger.exception("Failed to list Databricks users")
        raise HTTPException(status_code=500, detail="Failed to list workspace users")


@user_router.get("/groups")
def list_groups(
    check: dict = Depends(require_user_management),
    db: Session = Depends(get_db),
):
    """List all groups in the Databricks workspace."""
    try:
        svc = User_Management(token=check.get("token"))
        groups = svc.list_groups()
        return [
            {
                "id": g.id,
                "displayName": g.display_name,
                "members": [
                    {"value": m.value, "display": getattr(m, "display", None)}
                    for m in (g.members or [])
                ],
            }
            for g in groups
        ]
    except Exception as e:
        logger.exception("Failed to list Databricks groups")
        raise HTTPException(status_code=500, detail="Failed to list workspace groups")


@user_router.get("/groups/{group_id}/members")
def list_group_members(
    group_id: str,
    check: dict = Depends(require_user_management),
    db: Session = Depends(get_db),
):
    """List members of a specific Databricks group."""
    try:
        svc = User_Management(token=check.get("token"))
        group = svc.w.groups.get(id=group_id)
        if not group:
            raise HTTPException(status_code=404, detail="Group not found")
        return [
            {"value": m.value, "display": getattr(m, "display", None)}
            for m in (group.members or [])
        ]
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Failed to list members for group {group_id}")
        raise HTTPException(status_code=500, detail="Failed to list group members")
