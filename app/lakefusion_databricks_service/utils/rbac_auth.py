"""
RBAC auth dependencies for the Databricks service.

Provides FastAPI dependencies to enforce role-based access on routes.
Uses token_required_wrapper for authentication, adds RBAC checks on top.
"""
from fastapi import Depends, HTTPException
from sqlalchemy.orm import Session

from app.lakefusion_databricks_service.utils.app_db import get_db, token_required_wrapper
from lakefusion_utility.services.rbac_service import RBACService
from lakefusion_utility.utils.logging_utils import get_logger

logger = get_logger(__name__)


def _extract_user(check: dict) -> tuple:
    """Extract (user_id, token) from token_required_wrapper result."""
    decoded = check.get("decoded") or {}
    user_id = decoded.get("sub") or decoded.get("email", "")
    token = check.get("token", "")
    return user_id.lower() if user_id else "", token


def require_admin(
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db),
) -> dict:
    """Dependency that ensures the caller has the Admin role (global)."""
    user_id, token = _extract_user(check)
    svc = RBACService(db)
    result = svc.check_role(user_id, required_roles=["Admin"])
    if not result.get("allowed"):
        raise HTTPException(
            status_code=403,
            detail="You do not have the necessary permissions to access this resource. Please contact your administrator.",
        )
    check["user_id"] = user_id
    return check


def require_user_management(
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db),
) -> dict:
    """Dependency that ensures the caller is authenticated.

    The User Management page is accessible to all authenticated users.
    Everyone can view groups and users, but only entity owners can edit
    groups associated with their entities (enforced at the UI level and
    by per-mutation ownership checks).

    Populates ``check["roles"]`` with the caller's globally-scoped roles so
    downstream ownership gates (see ``_is_admin`` in rbac_groups_route) can
    short-circuit admin mutations without re-querying.
    """
    user_id, _token = _extract_user(check)
    check["user_id"] = user_id
    if user_id:
        svc = RBACService(db)
        try:
            result = svc.check_role(user_id, required_roles=["Admin"])
            check["roles"] = result.get("roles", [])
        except Exception as e:
            logger.warning(f"Could not resolve roles for {user_id}: {e}")
            check["roles"] = []
    else:
        check["roles"] = []
    return check
