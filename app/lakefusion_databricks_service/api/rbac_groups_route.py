"""
RBAC Groups routes for the Databricks service.

Clean implementation providing group-based RBAC CRUD operations.
Syncs group membership changes to Databricks account-level groups
via the workspace-proxied SCIM API.
"""
from typing import List, Optional
from urllib.parse import unquote

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session, joinedload

from app.lakefusion_databricks_service.utils.app_db import get_db
from app.lakefusion_databricks_service.utils.rbac_auth import require_admin, require_user_management  # noqa: F401
from lakefusion_utility.models.entity import Entity
from lakefusion_utility.models.rbac import (
    Role,
    RBACGroup,
    GroupPermission,
    GroupMember,
    MemberType,
    RBACGroupCreateRequest,
)
from lakefusion_utility.services.rbac_service import RBACService
from lakefusion_utility.utils.databricks_util import _create_workspace_client
from lakefusion_utility.utils.logging_utils import get_logger

logger = get_logger(__name__)


def _is_admin(check: dict) -> bool:
    """True if the resolved auth context has Admin role."""
    roles = check.get("roles") or []
    return any((r or "").lower() == "admin" for r in roles)


def _gate_entity_group_ownership(
    db: Session, group: "RBACGroup", check: dict
) -> None:
    """Authorize mutations on per-entity RBAC groups.

    Admins pass through. Non-admins are only allowed if the group corresponds to an
    entity they created. The mapping is resolved via the RBACGroup id matching
    entity.developer_group_id / entity.steward_group_id (populated by
    RBACProvisioningService). Non-entity groups (e.g. workspace-scoped admin groups)
    are admin-only.
    """
    if _is_admin(check):
        return

    dbx_id = group.databricks_group_id
    if not dbx_id:
        raise HTTPException(status_code=403, detail="Only admins can modify this group")

    entity = (
        db.query(Entity)
        .filter(
            (Entity.developer_group_id == dbx_id) | (Entity.steward_group_id == dbx_id)
        )
        .first()
    )
    if not entity:
        raise HTTPException(status_code=403, detail="Only admins can modify this group")

    caller = (check.get("user_id") or "").lower()
    owner = (entity.created_by or "").lower()
    if caller != owner:
        raise HTTPException(
            status_code=403,
            detail=f"Only the entity owner ({entity.created_by}) or an admin can modify this group",
        )


# ---------------------------------------------------------------------------
# Databricks SCIM sync helpers
# ---------------------------------------------------------------------------

def _get_workspace_client(token: str) -> WorkspaceClient:
    """Create a WorkspaceClient using the caller's token.

    Delegates to ``_create_workspace_client`` so OAuth (Databricks Apps) and
    PAT (local dev) are both handled, and the DATABRICKS_HOST normalization
    (``https://`` prefix) lives in one place.
    """
    return _create_workspace_client(token)


def _resolve_databricks_user_id(w: WorkspaceClient, email: str) -> Optional[str]:
    """Look up a user's Databricks ID by email.

    Returns the Databricks user ID string, or None if not found.
    """
    try:
        users = list(w.users.list(filter=f'userName eq "{email}"'))
        if users:
            return users[0].id
        logger.warning(f"Databricks user not found for email: {email}")
        return None
    except Exception as e:
        logger.warning(f"Failed to resolve Databricks user ID for {email}: {e}")
        return None


def _resolve_or_create_databricks_group(
    w: WorkspaceClient,
    group: "RBACGroup",
    db: Session,
) -> Optional[str]:
    """Ensure the Databricks account-level group exists and return its ID.

    If the group's ``databricks_group_id`` is already set, return it.
    Otherwise, look up or create the group by ``dbx_display_name`` (or
    fallback to ``group.name``), update the DB record, and return the ID.
    """
    if group.databricks_group_id:
        return group.databricks_group_id

    display_name = group.dbx_display_name or group.name
    api = w.api_client

    try:
        # Look up existing account-level group by display name
        resp = api.do(
            "GET",
            "/api/2.0/account/scim/v2/Groups",
            query={"filter": f'displayName eq "{display_name}"'},
        )
        resources = resp.get("Resources", [])

        if resources:
            dbx_group_id = str(resources[0]["id"])
            logger.info(
                f"Found existing Databricks group '{display_name}' (id={dbx_group_id})"
            )
        else:
            # Create the account-level group
            created = api.do(
                "POST",
                "/api/2.0/account/scim/v2/Groups",
                body={"displayName": display_name},
            )
            dbx_group_id = str(created["id"])
            logger.info(
                f"Created Databricks group '{display_name}' (id={dbx_group_id})"
            )

            # Assign to workspace so it's visible
            api.do(
                "PUT",
                f"/api/2.0/preview/permissionassignments/principals/{dbx_group_id}",
                body={"permissions": ["USER"]},
            )
            logger.info(f"Assigned group '{display_name}' to workspace")

        # Persist back to DB so future calls skip the lookup
        group.databricks_group_id = dbx_group_id
        group.dbx_display_name = display_name
        db.commit()

        return dbx_group_id
    except Exception as e:
        logger.error(
            f"Failed to resolve/create Databricks group '{display_name}': {e}"
        )
        return None


def _sync_add_member_to_databricks(
    w: WorkspaceClient,
    databricks_group_id: str,
    dbx_display_name: str,
    user_email: str,
) -> None:
    """Add a user to a Databricks account-level group via SCIM PATCH.

    Non-blocking: logs warnings on failure but does NOT raise.
    The DB record is the source of truth; Databricks sync is best-effort.
    """
    try:
        dbx_user_id = _resolve_databricks_user_id(w, user_email)
        if not dbx_user_id:
            logger.warning(
                f"Skipping Databricks sync — user {user_email} not found in Databricks"
            )
            return

        api = w.api_client
        api.do(
            "PATCH",
            f"/api/2.0/account/scim/v2/Groups/{databricks_group_id}",
            body={
                "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                "Operations": [
                    {
                        "op": "add",
                        "path": "members",
                        "value": [{"value": dbx_user_id}],
                    }
                ],
            },
        )
        logger.info(
            f"Synced to Databricks: added {user_email} (id={dbx_user_id}) "
            f"to group '{dbx_display_name}' (id={databricks_group_id})"
        )
    except Exception as e:
        logger.error(
            f"Failed to sync add-member to Databricks: "
            f"user={user_email}, group={dbx_display_name}, error={e}"
        )


def _sync_remove_member_from_databricks(
    w: WorkspaceClient,
    databricks_group_id: str,
    dbx_display_name: str,
    user_email: str,
) -> None:
    """Remove a user from a Databricks account-level group via SCIM PATCH.

    Non-blocking: logs warnings on failure but does NOT raise.
    """
    try:
        dbx_user_id = _resolve_databricks_user_id(w, user_email)
        if not dbx_user_id:
            logger.warning(
                f"Skipping Databricks sync — user {user_email} not found in Databricks"
            )
            return

        api = w.api_client
        api.do(
            "PATCH",
            f"/api/2.0/account/scim/v2/Groups/{databricks_group_id}",
            body={
                "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                "Operations": [
                    {
                        "op": "remove",
                        "path": "members",
                        "value": [{"value": dbx_user_id}],
                    }
                ],
            },
        )
        logger.info(
            f"Synced to Databricks: removed {user_email} (id={dbx_user_id}) "
            f"from group '{dbx_display_name}' (id={databricks_group_id})"
        )
    except Exception as e:
        logger.error(
            f"Failed to sync remove-member from Databricks: "
            f"user={user_email}, group={dbx_display_name}, error={e}"
        )


def _sync_add_group_member_to_databricks(
    w: WorkspaceClient,
    parent_databricks_group_id: str,
    parent_display_name: str,
    child_databricks_group_id: str,
    child_display_name: str = "",
) -> tuple[bool, Optional[str]]:
    """Nest ``child`` inside ``parent`` on the Databricks account-level group via SCIM.
    """
    try:
        api = w.api_client

        try:
            current = api.do(
                "GET",
                f"/api/2.0/account/scim/v2/Groups/{parent_databricks_group_id}",
            )
            existing = {
                str(m.get("value"))
                for m in (current.get("members") or [])
            }
            if str(child_databricks_group_id) in existing:
                logger.info(
                    f"Group '{child_display_name or child_databricks_group_id}' "
                    f"already nested in '{parent_display_name}' — skipping PATCH"
                )
                return True, None
        except Exception as read_err:
            logger.debug(
                f"Could not pre-read members of {parent_display_name}: {read_err}"
            )

        api.do(
            "PATCH",
            f"/api/2.0/account/scim/v2/Groups/{parent_databricks_group_id}",
            body={
                "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                "Operations": [
                    {
                        "op": "add",
                        "path": "members",
                        "value": [
                            {
                                "value": str(child_databricks_group_id),
                                "type": "Group",
                            }
                        ],
                    }
                ],
            },
        )
        logger.info(
            f"Synced to Databricks: nested group "
            f"'{child_display_name or child_databricks_group_id}' "
            f"(id={child_databricks_group_id}) inside "
            f"'{parent_display_name}' (id={parent_databricks_group_id})"
        )
        return True, None
    except Exception as e:
        err = str(e)
        logger.error(
            f"Failed to sync nested-group add to Databricks: "
            f"parent={parent_display_name}, child={child_databricks_group_id}, error={err}"
        )
        return False, err


def _sync_remove_group_member_from_databricks(
    w: WorkspaceClient,
    parent_databricks_group_id: str,
    parent_display_name: str,
    child_databricks_group_id: str,
) -> bool:
    """Un-nest ``child`` from ``parent`` on Databricks via SCIM PATCH.

    Non-blocking; mirrors :func:`_sync_add_group_member_to_databricks`.
    """
    try:
        api = w.api_client
        api.do(
            "PATCH",
            f"/api/2.0/account/scim/v2/Groups/{parent_databricks_group_id}",
            body={
                "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                "Operations": [
                    {
                        "op": "remove",
                        "path": f'members[value eq "{child_databricks_group_id}"]',
                    }
                ],
            },
        )
        logger.info(
            f"Synced to Databricks: un-nested group "
            f"id={child_databricks_group_id} from "
            f"'{parent_display_name}' (id={parent_databricks_group_id})"
        )
        return True
    except Exception as e:
        logger.error(
            f"Failed to sync nested-group remove from Databricks: "
            f"parent={parent_display_name}, child={child_databricks_group_id}, error={e}"
        )
        return False


def _resolve_entity_role_group_dbx_id(
    db: Session, entity_id: int, role_name: str
) -> tuple[Optional[str], Optional[str]]:
    """Resolve the Databricks group ID + display name for (entity, role).
    """
    entity = db.query(Entity).filter(Entity.id == entity_id).first()
    if not entity:
        return None, None

    role_lower = (role_name or "").lower()
    if role_lower == "developer":
        return entity.developer_group_id, f"LAKEFUSION_DEVELOPER_{entity.name.upper()}"
    if role_lower in ("data steward", "data_steward", "steward"):
        return entity.steward_group_id, f"LAKEFUSION_DATA_STEWARD_{entity.name.upper()}"
    return None, None


rbac_groups_router = APIRouter(tags=["RBAC Groups API"], prefix="/rbac-groups")


# ---------------------------------------------------------------------------
# Pydantic request models
# ---------------------------------------------------------------------------

class AssignRequest(BaseModel):
    user_id: str
    role_id: int
    entity_id: Optional[int] = None


class UnassignRequest(BaseModel):
    user_id: str
    role_id: int
    entity_id: Optional[int] = None


class OnboardRequest(BaseModel):
    user_ids: List[str]


class AddGroupMemberRequest(BaseModel):
    child_group_id: int


class AddUserMemberRequest(BaseModel):
    user_id: str


class AddPermissionRequest(BaseModel):
    role_id: int
    entity_id: Optional[int] = None


class WorkspaceGroupPermissionRequest(BaseModel):
    """Grant entity access to a Databricks workspace group.

    If the workspace group doesn't have an rbac_groups record yet,
    one is auto-created with managed_by='databricks'.
    """
    databricks_group_id: str
    display_name: str
    role_id: int
    entity_id: Optional[int] = None


class AccessPreviewRequest(BaseModel):
    user_id: str
    entity_ids: List[int]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _group_to_response(group: RBACGroup) -> dict:
    """Convert an RBACGroup ORM instance to a serializable dict."""
    perms = []
    for p in (group.permissions or []):
        perms.append({
            "id": p.id,
            "group_id": p.group_id,
            "entity_id": p.entity_id,
            "entity_name": p.entity.name if p.entity else None,
            "role_id": p.role_id,
            "role_name": p.role.name if p.role else None,
            "is_global_permission": p.entity_id is None,
            "created_at": p.created_at.isoformat() if p.created_at else None,
            "created_by": p.created_by,
        })
    return {
        "id": group.id,
        "name": group.name,
        "description": group.description,
        "is_global": group.is_global,
        "managed_by": group.managed_by,
        "databricks_group_id": group.databricks_group_id,
        "dbx_display_name": group.dbx_display_name,
        "permission_count": len(group.permissions or []),
        "member_count": len(group.members or []),
        "permissions": perms,
        "created_at": group.created_at.isoformat() if group.created_at else None,
        "updated_at": group.updated_at.isoformat() if group.updated_at else None,
        "created_by": group.created_by,
        "source": group.managed_by or "lakefusion",
    }


def _get_group_or_404(db: Session, group_id: int) -> RBACGroup:
    group = (
        db.query(RBACGroup)
        .options(
            joinedload(RBACGroup.permissions).joinedload(GroupPermission.role),
            joinedload(RBACGroup.permissions).joinedload(GroupPermission.entity),
            joinedload(RBACGroup.members),
        )
        .filter(RBACGroup.id == group_id)
        .first()
    )
    if not group:
        raise HTTPException(status_code=404, detail="RBAC group not found")
    return group


# ---------------------------------------------------------------------------
# Group CRUD
# ---------------------------------------------------------------------------

@rbac_groups_router.get("")
def list_rbac_groups(
    check: dict = Depends(require_user_management),
    db: Session = Depends(get_db),
):
    """List all RBAC groups."""
    try:
        groups = (
            db.query(RBACGroup)
            .options(
                joinedload(RBACGroup.permissions).joinedload(GroupPermission.role),
                joinedload(RBACGroup.permissions).joinedload(GroupPermission.entity),
                joinedload(RBACGroup.members),
            )
            .all()
        )
        return [_group_to_response(g) for g in groups]
    except Exception as e:
        logger.exception("Failed to list RBAC groups")
        raise HTTPException(status_code=500, detail="Failed to list RBAC groups")


@rbac_groups_router.post("")
def create_rbac_group(
    req: RBACGroupCreateRequest,
    check: dict = Depends(require_user_management),
    db: Session = Depends(get_db),
):
    """Create a new RBAC group."""
    try:
        actor = check.get("user_id", "")
        existing = db.query(RBACGroup).filter(RBACGroup.name == req.name).first()
        if existing:
            raise HTTPException(status_code=409, detail=f"Group '{req.name}' already exists")

        group = RBACGroup(
            name=req.name,
            description=req.description,
            is_global=req.is_global,
            created_by=actor,
        )
        db.add(group)
        db.flush()

        for perm_req in (req.permissions or []):
            perm = GroupPermission(
                group_id=group.id,
                role_id=perm_req.role_id,
                entity_id=perm_req.entity_id,
                created_by=actor,
            )
            db.add(perm)

        db.commit()
        db.refresh(group)

        group = _get_group_or_404(db, group.id)
        return _group_to_response(group)
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.exception("Failed to create RBAC group")
        raise HTTPException(status_code=500, detail="Failed to create RBAC group")


# ---------------------------------------------------------------------------
# Group Members
# ---------------------------------------------------------------------------

@rbac_groups_router.get("/{group_id}/members")
def get_group_members(
    group_id: int,
    check: dict = Depends(require_user_management),
    db: Session = Depends(get_db),
):
    """Get members of an RBAC group."""
    try:
        group = _get_group_or_404(db, group_id)
        members = (
            db.query(GroupMember)
            .filter(GroupMember.group_id == group_id)
            .all()
        )
        result = []
        for m in members:
            member_group_name = None
            if m.member_type == MemberType.GROUP.value:
                child = db.query(RBACGroup).filter(RBACGroup.id == int(m.user_id)).first()
                member_group_name = child.name if child else None
            result.append({
                "id": m.id,
                "user_id": m.user_id,
                "group_id": m.group_id,
                "group_name": group.name,
                "is_global_group": group.is_global,
                "member_type": m.member_type,
                "member_group_name": member_group_name,
                "created_at": m.created_at.isoformat() if m.created_at else None,
                "updated_at": m.updated_at.isoformat() if m.updated_at else None,
                "created_by": m.created_by,
            })
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Failed to get members for group {group_id}")
        raise HTTPException(status_code=500, detail="Failed to get group members")


@rbac_groups_router.post("/{group_id}/members")
def add_user_member(
    group_id: int,
    req: AddUserMemberRequest,
    check: dict = Depends(require_user_management),
    db: Session = Depends(get_db),
):
    """Add a user to an RBAC group and sync to Databricks."""
    try:
        actor = check.get("user_id", "")
        group = _get_group_or_404(db, group_id)
        _gate_entity_group_ownership(db, group, check)

        existing = (
            db.query(GroupMember)
            .filter(
                GroupMember.group_id == group_id,
                GroupMember.user_id == req.user_id,
                GroupMember.member_type == MemberType.USER.value,
            )
            .first()
        )
        if existing:
            raise HTTPException(status_code=409, detail="User is already a member of this group")

        member = GroupMember(
            user_id=req.user_id,
            group_id=group_id,
            created_by=actor,
            member_type=MemberType.USER.value,
        )
        db.add(member)
        db.commit()

        # ── Sync to Databricks account-level group ────────────────────
        dbx_synced = False
        token = check.get("token", "")
        if token:
            w = _get_workspace_client(token)
            dbx_group_id = _resolve_or_create_databricks_group(w, group, db)
            if dbx_group_id:
                _sync_add_member_to_databricks(
                    w, dbx_group_id, group.dbx_display_name or group.name, req.user_id,
                )
                dbx_synced = True
            else:
                logger.warning(
                    f"Could not resolve Databricks group for {group.name} — "
                    f"user {req.user_id} added to DB only"
                )
        else:
            logger.warning(
                f"No token available for Databricks sync — "
                f"user {req.user_id} added to DB only"
            )

        return {
            "message": f"User {req.user_id} added to group {group_id}",
            "databricks_synced": dbx_synced,
        }
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.exception(f"Failed to add member to group {group_id}")
        raise HTTPException(status_code=500, detail="Failed to add member")


@rbac_groups_router.delete("/{group_id}/members/{user_id}")
def remove_user_member(
    group_id: int,
    user_id: str,
    check: dict = Depends(require_user_management),
    db: Session = Depends(get_db),
):
    """Remove a user from an RBAC group and sync to Databricks."""
    try:
        decoded_user_id = unquote(user_id)
        group = _get_group_or_404(db, group_id)
        _gate_entity_group_ownership(db, group, check)

        member = (
            db.query(GroupMember)
            .filter(
                GroupMember.group_id == group_id,
                GroupMember.user_id == decoded_user_id,
                GroupMember.member_type == MemberType.USER.value,
            )
            .first()
        )
        if not member:
            raise HTTPException(status_code=404, detail="Member not found in group")

        db.delete(member)
        db.commit()

        # ── Sync to Databricks account-level group ────────────────────
        dbx_synced = False
        token = check.get("token", "")
        if token:
            w = _get_workspace_client(token)
            dbx_group_id = _resolve_or_create_databricks_group(w, group, db)
            if dbx_group_id:
                _sync_remove_member_from_databricks(
                    w, dbx_group_id, group.dbx_display_name or group.name, decoded_user_id,
                )
                dbx_synced = True
            else:
                logger.warning(
                    f"Could not resolve Databricks group for {group.name} — "
                    f"user {decoded_user_id} removed from DB only"
                )
        else:
            logger.warning(
                f"No token available for Databricks sync — "
                f"user {decoded_user_id} removed from DB only"
            )

        return {
            "message": f"User {decoded_user_id} removed from group {group_id}",
            "databricks_synced": dbx_synced,
        }
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.exception(f"Failed to remove member from group {group_id}")
        raise HTTPException(status_code=500, detail="Failed to remove member")


# ---------------------------------------------------------------------------
# Nested Group Members
# ---------------------------------------------------------------------------

@rbac_groups_router.post("/{group_id}/group-members")
def add_group_member(
    group_id: int,
    req: AddGroupMemberRequest,
    check: dict = Depends(require_user_management),
    db: Session = Depends(get_db),
):
    """Add a child group as a member of an RBAC group (nesting).

    **Fail-fast**: SCIM nesting runs first; the GroupMember row is only
    persisted if Databricks confirms. A failure here returns 502, not a
    misleading 200 with a warning.
    """
    try:
        actor = check.get("user_id", "")
        parent = _get_group_or_404(db, group_id)
        _gate_entity_group_ownership(db, parent, check)
        child = db.query(RBACGroup).filter(RBACGroup.id == req.child_group_id).first()
        if not child:
            raise HTTPException(status_code=404, detail="Child group not found")
        if req.child_group_id == group_id:
            raise HTTPException(status_code=400, detail="Cannot nest a group inside itself")

        existing = (
            db.query(GroupMember)
            .filter(
                GroupMember.group_id == group_id,
                GroupMember.user_id == str(req.child_group_id),
                GroupMember.member_type == MemberType.GROUP.value,
            )
            .first()
        )
        if existing:
            raise HTTPException(status_code=409, detail="Group is already a member")

        # ── Databricks SCIM first ─────────────────────────────────────
        if not parent.databricks_group_id:
            raise HTTPException(
                status_code=502,
                detail=(
                    f"Parent group '{parent.name}' has no Databricks group ID. "
                    f"Cannot create nesting that won't propagate permissions."
                ),
            )
        if not child.databricks_group_id:
            raise HTTPException(
                status_code=502,
                detail=(
                    f"Child group '{child.name}' has no Databricks group ID. "
                    f"Cannot create nesting that won't propagate permissions."
                ),
            )

        token = check.get("token", "")
        if not token:
            raise HTTPException(
                status_code=503,
                detail="No Databricks token in request context — cannot sync.",
            )

        w = _get_workspace_client(token)
        ok, sync_err = _sync_add_group_member_to_databricks(
            w,
            parent.databricks_group_id,
            parent.dbx_display_name or parent.name,
            child.databricks_group_id,
            child.dbx_display_name or child.name,
        )
        if not ok:
            raise HTTPException(
                status_code=502,
                detail=(
                    f"Databricks SCIM sync failed: {sync_err}. No DB row created."
                ),
            )

        # SCIM confirmed — persist the membership row.
        member = GroupMember(
            user_id=str(req.child_group_id),
            group_id=group_id,
            created_by=actor,
            member_type=MemberType.GROUP.value,
        )
        db.add(member)
        db.commit()

        return {
            "message": f"Group {req.child_group_id} added as member of group {group_id}",
            "databricks_synced": True,
        }
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.exception(f"Failed to add group member to group {group_id}")
        raise HTTPException(status_code=500, detail="Failed to add group member")


@rbac_groups_router.delete("/{group_id}/group-members/{child_group_id}")
def remove_group_member(
    group_id: int,
    child_group_id: int,
    check: dict = Depends(require_user_management),
    db: Session = Depends(get_db),
):
    """Remove a nested group from an RBAC group."""
    try:
        parent = _get_group_or_404(db, group_id)
        _gate_entity_group_ownership(db, parent, check)
        member = (
            db.query(GroupMember)
            .filter(
                GroupMember.group_id == group_id,
                GroupMember.user_id == str(child_group_id),
                GroupMember.member_type == MemberType.GROUP.value,
            )
            .first()
        )
        if not member:
            raise HTTPException(status_code=404, detail="Group member not found")

        # Capture child's Databricks group ID before delete so we can un-nest.
        child = db.query(RBACGroup).filter(RBACGroup.id == child_group_id).first()
        child_dbx_id = child.databricks_group_id if child else None
        child_display = (child.dbx_display_name or child.name) if child else ""

        db.delete(member)
        db.commit()

        # ── Mirror the removal on Databricks SCIM.
        dbx_synced = False
        sync_warning: Optional[str] = None
        if not parent.databricks_group_id or not child_dbx_id:
            sync_warning = "Missing Databricks group ID; un-nest skipped."
            logger.warning(sync_warning)
        else:
            token = check.get("token", "")
            if token:
                w = _get_workspace_client(token)
                dbx_synced = _sync_remove_group_member_from_databricks(
                    w,
                    parent.databricks_group_id,
                    parent.dbx_display_name or parent.name,
                    child_dbx_id,
                )
                if not dbx_synced:
                    sync_warning = "Databricks SCIM PATCH failed."
            else:
                sync_warning = "No token in request context — Databricks sync skipped."
                logger.warning(sync_warning)

        return {
            "message": f"Group {child_group_id} removed from group {group_id}",
            "databricks_synced": dbx_synced,
            "warning": sync_warning,
        }
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.exception(f"Failed to remove group member from group {group_id}")
        raise HTTPException(status_code=500, detail="Failed to remove group member")


# ---------------------------------------------------------------------------
# Group Permissions
# ---------------------------------------------------------------------------

@rbac_groups_router.get("/{group_id}/permissions")
def get_group_permissions(
    group_id: int,
    check: dict = Depends(require_user_management),
    db: Session = Depends(get_db),
):
    """Get permissions assigned to an RBAC group."""
    try:
        group = _get_group_or_404(db, group_id)
        perms = (
            db.query(GroupPermission)
            .options(
                joinedload(GroupPermission.role),
                joinedload(GroupPermission.entity),
            )
            .filter(GroupPermission.group_id == group_id)
            .all()
        )
        return [
            {
                "id": p.id,
                "group_id": p.group_id,
                "entity_id": p.entity_id,
                "entity_name": p.entity.name if p.entity else None,
                "role_id": p.role_id,
                "role_name": p.role.name if p.role else None,
                "is_global_permission": p.entity_id is None,
                "created_at": p.created_at.isoformat() if p.created_at else None,
                "created_by": p.created_by,
            }
            for p in perms
        ]
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Failed to get permissions for group {group_id}")
        raise HTTPException(status_code=500, detail="Failed to get group permissions")


@rbac_groups_router.post("/{group_id}/permissions")
def add_permission(
    group_id: int,
    req: AddPermissionRequest,
    check: dict = Depends(require_user_management),
    db: Session = Depends(get_db),
):
    """Add a permission (role+entity pair) to an RBAC group."""
    try:
        actor = check.get("user_id", "")
        _get_group_or_404(db, group_id)

        role = db.query(Role).filter(Role.id == req.role_id).first()
        if not role:
            raise HTTPException(status_code=404, detail="Role not found")

        existing = (
            db.query(GroupPermission)
            .filter(
                GroupPermission.group_id == group_id,
                GroupPermission.role_id == req.role_id,
                GroupPermission.entity_id == req.entity_id if req.entity_id else GroupPermission.entity_id.is_(None),
            )
            .first()
        )
        if existing:
            raise HTTPException(status_code=409, detail="Permission already exists on this group")

        perm = GroupPermission(
            group_id=group_id,
            role_id=req.role_id,
            entity_id=req.entity_id,
            created_by=actor,
        )
        db.add(perm)
        db.commit()
        return {"message": "Permission added", "permission_id": perm.id}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.exception(f"Failed to add permission to group {group_id}")
        raise HTTPException(status_code=500, detail="Failed to add permission")


@rbac_groups_router.delete("/{group_id}/permissions/global/{role_id}")
def remove_global_permission(
    group_id: int,
    role_id: int,
    check: dict = Depends(require_user_management),
    db: Session = Depends(get_db),
):
    """Remove a global (entity_id=NULL) permission from an RBAC group."""
    try:
        perm = (
            db.query(GroupPermission)
            .filter(
                GroupPermission.group_id == group_id,
                GroupPermission.role_id == role_id,
                GroupPermission.entity_id.is_(None),
            )
            .first()
        )
        if not perm:
            raise HTTPException(status_code=404, detail="Global permission not found")

        db.delete(perm)
        db.commit()
        return {"message": "Global permission removed"}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.exception(f"Failed to remove global permission from group {group_id}")
        raise HTTPException(status_code=500, detail="Failed to remove permission")


@rbac_groups_router.delete("/{group_id}/permissions/{role_id}/{entity_id}")
def remove_entity_permission(
    group_id: int,
    role_id: int,
    entity_id: int,
    check: dict = Depends(require_user_management),
    db: Session = Depends(get_db),
):
    """Remove an entity-scoped permission from an RBAC group."""
    try:
        perm = (
            db.query(GroupPermission)
            .filter(
                GroupPermission.group_id == group_id,
                GroupPermission.role_id == role_id,
                GroupPermission.entity_id == entity_id,
            )
            .first()
        )
        if not perm:
            raise HTTPException(status_code=404, detail="Permission not found")

        # Resolve the workspace group + entity-role group BEFORE the delete so
        # we can mirror the un-nest on Databricks SCIM. If we don't un-nest,
        # the user will keep inheriting permissions through the parent group
        # even though our DB says the permission is gone.
        group = db.query(RBACGroup).filter(RBACGroup.id == group_id).first()
        role = db.query(Role).filter(Role.id == role_id).first()
        child_dbx_id = group.databricks_group_id if group else None
        parent_dbx_id, parent_display = (None, None)
        if role:
            parent_dbx_id, parent_display = _resolve_entity_role_group_dbx_id(
                db, entity_id, role.name
            )

        db.delete(perm)
        db.commit()

        dbx_synced = False
        sync_warning: Optional[str] = None
        if parent_dbx_id and child_dbx_id:
            token = check.get("token", "")
            if token:
                w = _get_workspace_client(token)
                dbx_synced = _sync_remove_group_member_from_databricks(
                    w, parent_dbx_id, parent_display or "", child_dbx_id,
                )
                if not dbx_synced:
                    sync_warning = (
                        "Databricks SCIM PATCH failed; child group may still "
                        "inherit permissions from parent."
                    )
            else:
                sync_warning = "No token in request context — Databricks sync skipped."
                logger.warning(sync_warning)

        return {
            "message": "Permission removed",
            "databricks_synced": dbx_synced,
            "warning": sync_warning,
        }
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.exception(f"Failed to remove permission from group {group_id}")
        raise HTTPException(status_code=500, detail="Failed to remove permission")


# ---------------------------------------------------------------------------
# Workspace Group Permissions
# ---------------------------------------------------------------------------

@rbac_groups_router.post("/workspace-group-permission")
def add_workspace_group_permission(
    req: WorkspaceGroupPermissionRequest,
    check: dict = Depends(require_user_management),
    db: Session = Depends(get_db),
):
    """Grant entity access to a Databricks workspace group.

    Auto-creates an rbac_groups record (managed_by='databricks') on first
    permission assignment. Subsequent calls reuse the existing record.

    **Fail-fast contract**: the GroupPermission row is only persisted *after*
    the Databricks SCIM nesting succeeds. If provisioning or the PATCH fail,
    we rollback and return a non-2xx so the UI doesn't show a misleading
    "success" toast for a permission that won't actually propagate.
    """
    try:
        actor = check.get("user_id", "")

        role = db.query(Role).filter(Role.id == req.role_id).first()
        if not role:
            raise HTTPException(status_code=404, detail="Role not found")

        # Find or create the rbac_groups record for this workspace group.
        # Note: we only flush — the perm row goes in after Databricks confirms.
        group = (
            db.query(RBACGroup)
            .filter(RBACGroup.databricks_group_id == req.databricks_group_id)
            .first()
        )
        if not group:
            group = RBACGroup(
                name=req.display_name,
                managed_by="databricks",
                databricks_group_id=req.databricks_group_id,
                dbx_display_name=req.display_name,
                is_global=False,
                created_by=actor,
            )
            db.add(group)
            db.flush()
            logger.info(
                f"Auto-created rbac_groups record for workspace group "
                f"'{req.display_name}' (dbx_id={req.databricks_group_id})"
            )

        # Check for duplicate permission
        existing = (
            db.query(GroupPermission)
            .filter(
                GroupPermission.group_id == group.id,
                GroupPermission.role_id == req.role_id,
                GroupPermission.entity_id == req.entity_id
                if req.entity_id
                else GroupPermission.entity_id.is_(None),
            )
            .first()
        )
        if existing:
            raise HTTPException(
                status_code=409,
                detail="This group already has this permission",
            )

        # ── Sync Databricks BEFORE committing the perm row ─────────────
        # Without this ordering, a SCIM failure leaves an orphan DB row
        # that lies to the UI. Global (entity_id=None) permissions skip
        # the nesting step — they don't map to a per-entity LAKEFUSION_ group.
        if req.entity_id:
            token = check.get("token", "")
            if not token:
                db.rollback()
                raise HTTPException(
                    status_code=503,
                    detail="No Databricks token in request context — cannot sync.",
                )

            parent_dbx_id, parent_display = _resolve_entity_role_group_dbx_id(
                db, req.entity_id, role.name
            )

            # On-demand provision if the entity predates the new entity-create flow.
            if not parent_dbx_id:
                try:
                    from lakefusion_utility.services.rbac_provisioning_service import (
                        RBACProvisioningService,
                    )
                    entity_obj = (
                        db.query(Entity).filter(Entity.id == req.entity_id).first()
                    )
                    if not entity_obj:
                        db.rollback()
                        raise HTTPException(
                            status_code=404,
                            detail=f"Entity {req.entity_id} not found",
                        )
                    provisioner = RBACProvisioningService(db=db, token=token)
                    # Use the broader scope so the workspace group, once nested,
                    # actually inherits SELECT/MODIFY on already-existing prod
                    # tables. NotFound rows are skipped silently for entities
                    # whose tables don't exist yet.
                    result = provisioner.provision_entity(
                        entity_obj,
                        creator_email=entity_obj.created_by,
                        applies_to=("always", "prod"),
                        pipeline_type="integration",
                    )
                    db.flush()
                    logger.info(
                        f"Auto-provisioned RBAC groups for entity "
                        f"'{entity_obj.name}' (dev={result.get('developer_group_id')}, "
                        f"steward={result.get('steward_group_id')}, "
                        f"grants={result.get('grants_applied')}, "
                        f"skipped={result.get('grants_skipped')})"
                    )
                    parent_dbx_id, parent_display = _resolve_entity_role_group_dbx_id(
                        db, req.entity_id, role.name
                    )
                except HTTPException:
                    raise
                except Exception as prov_err:
                    db.rollback()
                    logger.exception(
                        f"On-demand provisioning failed for entity {req.entity_id}"
                    )
                    raise HTTPException(
                        status_code=502,
                        detail=(
                            f"Databricks provisioning failed for entity "
                            f"{req.entity_id}: {prov_err}"
                        ),
                    )

            if not parent_dbx_id:
                db.rollback()
                raise HTTPException(
                    status_code=502,
                    detail=(
                        f"Could not resolve '{role.name}' group for entity "
                        f"{req.entity_id} after provisioning. Check Databricks logs."
                    ),
                )

            # Now nest the workspace group inside the entity-role group.
            w = _get_workspace_client(token)
            ok, sync_err = _sync_add_group_member_to_databricks(
                w,
                parent_dbx_id,
                parent_display or "",
                req.databricks_group_id,
                req.display_name,
            )
            if not ok:
                db.rollback()
                raise HTTPException(
                    status_code=502,
                    detail=(
                        f"Databricks SCIM sync failed: {sync_err}. "
                        f"No DB row created."
                    ),
                )

        # Databricks is in sync (or skipped for global perms). Persist the row.
        perm = GroupPermission(
            group_id=group.id,
            role_id=req.role_id,
            entity_id=req.entity_id,
            created_by=actor,
        )
        db.add(perm)
        db.commit()

        return {
            "message": f"Granted {role.name} access to {req.display_name}",
            "rbac_group_id": group.id,
            "permission_id": perm.id,
            "databricks_synced": bool(req.entity_id),
        }
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.exception("Failed to add workspace group permission")
        raise HTTPException(
            status_code=500, detail="Failed to grant access to workspace group"
        )


# ---------------------------------------------------------------------------
# User Assignments (derived from groups)
# ---------------------------------------------------------------------------

@rbac_groups_router.get("/user/{user_id}/assignments")
def get_user_assignments(
    user_id: str,
    check: dict = Depends(require_user_management),
    db: Session = Depends(get_db),
):
    """Get role assignments derived from a user's RBAC group memberships.

    Includes both permission-based assignments and assignments derived
    from LAKEFUSION_ system group membership.
    """
    try:
        decoded_user_id = unquote(user_id)

        # Pre-load entity and role maps for system group derivation
        entities = db.query(Entity).all()
        entity_by_name_upper = {e.name.upper(): e for e in entities}
        roles = db.query(Role).all()
        role_by_name_lower = {r.name.lower(): r for r in roles}

        # Get the user's group memberships (with group info) for system group derivation
        user_memberships = (
            db.query(GroupMember, RBACGroup)
            .join(RBACGroup, RBACGroup.id == GroupMember.group_id)
            .filter(
                GroupMember.user_id == decoded_user_id,
                GroupMember.member_type == MemberType.USER.value,
            )
            .all()
        )

        seen = set()
        assignments = []

        # Phase 1: Derive assignments from system group names
        for member, group in user_memberships:
            parsed = _parse_system_group_role(group.name)
            if not parsed:
                continue
            role_name, entity_name_upper = parsed
            entity = entity_by_name_upper.get(entity_name_upper) if entity_name_upper else None
            role = role_by_name_lower.get(role_name.lower())

            key = (role_name, entity_name_upper or "GLOBAL")
            if key in seen:
                continue
            seen.add(key)

            assignments.append({
                "user_id": decoded_user_id,
                "role_id": role.id if role else 0,
                "role_name": role_name,
                "entity_id": entity.id if entity else None,
                "entity_name": entity.name if entity else None,
                "group_id": group.id,
                "group_name": group.name,
                "is_global": entity_name_upper is None,
                "uc_sync_status": "not_applicable",
            })

        # Phase 2: Permission-based assignments (from custom/personal groups)
        svc = RBACService(db)
        effective = svc.get_effective_user_roles(decoded_user_id)
        for perm in effective.inherited_permissions:
            key = (perm.role_name, (perm.entity_name or "GLOBAL").upper())
            if key in seen:
                continue
            seen.add(key)

            group_info = next(
                (g for g in effective.rbac_groups if g.group_id == perm.group_id),
                None,
            )
            assignments.append({
                "user_id": decoded_user_id,
                "role_id": perm.role_id,
                "role_name": perm.role_name,
                "entity_id": perm.entity_id,
                "entity_name": perm.entity_name,
                "group_id": perm.group_id,
                "group_name": group_info.group_name if group_info else None,
                "is_global": perm.is_global_permission,
                "uc_sync_status": "not_applicable",
            })
        return assignments
    except Exception as e:
        logger.exception(f"Failed to get assignments for user {user_id}")
        raise HTTPException(status_code=500, detail="Failed to get user assignments")


def _parse_system_group_role(group_name: str) -> Optional[tuple]:
    """Parse a LAKEFUSION_ system group name into (role_name, entity_name_upper).

    Returns None if the group name is not a recognized pattern.
    e.g. "LAKEFUSION_DEVELOPER_PERSON" → ("Developer", "PERSON")
         "LAKEFUSION_DATA_STEWARD_PERSON" → ("Data Steward", "PERSON")
         "LAKEFUSION_ADMINS_12345" → ("Admin", None)
    """
    if not group_name.startswith("LAKEFUSION_"):
        return None
    without_prefix = group_name[len("LAKEFUSION_"):]
    if without_prefix.startswith("DEVELOPER_"):
        return ("Developer", without_prefix[len("DEVELOPER_"):])
    if without_prefix.startswith("DATA_STEWARD_"):
        return ("Data Steward", without_prefix[len("DATA_STEWARD_"):])
    if without_prefix.startswith("ADMINS"):
        return ("Admin", None)
    return None


@rbac_groups_router.get("/all-assignments")
def get_all_assignments(
    limit: int = Query(default=2000, ge=1, le=10000),
    offset: int = Query(default=0, ge=0),
    check: dict = Depends(require_user_management),
    db: Session = Depends(get_db),
):
    """Get all group-derived role assignments (paginated).

    Includes both:
    - Permission-based assignments (from GroupPermission records)
    - System group assignments (derived from LAKEFUSION_DEVELOPER_* / DATA_STEWARD_* group names)
    """
    try:
        # Pre-load entity name→id map and role name→id map for system group derivation
        entities = db.query(Entity).all()
        entity_by_name_upper = {e.name.upper(): e for e in entities}

        roles = db.query(Role).all()
        role_by_name_lower = {r.name.lower(): r for r in roles}

        # Get all user memberships with their group info in a single query
        user_members = (
            db.query(GroupMember, RBACGroup)
            .join(RBACGroup, RBACGroup.id == GroupMember.group_id)
            .filter(GroupMember.member_type == MemberType.USER.value)
            .all()
        )

        # Track which (user, group) combos produce assignments to avoid duplicates
        seen = set()
        all_assignments = []

        # ── Phase 1: Derive assignments from system group names ───────
        for member, group in user_members:
            parsed = _parse_system_group_role(group.name)
            if not parsed:
                continue
            role_name, entity_name_upper = parsed
            entity = entity_by_name_upper.get(entity_name_upper) if entity_name_upper else None
            role = role_by_name_lower.get(role_name.lower())

            key = (member.user_id, role_name, entity_name_upper or "GLOBAL")
            if key in seen:
                continue
            seen.add(key)

            all_assignments.append({
                "user_id": member.user_id,
                "role_id": role.id if role else 0,
                "role_name": role_name,
                "entity_id": entity.id if entity else None,
                "entity_name": entity.name if entity else None,
                "group_id": group.id,
                "group_name": group.name,
                "is_global": entity_name_upper is None,
                "uc_sync_status": "not_applicable",
            })

        # ── Phase 2: Permission-based assignments (custom/personal groups) ─
        unique_users = list({m.user_id for m, _ in user_members})
        svc = RBACService(db)
        for uid in unique_users:
            effective = svc.get_effective_user_roles(uid)
            for perm in effective.inherited_permissions:
                key = (uid, perm.role_name, (perm.entity_name or "GLOBAL").upper())
                if key in seen:
                    continue
                seen.add(key)

                group_info = next(
                    (g for g in effective.rbac_groups if g.group_id == perm.group_id),
                    None,
                )
                all_assignments.append({
                    "user_id": uid,
                    "role_id": perm.role_id,
                    "role_name": perm.role_name,
                    "entity_id": perm.entity_id,
                    "entity_name": perm.entity_name,
                    "group_id": perm.group_id,
                    "group_name": group_info.group_name if group_info else None,
                    "is_global": perm.is_global_permission,
                    "uc_sync_status": "not_applicable",
                })

        paginated = all_assignments[offset: offset + limit]
        return paginated
    except Exception as e:
        logger.exception("Failed to get all assignments")
        raise HTTPException(status_code=500, detail="Failed to get all assignments")


# ---------------------------------------------------------------------------
# Assign / Unassign
# ---------------------------------------------------------------------------

@rbac_groups_router.post("/assign")
def assign_user(
    req: AssignRequest,
    check: dict = Depends(require_user_management),
    db: Session = Depends(get_db),
):
    """Assign a user to a role+entity combination."""
    try:
        actor = check.get("user_id", "")
        role = db.query(Role).filter(Role.id == req.role_id).first()
        if not role:
            raise HTTPException(status_code=404, detail="Role not found")

        existing_perm = (
            db.query(GroupPermission)
            .join(GroupMember, GroupMember.group_id == GroupPermission.group_id)
            .filter(
                GroupMember.user_id == req.user_id,
                GroupMember.member_type == MemberType.USER.value,
                GroupPermission.role_id == req.role_id,
                GroupPermission.entity_id == req.entity_id if req.entity_id else GroupPermission.entity_id.is_(None),
            )
            .first()
        )
        if existing_perm:
            return {"message": "User already has this assignment"}

        personal_group_name = f"_personal_{req.user_id}"
        group = db.query(RBACGroup).filter(RBACGroup.name == personal_group_name).first()
        if not group:
            group = RBACGroup(
                name=personal_group_name,
                description=f"Personal assignments for {req.user_id}",
                is_global=False,
                created_by=actor,
            )
            db.add(group)
            db.flush()

            member = GroupMember(
                user_id=req.user_id,
                group_id=group.id,
                created_by=actor,
                member_type=MemberType.USER.value,
            )
            db.add(member)

        perm = GroupPermission(
            group_id=group.id,
            role_id=req.role_id,
            entity_id=req.entity_id,
            created_by=actor,
        )
        db.add(perm)
        db.commit()
        return {"message": f"Assigned {role.name} to {req.user_id}"}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.exception("Failed to assign user")
        raise HTTPException(status_code=500, detail="Failed to assign user")


@rbac_groups_router.post("/unassign")
def unassign_user(
    req: UnassignRequest,
    check: dict = Depends(require_user_management),
    db: Session = Depends(get_db),
):
    """Remove a specific role+entity assignment from a user."""
    try:
        personal_group_name = f"_personal_{req.user_id}"
        group = db.query(RBACGroup).filter(RBACGroup.name == personal_group_name).first()
        if not group:
            raise HTTPException(status_code=404, detail="No personal assignments found for user")

        if req.entity_id:
            perm = (
                db.query(GroupPermission)
                .filter(
                    GroupPermission.group_id == group.id,
                    GroupPermission.role_id == req.role_id,
                    GroupPermission.entity_id == req.entity_id,
                )
                .first()
            )
        else:
            perm = (
                db.query(GroupPermission)
                .filter(
                    GroupPermission.group_id == group.id,
                    GroupPermission.role_id == req.role_id,
                    GroupPermission.entity_id.is_(None),
                )
                .first()
            )

        if not perm:
            raise HTTPException(status_code=404, detail="Assignment not found")

        db.delete(perm)
        db.commit()
        return {"message": "Assignment removed"}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.exception("Failed to unassign user")
        raise HTTPException(status_code=500, detail="Failed to unassign user")


# ---------------------------------------------------------------------------
# Onboard
# ---------------------------------------------------------------------------

@rbac_groups_router.post("/onboard", dependencies=[Depends(require_user_management)])
def onboard_admins(
    req: OnboardRequest,
    check: dict = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """Onboard users as global admins."""
    try:
        actor = check.get("user_id", "")
        admin_role = db.query(Role).filter(Role.name == "Admin").first()
        if not admin_role:
            raise HTTPException(status_code=500, detail="Admin role not found in DB")

        group_name = "Global Admins"
        group = db.query(RBACGroup).filter(RBACGroup.name == group_name).first()
        if not group:
            group = RBACGroup(
                name=group_name,
                description="Global admin access to all entities",
                is_global=True,
                created_by=actor,
            )
            db.add(group)
            db.flush()

            perm = GroupPermission(
                group_id=group.id,
                role_id=admin_role.id,
                entity_id=None,
                created_by=actor,
            )
            db.add(perm)

        added = []
        for uid in req.user_ids:
            existing = (
                db.query(GroupMember)
                .filter(
                    GroupMember.group_id == group.id,
                    GroupMember.user_id == uid,
                    GroupMember.member_type == MemberType.USER.value,
                )
                .first()
            )
            if not existing:
                member = GroupMember(
                    user_id=uid,
                    group_id=group.id,
                    created_by=actor,
                    member_type=MemberType.USER.value,
                )
                db.add(member)
                added.append(uid)

        db.commit()
        return {"message": f"Onboarded {len(added)} users as admins", "added": added}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.exception("Failed to onboard admins")
        raise HTTPException(status_code=500, detail="Failed to onboard admins")


# ---------------------------------------------------------------------------
# Access Preview
# ---------------------------------------------------------------------------

@rbac_groups_router.post("/access-preview")
def access_preview(
    req: AccessPreviewRequest,
    check: dict = Depends(require_user_management),
    db: Session = Depends(get_db),
):
    """Preview what access a user has on specified entities."""
    try:
        svc = RBACService(db)
        results = []
        for entity_id in req.entity_ids:
            read_check = svc.check_access(req.user_id, "read", entity_id=entity_id)
            write_check = svc.check_access(req.user_id, "create", entity_id=entity_id)
            results.append({
                "entity_id": entity_id,
                "can_read": read_check.get("allowed", False),
                "can_write": write_check.get("allowed", False),
                "roles": read_check.get("roles", []),
            })
        return results
    except Exception as e:
        logger.exception("Failed to preview access")
        raise HTTPException(status_code=500, detail="Failed to preview access")
