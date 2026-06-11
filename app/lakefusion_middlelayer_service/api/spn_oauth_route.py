import os
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from lakefusion_utility.models.httpresponse import HttpResponse
from lakefusion_utility.utils.logging_utils import get_logger
from lakefusion_utility.utils.databricks_util import SecretScopeService, get_app_sp_token
from lakefusion_utility.utils.db_config_utility import DBConfigPropertiesService
from app.lakefusion_middlelayer_service.utils.app_db import token_required_wrapper, get_db

app_logger = get_logger(__name__)

def _get_scope_name(db) -> str:
    """Read secret_scope_name from db_config_properties, default to 'lakefusion'."""
    try:
        return DBConfigPropertiesService(db=db).getDBConfigProperties('secret_scope_name', required=False) or 'lakefusion'
    except Exception:
        return 'lakefusion'
# Use the shared helper so the https:// prefix is always present.
from lakefusion_utility.utils.databricks_host import get_databricks_host
DATABRICKS_HOST = get_databricks_host() or "https://databricks.com"

spn_router = APIRouter(
    tags=["SPN OAuth API"],
    prefix="/spn"
)

class SpnCredentialsRequest(BaseModel):
    client_id: str
    client_secret: str


@spn_router.post("/credentials")
async def save_spn_credentials(
    payload: SpnCredentialsRequest,
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db)
):
    """
    Saves Service Principal OAuth credentials into Databricks Secret Scope.
    These credentials are used for all workflow OAuth flows including Vector Search optimized route.
    Grants READ access to all workspace users on the scope.
    """
    token = get_app_sp_token()

    try:
        scope_name = _get_scope_name(db)
        secret_service = SecretScopeService(
            token=token,
            scope_name=scope_name
        )

        secret_service.upsert_secret(key="lakefusion_spn", value=payload.client_id)
        secret_service.upsert_secret(key="lakefusion_spn_secret", value=payload.client_secret)
        secret_service.grant_read_acl(principal="users")

        return HttpResponse(
            message="Service Principal credentials saved successfully",
            data={
                "scope": SCOPE_NAME,
                "keys": ["lakefusion_spn", "lakefusion_spn_secret"]
            }
        )

    except HTTPException:
        raise

    except Exception as e:
        app_logger.exception(f"[SPN_API] Failed to store SPN credentials: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to store Service Principal credentials due to an internal error"
        )


@spn_router.post("/test-connection")
async def test_spn_connection(
    payload: SpnCredentialsRequest,
    check: dict = Depends(token_required_wrapper)
):
    """
    Validates Service Principal credentials by attempting to authenticate with the
    Databricks workspace using OAuth M2M flow.
    """
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient(
            host=DATABRICKS_HOST,
            client_id=payload.client_id,
            client_secret=payload.client_secret
        )

        current_user = w.current_user.me()
        spn_display = current_user.display_name or current_user.user_name or payload.client_id

        return HttpResponse(
            message=f"Successfully authenticated as {spn_display}",
            data={"connected": True, "display_name": spn_display}
        )

    except Exception as e:
        app_logger.warning(f"[SPN_API] Test connection failed: {e}")
        return HttpResponse(
            message=f"Authentication failed: {str(e)}",
            data={"connected": False}
        )
