import os
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from lakefusion_utility.models.httpresponse import HttpResponse
from lakefusion_utility.utils.logging_utils import get_logger
from lakefusion_utility.utils.databricks_util import SecretScopeService
from app.lakefusion_middlelayer_service.utils.app_db import token_required_wrapper

app_logger = get_logger(__name__)

SCOPE_NAME = "lakefusion"
DATABRICKS_HOST = os.environ.get('DATABRICKS_HOST', 'https://databricks.com')

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
    check: dict = Depends(token_required_wrapper)
):
    """
    Saves Service Principal OAuth credentials into Databricks Secret Scope using DAPI token.
    These credentials are used for all workflow OAuth flows including Vector Search optimized route.
    Grants READ access to all workspace users on the scope.
    """
    dapi_token = os.environ.get("LAKEFUSION_DATABRICKS_DAPI")
    if not dapi_token:
        app_logger.error("[SPN_API] LAKEFUSION_DATABRICKS_DAPI environment variable not set")
        raise HTTPException(status_code=500, detail="Server configuration error: DAPI token not available")

    try:
        secret_service = SecretScopeService(
            token=dapi_token,
            scope_name=SCOPE_NAME
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
