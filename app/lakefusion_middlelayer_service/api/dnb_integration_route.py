import os
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from lakefusion_utility.models.httpresponse import HttpResponse
from lakefusion_utility.utils.logging_utils import get_logger
from lakefusion_utility.utils.databricks_util import SecretScopeService, get_app_sp_token
from lakefusion_utility.utils.db_config_utility import DBConfigPropertiesService
from app.lakefusion_middlelayer_service.services.dnb_service import DnbService
from app.lakefusion_middlelayer_service.utils.app_db import token_required_wrapper, get_db

app_logger = get_logger(__name__)


def _get_scope_name(db) -> str:
    """Read secret_scope_name from db_config_properties, default to 'lakefusion'."""
    try:
        return DBConfigPropertiesService(db=db).getDBConfigProperties('secret_scope_name', required=False) or 'lakefusion'
    except Exception:
        return 'lakefusion'

dnb_router = APIRouter(
    tags=["DNB API"],
    prefix="/dnb"
)

# Request model for legacy secret operations
class SecretRequest(BaseModel):
    config_key: str
    key_value: str

# Request model for D&B OAuth2 credentials
class DnbCredentialsRequest(BaseModel):
    consumer_key: str
    consumer_secret: str


# Legacy endpoint — kept for backward compatibility
@dnb_router.post("/")
async def save_dnb_api_key(
    payload: SecretRequest,
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db)
):
    """
    Upserts a Dun & Bradstreet (DnB) API key into Databricks Secret Scope.
    Parameters:
    - payload: SecretRequest object containing config_key and key_value
    - check: Authentication token information from dependency injection
    """

    token = check.get("token")
    if not token:
        raise HTTPException(status_code=401, detail="Missing authentication token")

    # Perform the secret upsert operation
    try:
        secret_service = SecretScopeService(
            token=token,
            scope_name=_get_scope_name(db)
        )

        result = secret_service.upsert_secret(
            key=payload.config_key,
            value=payload.key_value
        )

        return HttpResponse(
            message=result.get("message", "Secret upserted successfully"),
            data={
                "scope": _get_scope_name(db),
                "key": payload.config_key
            }
        )

    except HTTPException:
        raise

    except Exception as e:
        app_logger.exception(f"[DNB_API] Failed to store secret: {e}")

        raise HTTPException(
            status_code=500,
            detail="Failed to store secret due to an internal error"
        )


@dnb_router.post("/credentials")
async def save_dnb_credentials(
    payload: DnbCredentialsRequest,
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db)
):
    """
    Saves D&B OAuth2 consumer key and secret into Databricks Secret Scope using DAPI token.
    Grants READ access to all workspace users on the scope.
    """
    token = get_app_sp_token()

    try:
        secret_service = SecretScopeService(
            token=token,
            scope_name=_get_scope_name(db)
        )

        secret_service.upsert_secret(key="dnb_consumer_key", value=payload.consumer_key)
        secret_service.upsert_secret(key="dnb_consumer_secret", value=payload.consumer_secret)
        secret_service.grant_read_acl()

        return HttpResponse(
            message="D&B credentials saved successfully",
            data={
                "scope": _get_scope_name(db),
                "keys": ["dnb_consumer_key", "dnb_consumer_secret"]
            }
        )

    except HTTPException:
        raise

    except Exception as e:
        app_logger.exception(f"[DNB_API] Failed to store D&B credentials: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to store D&B credentials due to an internal error"
        )


@dnb_router.post("/test-connection")
async def test_dnb_connection(
    payload: DnbCredentialsRequest,
    check: dict = Depends(token_required_wrapper)
):
    """
    Validates D&B OAuth2 credentials by requesting a token from the D&B token endpoint.
    """
    try:
        result = DnbService.validate_credentials(payload.consumer_key, payload.consumer_secret)
        return HttpResponse(
            message=result["message"],
            data={"connected": result["connected"]}
        )

    except Exception as e:
        app_logger.exception(f"[DNB_API] Test connection failed: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to test D&B connection"
        )
