from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from app.lakefusion_auth_service.utils.app_db import get_db
from app.lakefusion_auth_service.services.auth_service import get_oidc_url, generate_auth_token, refresh_auth_token, generate_m2m_auth_token
from lakefusion_utility.utils.logging_utils import get_logger
from lakefusion_utility.models.auth import AuthUrlResponse, AuthTokenResponse
from lakefusion_utility.models.auth_request import AuthTokenRequest, RefreshTokenRequest, AuthTokenRequestM2M


auth_router = APIRouter(tags=["Authentication API"])
app_logger = get_logger(__name__)


@auth_router.get("/get-oidc-url", response_model=AuthUrlResponse)
async def get_oidc_url_api(
    redirect_uri: Optional[str] = Query(
        None,
        description="Custom redirect URI for OAuth callback. Must be in the allowed whitelist."
    ),
    db: Session = Depends(get_db)
):
    """
    Endpoint to get the OIDC authorization URL.

    Args:
        redirect_uri (Optional[str]): Custom redirect URI for OAuth callback.
                                       If not provided, uses the default portal URL.
        db (Session): Database session for querying.

    Returns:
        AuthUrlResponse: Response containing the OIDC authorization URL.
    """
    return get_oidc_url(db, redirect_uri=redirect_uri)

@auth_router.post("/generate-auth-token", response_model=AuthTokenResponse)
async def generate_auth_token_api(request: AuthTokenRequest, db: Session = Depends(get_db)):
    """
    Endpoint to generate an auth token using the provided authorization code and state.

    Args:
        request (AuthTokenRequest): The request containing the authorization code, state,
                                    and optional redirect_uri.
        db (Session): Database session for querying.

    Returns:
        AuthTokenResponse: Response containing the generated auth token.
    """
    return generate_auth_token(request.code, request.state, db, redirect_uri=request.redirect_uri)

@auth_router.post("/generate-auth-token-m2m", response_model=AuthTokenResponse)
async def generate_auth_token_m2m_api(request: AuthTokenRequestM2M):
    """
    Endpoint to generate a machine-to-machine (M2M) auth token using client credentials.
    
    Args:
        request (AuthTokenRequest): The request containing the client ID and secret.
        db (Session): Database session for querying.

    Returns:
        AuthTokenResponse: Response containing the generated auth token.
    """
    return generate_m2m_auth_token(request.client_id, request.client_secret)

@auth_router.post("/refresh-auth-token", response_model=AuthTokenResponse)
async def refresh_auth_token_api(request: RefreshTokenRequest, db: Session = Depends(get_db)):
    """
    Endpoint to refresh the auth token using the provided refresh token.
    
    Args:
        request (RefreshTokenRequest): The request containing the refresh token.
        db (Session): Database session for querying.

    Returns:
        AuthTokenResponse: Response containing the refreshed auth token.
    """
    return refresh_auth_token(request.refresh_token)
