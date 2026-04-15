from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import Optional
import requests
import os
from lakefusion_utility.models.oidc_request import OIDCRequest
from app.lakefusion_auth_service.utils.auth_utils import generate_code_verifier_challenge
from lakefusion_utility.utils.logging_utils import get_logger
from lakefusion_utility.models.auth import AuthUrlResponse, AuthTokenResponse
from pydantic import ValidationError
import jwt

app_logger = get_logger(__name__)

# Environment variables for Databricks configuration
databricks_host = os.environ.get("DATABRICKS_HOST", "https://adb-490026345444040.0.azuredatabricks.net")
portal_url = os.environ.get("REDIRECT_URI", "http://localhost:3000")
databricks_oidc_client_id = os.environ.get("DATABRICKS_OIDC_CLIENT_ID", "")
databricks_oidc_client_secret = os.environ.get("DATABRICKS_OIDC_CLIENT_SECRET", "")

# Auth provider switch: "databricks" (default) or "azuread"
AUTH_PROVIDER = os.environ.get("AUTH_PROVIDER", "databricks")
AZURE_TENANT_ID = os.environ.get("AZURE_TENANT_ID", "")
AZURE_AD_CLIENT_ID = os.environ.get("AZURE_AD_CLIENT_ID", "")
AZURE_AD_CLIENT_SECRET = os.environ.get("AZURE_AD_CLIENT_SECRET", "")
DATABRICKS_RESOURCE_ID = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"

def get_oidc_url(db: Session, redirect_uri: Optional[str]=None) -> AuthUrlResponse:
    """
    Generate the OIDC authorization URL and store the code verifier and challenge in the database.

    Args:
        db (Session): Database session for querying.
        redirect_uri (Optional[str]): Custom redirect URI for OAuth callback.
                                       If not provided, uses default portal URL.

    Returns:
        AuthUrlResponse: A response containing the authorization URL.

    Raises:
        HTTPException: If an error occurs while generating the URL or if redirect_uri is invalid.
    """
    try:
        # Generate code verifier and challenge for OIDC
        code_verifier, code_challenge = generate_code_verifier_challenge()

        # Store the OIDC request in the database
        oidc_request = OIDCRequest(code_verifier=code_verifier, code_challenge=code_challenge)
        db.add(oidc_request)
        db.commit()

        if not redirect_uri:
            redirect_uri = portal_url

        # Construct the authorization URL based on provider
        if AUTH_PROVIDER == "azuread":
            client_id = AZURE_AD_CLIENT_ID
            auth_url = (
                f"https://login.microsoftonline.com/{AZURE_TENANT_ID}/oauth2/v2.0/authorize"
                f"?client_id={client_id}"
                f"&redirect_uri={redirect_uri}/auth/callback"
                f"&response_type=code"
                f"&state={oidc_request.id}"
                f"&code_challenge={code_challenge}"
                f"&code_challenge_method=S256"
                f"&scope={DATABRICKS_RESOURCE_ID}/user_impersonation+offline_access+openid+profile"
            )
        else:
            client_id = databricks_oidc_client_id
            auth_url = (
                f"{databricks_host}/oidc/v1/authorize"
                f"?client_id={client_id}"
                f"&redirect_uri={redirect_uri}/auth/callback"
                f"&response_type=code"
                f"&state={oidc_request.id}"
                f"&code_challenge={code_challenge}"
                f"&code_challenge_method=S256"
                f"&scope=all-apis+offline_access"
            )

        return {"auth_url": auth_url}
    
    except Exception as e:
        app_logger.exception(f"Error getting OIDC URL: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to generate OIDC URL")

def _fetch_auth_token(data: dict) -> AuthTokenResponse:
    """
    Helper function to make a POST request to fetch an auth token.

    Args:
        data (dict): The data required for the token request.

    Returns:
        AuthTokenResponse: A response containing the access token and other details.

    Raises:
        HTTPException: If an error occurs while fetching the token.
    """
    if AUTH_PROVIDER == "azuread":
        token_url = f"https://login.microsoftonline.com/{AZURE_TENANT_ID}/oauth2/v2.0/token"
    else:
        token_url = f"{databricks_host}/oidc/v1/token"
    response = requests.post(token_url, data=data)

    if response.status_code != 200:
        app_logger.exception(f"Error fetching auth token: {response.json()}")
        raise HTTPException(status_code=response.status_code, detail=response.json())

    response_data = response.json()

    try:
        # Decode the access token to get the username
        access_token = response_data['access_token']
        username = ''
        try:
            unverified_claims = jwt.decode(access_token, options={"verify_signature": False})
            if AUTH_PROVIDER == "azuread":
                username = unverified_claims.get('upn', unverified_claims.get('unique_name', unverified_claims.get('sub', '')))
            else:
                username = unverified_claims.get('sub', '')
        except jwt.DecodeError:
            pass

        return AuthTokenResponse(
            access_token=access_token,
            token_type=response_data.get("token_type", "Bearer"),
            expires_in=response_data.get("expires_in", 3600),
            refresh_token=response_data.get("refresh_token", ""),
            scope=response_data.get("scope", ""),
            username=username,
        )
    except ValidationError as e:
        raise HTTPException(status_code=500, detail=f"Invalid auth token data: {str(e)}")

def generate_auth_token(code: str, state: str, db: Session, redirect_uri: Optional[str] = None) -> AuthTokenResponse:
    """
    Generate an auth token using the authorization code and state.

    Args:
        code (str): The authorization code received from the OIDC provider.
        state (str): The state parameter to validate the request.
        db (Session): Database session for querying.

    Returns:
        AuthTokenResponse: A response containing the access token and other details.

    Raises:
        HTTPException: If an error occurs during the token generation process.
    """
    try:
        # Validate the state by checking the stored OIDC request
        oidc_request = db.query(OIDCRequest).filter(OIDCRequest.id == state).first()
        if not oidc_request:
            raise HTTPException(status_code=400, detail="Invalid state")

        if not redirect_uri:
            redirect_uri = portal_url

        # Generate auth token data with provider-specific credentials and scopes
        if AUTH_PROVIDER == "azuread":
            client_id = AZURE_AD_CLIENT_ID
            client_secret = AZURE_AD_CLIENT_SECRET
            scope = f"{DATABRICKS_RESOURCE_ID}/user_impersonation offline_access openid profile"
        else:
            client_id = databricks_oidc_client_id
            client_secret = databricks_oidc_client_secret
            scope = "all-apis offline_access"

        data = {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "authorization_code",
            "scope": scope,
            "redirect_uri": f"{redirect_uri}/auth/callback",
            "code_verifier": oidc_request.code_verifier,
            "code": code
        }

        return _fetch_auth_token(data)
    except Exception as e:
        app_logger.exception(f"Error generating auth token: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to create Auth token. Error {str(e)}")

def refresh_auth_token(refresh_token: str) -> AuthTokenResponse:
    """
    Refresh the auth token using the refresh token.

    Args:
        refresh_token (str): The refresh token obtained from the initial auth token response.

    Returns:
        AuthTokenResponse: A response containing the new access token and other details.

    Raises:
        HTTPException: If an error occurs during the token refresh process.
    """
    try:
        # Prepare the data for the token refresh request
        if AUTH_PROVIDER == "azuread":
            client_id = AZURE_AD_CLIENT_ID
            client_secret = AZURE_AD_CLIENT_SECRET
        else:
            client_id = databricks_oidc_client_id
            client_secret = databricks_oidc_client_secret

        data = {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token
        }

        return _fetch_auth_token(data)
    except Exception as e:
        app_logger.exception(f"Error refreshing auth token: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to refresh Auth token. Error {str(e)}")
    
def generate_m2m_auth_token(client_id: str, client_secret: str) -> AuthTokenResponse:
    """
    Generate a machine-to-machine (M2M) auth token using client credentials of the service principal.

    Args:
        client_id (str): The client ID of the service principal.
        client_secret (str): The client secret of the service principal.

    Returns:
        AuthTokenResponse: A response containing the access token and other details.

    Raises:
        HTTPException: If an error occurs during the token generation process.
    """
    try:
        if AUTH_PROVIDER == "azuread":
            scope = f"{DATABRICKS_RESOURCE_ID}/.default"
        else:
            scope = "all-apis"

        data = {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
            "scope": scope
        }

        return _fetch_auth_token(data)
    except Exception as e:
        app_logger.exception(f"Error generating M2M auth token: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to create M2M Auth token. Error {str(e)}")
