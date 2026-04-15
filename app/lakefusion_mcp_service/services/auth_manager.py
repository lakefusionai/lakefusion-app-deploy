"""
Authentication Manager for MCP Service.
Handles user OIDC authentication flow.
"""
from typing import Dict, Any
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, parse_qs

from app.lakefusion_mcp_service.utils.logger import get_logger, log_to_stderr
from app.lakefusion_mcp_service.config import AUTH_SERVICE_URL, REDIRECT_URI
from app.lakefusion_mcp_service.services.session_manager import session_manager, AuthState

logger = get_logger(__name__)


class AuthManager:
    """
    Manages user authentication for a specific session.

    Handles the complete OIDC flow:
    1. Initiate login -> returns auth URL for user
    2. Complete login -> exchanges code for tokens
    3. Token refresh -> automatic refresh before expiry
    """

    def __init__(self, session_id: str):
        self.session_id = session_id
        self.auth_state = session_manager.get_or_create_session(session_id)
        self.http_client = session_manager.get_or_create_client(session_id)

    async def initiate_oidc_login(self) -> Dict[str, Any]:
        """
        Initiate OIDC login flow.

        Requests the auth URL from the auth service, extracts the state parameter,
        and maps it to this session for callback handling.

        Returns:
            Dict containing success status, auth_url, and instructions.
        """
        log_to_stderr(f"[AUTH] Initiating OIDC login for session {self.session_id[:8]}...")
        logger.info(f"Initiating OIDC login for session {self.session_id[:8]}...")

        # Build the redirect URI for the MCP callback
        redirect_uri = f"{REDIRECT_URI}"

        try:
            response = await self.http_client.get(
                f"{AUTH_SERVICE_URL}/get-oidc-url",
                params={"redirect_uri": redirect_uri}
            )
            response.raise_for_status()
            data = response.json()

            auth_url = data.get("auth_url")
            if not auth_url:
                raise Exception("Auth service did not return auth_url")

            # Extract the state parameter from auth_url and map it to this session
            # The state is the oidc_request.id from auth service
            parsed_url = urlparse(auth_url)
            query_params = parse_qs(parsed_url.query)
            state = query_params.get("state", [None])[0]

            if state:
                session_manager.map_state_to_session(state, self.session_id)
                logger.info(f"Mapped OAuth state {state[:8]}... to session {self.session_id[:8]}...")
            else:
                logger.warning("No state parameter found in auth_url")

            # Update auth state
            self.auth_state.auth_url = auth_url
            self.auth_state.pending_auth = True

            log_to_stderr(f"[AUTH] Login URL generated - waiting for user to authenticate")
            logger.info(f"OIDC login initiated for session {self.session_id[:8]}...")

            return {
                "success": True,
                "auth_url": auth_url,
                "message": "Please click the URL to authenticate with SSO.",
                "instructions": [
                    "1. Click the authentication URL below",
                    "2. Login with your SSO credentials",
                    "3. You will be redirected back after successful authentication"
                ],
                "polling": {
                    "enabled": True,
                    "tool": "auth_status",
                    "interval_seconds": 10,
                    "timeout_seconds": 180,
                    "instruction": (
                        "IMPORTANT: After presenting the auth URL to the user, you MUST "
                        "automatically call 'auth_status' every 10 seconds to check if "
                        "authentication completed. Continue polling until you see "
                        "'authenticated: true' or 3 minutes have passed. Do not wait for "
                        "the user to tell you they logged in - poll automatically."
                    )
                }
            }

        except Exception as e:
            log_to_stderr(f"[AUTH] Failed to initiate login: {str(e)}", "ERROR")
            logger.error(f"Failed to initiate OIDC login: {str(e)}")
            raise Exception(f"Failed to initiate OIDC login: {str(e)}")

    async def complete_oidc_login(self, code: str, state: str) -> Dict[str, Any]:
        """
        Complete OIDC login by exchanging authorization code for tokens.

        Args:
            code: Authorization code from the OAuth callback
            state: State parameter for validation

        Returns:
            Dict containing success status and username.
        """
        log_to_stderr(f"[AUTH] Completing OIDC login - exchanging code for tokens...")
        logger.info(f"Completing OIDC login for session {self.session_id[:8]}...")
        redirect_uri = f"{REDIRECT_URI}"

        try:
            response = await self.http_client.post(
                f"{AUTH_SERVICE_URL}/generate-auth-token",
                json={"code": code, "state": state, "redirect_uri": redirect_uri}
            )
            response.raise_for_status()
            data = response.json()

            # Update auth state with tokens
            self.auth_state.access_token = data["access_token"]
            self.auth_state.refresh_token = data.get("refresh_token")
            self.auth_state.username = data.get("username")
            self.auth_state.token_expiry = datetime.now(timezone.utc) + timedelta(
                seconds=data.get("expires_in", 3600)
            )
            self.auth_state.pending_auth = False
            self.auth_state.auth_url = None

            log_to_stderr(f"[AUTH] Login successful! User: {self.auth_state.username}")
            logger.info(
                f"OIDC login completed for session {self.session_id[:8]}..., "
                f"user: {self.auth_state.username}"
            )

            return {
                "success": True,
                "message": f"Successfully authenticated as {self.auth_state.username}",
                "username": self.auth_state.username,
                "token_expires_at": self.auth_state.token_expiry.isoformat()
            }

        except Exception as e:
            self.auth_state.pending_auth = False
            log_to_stderr(f"[AUTH] Login completion failed: {str(e)}", "ERROR")
            logger.error(f"OIDC login completion failed: {str(e)}")
            raise Exception(f"OIDC login completion failed: {str(e)}")

    async def refresh_token(self) -> str:
        """
        Refresh the access token using the refresh token.

        Returns:
            The new access token.

        Raises:
            Exception if refresh fails or no refresh token is available.
        """
        if not self.auth_state.refresh_token:
            raise Exception("No refresh token available. Please login again.")

        log_to_stderr(f"[AUTH] Refreshing access token...")
        logger.info(f"Refreshing token for session {self.session_id[:8]}...")

        try:
            response = await self.http_client.post(
                f"{AUTH_SERVICE_URL}/refresh-auth-token",
                json={"refresh_token": self.auth_state.refresh_token}
            )
            response.raise_for_status()
            data = response.json()

            # Update auth state with new tokens
            self.auth_state.access_token = data["access_token"]
            self.auth_state.token_expiry = datetime.now(timezone.utc) + timedelta(
    seconds=data.get("expires_in", 3600)
)

            # Update refresh token if a new one was provided
            if "refresh_token" in data:
                self.auth_state.refresh_token = data["refresh_token"]

            log_to_stderr(f"[AUTH] Token refreshed successfully")
            logger.info(f"Token refreshed for session {self.session_id[:8]}...")
            return self.auth_state.access_token

        except Exception as e:
            log_to_stderr(f"[AUTH] Token refresh failed: {str(e)}", "ERROR")
            logger.error(f"Token refresh failed: {str(e)}")
            # Clear auth state on refresh failure
            self.clear_auth()
            raise Exception(f"Token refresh failed. Please login again: {str(e)}")

    async def get_valid_token(self) -> str:
        """
        Get a valid access token, refreshing if necessary.

        Returns:
            A valid access token.

        Raises:
            Exception if not authenticated or token refresh fails.
        """
        if not self.auth_state.access_token:
            if self.auth_state.pending_auth:
                raise Exception(
                    "Authentication in progress. Please complete the login flow "
                    "by clicking the auth URL."
                )
            raise Exception(
                "Not authenticated. Please use the 'login' tool first to authenticate."
            )

        # Check if token needs refresh (5 minute buffer before expiry)
        if self.auth_state.token_expiry:
            buffer = timedelta(minutes=5)
            if datetime.now(timezone.utc) >= self.auth_state.token_expiry - buffer:
                logger.info("Token expiring soon, refreshing...")
                return await self.refresh_token()

        return self.auth_state.access_token

    def get_auth_status(self) -> Dict[str, Any]:
        """
        Get the current authentication status for this session.

        Returns:
            Dict containing authentication status and details.
        """
        if self.auth_state.pending_auth:
            return {
                "authenticated": False,
                "status": "pending",
                "message": "Authentication in progress. Please complete the login flow.",
                "auth_url": self.auth_state.auth_url
            }

        if not self.auth_state.access_token:
            return {
                "authenticated": False,
                "status": "not_authenticated",
                "message": "Not authenticated. Please use the 'login' tool to authenticate."
            }

        return {
            "authenticated": True,
            "status": "authenticated",
            "username": self.auth_state.username,
            "token_expires_at": (
                self.auth_state.token_expiry.isoformat()
                if self.auth_state.token_expiry else None
            )
        }

    def clear_auth(self):
        """Clear authentication state (logout)."""
        self.auth_state.access_token = None
        self.auth_state.refresh_token = None
        self.auth_state.token_expiry = None
        self.auth_state.username = None
        self.auth_state.auth_url = None
        self.auth_state.pending_auth = False
        log_to_stderr(f"[AUTH] User logged out - session cleared")
        logger.info(f"Auth state cleared for session {self.session_id[:8]}...")


def get_auth_manager(session_id: str) -> AuthManager:
    """Create an AuthManager for the given session."""
    return AuthManager(session_id)
