"""
Middle Layer Client for MCP Service.
"""
import httpx
from typing import Dict, Any, Optional, List
import asyncio

from app.lakefusion_mcp_service.utils.logger import get_logger, log_to_stderr
from app.lakefusion_mcp_service.utils.errors import APIError, AuthRequiredError
from app.lakefusion_mcp_service.config import MIDDLELAYER_SERVICE_URL

logger = get_logger(__name__)

# Retry configuration
MAX_RETRIES = 3
RETRY_STATUS_CODES = {429, 502, 503, 504}
RETRY_DELAY_SECONDS = 1


class MiddleLayerClient:
    """Client for interacting with Middle Layer service."""

    def __init__(self):
        self.base_url = MIDDLELAYER_SERVICE_URL
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create reusable HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(60.0, connect=10.0),
                limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
            )
        return self._client

    async def close(self):
        """Close the HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    def _get_headers(self, token: str) -> Dict[str, str]:
        """Get HTTP headers with the provided authentication token."""
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    async def _make_request(
        self,
        method: str,
        endpoint: str,
        token: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None,
    ) -> Any:
        """Make HTTP request with retry logic."""
        # Strip trailing slash from base_url and leading slash from endpoint to avoid double slashes
        base = self.base_url.rstrip('/')
        endpoint_clean = endpoint.lstrip('/')
        url = f"{base}/{endpoint_clean}"
        logger.debug(f"Making {method} request to: {url}")
        headers = self._get_headers(token)
        client = await self._get_client()

        last_exception = None

        for attempt in range(MAX_RETRIES):
            try:
                if method.upper() == "GET":
                    response = await client.get(url, headers=headers, params=params)
                elif method.upper() == "POST":
                    response = await client.post(url, headers=headers, json=data, params=params)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")

                # Handle auth errors
                if response.status_code == 401:
                    logger.warning("Authentication failed - token may be expired")
                    raise AuthRequiredError()

                # Retry on specific status codes
                if response.status_code in RETRY_STATUS_CODES:
                    logger.warning(f"Retryable error {response.status_code}, attempt {attempt + 1}/{MAX_RETRIES}")
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(RETRY_DELAY_SECONDS * (attempt + 1))
                        continue
                    response.raise_for_status()

                response.raise_for_status()
                return response.json()

            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error: {e.response.status_code} - {e.response.text}")
                last_exception = APIError(
                    operation="API request",
                    details=f"{e.response.status_code} - {e.response.text}"
                )

            except httpx.RequestError as e:
                logger.error(f"Request failed: {str(e)}")
                last_exception = APIError(
                    operation="API request",
                    details=f"Connection error: {str(e)}"
                )

                # Retry on connection errors
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_DELAY_SECONDS * (attempt + 1))
                    continue

        raise last_exception or APIError(operation="API request", details="Unknown error")

    async def get_entities(
        self,
        token: str,
        is_active: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Get list of entities from Integration Hub.

        Args:
            token: Authentication token
            is_active: Filter by active status (default: True)

        Returns:
            List of integration hub entities with their configurations
        """
        log_to_stderr(f"[MIDDLELAYER] Fetching entities (is_active={is_active})")

        response = await self._make_request(
            "GET",
            "integration-hub/",
            token=token,
            params={"is_active": is_active},
        )

        result_count = len(response) if isinstance(response, list) else 0
        log_to_stderr(f"[MIDDLELAYER] Fetched {result_count} entities")
        return response


# Singleton client instance
_middlelayer_client: Optional[MiddleLayerClient] = None


def get_middlelayer_client() -> MiddleLayerClient:
    """Get or create the MiddleLayer client instance."""
    global _middlelayer_client
    if _middlelayer_client is None:
        _middlelayer_client = MiddleLayerClient()
    return _middlelayer_client
