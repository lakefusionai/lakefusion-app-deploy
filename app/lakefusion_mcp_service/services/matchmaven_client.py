"""
MatchMaven Client for MCP Service.
"""
import httpx
from typing import Dict, Any, Optional
import asyncio

from app.lakefusion_mcp_service.utils.logger import get_logger, log_to_stderr
from app.lakefusion_mcp_service.utils.errors import APIError, AuthRequiredError
from app.lakefusion_mcp_service.config import MATCHMAVEN_SERVICE_URL

logger = get_logger(__name__)

# Retry configuration
MAX_RETRIES = 3
RETRY_STATUS_CODES = {429, 502, 503, 504}
RETRY_DELAY_SECONDS = 1


class MatchMavenClient:
    """Client for interacting with Match-Maven service."""

    def __init__(self):
        self.base_url = MATCHMAVEN_SERVICE_URL
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
    ) -> Dict[str, Any]:
        """Make HTTP request with retry logic."""
        # Strip trailing slash from base_url and leading slash from endpoint to avoid double slashes
        base = self.base_url.rstrip('/')
        endpoint = endpoint.lstrip('/')
        url = f"{base}/{endpoint}"
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
                        await asyncio.sleep(RETRY_DELAY_SECONDS * (attempt + 1))  # Exponential backoff
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

    async def search_entity(
        self,
        entity_id: int,
        attributes: Dict[str, Any],
        warehouse_id: str,
        token: str,
        top_n: int = 3,
    ) -> Dict[str, Any]:
        """Search for similar entities in the master table."""
        log_to_stderr(f"[MATCHMAVEN] Searching entity {entity_id} (top_n={top_n})")
        
        response = await self._make_request(
            "POST",
            f"match-search/{entity_id}/search",
            token=token,
            data={"attributes": attributes},
            params={"warehouse_id": warehouse_id, "top_n": top_n},
        )
        
        result_count = len(response.get("results", []))
        log_to_stderr(f"[MATCHMAVEN] Search complete - {result_count} results")
        return response

    async def match_entity(
        self,
        entity_id: int,
        attributes: Dict[str, Any],
        warehouse_id: str,
        token: str,
    ) -> Dict[str, Any]:
        """Match an entity against the master table."""
        log_to_stderr(f"[MATCHMAVEN] Matching entity {entity_id}")
        
        response = await self._make_request(
            "POST",
            f"match-search/{entity_id}/match",
            token=token,
            data={"attributes": attributes},
            params={"warehouse_id": warehouse_id},
        )
        
        log_to_stderr(f"[MATCHMAVEN] Match complete - {response.get('match_status')}")
        return response

    async def sync_vector_index(self, entity_id: int, token: str) -> Dict[str, Any]:
        """Sync the vector search index for an entity."""
        log_to_stderr(f"[MATCHMAVEN] Syncing vector index for entity {entity_id}")
        
        response = await self._make_request(
            "POST",
            f"match-search/{entity_id}/sync-index",
            token=token,
        )
        
        log_to_stderr(f"[MATCHMAVEN] Sync complete - {response.get('sync_status')}")
        return response


# Singleton client instance
_matchmaven_client: Optional[MatchMavenClient] = None


def get_matchmaven_client() -> MatchMavenClient:
    """Get or create the MatchMaven client instance."""
    global _matchmaven_client
    if _matchmaven_client is None:
        _matchmaven_client = MatchMavenClient()
    return _matchmaven_client