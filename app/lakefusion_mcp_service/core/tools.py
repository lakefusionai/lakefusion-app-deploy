"""
MCP Tools for MatchMaven entity matching and search.
Includes authentication tools for Databricks SSO login.
"""
from typing import Dict, Any
from fastmcp import FastMCP, Context
from app.lakefusion_mcp_service.services.session_manager import sse_notifications
from app.lakefusion_mcp_service.utils.logger import get_logger
from app.lakefusion_mcp_service.services.matchmaven_client import get_matchmaven_client
from app.lakefusion_mcp_service.services.middlelayer_client import get_middlelayer_client
from app.lakefusion_mcp_service.services.auth_manager import AuthManager
from app.lakefusion_mcp_service.models.requests import SearchEntityRequest, MatchEntityRequest, GetEntitiesRequest
from pydantic import ValidationError
from app.lakefusion_mcp_service.models.responses import SearchResponse, MatchResponse
from app.lakefusion_mcp_service.utils.errors import format_error_response
from app.lakefusion_mcp_service.utils.responses import (
    validate_response,
    add_profile_urls_to_search_results,
    add_profile_url_to_match_result,
)

logger = get_logger(__name__)


def _get_session_id(ctx: Context) -> str:
    """
    Extract session ID from the MCP context.
    Falls back to request_id if session_id is not available.
    """
    session_id = ctx.session_id or ctx.request_id
    if not session_id:
        raise ValueError("No session identifier available")
    return session_id


async def _check_auth(ctx: Context) -> tuple[bool, Dict[str, Any] | None, str | None]:
    """
    Check authentication status and return token if authenticated.

    Returns:
        Tuple of (is_authenticated, error_response, token)
    """
    session_id = _get_session_id(ctx)
    auth_manager = AuthManager(session_id)
    status = auth_manager.get_auth_status()

    if not status.get("authenticated"):
        return False, {
            "error": "Authentication required",
            "message": "Please use the 'login' tool first to authenticate.",
            "status": status.get("status", "not_authenticated")
        }, None

    # get_valid_token is async and handles token refresh if needed
    token = await auth_manager.get_valid_token()
    return True, None, token




def load_tools(mcp: FastMCP):
    """Register all tools with the MCP server."""

    # =========================================================================
    # Authentication Tools
    # =========================================================================

    @mcp.tool()
    async def login(ctx: Context) -> Dict[str, Any]:
        """
        Login with Databricks SSO to authenticate for API access.

        This tool initiates the OAuth authentication flow with Databricks.
        You will receive a URL that must be opened in a browser to complete
        the login process. After successful authentication, you can use
        the search_entity and match_entity tools.

        Returns:
            Authentication response containing:
            - success: Whether the login initiation was successful
            - auth_url: URL to open in browser for authentication
            - message: Instructions for completing login
            - instructions: Step-by-step guide for authentication

        Example:
            login()
        """
        logger.info("MCP Tool called: login()")

        try:
            session_id = _get_session_id(ctx)
            sse_notifications.register_mcp_session(session_id, ctx.session)
            auth_manager = AuthManager(session_id)
            result = await auth_manager.initiate_oidc_login()
            logger.info(f"Login initiated for session {session_id[:8]}...")
            return result
        except Exception as e:
            logger.error(f"Login failed: {e}")
            return format_error_response(e)

    @mcp.tool()
    def auth_status(ctx: Context) -> Dict[str, Any]:
        """
        Check the current authentication status.

        Use this tool to verify if you are currently authenticated and see
        details about your session, including username and token expiry.

        Returns:
            Authentication status containing:
            - authenticated: Whether the user is currently authenticated
            - status: One of "authenticated", "pending", or "not_authenticated"
            - username: The authenticated user's username (if authenticated)
            - token_expires_at: When the current token will expire (if authenticated)
            - auth_url: URL to complete authentication (if status is "pending")
            - message: Human-readable status message

        Example:
            auth_status()
        """
        logger.info("MCP Tool called: auth_status()")

        try:
            session_id = _get_session_id(ctx)
            auth_manager = AuthManager(session_id)
            status = auth_manager.get_auth_status()
            logger.info(f"Auth status retrieved for session {session_id[:8]}...")

            # Add polling hints to guide Claude on next action
            if status.get("status") == "pending":
                status["polling_hint"] = (
                    "Authentication still pending. The user has not completed login yet. "
                    "Call auth_status again in 10 seconds to check again."
                )
            elif status.get("authenticated"):
                status["polling_hint"] = (
                    "Authentication complete! Stop polling. You can now use search_entity, "
                    "match_entity, get_entities and other authenticated tools."
                )
            else:
                status["polling_hint"] = (
                    "Not authenticated. Use the 'login' tool to start authentication."
                )

            return status

        except Exception as e:
            logger.error(f"Auth status failed: {e}")
            return format_error_response(e)

    @mcp.tool()
    def logout(ctx: Context) -> Dict[str, str]:
        """
        Logout and clear the current authentication session.

        This tool clears all stored tokens and authentication state.
        You will need to use the 'login' tool again to re-authenticate.

        Returns:
            Status message confirming logout

        Example:
            logout()
        """
        logger.info("MCP Tool called: logout()")

        try:
            session_id = _get_session_id(ctx)
            auth_manager = AuthManager(session_id)
            auth_manager.clear_auth()
            logger.info(f"Logged out session {session_id[:8]}...")
            return {"status": "success", "message": "Logged out successfully"}
        except Exception as e:
            logger.error(f"Logout failed: {e}")
            return format_error_response(e)

    # =========================================================================
    # Entity Matching and Search Tools
    # =========================================================================

    @mcp.tool()
    async def search_entity(
        ctx: Context,
        entity_id: int,
        attributes: Dict[str, Any],
        warehouse_id: str,
        top_n: int = 3,
    ) -> Dict[str, Any]:
        """
        Search for similar entities in the master table using vector search and ranking.

        This tool performs a semantic search to find entities similar to the provided attributes.
        Results are ranked by similarity score.

        IMPORTANT: You must be authenticated before using this tool. Use the 'login' tool first
        if you haven't authenticated yet.

        Args:
            entity_id: The entity type ID (e.g., 1 for Customer, 2 for Product)
            attributes: Dictionary of entity attributes to search for.
                    Must include all required matching attributes for the entity type.
                    Example: {"name": "John Doe", "email": "john@example.com", "phone": "1234567890"}
            warehouse_id: The warehouse ID to query (required for multi-tenant support)
            top_n: Number of top similar results to return (1-20, default: 3)

        Returns:
            SearchResponse containing:
            - entity_type: Name of the entity
            - results: List of similar entities with:
                - lakefusion_id: Clickable link to the golden record profile in LakeFusion portal
                - score: Similarity score (0-1)
                - golden_record_attributes: Full attributes of the matched record

        Example:
            search_entity(
                entity_id=1,
                attributes={"name": "John Doe", "email": "john@example.com"},
                warehouse_id="warehouse_123",
                top_n=5
            )

        HINT: When displaying results to the user, always include the lakefusion_id profile link
        for each result so users can click to view the full golden record in LakeFusion portal.
        """
        logger.info(
            f"MCP Tool called: search_entity(entity_id={entity_id}, warehouse_id={warehouse_id}, top_n={top_n})"
        )

        try:
            # Validate input
            try:
                validated_input = SearchEntityRequest(
                    entity_id=entity_id,
                    attributes=attributes,
                    warehouse_id=warehouse_id,
                    top_n=top_n,
                )
            except ValidationError as e:
                return {
                    "error": "Validation failed",
                    "message": "Invalid input parameters",
                    "details": e.errors()
                }

            # Check authentication
            is_auth, auth_error, token = await _check_auth(ctx)
            if not is_auth:
                return auth_error

            client = get_matchmaven_client()
            result = await client.search_entity(
                entity_id=validated_input.entity_id,
                attributes=validated_input.attributes,
                warehouse_id=validated_input.warehouse_id,
                token=token,
                top_n=validated_input.top_n,
            )

            validated = validate_response(result, SearchResponse)
            # Add clickable profile URLs to each result
            add_profile_urls_to_search_results(validated["results"], entity_id)
            logger.info(f"Search completed for entity_id {entity_id}: {len(validated['results'])} results")
            return validated

        except Exception as e:
            logger.error(f"Search failed: {e}")
            return format_error_response(e, entity_id=entity_id)
        

    @mcp.tool()
    async def match_entity(
        ctx: Context,
        entity_id: int,
        attributes: Dict[str, Any],
        warehouse_id: str,
    ) -> Dict[str, Any]:
        """
        Match an entity against the master table to find exact or potential matches.

        This tool performs vector search followed by LLM-based scoring to determine if the
        provided entity matches an existing golden record. It returns one of three statuses:
        ExactMatch, PotentialMatch, or NoMatch.

        IMPORTANT: You must be authenticated before using this tool. Use the 'login' tool first
        if you haven't authenticated yet.

        Args:
            entity_id: The entity type ID (e.g., 1 for Customer, 2 for Product)
            attributes: Dictionary of entity attributes to match.
                    Must include all required matching attributes for the entity type.
                    Example: {"name": "John Doe", "email": "john@example.com", "phone": "1234567890"}
            warehouse_id: The warehouse ID to query (required for multi-tenant support)

        Returns:
            MatchResponse containing:
            - entity_type: Name of the entity
            - match_status: One of "ExactMatch", "PotentialMatch", or "NoMatch"
            - score: Similarity/confidence score (0-1, null if NoMatch)
            - reason: LLM explanation for the match decision
            - matched_golden_record: If match found:
                - lakefusion_id: Clickable link to the golden record profile in LakeFusion portal
                - golden_record_attributes: Full attributes of the matched record

        Example:
            match_entity(
                entity_id=1,
                attributes={"name": "John Doe", "email": "john@example.com"},
                warehouse_id="warehouse_123"
            )
        """
        logger.info(
            f"MCP Tool called: match_entity(entity_id={entity_id}, warehouse_id={warehouse_id})"
        )

        try:
            # Validate input
            try:
                validated_input = MatchEntityRequest(
                    entity_id=entity_id,
                    attributes=attributes,
                    warehouse_id=warehouse_id,
                )
            except ValidationError as e:
                return {
                    "error": "Validation failed",
                    "message": "Invalid input parameters",
                    "details": e.errors()
                }

            # Check authentication
            is_auth, auth_error, token = await _check_auth(ctx)
            if not is_auth:
                return auth_error

            # Make the match request with user's token
            client = get_matchmaven_client()
            result = await client.match_entity(
                entity_id=validated_input.entity_id,
                attributes=validated_input.attributes,
                warehouse_id=validated_input.warehouse_id,
                token=token,
            )

            # Validate response format
            validated = validate_response(result, MatchResponse)
            # Add clickable profile URL if there's a matched golden record
            add_profile_url_to_match_result(validated, entity_id)
            logger.info(f"Match completed for entity {entity_id} : {validated['match_status']}")

            return validated

        except ValidationError as e:
            logger.error(f"Invalid API response format: {e}")
            return {"error": "Invalid response from API", "details": e.errors()}

        except Exception as e:
            logger.error(f"Match failed: {e}")
            return format_error_response(e, entity_id=entity_id)

    # =========================================================================
    # Entity Management Tools
    # =========================================================================

    @mcp.tool()
    async def get_entities(
        ctx: Context,
        is_active: bool = True,
    ) -> Dict[str, Any]:
        """
        Get list of available entities from the Integration Hub.

        This tool retrieves all configured entities that can be used with search_entity
        and match_entity tools. Use this to discover which entity types are available
        and their corresponding entity_id values.

        IMPORTANT: You must be authenticated before using this tool. Use the 'login' tool first
        if you haven't authenticated yet.

        Args:
            is_active: Filter by active status (default: True). Set to False to include
                      inactive entities.

        Returns:
            Response containing:
            - entities: List of available entities with:
                - id: Integration hub ID
                - task_name: Name of the integration task
                - entity_id: The entity type ID to use with search/match tools
                - entity_name: Human-readable name of the entity
                - entity_description: Description of the entity type
                - model_id: Associated model ID
                - model_name: Name of the model
                - is_active: Whether the entity is active
            - count: Total number of entities returned

        Example:
            get_entities()  # Get all active entities
            get_entities(is_active=False)  # Include inactive entities
        """
        logger.info(f"MCP Tool called: get_entities(is_active={is_active})")

        try:
            # Validate input
            try:
                validated_input = GetEntitiesRequest(is_active=is_active)
            except ValidationError as e:
                return {
                    "error": "Validation failed",
                    "message": "Invalid input parameters",
                    "details": e.errors()
                }

            # Check authentication
            is_auth, auth_error, token = await _check_auth(ctx)
            if not is_auth:
                return auth_error

            client = get_middlelayer_client()
            result = await client.get_entities(
                token=token,
                is_active=validated_input.is_active,
            )

            # Transform response to simplified format
            entities = []
            for item in result:
                entity_info = {
                    "id": item.get("id"),
                    "task_name": item.get("task_name"),
                    "entity_id": item.get("entity_id"),
                    "entity_name": item.get("entity", {}).get("name") if item.get("entity") else None,
                    "entity_description": item.get("entity", {}).get("description") if item.get("entity") else None,
                    "model_id": item.get("modelid"),
                    "model_name": item.get("model_experiment", {}).get("name") if item.get("model_experiment") else None,
                    "is_active": item.get("is_active"),
                }
                entities.append(entity_info)

            response = {
                "entities": entities,
                "count": len(entities),
            }

            logger.info(f"Get entities completed: {len(entities)} entities returned")
            return response

        except Exception as e:
            logger.error(f"Get entities failed: {e}")
            return format_error_response(e)

    logger.info(
        "MCP tools registered: login, logout, auth_status, search_entity, match_entity, get_entities"
    )
