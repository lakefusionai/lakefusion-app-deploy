"""
Base Prompt Router Module
==========================

This module defines the FastAPI router for managing Base Prompts in the LakeFusion system.
Base Prompts are template prompts that can be used as foundations for AI interactions.

The router provides a complete CRUD (Create, Read, Update, Delete) API with additional
functionality for status management and filtering.

Endpoints:
    POST   /base-prompts/                      - Create a new base prompt
    GET    /base-prompts/                      - List all base prompts with filters
    GET    /base-prompts/{prompt_id}           - Get a specific base prompt by ID
    PUT    /base-prompts/{prompt_id}           - Update a base prompt
    DELETE /base-prompts/{prompt_id}           - Delete a base prompt
    PATCH  /base-prompts/{prompt_id}/toggle-active - Toggle active/inactive status

Security:
    All endpoints require authentication via the token_required_wrapper dependency,
    which validates the JWT token and extracts user information.

Author: LakeFusion Team
Version: 1.0.0
"""

from fastapi import APIRouter, Depends, status
from sqlalchemy.orm import Session
from typing import List

from app.lakefusion_middlelayer_service.utils.app_db import get_db, token_required_wrapper
from lakefusion_utility.models.base_prompt import (
    BasePromptCreate,
    BasePromptResponse,
)
from lakefusion_utility.models.api_response import ApiResponse
from lakefusion_utility.services.base_prompt_service import BasePromptService

# ============================================================================
# Router Initialization
# ============================================================================

# Initialize the APIRouter with tags for OpenAPI documentation grouping
# and a prefix to namespace all base prompt endpoints
base_prompt_router = APIRouter(
    tags=["Base Prompt API"],  # Groups endpoints in Swagger/ReDoc UI
    prefix="/base-prompts"      # All routes will be prefixed with /base-prompts
)


# ============================================================================
# CREATE ENDPOINT
# ============================================================================

@base_prompt_router.post(
    "/",
    response_model=ApiResponse[BasePromptResponse],
    status_code=status.HTTP_201_CREATED,
    summary="Create a new Base Prompt",
    description="""
    Creates a new Base Prompt in the system.
    
    The version is automatically fixed to 1 for all new base prompts.
    The created_by field is automatically populated from the authenticated user's token.
    
    **Required Fields:**
    - prompt_name: Unique name for the prompt
    - prompt_content: The actual prompt text/template
    - description: Detailed description of the prompt's purpose
    - tags: Array of tags for categorization and search
    - is_active: Whether the prompt is active (defaults to True)
    
    **Returns:**
    - 201 Created: Successfully created base prompt with full details
    - 400 Bad Request: Invalid input data or duplicate prompt name
    - 401 Unauthorized: Missing or invalid authentication token
    """,
)
def create_base_prompt(
    base_prompt: BasePromptCreate,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    """
    Create a new BasePrompt with version automatically set to 1.
    
    This endpoint handles the creation of new base prompts. The version is fixed
    to 1 as per business logic requirements. The user who creates the prompt is
    automatically recorded from the JWT token's 'sub' (subject) claim.
    
    Args:
        base_prompt (BasePromptCreate): Pydantic model containing:
            - prompt_name (str): Unique identifier name for the prompt
            - prompt_content (str): The actual prompt template/text
            - description (str, optional): Detailed description
            - tags (List[str], optional): Categorization tags
            - is_active (bool, optional): Active status, defaults to True
        
        db (Session): SQLAlchemy database session injected via dependency
            - Provides database connection for the request lifecycle
            - Automatically manages connection pooling and cleanup
        
        check (dict): Authentication dictionary from token_required_wrapper containing:
            - decoded (dict): Decoded JWT payload with user information
                - sub (str): User identifier (typically user ID or email)
                - Additional claims like roles, permissions, etc.
    
    Returns:
        ApiResponse[BasePromptResponse]: Standardized API response containing:
            - status (str): "success" or "error"
            - message (str): Human-readable success/error message
            - data (BasePromptResponse): Created prompt details including:
                - id: Generated database ID
                - prompt_name: Name of the prompt
                - prompt_content: Prompt text
                - version: Always 1 for new prompts
                - created_by: User ID from token
                - created_at: Timestamp of creation
                - is_active: Active status
    
    Raises:
        HTTPException: 
            - 400: If prompt_name already exists or validation fails
            - 401: If authentication token is missing or invalid
            - 500: If database operation fails
    
    Example:
        Request Body:
        {
            "prompt_name": "customer_service_greeting",
            "prompt_content": "Hello! How can I assist you today?",
            "description": "Standard greeting for customer service interactions",
            "tags": ["customer_service", "greeting"],
            "is_active": true
        }
        
        Response (201):
        {
            "status": "success",
            "message": "Base prompt created successfully",
            "data": {
                "id": 1,
                "prompt_name": "customer_service_greeting",
                "prompt_content": "Hello! How can I assist you today?",
                "version": 1,
                "created_by": "user_123",
                "created_at": "2025-11-18T10:30:00Z",
                "is_active": true
            }
        }
    """
    # Extract the user identifier from the JWT token's 'sub' claim
    # The 'sub' claim typically contains the user ID or email
    # If not present, defaults to empty string (though this should be prevented by auth)
    base_prompt.created_by = check.get("decoded", {}).get("sub", "")
    
    # Initialize the service layer with the database session
    # Service layer handles business logic and database operations
    service = BasePromptService(db)
    
    # Delegate the creation operation to the service layer
    # Service handles validation, database insertion, and response formatting
    return service.create_base_prompt(base_prompt)


# ============================================================================
# READ ENDPOINTS (List and Detail)
# ============================================================================

@base_prompt_router.get(
    "/",
    response_model=ApiResponse[List[BasePromptResponse]],
    status_code=status.HTTP_200_OK,
    summary="List all Base Prompts",
    description="""
    Retrieves a paginated list of all Base Prompts with optional filtering and sorting.
    
    **Query Parameters:**
    - is_active: Filter by active status (true/false/null for all)
    - sort_by: Field to sort by (created_at, updated_at, prompt_name)
    - sort_order: Sort direction (asc/desc)
    - limit: Maximum number of results (default: 50)
    - offset: Number of results to skip (default: 0)
    
    **Returns:**
    - 200 OK: List of base prompts matching criteria
    - 401 Unauthorized: Missing or invalid authentication token
    
    **Pagination:**
    Use limit and offset for pagination. Example: limit=20&offset=40 gets items 41-60.
    """,
)
def get_all_base_prompts(
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
    is_active: bool = None,
    sort_by: str = "created_at",
    sort_order: str = "desc",
    limit: int = 50,
    offset: int = 0,
):
    """
    Fetch a paginated and filtered list of all BasePrompts.
    
    This endpoint supports filtering by active status, sorting by various fields,
    and pagination to handle large datasets efficiently. It's designed for list
    views in the UI where users need to browse and search through prompts.
    
    Args:
        db (Session): SQLAlchemy database session
            - Injected via FastAPI dependency injection
            - Provides transactional database access
        
        check (dict): Authentication dictionary from JWT token validation
            - Ensures only authenticated users can access the endpoint
            - Contains user identity and permissions
        
        is_active (bool, optional): Filter for active status
            - True: Only return active prompts
            - False: Only return inactive prompts
            - None (default): Return all prompts regardless of status
        
        sort_by (str, optional): Field name to sort results by
            - Default: "created_at"
            - Common options: "created_at", "updated_at", "prompt_name"
            - Allows users to organize results by relevance
        
        sort_order (str, optional): Sort direction
            - "desc" (default): Descending order (newest/highest first)
            - "asc": Ascending order (oldest/lowest first)
        
        limit (int, optional): Maximum number of results to return
            - Default: 50
            - Used for pagination to prevent overwhelming the client
            - Recommended range: 10-100 depending on use case
        
        offset (int, optional): Number of results to skip
            - Default: 0
            - Used for pagination (offset = page_number * limit)
            - Example: offset=50 with limit=50 gets page 2
    
    Returns:
        ApiResponse[List[BasePromptResponse]]: Standardized response with:
            - status: "success"
            - message: Confirmation message
            - data: List of BasePromptResponse objects, each containing:
                - id: Unique prompt identifier
                - prompt_name: Prompt name
                - prompt_content: Full prompt text
                - version: Version number (always 1)
                - description: Prompt description
                - tags: List of associated tags
                - is_active: Active status
                - created_by: Creator user ID
                - created_at: Creation timestamp
                - updated_at: Last update timestamp
    
    Raises:
        HTTPException:
            - 401: If authentication fails
            - 422: If query parameters are invalid (e.g., negative limit)
            - 500: If database query fails
    
    Example Usage:
        GET /base-prompts/?is_active=true&sort_by=prompt_name&sort_order=asc&limit=20&offset=0
        
        Response (200):
        {
            "status": "success",
            "message": "Base prompts fetched successfully",
            "data": [
                {
                    "id": 1,
                    "prompt_name": "greeting_prompt",
                    "prompt_content": "Hello! How can I help?",
                    "version": 1,
                    "is_active": true,
                    "created_at": "2025-11-18T10:00:00Z"
                },
                // ... more prompts
            ]
        }
    
    Performance Notes:
        - Uses database indexing on created_at and prompt_name for fast sorting
        - Limit parameter prevents excessive data transfer
        - Consider implementing cursor-based pagination for very large datasets
    """
    # Initialize service layer with database session
    service = BasePromptService(db)
    
    # Query the database with all filter and pagination parameters
    # The service layer handles the actual SQL query construction
    prompts = service.read_base_prompts(
        is_active=is_active,      # Filter by active status
        sort_by=sort_by,          # Sort field
        sort_order=sort_order,    # Sort direction
        limit=limit,              # Maximum results
        offset=offset,            # Pagination offset
    )
    
    # Convert SQLAlchemy ORM objects to Pydantic response models
    # This ensures proper serialization and data validation for the response
    # from_orm() is a Pydantic method that creates a model instance from an ORM object
    return ApiResponse[List[BasePromptResponse]](
        status="success",
        message="Base prompts fetched successfully",
        data=[BasePromptResponse.from_orm(p) for p in prompts]
    )


@base_prompt_router.get(
    "/{prompt_id}",
    response_model=ApiResponse[BasePromptResponse],
    status_code=status.HTTP_200_OK,
    summary="Get Base Prompt by ID",
    description="""
    Retrieves a single Base Prompt by its unique identifier.
    
    Use this endpoint to fetch detailed information about a specific prompt,
    typically for display in a detail view or for editing purposes.
    
    **Path Parameters:**
    - prompt_id: Unique integer ID of the base prompt
    
    **Returns:**
    - 200 OK: Base prompt details
    - 404 Not Found: Prompt with specified ID doesn't exist
    - 401 Unauthorized: Missing or invalid authentication token
    """,
)
def get_base_prompt_by_id(
    prompt_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    """
    Retrieve a specific BasePrompt by its unique database ID.
    
    This endpoint is typically used when a user clicks on a prompt in a list view
    to see full details, or when loading a prompt for editing. It returns the
    complete prompt data including all fields and metadata.
    
    Args:
        prompt_id (int): The unique database identifier for the base prompt
            - Must be a positive integer
            - Corresponds to the primary key in the database
            - Used in URL path as /base-prompts/{prompt_id}
        
        db (Session): SQLAlchemy database session
            - Injected via FastAPI dependency
            - Provides database access for the query
        
        check (dict): Authentication dictionary from JWT validation
            - Ensures user is authenticated before accessing prompt data
            - May be used for permission checks in future enhancements
    
    Returns:
        ApiResponse[BasePromptResponse]: Standardized response containing:
            - status: "success" or "error"
            - message: Confirmation with prompt ID
            - data: Complete BasePromptResponse object with:
                - id: The prompt's unique identifier
                - prompt_name: Name of the prompt
                - prompt_content: Full prompt text/template
                - version: Version number (always 1)
                - description: Detailed description
                - tags: List of categorization tags
                - is_active: Current active status
                - created_by: User ID who created the prompt
                - created_at: Creation timestamp
                - updated_at: Last modification timestamp
    
    Raises:
        HTTPException:
            - 404: If no prompt exists with the specified ID
                  Message: "BasePrompt with ID {prompt_id} not found"
            - 401: If authentication token is missing or invalid
            - 500: If database query fails unexpectedly
    
    Example Usage:
        GET /base-prompts/42
        
        Response (200):
        {
            "status": "success",
            "message": "BasePrompt with ID 42 fetched successfully",
            "data": {
                "id": 42,
                "prompt_name": "summarization_prompt",
                "prompt_content": "Please summarize the following text...",
                "version": 1,
                "description": "General purpose text summarization",
                "tags": ["summarization", "nlp"],
                "is_active": true,
                "created_by": "user_123",
                "created_at": "2025-11-18T09:00:00Z",
                "updated_at": "2025-11-18T10:30:00Z"
            }
        }
        
        Response (404):
        {
            "status": "error",
            "message": "BasePrompt with ID 42 not found",
            "data": null
        }
    
    Use Cases:
        - Loading prompt details for a detail/view page
        - Fetching prompt data before editing
        - Retrieving a specific prompt for AI interaction
        - Audit or review purposes
    """
    # Initialize service layer with database session
    service = BasePromptService(db)
    
    # Retrieve the prompt from database via service layer
    # Service handles the query and raises HTTPException if not found
    prompt = service.get_base_prompt_by_id(prompt_id)
    
    # Return standardized response with the prompt data
    # The prompt is already a BasePromptResponse object from the service
    return ApiResponse[BasePromptResponse](
        status="success",
        message=f"BasePrompt with ID {prompt_id} fetched successfully",
        data=prompt,
    )


# ============================================================================
# UPDATE ENDPOINT
# ============================================================================

@base_prompt_router.put(
    "/{prompt_id}",
    response_model=ApiResponse[BasePromptResponse],
    status_code=status.HTTP_200_OK,
    summary="Update a Base Prompt",
    description="""
    Updates an existing Base Prompt with new data.
    
    This is a full update operation (PUT) that replaces the prompt's data.
    You must provide all fields, even if you're only changing some.
    
    **Path Parameters:**
    - prompt_id: Unique integer ID of the prompt to update
    
    **Request Body:**
    All fields from BasePromptCreate (same as create endpoint)
    
    **Note:** Version remains fixed at 1 and cannot be changed.
    
    **Returns:**
    - 200 OK: Successfully updated prompt with new data
    - 404 Not Found: Prompt with specified ID doesn't exist
    - 400 Bad Request: Invalid update data or duplicate name
    - 401 Unauthorized: Missing or invalid authentication token
    """,
)
def update_base_prompt(
    prompt_id: int,
    update_data: BasePromptCreate,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    """
    Update an existing BasePrompt with new data.
    
    This endpoint performs a full update (PUT operation) on a base prompt.
    All fields can be modified except the version (which remains 1) and the
    created_by/created_at fields (which are immutable). The updated_at timestamp
    is automatically set to the current time.
    
    Args:
        prompt_id (int): Unique database ID of the prompt to update
            - Must correspond to an existing prompt
            - Used as the primary key for database lookup
            - Specified in the URL path
        
        update_data (BasePromptCreate): Pydantic model containing updated fields:
            - prompt_name (str): New name for the prompt
                * Must be unique across all prompts (unless keeping same name)
                * Can be same as current name for the same prompt
            - prompt_content (str): Updated prompt text/template
                * This is the core content that gets updated
            - description (str, optional): New description
            - tags (List[str], optional): Updated list of tags
                * Completely replaces existing tags
            - is_active (bool, optional): Updated active status
        
        db (Session): SQLAlchemy database session
            - Provides transactional database access
            - Ensures atomicity of the update operation
        
        check (dict): Authentication dictionary from JWT token
            - Verifies user is authenticated
            - May be used for authorization checks (e.g., only creator can update)
    
    Returns:
        ApiResponse[BasePromptResponse]: Standardized response with:
            - status: "success"
            - message: Confirmation message
            - data: Updated BasePromptResponse object containing:
                - All updated fields with their new values
                - updated_at: New timestamp reflecting the update
                - created_by/created_at: Unchanged original values
                - version: Still 1 (immutable)
    
    Raises:
        HTTPException:
            - 404: If prompt with prompt_id doesn't exist
            - 400: If validation fails (e.g., duplicate prompt_name)
            - 401: If authentication fails
            - 422: If request body is malformed
            - 500: If database update operation fails
    
    Example Usage:
        PUT /base-prompts/42
        
        Request Body:
        {
            "prompt_name": "updated_greeting",
            "prompt_content": "Hello! Welcome to our service. How may I help you?",
            "description": "Updated greeting with warmer tone",
            "tags": ["greeting", "customer_service", "updated"],
            "is_active": true
        }
        
        Response (200):
        {
            "status": "success",
            "message": "Base prompt updated successfully",
            "data": {
                "id": 42,
                "prompt_name": "updated_greeting",
                "prompt_content": "Hello! Welcome to our service. How may I help you?",
                "version": 1,
                "description": "Updated greeting with warmer tone",
                "tags": ["greeting", "customer_service", "updated"],
                "is_active": true,
                "created_by": "user_123",
                "created_at": "2025-11-18T09:00:00Z",
                "updated_at": "2025-11-18T11:45:00Z"
            }
        }
    
    Notes:
        - This is a PUT operation, so all fields should be provided
        - For partial updates, consider implementing a PATCH endpoint
        - The service layer handles duplicate name validation
        - Original creator and creation date are preserved
        - Consider implementing audit logging for update operations
        - Version control could be enhanced in future to support multiple versions
    """
    # Initialize service layer with database session
    service = BasePromptService(db)
    
    # Delegate update operation to service layer
    # Service handles validation, database update, and error handling
    # Returns the updated prompt or raises HTTPException on failure
    return service.update_base_prompt(prompt_id, update_data)


# ============================================================================
# DELETE ENDPOINT
# ============================================================================

@base_prompt_router.delete(
    "/{prompt_id}",
    response_model=ApiResponse[str],
    status_code=status.HTTP_200_OK,
    summary="Delete a Base Prompt",
    description="""
    Permanently deletes a Base Prompt from the system.
    
    **WARNING:** This is a permanent deletion. The prompt cannot be recovered
    after deletion. Consider implementing soft delete (is_active=false) instead
    for production systems.
    
    **Path Parameters:**
    - prompt_id: Unique integer ID of the prompt to delete
    
    **Returns:**
    - 200 OK: Prompt successfully deleted
    - 404 Not Found: Prompt with specified ID doesn't exist
    - 401 Unauthorized: Missing or invalid authentication token
    - 409 Conflict: Prompt is in use and cannot be deleted (if implemented)
    """,
)
def delete_base_prompt(
    prompt_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    """
    Permanently delete a BasePrompt from the database.
    
    This endpoint performs a hard delete operation, completely removing the prompt
    from the database. This action is irreversible. In production environments,
    consider implementing soft delete (setting is_active to False) instead to
    maintain data integrity and audit trails.
    
    Args:
        prompt_id (int): Unique database ID of the prompt to delete
            - Must correspond to an existing prompt
            - Once deleted, this ID may be reused by the database
            - Cannot be recovered after deletion
        
        db (Session): SQLAlchemy database session
            - Provides transactional database access
            - Ensures atomic deletion (all or nothing)
            - Changes are committed only if operation succeeds
        
        check (dict): Authentication dictionary from JWT token
            - Verifies user is authenticated
            - May be used for authorization (e.g., only admins or creators can delete)
            - Could be extended to check user permissions
    
    Returns:
        ApiResponse[str]: Standardized response containing:
            - status: "success"
            - message: "BasePrompt deleted successfully"
            - data: Confirmation string from service layer
                * Typically contains the deleted prompt's name or ID
                * Useful for logging and audit purposes
    
    Raises:
        HTTPException:
            - 404: If prompt with prompt_id doesn't exist
                  Message: "BasePrompt with ID {prompt_id} not found"
            - 401: If authentication fails
            - 409: If prompt is referenced by other entities (if foreign key constraints exist)
            - 500: If database deletion fails
    
    Example Usage:
        DELETE /base-prompts/42
        
        Response (200):
        {
            "status": "success",
            "message": "BasePrompt deleted successfully",
            "data": "Deleted prompt: summarization_prompt (ID: 42)"
        }
        
        Response (404):
        {
            "status": "error",
            "message": "BasePrompt with ID 42 not found",
            "data": null
        }
    
    Security Considerations:
        - Implement role-based access control (only admins/creators)
        - Log all deletion operations for audit trails
        - Consider requiring additional confirmation for deletions
        - Implement rate limiting to prevent abuse
    
    Best Practices:
        - Soft Delete Alternative: Instead of permanent deletion, set is_active=False
        - Cascade Handling: Check for dependencies before deletion
        - Audit Logging: Record who deleted what and when
        - Backup Strategy: Ensure regular database backups exist
        - Confirmation: Require user confirmation in UI before calling this endpoint
    
    Future Enhancements:
        - Implement soft delete with a deleted_at timestamp
        - Add batch delete endpoint for multiple prompts
        - Implement "trash" system where prompts can be recovered within X days
        - Add cascade options for related data
    """
    # Initialize service layer with database session
    service = BasePromptService(db)
    
    # Perform the deletion operation via service layer
    # Service handles the database deletion and returns confirmation
    # Raises HTTPException if prompt doesn't exist or deletion fails
    result = service.delete_base_prompt(prompt_id)
    
    # Return standardized success response
    # The result string from service contains details about what was deleted
    return ApiResponse[str](
        status="success",
        message="BasePrompt deleted successfully",
        data=result,
    )


# ============================================================================
# STATUS MANAGEMENT ENDPOINT
# ============================================================================

@base_prompt_router.patch(
    "/{prompt_id}/toggle-active",
    response_model=ApiResponse[BasePromptResponse],
    status_code=status.HTTP_200_OK,
    summary="Toggle Base Prompt Active Status",
    description="""
    Toggles the active/inactive status of a Base Prompt.
    
    This is a soft delete alternative that allows prompts to be temporarily
    disabled without permanently removing them from the system.
    
    **Path Parameters:**
    - prompt_id: Unique integer ID of the prompt to toggle
    
    **Behavior:**
    - If prompt is active (true), it will be set to inactive (false)
    - If prompt is inactive (false), it will be set to active (true)
    
    **Use Cases:**
    - Temporarily disable a prompt without deleting it
    - Enable a previously disabled prompt
    - Soft delete mechanism for data retention
    
    **Returns:**
    - 200 OK: Status successfully toggled
    - 404 Not Found: Prompt with specified ID doesn't exist
    - 401 Unauthorized: Missing or invalid authentication token
    """,
)
def toggle_base_prompt_active(
    prompt_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper),
):
    """
    Toggle the active/inactive status of a BasePrompt.
    
    This endpoint implements a soft delete/restore mechanism by toggling the
    is_active boolean field. This is preferred over hard deletion as it:
    - Maintains data integrity and relationships
    - Preserves audit trails and history
    - Allows easy restoration if needed
    - Prevents accidental data loss
    
    The operation is idempotent - calling it multiple times alternates the state.
    
    Args:
        prompt_id (int): Unique database ID of the prompt to toggle
            - Must correspond to an existing prompt
            - The current active status will be reversed
            - Does not require knowing the current state
        
        db (Session): SQLAlchemy database session
            - Provides transactional database access
            - Ensures atomic toggle operation
            - Automatically updates the updated_at timestamp
        
        check (dict): Authentication dictionary from JWT token
            - Verifies user is authenticated
            - May be used for authorization checks
            - Could verify user has permission to toggle status
    
    Returns:
        ApiResponse[BasePromptResponse]: Standardized response with:
            - status: "success"
            - message: Descriptive message including prompt name and new status
                * Example: "BasePrompt 'greeting_prompt' status toggled to True"
            - data: Complete BasePromptResponse object with:
                - is_active: The new status (opposite of previous)
                - updated_at: New timestamp reflecting the change
                - All other fields unchanged
    
    Raises:
        HTTPException:
            - 404: If prompt with prompt_id doesn't exist
            - 401: If authentication fails
            - 500: If database update operation fails
    
    Example Usage:
        PATCH /base-prompts/42/toggle-active
        
        Response (200) - Deactivating:
        {
            "status": "success",
            "message": "BasePrompt 'greeting_prompt' status toggled to False",
            "data": {
                "id": 42,
                "prompt_name": "greeting_prompt",
                "prompt_content": "Hello! How can I help?",
                "version": 1,
                "is_active": false,  // Changed from true to false
                "created_by": "user_123",
                "created_at": "2025-11-18T09:00:00Z",
                "updated_at": "2025-11-18T12:00:00Z"  // Updated timestamp
            }
        }
        
        Response (200) - Reactivating:
        {
            "status": "success",
            "message": "BasePrompt 'greeting_prompt' status toggled to True",
            "data": {
                "id": 42,
                "prompt_name": "greeting_prompt",
                "is_active": true,  // Changed back from false to true
                "updated_at": "2025-11-18T12:05:00Z"  // New timestamp
                // ... other fields
            }
        }
    
    Use Cases:
        1. **Soft Delete**: Deactivate instead of deleting
           - User wants to "delete" but keep for potential future use
           - Maintains referential integrity with other tables
        
        2. **Temporary Disabling**: Turn off during maintenance
           - Prompt needs updating but shouldn't be used meanwhile
           - Testing different prompts by activating/deactivating
        
        3. **A/B Testing**: Toggle between different prompt versions
           - Easily switch active prompts for experiments
           - Quick rollback if new version has issues
        
        4. **Permission Management**: Control which prompts are available
           - Admin can control which prompts users can access
           - Seasonal or conditional prompt availability
    
    Workflow Integration:
        - UI can show "Activate"/"Deactivate" button based on current status
        - List views can filter by is_active to show only active prompts
        - Deleted items view can show inactive prompts with "Restore" option
        - Audit logs can track who toggled status and when
    
    Best Practices:
        - Always use this instead of hard delete for user-facing deletions
        - Implement UI confirmation before toggling
        - Show clear visual indication of active vs inactive status
        - Consider adding a "deleted_at" timestamp for soft deletes
        - Implement bulk toggle for batch operations
    
    Performance Notes:
        - Very fast operation (single row update)
        - No cascade operations needed
        - Index on is_active column improves filtered queries
    """
    # Initialize service layer with database session
    service = BasePromptService(db)
    
    # Perform the toggle operation via service layer
    # Service retrieves current status, inverts it, and saves
    # Returns the updated prompt object with new status
    prompt = service.toggle_base_prompt_active(prompt_id)
    
    # Return standardized response with updated prompt data
    # Message includes prompt name and new status for clarity
    return ApiResponse[BasePromptResponse](
        status="success",
        message=f"BasePrompt '{prompt.prompt_name}' status toggled to {prompt.is_active}",
        data=prompt,
    )


# ============================================================================
# END OF ROUTER DEFINITION
# ============================================================================