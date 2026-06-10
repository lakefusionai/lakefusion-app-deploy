from typing import Dict, Any, Optional
from pydantic import ValidationError
from app.lakefusion_mcp_service.utils.logger import get_logger

logger = get_logger(__name__)


class ToolError(Exception):
    """Base exception for tool errors."""
    def __init__(self, error: str, message: str, **extra):
        self.error = error
        self.message = message
        self.extra = extra
        super().__init__(message)


class AuthRequiredError(ToolError):
    """Raised when authentication is required."""
    def __init__(self):
        super().__init__(
            error="Authentication required",
            message="Please use the 'login' tool first to authenticate."
        )


class ValidationFailedError(ToolError):
    """Raised when input/output validation fails."""
    def __init__(self, details: list):
        super().__init__(
            error="Validation failed",
            message="Invalid input parameters.",
            details=details
        )


class APIError(ToolError):
    """Raised when API call fails."""
    def __init__(self, operation: str, details: str):
        super().__init__(
            error=f"{operation} failed",
            message=f"{operation} failed. Please verify your entity_id, attributes, and warehouse_id are correct.",
            details=details
        )

class AuthRequiredError(ToolError):
    """Raised when authentication is required."""
    def __init__(self):
        super().__init__(
            error="Authentication required",
            message="Your token may have expired. Please use the 'login' tool to re-authenticate."
        )


def format_error_response(e: Exception, entity_id: Optional[int] = None) -> Dict[str, Any]:
    """Format exception into standard error response."""
    
    if isinstance(e, ToolError):
        response = {"error": e.error, "message": e.message, **e.extra}
    elif isinstance(e, ValidationError):
        response = {"error": "Validation failed", "details": e.errors()}
    else:
        error_msg = str(e).lower()
        if "not authenticated" in error_msg or "token" in error_msg:
            response = {
                "error": "Authentication required",
                "message": "Please use the 'login' tool first to authenticate."
            }
        else:
            response = {
                "error": str(e),
                "message": "Operation failed. Please check your inputs and try again."
            }
    
    if entity_id is not None:
        response["entity_id"] = entity_id
    
    return response


def format_error_response(e: Exception, entity_id: Optional[int] = None) -> Dict[str, Any]:
    """Format exception into standard error response."""
    
    if isinstance(e, ToolError):
        response = {"error": e.error, "message": e.message, **e.extra}
    elif isinstance(e, ValidationError):
        response = {"error": "Validation failed", "details": e.errors()}
    else:
        error_msg = str(e).lower()
        if "not authenticated" in error_msg or "token" in error_msg:
            response = {
                "error": "Authentication required",
                "message": "Please use the 'login' tool first to authenticate."
            }
        else:
            response = {
                "error": str(e),
                "message": "Operation failed. Please check your inputs and try again."
            }
    
    if entity_id is not None:
        response["entity_id"] = entity_id
    
    return response