"""
Playground API Routes for Match Maven.

Provides endpoints for the playground feature:
- Execute LLM entity comparison
"""

from fastapi import APIRouter, Depends, HTTPException

from app.lakefusion_matchmaven_service.utils.app_db import token_required_wrapper
from app.lakefusion_matchmaven_service.schemas.playground import (
    ExecuteCompareRequest,
    ExecuteCompareResponse,
)
from app.lakefusion_matchmaven_service.services.playground_service import MatchMavenLLMService
from lakefusion_utility.utils.logging_utils import get_logger

app_logger = get_logger(__name__)

# Playground API Router
playground_router = APIRouter(tags=["Playground API"], prefix="/playground")


# =============================================================================
# Execute Compare Endpoint
# =============================================================================

@playground_router.post("/execute-compare", response_model=ExecuteCompareResponse)
async def execute_compare(
    request: ExecuteCompareRequest,
    check: dict = Depends(token_required_wrapper)
):
    """
    Execute LLM entity comparison using direct prompts.

    This endpoint accepts pre-built system and user prompts from the playground
    and sends them directly to the LLM for entity matching evaluation.

    Args:
        request: ExecuteCompareRequest with finalSystemPrompt, finalUserPrompt, and optional llmEndpoint

    Returns:
        ExecuteCompareResponse with match results, elapsed time, and endpoint used
    """
    token = check.get('token')

    if not token:
        raise HTTPException(
            status_code=401,
            detail="Authentication token is required"
        )

    app_logger.info("Executing playground compare")
    app_logger.info(f"System prompt length: {len(request.finalSystemPrompt)} chars")
    app_logger.info(f"User prompt length: {len(request.finalUserPrompt)} chars")

    try:
        result = await MatchMavenLLMService.execute_direct_prompts(
            system_prompt=request.finalSystemPrompt,
            user_prompt=request.finalUserPrompt,
            llm_endpoint=request.llmEndpoint,
            token=token,
            attributes=request.attributes,
            temperature=request.temperature,
            max_tokens=request.maxTokens,
            match_threshold=request.matchThreshold,
            possible_match_threshold=request.possibleMatchThreshold,
            enforce_json_format=request.enforceJsonFormat,
        )

        return ExecuteCompareResponse(
            results=result["results"],
            raw_response=result.get("raw_response"),
            llm_endpoint=result["llm_endpoint"],
            elapsed_ms=result["elapsed_ms"],
            mode=result["mode"],
            prompt_tokens=result.get("prompt_tokens"),
            completion_tokens=result.get("completion_tokens"),
            total_tokens=result.get("total_tokens"),
            reasoning_tokens=result.get("reasoning_tokens")
        )

    except HTTPException as he:
        # Re-raise HTTPExceptions with their original detail (already formatted)
        raise he
    except Exception as e:
        error_msg = str(e)
        app_logger.error(f"Execute compare failed: {error_msg}", exc_info=True)

        # Provide more specific error messages for common issues
        if "timeout" in error_msg.lower():
            detail = "LLM request timed out. Try reducing the number of records or using a faster model."
        elif "rate limit" in error_msg.lower():
            detail = "Rate limit exceeded. Please wait a moment before trying again."
        elif "endpoint" in error_msg.lower() and "not found" in error_msg.lower():
            detail = f"LLM endpoint not found. Please check your model configuration. Error: {error_msg}"
        elif "authentication" in error_msg.lower() or "unauthorized" in error_msg.lower():
            detail = "Authentication failed. Your session may have expired. Please refresh the page."
        else:
            detail = f"Failed to execute comparison: {error_msg}"

        raise HTTPException(
            status_code=500,
            detail=detail
        )


