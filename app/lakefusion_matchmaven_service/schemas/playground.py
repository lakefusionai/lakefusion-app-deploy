"""Pydantic schemas for Match Maven Playground API."""

from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field, field_validator, model_validator


class MatchStatus(str, Enum):
    """Enumeration of possible match statuses."""
    MATCH = "MATCH"
    NOT_A_MATCH = "NOT_A_MATCH"
    POSSIBLE = "POSSIBLE"


class MatchResult(BaseModel):
    """Single match result from LLM comparison."""
    id: str = Field(default="unknown", description="Record identifier")
    lakefusion_id: Optional[str] = Field(default=None, description="LakeFusion 32-char hex ID")
    score: float = Field(default=0.0, ge=0.0, le=1.0, description="Match confidence score")
    match: str = Field(default=MatchStatus.NOT_A_MATCH.value, description="Match status")
    reason: str = Field(default="", description="Explanation for match decision")

    @field_validator('id', mode='before')
    @classmethod
    def coerce_id(cls, v):
        return str(v) if v is not None else "unknown"

    @field_validator('score', mode='before')
    @classmethod
    def coerce_score(cls, v):
        """Convert score to float, normalize percentages to 0-1 range."""
        if v is None:
            return 0.0
        try:
            score = float(str(v).replace('%', '').strip())
            if score > 1:
                score = score / 100
            return max(0.0, min(1.0, score))
        except (ValueError, TypeError):
            return 0.0

    @field_validator('match', mode='before')
    @classmethod
    def coerce_match(cls, v):
        """Normalize match status to MATCH, NOT_A_MATCH, or POSSIBLE."""
        if isinstance(v, bool):
            return MatchStatus.MATCH.value if v else MatchStatus.NOT_A_MATCH.value
        if isinstance(v, str):
            v = v.upper().strip()
            if v in ('MATCH', 'YES', 'TRUE'):
                return MatchStatus.MATCH.value
            if v in ('POSSIBLE', 'MAYBE', 'UNCERTAIN'):
                return MatchStatus.POSSIBLE.value
        return MatchStatus.NOT_A_MATCH.value

    @field_validator('reason', mode='before')
    @classmethod
    def coerce_reason(cls, v):
        if v is None:
            return ""
        if isinstance(v, list):
            return " ".join(str(item) for item in v)
        return str(v)

# =============================================================================
# Request Models
# =============================================================================

class ExecuteCompareRequest(BaseModel):
    """Request model for executing LLM entity comparison."""
    finalSystemPrompt: str = Field(
        ...,
        min_length=10,
        max_length=100000,
        description="The complete system prompt to send to the LLM"
    )
    finalUserPrompt: str = Field(
        ...,
        max_length=50000,
        description="The complete user prompt to send to the LLM"
    )
    llmEndpoint: Optional[str] = Field(
        None,
        max_length=500,
        description="Optional LLM endpoint name override"
    )
    attributes: Optional[List[str]] = Field(
        None,
        description="List of attribute names for parser context (used by auto-parser when direct parsing fails)"
    )
    # Playground configuration options
    temperature: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description="LLM temperature (0.0-1.0). Lower values make output more deterministic."
    )
    maxTokens: Optional[int] = Field(
        None,
        ge=100,
        le=10000,
        description="Maximum tokens in LLM response (100-10000)"
    )
    matchThreshold: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description="Score threshold for MATCH status (0.0-1.0)"
    )
    possibleMatchThreshold: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description="Score threshold for POSSIBLE status (0.0-1.0)"
    )
    enforceJsonFormat: Optional[bool] = Field(
        True,
        description="Whether to enforce JSON schema format on LLM response. Default true."
    )


# =============================================================================
# Response Models
# =============================================================================

class ExecuteCompareResponse(BaseModel):
    """Response model for LLM entity comparison."""
    results: List[MatchResult] = Field(default_factory=list, description="List of parsed match results")
    raw_response: Optional[str] = Field(None, description="Raw LLM response text before parsing")
    llm_endpoint: str = Field(..., description="LLM endpoint used for comparison")
    elapsed_ms: int = Field(..., description="Time taken in milliseconds")
    mode: str = Field(default="direct_prompts", description="Execution mode")
    # Token usage from LLM response
    prompt_tokens: Optional[int] = Field(None, description="Number of tokens in the prompt")
    completion_tokens: Optional[int] = Field(None, description="Number of tokens in the completion")
    total_tokens: Optional[int] = Field(None, description="Total tokens used")
    reasoning_tokens: Optional[int] = Field(None, description="Number of reasoning/thinking tokens (for models like Gemini)")
