"""Match Maven Playground Service.

Contains domain-specific classes for Match Maven entity matching:
- MatchResultNormalizer: Normalizer for match results
- MatchMavenLLMService: Pre-configured LLM service for entity matching
"""

from typing import Any, Callable, Dict, List, Optional, Set

from app.lakefusion_matchmaven_service.services.llm_service import LLMService, LLMServiceConfig
from app.lakefusion_matchmaven_service.services.llm_response_parser import (
    FieldConfig,
    NormalizerConfig,
    ResultNormalizer,
    coerce_string,
    coerce_score,
    create_match_status_coercer,
)

# ============================================================================
# Match Result Configuration
# ============================================================================

# Default LLM settings for Match Maven
DEFAULT_LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"
DEFAULT_MAX_TOKENS = 4000
DEFAULT_TEMPERATURE = 0.0

# Match status value mappings
MATCH_VALUES = {'match', 'yes', 'true', '1'}
POSSIBLE_VALUES = {'possible', 'maybe', 'uncertain', 'partial', 'likely'}

# Pre-configured coercer for match status
coerce_match_status = create_match_status_coercer(
    match_values=MATCH_VALUES,
    possible_values=POSSIBLE_VALUES,
    default_value="NOT_A_MATCH"
)

# Match Result normalizer configuration
MATCH_RESULT_CONFIG = NormalizerConfig(
    fields=[
        FieldConfig(
            target_name='id',
            source_names=['id', 'record_id', 'entity_id', 'row_id', 'index', 'matched_entity', 'entity', 'name'],
            default='unknown',
            coerce=coerce_string
        ),
        FieldConfig(
            target_name='lakefusion_id',
            source_names=['lakefusion_id', 'lakefusionId', 'lf_id', 'lfid'],
            default=None
        ),
        FieldConfig(
            target_name='score',
            source_names=['score', 'similarity', 'confidence', 'match_score', 'similarity_score'],
            default=0.0,
            coerce=coerce_score
        ),
        FieldConfig(
            target_name='match',
            source_names=['match', 'is_match', 'status', 'match_status', 'result'],
            default='NOT_A_MATCH',
            coerce=coerce_match_status
        ),
        FieldConfig(
            target_name='reason',
            source_names=['reason', 'explanation', 'rationale', 'reasoning', 'details'],
            default='',
            coerce=coerce_string
        ),
    ],
    id_field='id'
)


# ============================================================================
# Score-based Match Status Thresholds
# ============================================================================

# Match status is determined by score thresholds (overrides LLM's match field)
MATCH_THRESHOLD = 0.9      # score >= 0.9 → MATCH
POSSIBLE_THRESHOLD = 0.6   # score >= 0.6 → POSSIBLE
                           # score < 0.6  → NOT_A_MATCH


def apply_score_based_match_status(result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply score-based thresholds to determine match status using default thresholds.

    Thresholds:
    - score >= 0.9 → MATCH
    - score >= 0.6 → POSSIBLE
    - score < 0.6  → NOT_A_MATCH

    Args:
        result: Normalized result dict with 'score' field

    Returns:
        Result dict with 'match' field updated based on score
    """
    return apply_score_based_match_status_with_thresholds(
        result, MATCH_THRESHOLD, POSSIBLE_THRESHOLD
    )


def apply_score_based_match_status_with_thresholds(
    result: Dict[str, Any],
    match_threshold: float,
    possible_threshold: float
) -> Dict[str, Any]:
    """
    Apply score-based thresholds to determine match status with custom thresholds.

    Args:
        result: Normalized result dict with 'score' field
        match_threshold: Score threshold for MATCH status
        possible_threshold: Score threshold for POSSIBLE status

    Returns:
        Result dict with 'match' field updated based on score
    """
    score = result.get('score', 0.0)

    if score >= match_threshold:
        result['match'] = 'MATCH'
    elif score >= possible_threshold:
        result['match'] = 'POSSIBLE'
    else:
        result['match'] = 'NOT_A_MATCH'

    return result


# ============================================================================
# Match Result Normalizer
# ============================================================================

class MatchResultNormalizer:
    """
    Normalizer specific to Match Maven match results.
    Uses the generic ResultNormalizer with pre-configured MATCH_RESULT_CONFIG,
    then applies score-based thresholds to determine match status.
    """

    _normalizer = ResultNormalizer(MATCH_RESULT_CONFIG)

    @classmethod
    def normalize(cls, results: List[Any]) -> List[Dict[str, Any]]:
        """
        Normalize results to MatchResult schema format.

        Match status is determined by score thresholds:
        - score >= 0.9 → MATCH
        - score >= 0.6 → POSSIBLE
        - score < 0.6  → NOT_A_MATCH

        Args:
            results: List of raw result dicts from LLM

        Returns:
            List of normalized MatchResult-compatible dicts
        """
        normalized = cls._normalizer.normalize(results)

        # Apply score-based match status thresholds
        return [apply_score_based_match_status(r) for r in normalized]


# ============================================================================
# Match Maven LLM Service
# ============================================================================

# Configuration for Match Maven entity matching
MATCH_MAVEN_CONFIG = LLMServiceConfig(
    default_endpoint=DEFAULT_LLM_ENDPOINT,
    max_tokens=DEFAULT_MAX_TOKENS,
    temperature=DEFAULT_TEMPERATURE,
    endpoint_check_timeout=30,
    normalizer_config=MATCH_RESULT_CONFIG,
    require_results=True,
    mode="direct_prompts"
)


class MatchMavenLLMService:
    """
    LLM Service pre-configured for Match Maven entity matching.

    This is a thin wrapper around LLMService with MATCH_MAVEN_CONFIG.
    Provides a static interface for backward compatibility.
    """

    _instance = LLMService(MATCH_MAVEN_CONFIG)

    @classmethod
    async def execute_direct_prompts(
        cls,
        system_prompt: str,
        user_prompt: str,
        llm_endpoint: Optional[str] = None,
        token: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        match_threshold: Optional[float] = None,
        possible_match_threshold: Optional[float] = None,
        enforce_json_format: Optional[bool] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Execute LLM call for entity matching.

        Args:
            system_prompt: System prompt for entity matching
            user_prompt: User prompt with entities to compare
            llm_endpoint: Optional endpoint override
            token: Authentication token
            temperature: Optional LLM temperature override (0.0-1.0)
            max_tokens: Optional max tokens override
            match_threshold: Optional threshold for MATCH status (default 0.9)
            possible_match_threshold: Optional threshold for POSSIBLE status (default 0.6)
            **kwargs: Additional parameters (ignored)

        Returns:
            Dict with match results, timing, and token usage
        """
        # Build extra params for LLM call
        extra_params = {}
        if temperature is not None:
            extra_params['temperature'] = temperature
        if max_tokens is not None:
            extra_params['max_tokens'] = max_tokens
        if enforce_json_format is not None:
            extra_params['enforce_json_format'] = enforce_json_format

        result = await cls._instance.execute(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            token=token,
            llm_endpoint=llm_endpoint,
            **extra_params,
            **kwargs
        )

        # Rename raw_text to raw_response for API consistency
        if "raw_text" in result:
            result["raw_response"] = result.pop("raw_text")

        # Use provided thresholds or defaults
        effective_match_threshold = match_threshold if match_threshold is not None else MATCH_THRESHOLD
        effective_possible_threshold = possible_match_threshold if possible_match_threshold is not None else POSSIBLE_THRESHOLD

        # Apply score-based match status thresholds
        if "results" in result:
            result["results"] = [
                apply_score_based_match_status_with_thresholds(
                    r, effective_match_threshold, effective_possible_threshold
                ) for r in result["results"]
            ]

        return result
