"""Generic LLM Service with configuration-based result handling.

This module provides:
- LLMServiceConfig: Configuration for LLM service behavior
- LLMService: Generic LLM service that can be configured for different use cases

For Match Maven specific service, see playground_service.py
"""

import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from fastapi import HTTPException
from mlflow.deployments import get_deploy_client

from lakefusion_utility.utils.logging_utils import get_logger
from app.lakefusion_matchmaven_service.services.llm_response_parser import (
    LLMResponseParser,
    ResultNormalizer,
    NormalizerConfig,
)

app_logger = get_logger(__name__)

# Default Configuration
DATABRICKS_HOST = os.environ.get('DATABRICKS_HOST', 'https://databricks.com')
DEFAULT_LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"
DEFAULT_MAX_TOKENS = 4000
DEFAULT_TEMPERATURE = 0.0  # Nearly deterministic for consistent results


@dataclass
class LLMServiceConfig:
    """Configuration for LLM service."""
    # LLM parameters
    default_endpoint: str = DEFAULT_LLM_ENDPOINT
    max_tokens: int = DEFAULT_MAX_TOKENS
    temperature: float = DEFAULT_TEMPERATURE
    endpoint_check_timeout: int = 30

    # Result handling
    normalizer_config: Optional[NormalizerConfig] = None
    require_results: bool = True  # Raise error if no results parsed

    # Response mode
    mode: str = "direct_prompts"


class LLMService:
    """
    Generic LLM service for executing prompts and parsing responses.

    Can be configured with different normalizers for different use cases.
    """

    def __init__(self, config: Optional[LLMServiceConfig] = None):
        """
        Initialize LLM service with configuration.

        Args:
            config: LLMServiceConfig for customizing behavior.
                    If None, uses defaults without result normalization.
        """
        self.config = config or LLMServiceConfig()
        self._normalizer = (
            ResultNormalizer(self.config.normalizer_config)
            if self.config.normalizer_config
            else None
        )

    def _setup_environment(self, token: str) -> None:
        """Set up environment for MLflow client."""
        os.environ['DATABRICKS_TOKEN'] = token
        if not os.environ.get('DATABRICKS_HOST'):
            os.environ['DATABRICKS_HOST'] = DATABRICKS_HOST

    def _check_endpoint(self, endpoint: str, token: str) -> bool:
        """Check if endpoint is ready."""
        self._setup_environment(token)
        client = get_deploy_client("databricks")
        start = time.time()
        timeout = self.config.endpoint_check_timeout

        while (time.time() - start) < timeout:
            try:
                state = client.get_endpoint(endpoint).get("state", {})
                if state.get("ready") == "READY":
                    return True
                time.sleep(5)
            except Exception:
                time.sleep(5)

        app_logger.warning(f"Endpoint {endpoint} not ready after {timeout}s")
        return False

    async def execute(
        self,
        system_prompt: str,
        user_prompt: str,
        token: str,
        llm_endpoint: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Execute LLM call with prompts.

        Args:
            system_prompt: System prompt for the LLM
            user_prompt: User prompt for the LLM
            token: Authentication token
            llm_endpoint: Optional endpoint override
            **kwargs: Additional parameters (temperature, max_tokens)

        Returns:
            Dict containing:
            - results: Normalized results (if normalizer configured) or raw parsed JSON
            - raw_text: Raw extracted text from LLM
            - elapsed_ms: Time taken in milliseconds
            - llm_endpoint: Endpoint used
            - mode: Execution mode
            - prompt_tokens, completion_tokens, total_tokens: Token usage
        """
        if not token:
            raise HTTPException(status_code=401, detail="Authentication token required")

        endpoint = llm_endpoint or self.config.default_endpoint

        if not self._check_endpoint(endpoint, token):
            raise HTTPException(status_code=503, detail=f"Endpoint {endpoint} not ready")

        self._setup_environment(token)
        client = get_deploy_client("databricks")
        start_time = time.time()

        try:
            # Build messages array, only including messages with non-empty content
            messages = []
            if system_prompt and system_prompt.strip():
                messages.append({"role": "system", "content": system_prompt})
            if user_prompt and user_prompt.strip():
                messages.append({"role": "user", "content": user_prompt})

            # Ensure at least one message exists
            if not messages:
                raise HTTPException(
                    status_code=400,
                    detail="At least one of system_prompt or user_prompt must be non-empty"
                )

            # Determine max_tokens: use kwargs override if provided, else config default
            # Gemini models with reasoning need higher limits (reasoning tokens count toward max_tokens)
            max_tokens = kwargs.get("max_tokens", self.config.max_tokens)
            if "gemini" in endpoint.lower() and max_tokens < 10000:
                max_tokens = 10000  # Higher limit for Gemini models with reasoning

            # Get temperature from kwargs if provided, else use config default
            temperature = kwargs.get("temperature", self.config.temperature)

            # Whether to enforce JSON schema on LLM response (default: True)
            enforce_json_format = kwargs.get("enforce_json_format", True)

            app_logger.info(f"LLM call params - endpoint: {endpoint}, temperature: {temperature}, max_tokens: {max_tokens}, enforce_json_format: {enforce_json_format}")

            # Build inputs, excluding temperature for models that don't support it
            inputs = {
                "messages": messages,
                "max_tokens": max_tokens,
            }

            # Conditionally include response_format based on enforce_json_format flag
            if enforce_json_format:
                inputs["response_format"] = {
                    "type": "json_schema",
                    "json_schema": {
                        "name": "match_results",
                        "schema": {
                            "type": "object",
                            "properties": {
                                "results": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "id": {"type": "string"},
                                            "score": {"type": "number"},
                                            "reason": {"type": "string"},
                                            "lakefusion_id": {"type": "string"},
                                        },
                                        "required": ["id", "score", "reason", "lakefusion_id"],
                                        "additionalProperties": False,
                                    },
                                },
                            },
                            "required": ["results"],
                            "additionalProperties": False,
                        },
                        "strict": True,
                    },
                }
            # Exclude temperature for GPT-5 models that only support default temperature
            if "gpt-5" not in endpoint.lower():
                inputs["temperature"] = temperature

            # Try with current max_tokens, retry with lower value if it exceeds model limit
            try:
                response = client.predict(
                    endpoint=endpoint,
                    inputs=inputs
                )
            except Exception as e:
                error_str = str(e).lower()
                # Check if error is due to max_tokens exceeding limit
                if "max_tokens" in error_str or "maximum" in error_str or "token" in error_str:
                    app_logger.warning(f"max_tokens={max_tokens} may exceed limit, retrying with default {self.config.max_tokens}")
                    inputs["max_tokens"] = self.config.max_tokens
                    response = client.predict(
                        endpoint=endpoint,
                        inputs=inputs
                    )
                else:
                    raise
            elapsed_ms = int((time.time() - start_time) * 1000)

            # Debug: Log the full LLM response structure
            app_logger.debug(f"LLM raw response: {response}")

            # Extract text content
            text = LLMResponseParser.extract_content(response)

            # Parse JSON (simple parsing only - no fallbacks)
            parsed_results = LLMResponseParser.parse_json(text)

            # Normalize if normalizer configured
            if self._normalizer:
                results = self._normalizer.normalize(parsed_results)
            else:
                results = parsed_results

            # Log warning if no results parsed (but don't error - let UI show raw response)
            if not results:
                app_logger.warning(f"No results parsed from LLM response: {text[:200]}...")

            # Extract token usage
            usage = response.get("usage", {}) if isinstance(response, dict) else {}
            app_logger.info(f"LLM response usage: {usage}")
            app_logger.info(f"LLM response keys: {response.keys() if isinstance(response, dict) else 'not a dict'}")

            return {
                "results": results,
                "raw_text": text,
                "elapsed_ms": elapsed_ms,
                "llm_endpoint": endpoint,
                "mode": self.config.mode,
                "prompt_tokens": usage.get("prompt_tokens"),
                "completion_tokens": usage.get("completion_tokens"),
                "total_tokens": usage.get("total_tokens"),
                "reasoning_tokens": usage.get("reasoning_tokens"),
            }

        except HTTPException:
            raise
        except Exception as e:
            app_logger.error(f"LLM call failed: {e}")
            raise HTTPException(status_code=502, detail=str(e))
