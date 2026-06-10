"""Generic LLM Response Parser.

Handles various LLM response formats and extracts structured data.
This is a generic utility class with no domain-specific dependencies.
"""

import json
import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Set

from lakefusion_utility.utils.logging_utils import get_logger

app_logger = get_logger(__name__)


class LLMResponseParser:
    """Generic parser for LLM responses that handles multiple formats."""

    @classmethod
    def extract_content(cls, response: Any) -> str:
        """
        Extract text content from various LLM response formats.

        Supported formats:
        - Raw string
        - OpenAI/Anthropic chat completion format (choices[0].message.content)
        - Content as list with reasoning/text blocks (Claude, o1, etc.)
        - Predictions format (Databricks, MLflow)
        - Direct content/text/output/generated_text fields
        - Nested response structures

        Args:
            response: Raw LLM response in any supported format

        Returns:
            Extracted text content as string
        """
        if response is None:
            return ""

        if isinstance(response, str):
            return response

        if isinstance(response, list):
            if not response:
                return ""
            return cls.extract_content(response[0])

        if isinstance(response, dict):
            return cls._extract_from_dict(response)

        return str(response)

    @classmethod
    def _extract_from_dict(cls, response: Dict[str, Any]) -> str:
        """Extract content from dict response."""
        # OpenAI/Anthropic chat completion format
        choices = response.get('choices', [])
        if choices:
            message = choices[0].get('message', {})
            content = message.get('content', '')
            return cls._process_content_field(content)

        # Predictions format (Databricks/MLflow endpoints)
        predictions = response.get('predictions', [])
        if predictions:
            pred = predictions[0]
            if isinstance(pred, dict):
                return pred.get('content', '') or pred.get('text', '') or str(pred)
            return str(pred)

        # Direct content field
        if 'content' in response:
            return cls._process_content_field(response['content'])

        # Direct text field
        if 'text' in response:
            return response['text']

        # Output field (some models use this)
        if 'output' in response:
            return cls._process_content_field(response['output'])

        # Generated text field (HuggingFace format)
        if 'generated_text' in response:
            return response['generated_text']

        # Response field (some APIs wrap output)
        if 'response' in response:
            return cls._process_content_field(response['response'])

        # Result field
        if 'result' in response:
            return cls._process_content_field(response['result'])

        return str(response)

    @classmethod
    def _process_content_field(cls, content: Any) -> str:
        """Process content field which may be string or list of blocks."""
        if isinstance(content, str):
            return content

        if isinstance(content, list):
            text_parts = []

            for item in content:
                if isinstance(item, str):
                    text_parts.append(item)
                elif isinstance(item, dict):
                    item_type = item.get('type', '')

                    # Extract text blocks (skip reasoning/thinking blocks)
                    if item_type == 'text':
                        text_parts.append(item.get('text', ''))
                    elif item_type == 'tool_use':
                        # Some models return JSON in tool_use blocks
                        if 'input' in item:
                            text_parts.append(json.dumps(item['input']))
                    elif item_type in ('reasoning', 'thinking'):
                        # Skip reasoning blocks - they're internal thought process
                        continue
                    elif 'text' in item:
                        text_parts.append(item['text'])
                    elif 'content' in item:
                        text_parts.append(str(item['content']))

            return '\n'.join(text_parts)

        return str(content) if content else ""

    @classmethod
    def parse_json(cls, text: str) -> List[Dict[str, Any]]:
        """
        Parse JSON from text using simple parsing strategies only.

        Strategies (in order):
        1. Direct JSON parse
        2. Extract from markdown code blocks
        3. Extract JSON array/object from mixed content

        No complex fixing logic - if JSON is invalid, return empty list
        and let the UI show the raw response to the user.

        Args:
            text: Text containing JSON to parse

        Returns:
            List of parsed dictionaries (empty list if parsing fails)
        """
        if not text:
            return []

        text = text.strip()

        # Strategy 1: Direct parse
        result = cls._try_parse(text)
        if result is not None:
            return result

        # Strategy 2: Extract from markdown code blocks
        extracted = cls._extract_from_markdown(text)
        if extracted != text:
            result = cls._try_parse(extracted)
            if result is not None:
                return result

        # Strategy 3: Extract JSON array/object from mixed content
        extracted = cls._extract_json_structure(text)
        if extracted:
            result = cls._try_parse(extracted)
            if result is not None:
                return result

        # No valid JSON found - return empty (UI will show raw response)
        app_logger.warning(f"Could not parse JSON from response: {text[:200]}...")
        return []

    @classmethod
    def _try_parse(cls, text: str) -> Optional[List[Dict[str, Any]]]:
        """Try to parse text as JSON, returning list of dicts.

        Handles various response structures:
        - Direct array: [...]
        - Single object: {...}
        - Nested array: {"matches": [...]} or {"results": [...]} or {"data": [...]}
        """
        try:
            result = json.loads(text)
            if isinstance(result, list):
                return result
            if isinstance(result, dict):
                # Check for nested arrays under common keys
                nested_keys = ['matches', 'results', 'data', 'items', 'entities', 'records', 'evaluation']
                for key in nested_keys:
                    if key in result and isinstance(result[key], list):
                        app_logger.info(f"Extracted results from nested key '{key}'")
                        return result[key]
                # Single result, wrap in list
                return [result]
        except json.JSONDecodeError:
            pass
        return None

    @classmethod
    def _extract_from_markdown(cls, text: str) -> str:
        """Extract JSON from markdown code blocks."""
        if '```' not in text:
            return text

        # Try to extract from ```json ... ``` or ``` ... ```
        patterns = [
            r'```json\s*([\s\S]*?)\s*```',
            r'```\s*([\s\S]*?)\s*```',
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                extracted = match.group(1).strip()
                if extracted:
                    return extracted

        return text

    @classmethod
    def _extract_json_structure(cls, text: str) -> Optional[str]:
        """Extract JSON array or object from text that may contain other content."""
        # Already starts with JSON
        if text.startswith('[') or text.startswith('{'):
            return text

        # Find JSON array in text
        match = re.search(r'\[[\s\S]*\]', text)
        if match:
            return match.group(0)

        # Try to find JSON object and wrap in array
        match = re.search(r'\{[\s\S]*\}', text)
        if match:
            return f'[{match.group(0)}]'

        return None


# ============================================================================
# Configuration-based Result Normalizer
# ============================================================================

@dataclass
class FieldConfig:
    """Configuration for a single field in the normalizer."""
    target_name: str
    source_names: List[str]
    default: Any = None
    coerce: Optional[Callable[[Any], Any]] = None


@dataclass
class NormalizerConfig:
    """Configuration for the result normalizer."""
    fields: List[FieldConfig]
    id_field: str = "id"  # Field to use for auto-generated IDs on error

    def get_field_mappings(self) -> Dict[str, List[str]]:
        """Get field mappings dict for lookup."""
        return {f.target_name: f.source_names for f in self.fields}

    def get_defaults(self) -> Dict[str, Any]:
        """Get defaults dict."""
        return {f.target_name: f.default for f in self.fields if f.default is not None}

    def get_coercions(self) -> Dict[str, Callable]:
        """Get coercions dict."""
        return {f.target_name: f.coerce for f in self.fields if f.coerce is not None}


class ResultNormalizer:
    """
    Generic normalizer for converting parsed results to a target schema.
    Uses configuration to define field mappings, defaults, and coercions.
    """

    def __init__(self, config: NormalizerConfig):
        """
        Initialize normalizer with configuration.

        Args:
            config: NormalizerConfig defining fields, mappings, and coercions
        """
        self.config = config
        self.field_mappings = config.get_field_mappings()
        self.defaults = config.get_defaults()
        self.coercions = config.get_coercions()

    def normalize(self, results: List[Any]) -> List[Dict[str, Any]]:
        """
        Normalize a list of results using configured mappings.

        Args:
            results: List of raw result dicts from LLM

        Returns:
            List of normalized dicts
        """
        normalized = []

        for idx, item in enumerate(results):
            if not isinstance(item, dict):
                continue

            try:
                result = self._normalize_item(item, idx)
                normalized.append(result)
            except Exception:
                # Return item with defaults on error
                result = {**self.defaults, self.config.id_field: f"record_{idx}"}
                normalized.append(result)

        return normalized

    def _normalize_item(self, item: Dict[str, Any], idx: int) -> Dict[str, Any]:
        """Normalize a single item."""
        result = {}

        for target_field, source_fields in self.field_mappings.items():
            # Get value from source fields
            value = self._get_field_value(item, source_fields)

            # Apply default if no value
            if value is None:
                value = self.defaults.get(target_field)

            # Apply type coercion if configured
            if target_field in self.coercions and value is not None:
                try:
                    value = self.coercions[target_field](value)
                except Exception:
                    value = self.defaults.get(target_field)

            result[target_field] = value

        # Ensure id field has a value
        if not result.get(self.config.id_field):
            result[self.config.id_field] = f"record_{idx}"

        return result

    def _get_field_value(self, item: Dict[str, Any], source_fields: List[str]) -> Any:
        """Get value from item using list of possible field names."""
        for field_name in source_fields:
            # Try exact match
            if field_name in item:
                return item[field_name]

            # Try case-insensitive match
            for key in item.keys():
                if key.lower() == field_name.lower():
                    return item[key]

        return None


# ============================================================================
# Coercion Functions (reusable across configs)
# ============================================================================

def coerce_string(value: Any) -> str:
    """Coerce value to string."""
    if value is None:
        return ""
    if isinstance(value, list):
        return " ".join(str(item) for item in value)
    return str(value)


def coerce_score(value: Any) -> float:
    """Coerce value to float score in 0.0-1.0 range."""
    if value is None:
        return 0.0
    try:
        if isinstance(value, str):
            value = value.replace('%', '').strip()
        score = float(value)
        if score > 1:
            score = score / 100
        return max(0.0, min(1.0, score))
    except (ValueError, TypeError):
        return 0.0


def create_match_status_coercer(
    match_values: Set[str],
    possible_values: Set[str],
    default_value: str
) -> Callable[[Any], str]:
    """
    Create a match status coercion function with custom value mappings.

    Args:
        match_values: Set of values that map to "MATCH"
        possible_values: Set of values that map to "POSSIBLE"
        default_value: Default value for non-matching inputs

    Returns:
        Coercion function
    """
    match_upper = {v.upper() for v in match_values}
    possible_upper = {v.upper() for v in possible_values}

    def coerce_match_status(value: Any) -> str:
        if value is None:
            return default_value
        if isinstance(value, bool):
            return "MATCH" if value else default_value
        if isinstance(value, str):
            normalized = value.upper().strip()
            if normalized in match_upper:
                return "MATCH"
            if normalized in possible_upper:
                return "POSSIBLE"
        return default_value

    return coerce_match_status


