"""Match Maven Playground Service.

Contains domain-specific classes for Match Maven entity matching:
- MatchResultNormalizer: Normalizer for match results
- MatchMavenLLMService: Pre-configured LLM service for entity matching
- DeterministicRulesService: In-process deterministic rules evaluation
"""

import pandas as pd
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
        reasoning_effort: Optional[str] = None,
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
        if reasoning_effort is not None:
            extra_params['reasoning_effort'] = reasoning_effort

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


# ============================================================================
# Deterministic Rules Service (pandas-based)
# ============================================================================

class DeterministicRulesService:
    """
    Evaluates deterministic match rules against master records using pandas.

    Rules are evaluated in order; the first matching rule is applied.
    Supports:
      - match_type "exact": equality comparison (case-insensitive)
      - match_type "fuzzy": levenshtein_standard, levenshtein_normalized, jaro_winkler
    """

    # ── String distance helpers ──────────────────────────────────────────

    @staticmethod
    def _levenshtein_distance(s1: str, s2: str) -> int:
        """Compute Levenshtein edit distance between two strings."""
        if len(s1) < len(s2):
            return DeterministicRulesService._levenshtein_distance(s2, s1)
        if len(s2) == 0:
            return len(s1)
        prev_row = list(range(len(s2) + 1))
        for i, c1 in enumerate(s1):
            curr_row = [i + 1]
            for j, c2 in enumerate(s2):
                cost = 0 if c1 == c2 else 1
                curr_row.append(min(curr_row[j] + 1, prev_row[j + 1] + 1, prev_row[j] + cost))
            prev_row = curr_row
        return prev_row[-1]

    @staticmethod
    def _jaro_similarity(s1: str, s2: str) -> float:
        """Compute Jaro similarity between two strings (0.0 to 1.0)."""
        if s1 == s2:
            return 1.0
        len1, len2 = len(s1), len(s2)
        if len1 == 0 or len2 == 0:
            return 0.0
        match_dist = max(len1, len2) // 2 - 1
        if match_dist < 0:
            match_dist = 0
        s1_matches = [False] * len1
        s2_matches = [False] * len2
        matches = 0
        transpositions = 0
        for i in range(len1):
            start = max(0, i - match_dist)
            end = min(i + match_dist + 1, len2)
            for j in range(start, end):
                if s2_matches[j] or s1[i] != s2[j]:
                    continue
                s1_matches[i] = True
                s2_matches[j] = True
                matches += 1
                break
        if matches == 0:
            return 0.0
        k = 0
        for i in range(len1):
            if not s1_matches[i]:
                continue
            while not s2_matches[k]:
                k += 1
            if s1[i] != s2[k]:
                transpositions += 1
            k += 1
        return (matches / len1 + matches / len2 + (matches - transpositions / 2) / matches) / 3

    @staticmethod
    def _jaro_winkler_similarity(s1: str, s2: str, p: float = 0.1) -> float:
        """Compute Jaro-Winkler similarity (0.0 to 1.0). p = scaling factor (default 0.1)."""
        jaro = DeterministicRulesService._jaro_similarity(s1, s2)
        prefix_len = 0
        for i in range(min(len(s1), len(s2), 4)):
            if s1[i] == s2[i]:
                prefix_len += 1
            else:
                break
        return jaro + prefix_len * p * (1 - jaro)

    # ── Evaluation methods ───────────────────────────────────────────────

    @staticmethod
    def _eval_exact(col: pd.Series, query_val: Any, allow_nulls: bool) -> tuple:
        """Exact equality match (case-insensitive, trimmed). Score: 1.0 or 0.0."""
        n = len(col)
        if query_val is None:
            mask = pd.Series([allow_nulls] * n, dtype=bool)
            return mask, mask.astype(float)
        q_str = str(query_val).strip().lower()
        match_mask = col.fillna("").astype(str).str.strip().str.lower() == q_str
        if allow_nulls:
            match_mask = match_mask | col.isna()
        return match_mask, match_mask.astype(float)

    @staticmethod
    def _eval_fuzzy(col: pd.Series, query_val: Any, function: str, threshold: float,
                    allow_nulls: bool) -> tuple:
        """Fuzzy match using the specified function. Returns (mask, scores)."""
        n = len(col)
        if query_val is None:
            return pd.Series([allow_nulls] * n, dtype=bool), pd.Series([0.0] * n)

        q_str = str(query_val).strip().lower()
        scores = []
        for val in col:
            if pd.isna(val) or val is None:
                scores.append(0.0 if not allow_nulls else 1.0)
                continue
            m_str = str(val).strip().lower()

            if function == "jaro_winkler":
                scores.append(round(DeterministicRulesService._jaro_winkler_similarity(q_str, m_str), 4))
            elif function == "levenshtein_standard":
                dist = DeterministicRulesService._levenshtein_distance(q_str, m_str)
                scores.append(round(1.0 - dist / max(len(q_str), len(m_str), 1), 4))
            else:
                # Default: levenshtein_normalized
                max_len = max(len(q_str), len(m_str))
                if max_len == 0:
                    scores.append(1.0)
                else:
                    dist = DeterministicRulesService._levenshtein_distance(q_str, m_str)
                    scores.append(round(1.0 - dist / max_len, 4))

        scores_series = pd.Series(scores)
        mask = scores_series >= threshold
        if allow_nulls:
            mask = mask | col.isna()
        return mask, scores_series

    def execute_rules_compare(
        self,
        query_record: Dict[str, Any],
        master_records: List[Dict[str, Any]],
        rules: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        if not master_records:
            return []

        df = pd.DataFrame(master_records)
        n = len(df)

        applied_rule: List[Optional[str]] = [None] * n
        overall_result: List[str] = ["NO_MATCH"] * n
        rules_eval_per_record: List[List[Dict]] = [[] for _ in range(n)]

        for rule in rules:
            rule_name = rule.get("name", "")
            action = rule.get("action_on_match", "MATCH")
            logical_op = rule.get("logical_operator", "AND").upper()
            conditions = rule.get("conditions", [])

            cond_masks: List[pd.Series] = []
            cond_scores: List[pd.Series] = []
            cond_meta: List[tuple] = []

            for cond in conditions:
                attr = cond.get("attribute", "")
                allow_nulls = cond.get("allow_nulls", False)
                match_type = cond.get("match_type", "exact")
                threshold = cond.get("threshold")
                query_val = query_record.get(attr)

                col = df[attr] if attr in df.columns else pd.Series([None] * n)
                function = cond.get("function", "")
                if match_type == "exact":
                    mask, scores = self._eval_exact(col, query_val, allow_nulls)
                elif match_type == "fuzzy":
                    t = float(threshold) if threshold is not None else 0.8
                    mask, scores = self._eval_fuzzy(col, query_val, function or "levenshtein_normalized", t, allow_nulls)
                else:
                    raise NotImplementedError(f"match_type '{match_type}' is not yet supported")

                cond_masks.append(mask)
                cond_scores.append(scores)
                cond_meta.append((attr, match_type, cond.get("function"), cond.get("operator"), threshold, allow_nulls, query_val))

            if not cond_masks:
                rule_mask = pd.Series([False] * n, dtype=bool)
            elif logical_op == "OR":
                rule_mask = cond_masks[0].copy()
                for m in cond_masks[1:]:
                    rule_mask |= m
            else:  # AND
                rule_mask = cond_masks[0].copy()
                for m in cond_masks[1:]:
                    rule_mask &= m

            for i in range(n):
                rule_matched = bool(rule_mask.iloc[i])
                is_applied = rule_matched and applied_rule[i] is None

                if is_applied:
                    applied_rule[i] = rule_name
                    overall_result[i] = action

                cond_evals = []
                for j, (attr, match_type, function, operator, threshold, allow_nulls, query_val) in enumerate(cond_meta):
                    passed = bool(cond_masks[j].iloc[i])
                    raw = df[attr].iloc[i] if attr in df.columns else None
                    master_val = raw.item() if hasattr(raw, "item") else raw
                    cond_evals.append({
                        "attribute": attr,
                        "match_type": match_type,
                        "function": function,
                        "operator": operator,
                        "threshold": threshold,
                        "allow_nulls": allow_nulls,
                        "query_value": query_val,
                        "master_value": master_val,
                        "score": float(cond_scores[j].iloc[i]),
                        "passed": passed,
                    })

                rules_eval_per_record[i].append({
                    "name": rule_name,
                    "matched": rule_matched,
                    "applied": is_applied,
                    "action_on_match": action,
                    "logical_operator": logical_op,
                    "conditions_evaluation": cond_evals,
                })

        results = []
        for i in range(n):
            record_id = str(df["id"].iloc[i]) if "id" in df.columns else str(i)
            results.append({
                "id": record_id,
                "overall_result": overall_result[i],
                "applied_rule": applied_rule[i],
                "rules_evaluation": rules_eval_per_record[i],
            })

        return results
