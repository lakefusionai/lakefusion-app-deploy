"""Match Maven Playground Service.

Contains domain-specific classes for Match Maven entity matching:
- MatchResultNormalizer: Normalizer for match results
- MatchMavenLLMService: Pre-configured LLM service for entity matching
- DeterministicRulesService: In-process deterministic rules evaluation
"""

import json
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

    Mirrors the Spark/Databricks notebook rules engine — same operator
    semantics, same fuzzy functions, same null handling — so a rule that
    matches in the notebook matches the same way here:

      - match_type "exact":  equality (case-insensitive, trimmed, with
                              empty/whitespace normalized to NULL on both sides)
      - match_type "fuzzy":  levenshtein_standard, levenshtein_normalized,
                              jaro_winkler, soundex
      - operator:            ">=", "<=", ">", "<", "=", "==", "!="
                              (used to compare a fuzzy score against threshold;
                              defaults to ">=" when not supplied)
      - allow_nulls:         True  -> NULL-NULL counts as a match (null_match | base)
                              False -> both sides must be non-NULL (not_null & base)

    Rules are evaluated in order; the first matching rule per record wins
    (`applied_rule`). All other rules are still recorded in
    `rules_evaluation` for explainability.

    The helpers below are deliberately structured to mirror the Spark side
    one-for-one — same names (`_apply_operator`, `_build_condition_mask`),
    same behavior. Notebooks build a Spark Column; the middlelayer builds
    a pandas Series / mask. The shapes match so the JSON shape returned to
    the UI is identical between playground (here) and a live job.
    """

    # ── String distance helpers (pure-Python; no Spark / rapidfuzz dep) ──

    @staticmethod
    def _levenshtein_distance(s1: str, s2: str) -> int:
        """Compute Levenshtein edit distance between two strings.
        Mirrors `F.levenshtein(left, right)` on the Spark side."""
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
        """Jaro similarity (0.0–1.0). Building block for Jaro-Winkler below."""
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
        """Jaro-Winkler similarity (0.0–1.0). Mirrors the rapidfuzz-backed
        `jaro_winkler_udf` on the Spark side; pure-Python here to avoid
        adding rapidfuzz as a middlelayer dependency."""
        jaro = DeterministicRulesService._jaro_similarity(s1, s2)
        prefix_len = 0
        for i in range(min(len(s1), len(s2), 4)):
            if s1[i] == s2[i]:
                prefix_len += 1
            else:
                break
        return jaro + prefix_len * p * (1 - jaro)

    @staticmethod
    def _soundex(s: str) -> str:
        """Standard English Soundex (4 chars, e.g. 'Robert' -> 'R163').
        Mirrors Spark SQL's `F.soundex(...)`."""
        if not s:
            return ""
        # Discard non-alpha and uppercase
        s = "".join(c for c in s.upper() if c.isalpha())
        if not s:
            return ""

        codes = {
            "B": "1", "F": "1", "P": "1", "V": "1",
            "C": "2", "G": "2", "J": "2", "K": "2", "Q": "2", "S": "2", "X": "2", "Z": "2",
            "D": "3", "T": "3",
            "L": "4",
            "M": "5", "N": "5",
            "R": "6",
        }

        first = s[0]
        # `prev` tracks the previously emitted code so consecutive duplicates
        # collapse — seeded with the first letter's own code so e.g. 'Pf'
        # doesn't emit '11'.
        prev = codes.get(first, "0")
        encoded = []
        for ch in s[1:]:
            if ch in ("H", "W"):
                continue  # ignored; doesn't break runs
            code = codes.get(ch, "0")  # vowels and other non-coded chars
            if code == "0":
                # Vowel-class — emits nothing but DOES break consonant runs.
                prev = "0"
                continue
            if code != prev:
                encoded.append(code)
            prev = code

        return (first + "".join(encoded) + "000")[:4]

    # ── Operator dispatcher (mirrors `_apply_operator` on the Spark side) ──

    @staticmethod
    def _apply_operator(value, operator: str, threshold):
        """Compare a value (or pandas Series) against `threshold` using the
        given operator. Same operator set as the Spark version so a rule
        config that targets ">=" / "!=" / etc. behaves identically here."""
        ops = {
            ">=": lambda a, b: a >= b,
            "<=": lambda a, b: a <= b,
            ">":  lambda a, b: a >  b,
            "<":  lambda a, b: a <  b,
            "=":  lambda a, b: a == b,
            "==": lambda a, b: a == b,
            "!=": lambda a, b: a != b,
        }
        if operator not in ops:
            raise ValueError(f"Unsupported operator: {operator!r}")
        return ops[operator](value, threshold)

    # ── Complex-type (STRUCT / ARRAY) helpers — mirror the Spark side ─────
    # ARRAY set-membership functions (match_type "fuzzy", no operator/threshold):
    #   intersect -> share ≥1 element; disjoint -> share none;
    #   subset -> source ⊆ candidate; superset -> source ⊇ candidate.
    _ARRAY_SET_FUNCS = {"intersect", "disjoint", "subset", "superset"}

    @staticmethod
    def _coerce_complex(v):
        """Parse a JSON string into dict/list; pass dict/list through unchanged."""
        if isinstance(v, str):
            s = v.strip()
            if s[:1] in ("{", "["):
                try:
                    return json.loads(s)
                except (ValueError, TypeError):
                    return v
        return v

    @staticmethod
    def _struct_subfield(value, sub_field):
        """Extract a sub-field from a STRUCT value (dict); None if absent."""
        return value.get(sub_field) if isinstance(value, dict) else None

    @staticmethod
    def _elem_values(value, sub_field, case_insensitive):
        """Flatten an ARRAY value to comparison strings.
        ARRAY<STRUCT> + sub_field -> each element[sub_field];
        ARRAY<STRUCT> no sub_field -> JSON of each element;
        ARRAY<scalar> -> the element itself."""
        if not isinstance(value, list):
            return []
        out = []
        for el in value:
            if sub_field is not None and isinstance(el, dict):
                ev = el.get(sub_field)
            elif isinstance(el, (dict, list)):
                ev = json.dumps(el, sort_keys=True, default=str)
            else:
                ev = el
            if ev is None:
                continue
            s = str(ev).strip()
            if not s:
                continue
            out.append(s.lower() if case_insensitive else s)
        return out

    @staticmethod
    def _array_set_match_py(src_set: set, cand_set: set, func: str) -> bool:
        if func == "intersect":
            return len(src_set & cand_set) > 0
        if func == "disjoint":
            return len(src_set & cand_set) == 0
        if func == "subset":      # source ⊆ candidate
            return src_set <= cand_set
        if func == "superset":    # source ⊇ candidate
            return src_set >= cand_set
        raise ValueError(f"Unsupported array set function: {func!r}")

    def _array_elem_any(self, src_vals, cand_vals, match_type, fuzzy_func,
                        threshold, operator) -> bool:
        """True if any (source element, candidate element) pair matches under
        exact / fuzzy semantics — the element-wise 'any element matches'."""
        op = operator or ">="
        for a in src_vals:
            for b in cand_vals:
                if match_type == "exact":
                    if ((a != b) if op in ("!=", "<>") else (a == b)):
                        return True
                    continue
                if fuzzy_func == "levenshtein_normalized":
                    ml = max(len(a), len(b)) or 1
                    score = 1.0 - self._levenshtein_distance(a, b) / ml
                elif fuzzy_func == "levenshtein_standard":
                    score = float(self._levenshtein_distance(a, b))
                elif fuzzy_func == "jaro_winkler":
                    score = self._jaro_winkler_similarity(a, b)
                elif fuzzy_func == "soundex":
                    if self._soundex(a) == self._soundex(b):
                        return True
                    continue
                else:
                    raise ValueError(f"Unsupported fuzzy function: {fuzzy_func!r}")
                if threshold is not None and self._apply_operator(score, op, float(threshold)):
                    return True
        return False

    def _array_condition_mask(self, q_coerced, col_coerced, sub_field,
                              case_insensitive, match_type, fuzzy_func,
                              threshold, operator, allow_nulls, n) -> tuple:
        """ARRAY attribute path: element-wise 'any match' (exact/fuzzy) or set
        membership (intersect/disjoint/subset/superset). Returns (mask, scores)."""
        src_vals = self._elem_values(
            q_coerced if isinstance(q_coerced, list) else [], sub_field, case_insensitive
        )
        src_empty = len(src_vals) == 0

        masks, scores = [], []
        for v in col_coerced:
            cand_vals = self._elem_values(
                v if isinstance(v, list) else [], sub_field, case_insensitive
            )
            cand_empty = len(cand_vals) == 0

            if fuzzy_func in self._ARRAY_SET_FUNCS:
                ok = self._array_set_match_py(set(src_vals), set(cand_vals), fuzzy_func)
            else:
                ok = self._array_elem_any(
                    src_vals, cand_vals, match_type, fuzzy_func, threshold, operator
                )

            if allow_nulls:
                passed = (src_empty and cand_empty) or (not src_empty and not cand_empty and ok)
            else:
                passed = (not src_empty) and (not cand_empty) and ok
            masks.append(bool(passed))
            scores.append(1.0 if passed else 0.0)
        return pd.Series(masks, dtype=bool), pd.Series(scores)

    # ── Condition evaluator (mirrors `build_condition_column`) ───────────

    def _build_condition_mask(
        self,
        cond: Dict[str, Any],
        query_val: Any,
        col: pd.Series,
        rule_allow_nulls: bool = False,
    ) -> tuple:
        """Pandas mirror of the Spark-side `build_condition_column`.

        For each row of `col` (master/candidate side), decide whether the
        condition passes against `query_val` (source side). Returns
        `(mask, scores)` — both pandas Series of length len(col).

        Semantics matched to Spark:
          - empty/whitespace -> NULL on both source and candidate sides
          - case_insensitive (default True): lowercases both sides before
                              comparison. Soundex ignores this flag since
                              the algorithm uppercases internally.
          - exact: equality (after optional lowering)
          - fuzzy: compute score, then `_apply_operator(score, operator, threshold)`
                   (operator defaults to ">=" when missing)
          - soundex: equality of the 4-char Soundex codes (no threshold)
          - allow_nulls: True  -> `null_match | base`
                          False -> `not_null   & base`
        """
        match_type       = cond.get("match_type", "exact")
        fuzzy_func       = cond.get("function")
        threshold        = cond.get("threshold")
        operator         = cond.get("operator") or ">="
        allow_nulls      = cond.get("allow_nulls", rule_allow_nulls)
        case_insensitive = cond.get("case_insensitive", False)
        sub_field        = cond.get("sub_field")
        if sub_field in (None, "", "None", "null"):
            sub_field = None

        n = len(col)

        # ── Complex-type handling (STRUCT / ARRAY) ───────────────────────
        # Coerce JSON-string values to dict/list, detect the attribute kind
        # from the data, and either (ARRAY) compute the mask directly or
        # (STRUCT) rewrite query_val/col to the scalar comparison form and fall
        # through to the scalar logic below. Mirrors build_condition_column.
        q_coerced = self._coerce_complex(query_val)
        col_coerced = col.apply(self._coerce_complex)

        _rep = q_coerced
        if _rep is None:
            for _v in col_coerced:
                if isinstance(_v, (dict, list)):
                    _rep = _v
                    break

        # ARRAY / ARRAY<STRUCT> → element-wise 'any match' or set membership.
        if isinstance(_rep, list):
            return self._array_condition_mask(
                q_coerced, col_coerced, sub_field, case_insensitive,
                match_type, fuzzy_func, threshold, operator, allow_nulls, n,
            )

        # STRUCT → sub-field scalar, or canonical JSON of the whole struct.
        if isinstance(_rep, dict):
            if sub_field is not None:
                query_val = self._struct_subfield(q_coerced, sub_field)
                col = col_coerced.apply(lambda v: self._struct_subfield(v, sub_field))
            else:
                query_val = (
                    json.dumps(q_coerced, sort_keys=True, default=str)
                    if isinstance(q_coerced, dict) else None
                )
                col = col_coerced.apply(
                    lambda v: json.dumps(v, sort_keys=True, default=str)
                    if isinstance(v, dict) else None
                )
            # fall through to the scalar comparison below with the rewrites.

        # ── Normalize source (single value) ──────────────────────────────
        # Empty/whitespace -> None; case is preserved here so case-sensitive
        # comparisons (case_insensitive=False) see the original string.
        if query_val is None:
            src = None
        else:
            s = str(query_val).strip()
            src = s if s else None

        # ── Normalize candidate column (per row) ─────────────────────────
        # Mirrors the Spark `F.when(F.trim(...) == "", null).otherwise(...)`
        # block but uses `.apply` for explicit per-cell handling — avoids
        # subtle pandas dtype/NA-propagation differences across versions.
        # Case is preserved here, then optionally folded below.
        def _normalize_cell(v):
            if v is None:
                return None
            try:
                if pd.isna(v):
                    return None
            except (TypeError, ValueError):
                pass  # non-scalar (rare) — fall through to str()
            s = str(v).strip()
            return s if s else None

        cand_clean = col.apply(_normalize_cell)
        cand_is_null = cand_clean.isna()

        # ── Apply case-folding for the comparison view ────────────────────
        # `src` / `cand_clean` keep original case (used by soundex, which
        # is inherently case-insensitive). `src_cmp` / `cand_cmp` are
        # lower-cased iff case_insensitive=True and are used for every
        # other comparison (exact, levenshtein, jaro_winkler).
        if case_insensitive:
            src_cmp = src.lower() if src is not None else None
            cand_cmp = cand_clean.apply(
                lambda v: v.lower() if v is not None else None
            )
        else:
            src_cmp = src
            cand_cmp = cand_clean

        # ── Base mask + scores per match_type ────────────────────────────
        if match_type == "exact":
            if src_cmp is None:
                base = pd.Series([False] * n, dtype=bool)
                scores = pd.Series([0.0] * n)
            else:
                # Exact match honors operator: "=" (default) or "!=".
                if operator == "!=":
                    # Mirror Spark: `NULL != x` evaluates to NULL (no-match),
                    # so a null candidate must NOT satisfy "!=". Only non-null
                    # candidates that differ from the source pass.
                    base = ((cand_cmp != src_cmp) & (~cand_is_null)).fillna(False).astype(bool)
                else:
                    base = (cand_cmp == src_cmp).fillna(False).astype(bool)
                scores = base.astype(float)

        elif match_type == "fuzzy":
            scores_list = []
            for v in cand_cmp:
                if src_cmp is None or pd.isna(v):
                    scores_list.append(0.0)
                    continue
                if fuzzy_func == "levenshtein_normalized":
                    max_len = max(len(src_cmp), len(v))
                    score = 1.0 if max_len == 0 else 1.0 - self._levenshtein_distance(src_cmp, v) / max_len
                elif fuzzy_func == "levenshtein_standard":
                    score = float(self._levenshtein_distance(src_cmp, v))
                elif fuzzy_func == "jaro_winkler":
                    score = self._jaro_winkler_similarity(src_cmp, v)
                elif fuzzy_func == "soundex":
                    # Soundex is case-insensitive by construction —
                    # call against the case-preserving values (no need
                    # to look at the folded view).
                    cand_orig = cand_clean.iloc[len(scores_list)]
                    score = 1.0 if self._soundex(src) == self._soundex(cand_orig) else 0.0
                else:
                    raise ValueError(f"Unsupported fuzzy function: {fuzzy_func!r}")
                scores_list.append(round(score, 4))
            scores = pd.Series(scores_list)

            if fuzzy_func == "soundex":
                # Soundex is binary; operator/threshold are ignored.
                base = (scores == 1.0)
            elif threshold is None:
                base = pd.Series([False] * n, dtype=bool)
            else:
                base = self._apply_operator(scores, operator, float(threshold))
                base = base.fillna(False).astype(bool)
        else:
            raise ValueError(f"Unsupported match_type: {match_type!r}")

        # ── Null handling (identical logic to Spark side) ────────────────
        src_is_null = src is None
        if allow_nulls:
            # null_match | base   — NULL on both sides counts as a match
            null_match = cand_is_null & src_is_null
            mask = (null_match | base).fillna(False).astype(bool)
            # If a row passed only via null_match (both sides NULL), the
            # similarity comparison never ran — but reporting score=0.0
            # alongside passed=True is misleading in the UI. Surface
            # score=1.0 on those rows so the response is internally
            # consistent ("matched -> high score").
            scores = scores.mask(null_match, 1.0)
        else:
            # not_null & base    — both sides must be non-NULL
            not_null = (~cand_is_null) & (not src_is_null)
            mask = (not_null & base).fillna(False).astype(bool)
            # Conversely: a row where one side is NULL can never pass
            # under allow_nulls=False, so its score is moot. Zero it for
            # consistency (some fuzzy paths leave a stray score from a
            # previous code path).
            scores = scores.mask(~not_null, 0.0)

        return mask, scores

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

            # `rule.allow_nulls` is a rule-level default; per-condition
            # `cond.allow_nulls` overrides it (mirroring the Spark side
            # where `build_condition_column` takes `rule_allow_nulls` as a
            # fallback).
            rule_allow_nulls = rule.get("allow_nulls", False)

            for cond in conditions:
                attr       = cond.get("attribute", "")
                match_type = cond.get("match_type", "exact")
                threshold  = cond.get("threshold")
                query_val  = query_record.get(attr)
                # cond.allow_nulls effective value (defaults to rule's, then False)
                eff_allow_nulls = cond.get("allow_nulls", rule_allow_nulls)

                col = df[attr] if attr in df.columns else pd.Series([None] * n)
                mask, scores = self._build_condition_mask(
                    cond, query_val, col, rule_allow_nulls=rule_allow_nulls,
                )

                cond_masks.append(mask)
                cond_scores.append(scores)
                cond_meta.append((
                    attr, match_type, cond.get("function"),
                    cond.get("operator"), threshold, eff_allow_nulls, query_val,
                ))

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
                        "query_value": query_val,
                        "master_value": master_val,
                        "score": float(cond_scores[j].iloc[i]),
                        "passed": passed,
                    })

                rules_eval_per_record[i].append({
                    "name": rule_name,
                    "matched": rule_matched,
                    "applied": is_applied,
                    "conditions_evaluation": cond_evals,
                })

        results = []
        for i in range(n):
            record_id = str(df["id"].iloc[i]) if "id" in df.columns else str(i)
            reason = f"Due to Match Rule: {applied_rule[i]}" if applied_rule[i] else "No rule matched"
            results.append({
                "id": record_id,
                "match": overall_result[i],
                "score": 1.0 if overall_result[i] == "MATCH" else 0.0,
                "reason": reason,
                "applied_rule": applied_rule[i],
                "rules_evaluation": rules_eval_per_record[i],
            })

        return results
