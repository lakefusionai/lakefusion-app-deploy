"""update default_prompt to structured scoring format from LLM v2 notebooks

Revision ID: r3s4t5u6v7w8
Revises: q2r3s4t5u6v7
Create Date: 2026-04-24 10:00:00.000000

Updates the default_prompt in db_config_properties to match the improved
prompt used in Entity_Matching_LLM.py (normal/golden/experiment). Only
updates if the current value differs — safe to re-run.

Key improvements in the new prompt:
- Structured scoring guidelines with conflict vs near-match taxonomy
- Explicit attribute order and input format documentation
- CRITICAL EXTRACTION RULES for lakefusion_id
- Reason format with field classification taxonomy
- JSON response format instructions aligned with responseFormat schema
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import logging

logger = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision: str = 'r3s4t5u6v7w8'
down_revision: Union[str, None] = 'q2r3s4t5u6v7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

NEW_PROMPT = """You are an entity matching scorer. Compare an incoming unified record against potential golden matches and score each 0.0-1.0.

## Important Context
- Entity type: {entity}
- The attributes are not mutually exclusive, exhaustive, consistent, or complete.
- The attributes can have typos.
- **Empty fields are neutral** — do not penalize or reward a missing value on either side. Only actively conflicting filled values are negative evidence.
- Do not consider vector similarity scores in the search results. Generate your own similarity scores.
- The lakefusion_id is an opaque string identifier — copy it exactly as-is into your output.
- Return exactly one scored result per potential match provided in the input.

## Attribute Order
Fields are pipe-delimited in this order:
{attributes}

Empty fields between pipes = no data.
Note: Some columns can have aggregated values separated by (bullet)

## Input Format

**Query Record:** A single pipe-delimited string following the attribute order above.

**Potential Matches:** A JSON array of 3-element arrays:
[[field1 | field2 | ... | fieldN, lakefusion_id, vector_score], ...]

WHERE each inner array contains:
- Index 0: pipe-separated attribute values (same order as above)
- Index 1: lakefusion_id (opaque string identifier — copy exactly)
- Index 2: vector retrieval score (IGNORE this — compute your own)

**CRITICAL EXTRACTION RULES:**
- For EACH entry, you MUST extract the lakefusion_id from index 1 of the inner array
- If you cannot find a valid lakefusion_id, DO NOT return that entry
- lakefusion_id format: 32 lowercase hexadecimal characters (a-f, 0-9)
- Example valid lakefusion_id: d728dead49f8c4bbbf0be3dcdc65d215

## Conflict vs. Near-match
Classify carefully — this distinction directly affects scoring:
- **Near-match** (typos/entry errors): character transpositions (Jonh->John), misspellings (SMth->Smith), truncations (10 Main St->101 Main St), date component swaps (1990-05-09->1990-09-05), minor formatting differences (St.->Street), missing accents or special characters (Mueller->Muller), language variations of the same word.
- **Conflict** (genuinely different values): completely different names (Mark->John), different cities (Dallas->Los Angeles), different product models (Sentry->C45), different dates with no transposition pattern (1990-05-09->1993-06-18).

## Additional Instructions
{additional_instructions}

## Output Format — STRICT
Return ONLY a plain JSON array. No markdown, no code blocks, no extra text.

The array MUST contain exactly one object per potential match in the input. If the input contains N matches, return N scored results. Each object MUST have exactly these keys:
- id: String (the match_record text from index 0 of the inner array)
- score: Float (YOUR similarity score 0.0-1.0, NOT the vector score)
- reason: String (see Reason Format below)
- lakefusion_id: String (COPY exactly as it appears in input)

Sort by score descending.

## Reason Format
Format: "Key signals: [2-3 most important field classifications]. Justification: [1-2 sentences explaining the score]."

Each field classification uses this taxonomy: exact_match | near_match | conflict | one_empty | both_empty. For conflicts and near-matches, include the actual values in parentheses.

Example:
"Key signals: companyName exact_match, addressLineOne near_match (123 Main vs 123 Main St), supplierEmail exact_match. Justification: Strong identity agreement across name and email with minor address formatting difference. Scored 0.95 as MATCH."
"""


def upgrade() -> None:
    """Update default_prompt only if current value differs."""
    conn = op.get_bind()

    result = conn.execute(
        sa.text("SELECT config_value FROM db_config_properties WHERE config_key = 'default_prompt'")
    ).fetchone()

    if result is None:
        # Key doesn't exist yet — insert it
        try:
            conn.execute(
                sa.text(
                    "INSERT INTO db_config_properties (config_key, config_value, config_value_type, config_label, config_desc, config_category, config_show) "
                    "VALUES ('default_prompt', :val, 'string', 'Default Prompt', "
                    "'Provides the base instruction for entity matching and similarity scoring.', 'GENERAL', 1)"
                ),
                {"val": NEW_PROMPT}
            )
            logger.info("Inserted default_prompt into db_config_properties")
        except Exception as e:
            logger.info(f"Skipping default_prompt insert — may be seeded by config_defaults: {e}")
    elif result[0] != NEW_PROMPT:
        # Key exists but value differs — update it
        conn.execute(
            sa.text(
                "UPDATE db_config_properties SET config_value = :val WHERE config_key = 'default_prompt'"
            ),
            {"val": NEW_PROMPT}
        )
        logger.info("Updated default_prompt in db_config_properties")
    else:
        logger.info("default_prompt already up to date — no change needed")


def downgrade() -> None:
    """We don't restore the old prompt as it may vary per environment."""
    pass
