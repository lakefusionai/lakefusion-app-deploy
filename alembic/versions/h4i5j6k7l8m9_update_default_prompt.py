"""update default_prompt to match LLM Experiment notebook

Revision ID: h4i5j6k7l8m9
Revises: g3h4i5j6k7l8
Create Date: 2026-03-17 10:00:00.000000

Updates the default_prompt in db_config_properties to match the prompt
used in Entity_Matching_LLM_Experiment.py, only if the current value
differs.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import logging

logger = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision: str = 'h4i5j6k7l8m9'
down_revision: Union[str, None] = 'g3h4i5j6k7l8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

NEW_PROMPT = """You are an expert in classifying two records as matching or not matching.
You will be provided with a golden record and a list of potential matches.
You need to find the closest matching record with a similarity score to indicate how similar they are.
Take into account:
1. The attributes are not mutually exclusive.
2. The attributes are not exhaustive.
3. The attributes are not consistent.
4. The attributes are not complete.
5. The attributes can have typos.
6. The entity type being considered is: {entity}.
7. Do not consider scores in the search results. Generate your own similarity scores.
8. If additional instructions are present, consider them too.{additional_instructions_clause}
Follow these rules for input and output:
**Input:**
- Golden Record Format:
  The golden record will be provided as a single string in the following format:
  {attributes}
  Each field is separated by a vertical bar (|).
  Note: Some columns can have aggregated values separated by (bullet)
- List of Potential Matches:
  A string containing comma-separated entries in this EXACT format:
  [match_record, lakefusion_id, score], [match_record, lakefusion_id, score], ...
  Example: [John | Smith | john@email.com | 555-1234, abc123def456, 0.85], [Jane | Doe | jane@email.com | 555-5678, xyz789ghi012, 0.75], ...
  WHERE:
  - match_record: Pipe-separated fields (BEFORE first comma inside brackets)
  - lakefusion_id: 32-character hexadecimal string (BETWEEN first and second comma)
  - score: Decimal number (AFTER second comma, before closing bracket)
**CRITICAL EXTRACTION RULES:**
- For EACH entry in the list, you MUST extract the lakefusion_id
- The lakefusion_id is ALWAYS between the FIRST comma and SECOND comma inside each bracket
- If you cannot find a valid lakefusion_id, DO NOT return that entry
- lakefusion_id format: 32 lowercase hexadecimal characters (a-f, 0-9)
- Example valid lakefusion_id: d728dead49f8c4bbbf0be3dcdc65d215
**Output:**
Only return a plain JSON list of objects:
1. No extra text, no headers, no introductory phrases, and no comments.
2. Do not use code blocks, markdown, or any other formatting.
3. The JSON list MUST contain EXACTLY {max_potential_matches} objects (one for each potential match in the input list).
4. Each object must have these keys:
    a. id: String (the match_record - text BEFORE first comma in brackets).
    b. score: Float (YOUR similarity score from 0.0 to 1.0 - NOT the vector score).
    c. reason: String (brief explanation for YOUR score, comparing to the golden record).
    d. lakefusion_id: String (COPY the 32-char hex string between 1st and 2nd comma EXACTLY).
5. VERIFY each lakefusion_id is exactly 32 characters of a-f and 0-9.
6. Return EXACTLY {max_potential_matches} results, no more, no less.
7. Ensure scores are in descending order (highest similarity first)."""


def upgrade() -> None:
    """Update default_prompt only if current value differs."""
    conn = op.get_bind()

    result = conn.execute(
        sa.text("SELECT config_value FROM db_config_properties WHERE config_key = 'default_prompt'")
    ).fetchone()

    if result is None:
        # Key doesn't exist yet — use savepoint so a failed INSERT doesn't abort
        # the entire PG transaction (MySQL auto-recovers, PG does not)
        nested = conn.begin_nested()
        try:
            conn.execute(
                sa.text(
                    "INSERT INTO db_config_properties (config_key, config_value, config_value_type, config_label, config_desc, config_category, config_show) "
                    "VALUES ('default_prompt', :val, 'STRING', 'Default Prompt', "
                    "'Provides the base instruction for entity matching and similarity scoring.', 'GENERAL', 1)"
                ),
                {"val": NEW_PROMPT}
            )
            nested.commit()
        except Exception as e:
            nested.rollback()
            logger.info(f"Skipping default_prompt insert — will be seeded by config_defaults: {e}")
    elif result[0] != NEW_PROMPT:
        # Key exists but value differs, update it
        conn.execute(
            sa.text(
                "UPDATE db_config_properties SET config_value = :val WHERE config_key = 'default_prompt'"
            ),
            {"val": NEW_PROMPT}
        )


def downgrade() -> None:
    """We don't restore the old prompt as it may vary per environment."""
    pass
