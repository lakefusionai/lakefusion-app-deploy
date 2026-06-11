"""enable_playground_feature_flag

Revision ID: a1b2c3d4e5f6
Revises: 8d593a92e3c6
Create Date: 2025-12-23

Enables the Match Maven Playground feature:
1. Adds ENABLE_PLAYGROUND_FEATURE flag
2. Updates default_prompt template with {attributes} placeholder
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import table, column
from sqlalchemy import String, Enum, Text, DateTime
from datetime import datetime

# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, None] = '8d593a92e3c6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Insert ENABLE_PLAYGROUND_FEATURE flag
    feature_flags = table(
        'feature_flags',
        column('name', String),
        column('status', Enum),
        column('description', Text),
        column('owner_team', String),
        column('created_at', DateTime),
        column('updated_at', DateTime),
        column('expires_at', DateTime),
    )

    op.bulk_insert(
        feature_flags,
        [
            {
                "name": "ENABLE_PLAYGROUND_FEATURE",
                "status": "INACTIVE",
                "description": "Enables Prompt Playground feature for testing LLM prompts in MatchMaven",
                "owner_team": "LakeFusion",
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "expires_at": None,
            }
        ]
    )

    # 2. Update default_prompt in db_config_properties
    # Uses {attributes} placeholder (not {attributes_format})
    op.execute("""
        UPDATE db_config_properties
        SET config_value = 'You are an expert in classifying two entities as matching or not matching.
You will be provided with a query entity and a list of possible entities.
You need to find the closest matching entity with a similarity score to indicate how similar they are.

Entity type: {entity}
Matching attributes: {attributes}

Take into account:
1. The attributes are not mutually exclusive.
2. The attributes are not exhaustive.
3. The attributes are not consistent.
4. The attributes are not complete.
5. The attributes can have typos.
6. Do not consider scores in the search results. Generate your own similarity scores.
7. If additional instructions are present, consider them too.{additional_instructions_clause}

Follow these rules for input and output:

**Input:**
- Query Entity Format:
  The query entity will be provided as a single string in the following format:
  {attributes}
  Each field is separated by a vertical bar (|).
  Note: Some columns can have aggregated values separated by (•)

- List of Possible Entities:
  A string containing comma-separated entries in this EXACT format:
  [entity_description, lakefusion_id, score], [entity_description, lakefusion_id, score], ...
  Example: [John | Smith | john@email.com | 555-1234, abc123def456, 0.85], [Jane | Doe | jane@email.com | 555-5678, xyz789ghi012, 0.75], ...
  WHERE:
  - entity_description: Pipe-separated fields (BEFORE first comma inside brackets)
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
3. The JSON list MUST contain EXACTLY {max_potential_matches} objects (one for each entity in the input list).
4. Each object must have these keys:
    a. id: String (the entity_description - text BEFORE first comma in brackets).
    b. score: Float (YOUR similarity score from 0.0 to 1.0 - NOT the vector score).
    c. reason: String (brief explanation for YOUR score).
    d. lakefusion_id: String (COPY the 32-char hex string between 1st and 2nd comma EXACTLY).
5. VERIFY each lakefusion_id is exactly 32 characters of a-f and 0-9.
6. Return EXACTLY {max_potential_matches} results, no more, no less.
7. Ensure scores are in descending order (highest similarity first).

Query entity: {query_entity}

List of Possible entities with vector search scores: {search_results}'
        WHERE config_key = 'default_prompt';
    """)


def downgrade() -> None:
    # Remove feature flag
    op.execute(
        "DELETE FROM feature_flags WHERE name='ENABLE_PLAYGROUND_FEATURE';"
    )
    # Note: We don't restore old default_prompt value as it may vary per environment
