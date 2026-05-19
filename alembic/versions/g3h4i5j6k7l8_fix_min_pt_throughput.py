"""fix min_provisioned_throughput to equal chunk_size where zero

Revision ID: g3h4i5j6k7l8
Revises: f2b436f4c2d5
Create Date: 2026-03-16 20:00:00.000000

Updates legacy PT models where min_provisioned_throughput is 0 to use
chunk_size as the minimum, preventing scale-to-zero issues.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'g3h4i5j6k7l8'
down_revision: Union[str, None] = 'f2b436f4c2d5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Set min_provisioned_throughput = chunk_size for legacy PT models where min is currently 0."""
    conn = op.get_bind()
    dialect = conn.dialect.name

    # Find legacy models where min_provisioned_throughput is 0
    if dialect == 'mysql':
        select_sql = """
            SELECT id, pt_config
            FROM pt_models_config
            WHERE pt_type = 'legacy'
            AND JSON_EXTRACT(pt_config, '$.min_provisioned_throughput') = 0
        """
    else:
        select_sql = """
            SELECT id, pt_config
            FROM pt_models_config
            WHERE pt_type = 'legacy'
            AND (pt_config->>'min_provisioned_throughput')::int = 0
        """

    results = conn.execute(sa.text(select_sql)).fetchall()

    for row in results:
        model_id = row[0]
        if dialect == 'mysql':
            conn.execute(
                sa.text("""
                    UPDATE pt_models_config
                    SET pt_config = JSON_SET(
                        pt_config,
                        '$.min_provisioned_throughput',
                        JSON_EXTRACT(pt_config, '$.chunk_size')
                    )
                    WHERE id = :id
                """),
                {"id": model_id}
            )
        else:
            conn.execute(
                sa.text("""
                    UPDATE pt_models_config
                    SET pt_config = jsonb_set(
                        pt_config::jsonb,
                        '{min_provisioned_throughput}',
                        (pt_config->>'chunk_size')::jsonb
                    )::json
                    WHERE id = :id
                """),
                {"id": model_id}
            )


def downgrade() -> None:
    """Revert min_provisioned_throughput back to 0 for affected legacy models."""
    conn = op.get_bind()
    dialect = conn.dialect.name

    if dialect == 'mysql':
        select_sql = """
            SELECT id
            FROM pt_models_config
            WHERE pt_type = 'legacy'
            AND JSON_EXTRACT(pt_config, '$.min_provisioned_throughput') = JSON_EXTRACT(pt_config, '$.chunk_size')
        """
    else:
        select_sql = """
            SELECT id
            FROM pt_models_config
            WHERE pt_type = 'legacy'
            AND (pt_config->>'min_provisioned_throughput') = (pt_config->>'chunk_size')
        """

    results = conn.execute(sa.text(select_sql)).fetchall()

    for row in results:
        if dialect == 'mysql':
            conn.execute(
                sa.text("""
                    UPDATE pt_models_config
                    SET pt_config = JSON_SET(
                        pt_config,
                        '$.min_provisioned_throughput',
                        0
                    )
                    WHERE id = :id
                """),
                {"id": row[0]}
            )
        else:
            conn.execute(
                sa.text("""
                    UPDATE pt_models_config
                    SET pt_config = jsonb_set(
                        pt_config::jsonb,
                        '{min_provisioned_throughput}',
                        '0'
                    )::json
                    WHERE id = :id
                """),
                {"id": row[0]}
            )
