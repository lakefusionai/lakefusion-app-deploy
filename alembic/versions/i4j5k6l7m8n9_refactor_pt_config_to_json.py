"""create and seed pt_models_config with JSON schema

Revision ID: i4j5k6l7m8n9
Revises: 64a5cc16b57e
Create Date: 2026-02-12 16:45:00.000000
"""

from typing import Sequence, Union
from datetime import datetime
import json

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "i4j5k6l7m8n9"
down_revision: Union[str, None] = "64a5cc16b57e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """
    Create pt_models_config table and seed initial PT-supported models.
    Supports both MySQL and PostgreSQL.
    """

    conn = op.get_bind()
    dialect = conn.dialect.name

    # ------------------------------------------------------------------
    # DB-specific defaults
    # ------------------------------------------------------------------

    if dialect == "mysql":
        updated_at_default = sa.text(
            "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
        )
    else:
        updated_at_default = sa.text("CURRENT_TIMESTAMP")

    # ------------------------------------------------------------------
    # Create table
    # ------------------------------------------------------------------

    op.create_table(
        "pt_models_config",

        sa.Column(
            "id",
            sa.Integer(),
            primary_key=True,
            autoincrement=True
        ),

        sa.Column(
            "entity_name",
            sa.String(255),
            nullable=False,
            unique=True
        ),

        sa.Column(
            "entity_version",
            sa.String(10),
            nullable=False,
            server_default="1"
        ),

        sa.Column(
            "pt_type",
            sa.Enum(
                "model_units",
                "legacy",
                name="pttype"
            ),
            nullable=False
        ),

        sa.Column(
            "model_category",
            sa.Enum(
                "llm",
                "embedding",
                name="modelcategory"
            ),
            nullable=False
        ),

        sa.Column(
            "pt_config",
            sa.JSON(),
            nullable=False
        ),

        sa.Column(
            "is_active",
            sa.Boolean(),
            nullable=False,
            server_default=sa.true()
        ),

        sa.Column(
            "created_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP")
        ),

        sa.Column(
            "updated_at",
            sa.DateTime(),
            nullable=False,
            server_default=updated_at_default
        ),
    )

    # ------------------------------------------------------------------
    # Create indexes
    # ------------------------------------------------------------------

    op.create_index(
        "ix_pt_models_config_entity_name",
        "pt_models_config",
        ["entity_name"]
    )

    op.create_index(
        "ix_pt_models_config_is_active",
        "pt_models_config",
        ["is_active"]
    )

    # ------------------------------------------------------------------
    # Seed data
    # ------------------------------------------------------------------

    now = datetime.utcnow()

    seed_data = [

        # --------------------------------------------------------------
        # Model Units PT models
        # --------------------------------------------------------------

        {
            "entity_name": "system.ai.gpt-oss-120b",
            "entity_version": "1",
            "pt_type": "model_units",
            "model_category": "llm",
            "pt_config": {
                "chunk_size": 50,
                "provisioned_model_units": 50
            },
            "is_active": True,
            "created_at": now,
            "updated_at": now,
        },

        {
            "entity_name": "system.ai.gpt-oss-20b",
            "entity_version": "1",
            "pt_type": "model_units",
            "model_category": "llm",
            "pt_config": {
                "chunk_size": 50,
                "provisioned_model_units": 50
            },
            "is_active": True,
            "created_at": now,
            "updated_at": now,
        },

        {
            "entity_name": "system.ai.gemma-3-12b-it",
            "entity_version": "1",
            "pt_type": "model_units",
            "model_category": "llm",
            "pt_config": {
                "chunk_size": 50,
                "provisioned_model_units": 50
            },
            "is_active": True,
            "created_at": now,
            "updated_at": now,
        },

        {
            "entity_name": "system.ai.llama-4-maverick",
            "entity_version": "1",
            "pt_type": "model_units",
            "model_category": "llm",
            "pt_config": {
                "chunk_size": 50,
                "provisioned_model_units": 50
            },
            "is_active": True,
            "created_at": now,
            "updated_at": now,
        },

        # --------------------------------------------------------------
        # Legacy PT models
        # --------------------------------------------------------------

        {
            "entity_name": "system.ai.llama_v3_3_70b_instruct",
            "entity_version": "1",
            "pt_type": "legacy",
            "model_category": "llm",
            "pt_config": {
                "chunk_size": 1580,
                "min_provisioned_throughput": 0,
                "max_provisioned_throughput": 3160
            },
            "is_active": True,
            "created_at": now,
            "updated_at": now,
        },

        {
            "entity_name": "system.ai.gte_large_en_v1_5",
            "entity_version": "1",
            "pt_type": "legacy",
            "model_category": "embedding",
            "pt_config": {
                "chunk_size": 19000,
                "min_provisioned_throughput": 0,
                "max_provisioned_throughput": 38000
            },
            "is_active": True,
            "created_at": now,
            "updated_at": now,
        },
    ]

    # ------------------------------------------------------------------
    # Insert seed data (idempotent)
    # ------------------------------------------------------------------

    for row in seed_data:

        # --------------------------------------------------------------
        # MySQL
        # --------------------------------------------------------------

        if dialect == "mysql":

            query = sa.text("""
                INSERT IGNORE INTO pt_models_config
                (
                    entity_name,
                    entity_version,
                    pt_type,
                    model_category,
                    pt_config,
                    is_active,
                    created_at,
                    updated_at
                )
                VALUES
                (
                    :entity_name,
                    :entity_version,
                    :pt_type,
                    :model_category,
                    :pt_config,
                    :is_active,
                    :created_at,
                    :updated_at
                )
            """)

        # --------------------------------------------------------------
        # PostgreSQL
        # --------------------------------------------------------------

        elif dialect == "postgresql":

            query = sa.text("""
                INSERT INTO pt_models_config
                (
                    entity_name,
                    entity_version,
                    pt_type,
                    model_category,
                    pt_config,
                    is_active,
                    created_at,
                    updated_at
                )
                VALUES
                (
                    :entity_name,
                    :entity_version,
                    :pt_type,
                    :model_category,
                    :pt_config,
                    :is_active,
                    :created_at,
                    :updated_at
                )
                ON CONFLICT (entity_name)
                DO NOTHING
            """)

        else:
            raise Exception(f"Unsupported dialect: {dialect}")

        # Convert JSON dict to string for raw SQL execution
        insert_row = dict(row)
        insert_row["pt_config"] = json.dumps(row["pt_config"])

        conn.execute(query, insert_row)


def downgrade() -> None:
    """Drop pt_models_config table."""

    op.drop_index(
        "ix_pt_models_config_is_active",
        table_name="pt_models_config"
    )

    op.drop_index(
        "ix_pt_models_config_entity_name",
        table_name="pt_models_config"
    )

    op.drop_table("pt_models_config")