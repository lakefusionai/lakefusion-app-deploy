"""Seed RBAC roles and permissions_definition

Revision ID: d3e4f5a6b7c8
Revises: r3s4t5u6v7w8
Create Date: 2026-02-14 10:00:00.000000

Creates the three default RBAC roles (if they don't exist) and populates
their ``permissions_definition`` JSON column:

- Admin:        read, create, update, delete
- Developer:    read, create, update
- Data Steward: read

Schema: ``{"actions": ["read", "create", "update", "delete"]}``
"""

from typing import Sequence, Union
import json
from datetime import datetime, timezone
from alembic import op
import sqlalchemy as sa


revision: str = "d3e4f5a6b7c8"
down_revision: Union[str, None] = "r3s4t5u6v7w8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_DEFAULT_ROLES = [
    {"name": "Admin", "description": "Full access — read, create, update, delete", "permissions": {"actions": ["read", "create", "update", "delete"]}},
    {"name": "Developer", "description": "Read, create, and update access", "permissions": {"actions": ["read", "create", "update"]}},
    {"name": "Data Steward", "description": "Read-only access", "permissions": {"actions": ["read"]}},
]


def _normalize_role_name(role_name: str) -> str:
    return " ".join((role_name or "").strip().lower().split())


def _roles_table_exists(conn) -> bool:
    inspector = sa.inspect(conn)
    return "roles" in inspector.get_table_names()


def upgrade() -> None:
    conn = op.get_bind()

    if not _roles_table_exists(conn):
        print("  → Table 'roles' does not exist, skipping")
        return

    now = datetime.now(timezone.utc)

    # Step 1: Insert roles if they don't exist
    for role_def in _DEFAULT_ROLES:
        existing = conn.execute(
            sa.text("SELECT id FROM roles WHERE name = :name"),
            {"name": role_def["name"]},
        ).fetchone()

        if not existing:
            conn.execute(
                sa.text(
                    "INSERT INTO roles (name, description, permissions_definition, created_at) "
                    "VALUES (:name, :desc, :perms, :ts)"
                ),
                {
                    "name": role_def["name"],
                    "desc": role_def["description"],
                    "perms": json.dumps(role_def["permissions"]),
                    "ts": now,
                },
            )
            print(f"  ✓ Created role '{role_def['name']}' with permissions: {role_def['permissions']}")
        else:
            # Step 2: Update permissions_definition if role already exists
            conn.execute(
                sa.text(
                    "UPDATE roles SET permissions_definition = :perms WHERE id = :role_id"
                ),
                {"perms": json.dumps(role_def["permissions"]), "role_id": existing[0]},
            )
            print(f"  ✓ Updated permissions_definition for '{role_def['name']}'")

    print("\n✅ RBAC roles seeded")


def downgrade() -> None:
    conn = op.get_bind()

    if not _roles_table_exists(conn):
        print("  → Table 'roles' does not exist, skipping rollback")
        return

    for role_def in _DEFAULT_ROLES:
        normalized = _normalize_role_name(role_def["name"])
        conn.execute(
            sa.text("UPDATE roles SET permissions_definition = NULL WHERE LOWER(name) = :name"),
            {"name": normalized},
        )

    print("✅ Rollback: Cleared permissions_definition on default roles")
