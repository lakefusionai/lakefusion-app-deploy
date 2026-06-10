import os
import sys
from logging.config import fileConfig
from sqlalchemy import inspect
from alembic import context
from dotenv import load_dotenv

from lakefusion_utility.utils.logging_utils import get_logger,init_logger

init_logger(service="cron_alembic_service")
logger = get_logger(__name__)

# --------------------------------------------------------
# STEP 1 — Load correct .env file
# --------------------------------------------------------
load_dotenv('.env')

# --------------------------------------------------------
# STEP 2 — Add app path & import database.py
# --------------------------------------------------------
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from lakefusion_utility.utils.app_db import Base
from lakefusion_utility.utils import database
import lakefusion_utility.models  # noqa

# --------------------------------------------------------
# STEP 3 — Set up Alembic Config
# --------------------------------------------------------
config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name, disable_existing_loggers=False)

target_metadata = Base.metadata

# --------------------------------------------------------
# STEP 4 — Build DB URL
# --------------------------------------------------------
database.init_engine()
db_url = str(database.url)
safe_url = db_url.replace("%", "%%")
config.set_main_option("sqlalchemy.url", safe_url)


# --------------------------------------------------------
# STEP 4.5 — Patch Alembic ops for idempotency  🔒
# --------------------------------------------------------
from alembic import op
from functools import wraps

def patch_safe_ops():
    """Patch Alembic ops for idempotency.
    Uses op.get_bind() at runtime (inside each migration step) to get a fresh
    inspector from the migration's own connection — avoids deadlocks on PostgreSQL.
    """

    def safe_wrap(fn_name, checker):
        orig_fn = getattr(op, fn_name)

        @wraps(orig_fn)
        def wrapper(*args, **kwargs):
            # Get inspector from migration connection at call time (not at patch time)
            insp = inspect(op.get_bind())
            if checker(insp, *args, **kwargs):
                logger.info(f"op.{fn_name} skipped: already satisfied")
                return None
            return orig_fn(*args, **kwargs)

        setattr(op, fn_name, wrapper)

    # Existence checkers — receive inspector as first arg
    def table_exists(insp, name, *_, **__): return name in insp.get_table_names()
    def table_not_exists(insp, name, *_, **__): return name not in insp.get_table_names()
    def index_exists(insp, name, table_name=None, *_, **__):
        return table_name and any(i["name"] == name for i in insp.get_indexes(table_name))
    def index_not_exists(insp, name, table_name=None, **__):
        return not index_exists(insp, name, table_name)
    def constraint_exists(insp, name, table_name=None, *_, **__):
        if not table_name: return False
        fks = [fk["name"] for fk in insp.get_foreign_keys(table_name)]
        uqs = [uc["name"] for uc in insp.get_unique_constraints(table_name)]
        return name in (fks + uqs)
    def constraint_not_exists(insp, name, table_name=None, **__):
        return not constraint_exists(insp, name, table_name)
    def column_exists(insp, table_name, col, **__):
        return any(c["name"] == col for c in insp.get_columns(table_name))
    def column_not_exists(insp, table_name, col, **__):
        return not column_exists(insp, table_name, col)

    # Patch relevant ops
    safe_wrap("create_table", table_exists)
    safe_wrap("drop_table", table_not_exists)
    safe_wrap("create_index", index_exists)
    safe_wrap("drop_index", index_not_exists)
    safe_wrap("create_unique_constraint", constraint_exists)
    safe_wrap("drop_constraint", constraint_not_exists)
    safe_wrap("create_foreign_key", constraint_exists)
    safe_wrap("add_column", lambda insp, table_name, col, **__: column_exists(insp, table_name, col.name))
    safe_wrap("drop_column", lambda insp, table_name, col, **__: column_not_exists(insp, table_name, col))

    logger.info("Alembic op.* patched for idempotency (env.py).")

patch_safe_ops()  # run patch once here

# --------------------------------------------------------
# STEP 5 — Run migrations
# --------------------------------------------------------
def run_migrations_offline():
    logger.info("Running migrations in offline mode")
    context.configure(
        url=db_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online():
    logger.info("Running migrations in online mode")
    connectable = database.engine
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()