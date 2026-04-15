import re
import importlib.util
import traceback
import sys
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from alembic.runtime.migration import MigrationContext

from sqlalchemy import text, inspect
from sqlalchemy.exc import SQLAlchemyError

from lakefusion_utility.utils import database
from lakefusion_utility.utils.logging_utils import get_logger
logger = get_logger(__name__)

# ----------------------------
# Alembic / Engine bootstrap
# ----------------------------
alembic_cfg = Config("alembic.ini")

def _log_error_details(e: Exception) -> None:
    """Log DB error with essential details only."""
    if isinstance(e, SQLAlchemyError):
        # Extract the root database error
        orig = getattr(e, "orig", None)
        if orig is not None:
            logger.error(f"Database error: {orig}")
        else:
            logger.error(f"SQLAlchemy error: {e}")
        
        # Log the actual SQL statement that failed
        stmt = getattr(e, "statement", None)
        if stmt:
            logger.error(f"Failed SQL: {stmt}")
    else:
        logger.error(f"Migration error: {type(e).__name__}: {e}")
    
    # Full traceback only at debug level
    logger.debug("Full traceback:", exc_info=True)

# ----------------------------
# Main migration flow
# ----------------------------
def run_migrations() -> None:
    try:
        logger.info("Starting database migration upgrade to head")
        command.upgrade(alembic_cfg, "head")
        logger.info("Database migration upgrade completed successfully")
    except Exception as e:
        logger.error("Database migration upgrade failed")
        _log_error_details(e)
        # Print concise error to console
        print(f"❌ Migration failed: {e}")
        if isinstance(e, SQLAlchemyError):
            orig = getattr(e, "orig", None)
            if orig:
                print(f"Database error: {orig}")
            stmt = getattr(e, "statement", None)
            if stmt:
                print(f"Failed SQL: {stmt}")
        sys.exit(1)  # Exit with error code, no traceback


# ----------------------------
# Developer-only downgrade
# ----------------------------
def dev_downgrade(step: str = "-1") -> None:
    """
    Developer-only manual downgrade.
    Use only in DEV/ITG environments, never in PROD.
    """
    logger.warning(f"DEV downgrade requested: {step}")
    try:
        command.downgrade(alembic_cfg, step)
        logger.info(f"Downgrade {step} executed successfully")
    except Exception as e:
        logger.error(f"Dev downgrade failed for step {step}")
        _log_error_details(e)
        print(f"❌ Downgrade failed: {e}")
        if isinstance(e, SQLAlchemyError):
            orig = getattr(e, "orig", None)
            if orig:
                print(f"Database error: {orig}")
            stmt = getattr(e, "statement", None)
            if stmt:
                print(f"Failed SQL: {stmt}")
        sys.exit(1)


# ----------------------------
# CLI entry
# ----------------------------
if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "dev_downgrade":
        step = sys.argv[2] if len(sys.argv) > 2 else "-1"
        dev_downgrade(step)
    else:
        run_migrations()