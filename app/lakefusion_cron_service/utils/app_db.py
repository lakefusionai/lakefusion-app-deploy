from contextlib import contextmanager
import lakefusion_utility.utils.database as db_module
from lakefusion_utility.utils.logging_utils import get_logger

logger = get_logger(__name__)


def enable_pool_pre_ping():
    engine_obj = getattr(db_module, "engine", None)
    pool = getattr(engine_obj, "pool", None)
    if pool is None:
        return False

    # pool._pre_ping is a private SQLAlchemy attribute.  There is no public API
    # to enable pre-ping after engine creation, and the engine lives in the
    # external lakefusion_utility wheel so we cannot pass pool_pre_ping=True to
    # create_engine().  Verified working on SQLAlchemy 2.0.34 (see requirements.txt).
    # The AttributeError guard below ensures a future rename degrades to a loud
    # startup warning rather than a silent crash.
    try:
        pool._pre_ping = True
    except AttributeError:
        import sqlalchemy
        logger.warning(
            "enable_pool_pre_ping: pool._pre_ping not found — "
            "pre-ping skipped (SQLAlchemy %s; API may have changed)",
            sqlalchemy.__version__,
        )
        return False
    return True

@contextmanager
def db_context():
    session = None
    try:
        session = db_module.SessionLocal()
        yield session
    except Exception:
        if session is not None:
            session.rollback()
        # Do not log here; let callers (job_wrapper) log with job context.
        raise
    finally:
        if session is not None:
            session.close()

engine = db_module.engine
get_db = db_module.get_db
token_required_wrapper = db_module.token_required_wrapper