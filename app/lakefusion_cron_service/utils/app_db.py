from contextlib import contextmanager
import lakefusion_utility.utils.database as db_module
from lakefusion_utility.utils.logging_utils import get_logger

logger = get_logger(__name__)

@contextmanager
def db_context():
    session = None
    try:
        session = db_module.SessionLocal()
        yield session
    except Exception:
        # Do not log here; let callers (job_wrapper) log with job context.
        raise
    finally:
        if session is not None:
            session.close()

engine = db_module.engine
get_db = db_module.get_db
token_required_wrapper = db_module.token_required_wrapper