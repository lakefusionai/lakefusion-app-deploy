from contextlib import contextmanager
import lakefusion_utility.utils.database as db_module

@contextmanager
def db_context():
    with db_module.SessionLocal() as session:
        try:
            yield session
        finally:
            session.close()

engine = db_module.engine
get_db = db_module.get_db
token_required_wrapper = db_module.token_required_wrapper