from fastapi import FastAPI
from sqlalchemy.orm import Session
import logging

class DBCleanupMiddleware:
    def __init__(self, app: FastAPI, db_session: Session):
        self.app = app
        self.db_session = db_session
        
    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        async def cleanup_send(message):
            if message["type"] == "http.response.start":
                try:
                    if hasattr(self.db_session, 'close'):
                        self.db_session.close()
                    elif hasattr(self.db_session, 'session'):
                        self.db_session.session.close()
                    logging.info("DB session cleaned up")
                except Exception as e:
                    logging.error(f"Error cleaning up DB session: {e}")
            await send(message)

        return await self.app(scope, receive, cleanup_send)