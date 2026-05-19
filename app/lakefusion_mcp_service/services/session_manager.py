"""
Session Manager for MCP Service.
Manages per-user authentication sessions with OAuth state mapping.
"""
from typing import Optional, Dict, Any
from datetime import datetime, timedelta, timezone
import httpx

from app.lakefusion_mcp_service.utils.logger import get_logger, log_to_stderr
from app.lakefusion_mcp_service.models import AuthState

logger = get_logger(__name__)


class SSENotificationManager:
    """Manages MCP session storage for sending notifications outside of tool context."""

    def __init__(self):
        self._mcp_sessions: Dict[str, Any] = {}
        log_to_stderr("[NOTIFICATION] SSENotificationManager initialized")

    def register_mcp_session(self, session_id: str, mcp_session):
        """Store MCP ServerSession for later notifications."""
        self._mcp_sessions[session_id] = mcp_session
        log_to_stderr(f"[NOTIFICATION] Registered MCP session: {session_id[:8]}...")

    def unregister_mcp_session(self, session_id: str):
        """Remove MCP session."""
        self._mcp_sessions.pop(session_id, None)
        log_to_stderr(f"[NOTIFICATION] Unregistered MCP session: {session_id[:8]}...")

    def get_session(self, session_id: str) -> Any:
        """Get stored MCP session."""
        return self._mcp_sessions.get(session_id)

    async def send_log_message(
        self,
        session_id: str,
        level: str,
        message: str,
        data: dict = None
    ) -> bool:
        """
        Send log message to MCP client via stored session.

        Args:
            session_id: The session ID to send notification to
            level: Log level - "debug", "info", "warning", "error"
            message: The log message
            data: Optional additional data to include

        Returns:
            True if notification was sent successfully, False otherwise
        """
        session = self._mcp_sessions.get(session_id)
        if not session:
            log_to_stderr(f"[NOTIFICATION] No session found for {session_id[:8]}...")
            return False

        try:
            # Build the data dict with message and any extra fields
            log_data = {"message": message}
            if data:
                log_data.update(data)

            await session.send_log_message(
                level=level,
                data=log_data,
                logger="lakefusion-auth"
            )
            log_to_stderr(f"[NOTIFICATION] Sent {level} message to {session_id[:8]}...")
            return True
        except Exception as e:
            log_to_stderr(f"[NOTIFICATION] Failed to send: {e}", "ERROR")
            logger.error(f"Failed to send notification to {session_id[:8]}...: {e}")
            return False


class SessionManager:
    """
    Manages authentication sessions for MCP clients.

    Each SSE connection gets a unique session, and OAuth state parameters
    are mapped to sessions to complete the authentication flow.
    """

    SESSION_TTL = timedelta(hours=24)  # Sessions expire after 24h

    def __init__(self):
        self.sessions: Dict[str, AuthState] = {}
        self.session_created_at: Dict[str, datetime] = {}
        self.state_to_session: Dict[str, str] = {}
        self.http_clients: Dict[str, httpx.AsyncClient] = {}
        log_to_stderr("[SESSION] SessionManager initialized")
        logger.info("SessionManager initialized")

    def get_or_create_session(self, session_id: str) -> AuthState:
        """Get existing session or create a new one."""
        if session_id not in self.sessions:
            self.sessions[session_id] = AuthState()
            self.session_created_at[session_id] = datetime.now(timezone.utc)
            log_to_stderr(f"[SESSION] New client session created: {session_id[:8]}...")
            logger.info(f"Created new session: {session_id}")
        return self.sessions[session_id]

    def get_session(self, session_id: str) -> Optional[AuthState]:
        """Get existing session without creating a new one."""
        return self.sessions.get(session_id)

    def get_or_create_client(self, session_id: str) -> httpx.AsyncClient:
        """Get or create an HTTP client for a session."""
        if session_id not in self.http_clients:
            self.http_clients[session_id] = httpx.AsyncClient(timeout=30.0)
        return self.http_clients[session_id]

    def map_state_to_session(self, state: str, session_id: str):
        """Map an OAuth state parameter to a session ID for callback handling."""
        self.state_to_session[state] = session_id
        logger.debug(f"Mapped OAuth state {state[:8]}... to session {session_id[:8]}...")

    def consume_state(self, state: str) -> Optional[str]:
        """
        Get session ID from state and remove mapping (single-use).
        Prevents replay attacks by ensuring state can only be used once.
        """
        session_id = self.state_to_session.pop(state, None)
        if session_id:
            logger.debug(f"Consumed OAuth state {state[:8]}... for session {session_id[:8]}...")
        return session_id

    async def cleanup_session(self, session_id: str):
        """Clean up a session and its associated resources."""
        # Close HTTP client if exists
        if session_id in self.http_clients:
            await self.http_clients[session_id].aclose()
            del self.http_clients[session_id]

        # Remove session
        if session_id in self.sessions:
            # Clean up state mappings for this session
            states_to_remove = [
                state for state, sid in self.state_to_session.items()
                if sid == session_id
            ]
            for state in states_to_remove:
                del self.state_to_session[state]

            del self.sessions[session_id]
            log_to_stderr(f"[SESSION] Session cleaned up: {session_id[:8]}...")
            logger.info(f"Cleaned up session: {session_id}")
    
    async def cleanup_expired_sessions(self):
        """Remove sessions older than TTL."""
        now = datetime.utcnow()
        expired = [
            sid for sid, created in self.session_created_at.items()
            if now - created > self.SESSION_TTL
        ]
        for sid in expired:
            await self.cleanup_session(sid)
        
        if expired:
            logger.info(f"Cleaned up {len(expired)} expired sessions")

    def get_active_session_count(self) -> int:
        """Get the number of active sessions."""
        return len(self.sessions)

    def get_authenticated_session_count(self) -> int:
        """Get the number of authenticated sessions."""
        return sum(
            1 for session in self.sessions.values()
            if session.access_token is not None
        )


# Global singleton instance
session_manager = SessionManager()

sse_notifications = SSENotificationManager()

def get_session_manager() -> SessionManager:
    """Get the global SessionManager instance."""
    return session_manager
