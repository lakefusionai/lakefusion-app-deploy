from typing import Optional
from datetime import datetime
from pydantic import BaseModel


class AuthState(BaseModel):
    """Authentication state for a user session."""
    access_token: Optional[str] = None
    refresh_token: Optional[str] = None
    token_expiry: Optional[datetime] = None
    username: Optional[str] = None
    auth_url: Optional[str] = None
    pending_auth: bool = False

    class Config:
        arbitrary_types_allowed = True