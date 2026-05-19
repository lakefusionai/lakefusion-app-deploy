import os
import json
from pathlib import Path

def _get_version_from_file() -> str:
    """Read version from version.json file directly to avoid circular imports."""
    try:
        version_file = Path(__file__).parent.parent / "version.json"
        if version_file.exists():
            with open(version_file, "r") as f:
                data = json.load(f)
                return data.get("productVersion", "1.0.0")
    except Exception:
        pass
    return "1.0.0"

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "debug")

# MCP Server Configuration
MCP_SERVER_NAME = os.getenv("MCP_SERVER_NAME", "lakefusion-mcp")


# Service URLs
AUTH_SERVICE_URL = os.getenv("AUTH_SERVICE_URL", "http://auth-service/api/auth")
MATCHMAVEN_SERVICE_URL = os.getenv("MATCHMAVEN_SERVICE_URL", "http://matchmaven-service/api/match-maven")
MIDDLELAYER_SERVICE_URL = os.getenv("MIDDLELAYER_SERVICE_URL", "http://middlelayer-service/api/middle-layer")

# MCP Version - read from version.json, can be overridden by env var
MCP_VERSION = os.getenv("MCP_VERSION") or _get_version_from_file()

# LakeFusion Portal URL (for generating profile links)
PORTAL_URL = os.getenv("PORTAL_URL", "http://localhost:3000")

# OAuth redirect URI for callbacks - defaults to PORTAL_URL if not set
REDIRECT_URI = os.getenv("REDIRECT_URI", "http://mcp-service")
