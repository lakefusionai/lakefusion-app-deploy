"""Logger helper that ensures initialization before use and logs to stderr for Claude visibility."""
import sys
import logging
from lakefusion_utility.utils.logging_utils import init_logger, get_logger as _get_logger
from app.lakefusion_mcp_service.config import MCP_SERVER_NAME, LOG_LEVEL

_initialized = False


def _setup_stderr_handler():
    """Ensure root logger has a stderr handler for Claude Desktop visibility."""
    root_logger = logging.getLogger()

    # Check if stderr handler already exists
    has_stderr = any(
        isinstance(h, logging.StreamHandler) and h.stream == sys.stderr
        for h in root_logger.handlers
    )

    if not has_stderr:
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))
        formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        stderr_handler.setFormatter(formatter)
        root_logger.addHandler(stderr_handler)


def get_logger(name: str):
    """Get logger, initializing if needed. Ensures output to stderr for Claude visibility."""
    global _initialized
    if not _initialized:
        init_logger(MCP_SERVER_NAME)
        _setup_stderr_handler()
        _initialized = True
    return _get_logger(name)


def log_to_stderr(message: str, level: str = "INFO"):
    """
    Print a message directly to stderr for Claude Desktop logs.
    Use this for important operational messages that should always be visible.
    """
    timestamp = __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] [mcp-service] {message}", file=sys.stderr)
