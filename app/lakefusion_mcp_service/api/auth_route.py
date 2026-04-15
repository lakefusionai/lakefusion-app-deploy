"""
OAuth callback endpoint for SSO authentication.
"""
from fastapi import APIRouter, Query
from fastapi.responses import HTMLResponse
from app.lakefusion_mcp_service.utils.logger import get_logger, log_to_stderr
from app.lakefusion_mcp_service.services.session_manager import session_manager, sse_notifications
from app.lakefusion_mcp_service.services.auth_manager import AuthManager

logger = get_logger(__name__)

router = APIRouter(tags=["Authentication"])


# HTML Templates
def _base_template(title: str, content: str, is_error: bool = False) -> str:
    """Base HTML template with external CSS and LakeFusion branding."""
    status_class = "error" if is_error else "success"
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title} - LakeFusion</title>
    <link rel="stylesheet" href="/api/mcp/static/css/auth.css">
</head>
<body>
    <div class="container {status_class}">
        {content}
    </div>
</body>
</html>"""


def _success_icon() -> str:
    """Checkmark SVG icon."""
    return """<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2.5">
        <path stroke-linecap="round" stroke-linejoin="round" d="M9 12.75L11.25 15 15 9.75M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
    </svg>"""


def _error_icon() -> str:
    """X mark SVG icon."""
    return """<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2.5">
        <path stroke-linecap="round" stroke-linejoin="round" d="M9.75 9.75l4.5 4.5m0-4.5l-4.5 4.5M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
    </svg>"""


def _shield_icon() -> str:
    """Shield check SVG icon."""
    return """<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
        <path stroke-linecap="round" stroke-linejoin="round" d="M9 12.75L11.25 15 15 9.75m-3-7.036A11.959 11.959 0 013.598 6 11.99 11.99 0 003 9.749c0 5.592 3.824 10.29 9 11.623 5.176-1.332 9-6.03 9-11.622 0-1.31-.21-2.571-.598-3.751h-.152c-3.196 0-6.1-1.248-8.25-3.285z" />
    </svg>"""


def _zap_icon() -> str:
    """Zap/lightning SVG icon."""
    return """<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
        <path stroke-linecap="round" stroke-linejoin="round" d="M13 10V3L4 14h7v7l9-11h-7z" />
    </svg>"""


def _logo_with_badge(is_error: bool = False) -> str:
    """LakeFusion logo with status badge."""
    icon = _error_icon() if is_error else _success_icon()
    return f"""
        <div class="logo-container">
            <div class="logo-box">
                <img src="/api/mcp/static/images/ic_logomark.svg" alt="LakeFusion" onerror="this.style.display='none'">
            </div>
            <div class="status-badge">{icon}</div>
        </div>
    """


def _auto_close_script() -> str:
    """JavaScript for auto-closing the window."""
    return """<script>
    let seconds = 5;
    const timerEl = document.getElementById('timer');
    const countdownEl = document.getElementById('countdown');

    const countdown = setInterval(() => {
        seconds--;
        if (timerEl) timerEl.textContent = seconds;
        if (seconds <= 0) {
            clearInterval(countdown);
            window.close();
            setTimeout(() => {
                if (countdownEl) countdownEl.textContent = 'You can safely close this tab now.';
            }, 500);
        }
    }, 1000);
</script>"""


def _footer() -> str:
    """LakeFusion branded footer."""
    from lakefusion_utility.utils.lf_utils import get_version_info
    version_info = get_version_info()
    logger.info(f"Version info for footer: {version_info}")
    version = version_info.get("productVersion", "1.0.0")
    return f"""
        <div class="footer">
            <span class="zap-icon">{_zap_icon()}</span>
            <span>Powered by</span>
            <img src="/api/mcp/static/images/ic_logomark.svg" alt="LakeFusion" class="footer-logo" onerror="this.style.display='none'">
            <span class="brand">LakeFusion</span>
            <span class="separator">•</span>
            <span class="version">v{version}</span>
        </div>
    """


def _security_badge() -> str:
    """Security badge element."""
    return f"""
        <div class="security-badge">
            {_shield_icon()}
            <span>Secured Data Processing</span>
        </div>
    """


def success_page(username: str) -> str:
    """Generate success page HTML with LakeFusion branding."""
    content = f"""
        {_logo_with_badge(is_error=False)}
        <h1>Authentication Successful</h1>
        <div class="info-card">
            <p class="username">Welcome, {username}</p>
            <p class="description">You've been authenticated with SSO. You can now use LakeFusion MCP tools.</p>
        </div>
        {_security_badge()}
        <div class="divider"></div>
        <p class="countdown" id="countdown">Closing in <span class="timer" id="timer">5</span> seconds...</p>
        <div class="progress-bar"><div class="progress"></div></div>
        {_footer()}
        {_auto_close_script()}
    """
    return _base_template("Authentication Successful", content, is_error=False)


def error_page(message: str, details: str = None) -> str:
    """Generate error page HTML with LakeFusion branding."""
    details_html = f'<p class="error-message">{details}</p>' if details else ""
    content = f"""
        {_logo_with_badge(is_error=True)}
        <h1>Authentication Failed</h1>
        <div class="info-card">
            <p class="description">{message}</p>
            {details_html}
        </div>
        {_security_badge()}
        <div class="divider"></div>
        <p class="retry-text">Please try authenticating again or contact support if the issue persists.</p>
        {_footer()}
    """
    return _base_template("Authentication Failed", content, is_error=True)


@router.get(
    "/auth/callback",
    responses={
        200: {
            "description": "Authentication successful - returns HTML page with success message",
            "content": {
                "text/html": {
                    "example": "<html>...Authentication Successful...</html>"
                }
            },
        },
        400: {
            "description": "Authentication failed - returns HTML page with error message",
            "content": {
                "text/html": {
                    "example": "<html>...Authentication Failed...</html>"
                }
            },
        },
    },
)
async def auth_callback(
    code: str = Query(..., min_length=1, description="Authorization code from OAuth provider"),
    state: str = Query(..., min_length=1, description="State parameter for session validation"),
):
    """
    OAuth callback endpoint for SSO authentication.

    This endpoint receives the authorization code from the OAuth provider after user login
    and exchanges it for access tokens.

    **Request Parameters:**
    - `code`: Authorization code received from the OAuth provider
    - `state`: State parameter used for session validation and CSRF protection

    **Response:**
    - On success: HTML page showing authentication success with username
    - On failure: HTML page showing error message
    """
    logger.info(f"Received OAuth callback with state: {state[:8]}...")

    # Find and consume the session (single-use, prevents replay)
    session_id = session_manager.consume_state(state)

    if not session_id:
        logger.warning(f"OAuth callback with invalid/expired state: {state[:8]}...")
        return HTMLResponse(
            content=error_page("Invalid or expired authentication session"),
            status_code=400
        )

    # Complete the authentication
    auth_manager = AuthManager(session_id)

    try:
        result = await auth_manager.complete_oidc_login(code, state)
        username = result.get("username", "User")

        # Send notification to MCP client that auth is complete
        notification_sent = await sse_notifications.send_log_message(
            session_id=session_id,
            level="info",
            message=f"Authentication successful! Welcome, {username}. You can now use the MCP tools.",
            data={
                "type": "auth_complete",
                "status": "authenticated",
                "username": username
            }
        )

        log_to_stderr(f"[AUTH_CALLBACK] Auth complete for {username}, notification_sent={notification_sent}")
        logger.info(f"OAuth callback successful for user: {username}, notification_sent={notification_sent}")

        return HTMLResponse(content=success_page(username))

    except Exception as e:
        logger.error(f"OAuth callback failed: {str(e)}")
        return HTMLResponse(
            content=error_page("Authentication could not be completed", str(e)),
            status_code=400
        )
