import base64
import requests
from lakefusion_utility.utils.logging_utils import get_logger

logger = get_logger(__name__)

DNB_TOKEN_URL = "https://plus.dnb.com/v3/token"


class DnbService:
    """Service class for Dun & Bradstreet API operations."""

    @staticmethod
    def validate_credentials(consumer_key: str, consumer_secret: str) -> dict:
        """
        Validates D&B OAuth2 credentials by requesting a token from the D&B token endpoint.
        Returns dict with 'connected' (bool) and 'message' (str).
        """
        try:
            encoded = base64.b64encode(f"{consumer_key}:{consumer_secret}".encode()).decode()
            headers = {
                "Authorization": f"Basic {encoded}",
                "Content-Type": "application/x-www-form-urlencoded"
            }
            resp = requests.post(DNB_TOKEN_URL, headers=headers, data={"grant_type": "client_credentials"})

            if resp.status_code == 200:
                return {"connected": True, "message": "Successfully authenticated with D&B API"}
            else:
                error_detail = resp.json().get("error_description", resp.text) if resp.text else "Unknown error"
                logger.warning(f"D&B token validation failed ({resp.status_code}): {error_detail}")
                return {"connected": False, "message": f"Authentication failed: {error_detail}"}
        except requests.exceptions.ConnectionError:
            logger.error("Unable to reach D&B token endpoint")
            return {"connected": False, "message": "Unable to reach D&B API endpoint"}
        except Exception as e:
            logger.exception("D&B credential validation error")
            return {"connected": False, "message": f"Validation error: {str(e)}"}
