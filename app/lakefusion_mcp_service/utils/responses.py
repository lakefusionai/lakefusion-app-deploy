from typing import Dict, Any
from urllib.parse import urlencode
from pydantic import BaseModel, ValidationError
from app.lakefusion_mcp_service.utils.logger import get_logger
from app.lakefusion_mcp_service.config import PORTAL_URL

logger = get_logger(__name__)


def validate_response(data: Dict[str, Any], model: type[BaseModel]) -> Dict[str, Any]:
    """Validate API response against a Pydantic model."""
    validated = model(**data)
    return validated.model_dump()


def generate_profile_url(lakefusion_id: str, entity_id: int) -> str:
    """
    Generate a clickable profile URL for a LakeFusion entity.

    Args:
        lakefusion_id: The unique LakeFusion ID of the golden record
        entity_id: The entity type ID

    Returns:
        Full URL to the entity profile page in LakeFusion portal
    """
    params = {
        "entity": entity_id,
        "filter": "allMasterRecords",
        "profileId": lakefusion_id,
        "showMerge": "false",
        "entityId": entity_id,
        "filterId": "allMasterRecords",
        "tab": "profile",
    }
    base_url = PORTAL_URL.rstrip("/")
    return f"{base_url}/entity-search/profile?{urlencode(params)}"


def make_lakefusion_id_clickable(lakefusion_id: str, entity_id: int) -> str:
    """
    Convert lakefusion_id to a clickable markdown link.

    Args:
        lakefusion_id: The unique LakeFusion ID of the golden record
        entity_id: The entity type ID

    Returns:
        Markdown formatted clickable link: [lakefusion_id](profile_url)
    """
    url = generate_profile_url(lakefusion_id, entity_id)
    return f"[{lakefusion_id}]({url})"


def add_profile_urls_to_search_results(results: list, entity_id: int) -> list:
    """Make lakefusion_id clickable in each search result."""
    for result in results:
        if "lakefusion_id" in result:
            result["lakefusion_id"] = make_lakefusion_id_clickable(result["lakefusion_id"], entity_id)
    return results


def add_profile_url_to_match_result(result: Dict[str, Any], entity_id: int) -> Dict[str, Any]:
    """Make lakefusion_id clickable in match result if there's a matched golden record."""
    if result.get("matched_golden_record") and result["matched_golden_record"].get("lakefusion_id"):
        lakefusion_id = result["matched_golden_record"]["lakefusion_id"]
        result["matched_golden_record"]["lakefusion_id"] = make_lakefusion_id_clickable(lakefusion_id, entity_id)
    return result