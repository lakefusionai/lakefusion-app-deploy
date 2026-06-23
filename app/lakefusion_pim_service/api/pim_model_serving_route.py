import os
from fastapi import APIRouter, Depends, HTTPException
from app.lakefusion_pim_service.utils.app_db import token_required_wrapper
from lakefusion_utility.utils.logging_utils import get_logger

app_logger = get_logger(__name__)

pim_model_serving_router = APIRouter(
    tags=["PIM Model Serving"],
    prefix="/model-serving",
)


@pim_model_serving_router.get("/endpoints")
def list_model_endpoints(
    check: dict = Depends(token_required_wrapper),
):
    """
    List available Databricks Model Serving endpoints.
    Filters to llm/chat-style endpoints only.
    """
    from databricks.sdk import WorkspaceClient

    databricks_host = os.getenv("DATABRICKS_HOST")
    databricks_dapi = os.getenv("LAKEFUSION_DATABRICKS_DAPI")

    if not databricks_host and not databricks_dapi:
        return {"status": "success", "data": []}

    try:
        w = WorkspaceClient(host=databricks_host, token=databricks_dapi) if databricks_host else WorkspaceClient()
        raw_endpoints = w.serving_endpoints.list()

        endpoints = []
        for ep in raw_endpoints:
            # Extract model info from the served entities
            model_name = None
            model_type = None
            task = None

            if ep.config and ep.config.served_entities:
                first = ep.config.served_entities[0]
                model_name = getattr(first, "foundation_model", None) or getattr(first, "external_model", None)
                if model_name and isinstance(model_name, dict):
                    model_name = model_name.get("name")
                elif model_name and hasattr(model_name, "name"):
                    model_name = model_name.name

                # Determine type
                if getattr(first, "foundation_model", None):
                    model_type = "foundation"
                elif getattr(first, "external_model", None):
                    model_type = "external"
                else:
                    model_type = "custom"

            # Determine task from endpoint config
            if ep.task:
                task = ep.task

            state = ep.state.ready if ep.state else None

            endpoints.append({
                "name": ep.name,
                "state": state,
                "endpoint_type": getattr(ep, "endpoint_type", None),
                "model_name": model_name,
                "model_type": model_type,
                "task": task,
                "creator": getattr(ep, "creator", None),
                "creation_timestamp": getattr(ep, "creation_timestamp", None),
            })

        return {"status": "success", "data": endpoints}

    except Exception as e:
        app_logger.error(f"Failed to list model serving endpoints: {e}")
        return {"status": "error", "data": [], "message": str(e)}
