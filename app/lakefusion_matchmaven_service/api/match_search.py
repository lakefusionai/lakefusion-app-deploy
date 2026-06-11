from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional

from app.lakefusion_matchmaven_service.utils.app_db import get_db, token_required_wrapper
from app.lakefusion_matchmaven_service.services.match_search import MatchSearchService
from lakefusion_utility.models.match_search import (
    MatchSearchRequestBody,
    MatchResponse,
    SearchResponse,
    ErrorResponse
)
from lakefusion_utility.utils.logging_utils import get_logger

app_logger = get_logger(__name__)

# Initialize the router
match_search_router = APIRouter(tags=["Match Search API"], prefix='/match-search')


@match_search_router.post(
    "/{entity_id}/match",
    response_model=MatchResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid input"},
        404: {"model": ErrorResponse, "description": "Entity not found"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
        502: {"model": ErrorResponse, "description": "External service error"},
        503: {"model": ErrorResponse, "description": "Service unavailable"}
    }
)
def match_entity(
    entity_id: int,
    request_body: MatchSearchRequestBody,
    warehouse_id: str = Query(..., description="Databricks warehouse ID for SQL queries"),
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    try:
        token = check.get('token')
        
        app_logger.info(f"Match request received for entity {entity_id}")
        
        # Initialize service
        service = MatchSearchService(db)
        
        # Execute match
        result = service.execute_match(
            entity_id=entity_id,
            input_attributes=request_body.attributes,
            token=token,
            warehouse_id=warehouse_id
        )
        
        app_logger.info(
            f"Match completed for entity {entity_id}: "
            f"status={result['match_status']}, score={result.get('score')}"
        )
        
        return result
        
    except Exception as e:
        app_logger.exception(f"Match endpoint error: {str(e)}")
        raise


@match_search_router.post(
    "/{entity_id}/search",
    response_model=SearchResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid input"},
        404: {"model": ErrorResponse, "description": "Entity not found"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
        502: {"model": ErrorResponse, "description": "External service error"},
        503: {"model": ErrorResponse, "description": "Service unavailable"}
    }
)
def search_entity(
    entity_id: int,
    request_body: MatchSearchRequestBody,
    warehouse_id: str = Query(..., description="Databricks warehouse ID for SQL queries"),
    top_n: int = Query(
        3,
        ge=1,
        le=20,
        description="Number of results to return (1-20, default: 3)"
    ),
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    try:
        token = check.get('token')
        
        app_logger.info(f"Search request received for entity {entity_id}, top_n={top_n}")
        
        # Initialize service
        service = MatchSearchService(db)
        
        # Execute search
        result = service.execute_search(
            entity_id=entity_id,
            input_attributes=request_body.attributes,
            token=token,
            warehouse_id=warehouse_id,
            top_n=top_n
        )
        
        app_logger.info(
            f"Search completed for entity {entity_id}: "
            f"returned {len(result.get('results', []))} results"
        )
        
        return result
        
    except Exception as e:
        app_logger.exception(f"Search endpoint error: {str(e)}")
        raise

@match_search_router.post(
    "/{entity_id}/sync-index",
    responses={
        200: {"description": "Index sync completed successfully"},
        404: {"model": ErrorResponse, "description": "Entity not found"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
        502: {"model": ErrorResponse, "description": "Vector search sync failed"},
        503: {"model": ErrorResponse, "description": "Service unavailable"}
    }
)
def sync_vector_index(
    entity_id: int,
    db: Session = Depends(get_db),
    check: dict = Depends(token_required_wrapper)
):
    """
    Sync the vector search index with the master table.
    This triggers an index refresh to pick up any new or updated records.
    
    Args:
        entity_id: ID of the entity whose index should be synced
        
    Returns:
        Sync status information
    """
    try:
        token = check.get('token')
        
        app_logger.info(f"Index sync request received for entity {entity_id}")
        
        # Initialize service
        service = MatchSearchService(db)
        
        # Execute sync
        result = service.sync_vector_index(
            entity_id=entity_id,
            token=token
        )
        
        app_logger.info(f"Index sync completed for entity {entity_id}")
        
        return result
        
    except Exception as e:
        app_logger.exception(f"Sync endpoint error: {str(e)}")
        raise