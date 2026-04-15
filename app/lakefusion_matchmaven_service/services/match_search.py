import os
import traceback
import json
import time
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
from fastapi import HTTPException
from databricks.vector_search.client import VectorSearchClient
from mlflow.deployments import get_deploy_client

from lakefusion_utility.models.integration_hub import Integration_Hub
from lakefusion_utility.models.dbconfig import DBConfigProperties
from lakefusion_utility.utils.databricks_util import DataSetSQLService
from lakefusion_utility.utils.logging_utils import get_logger
from app.lakefusion_matchmaven_service.services.llm_response_parser import LLMResponseParser

app_logger = get_logger(__name__)

DATABRICKS_HOST = os.environ.get('DATABRICKS_HOST', 'https://databricks.com')

class MatchSearchService:
    """Service for real-time match and search operations"""
    
    def __init__(self, db: Session):
        """
        Initialize the MatchSearchService.
        
        Args:
            db: SQLAlchemy database session
        """
        self.db = db
        self.catalog_name = (
            self.db.query(DBConfigProperties)
            .filter(DBConfigProperties.config_key == "catalog_name")
            .first().config_value
        )
        
        # Fetch default prompt from DB config
        default_prompt_record = (
            self.db.query(DBConfigProperties)
            .filter(DBConfigProperties.config_key == "default_prompt")
            .first()
        )
        
        # Store default prompt or use fallback
        self.default_prompt = default_prompt_record.config_value if default_prompt_record else self._get_fallback_prompt()
        
        app_logger.info("MatchSearchService initialized with default prompt from DB")

    def _get_fallback_prompt(self) -> str:
        """
        Fallback prompt if not found in DB.
        
        Returns:
            Default system prompt string
        """
        return """You are an expert in classifying two entities as matching or not matching.
You will be provided with a query entity and a list of possible entities.
You need to find the closest matching entity with a similarity score to indicate how similar they are.
Take into account:
1. The attributes are not mutually exclusive.
2. The attributes are not exhaustive.
3. The attributes are not consistent.
4. The attributes are not complete.
5. The attributes can have typos.
6. The entity type that is being considered here is - {entity}.
7. Do not consider scores in the search results / query entities. Generate scores by yourself."""

    def get_entity_configuration(self, entity_id: int) -> Dict[str, Any]:
        """
        Fetch complete configuration from Integration Hub.
        
        Args:
            entity_id: ID of the entity
            
        Returns:
            Configuration dictionary with all necessary details
            
        Raises:
            HTTPException: If entity or configuration not found
        """
        try:
            # Query Integration Hub with entity and model_experiment
            integration_hub = (
                self.db.query(Integration_Hub)
                .filter(Integration_Hub.entity_id == entity_id)
                .filter(Integration_Hub.is_active == True)
                .first()
            )
            
            if not integration_hub:
                raise HTTPException(
                    status_code=404,
                    detail={
                        "error_type": "ENTITY_NOT_FOUND",
                        "message": "No active integration hub found for entity",
                        "detail": {"entity_id": entity_id}
                    }
                )
            
            # Access nested objects
            entity = integration_hub.entity
            model_experiment = integration_hub.model_experiment
            
            if not model_experiment:
                raise HTTPException(
                    status_code=404,
                    detail={
                        "error_type": "MODEL_NOT_FOUND",
                        "message": "No model experiment associated with entity",
                        "detail": {"entity_id": entity_id}
                    }
                )
            
            # Extract matching attributes (ONLY from model_experiment) - REQUIRED
            matching_attributes = []
            for attr in model_experiment.attributes:
                matching_attributes.append({
                    "name": attr['name'],
                    "type": attr['type'],
                    "label": attr['label']
                })
            
            # Extract ALL entity attributes (for validation and response)
            all_entity_attributes = []
            for attr in entity.attributes:
                all_entity_attributes.append({
                    "name": attr.name,
                    "type": attr.type,
                    "label": attr.label,
                    "show_in_ui": attr.show_in_ui,
                    "is_label": attr.is_label
                })
            
            # Get LLM configuration
            llm_model_source = model_experiment.llm_model_source
            llm_model_config = model_experiment.llm_model
            llm_endpoint = self.get_endpoint_name(llm_model_source, llm_model_config)
            
            # Get Embedding configuration
            embedding_model_source = model_experiment.embedding_model_source
            embedding_model_config = model_experiment.embedding_model
            embedding_endpoint = self.get_endpoint_name(embedding_model_source, embedding_model_config)
            
            # Build vector search index name
            entity_name = entity.name.lower().replace(' ', '_')
            vs_index_name = f"{self.catalog_name}.gold.{entity_name}_master_prod_index"
            
            # Extract thresholds
            config_threshold = model_experiment.config_thresold or {}
            match_thresholds = {
                "exact_match": config_threshold.get("merge", [0.9, 1.0]),
                "potential_match": config_threshold.get("matches", [0.7, 0.89]),
                "no_match": config_threshold.get("not_match", [0.0, 0.69])
            }
            
            # Get additional instructions from model experiment
            additional_instructions = model_experiment.additional_instructions or ""
            
            config = {
                "entity_name": entity_name,
                "entity_id": entity_id,
                "matching_attributes": matching_attributes,  # Required attributes
                "all_entity_attributes": all_entity_attributes,  # All attributes for validation
                "llm_model_source": llm_model_source,
                "llm_endpoint": llm_endpoint,
                "embedding_model_source": embedding_model_source,
                "embedding_endpoint": embedding_endpoint,
                "vs_endpoint": model_experiment.vs_endpoint,
                "vs_index_name": vs_index_name,
                "max_potential_matches": model_experiment.max_potential_matches or 3,
                "match_thresholds": match_thresholds,
                "additional_instructions": additional_instructions
            }
            
            app_logger.info(f"Configuration loaded for entity {entity_id}: {entity_name}")
            return config
            
        except HTTPException:
            raise
        except Exception as e:
            message = traceback.format_exc()
            app_logger.exception(f"Failed to get entity configuration: {message}")
            raise HTTPException(
                status_code=500,
                detail={
                    "error_type": "CONFIGURATION_ERROR",
                    "message": f"Failed to load entity configuration: {str(e)}",
                    "detail": {"entity_id": entity_id}
                }
            )

    def get_endpoint_name(self, model_source: str, model_config: Dict[str, Any]) -> str:
        """
        Generate endpoint name from model configuration.
        
        Args:
            model_source: "databricks_foundation" or "databricks_custom"
            model_config: Full model config dict
            
        Returns:
            Endpoint name string
            
        Raises:
            ValueError: If model source is unknown
        """
        try:
            if model_source == "databricks_foundation":
                foundation_config = model_config[model_source]
                model_name = foundation_config["foundation_model_name"]
                provisionless = foundation_config.get("provisionless", False)

                if provisionless:
                    return model_name
                else:
                    pt_endpoint = f"lakefusion-{model_name}"
                    try:
                        client = get_deploy_client("databricks")
                        endpoint = client.get_endpoint(pt_endpoint)
                        state = endpoint.get("state", {})
                        config_update = state.get("config_update", "")
                        if config_update not in ("UPDATE_FAILED", "UPDATE_CANCELED"):
                            return pt_endpoint
                    except Exception:
                        pass
                    app_logger.info(f"PT endpoint '{pt_endpoint}' not found or failed - falling back to pay-per-token: {model_name}")
                    return model_name
            
            elif model_source in ["databricks_custom", "databricks_custom_hugging_face"]:
                model_name = model_config.get(model_source, {}).get("modelname", "")
                if not model_name:
                    # Try alternate key
                    model_name = model_config.get("databricks_custom", {}).get("modelname", "")
                return model_name.replace('/', '-')
            
            else:
                raise ValueError(f"Unknown model source: {model_source}")
                
        except Exception as e:
            app_logger.error(f"Error getting endpoint name: {str(e)}")
            raise ValueError(f"Failed to determine endpoint name: {str(e)}")

    def check_and_start_endpoint(self, endpoint_name: str, token: str) -> bool:
        """
        Check if model serving endpoint is ready, wait if not.
        
        Args:
            endpoint_name: Name of the serving endpoint
            token: Databricks authentication token
            
        Returns:
            True if endpoint is ready
            
        Raises:
            HTTPException: If endpoint not ready or doesn't exist
        """
        try:
            os.environ['DATABRICKS_TOKEN'] = token
            if not os.environ.get('DATABRICKS_HOST'):
                os.environ['DATABRICKS_HOST'] = DATABRICKS_HOST
            
            client = get_deploy_client("databricks")
            
            max_wait = 60  # seconds
            check_interval = 5
            elapsed = 0
            
            app_logger.info(f"Checking endpoint status: {endpoint_name}")
            
            while elapsed < max_wait:
                try:
                    endpoint = client.get_endpoint(endpoint_name)
                    state = endpoint.get("state", {})
                    
                    if state.get("ready") == "READY":
                        app_logger.info(f"Endpoint {endpoint_name} is ready")
                        return True
                    
                    app_logger.info(
                        f"Endpoint {endpoint_name} not ready yet, waiting... "
                        f"({elapsed}s/{max_wait}s)"
                    )
                    time.sleep(check_interval)
                    elapsed += check_interval
                    
                except Exception as e:
                    if "RESOURCE_DOES_NOT_EXIST" in str(e):
                        raise HTTPException(
                            status_code=500,
                            detail={
                                "error_type": "ENDPOINT_NOT_FOUND",
                                "message": f"Endpoint {endpoint_name} does not exist",
                                "detail": {"endpoint_name": endpoint_name}
                            }
                        )
                    raise
            
            # Timeout reached
            raise HTTPException(
                status_code=503,
                detail={
                    "error_type": "ENDPOINT_NOT_READY",
                    "message": f"Endpoint {endpoint_name} not ready after {max_wait}s",
                    "detail": {"endpoint_name": endpoint_name, "timeout": max_wait}
                }
            )
            
        except HTTPException:
            raise
        except Exception as e:
            message = traceback.format_exc()
            app_logger.exception(f"Error checking endpoint: {message}")
            raise HTTPException(
                status_code=500,
                detail={
                    "error_type": "ENDPOINT_CHECK_FAILED",
                    "message": f"Failed to check endpoint status: {str(e)}",
                    "detail": {"endpoint_name": endpoint_name}
                }
            )

    def validate_input_attributes(
        self,
        matching_attributes: List[Dict[str, Any]],
        all_entity_attributes: List[Dict[str, Any]],
        input_attributes: Dict[str, Any]
    ) -> None:
        """
        Validate input attributes - matching attributes are REQUIRED, all others are OPTIONAL.
        
        Args:
            matching_attributes: List of attribute dicts from model_experiment (REQUIRED)
            all_entity_attributes: List of all entity attributes (OPTIONAL)
            input_attributes: Dict of attributes from API request
            
        Raises:
            HTTPException: If validation fails
        """
        try:
            # Check required matching attributes
            required_attr_names = [attr['name'] for attr in matching_attributes]
            missing_attrs = [name for name in required_attr_names if name not in input_attributes]
            
            if missing_attrs:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "error_type": "MISSING_REQUIRED_ATTRIBUTES",
                        "message": "Missing required matching attributes",
                        "detail": {
                            "missing_attributes": missing_attrs,
                            "required_attributes": required_attr_names
                        }
                    }
                )
            
            # Check for unknown attributes
            all_attr_names = [attr['name'] for attr in all_entity_attributes]
            unknown_attrs = [name for name in input_attributes.keys() if name not in all_attr_names]
            
            if unknown_attrs:
                app_logger.warning(f"Unknown attributes provided: {unknown_attrs}")
                # Don't fail, just warn
            
            app_logger.info("Input attributes validated successfully")
            
        except HTTPException:
            raise
        except Exception as e:
            app_logger.exception(f"Attribute validation failed: {str(e)}")
            raise HTTPException(
                status_code=400,
                detail={
                    "error_type": "VALIDATION_ERROR",
                    "message": f"Failed to validate input attributes: {str(e)}"
                }
            )

    def format_attributes_for_embedding(
        self,
        input_attributes: Dict[str, Any],
        matching_attributes: List[Dict[str, Any]]
    ) -> str:
        """
        Format attributes as pipe-separated text for embedding.
        
        Args:
            input_attributes: Dictionary of attribute values
            matching_attributes: List of attribute definitions (with order)
            
        Returns:
            Formatted string: "value1 | value2 | value3"
        """
        # Extract values in the order defined by matching_attributes
        values = []
        for attr in matching_attributes:
            attr_name = attr['name']
            value = input_attributes.get(attr_name, '')
            # Convert to string and handle None
            value_str = str(value) if value is not None else ''
            values.append(value_str)
        
        formatted = ' | '.join(values)
        app_logger.info(f"Formatted input for embedding: {formatted}")
        return formatted

    def query_vector_search(
        self,
        formatted_text: str,
        config: Dict[str, Any],
        token: str,
        top_n: int = 3
    ) -> List[Dict[str, Any]]:
        """
        Query vector search index and return candidates with all attributes.
        
        Args:
            formatted_text: Text to search for
            config: Entity configuration
            token: Databricks token
            top_n: Number of results to return
            
        Returns:
            List of candidate records with scores and all attributes
            
        Raises:
            HTTPException: If vector search fails
        """
        try:
            app_logger.info(f"Querying vector search index: {config['vs_index_name']}")
            
            os.environ['DATABRICKS_TOKEN'] = token
            if not os.environ.get('DATABRICKS_HOST'):
                os.environ['DATABRICKS_HOST'] = DATABRICKS_HOST
            
            client = VectorSearchClient(disable_notice=True)
            index = client.get_index(index_name=config['vs_index_name'])
            
            # Request ALL columns from the index
            all_column_names = [attr['name'] for attr in config['all_entity_attributes']]
            columns_to_fetch = ["lakefusion_id", "attributes_combined"] + all_column_names
            
            results = index.similarity_search(
                query_text=formatted_text,
                columns=columns_to_fetch,
                num_results=top_n
            )
            
            # Parse results
            candidates = []
            data_array = results.get("result", {}).get("data_array", [])
            
            for row in data_array:
                if len(row) >= 3:
                    # Extract lakefusion_id, attributes_combined, and score
                    lakefusion_id = row[0]
                    attributes_combined = row[1]
                    vector_score = float(row[-1])  # Score is always last
                    
                    # Parse all attributes from the row
                    # Row format: [lakefusion_id, attributes_combined, attr1, attr2, ..., score]
                    attributes_dict = {}
                    for i, attr in enumerate(config['all_entity_attributes']):
                        # Attributes start at index 2, score is last
                        attr_index = i + 2
                        if attr_index < len(row) - 1:  # -1 to exclude score
                            attributes_dict[attr['name']] = row[attr_index]
                    
                    candidates.append({
                        "lakefusion_id": lakefusion_id,
                        "attributes_combined": attributes_combined,
                        "vector_score": vector_score,
                        "attributes": attributes_dict
                    })
            
            app_logger.info(f"Vector search returned {len(candidates)} candidates with all attributes")
            return candidates
            
        except Exception as e:
            message = traceback.format_exc()
            app_logger.exception(f"Vector search failed: {message}")
            raise HTTPException(
                status_code=502,
                detail={
                    "error_type": "VECTOR_SEARCH_FAILED",
                    "message": f"Vector search operation failed: {str(e)}",
                    "detail": {"index_name": config.get('vs_index_name')}
                }
            )

    def score_with_llm(
        self,
        input_attributes: Dict[str, Any],
        candidates: List[Dict[str, Any]],
        config: Dict[str, Any],
        token: str
    ) -> List[Dict[str, Any]]:
        """
        Score candidates using LLM and determine MATCH/NOT_A_MATCH.
        
        Args:
            input_attributes: Original input attributes dict
            candidates: List from vector search with text, lakefusion_id, vector_score
            config: Entity configuration
            token: Databricks token
            
        Returns:
            List of scored candidates with match decision and reason
            
        Raises:
            HTTPException: If LLM scoring fails
        """
        try:
            os.environ['DATABRICKS_TOKEN'] = token
            if not os.environ.get('DATABRICKS_HOST'):
                os.environ['DATABRICKS_HOST'] = DATABRICKS_HOST
            # Format input for LLM (only matching attributes)
            matching_attr_names = [attr['name'] for attr in config['matching_attributes']]
            formatted_input = self.format_attributes_for_embedding(
                input_attributes,
                config['matching_attributes']
            )
            
            # Prepare candidates for LLM (include vector search score)
            candidates_for_llm = []
            for c in candidates:
                candidates_for_llm.append({
                    "id": c['attributes_combined'],
                    "lakefusion_id": c['lakefusion_id'],
                    "vector_score": c['vector_score']
                })
            
            # Build system prompt using default prompt from DB
            entity_name = config['entity_name']
            additional_instructions = config.get('additional_instructions', '')
            
            # Start with default prompt from DB and replace {entity} placeholder
            system_prompt = self.default_prompt.replace('{entity}', entity_name)
            
            # Add additional instructions if provided
            if additional_instructions:
                system_prompt += f"\n8. Additional instructions: {additional_instructions}"
            
            # Add output format instructions
            system_prompt += f"""

**Input Format:**
- Query Entity: {' | '.join(matching_attr_names)}
Each field separated by |
- List of Possible Entities: JSON array with id, lakefusion_id, vector_score

**Output Format:**
Return ONLY a JSON array (no markdown, no code blocks, no extra text) with:
- id: entity string
- match: "MATCH" or "NOT_A_MATCH"
- score: similarity score (float between 0 and 1)
- reason: explanation of why this is or isn't a match
- lakefusion_id: the lakefusion_id

Order by score descending."""

            user_prompt = f"""Query entity: {formatted_input}

Possible entities: {json.dumps(candidates_for_llm)}"""

            app_logger.info(f"Calling LLM endpoint: {config['llm_endpoint']}")
            
            client = get_deploy_client("databricks")
            
            # Use chat format for foundation models
            response = client.predict(
                endpoint=config['llm_endpoint'],
                inputs={
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    "max_tokens": 2000,
                    "temperature": 0.1  # Low temperature for consistent scoring
                }
            )
            
            # Parse LLM response using shared parser (handles all model response formats)
            response_text = LLMResponseParser.extract_content(response)

            if not response_text:
                app_logger.error(f"Failed to extract response text from LLM response: {response}")
                raise ValueError("No response text found in LLM response")

            # Parse JSON (handles markdown code blocks, nested structures, etc.)
            scored_candidates = LLMResponseParser.parse_json(response_text)
            
            if not isinstance(scored_candidates, list):
                raise ValueError("LLM response is not a JSON array")
            
            app_logger.info(f"LLM scored {len(scored_candidates)} candidates")
            return scored_candidates
            
        except json.JSONDecodeError as e:
            app_logger.error(f"Failed to parse LLM JSON response: {str(e)}")
            app_logger.error(f"Response text was: {response_text if 'response_text' in locals() else 'N/A'}")
            raise HTTPException(
                status_code=502,
                detail={
                    "error_type": "LLM_RESPONSE_PARSE_ERROR",
                    "message": "Failed to parse LLM response as JSON",
                    "detail": {"error": str(e)}
                }
            )
        except Exception as e:
            message = traceback.format_exc()
            app_logger.exception(f"LLM scoring failed: {message}")
            raise HTTPException(
                status_code=502,
                detail={
                    "error_type": "LLM_SCORING_FAILED",
                    "message": f"LLM scoring operation failed: {str(e)}",
                    "detail": {"endpoint": config.get('llm_endpoint')}
                }
            )

    def execute_match(
        self,
        entity_id: int,
        input_attributes: Dict[str, Any],
        token: str,
        warehouse_id: str
    ) -> Dict[str, Any]:
        """
        Execute match workflow:
        1. Get configuration
        2. Validate inputs (only matching attributes required)
        3. Check endpoints
        4. Vector search (returns all attributes)
        5. LLM scoring
        6. Determine match status
        7. Format response
        
        Args:
            entity_id: ID of the entity
            input_attributes: Attributes to match (matching attrs required, others optional)
            token: Databricks token
            warehouse_id: Warehouse ID
            
        Returns:
            Match response dict with all entity attributes and reason
        """
        try:
            app_logger.info(f"Starting match execution for entity {entity_id}")
            
            # 1. Get configuration
            config = self.get_entity_configuration(entity_id)
            
            # 2. Validate input attributes
            self.validate_input_attributes(
                config['matching_attributes'], 
                config['all_entity_attributes'],
                input_attributes
            )
            
            # 3. Check and start endpoints
            self.check_and_start_endpoint(config['embedding_endpoint'], token)
            self.check_and_start_endpoint(config['llm_endpoint'], token)
            
            # 4. Vector search (returns all attributes from index)
            formatted_text = self.format_attributes_for_embedding(
                input_attributes,
                config['matching_attributes']
            )
            
            vector_results = self.query_vector_search(
                formatted_text,
                config,
                token,
                top_n=config['max_potential_matches']
            )
            
            # Handle no results case
            if not vector_results:
                app_logger.info("No vector search results, returning NoMatch")
                return {
                    "entity_type": config['entity_name'],
                    "match_status": "NoMatch",
                    "score": None,
                    "reason": "No candidates found in vector search",
                    "matched_golden_record": None
                }
            
            # 5. Score with LLM
            llm_results = self.score_with_llm(
                input_attributes,
                vector_results,
                config,
                token
            )
            
            # 6. Find best match
            best_match = max(llm_results, key=lambda x: x.get('score', 0))
            
            # 7. Determine match status based on thresholds
            score = best_match.get('score', 0)
            reason = best_match.get('reason', 'No reason provided')
            thresholds = config['match_thresholds']
            
            if thresholds['exact_match'][0] <= score <= thresholds['exact_match'][1]:
                match_status = "ExactMatch"
            elif thresholds['potential_match'][0] <= score <= thresholds['potential_match'][1]:
                match_status = "PotentialMatch"
            else:
                match_status = "NoMatch"
            
            app_logger.info(f"Match status: {match_status} with score: {score}")
            
            # 8. Get golden record attributes from vector search results
            matched_record = None
            # Find the matching candidate from vector search results
            matched_candidate = next(
                (c for c in vector_results if c['lakefusion_id'] == best_match['lakefusion_id']),
                None
            )
                
            if matched_candidate:
                matched_record = {
                    "lakefusion_id": best_match['lakefusion_id'],
                    "golden_record_attributes": matched_candidate['attributes']
                }
            
            # 9. Return formatted response
            return {
                "entity_type": config['entity_name'],
                "match_status": match_status,
                "score": score,
                "reason": reason,
                "matched_golden_record": matched_record
            }
            
        except HTTPException:
            raise
        except Exception as e:
            message = traceback.format_exc()
            app_logger.exception(f"Match execution failed: {message}")
            raise HTTPException(
                status_code=500,
                detail={
                    "error_type": "MATCH_EXECUTION_FAILED",
                    "message": f"Failed to execute match: {str(e)}",
                    "detail": {"entity_id": entity_id}
                }
            )

    def execute_search(
        self,
        entity_id: int,
        input_attributes: Dict[str, Any],
        token: str,
        warehouse_id: str,
        top_n: int = 3
    ) -> Dict[str, Any]:
        """
        Execute search workflow:
        1. Get configuration
        2. Validate inputs (only matching attributes required)
        3. Check embedding endpoint only (no LLM needed)
        4. Vector search (returns all attributes)
        5. Format response
        
        Args:
            entity_id: ID of the entity
            input_attributes: Attributes to search (matching attrs required, others optional)
            token: Databricks token
            warehouse_id: Warehouse ID
            top_n: Number of results to return
            
        Returns:
            Search response dict with all entity attributes
        """
        try:
            app_logger.info(f"Starting search execution for entity {entity_id}, top_n={top_n}")
            
            # 1. Get configuration
            config = self.get_entity_configuration(entity_id)
            
            # 2. Validate input attributes
            self.validate_input_attributes(
                config['matching_attributes'],
                config['all_entity_attributes'],
                input_attributes
            )
            
            # 3. Check embedding endpoint only (no LLM for search)
            self.check_and_start_endpoint(config['embedding_endpoint'], token)
            
            # 4. Vector search (returns all attributes from index)
            formatted_text = self.format_attributes_for_embedding(
                input_attributes,
                config['matching_attributes']
            )
            
            vector_results = self.query_vector_search(
                formatted_text,
                config,
                token,
                top_n=top_n
            )
            
            # Handle no results case
            if not vector_results:
                app_logger.info("No vector search results, returning empty results")
                return {
                    "entity_type": config['entity_name'],
                    "results": []
                }
            
            # 5. Format results (attributes already in vector_results)
            results = []
            for candidate in vector_results:
                results.append({
                    "lakefusion_id": candidate['lakefusion_id'],
                    "score": candidate['vector_score'],
                    "golden_record_attributes": candidate['attributes']
                })
            
            app_logger.info(f"Search completed with {len(results)} results")
            
            # 6. Return formatted response
            return {
                "entity_type": config['entity_name'],
                "results": results
            }
            
        except HTTPException:
            raise
        except Exception as e:
            message = traceback.format_exc()
            app_logger.exception(f"Search execution failed: {message}")
            raise HTTPException(
                status_code=500,
                detail={
                    "error_type": "SEARCH_EXECUTION_FAILED",
                    "message": f"Failed to execute search: {str(e)}",
                    "detail": {"entity_id": entity_id}
                }
            )
        
    def sync_vector_index(
        self,
        entity_id: int,
        token: str
    ) -> Dict[str, Any]:
        """
        Sync the vector search index with the master table.
        Triggers index refresh and waits for completion.
        
        Args:
            entity_id: ID of the entity
            token: Databricks token
            
        Returns:
            Dict with sync status information
            
        Raises:
            HTTPException: If sync fails
        """
        try:
            app_logger.info(f"Starting vector index sync for entity {entity_id}")
            
            # 1. Get configuration
            config = self.get_entity_configuration(entity_id)

            os.environ['DATABRICKS_TOKEN'] = token
            if not os.environ.get('DATABRICKS_HOST'):
                os.environ['DATABRICKS_HOST'] = DATABRICKS_HOST
            
            # 2. Initialize vector search client (now with credentials set)
            client = VectorSearchClient(disable_notice=True)
            
            # 3. Get the index
            index_name = config['vs_index_name']
            vs_endpoint = config['vs_endpoint']
            
            app_logger.info(f"Getting index: {index_name} from endpoint: {vs_endpoint}")
            
            try:
                index = client.get_index(
                    endpoint_name=vs_endpoint,
                    index_name=index_name
                )
            except Exception as e:
                app_logger.error(f"Error getting index: {str(e)}")
                raise HTTPException(
                    status_code=404,
                    detail={
                        "error_type": "INDEX_NOT_FOUND",
                        "message": f"Vector search index not found: {index_name}",
                        "detail": {
                            "index_name": index_name, 
                            "endpoint": vs_endpoint,
                            "error": str(e)
                        }
                    }
                )
            
            # 4. Trigger sync
            app_logger.info(f"Triggering sync for index: {index_name}")
            index.sync()
            
            # 5. Wait for sync to complete
            app_logger.info("Waiting for index to be ready...")
            max_wait_time = 300  # 5 minutes
            check_interval = 10  # 10 seconds
            elapsed = 0
            detailed_state = "UNKNOWN"
            
            while elapsed < max_wait_time:
                try:
                    status_info = index.describe()
                    detailed_state = status_info.get('status', {}).get('detailed_state', '')
                    
                    app_logger.info(f"Index status: {detailed_state} (elapsed: {elapsed}s)")
                    
                    # Check if sync is complete
                    if detailed_state in ["ONLINE_NO_PENDING_UPDATE", "ONLINE"]:
                        app_logger.info("Index sync completed successfully")
                        
                        # Get final status info
                        final_status = index.describe()
                        
                        return {
                            "entity_id": entity_id,
                            "entity_name": config['entity_name'],
                            "index_name": index_name,
                            "sync_status": "completed",
                            "index_state": detailed_state,
                            "message": "Vector search index synced successfully",
                            "details": {
                                "endpoint": vs_endpoint,
                                "master_table": f"{self.catalog_name}.gold.{config['entity_name']}_master_prod",
                                "sync_duration_seconds": elapsed,
                                "index_info": {
                                    "delta_sync_index_spec": final_status.get('delta_sync_index_spec', {}),
                                    "status": final_status.get('status', {})
                                }
                            }
                        }
                    
                    # If still syncing, wait and check again
                    time.sleep(check_interval)
                    elapsed += check_interval
                    
                except Exception as e:
                    app_logger.warning(f"Error checking index status: {str(e)}")
                    time.sleep(check_interval)
                    elapsed += check_interval
            
            # Timeout reached
            raise HTTPException(
                status_code=503,
                detail={
                    "error_type": "SYNC_TIMEOUT",
                    "message": f"Index sync did not complete within {max_wait_time} seconds",
                    "detail": {
                        "index_name": index_name,
                        "timeout_seconds": max_wait_time,
                        "last_known_state": detailed_state
                    }
                }
            )
            
        except HTTPException:
            raise
        except Exception as e:
            message = traceback.format_exc()
            app_logger.exception(f"Vector index sync failed: {message}")
            raise HTTPException(
                status_code=502,
                detail={
                    "error_type": "SYNC_FAILED",
                    "message": f"Failed to sync vector search index: {str(e)}",
                    "detail": {"entity_id": entity_id}
                }
            )