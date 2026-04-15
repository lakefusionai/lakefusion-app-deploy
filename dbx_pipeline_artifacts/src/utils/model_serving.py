# Databricks notebook source
# MAGIC %run ./parse_utils

# COMMAND ----------

import logging
if 'logger' not in dir():
    logger = logging.getLogger(__name__)

# COMMAND ----------

as_list = True
resource_tags = get_resource_tags(as_list=as_list)
if resource_tags is None:
    resource_tags = []

# COMMAND ----------

from mlflow.deployments import get_deploy_client

def get_serving_endpoint_status(endpoint_name):

    client = get_deploy_client("databricks")
    endpoint = client.get_endpoint(endpoint=endpoint_name)
    logger.info(f"Endpoint status: {endpoint}")
    if not endpoint.get("state", {}).get("ready") == "READY":
        raise ValueError("Endpoint is not ready.")
    return True

# COMMAND ----------

import time

def wait_until_serving_endpoint_ready(endpoint_name, timeout_minutes=20, check_interval_seconds=10):
    from mlflow.deployments import get_deploy_client
    client = get_deploy_client("databricks")

    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    ready_state = "unknown"
    config_update = "unknown"

    logger.info(f"Waiting for endpoint '{endpoint_name}' to become READY (timeout={timeout_minutes} min)...")

    while time.time() - start_time < timeout_seconds:
        try:
            endpoint = client.get_endpoint(endpoint=endpoint_name)
            state = endpoint.get("state", {})
            ready_state = state.get("ready", "")
            config_update = state.get("config_update", "")
            logger.info(f"Endpoint '{endpoint_name}' state: ready={ready_state}, config_update={config_update}")

            if ready_state.upper() == "READY":
                logger.info(f"Endpoint '{endpoint_name}' is READY!")
                return state

            if config_update.upper() in ("UPDATE_FAILED", "UPDATE_CANCELED"):
                # Extract failure reason from pending_config.served_entities
                failure_reasons = []
                pending_config = endpoint.get("pending_config", {})
                for entity in pending_config.get("served_entities", []):
                    entity_state = entity.get("state", {})
                    msg = entity_state.get("deployment_state_message", "")
                    deployment = entity_state.get("deployment", "")
                    if msg:
                        failure_reasons.append(msg)
                    elif deployment:
                        failure_reasons.append(f"deployment={deployment}")

                reason = "; ".join(failure_reasons) if failure_reasons else "unknown"
                raise RuntimeError(
                    f"Endpoint '{endpoint_name}' failed to deploy. "
                    f"Reason: {reason} "
                    f"(ready={ready_state}, config_update={config_update})"
                )

            logger.info(f"Still waiting... next check in {check_interval_seconds} seconds.")
            time.sleep(check_interval_seconds)

        except RuntimeError:
            raise
        except Exception as e:
            logger.warning(f"Error checking endpoint status: {e}")
            logger.info(f"Retrying in {check_interval_seconds} seconds...")
            time.sleep(check_interval_seconds)

    raise TimeoutError(
        f"Endpoint '{endpoint_name}' did not become READY within {timeout_minutes} minutes. "
        f"Last state: ready={ready_state}, config_update={config_update}"
    )


# COMMAND ----------

import time

def wait_until_vector_search_ready(endpoint_name, client=None, timeout_minutes=20, interval_seconds=10):
    if client is None:
        from utils.vs_utils import create_vs_client
        client = create_vs_client()
    start_time = time.time()
    timeout = timeout_minutes * 60
    state = "unknown"
    message = ""

    while time.time() - start_time < timeout:
        try:
            ep = client.get_endpoint(endpoint_name)
            status = ep.get("endpoint_status", {})
            state = status.get("state", "unknown")
            message = status.get("message", "")
            logger.info(f"VS endpoint '{endpoint_name}' state: {state}, message: {message}")

            if state == "ONLINE":
                logger.info(f"Vector Search endpoint '{endpoint_name}' is ONLINE and ready.")
                return True

            if state == "OFFLINE":
                raise RuntimeError(
                    f"Vector Search endpoint '{endpoint_name}' is OFFLINE: {message}"
                )

        except RuntimeError:
            raise
        except Exception as e:
            if "does not exist" in str(e).lower():
                logger.error(f"Endpoint '{endpoint_name}' does not exist. Please recreate it manually in Databricks.")
                raise
            else:
                logger.warning(f"Error checking VS endpoint status: {e}")

        time.sleep(interval_seconds)

    raise TimeoutError(
        f"Vector Search endpoint '{endpoint_name}' did not become READY within {timeout_minutes} minutes. "
        f"Last state: {state}, message: {message}"
    )

# COMMAND ----------

def create_serving_endpoint(endpoint_name, entity_name, entity_version=1, workload_size="Small", workload_type="CPU", scale_to_zero_enabled=True):

    client = get_deploy_client("databricks")
    endpoint = client.create_endpoint(
        name=endpoint_name,
        config={
            "served_entities": [
                {
                    "entity_name": entity_name,
                    "entity_version": entity_version,
                    "workload_size": workload_size,
                    "workload_type": workload_type,
                    "scale_to_zero_enabled": scale_to_zero_enabled
                }
            ],
            "tags": resource_tags
        }
    )
    wait_until_serving_endpoint_ready(endpoint_name)
    return endpoint

def wait_for_serving_endpoint(client, endpoint_name, timeout=600, interval=10):
    """
    Waits until the Databricks serving endpoint is in 'READY' state.
    Raises RuntimeError if endpoint enters a failed state.
    """
    logger.info(f"Waiting for serving endpoint '{endpoint_name}' to become READY...")

    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            endpoint = client.get_endpoint(endpoint_name)
            ready_state = endpoint.state.ready
            config_update = endpoint.state.config_update
            logger.info(f"Endpoint '{endpoint_name}' state: ready={ready_state}, config_update={config_update}")

            if ready_state == 'READY':
                logger.info(f"Serving endpoint '{endpoint_name}' is READY!")
                return True

            if str(config_update).upper() in ("UPDATE_FAILED", "UPDATE_CANCELED"):
                raise RuntimeError(
                    f"Endpoint '{endpoint_name}' failed with state: ready={ready_state}, config_update={config_update}"
                )

        except RuntimeError:
            raise
        except Exception as err:
            logger.warning(f"Error checking endpoint status: {str(err)}")

        time.sleep(interval)

    raise TimeoutError(f"Endpoint '{endpoint_name}' did not become READY within {timeout} seconds.")


def is_endpoint_healthy(endpoint_name: str) -> bool:
    """
    Check if a serving endpoint exists and is not in a failed state.

    Returns:
        True if the endpoint exists and is not failed, False if it doesn't exist or is failed.
    """
    from mlflow.deployments import get_deploy_client

    try:
        client = get_deploy_client("databricks")
        endpoint = client.get_endpoint(endpoint_name)
    except Exception:
        return False

    state = endpoint.get("state", {})
    config_update = state.get("config_update", "")
    return config_update not in ("UPDATE_FAILED", "UPDATE_CANCELED")


def check_and_cleanup_failed_endpoint(endpoint_name: str) -> bool:
    """
    Check if a serving endpoint exists and determine if it needs creation.

    - If the endpoint doesn't exist -> return True (needs creation).
    - If the endpoint exists and is READY -> return False (skip creation).
    - If the endpoint exists and is IN_PROGRESS -> wait for it to become ready,
      then return False.
    - If the endpoint exists but is FAILED -> delete it and return True (needs creation).

    Returns:
        True if the endpoint was deleted (or didn't exist) — needs creation.
        False if it exists and is ready — skip creation.
    """
    from mlflow.deployments import get_deploy_client

    try:
        client = get_deploy_client("databricks")
        endpoint = client.get_endpoint(endpoint_name)
    except Exception:
        # Endpoint doesn't exist at all
        return True

    state = endpoint.get("state", {})
    ready_state = state.get("ready", "")
    config_update = state.get("config_update", "")

    # Endpoint is fully ready
    if ready_state.upper() == "READY":
        logger.info(f"Endpoint '{endpoint_name}' is healthy and READY -- skipping recreation")
        return False

    # Endpoint is still deploying — wait for it to finish
    if config_update.upper() in ("IN_PROGRESS", "NOT_UPDATING") and ready_state.upper() != "READY":
        logger.info(f"Endpoint '{endpoint_name}' exists but is still deploying (ready={ready_state}, config_update={config_update}) -- waiting...")
        wait_until_serving_endpoint_ready(endpoint_name)
        logger.info(f"Endpoint '{endpoint_name}' is now READY")
        return False

    # Endpoint is in a failed state — delete for retry
    if config_update.upper() in ("UPDATE_FAILED", "UPDATE_CANCELED"):
        logger.warning(f"Endpoint '{endpoint_name}' is in a failed state -- deleting for retry")
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            w.serving_endpoints.delete(endpoint_name)
            import time
            time.sleep(5)  # Brief wait for deletion to propagate
            logger.info(f"Deleted failed endpoint '{endpoint_name}'")
            return True
        except Exception as e:
            logger.error(f"Failed to delete endpoint '{endpoint_name}': {e}")
            return False

    # Unknown state — treat as needing check, wait for it
    logger.warning(f"Endpoint '{endpoint_name}' in unexpected state (ready={ready_state}, config_update={config_update}) -- waiting...")
    wait_until_serving_endpoint_ready(endpoint_name)
    return False


def resolve_entity_name(model_name: str) -> str:
    """
    Resolve a model display name to its UC entity name using the Databricks SDK.

    If the model_name is already a UC path (e.g., system.ai.xxx), returns it as-is.
    Otherwise, looks up the serving endpoint to get the entity name from served_entities.

    Args:
        model_name: Model display/endpoint name (e.g., 'databricks-gte-large-en')
                    or UC entity name (e.g., 'system.ai.gte_large_en_v1_5')

    Returns:
        UC entity name (e.g., 'system.ai.gte_large_en_v1_5')
    """
    if not model_name:
        return model_name

    # Already a UC entity name
    if '.' in model_name:
        return model_name

    # Look up the serving endpoint to get the entity name
    from databricks.sdk import WorkspaceClient
    try:
        w = WorkspaceClient()
        endpoint = w.serving_endpoints.get(model_name)
        if endpoint and endpoint.config and endpoint.config.served_entities:
            served = endpoint.config.served_entities[0]
            # Try entity_name first (custom models), then foundation_model.name
            if served.entity_name:
                return served.entity_name
            elif served.foundation_model and served.foundation_model.name:
                entity_name = served.foundation_model.name
                logger.info(f"Resolved '{model_name}' -> '{entity_name}'")
                return entity_name
    except Exception as e:
        logger.warning(f"Could not resolve entity name for '{model_name}': {e}")

    # Fallback: return as-is
    return model_name

# COMMAND ----------

def create_serving_endpoint_foundational(
    endpoint_name: str,
    serving_entity: str,
    entity_version: int = 1,
    pt_config: dict = None,
) -> dict:
    """
    Create a Databricks serving endpoint for foundation models.

    Modes:
      - pt_type="model_units" -> SDK create_provisioned_throughput_endpoint + poll
      - pt_type="legacy"      -> SDK create + poll
      - None                  -> Raises RuntimeError (pt_config required)
    """
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.serving import EndpointTag

    w = WorkspaceClient()

    # Convert resource_tags to EndpointTag objects
    tags = None
    if resource_tags:
        tags = [
            EndpointTag(key=t.get("key", ""), value=t.get("value", ""))
            for t in resource_tags
            if t.get("key") and t.get("value")
        ]
        if not tags:
            tags = None

    pt_type = pt_config.get("pt_type") if pt_config else None
    # Use entity_version from pt_config if available
    version = pt_config.get("entity_version", entity_version) if pt_config else entity_version

    if pt_type == "model_units":
        from databricks.sdk.service.serving import PtEndpointCoreConfig, PtServedModel

        chunk_size = pt_config["chunk_size"]
        units = pt_config.get("provisioned_model_units", chunk_size)
        logger.info(f"Creating model_units PT endpoint '{endpoint_name}': {units} units for {serving_entity} v{version}")

        try:
            w.serving_endpoints.create_provisioned_throughput_endpoint(
                name=endpoint_name,
                config=PtEndpointCoreConfig(
                    served_entities=[
                        PtServedModel(
                            entity_name=serving_entity,
                            entity_version=version,
                            provisioned_model_units=units
                        )
                    ]
                ),
                tags=tags,
            )
            wait_until_serving_endpoint_ready(endpoint_name)
            logger.info(f"Model units PT endpoint '{endpoint_name}' ready")
            return {"name": endpoint_name}
        except Exception as e:
            raise RuntimeError(
                f"Model units PT endpoint '{endpoint_name}' creation failed: {e}"
            ) from e

    elif pt_type == "legacy":
        from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

        chunk_size = pt_config["chunk_size"]
        min_tp = pt_config.get("min_provisioned_throughput", 0)
        max_tp = pt_config.get("max_provisioned_throughput", chunk_size * 2)
        logger.info(f"Creating legacy PT endpoint '{endpoint_name}': {min_tp}-{max_tp} tokens/sec for {serving_entity} v{version}")

        try:
            w.serving_endpoints.create(
                name=endpoint_name,
                config=EndpointCoreConfigInput(
                    name=endpoint_name,
                    served_entities=[
                        ServedEntityInput(
                            entity_name=serving_entity,
                            entity_version=version,
                            min_provisioned_throughput=min_tp,
                            max_provisioned_throughput=max_tp
                        )
                    ]
                ),
                tags=tags,
            )
            wait_until_serving_endpoint_ready(endpoint_name)
            logger.info(f"Legacy PT endpoint '{endpoint_name}' ready")
            return {"name": endpoint_name}
        except Exception as e:
            raise RuntimeError(
                f"Legacy PT endpoint '{endpoint_name}' creation failed: {e}"
            ) from e

    else:
        raise RuntimeError(
            f"No PT config found for '{serving_entity}'. "
            f"Cannot create endpoint '{endpoint_name}' without provisioned throughput configuration."
        )

# COMMAND ----------

import time

def create_and_sync_vector_index(
    client,
    vs_endpoint,
    source_table,
    index_name,
    primary_key,
    embedding_source_column,
    embedding_endpoint,
    pipeline_type="TRIGGERED",
    sync_computed_embeddings=True,
    registration_timeout=3600,
    ready_timeout=900,
    sync_timeout=3600,
    poll_interval=20,
):
    """
    Create (or attach to) a Databricks Vector Search index and wait
    until it is fully synced and ready for queries.

    Returns:
        index: VectorSearchIndex
    """

    index = None

    # --------------------------------------------------------------
    # 1. CREATE INDEX (ASYNC-SAFE)
    # --------------------------------------------------------------
    try:
        index = client.create_delta_sync_index(
            endpoint_name=vs_endpoint,
            source_table_name=source_table,
            index_name=index_name,
            pipeline_type=pipeline_type,
            primary_key=primary_key,
            embedding_source_column=embedding_source_column,
            embedding_model_endpoint_name=embedding_endpoint,
            sync_computed_embeddings=sync_computed_embeddings,
        )
        logger.info(f"Create request submitted for index: {index_name}")

    except Exception as e:
        msg = str(e)

        if "TEMPORARILY_UNAVAILABLE" in msg or "504" in msg:
            logger.warning("Create request timed out -- creation may still be in progress")
        elif "RESOURCE_ALREADY_EXISTS" in msg:
            logger.info("Index already exists")
        else:
            raise RuntimeError(f"Index creation failed unexpectedly: {e}")

    # --------------------------------------------------------------
    # 2. WAIT FOR REGISTRATION
    # --------------------------------------------------------------
    logger.info("Waiting for index registration...")

    start = time.time()
    while index is None:
        if time.time() - start > registration_timeout:
            raise TimeoutError(
                f"Index '{index_name}' not registered within {registration_timeout}s"
            )

        try:
            index = client.get_index(
                endpoint_name=vs_endpoint,
                index_name=index_name,
            )
            logger.info("Index registered")
            break

        except Exception as e:
            if "RESOURCE_DOES_NOT_EXIST" in str(e):
                time.sleep(poll_interval)
            else:
                raise RuntimeError(f"Unexpected registration error: {e}")

    # --------------------------------------------------------------
    # 3. WAIT UNTIL INDEX IS READY FOR SYNC
    # --------------------------------------------------------------
    logger.info("Waiting for index to become ready for sync...")

    start = time.time()
    while True:
        if time.time() - start > ready_timeout:
            raise TimeoutError("Index did not become ready for sync in time")

        try:
            state = index.describe()["status"]["detailed_state"]
            logger.info(f"  Lifecycle state: {state}")

            if state in (
                "ONLINE_NO_PENDING_UPDATE",
                "ONLINE_PENDING_UPDATE",
                "ONLINE_TRIGGERED_UPDATE",
            ):
                logger.info("Index ready for sync")
                break

            if "FAILED" in state:
                raise RuntimeError(f"Index entered FAILED state: {state}")

        except Exception as e:
            logger.warning(f"  Error checking readiness: {e}")

        time.sleep(poll_interval)

    # --------------------------------------------------------------
    # 4. TRIGGER SYNC
    # --------------------------------------------------------------
    logger.info("Triggering index sync...")
    index.sync()
    time.sleep(poll_interval)

    # --------------------------------------------------------------
    # 5. WAIT FOR SYNC COMPLETION
    # --------------------------------------------------------------
    logger.info("Waiting for index to fully sync...")

    start = time.time()
    consecutive_errors = 0

    while True:
        if time.time() - start > sync_timeout:
            raise TimeoutError("Index sync exceeded maximum wait time")

        try:
            info = index.describe()
            state = info["status"]["detailed_state"]
            msg = info["status"].get("message", "")

            logger.info(f"  Index state: {state}")
            if msg:
                logger.info(f"    Message: {msg}")

            consecutive_errors = 0

            if state == "ONLINE_NO_PENDING_UPDATE":
                logger.info("Index fully synced and ready")
                return index

            if "FAILED" in state:
                raise RuntimeError(
                    f"Index sync failed: {state}. Message: {msg}"
                )

        except Exception as e:
            consecutive_errors += 1
            logger.warning(
                f"  Backend error while polling "
                f"({consecutive_errors}/10): {e}"
            )
            if consecutive_errors >= 10:
                raise RuntimeError("Vector Search backend unstable")

        time.sleep(poll_interval)

# COMMAND ----------

def _retry_with_backoff(
    endpoint_name: str,
    endpoint_type: str,
    execute_fn,
    max_retries: int = 10,
    retry_interval_seconds: int = 60,
    timeout_minutes: int = 10
):
    """
    Internal function: Common retry logic for warming up model serving endpoints.
    Handles scale-from-zero by retrying on transient errors until success or timeout.

    Args:
        endpoint_name: Name of the endpoint being warmed up (for logging)
        endpoint_type: Type of endpoint - "embedding" or "LLM" (for logging)
        execute_fn: Callable that triggers the endpoint (should raise on failure)
        max_retries: Maximum number of retry attempts (default: 10)
        retry_interval_seconds: Seconds to wait between retries (default: 60)
        timeout_minutes: Total timeout in minutes (default: 10)

    Returns:
        True if warm-up successful

    Raises:
        RuntimeError: If warm-up fails after all retries or timeout
    """
    import time

    start_time = time.time()
    timeout_seconds = timeout_minutes * 60

    logger.info(f"Warming up {endpoint_type} endpoint '{endpoint_name}' (scale-from-zero handling)")
    logger.info(f"   Max retries: {max_retries}, Retry interval: {retry_interval_seconds}s, Timeout: {timeout_minutes} min")

    last_error = None

    for attempt in range(1, max_retries + 1):
        # Check timeout
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            raise RuntimeError(
                f"Warm-up timeout: {endpoint_type} endpoint '{endpoint_name}' "
                f"did not respond within {timeout_minutes} minutes. Last error: {last_error}"
            )

        try:
            logger.info(f"   Attempt {attempt}/{max_retries}: Processing warm-up record...")

            # Execute the warm-up function
            execute_fn()

            logger.info(f"Warm-up successful! {endpoint_type} endpoint '{endpoint_name}' is ready.")
            return True

        except Exception as e:
            last_error = str(e)
            error_lower = last_error.lower()

            # Log FULL error message for diagnosis (helps identify scale-to-zero error patterns)
            logger.debug(f"   Full error message: {last_error}")
            logger.debug(f"   Error type: {type(e).__name__}")

            # Check for transient scale-from-zero related errors
            transient_keywords = [
                "503", "service unavailable", "timeout", "temporarily unavailable",
                "scaling", "scale to zero", "not ready", "connection",
                "rate limit", "throttl", "capacity", "overloaded", "starting",
                "pending", "provisioning", "initializing", "warming"
            ]

            matched_keywords = [kw for kw in transient_keywords if kw in error_lower]
            is_transient = len(matched_keywords) > 0

            if is_transient:
                logger.info(f"   Transient error (attempt {attempt}/{max_retries})")
                logger.debug(f"   Matched transient keywords: {matched_keywords}")

                if attempt < max_retries:
                    remaining_time = timeout_seconds - (time.time() - start_time)
                    wait_time = min(retry_interval_seconds, remaining_time)

                    if wait_time > 0:
                        logger.info(f"   Waiting {int(wait_time)}s before retry...")
                        time.sleep(wait_time)
                    else:
                        raise RuntimeError(
                            f"Warm-up timeout: {endpoint_type} endpoint '{endpoint_name}' "
                            f"did not respond within {timeout_minutes} minutes. Last error: {last_error}"
                        )
                else:
                    raise RuntimeError(
                        f"Warm-up failed: {endpoint_type} endpoint '{endpoint_name}' "
                        f"did not respond after {max_retries} attempts. Last error: {last_error}"
                    )
            else:
                # Non-transient error - fail immediately
                logger.debug(f"   No transient keywords matched - treating as non-transient error")
                logger.debug(f"   Checked keywords: {transient_keywords}")
                raise RuntimeError(
                    f"Warm-up failed with non-transient error on {endpoint_type} endpoint "
                    f"'{endpoint_name}': {last_error}"
                )

    # Should not reach here, but just in case
    raise RuntimeError(
        f"Warm-up failed: {endpoint_type} endpoint '{endpoint_name}' "
        f"did not respond after {max_retries} attempts. Last error: {last_error}"
    )

# COMMAND ----------

def warm_up_llm_endpoint(
    spark,
    endpoint_name: str,
    warmup_query: str,
    max_retries: int = 10,
    retry_interval_seconds: int = 60,
    timeout_minutes: int = 10
):
    """
    Warm up an LLM model serving endpoint by executing a SQL query with ai_query().
    Handles scale-from-zero by retrying until the endpoint responds successfully.

    Args:
        spark: SparkSession instance
        endpoint_name: Name of the LLM endpoint being warmed up
        warmup_query: SQL query string that uses ai_query() with LIMIT 1
        max_retries: Maximum number of retry attempts (default: 10)
        retry_interval_seconds: Seconds to wait between retries (default: 60)
        timeout_minutes: Total timeout in minutes (default: 10)

    Returns:
        True if warm-up successful

    Raises:
        RuntimeError: If warm-up fails after all retries or timeout

    Example:
        warmup_query = f'''
        SELECT ai_query('{llm_endpoint}', 'Say hello')
        FROM {table} LIMIT 1
        '''
        warm_up_llm_endpoint(spark, llm_endpoint, warmup_query)
    """
    def execute_fn():
        # Using collect() since LIMIT 1 ensures minimal data transfer
        # Note: foreach() doesn't work with Spark Connect
        spark.sql(warmup_query).collect()

    return _retry_with_backoff(
        endpoint_name=endpoint_name,
        endpoint_type="LLM",
        execute_fn=execute_fn,
        max_retries=max_retries,
        retry_interval_seconds=retry_interval_seconds,
        timeout_minutes=timeout_minutes
    )

# COMMAND ----------

def sync_index_with_retry(
    index,
    embedding_endpoint_name: str,
    max_retries: int = 10,
    retry_interval_seconds: int = 60,
    timeout_minutes: int = 10
):
    """
    Sync a Vector Search index with retry logic for scale-from-zero handling.

    The index sync operation uses the embedding endpoint to generate embeddings
    for source data. If the embedding endpoint is scaled to zero, this function
    retries until the endpoint spins up and responds successfully.

    Args:
        index: VectorSearchIndex object to sync
        embedding_endpoint_name: Name of the embedding endpoint (for logging)
        max_retries: Maximum number of retry attempts (default: 10)
        retry_interval_seconds: Seconds to wait between retries (default: 60)
        timeout_minutes: Total timeout in minutes (default: 10)

    Returns:
        True if sync initiated successfully

    Raises:
        RuntimeError: If sync fails after all retries or timeout

    Example:
        from databricks.vector_search.client import VectorSearchClient
        client = VectorSearchClient()
        index = client.get_index(endpoint_name=vs_endpoint, index_name=index_name)
        sync_index_with_retry(index, embedding_endpoint_name=embedding_endpoint)
    """
    def execute_fn():
        index.sync()

    return _retry_with_backoff(
        endpoint_name=embedding_endpoint_name,
        endpoint_type="embedding",
        execute_fn=execute_fn,
        max_retries=max_retries,
        retry_interval_seconds=retry_interval_seconds,
        timeout_minutes=timeout_minutes
    )

# COMMAND ----------

def warm_up_embedding_endpoint(
    embedding_endpoint: str,
    max_retries: int = 10,
    retry_interval_seconds: int = 60,
    timeout_minutes: int = 10
):
    """
    Warm up an embedding endpoint by sending a test embedding request.

    This should be called BEFORE index creation to ensure the embedding endpoint
    is hot and ready. Index creation validates the embedding endpoint, and if
    the endpoint is scaled to zero, the creation will fail.

    Args:
        embedding_endpoint: Name of the embedding model serving endpoint
        max_retries: Maximum number of retry attempts (default: 10)
        retry_interval_seconds: Seconds to wait between retries (default: 60)
        timeout_minutes: Total timeout in minutes (default: 10)

    Returns:
        True if warm-up successful

    Raises:
        RuntimeError: If warm-up fails after all retries or timeout

    Example:
        warm_up_embedding_endpoint(embedding_endpoint="databricks-gte-large-en")
        # Now safe to create index
        index = client.create_delta_sync_index(...)
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    def execute_fn():
        # Use ai_query() via Spark SQL to warm up the embedding endpoint
        # This is the same approach used for LLM warm-up
        warmup_query = f"SELECT ai_query('{embedding_endpoint}', 'warm up test') LIMIT 1"
        spark.sql(warmup_query).collect()
        return True

    return _retry_with_backoff(
        endpoint_name=embedding_endpoint,
        endpoint_type="embedding",
        execute_fn=execute_fn,
        max_retries=max_retries,
        retry_interval_seconds=retry_interval_seconds,
        timeout_minutes=timeout_minutes
    )
