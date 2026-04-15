# Databricks notebook source
# Vector Search utility functions

# COMMAND ----------

import random
import time
import logging
if 'logger' not in dir():
    logger = logging.getLogger(__name__)

# COMMAND ----------

# Load Service Principal credentials for network-optimized Vector Search route
_spn_client_id = None
_spn_client_secret = None
try:
    _spn_client_id = dbutils.secrets.get(scope="lakefusion", key="lakefusion_spn")
    _spn_client_secret = dbutils.secrets.get(scope="lakefusion", key="lakefusion_spn_secret")
    if _spn_client_id and _spn_client_secret:
        logger.info("Service Principal credentials loaded -- using network-optimized route")
    else:
        _spn_client_id = None
        _spn_client_secret = None
        logger.warning("Service Principal credentials empty -- using default authentication")
except Exception:
    logger.warning("Service Principal credentials not configured -- using default authentication")
    _spn_client_id = None
    _spn_client_secret = None

# COMMAND ----------

_workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)

# COMMAND ----------

def create_vs_client(**kwargs):
    """
    Create VectorSearchClient with Service Principal OAuth if credentials are configured
    in Databricks secret scope (lakefusion:lakefusion_spn, lakefusion:lakefusion_spn_secret).
    Falls back to default notebook authentication if credentials are not available.

    SP OAuth enables the network-optimized route for Vector Search, which provides
    isolated rate limits and avoids impacting other workspace users.
    """
    from databricks.vector_search.client import VectorSearchClient
    if _spn_client_id and _spn_client_secret:
        logger.info(f"[VS Client] Using SPN OAuth (network-optimized route) | workspace: {_workspace_url}")
        client = VectorSearchClient(
            workspace_url=_workspace_url,
            service_principal_client_id=_spn_client_id,
            service_principal_client_secret=_spn_client_secret,
            **kwargs
        )
        logger.debug(f"[VS Client] _is_notebook_pat: {client._is_notebook_pat} | _using_user_passed_credentials: {client._using_user_passed_credentials}")
        return client
    logger.info("[VS Client] Using default PAT authentication (standard route)")
    return VectorSearchClient(**kwargs)

# COMMAND ----------

def similarity_search_with_retry(index, query_text, columns, num_results,
                                  max_retries=5, initial_delay=1.0,
                                  max_delay=30.0, backoff_factor=2.0,
                                  jitter_factor=0.5):
    """
    Wrapper around index.similarity_search() with exponential backoff and jitter.

    Retries on transient errors (429 rate limits, 5xx server errors, timeouts,
    connection errors). Non-retriable errors are raised immediately.

    Args:
        index: VectorSearchIndex object
        query_text: Text to search for
        columns: Columns to return from the index
        num_results: Number of results to return
        max_retries: Maximum number of retry attempts (default 5)
        initial_delay: Initial delay in seconds before first retry (default 1.0)
        max_delay: Maximum delay cap in seconds (default 30.0)
        backoff_factor: Multiplier for exponential backoff (default 2.0)
        jitter_factor: Random jitter as fraction of delay, 0-1 (default 0.5)
    """
    for attempt in range(max_retries + 1):
        try:
            return index.similarity_search(
                query_text=query_text, columns=columns, num_results=num_results
            )
        except Exception as e:
            error_str = str(e).lower()
            is_retriable = any(term in error_str for term in
                ['429', 'rate limit', '500', '502', '503', '504',
                 'timeout', 'unavailable', 'connection', 'temporary'])
            if not is_retriable or attempt == max_retries:
                raise
            delay = min(initial_delay * (backoff_factor ** attempt), max_delay)
            sleep_time = delay + delay * random.uniform(0, jitter_factor)
            logger.info(f"[VS Retry] Attempt {attempt+1}/{max_retries+1} failed. "
                  f"Retrying in {sleep_time:.1f}s...")
            time.sleep(sleep_time)
