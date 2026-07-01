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
    _spn_client_id = dbutils.secrets.get(scope=SECRET_SCOPE_NAME, key="lakefusion_spn")
    _spn_client_secret = dbutils.secrets.get(scope=SECRET_SCOPE_NAME, key="lakefusion_spn_secret")
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

# COMMAND ----------

def log_sync_progress(idx, desc=None, endpoint_type="STANDARD", source_row_count=None):
    """Log Vector Search index sync progress. Works for both STANDARD and
    STORAGE_OPTIMIZED endpoints.

    `idx.describe()` returns the raw control-plane JSON. The stable fields present for
    BOTH endpoint types are: status.detailed_state, status.ready, status.indexed_row_count,
    status.message, status.index_url. The rich progress / throughput blocks
    (provisioning_status.initial_pipeline_sync_progress, triggered_update_status.
    triggered_update_progress, pipeline_metrics) are Delta-Live-Tables managed-pipeline
    extras that only STANDARD endpoints populate; STORAGE_OPTIMIZED endpoints support only
    TRIGGERED sync and partially rebuild the index, so those blocks are typically absent.

    When the rich progress block is missing (storage-optimized), fall back to
    indexed_row_count / source_row_count for a meaningful progress estimate.

    Args:
        idx: VectorSearchIndex object.
        desc: Optional pre-fetched idx.describe() result (avoids an extra call).
        endpoint_type: "STANDARD" or "STORAGE_OPTIMIZED" (for labelling + fallback logic).
        source_row_count: Total source rows; used for the storage-optimized progress fallback.
    """
    try:
        if desc is None:
            desc = idx.describe()
        st = desc.get("status", {})
        detailed_state = st.get("detailed_state", "UNKNOWN")
        indexed_rows = st.get("indexed_row_count")  # absent for STORAGE_OPTIMIZED -> None
        ready = st.get("ready", False)

        # Rich progress lives in different places depending on sync phase (STANDARD only):
        #   - Initial snapshot: status.provisioning_status.initial_pipeline_sync_progress
        #   - Triggered update: status.triggered_update_status.triggered_update_progress
        prov = (
            st.get("triggered_update_status", {}).get("triggered_update_progress")
            or st.get("provisioning_status", {}).get("initial_pipeline_sync_progress")
            or {}
        )

        logger.info("=" * 70)
        logger.info(f"Index:         {desc.get('name')}")
        logger.info(f"Endpoint type: {endpoint_type}")
        logger.info(f"State:         {detailed_state}  (ready={ready})")

        if prov:
            # Percent + ETA are present for BOTH endpoint types (storage-optimized exposes them via
            # triggered_update_progress / initial_pipeline_sync_progress). Row counts
            # (total_rows_to_sync / num_synced_rows / indexed_row_count) are STANDARD-only — log the
            # row breakdown only when those fields actually exist, else just the percent.
            pct = prov.get("sync_progress_completion", 0.0)
            eta_sec = prov.get("estimated_completion_time_seconds", 0.0)
            current_version = prov.get("latest_version_currently_processing")
            has_row_counts = "total_rows_to_sync" in prov

            if current_version is not None:
                logger.info(f"Version:       {current_version}")
            logger.info("-" * 70)
            if has_row_counts:
                total_rows = prov.get("total_rows_to_sync", 0)
                synced_rows = prov.get("num_synced_rows", 0)
                logger.info(f"Progress:      {synced_rows:,} / {total_rows:,}  ({pct * 100:.2f}%)")
                if indexed_rows is not None:
                    logger.info(f"Indexed rows:  {indexed_rows:,}   (queryable snapshot)")
                logger.info(f"Remaining:     {total_rows - synced_rows:,} rows")
            else:
                logger.info(f"Progress:      {pct * 100:.2f}%")
            if eta_sec and eta_sec > 0:
                logger.info(f"ETA:           {eta_sec:.0f}s  (~{eta_sec / 60:.1f} min)")
                logger.info(f"               Expected finish: "
                            f"{time.strftime('%H:%M:%S', time.localtime(time.time() + eta_sec))}")

            metrics = prov.get("pipeline_metrics", {})
            ms_per_row = metrics.get("total_sync_time_per_row_ms", 0)
            if ms_per_row and ms_per_row > 0:
                ing = metrics.get("ingestion_metrics", {})
                emb = metrics.get("embedding_metrics", {})
                logger.info("-" * 70)
                logger.info(f"Throughput:    {ms_per_row:.2f} ms/row  (~{1000 / ms_per_row:.0f} rows/sec)")
                logger.info(f"  Ingestion:   {ing.get('ingestion_time_per_row_ms', 0):.2f} ms/row, "
                            f"batch={ing.get('ingestion_batch_size')}")
                logger.info(f"  Embedding:   {emb.get('embedding_generation_time_per_row_ms', 0):.2f} ms/row, "
                            f"batch={emb.get('embedding_generation_batch_size')}")

            # Commit lag for triggered updates
            if detailed_state in ("ONLINE_TRIGGERED_UPDATE", "ONLINE_UPDATING_PIPELINE_RESOURCES"):
                tus = st.get("triggered_update_status", {})
                last_v = tus.get("last_processed_commit_version")
                if last_v is not None and current_version is not None:
                    logger.info(f"Commit lag:    processed={last_v}, current={current_version} "
                                f"({current_version - last_v} behind)")
        else:
            # No progress block at all: show indexed_row_count if the endpoint reports it, else a
            # provisioning note (storage-optimized omits indexed_row_count during provisioning).
            logger.info("-" * 70)
            if indexed_rows is not None:
                if source_row_count:
                    pct = (indexed_rows / source_row_count * 100) if source_row_count else 0.0
                    logger.info(f"Indexed rows:  {indexed_rows:,} / {source_row_count:,}  ({pct:.2f}%)")
                else:
                    logger.info(f"Indexed rows:  {indexed_rows:,}   (queryable snapshot)")
            else:
                logger.info("Provisioning / syncing… (row count not reported for this endpoint)")
            msg = st.get("message", "")
            if msg:
                logger.info(f"Message:       {msg}")
        logger.info("=" * 70)
    except Exception as e:
        logger.debug(f"log_sync_progress failed (non-fatal): {e}")

# COMMAND ----------

# =====================================================================================
# Vector Search index lifecycle (create -> register -> provision-wait -> sync -> sync-wait)
# Consolidated here so every VS notebook (normal_deduplication / golden_deduplication /
# match_maven) shares ONE implementation instead of duplicating ~250 lines each.
# Handles STANDARD and STORAGE_OPTIMIZED endpoints and managed / precomputed embedding modes.
# Requires model_serving (%run) for warm_up_embedding_endpoint / sync_index_with_retry.
# =====================================================================================

# Tunable lifecycle constants (override per call via create_and_sync_index kwargs).
VS_REGISTRATION_TIMEOUT = 1800        # 30 min for the index to register / become accessible
VS_MAX_WAIT_SECONDS = 3 * 60 * 60     # 3 hr ceiling per wait phase (resets while making progress)
VS_POLL_INTERVAL = 20                 # seconds between status polls
VS_MAX_PIPELINE_FAILURES = 3          # tolerate transient ONLINE_PIPELINE_FAILED up to this many times
VS_MAX_BACKEND_ERRORS = 10            # tolerate transient describe()/backend errors up to this many

# detailed_state values that mean "past provisioning — safe to proceed to sync"
_VS_POST_PROVISIONING_STATES = (
    "ONLINE_NO_PENDING_UPDATE", "ONLINE_TRIGGERED_UPDATE",
    "ONLINE_UPDATING_PIPELINE_RESOURCES", "ONLINE_PIPELINE_FAILED",
)
# states meaning a sync/update is already running (don't re-trigger)
_VS_SYNC_IN_PROGRESS_STATES = (
    "ONLINE_TRIGGERED_UPDATE", "ONLINE_UPDATING_PIPELINE_RESOURCES", "ONLINE_PIPELINE_FAILED",
)


def index_ui_url(index_name):
    """Build the Catalog-Explorer URL for a 3-level UC index name (catalog.schema.table)."""
    try:
        parts = index_name.split(".")
        if len(parts) == 3 and _workspace_url:
            cat, sch, tbl = parts
            return f"{_workspace_url}/explore/data/{cat}/{sch}/{tbl}"
    except Exception:
        pass
    return None


def create_delta_sync_index_tolerant(client, *, endpoint_name, index_name, source_table,
                                      primary_key, embedding_mode,
                                      embedding_vector_column=None, embedding_dimension=None,
                                      embedding_source_column=None, embedding_model_endpoint=None):
    """Create a TRIGGERED Delta-Sync index, tolerating create-time races.

    Returns the index handle if create() returned one, else None (caller waits for registration).
    Reraises only on permanent errors (quota / permission / unexpected).
      precomputed -> embedding_vector_column + embedding_dimension
      managed     -> embedding_source_column + embedding_model_endpoint (sync_computed_embeddings)
    """
    try:
        logger.info(f"Attempting to create index: {index_name}")
        if embedding_mode == "precomputed":
            idx = client.create_delta_sync_index(
                endpoint_name=endpoint_name,
                source_table_name=source_table,
                index_name=index_name,
                pipeline_type="TRIGGERED",
                primary_key=primary_key,
                embedding_vector_column=embedding_vector_column,
                embedding_dimension=embedding_dimension,
            )
        else:
            idx = client.create_delta_sync_index(
                endpoint_name=endpoint_name,
                source_table_name=source_table,
                index_name=index_name,
                pipeline_type="TRIGGERED",
                primary_key=primary_key,
                embedding_source_column=embedding_source_column,
                embedding_model_endpoint_name=embedding_model_endpoint,
                sync_computed_embeddings=True,
            )
        logger.info(f"Create request submitted for index: {index_name}")
        return idx
    except Exception as e:
        error_msg = str(e)
        low = error_msg.lower()
        # Timeout / temporarily unavailable — creation may still be in progress; wait for registration.
        if ("temporarily_unavailable" in low or "504" in low
                or "taking too long" in low or "timeout" in low):
            logger.warning(f"Create request timed out — creation may still be in progress: {error_msg}")
            return None
        # Already exists — fine, proceed to sync.
        if "already exists" in low:
            logger.info(f"Index already exists, proceeding to sync: {error_msg}")
            return None
        # Permanent errors — fail.
        if ("maximum number of indexes" in low or "permission denied" in low
                or "unauthorized" in low):
            raise RuntimeError(f"Index creation failed: {error_msg}")
        raise RuntimeError(f"Index creation failed unexpectedly: {error_msg}")


def wait_for_index_registration(client, *, endpoint_name, index_name, index=None,
                                timeout=VS_REGISTRATION_TIMEOUT, poll=VS_POLL_INTERVAL):
    """Poll client.get_index until the index is registered/accessible. Returns the index handle."""
    if index is not None:
        return index
    logger.info("Waiting for index registration...")
    start = time.time()
    while index is None:
        if time.time() - start > timeout:
            raise TimeoutError(f"Index '{index_name}' not registered within {timeout}s")
        try:
            index = client.get_index(endpoint_name=endpoint_name, index_name=index_name)
            logger.info("Index is now registered and accessible")
        except Exception as e:
            low = str(e).lower()
            if "resource_does_not_exist" in low or "not exist" in low or "not_exist" in low:
                logger.info("  Index not visible yet — waiting...")
                time.sleep(poll)
            else:
                raise RuntimeError(f"Unexpected error while locating index: {e}")
    return index


def wait_until_post_provisioning(index, *, index_name, endpoint_type, source_row_count=None,
                                 max_wait=VS_MAX_WAIT_SECONDS, poll=VS_POLL_INTERVAL):
    """Wait until the index leaves the pure-provisioning phase (any post-provisioning detailed_state)."""
    logger.info("Waiting for index to be ready before sync...")
    wait_start = time.time()
    while True:
        if time.time() - wait_start > max_wait:
            raise TimeoutError(
                f"Index '{index_name}' did not reach a post-provisioning state within {max_wait}s"
            )
        log_sync_progress(index, endpoint_type=endpoint_type, source_row_count=source_row_count)
        try:
            desc = index.describe()
            st = desc.get("status", {}).get("detailed_state", "UNKNOWN")
            if st in _VS_POST_PROVISIONING_STATES:
                break
        except Exception as e:
            logger.warning(f"describe() failed during pre-sync wait (continuing): {e}")
        time.sleep(poll)
    logger.info("Index is ready")


def trigger_sync_if_needed(index, *, embedding_endpoint, max_retries=10,
                           retry_interval_seconds=60, timeout_minutes=10):
    """Trigger a sync unless one is already running (can't sync while RUNNING). Uses the
    scale-from-zero-tolerant sync_index_with_retry (from model_serving)."""
    index_status = index.describe()['status']
    current_status = index_status.get('detailed_state', 'UNKNOWN')
    logger.info(f"Current index status: {current_status}")
    if current_status == "ONLINE_PIPELINE_FAILED":
        logger.warning(f"Pipeline failure details: {index_status.get('message', 'No message available')}")
        logger.warning(f"  Full index status: {index_status}")
    if current_status in _VS_SYNC_IN_PROGRESS_STATES:
        logger.info("Sync already in progress or failed. Skipping sync trigger...")
        return
    logger.info("Triggering index sync (with scale-from-zero retry handling)...")
    try:
        sync_index_with_retry(
            index=index,
            embedding_endpoint_name=embedding_endpoint,
            max_retries=max_retries,
            retry_interval_seconds=retry_interval_seconds,
            timeout_minutes=timeout_minutes,
        )
        logger.info("Index sync initiated successfully!")
    except Exception as sync_error:
        error_msg = f"ERROR: Index sync failed: {str(sync_error)}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)


def wait_until_synced(index, *, endpoint_type, source_row_count=None, max_wait=VS_MAX_WAIT_SECONDS,
                      poll=VS_POLL_INTERVAL, max_pipeline_failures=VS_MAX_PIPELINE_FAILURES,
                      max_backend_errors=VS_MAX_BACKEND_ERRORS):
    """Tolerant wait until the index reaches ONLINE_NO_PENDING_UPDATE. Resets the timeout while the
    index is actively updating; tolerates transient backend errors and ONLINE_PIPELINE_FAILED bounces."""
    logger.info("Waiting for index to be ready and synced (tolerant mode)...")
    start_time = time.time()
    consecutive_backend_errors = 0
    pipeline_failed_count = 0
    while True:
        try:
            index.wait_until_ready()

            status_info = index.describe()
            status = status_info["status"]["detailed_state"]
            message = status_info["status"].get("message", "")

            logger.info(f"  Index status: {status}")
            log_sync_progress(index, desc=status_info, endpoint_type=endpoint_type,
                              source_row_count=source_row_count)
            if message:
                logger.info(f"    Message: {message}")

            consecutive_backend_errors = 0

            # Success
            if status == "ONLINE_NO_PENDING_UPDATE":
                logger.info("Index is fully synced and ready.")
                break

            # Actively updating — reset timer (no timeout while making progress)
            if status in ("ONLINE_TRIGGERED_UPDATE", "ONLINE_UPDATING_PIPELINE_RESOURCES"):
                start_time = time.time()
                logger.info("  Sync in progress, resetting timeout timer...")

            # Transient failure handling
            if status == "ONLINE_PIPELINE_FAILED":
                pipeline_failed_count += 1
                logger.info(
                    f"⚠️ ONLINE_PIPELINE_FAILED detected "
                    f"({pipeline_failed_count}/{max_pipeline_failures})"
                )
                logger.warning(f"  Failure reason: {message}")

                elapsed = time.time() - start_time
                if elapsed > max_wait:
                    raise TimeoutError(
                        f"Index sync timed out after {int(elapsed)} seconds in failed state"
                    )
                if pipeline_failed_count >= max_pipeline_failures:
                    raise RuntimeError(
                        f"Index entered ONLINE_PIPELINE_FAILED state too many times. Last message: {message}"
                    )

        except Exception as e:
            if isinstance(e, (TimeoutError, RuntimeError)):
                raise
            consecutive_backend_errors += 1
            logger.info(
                f"⚠️ Backend error tolerated "
                f"({consecutive_backend_errors}/{max_backend_errors}): {e}"
            )
            if consecutive_backend_errors >= max_backend_errors:
                raise RuntimeError(
                    "Too many consecutive backend errors while waiting for index readiness"
                )

        time.sleep(poll)


def create_and_sync_index(client, spark, *, endpoint_name, index_name, source_table, primary_key,
                          embedding_mode, embedding_vector_column=None, embedding_dimension=None,
                          embedding_source_column=None, embedding_model_endpoint=None,
                          endpoint_type="STANDARD", warm_up=True,
                          registration_timeout=VS_REGISTRATION_TIMEOUT, max_wait=VS_MAX_WAIT_SECONDS,
                          poll=VS_POLL_INTERVAL, max_pipeline_failures=VS_MAX_PIPELINE_FAILURES,
                          max_backend_errors=VS_MAX_BACKEND_ERRORS):
    """Full VS index lifecycle; returns a ready (synced) index handle.

    Steps: warm up embedding endpoint (managed only) -> tolerant create -> wait for registration ->
    wait past provisioning -> trigger sync if needed -> wait until ONLINE_NO_PENDING_UPDATE.
    Handles STANDARD/STORAGE_OPTIMIZED and managed/precomputed. For storage-optimized it counts the
    source table once to label progress. Requires model_serving (%run) for warm_up_embedding_endpoint
    and sync_index_with_retry.
    """
    # Warm up the embedding endpoint (managed mode only; precomputed syncs pre-existing vectors).
    if warm_up and embedding_mode == "managed":
        logger.info("Warming up embedding endpoint before index creation...")
        try:
            warm_up_embedding_endpoint(
                embedding_endpoint=embedding_model_endpoint,
                max_retries=10, retry_interval_seconds=60, timeout_minutes=10,
            )
            logger.info(f"Embedding endpoint '{embedding_model_endpoint}' is warm and ready")
        except Exception as e:
            logger.warning(f"Embedding warm-up failed: {e}. Proceeding with index creation anyway...")
    else:
        logger.info("Skipping embedding warm-up (precomputed mode — index syncs pre-existing vectors)")

    index = create_delta_sync_index_tolerant(
        client, endpoint_name=endpoint_name, index_name=index_name, source_table=source_table,
        primary_key=primary_key, embedding_mode=embedding_mode,
        embedding_vector_column=embedding_vector_column, embedding_dimension=embedding_dimension,
        embedding_source_column=embedding_source_column, embedding_model_endpoint=embedding_model_endpoint,
    )
    index = wait_for_index_registration(
        client, endpoint_name=endpoint_name, index_name=index_name, index=index,
        timeout=registration_timeout, poll=poll,
    )

    url = index_ui_url(index_name)
    if url:
        logger.info(f"Index URL: {url}")

    # Storage-optimized endpoints don't report row counts during sync; a source count lets
    # log_sync_progress show a meaningful number once indexed_row_count appears.
    source_row_count = None
    if endpoint_type == "STORAGE_OPTIMIZED":
        try:
            source_row_count = spark.table(source_table).count()
        except Exception as e:
            logger.warning(f"Could not count {source_table} for progress estimate: {e}")

    wait_until_post_provisioning(
        index, index_name=index_name, endpoint_type=endpoint_type,
        source_row_count=source_row_count, max_wait=max_wait, poll=poll,
    )
    trigger_sync_if_needed(index, embedding_endpoint=embedding_model_endpoint)
    wait_until_synced(
        index, endpoint_type=endpoint_type, source_row_count=source_row_count,
        max_wait=max_wait, poll=poll, max_pipeline_failures=max_pipeline_failures,
        max_backend_errors=max_backend_errors,
    )
    return index
