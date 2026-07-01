# Databricks notebook source
# DBTITLE 1,Widgets
dbutils.widgets.text("entity_id", "", "Entity ID")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("catalog_name", "", "Catalog Name")

# COMMAND ----------

# DBTITLE 1,Read Task Values
embedding_mode = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="embedding_mode",
    debugValue="managed"
)

# COMMAND ----------

# DBTITLE 1,Early Exit for Managed Mode
if embedding_mode != "precomputed":
    dbutils.notebook.exit("Skipping — managed mode (no precomputed embeddings needed)")

# COMMAND ----------

import json
from pyspark.sql.functions import col, lit, sha2, concat, current_timestamp, when
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Read Configuration from Task Values
entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity")
catalog_name = dbutils.widgets.get("catalog_name")
experiment_id = dbutils.widgets.get("experiment_id")

# Resolve embedding endpoint (same precedence as VS notebook)
embedding_endpoint = dbutils.jobs.taskValues.get(
    taskKey="Endpoints_Mapping",
    key="embedding_model_endpoint",
    debugValue=None
)
if not embedding_endpoint:
    embedding_endpoint = dbutils.jobs.taskValues.get(
        taskKey="Parse_Entity_Model_JSON",
        key="embedding_model_endpoint"
    )

# COMMAND ----------

# MAGIC %run ../utils/execute_utils

# COMMAND ----------

# MAGIC %run ../utils/model_serving

# COMMAND ----------

# Resolve endpoint name to stable UC entity name for cache key
# This ensures cache hits even if the endpoint is renamed or a PT version is created
embedding_model_id = resolve_entity_name(embedding_endpoint)

logger.info("=" * 60)
logger.info("COMPUTE EMBEDDINGS — PRECOMPUTED MODE")
logger.info("=" * 60)
logger.info(f"  Entity:             {entity}")
logger.info(f"  Catalog:            {catalog_name}")
logger.info(f"  Experiment:         {experiment_id}")
logger.info(f"  Embedding Endpoint: {embedding_endpoint}")
logger.info(f"  Resolved Model ID:  {embedding_model_id}")

# COMMAND ----------

# DBTITLE 1,Resolve Table Names
master_table = f"{catalog_name}.gold.{entity}_master"
unified_table = f"{catalog_name}.silver.{entity}_unified"
unified_dedup_table = f"{catalog_name}.silver.{entity}_unified_deduplicate"
embeddings_table = f"{catalog_name}.silver.{entity}_embeddings"

if experiment_id:
    master_table += f"_{experiment_id}"
    unified_table += f"_{experiment_id}"
    unified_dedup_table += f"_{experiment_id}"

# Check existence of every source table up front. Single-source entities (and some
# experiment runs) have no unified table, and golden dedup's unified_deduplicate isn't
# enabled for every entity — so each downstream section is guarded and skipped when its
# table is missing instead of failing with TABLE_OR_VIEW_NOT_FOUND.
master_exists = spark.catalog.tableExists(master_table)
unified_exists = spark.catalog.tableExists(unified_table)
unified_dedup_exists = spark.catalog.tableExists(unified_dedup_table)

logger.info(f"  Master Table:       {master_table} (exists={master_exists})")
logger.info(f"  Unified Table:      {unified_table} (exists={unified_exists})")
logger.info(f"  Unified Dedup Table: {unified_dedup_table} (exists={unified_dedup_exists})")
logger.info(f"  Embeddings Table:   {embeddings_table}")

# Default stats so the final summary / exit payload still works when a section is
# skipped (e.g. single-source entities have no unified table to embed).
master_embedded = {"embedded": 0, "total": 0}
unified_stats = {"embedded": 0, "total_active": 0}
unified_dedup_stats = {"embedded": 0, "total": 0}

# COMMAND ----------

# DBTITLE 1,Step 1: Create Shared Embeddings Table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {embeddings_table} (
        embedding_key    STRING,
        embedding_hash   STRING,
        embedding_dim    INT,
        embedding_model  STRING,
        embedding_vector ARRAY<FLOAT>,
        created_at       TIMESTAMP
    ) USING DELTA
""")
logger.info(f"Shared embeddings table ready: {embeddings_table}")

# COMMAND ----------

# DBTITLE 1,Step 2: Probe Embedding Dimension
# Prewarm the embedding endpoint (handles scale-from-zero) before probing / bulk ai_query.
# In precomputed mode the VS layer skips embedding warm-up, so Compute_Embeddings is the
# only place the embedding endpoint is exercised — warm it here to avoid cold-start failures.
try:
    warm_up_embedding_endpoint(
        embedding_endpoint=embedding_endpoint,
        max_retries=10,
        retry_interval_seconds=60,
        timeout_minutes=10,
    )
    logger.info(f"Embedding endpoint '{embedding_endpoint}' is warm and ready")
except Exception as e:
    logger.warning(f"Embedding warm-up failed: {e}. Proceeding with dimension probe anyway...")

from pyspark.sql.functions import size as spark_size

probe_df = spark.sql(f"""
    SELECT CAST(ai_query('{embedding_endpoint}', 'dimension probe') AS ARRAY<FLOAT>) AS embedding
""")
embedding_dim = probe_df.select(spark_size("embedding").alias("dim")).collect()[0]["dim"]
logger.info(f"Embedding dimension: {embedding_dim} (from endpoint: {embedding_endpoint})")

# COMMAND ----------

# DBTITLE 1,Step 3: Add Embedding Column to Master and Unified Tables
# Only touch tables that actually exist (see existence checks above).
add_col_tables = []
for tbl, tbl_exists in (
    (master_table, master_exists),
    (unified_table, unified_exists),
    (unified_dedup_table, unified_dedup_exists),
):
    if tbl_exists:
        add_col_tables.append(tbl)
    else:
        logger.info(f"Table does not exist ({tbl}), skipping column add")

for tbl in add_col_tables:
    tbl_cols_lower = {f.name.strip().lower() for f in spark.table(tbl).schema}
    if "attributes_combined_embedding" not in tbl_cols_lower:
        spark.sql(f"ALTER TABLE {tbl} ADD COLUMNS (attributes_combined_embedding ARRAY<FLOAT>)")
        logger.info(f"Added attributes_combined_embedding column to {tbl}")
    else:
        logger.info(f"Column attributes_combined_embedding already exists in {tbl}")

# COMMAND ----------

# DBTITLE 1,Step 4: Compute & Cache Embeddings for Master Table
# embedding_model_id_safe is used by every downstream section, so it stays outside the
# master_exists guard.
embedding_model_id_safe = embedding_model_id.replace("'", "''")

if master_exists:
    master_misses_table = f"{catalog_name}.silver._{entity}_master_emb_misses_{experiment_id or 'prod'}"

    # NOTE: Concurrent runs on the same entity will independently compute the same
    # embeddings (each takes its own miss snapshot). Delta MERGE handles the duplicate
    # INSERT correctly, but both runs pay the ai_query cost. Acceptable for MVP.

    # Phase 1: Snapshot cache misses into a real Delta table (not a lazy view).
    # This freezes the set of texts to embed so concurrent runs don't duplicate work
    # within a single run, and guarantees ai_query only runs on actual cache misses.
    spark.sql(f"""
        CREATE OR REPLACE TABLE {master_misses_table} AS
        SELECT DISTINCT attributes_combined
        FROM {master_table}
        WHERE attributes_combined IS NOT NULL
          AND concat(sha2(attributes_combined, 256), '_{embedding_dim}_{embedding_model_id_safe}')
              NOT IN (SELECT embedding_key FROM {embeddings_table})
    """)

    cache_miss_count = spark.table(master_misses_table).count()
    logger.info(f"Master cache misses: {cache_miss_count} distinct texts to embed")

    # Phase 2: Compute embeddings only for the snapshotted misses and MERGE into cache.
    # Materialize the source once so we can count NULL results without re-running ai_query,
    # and guard the MERGE so failed (NULL) embeddings don't poison the cache and lock
    # their inputs out of future retries.
    # NOTE: temp tables are cleaned up at the end of the notebook (Step 7b), not in a
    # finally block — finally cleanup has been observed to compound failures in some
    # Databricks runtimes. CREATE OR REPLACE in the next run handles any leftover.
    master_source_table = f"{catalog_name}.silver._{entity}_master_emb_source_{experiment_id or 'prod'}"
    if cache_miss_count > 0:
        spark.sql(f"""
            CREATE OR REPLACE TABLE {master_source_table} AS
            SELECT
                concat(sha2(m.attributes_combined, 256), '_{embedding_dim}_{embedding_model_id_safe}') AS embedding_key,
                sha2(m.attributes_combined, 256) AS embedding_hash,
                {embedding_dim} AS embedding_dim,
                '{embedding_model_id_safe}' AS embedding_model,
                CAST(ai_query('{embedding_endpoint}', m.attributes_combined) AS ARRAY<FLOAT>) AS embedding_vector,
                current_timestamp() AS created_at
            FROM {master_misses_table} m
        """)

        null_count = spark.sql(f"""
            SELECT COUNT(*) AS c FROM {master_source_table} WHERE embedding_vector IS NULL
        """).collect()[0]["c"]
        success_count = cache_miss_count - null_count

        if null_count > 0:
            logger.warning(
                f"Master phase: {null_count}/{cache_miss_count} ai_query calls returned NULL "
                f"(will retry on next run)"
            )
        logger.info(f"Master phase: {success_count}/{cache_miss_count} embeddings successful")

        spark.sql(f"""
            MERGE INTO {embeddings_table} AS target
            USING {master_source_table} AS source
            ON target.embedding_key = source.embedding_key
            WHEN NOT MATCHED AND source.embedding_vector IS NOT NULL THEN INSERT *
        """)
    logger.info("Master embeddings computed and cached in shared embeddings table")
else:
    logger.info(f"Master table does not exist ({master_table}), skipping master embedding compute")

# COMMAND ----------

# DBTITLE 1,Step 5: Write Embeddings Back to Master Table
if master_exists:
    spark.sql(f"""
        MERGE INTO {master_table} AS t
        USING {embeddings_table} AS e
        ON concat(sha2(t.attributes_combined, 256), '_{embedding_dim}_{embedding_model_id_safe}') = e.embedding_key
        WHEN MATCHED AND t.attributes_combined_embedding IS NULL
        THEN UPDATE SET t.attributes_combined_embedding = e.embedding_vector
    """)

    master_embedded = spark.sql(f"""
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN attributes_combined_embedding IS NOT NULL THEN 1 ELSE 0 END) AS embedded
        FROM {master_table}
    """).collect()[0]
    logger.info(f"Master table: {master_embedded['embedded']}/{master_embedded['total']} records have embeddings")
else:
    logger.info(f"Master table does not exist ({master_table}), skipping master embedding writeback")

# COMMAND ----------

# DBTITLE 1,Step 6: Compute & Cache Embeddings for Unified Table (Query Side)
# Only ACTIVE records need embeddings — these are the secondary/incremental records
# that will be queried against the master index via similarity_search(query_vector=...)
# MERGED records (primary source, pre-merged to master) are NOT queried.
# Single-source entities have no unified table, so this whole section is skipped then.
if unified_exists:
    unified_misses_table = f"{catalog_name}.silver._{entity}_unified_emb_misses_{experiment_id or 'prod'}"

    # Phase 1: Snapshot cache misses into a real Delta table
    spark.sql(f"""
        CREATE OR REPLACE TABLE {unified_misses_table} AS
        SELECT DISTINCT attributes_combined
        FROM {unified_table}
        WHERE attributes_combined IS NOT NULL
          AND record_status = 'ACTIVE'
          AND concat(sha2(attributes_combined, 256), '_{embedding_dim}_{embedding_model_id_safe}')
              NOT IN (SELECT embedding_key FROM {embeddings_table})
    """)

    unified_miss_count = spark.table(unified_misses_table).count()
    logger.info(f"Unified ACTIVE cache misses: {unified_miss_count} distinct texts to embed")

    # Phase 2: Compute embeddings only for the snapshotted misses and MERGE into cache.
    # Materialize the source once so we can count NULL results without re-running ai_query,
    # and guard the MERGE so failed (NULL) embeddings don't poison the cache and lock
    # their inputs out of future retries.
    # Cleanup deferred to Step 7b (see note in master phase).
    unified_source_table = f"{catalog_name}.silver._{entity}_unified_emb_source_{experiment_id or 'prod'}"
    if unified_miss_count > 0:
        spark.sql(f"""
            CREATE OR REPLACE TABLE {unified_source_table} AS
            SELECT
                concat(sha2(u.attributes_combined, 256), '_{embedding_dim}_{embedding_model_id_safe}') AS embedding_key,
                sha2(u.attributes_combined, 256) AS embedding_hash,
                {embedding_dim} AS embedding_dim,
                '{embedding_model_id_safe}' AS embedding_model,
                CAST(ai_query('{embedding_endpoint}', u.attributes_combined) AS ARRAY<FLOAT>) AS embedding_vector,
                current_timestamp() AS created_at
            FROM {unified_misses_table} u
        """)

        null_count = spark.sql(f"""
            SELECT COUNT(*) AS c FROM {unified_source_table} WHERE embedding_vector IS NULL
        """).collect()[0]["c"]
        success_count = unified_miss_count - null_count

        if null_count > 0:
            logger.warning(
                f"Unified ACTIVE phase: {null_count}/{unified_miss_count} ai_query calls returned NULL "
                f"(will retry on next run)"
            )
        logger.info(f"Unified ACTIVE phase: {success_count}/{unified_miss_count} embeddings successful")

        spark.sql(f"""
            MERGE INTO {embeddings_table} AS target
            USING {unified_source_table} AS source
            ON target.embedding_key = source.embedding_key
            WHEN NOT MATCHED AND source.embedding_vector IS NOT NULL THEN INSERT *
        """)
    logger.info("Unified ACTIVE record embeddings computed and cached")
else:
    logger.info(f"Unified table does not exist ({unified_table}), skipping unified embedding compute")

# COMMAND ----------

# DBTITLE 1,Step 7: Write Embeddings Back to Unified Table
if unified_exists:
    spark.sql(f"""
        MERGE INTO {unified_table} AS t
        USING {embeddings_table} AS e
        ON concat(sha2(t.attributes_combined, 256), '_{embedding_dim}_{embedding_model_id_safe}') = e.embedding_key
        WHEN MATCHED AND t.attributes_combined_embedding IS NULL AND t.record_status = 'ACTIVE'
        THEN UPDATE SET t.attributes_combined_embedding = e.embedding_vector
    """)

    unified_stats = spark.sql(f"""
        SELECT COUNT(*) AS total_active,
               SUM(CASE WHEN attributes_combined_embedding IS NOT NULL THEN 1 ELSE 0 END) AS embedded
        FROM {unified_table}
        WHERE record_status = 'ACTIVE'
    """).collect()[0]
    logger.info(f"Unified ACTIVE records: {unified_stats['embedded']}/{unified_stats['total_active']} have embeddings")
else:
    logger.info(f"Unified table does not exist ({unified_table}), skipping unified embedding writeback")

# COMMAND ----------

# DBTITLE 1,Step 7a: Compute & Cache Embeddings for Unified Deduplicate Table (Golden Dedup)
# Normal dedup's unified table ({entity}_unified) is handled by Steps 6-7 above.
# This step handles golden dedup's unified_deduplicate table ({entity}_unified_deduplicate).
# Only runs if the table exists — golden dedup may not be enabled for every entity.
unified_dedup_stats = {"embedded": 0, "total": 0}

if unified_dedup_exists:
    unified_dedup_misses_table = f"{catalog_name}.silver._{entity}_unified_dedup_emb_misses_{experiment_id or 'prod'}"

    # Phase 1: Snapshot cache misses into a real Delta table
    spark.sql(f"""
        CREATE OR REPLACE TABLE {unified_dedup_misses_table} AS
        SELECT DISTINCT attributes_combined
        FROM {unified_dedup_table}
        WHERE attributes_combined IS NOT NULL
          AND concat(sha2(attributes_combined, 256), '_{embedding_dim}_{embedding_model_id_safe}')
              NOT IN (SELECT embedding_key FROM {embeddings_table})
    """)

    unified_dedup_miss_count = spark.table(unified_dedup_misses_table).count()
    logger.info(f"Unified dedup cache misses: {unified_dedup_miss_count} distinct texts to embed")

    # Phase 2: Compute embeddings only for the snapshotted misses and MERGE into cache.
    # Materialize the source once so we can count NULL results without re-running ai_query,
    # and guard the MERGE so failed (NULL) embeddings don't poison the cache and lock
    # their inputs out of future retries.
    # Cleanup deferred to Step 7b (see note in master phase).
    unified_dedup_source_table = f"{catalog_name}.silver._{entity}_unified_dedup_emb_source_{experiment_id or 'prod'}"
    if unified_dedup_miss_count > 0:
        spark.sql(f"""
            CREATE OR REPLACE TABLE {unified_dedup_source_table} AS
            SELECT
                concat(sha2(u.attributes_combined, 256), '_{embedding_dim}_{embedding_model_id_safe}') AS embedding_key,
                sha2(u.attributes_combined, 256) AS embedding_hash,
                {embedding_dim} AS embedding_dim,
                '{embedding_model_id_safe}' AS embedding_model,
                CAST(ai_query('{embedding_endpoint}', u.attributes_combined) AS ARRAY<FLOAT>) AS embedding_vector,
                current_timestamp() AS created_at
            FROM {unified_dedup_misses_table} u
        """)

        null_count = spark.sql(f"""
            SELECT COUNT(*) AS c FROM {unified_dedup_source_table} WHERE embedding_vector IS NULL
        """).collect()[0]["c"]
        success_count = unified_dedup_miss_count - null_count

        if null_count > 0:
            logger.warning(
                f"Unified dedup phase: {null_count}/{unified_dedup_miss_count} ai_query calls returned NULL "
                f"(will retry on next run)"
            )
        logger.info(f"Unified dedup phase: {success_count}/{unified_dedup_miss_count} embeddings successful")

        spark.sql(f"""
            MERGE INTO {embeddings_table} AS target
            USING {unified_dedup_source_table} AS source
            ON target.embedding_key = source.embedding_key
            WHEN NOT MATCHED AND source.embedding_vector IS NOT NULL THEN INSERT *
        """)
    logger.info("Unified dedup embeddings computed and cached")

    # Write embeddings back to unified_deduplicate table
    spark.sql(f"""
        MERGE INTO {unified_dedup_table} AS t
        USING {embeddings_table} AS e
        ON concat(sha2(t.attributes_combined, 256), '_{embedding_dim}_{embedding_model_id_safe}') = e.embedding_key
        WHEN MATCHED AND t.attributes_combined_embedding IS NULL
        THEN UPDATE SET t.attributes_combined_embedding = e.embedding_vector
    """)

    unified_dedup_stats = spark.sql(f"""
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN attributes_combined_embedding IS NOT NULL THEN 1 ELSE 0 END) AS embedded
        FROM {unified_dedup_table}
    """).collect()[0]
    logger.info(f"Unified dedup: {unified_dedup_stats['embedded']}/{unified_dedup_stats['total']} records have embeddings")
else:
    logger.info(f"Unified dedup table does not exist, skipping golden dedup embedding compute")

# COMMAND ----------

# DBTITLE 1,Step 7b: Drop Temp Tables (consolidated cleanup)
# All Phase 2 temp tables are dropped here on the success path. Earlier versions
# used try/finally per phase, but in some Databricks runtimes a finally-block DROP
# can compound a still-failing transaction. Deferring to a single end-of-pipeline
# block lets a crashed run leave its temp tables in place for debugging; the next
# run's CREATE OR REPLACE will overwrite them, and table names are deterministic
# per (entity, experiment_id) so leaks don't accumulate across runs.
_cleanup_tables = [
    locals().get("master_misses_table"),
    locals().get("master_source_table"),
    locals().get("unified_misses_table"),
    locals().get("unified_source_table"),
    locals().get("unified_dedup_misses_table"),
    locals().get("unified_dedup_source_table"),
]
for tbl in _cleanup_tables:
    if not tbl:
        continue
    try:
        spark.sql(f"DROP TABLE IF EXISTS {tbl}")
    except Exception as drop_err:
        logger.warning(f"Cleanup of {tbl} failed (non-fatal): {drop_err}")
logger.info("Temp tables cleaned up")

# COMMAND ----------

# DBTITLE 1,Step 7.5: Canary Check — Detect Stale Embeddings
if master_exists:
    stale_count = spark.sql(f"""
        SELECT COUNT(*) AS stale_count
        FROM {master_table} m
        WHERE m.attributes_combined_embedding IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM {embeddings_table} e
              WHERE e.embedding_key = concat(
                  sha2(m.attributes_combined, 256),
                  '_{embedding_dim}_{embedding_model_id_safe}'
              )
          )
    """).collect()[0]["stale_count"]

    if stale_count > 0:
        logger.warning(
            f"CANARY: {stale_count} master rows have stale embeddings — "
            "some upstream notebook isn't invalidating correctly"
        )
    else:
        logger.info("Canary check passed: zero stale embeddings detected")
else:
    logger.info(f"Master table does not exist ({master_table}), skipping canary check")

# COMMAND ----------

# DBTITLE 1,Step 8: Export Embedding Dimension for VS Notebook
dbutils.jobs.taskValues.set("embedding_dim", embedding_dim)

logger.info("=" * 60)
logger.info("COMPUTE EMBEDDINGS — COMPLETE")
logger.info("=" * 60)
logger.info(f"  Embedding dimension: {embedding_dim}")
logger.info(f"  Endpoint:            {embedding_endpoint}")

logger_instance.shutdown()
dbutils.notebook.exit(json.dumps({
    "status": "success",
    "embedding_dim": embedding_dim,
    "master_embedded": master_embedded['embedded'],
    "unified_active_embedded": unified_stats['embedded'],
    "unified_dedup_embedded": unified_dedup_stats['embedded']
}))
