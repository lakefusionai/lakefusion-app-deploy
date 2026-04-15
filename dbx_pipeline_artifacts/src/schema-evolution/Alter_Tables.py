# Databricks notebook source
import json

# COMMAND ----------

# MAGIC %run ../utils/taskvalues_enum

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("se_job_id", "", "Schema Evolution Job ID")
dbutils.widgets.text("entity_id", "", "Entity ID")
dbutils.widgets.text("catalog_name", "", "Catalog Name")

# COMMAND ----------

# DBTITLE 1,Get parameters
catalog_name = dbutils.widgets.get("catalog_name")
experiment_id = "prod"  # Schema evolution always runs against prod

# COMMAND ----------

# DBTITLE 1,Get task values from Parse step
edits_json = dbutils.jobs.taskValues.get(
    taskKey="Parse_Schema_Evolution_JSON",
    key=TaskValueKey.SCHEMA_EVOLUTION_EDITS.value,
    debugValue="[]"
)

master_table = dbutils.jobs.taskValues.get(
    taskKey="Parse_Schema_Evolution_JSON",
    key=TaskValueKey.SCHEMA_EVOLUTION_MASTER_TABLE.value,
    debugValue=""
)

unified_table = dbutils.jobs.taskValues.get(
    taskKey="Parse_Schema_Evolution_JSON",
    key=TaskValueKey.SCHEMA_EVOLUTION_UNIFIED_TABLE.value,
    debugValue=""
)

entity_name = dbutils.jobs.taskValues.get(
    taskKey="Parse_Schema_Evolution_JSON",
    key=TaskValueKey.SCHEMA_EVOLUTION_ENTITY_NAME.value,
    debugValue=""
)

# COMMAND ----------

# MAGIC %run ../utils/execute_utils

# COMMAND ----------

# DBTITLE 1,Parse edits and setup
edits = json.loads(edits_json) if isinstance(edits_json, str) else edits_json

# Supported type mapping for ALTER TABLE ADD COLUMNS
DTYPE_TO_SQL = {
    'BIGINT': 'BIGINT',
    'BOOLEAN': 'BOOLEAN',
    'DATE': 'DATE',
    'DOUBLE': 'DOUBLE',
    'FLOAT': 'FLOAT',
    'INT': 'INT',
    'SMALLINT': 'SMALLINT',
    'STRING': 'STRING',
    'TINYINT': 'TINYINT',
    'TIMESTAMP': 'TIMESTAMP',
}

logger.info("=" * 80)
logger.info("SCHEMA EVOLUTION - ALTER TABLES")
logger.info("=" * 80)
logger.info(f"Master Table: {master_table}")
logger.info(f"Unified Table: {unified_table}")
logger.info(f"Entity Name: {entity_name}")
logger.info(f"Edits count: {len(edits)}")
logger.info("=" * 80)

# COMMAND ----------

# DBTITLE 1,Build unified_deduplicate table name
experiment_suffix = f"_{experiment_id}" if experiment_id else ""
unified_dedup_table = ""
if entity_name:
    unified_dedup_table = f"{catalog_name}.silver.{entity_name}_unified_deduplicate{experiment_suffix}"

logger.info(f"Unified Deduplicate Table: {unified_dedup_table or '(not applicable)'}")

# COMMAND ----------

# DBTITLE 1,Enable column mapping (required for future rollback DROP COLUMN)
def enable_column_mapping(table_name):
    """Enable Delta column mapping so future DROP COLUMN (rollback) is supported. Idempotent."""
    try:
        spark.sql(f"""
            ALTER TABLE {table_name} SET TBLPROPERTIES (
                'delta.columnMapping.mode' = 'name',
                'delta.minReaderVersion' = '2',
                'delta.minWriterVersion' = '5'
            )
        """)
        logger.info(f"  Enabled column mapping on {table_name}")
    except Exception as e:
        if "already enabled" in str(e).lower() or "already set" in str(e).lower():
            logger.info(f"  Column mapping already enabled on {table_name}")
        else:
            raise

logger.info("Enabling column mapping on target tables...")
enable_column_mapping(master_table)
enable_column_mapping(unified_table)
if unified_dedup_table and spark.catalog.tableExists(unified_dedup_table):
    enable_column_mapping(unified_dedup_table)

# COMMAND ----------

# DBTITLE 1,Alter tables - add columns for each ADD_ATTRIBUTE edit
columns_added = []
columns_skipped = []

for edit in edits:
    if edit.get('action_type') != 'ADD_ATTRIBUTE':
        continue

    attr_name = edit['attribute_name']
    sql_type = DTYPE_TO_SQL.get(edit['attribute_type'].upper(), 'STRING')

    # -- Master table --
    master_cols = [f.name for f in spark.table(master_table).schema]
    if attr_name not in master_cols:
        spark.sql(f"ALTER TABLE {master_table} ADD COLUMNS (`{attr_name}` {sql_type})")
        logger.info(f"  Added column `{attr_name}` ({sql_type}) to master table")
        columns_added.append(f"master:{attr_name}")
    else:
        logger.info(f"  Column `{attr_name}` already exists in master table - skipping")
        columns_skipped.append(f"master:{attr_name}")

    # -- Unified table --
    unified_cols = [f.name for f in spark.table(unified_table).schema]
    if attr_name not in unified_cols:
        spark.sql(f"ALTER TABLE {unified_table} ADD COLUMNS (`{attr_name}` {sql_type})")
        logger.info(f"  Added column `{attr_name}` ({sql_type}) to unified table")
        columns_added.append(f"unified:{attr_name}")
    else:
        logger.info(f"  Column `{attr_name}` already exists in unified table - skipping")
        columns_skipped.append(f"unified:{attr_name}")

    # -- Unified Deduplicate table (if exists) --
    if unified_dedup_table and spark.catalog.tableExists(unified_dedup_table):
        dedup_cols = [f.name for f in spark.table(unified_dedup_table).schema]
        if attr_name not in dedup_cols:
            spark.sql(f"ALTER TABLE {unified_dedup_table} ADD COLUMNS (`{attr_name}` {sql_type})")
            logger.info(f"  Added column `{attr_name}` ({sql_type}) to unified_deduplicate table")
            columns_added.append(f"unified_dedup:{attr_name}")
        else:
            logger.info(f"  Column `{attr_name}` already exists in unified_deduplicate table - skipping")
            columns_skipped.append(f"unified_dedup:{attr_name}")

# COMMAND ----------

# DBTITLE 1,Summary
logger.info("\n" + "=" * 80)
logger.info("SCHEMA EVOLUTION - ALTER TABLES - COMPLETE")
logger.info("=" * 80)
logger.info(f"Columns added: {len(columns_added)}")
for col_info in columns_added:
    logger.info(f"  + {col_info}")
logger.info(f"Columns skipped: {len(columns_skipped)}")
for col_info in columns_skipped:
    logger.info(f"  - {col_info}")
logger.info("=" * 80)

# COMMAND ----------

# Shutdown logger
logger_instance.shutdown()
