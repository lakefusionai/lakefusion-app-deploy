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

# DBTITLE 1,Parse edits and log configuration
edits = json.loads(edits_json) if isinstance(edits_json, str) else edits_json

logger.info("=" * 80)
logger.info("SCHEMA EVOLUTION - VALIDATE")
logger.info("=" * 80)
logger.info(f"Entity Name: {entity_name}")
logger.info(f"Master Table: {master_table}")
logger.info(f"Unified Table: {unified_table}")
logger.info(f"Edits count: {len(edits)}")
logger.info("=" * 80)

# COMMAND ----------

# DBTITLE 1,Step 1: Validate columns in master and unified tables
validation_errors = []

for edit in edits:
    if edit.get('action_type') != 'ADD_ATTRIBUTE':
        continue

    attr_name = edit['attribute_name']

    # Verify column exists in master table (schema-only, no data scan)
    master_col_names = {f.name for f in spark.table(master_table).schema}
    if attr_name not in master_col_names:
        validation_errors.append(
            f"Column '{attr_name}' missing from master table '{master_table}'"
        )

    # Verify column exists in unified table (schema-only, no data scan)
    unified_col_names = {f.name for f in spark.table(unified_table).schema}
    if attr_name not in unified_col_names:
        validation_errors.append(
            f"Column '{attr_name}' missing from unified table '{unified_table}'"
        )

logger.info(f"Master/Unified validation: {len(validation_errors)} error(s) so far")

# COMMAND ----------

# DBTITLE 1,Step 2: Validate derived tables (if they exist)
from pyspark.sql.types import ArrayType, StructType

add_attrs = [
    edit['attribute_name']
    for edit in edits
    if edit.get('action_type') == 'ADD_ATTRIBUTE'
]

if not entity_name:
    logger.info("No entity_name available - skipping derived table validation")
elif not add_attrs:
    logger.info("No ADD_ATTRIBUTE edits - skipping derived table validation")
else:
    experiment_suffix = f"_{experiment_id}" if experiment_id else ""

    # -- Validate unified_deduplicate (if exists) --
    unified_dedup = f"{catalog_name}.silver.{entity_name}_unified_deduplicate{experiment_suffix}"
    if spark.catalog.tableExists(unified_dedup):
        dedup_cols = {f.name for f in spark.table(unified_dedup).schema}
        for attr_name in add_attrs:
            if attr_name not in dedup_cols:
                validation_errors.append(
                    f"Column '{attr_name}' missing from unified_deduplicate table '{unified_dedup}'"
                )
        logger.info(f"  Validated unified_deduplicate table: {unified_dedup}")
    else:
        logger.info(f"  Unified dedup table does not exist - skipping: {unified_dedup}")

    # -- Validate potential_match struct includes new attrs (if exists) --
    pm_table = f"{catalog_name}.gold.{entity_name}_master_potential_match{experiment_suffix}"
    if spark.catalog.tableExists(pm_table):
        schema = spark.table(pm_table).schema
        for field in schema:
            if field.name == "potential_matches":
                if isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
                    struct_fields = {sf.name for sf in field.dataType.elementType.fields}
                    for attr_name in add_attrs:
                        if attr_name not in struct_fields:
                            validation_errors.append(
                                f"Column '{attr_name}' missing from potential_matches struct in '{pm_table}'"
                            )
                break
        logger.info(f"  Validated potential_match table: {pm_table}")
    else:
        logger.info(f"  Potential match table does not exist - skipping: {pm_table}")

    # -- Validate potential_match_deduplicate struct (if exists) --
    pm_dedup_table = f"{catalog_name}.gold.{entity_name}_master_potential_match_deduplicate{experiment_suffix}"
    if spark.catalog.tableExists(pm_dedup_table):
        schema = spark.table(pm_dedup_table).schema
        for field in schema:
            if field.name == "potential_matches":
                if isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
                    struct_fields = {sf.name for sf in field.dataType.elementType.fields}
                    for attr_name in add_attrs:
                        if attr_name not in struct_fields:
                            validation_errors.append(
                                f"Column '{attr_name}' missing from potential_matches struct in '{pm_dedup_table}'"
                            )
                break
        logger.info(f"  Validated potential_match_deduplicate table: {pm_dedup_table}")
    else:
        logger.info(f"  Potential match dedup table does not exist - skipping: {pm_dedup_table}")

# COMMAND ----------

# DBTITLE 1,Report validation results
if validation_errors:
    logger.error("VALIDATION ERRORS:")
    for err in validation_errors:
        logger.error(f"  {err}")
    logger.error(f"Schema evolution validation failed with {len(validation_errors)} error(s)")
    logger_instance.shutdown()
    raise Exception(f"Schema evolution validation failed: {'; '.join(validation_errors)}")
else:
    logger.info("\nAll validations passed - all columns present in all tables")

# COMMAND ----------

# DBTITLE 1,Summary
columns_validated = len([e for e in edits if e.get('action_type') == 'ADD_ATTRIBUTE'])

logger.info("\n" + "=" * 80)
logger.info("SCHEMA EVOLUTION - VALIDATE - COMPLETE")
logger.info("=" * 80)
logger.info(f"Validations passed: True")
logger.info(f"Columns validated: {columns_validated}")
logger.info("=" * 80)

# COMMAND ----------

# Shutdown logger
logger_instance.shutdown()
