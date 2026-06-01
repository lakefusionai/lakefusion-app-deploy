# Databricks notebook source
# MAGIC %pip install --upgrade "databricks-sdk>=0.61.0" psycopg2-binary
# MAGIC %restart_python

# COMMAND ----------

import json
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, FloatType, BooleanType, DateType, TimestampType,
    ShortType, ByteType, ArrayType
)
from pyspark.sql.functions import col, lit, current_timestamp
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run ../../utils/execute_utils

# COMMAND ----------

dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("id_key", "", "ID Key")
dbutils.widgets.text("primary_key", "", "Primary Key")
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")
dbutils.widgets.text("dataset_tables", "", "Dataset Tables")
dbutils.widgets.text("entity_reference_config", "", "entity_reference_config")
dbutils.widgets.text("entity_attributes_datatype", "", "entity_attributes_datatype")
dbutils.widgets.text("lakebase_instance_id", "", "lakebase_instance_id")
dbutils.widgets.text("lakebase_branch_id", "", "lakebase_branch_id")
dbutils.widgets.text("lakebase_endpoint_id", "", "lakebase_endpoint_id")
dbutils.widgets.text("lakebase_database", "", "lakebase_database")
dbutils.widgets.text("write_mode", "delta", "Write Mode (delta/lakebase)")

# COMMAND ----------

entity = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON", key="entity",
    debugValue=dbutils.widgets.get("entity")
)
catalog_name = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON", key="catalog_name",
    debugValue=dbutils.widgets.get("catalog_name")
)
id_key = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON", key="id_key",
    debugValue=dbutils.widgets.get("id_key")
)
primary_key = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON", key="primary_key",
    debugValue=dbutils.widgets.get("primary_key")
)
entity_attributes = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON", key="entity_attributes",
    debugValue=dbutils.widgets.get("entity_attributes")
)
dataset_tables = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON", key="dataset_tables",
    debugValue=dbutils.widgets.get("dataset_tables")
)
entity_reference_config = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON", key="entity_reference_config",
    debugValue=dbutils.widgets.get("entity_reference_config")
)
entity_attributes_datatype = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON", key="entity_attributes_datatype",
    debugValue=dbutils.widgets.get("entity_attributes_datatype")
)
experiment_id = dbutils.widgets.get("experiment_id")
lakebase_instance_id = dbutils.widgets.get("lakebase_instance_id")
lakebase_branch_id = dbutils.widgets.get("lakebase_branch_id")
lakebase_endpoint_id = dbutils.widgets.get("lakebase_endpoint_id")
lakebase_database = dbutils.widgets.get("lakebase_database")
write_mode = dbutils.widgets.get("write_mode")

# COMMAND ----------

entity_attributes = json.loads(entity_attributes)
dataset_tables = json.loads(dataset_tables)
entity_attributes_datatype = json.loads(entity_attributes_datatype)

# COMMAND ----------

from lakefusion_core_engine.write_ops import create_write_ops,WriteMode
lakebase_params = {                                # becomes PG schema name
    "lakebase_instance_id": lakebase_instance_id,  # project display_name
    "lakebase_branch_id": lakebase_branch_id,        # default
    "lakebase_endpoint_id": lakebase_endpoint_id,         # default
    "lakebase_database": entity,
}
 
ops = create_write_ops(mode=write_mode, spark=spark, params=lakebase_params)

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType,
    TimestampType, IntegerType, ArrayType, _parse_datatype_string,
)

# =========================
# MASTER TABLE
# =========================
entity_datatype=entity_attributes_datatype
table_fqn = f"{catalog_name}.gold.{entity}_reference_prod"

master_fields = [
    StructField("ref_lakefusion_id", StringType(), nullable=False)
]

for name, dtype in entity_datatype.items():
    is_pk = name.lower() == primary_key.lower()

    master_fields.append(
        StructField(
            name,
            _parse_datatype_string(dtype),
            nullable=not is_pk
        )
    )

master_schema = StructType(master_fields)

empty_master = spark.createDataFrame([], master_schema)

if ops is not None:

    ops.create_table(
        empty_master,
        table_fqn,
        enable_cdf=True,
        primary_key=["ref_lakefusion_id"],
    )

    logger.info(f"Created MASTER {table_fqn} (via ops)")

else:

    if not spark.catalog.tableExists(table_fqn):

        (
            empty_master.write
            .format("delta")
            .option("delta.enableChangeDataFeed", "true")
            .option("delta.feature.allowColumnDefaults", "supported")
            .saveAsTable(table_fqn)
        )

    logger.info(f"Created MASTER {table_fqn} (delta)")
    logger.info(f"Business columns: {len(entity_datatype)} (+ ref_lakefusion_id)")
    logger.info("CDF: Enabled")


# =========================
# AUDIT TABLE
# =========================

audit_table_fqn = f"{catalog_name}.gold.{entity}_reference_audit_prod"

audit_fields = [
    StructField("ref_lakefusion_id", StringType(), nullable=False)
]

for name, dtype in entity_datatype.items():

    audit_fields.append(
        StructField(
            name,
            _parse_datatype_string(dtype),
            nullable=True
        )
    )

audit_fields += [
    StructField("valid_from", TimestampType(), nullable=False),
    StructField("valid_to", TimestampType(), nullable=True),
    StructField("is_current", BooleanType(), nullable=False),
    StructField("version", IntegerType(), nullable=False),
    StructField("source", StringType(), nullable=True),
    StructField("action_type", StringType(), nullable=True),
    StructField("steward_locked", BooleanType(), nullable=False),
    StructField("steward_edited_cols", ArrayType(StringType()), nullable=True),
    StructField("steward_edited_by", StringType(), nullable=True),
]

audit_schema = StructType(audit_fields)

empty_audit = spark.createDataFrame([], audit_schema)

if ops is not None:

    ops.create_table(
        empty_audit,
        audit_table_fqn,
        enable_cdf=True,
        primary_key=["ref_lakefusion_id", "version"],
    )

    logger.info(f"Created AUDIT {audit_table_fqn} (via ops)")

else:

    if not spark.catalog.tableExists(audit_table_fqn):

        (
            empty_audit.write
            .format("delta")
            .option("delta.enableChangeDataFeed", "true")
            .option("delta.feature.allowColumnDefaults", "supported")
            .saveAsTable(audit_table_fqn)
        )

        spark.sql(
            f"""
            ALTER TABLE {audit_table_fqn}
            ALTER COLUMN valid_from
            SET DEFAULT current_timestamp()
            """
        )

        spark.sql(
            f"""
            ALTER TABLE {audit_table_fqn}
            ALTER COLUMN is_current
            SET DEFAULT TRUE
            """
        )

        spark.sql(
            f"""
            ALTER TABLE {audit_table_fqn}
            ALTER COLUMN steward_locked
            SET DEFAULT FALSE
            """
        )

    logger.info(f"Created AUDIT {audit_table_fqn} (delta)")

    logger.info(
        f"Business columns: {len(entity_datatype)} "
        f"(+ ref_lakefusion_id, +5 SCD-2/source, +4 steward/provenance)"
    )

    logger.info("CDF: Enabled")

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
)

# =========================
# CONFLICT QUEUE TABLE
# =========================

conflict_table_fqn = (
    f"{catalog_name}.gold.{entity}_reference_conflict_queue_prod"
)

conflict_schema = StructType([

    StructField("ref_lakefusion_id", StringType(), nullable=False),
    StructField("conflict_id", StringType(), nullable=False),

    StructField("conflict_type", StringType(), nullable=True),
    StructField("entity_key_value", StringType(), nullable=True),

    StructField("field_name", StringType(), nullable=True),

    StructField("rdm_value", StringType(), nullable=True),
    StructField("source_value", StringType(), nullable=True),

    StructField("edited_by", StringType(), nullable=True),

    StructField("status", StringType(), nullable=True),

    StructField("resolved_by", StringType(), nullable=True),

    StructField("resolved_at", TimestampType(), nullable=True),

    StructField("created_at", TimestampType(), nullable=True),

])

empty_conflict = spark.createDataFrame([], conflict_schema)

if ops is not None:

    ops.create_table(
        empty_conflict,
        conflict_table_fqn,
        enable_cdf=True,
        primary_key=["ref_lakefusion_id", "conflict_id"],
    )

    logger.info(
        f"Created CONFLICT QUEUE {conflict_table_fqn} (via ops)"
    )

else:

    if not spark.catalog.tableExists(conflict_table_fqn):

        (
            empty_conflict.write
            .format("delta")
            .option("delta.enableChangeDataFeed", "true")
            .option("delta.feature.allowColumnDefaults", "supported")
            .saveAsTable(conflict_table_fqn)
        )

        spark.sql(
            f"""
            ALTER TABLE {conflict_table_fqn}
            ALTER COLUMN status
            SET DEFAULT 'PENDING'
            """
        )

    logger.info(
        f"Created CONFLICT QUEUE {conflict_table_fqn} (delta)"
    )

    logger.info("CDF: Enabled")