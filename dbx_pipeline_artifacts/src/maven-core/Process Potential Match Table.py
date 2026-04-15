# Databricks notebook source
import json
import ast
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,lit

# COMMAND ----------

dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("primary_table", "", "Primary Table")
dbutils.widgets.text("id_key", "", "Primary Key")
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")
dbutils.widgets.text("experiment_id", "", "Match Maven Experiment ID")
dbutils.widgets.text("is_integration_hub", "", "Integration Hub Pipeline")
dbutils.widgets.text("config_thresold", "", "Model Config Thresholds")
dbutils.widgets.text("catalog_name", "lakefusion_ai_uat", "Catalog Name")
# dbutils.widgets.text('validation_functions', '', 'Validation Functions')


# COMMAND ----------

entity = dbutils.widgets.get("entity")
primary_table = dbutils.widgets.get("primary_table")
id_key = dbutils.widgets.get("id_key")
entity_attributes = dbutils.widgets.get("entity_attributes")
#below inputs are to be passed as notebook params OR add them to entity_json itself
experiment_id = dbutils.widgets.get("experiment_id") # cannot contain "-" for table name
experiment_id = experiment_id.replace("-", "") #remove "-" which is invalid for table name
incremental_load = False
# validation_functions=dbutils.widgets.get("validation_functions")
catalog_name=dbutils.widgets.get("catalog_name")
config_thresold = dbutils.widgets.get("config_thresold")
is_integration_hub=dbutils.widgets.get("is_integration_hub")

# COMMAND ----------

entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
primary_table = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "primary_table", debugValue=primary_table
)
id_key = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON", "id_key", debugValue=id_key
)
attributes = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes", debugValue=entity_attributes)
config_thresold = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "config_thresold", debugValue=config_thresold)

# COMMAND ----------

attributes = json.loads(attributes)
attributes.append("lakefusion_id")
config_thresold=json.loads(config_thresold)

# COMMAND ----------

not_match_min_max = config_thresold.get('not_match')
not_match_min = not_match_min_max[0]
not_match_max = not_match_min_max[1]

# COMMAND ----------

master_potential_match_table = f"{catalog_name}.gold.{entity}_master_potential_match"
if experiment_id:
  master_potential_match_table += f"_{experiment_id}"

# COMMAND ----------

entity = entity.lower().replace(' ', '_')
select_attributes = [f"ma.{attr}" for attr in attributes]
select_clause = ", ".join(select_attributes)

# --- Build named_struct expression ---
named_struct_parts = []
for attr in attributes:
    named_struct_parts.append(f"'{attr}', unified.{attr}")

named_struct_parts.extend([
    "'__score__', pu.exploded_result.score",
    "'__sourcename__', unified.dataset.name",
    "'__sourceid__', unified.dataset.id",
    "'__reason__', pu.exploded_result.reason",
    """'__mergestatus__', 
        CASE 
            WHEN merges.merge_key IS NOT NULL THEN 'MERGED'
            ELSE 'PENDING'
        END"""
])

named_struct_expr = f"named_struct({', '.join(named_struct_parts)})"
potential_matches_expr = f"COLLECT_SET({named_struct_expr}) AS potential_matches"

# --- Tags mapping clause ---
map_entries = [
    f"'{attr}', (SELECT tags_map FROM column_tags WHERE column_name = '{attr}')"
    for attr in attributes
]
map_clause = "map(" + ", ".join(map_entries) + ") AS tags"

# --- Final query ---
query = f"""
WITH merges AS (
    SELECT
        *,
        CONCAT(master_{id_key}, ':', {id_key}, ':', `source`) as merge_key
    FROM
        {catalog_name}.gold.{entity}_master_prod_merge_activities
    WHERE
        action_type IN ('MANUAL_MERGE', 'JOB_MERGE')
),
no_match AS (
    SELECT
        *,
        CONCAT(master_{id_key}, ':', {id_key}, ':', `source`) as no_match_key
    FROM
        {catalog_name}.gold.{entity}_master_prod_merge_activities
    WHERE
        action_type IN ('MANUAL_NOT_A_MATCH', 'JOB_NOT_A_MATCH','JOB_INSERT', 'MANUAL_UNMERGE')
),
merged_masters AS (
    SELECT DISTINCT {id_key} as merged_master_id
    FROM {catalog_name}.gold.{entity}_master_prod_merge_activities
    WHERE action_type IN ('MANUAL_MERGE', 'JOB_MERGE')
      AND {id_key} != ''
      AND {id_key} IS NOT NULL
),
merged_unified_records AS (
    SELECT DISTINCT master_{id_key} as merged_unified_id
    FROM {catalog_name}.gold.{entity}_master_prod_merge_activities
    WHERE action_type IN ('MANUAL_MERGE', 'JOB_MERGE')
      AND master_{id_key} != ''
      AND master_{id_key} IS NOT NULL
),
source_deleted_records AS (
    SELECT DISTINCT 
        master_{id_key} as deleted_master_id,
        CASE WHEN {id_key} != '' AND {id_key} IS NOT NULL 
             THEN {id_key} 
             ELSE NULL 
        END as deleted_unified_id
    FROM {catalog_name}.gold.{entity}_master_prod_merge_activities
    WHERE action_type = 'SOURCE_DELETE'
),
source_deleted_masters AS (
    SELECT DISTINCT deleted_master_id
    FROM source_deleted_records
    WHERE deleted_master_id IS NOT NULL
),
source_deleted_unified AS (
    SELECT DISTINCT deleted_unified_id
    FROM source_deleted_records
    WHERE deleted_unified_id IS NOT NULL
),
pot_match AS (
    SELECT
        {select_clause},
        pu.potential_matches
    FROM (
        SELECT
            pu.master_{id_key},
            {potential_matches_expr}
        FROM
            {catalog_name}.silver.{entity}_processed_unified_prod pu
            JOIN {catalog_name}.silver.{entity}_unified_prod unified
                ON pu.{id_key} = unified.{id_key}
            JOIN {catalog_name}.gold.{entity}_master_prod m
                ON m.{id_key} = pu.master_{id_key}
            LEFT JOIN merges ON (
                pu.master_{id_key} = merges.master_{id_key}
                AND pu.{id_key} = merges.{id_key}
                AND unified.table_name = merges.`source`
            )
            LEFT ANTI JOIN no_match ON (
                pu.master_{id_key} = no_match.master_{id_key}
                AND pu.{id_key} = no_match.{id_key}
                AND unified.table_name = no_match.`source`
            )
            LEFT ANTI JOIN no_match ON (
                pu.{id_key} = no_match.master_{id_key}
                AND unified.table_name = no_match.`source`
            )
            -- Exclude masters that have been merged into other records
            LEFT ANTI JOIN merged_masters ON (
                pu.master_{id_key} = merged_masters.merged_master_id
            )
            -- Exclude unified records that have been merged already
            LEFT ANTI JOIN merged_unified_records ON (
                pu.{id_key} = merged_unified_records.merged_unified_id
            )
            -- Exclude masters that have been source deleted
            LEFT ANTI JOIN source_deleted_masters ON (
                pu.master_{id_key} = source_deleted_masters.deleted_master_id
            )
            -- Exclude unified records that have been source deleted
            LEFT ANTI JOIN source_deleted_unified ON (
                pu.{id_key} = source_deleted_unified.deleted_unified_id
            )
        WHERE
            pu.exploded_result.match = 'MATCH'
            AND pu.exploded_result.score >= {not_match_max}
        GROUP BY
            pu.master_{id_key}
    ) pu
    JOIN {catalog_name}.gold.{entity}_master_prod ma
        ON pu.master_{id_key} = ma.{id_key}
),
column_tags AS (
    SELECT
        column_name,
        map_from_arrays(collect_list(tag_name), collect_list(tag_value)) AS tags_map
    FROM {catalog_name}.information_schema.column_tags
    WHERE schema_name = 'gold'
    AND table_name = '{entity}_master_prod'
    GROUP BY column_name
)
SELECT
    *,
    {map_clause}
FROM pot_match
"""

# Run the query
potential_match_df = spark.sql(query)

# COMMAND ----------

master_potential_match_table_exists = spark.catalog.tableExists(master_potential_match_table)

# COMMAND ----------

if not master_potential_match_table_exists:
  potential_match_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(master_potential_match_table)
else:
  # Define the Delta table
  delta_table = DeltaTable.forName(spark, master_potential_match_table)

  # Perform merge operation
  delta_table.alias("target").merge(
      source=potential_match_df.alias("source"),
      condition=f"target.{id_key} = source.{id_key}"
  ).whenMatchedUpdateAll().whenNotMatchedInsertAll().whenNotMatchedBySourceDelete().execute()

# COMMAND ----------

num_cols = len(potential_match_df.columns)+1
spark.sql(f"ALTER TABLE {master_potential_match_table} SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '{num_cols}')")
optimise_res = spark.sql(f"OPTIMIZE {master_potential_match_table} ZORDER BY ({id_key})")
