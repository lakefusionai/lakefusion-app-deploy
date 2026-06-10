# Databricks notebook source
from uuid import uuid4
import json
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from databricks.sdk import WorkspaceClient
import requests
from dbruntime.databricks_repl_context import get_context
import re
from functools import reduce

# COMMAND ----------

dbutils.widgets.text("id_key", "", "Primary Key")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("dataset_objects", "", "Dataset Objects")
dbutils.widgets.text("default_survivorship_rules", "", "Default Survivorship Rules")
dbutils.widgets.text('experiment_id', '', 'Experiment ID')
dbutils.widgets.text("entity_attributes", "", "Entity Attributes")
dbutils.widgets.text("config_thresold", "", "Model Config Thresholds")
dbutils.widgets.text("master_id", "", "Master Record ID")
dbutils.widgets.text("unified_dataset_ids", "", "Unified Record IDs with Dataset IDs")
dbutils.widgets.text("catalog_name", "lakefusion_ai_uat", "Catalog Name")

# COMMAND ----------

id_key = dbutils.widgets.get("id_key")
entity = dbutils.widgets.get("entity")
dataset_objects = dbutils.widgets.get("dataset_objects")
default_survivorship_rules = dbutils.widgets.get("default_survivorship_rules")
experiment_id = dbutils.widgets.get('experiment_id')
entity_attributes = dbutils.widgets.get("entity_attributes")
config_thresold = dbutils.widgets.get("config_thresold")
master_id = dbutils.widgets.get("master_id")
unified_dataset_ids = dbutils.widgets.get("unified_dataset_ids")
catalog_name=dbutils.widgets.get("catalog_name")

# COMMAND ----------

entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
id_key = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="id_key", debugValue=id_key)
dataset_objects = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="dataset_objects", debugValue=dataset_objects)
default_survivorship_rules = dbutils.jobs.taskValues.get(taskKey="Parse_Entity_Model_JSON", key="default_survivorship_rules", debugValue=default_survivorship_rules)
entity_attributes = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity_attributes", debugValue=entity_attributes)
config_thresold = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "config_thresold", debugValue=config_thresold)

# COMMAND ----------

dataset_objects = json.loads(dataset_objects)
default_survivorship_rules = json.loads(default_survivorship_rules)
entity_attributes = json.loads(entity_attributes)
entity_attributes.append("lakefusion_id")
config_thresold = json.loads(config_thresold)
unified_dataset_ids = json.loads(unified_dataset_ids)

# COMMAND ----------

aggregation_strategy_separator = ' • '

# COMMAND ----------

unified_record_ids = []
dataset_ids = []
for unified_dataset_id in unified_dataset_ids:
    unified_record_ids.append(unified_dataset_id.get("id"))
    dataset_ids.append(unified_dataset_id.get("dataset_id"))

# COMMAND ----------

# MAGIC %run ../utils/execute_utils

# COMMAND ----------

# MAGIC %run /Workspace/Lakefusion_Notebooks/match-maven-notebooks/src/utils/match_merge_utils

# COMMAND ----------

unified_table = f"{catalog_name}.silver.{entity}_unified"
master_table = f"{catalog_name}.gold.{entity}_master"
processed_unified_table = f"{catalog_name}.silver.{entity}_processed_unified"
if experiment_id:
  unified_table += f"_{experiment_id}"
  master_table += f"_{experiment_id}"
  processed_unified_table += f"_{experiment_id}"

merge_activities_table = f"{master_table}_merge_activities"
master_attribute_version_sources_table = f"{master_table}_attribute_version_sources"

master_table_list = master_table.split(".")
master_catalog, master_schema, master_table_name = master_table_list[0], master_table_list[1], master_table_list[2]

# COMMAND ----------

# Create a mapping of id to path from dataset_objects
dataset_id_to_path = {dataset['id']: dataset['path'] for dataset in dataset_objects.values()}


# Iterate through the survivorship rules
for rule in default_survivorship_rules:
    if rule.get('strategy') == 'Source System' and 'strategyRule' in rule:
        # Replace the strategyRule with dataset paths
        
        rule['strategyRule'] = [dataset_id_to_path.get(dataset_id) for dataset_id in rule['strategyRule']]

# COMMAND ----------

master_record_query = f"""
SELECT *
FROM {master_table}
WHERE {id_key} = '{master_id}'
"""

master_record_df = spark.sql(master_record_query)

if master_record_df.count() == 0:
    raise Exception(f"Master record with ID {master_id} not found")

# COMMAND ----------

filter_conditions = []
for unified_id, dataset_id in zip(unified_record_ids, dataset_ids):
    filter_conditions.append(f"NOT (u.id_str = '{unified_id}' AND u.dataset.id = '{dataset_id}')")

filter_clause = " AND ".join(filter_conditions)

# Updated query that excludes records with MANUAL_UNMERGE action type
merged_records_query = f"""
WITH MergeActivitiesCast AS (
    SELECT *, 
           COALESCE(CAST(master_{id_key} AS STRING), '') AS master_id_str,
           COALESCE(CAST({id_key} AS STRING), '') AS id_str
    FROM {merge_activities_table}
),
UnifiedCast AS (
    SELECT *,
           COALESCE(CAST({id_key} AS STRING), '') AS id_str
    FROM {unified_table}
),
ValidMerges AS (
    SELECT *
    FROM MergeActivitiesCast
    WHERE master_id_str = '{master_id}'
    AND action_type = '{ActionType.MANUAL_MERGE.value}'
    AND NOT EXISTS (
        SELECT 1
        FROM MergeActivitiesCast unmerge
        WHERE unmerge.master_id_str = '{master_id}'
        AND unmerge.id_str = MergeActivitiesCast.id_str
        AND unmerge.action_type = '{ActionType.MANUAL_UNMERGE.value}'
        AND unmerge.version > MergeActivitiesCast.version
    )
)
SELECT u.* EXCEPT (id_str), m.master_{id_key}
FROM UnifiedCast u
JOIN ValidMerges m 
  ON u.id_str = m.id_str
WHERE 
  {filter_clause}
"""

remaining_merged_records_df = spark.sql(merged_records_query)

# Check if any records will remain after unmerge
is_only_record = remaining_merged_records_df.count() == 0

# COMMAND ----------

initial_record_query = f"""
WITH RankedVersions AS (
    SELECT {id_key}, version, attribute_source_mapping,
        ROW_NUMBER() OVER (PARTITION BY {id_key} ORDER BY version ASC) AS rn
    FROM {master_attribute_version_sources_table}
    WHERE {id_key} = '{master_id}'
)
SELECT {id_key}, version, attribute_source_mapping
FROM RankedVersions
WHERE rn = 1
"""

initial_record_df = spark.sql(initial_record_query)

if initial_record_df.count() == 0:
    raise Exception(f"No initial version found for master record {master_id}")

initial_record = initial_record_df.first()
initial_version = initial_record["version"]
initial_attribute_mapping = initial_record["attribute_source_mapping"]

initial_values = {}
initial_sources = {}

for attr_map in initial_attribute_mapping:
    attr_name = attr_map.attribute_name
    attr_value = attr_map.attribute_value
    attr_source = attr_map.source
    
    if attr_name and attr_name != id_key:
        initial_values[attr_name] = attr_value
        initial_sources[attr_name] = attr_source

# COMMAND ----------

attribute_source_query = f"""
WITH RankedRecords AS (
    SELECT {id_key}, version, attribute_source_mapping,
        ROW_NUMBER() OVER (PARTITION BY {id_key} ORDER BY version DESC) AS rn
    FROM {master_attribute_version_sources_table}
    WHERE {id_key} = '{master_id}'
)
SELECT {id_key}, version, attribute_source_mapping
FROM RankedRecords
WHERE rn = 1
"""

current_attribute_source_df = spark.sql(attribute_source_query)
current_attribute_source = current_attribute_source_df.first()["attribute_source_mapping"]


# COMMAND ----------

if not is_only_record:
    mergable_records_df = remaining_merged_records_df.select(
        *[col(attr) for attr in entity_attributes],
        col("dataset.id").alias("dataset_id"),
        col("dataset.path").alias("dataset_path"),
        col(f"master_{id_key}")
    )
    
    mergable_records_df_grouped = (
        mergable_records_df
        .select(
            col(id_key).alias(f'master_{id_key}'),
            struct(*[col(col_name) for col_name in entity_attributes]).alias('unified_record'),
            col('dataset_path'),
        )
        .groupBy(f'master_{id_key}')
        .agg(
            collect_list('unified_record').alias('unified_records'),
            collect_list('dataset_path').alias('unified_dataset_paths')
        )
    )
    
    master_record_struct = struct(*[col(col_name) for col_name in entity_attributes])
    
    mergable_records_df_grouped = (
        mergable_records_df_grouped
        .crossJoin(master_record_df.select(master_record_struct.alias('master_record')))
        .crossJoin(current_attribute_source_df.select(col('attribute_source_mapping').alias('master_attribute_source_mapping')))
    )


# COMMAND ----------

def apply_source_system_strategy(attribute, strategyRule, unified_records, master_record, unified_datasets, master_attribute_source_mapping_column, ignore_null=False):
    result_value = initial_values.get(attribute)
    result_source = initial_sources.get(attribute)
    
    # Apply ignore_null to initial value
    if ignore_null and (result_value is None or result_value == ""):
        result_value = None
        result_source = None

    for priority_dataset in strategyRule:
        try:
            unified_dataset_index = unified_datasets.index(priority_dataset)
            unified_record_column_value = unified_records[unified_dataset_index].get(attribute)
            
            # Apply ignore_null logic
            if ignore_null and (unified_record_column_value is None or unified_record_column_value == ""):
                continue
                
            if unified_record_column_value is not None:
                result_value = unified_record_column_value
                result_source = priority_dataset
                return result_value, result_source
        except (ValueError, IndexError):
            pass
    
    # Fallback to any available record if ignore_null allows it
    for idx, record in enumerate(unified_records):
        if idx < len(unified_datasets):
            record_value = record.get(attribute)
            if record_value is not None:
                if not ignore_null or (record_value != ""):
                    result_value = record_value
                    result_source = unified_datasets[idx]
                    return result_value, result_source
    
    return result_value, result_source

def apply_aggregation_strategy(attribute, master_record, unified_records, initial_value=None, ignore_null=False, aggregate_unique_only=False):
    # Get unified values
    unified_values = []
    for record in unified_records:
        value = record.get(attribute)
        if value is not None:
            if not ignore_null or value != "":
                unified_values.append(value)
    
    all_values = unified_values[:]
    
    # Handle initial value based on ignore_null
    if initial_value is not None:
        if not ignore_null or initial_value != "":
            all_values.append(initial_value)
    
    # Flatten aggregated values (split by separator)
    flat_values = []
    for value in all_values:
        parts = str(value).split(aggregation_strategy_separator)
        flat_values.extend(parts)
    
    # Clean and filter values
    clean_values = [v.strip() for v in flat_values if v.strip()]
    
    # Apply unique logic
    if aggregate_unique_only:
        # Preserve order while removing duplicates
        seen = set()
        unique_values = []
        for value in clean_values:
            if value not in seen:
                seen.add(value)
                unique_values.append(value)
        clean_values = unique_values
    else:
        # Original logic: use set for deduplication
        clean_values = list(set(clean_values))
    
    return aggregation_strategy_separator.join(clean_values) if clean_values else None

# COMMAND ----------

def process_survivorship_rules(unified_records, master_record, unified_datasets, master_attribute_source_mapping):
    master_record = master_record.asDict()
    unified_records = [record.asDict() for record in unified_records]
    master_attribute_source_mapping = [record.asDict() for record in master_attribute_source_mapping]

    resultant_record = {}
    resultant_master_attribute_source_mapping = []

    resultant_record[id_key] = master_record.get(id_key)

    for attribute in entity_attributes:
        if attribute == id_key:
            continue
        
        master_attribute_source_mapping_column = next(
            (item for item in master_attribute_source_mapping if item.get('attribute_name') == attribute), 
            None
        )
        
        initial_value = initial_values.get(attribute)
        initial_source = initial_sources.get(attribute)
        
        rule = next(
            (item for item in default_survivorship_rules 
             if item.get('attribute') == attribute or item.get('entity_attribute') == attribute), 
            None
        )
        
        result_value = initial_value
        result_source = initial_source
        
        if rule:
            # Extract toggle properties
            ignore_null = rule.get('ignoreNull', False)
            
            if rule['strategy'] == 'Source System':
                strategyRule = rule['strategyRule']
                value, source = apply_source_system_strategy(
                    attribute, strategyRule, unified_records, master_record, 
                    unified_datasets, master_attribute_source_mapping_column, ignore_null
                )
                result_value = value
                result_source = source
                
            elif rule['strategy'] == 'Aggregation':
                aggregate_unique_only = rule.get('aggregateUniqueOnly', False)
                aggregated_value = apply_aggregation_strategy(
                    attribute, master_record, unified_records, initial_value, 
                    ignore_null, aggregate_unique_only
                )
                result_value = aggregated_value
                
                sources = [s for s in [initial_source] + unified_datasets if s]
                result_source = ','.join(sources)
                
            elif rule['strategy'] == 'Recency':
                # Apply recency strategy with ignore_null
                for idx in range(len(unified_records) - 1, -1, -1):
                    if idx < len(unified_datasets):
                        record_value = unified_records[idx].get(attribute)
                        if record_value is not None:
                            if not ignore_null or record_value != "":
                                result_value = record_value
                                result_source = unified_datasets[idx]
                                break
                
                # If no valid unified record found and ignore_null is True, check initial value
                if ignore_null and result_value == initial_value and (initial_value is None or initial_value == ""):
                    result_value = None
                    result_source = None
        
        resultant_record[attribute] = result_value
        
        if result_value is not None:
            attr_value_str = str(result_value)
        else:
            attr_value_str = None
            
        resultant_master_attribute_source_mapping.append({
            'attribute_name': attribute,
            'attribute_value': attr_value_str,
            'source': result_source
        })
    
    return {
        'resultant_record': resultant_record,
        'resultant_master_attribute_source_mapping': resultant_master_attribute_source_mapping
    }

# COMMAND ----------

if is_only_record:
    logger.info("No records remain after unmerge. Using initial version values.")
    
    initial_record_values = {id_key: master_id}
    initial_record_mapping = []
    
    for attr in entity_attributes:
        if attr == id_key:
            continue
        
        attr_value = initial_values.get(attr)
        attr_source = initial_sources.get(attr)
        initial_record_values[attr] = attr_value
        
        if attr_value is not None:
            attr_value_str = str(attr_value)
        else:
            attr_value_str = None
            
        initial_record_mapping.append({
            'attribute_name': attr,
            'attribute_value': attr_value_str,
            'source': attr_source
        })
    
    updated_records = [initial_record_values]
    updated_attribute_mappings = [{
        id_key: master_id,
        'version': None,
        'attribute_source_mapping': initial_record_mapping
    }]
else:
    resultant_record_schema = mergable_records_df_grouped.select("master_record").schema["master_record"].dataType
    attribute_source_mapping_schema = mergable_records_df_grouped.select("master_attribute_source_mapping").schema["master_attribute_source_mapping"].dataType
    
    udf_output_schema = StructType([
        StructField("resultant_record", resultant_record_schema, True),
        StructField("resultant_master_attribute_source_mapping", attribute_source_mapping_schema, True)
    ])
    
    process_survivorship_udf = udf(process_survivorship_rules, udf_output_schema)
    
    mergable_records_df_grouped_survivorship = (
        mergable_records_df_grouped
        .withColumn(
            'survivorship_result', 
            process_survivorship_udf(
                col('unified_records'),
                col('master_record'),
                col('unified_dataset_paths'),
                col('master_attribute_source_mapping')
            )
        )
        .withColumn('resultant_record', col('survivorship_result.resultant_record'))
        .withColumn('resultant_master_attribute_source_mapping', col('survivorship_result.resultant_master_attribute_source_mapping'))
    )
    
    resultant_record_df = (
        mergable_records_df_grouped_survivorship
        .select('resultant_record')
    )
    
    nested_fields = resultant_record_df.schema["resultant_record"].dataType.fields
    
    columns_to_select = [col(f"resultant_record.{field.name}").alias(field.name) for field in nested_fields]
    
    updated_records_df = resultant_record_df.select(*columns_to_select)
    
    updated_attribute_mapping_df = (
        mergable_records_df_grouped_survivorship
        .select(
            col(f'resultant_record.{id_key}').alias(id_key),
            lit(None).alias('version'),
            col('resultant_master_attribute_source_mapping').alias('attribute_source_mapping')
        )
    )

# COMMAND ----------

if is_only_record:
    record_fields = []
    record_fields.append(StructField(id_key, StringType(), False))
    
    for attr in entity_attributes:
        if attr != id_key:
            record_fields.append(StructField(attr, StringType(), True))
    
    record_schema = StructType(record_fields)
    
    updated_records_df = spark.createDataFrame(updated_records, schema=record_schema)
    
    attr_struct = StructType([
        StructField("attribute_name", StringType(), True),
        StructField("attribute_value", StringType(), True),
        StructField("source", StringType(), True)
    ])
    
    attr_mapping_schema = StructType([
        StructField(id_key, StringType(), False),
        StructField("version", IntegerType(), True),
        StructField("attribute_source_mapping", ArrayType(attr_struct), True)
    ])
    
    updated_attribute_mapping_df = spark.createDataFrame(updated_attribute_mappings, schema=attr_mapping_schema)


# COMMAND ----------

# Get the unmerged record(s) that need to become new master records
unmerged_records_query = f"""
SELECT u.*
FROM {unified_table} u
WHERE u.{id_key} IN ({','.join([f"'{uid}'" for uid in unified_record_ids])})
"""
unmerged_records_df = spark.sql(unmerged_records_query)

if is_only_record:
    # When no records remain, we need to:
    # 1. Update the existing master record to revert to initial values
    # 2. Insert the unmerged record(s) as NEW master record(s)
    
    # Step 1: Update existing master record with initial values
    master_columns = master_record_df.columns
    extra_columns = [c for c in master_columns if c not in updated_records_df.columns]
    
    for col_name in extra_columns:
        updated_records_df = updated_records_df.withColumn(
            col_name, 
            lit(master_record_df.select(col_name).first()[0])
        )
    
    delta_table = DeltaTable.forName(spark, master_table)
    delta_table.alias("target").merge(
        source=updated_records_df.alias("source"),
        condition=f"target.{id_key} = source.{id_key}"
    ).whenMatchedUpdateAll().execute()
    
    # Step 2: Insert unmerged record(s) as new master record(s)
    unmerged_master_records = unmerged_records_df.select(
        *[col(attr) for attr in entity_attributes]
    )
    
    # Add any extra columns from master table schema
    for col_name in master_columns:
        if col_name not in unmerged_master_records.columns:
            # Use default values for system columns
            if col_name == 'created_at':
                unmerged_master_records = unmerged_master_records.withColumn(col_name, current_timestamp())
            elif col_name == 'updated_at':
                unmerged_master_records = unmerged_master_records.withColumn(col_name, current_timestamp())
            else:
                unmerged_master_records = unmerged_master_records.withColumn(col_name, lit(None))
    
    # Insert new master records
    unmerged_master_records.write.format("delta").mode("append").saveAsTable(master_table)
    
else:
    # Original logic: update the master record with merged values
    master_columns = master_record_df.columns
    extra_columns = [c for c in master_columns if c not in updated_records_df.columns]
    
    for col_name in extra_columns:
        updated_records_df = updated_records_df.withColumn(
            col_name, 
            lit(master_record_df.select(col_name).first()[0])
        )
    
    delta_table = DeltaTable.forName(spark, master_table)
    delta_table.alias("target").merge(
        source=updated_records_df.alias("source"),
        condition=f"target.{id_key} = source.{id_key}"
    ).whenMatchedUpdateAll().execute()

master_version = get_current_table_version(master_table)

# COMMAND ----------

updated_attribute_mapping_df = updated_attribute_mapping_df.withColumn('version', lit(master_version))

sql_parts = []
for row in updated_attribute_mapping_df.collect():
    attr_structs = []
    for attr_map in row.attribute_source_mapping:
        attr_name = attr_map.attribute_name
        
        if attr_map.attribute_value:
            escaped_value = attr_map.attribute_value.replace("'", "''")
            attr_value = f"'{escaped_value}'"
        else:
            attr_value = "NULL"
            
        if attr_map.source:
            escaped_source = attr_map.source.replace("'", "''")
            attr_source = f"'{escaped_source}'"
        else:
            attr_source = "NULL"
        
        attr_structs.append(f"named_struct('attribute_name', '{attr_name}', 'attribute_value', {attr_value}, 'source', {attr_source})")
    
    attr_array = f"array({', '.join(attr_structs)})"
    sql_parts.append(f"('{row[id_key]}', {row.version}, {attr_array})")

insert_sql = f"""
INSERT INTO {master_attribute_version_sources_table}
({id_key}, version, attribute_source_mapping)
VALUES {', '.join(sql_parts)}
"""

spark.sql(insert_sql)

# COMMAND ----------

unmerge_values = []
for unified_id, dataset_id in zip(unified_record_ids, dataset_ids):
    dataset_path = dataset_id_to_path.get(int(dataset_id))
    unmerge_values.append(f"('{master_id}', '{unified_id}', '{dataset_path}', {master_version}, '{ActionType.MANUAL_UNMERGE.value}')")

values_clause = ", ".join(unmerge_values)
unmerge_sql = f"""
INSERT INTO {merge_activities_table}
(`master_{id_key}`, `{id_key}`, source, version, action_type)
VALUES {values_clause}
"""

spark.sql(unmerge_sql)

