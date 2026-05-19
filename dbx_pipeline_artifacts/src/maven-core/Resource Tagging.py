# Databricks notebook source
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("experiment_id", "", "Match Maven Experiment ID")

# COMMAND ----------

catalog_name=dbutils.widgets.get("catalog_name")
entity = dbutils.widgets.get("entity")
experiment_id = dbutils.widgets.get("experiment_id")
experiment_id = experiment_id.replace("-", "")

# COMMAND ----------

entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)

# COMMAND ----------

entity = entity.lower().replace(' ', '_')

# COMMAND ----------

query = f"""
SELECT
  table_catalog,
  table_schema,
  concat(table_catalog, '.', table_schema) as schema_path,
  table_name,
  concat(table_catalog, '.', table_schema, '.', table_name) as table_path
FROM system.information_schema.tables
WHERE table_catalog = '{catalog_name}'
  AND table_name LIKE '%{entity}%{experiment_id}%'
ORDER BY table_schema
"""
entity_tables = spark.sql(query)

# COMMAND ----------

entity_tables.display()

# COMMAND ----------

# MAGIC %run ../utils/parse_utils

# COMMAND ----------

tags = get_resource_tags()
if tags is None:
    tags = {}

# COMMAND ----------

# Add tags to all the entity schemas
schemas = entity_tables.select("schema_path").distinct()
for schema in schemas.collect():
  schema_val = schema[0]
  apply_tags_to_uc_resource("schemas", schema_val, tags)

# COMMAND ----------

# Add tags to all the entity tables
tables = entity_tables.select("table_path").distinct()
for table in tables.collect():
  table = table[0]
  apply_tags_to_uc_resource("tables", table, tags)