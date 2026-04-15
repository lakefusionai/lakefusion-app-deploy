# Databricks notebook source
dbutils.widgets.text("is_golden_deduplication","")
is_golden_deduplication=dbutils.widgets.get("is_golden_deduplication")
is_golden_deduplication=dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON","is_golden_deduplication",debugValue=is_golden_deduplication)

dbutils.widgets.text("incremental_load","")
incremental_load=dbutils.widgets.get("incremental_load")

# COMMAND ----------

if(is_golden_deduplication==1):
    incremental_load=dbutils.jobs.taskValues.get("Prepare_Tables_DeDuplicate","incremental_load",debugValue=incremental_load)
else:
    incremental_load=dbutils.jobs.taskValues.get("Prepare_Tables","incremental_load",debugValue=incremental_load)


dbutils.jobs.taskValues.set("incremental_load",incremental_load)
