# Databricks notebook source
dbutils.widgets.text("query_1",'')
query_1=dbutils.widgets.get("query_1")
dbutils.widgets.text("query_2",'')
query_2=dbutils.widgets.get("query_2")
dbutils.widgets.text("query_3",'')
query_3=dbutils.widgets.get("query_3")


# COMMAND ----------

spark.sql(f"{query_1}")

# COMMAND ----------

spark.sql(f"{query_2}")

# COMMAND ----------

spark.sql(f"{query_3}")
