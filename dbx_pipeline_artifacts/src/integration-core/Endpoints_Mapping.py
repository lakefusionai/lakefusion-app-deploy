# Databricks notebook source
dbutils.widgets.text("embedding_model", "", "Embedding Model")
dbutils.widgets.text("llm_model", "", "LLM Model")
dbutils.widgets.dropdown("llm_model_source", "databricks_foundation", ["databricks_custom_hugging_face", "databricks_foundation"], "LLM Model Source")
dbutils.widgets.dropdown("embedding_model_source", "databricks_foundation", ["databricks_custom_hugging_face", "databricks_foundation"], "Embedding Model Source")
dbutils.widgets.text("llm_provisionless", "", "LLM Model Foundational Provisionless?")
dbutils.widgets.text("embedding_provisionless", "", "Embedding Model Foundational Provisionless?")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")


# COMMAND ----------

embedding_model = dbutils.widgets.get("embedding_model")
embedding_model_source = dbutils.widgets.get("embedding_model_source")
llm_model = dbutils.widgets.get("llm_model")
llm_model_source = dbutils.widgets.get("llm_model_source")
llm_provisionless = dbutils.widgets.get("llm_provisionless")
embedding_provisionless = dbutils.widgets.get("embedding_provisionless")

# COMMAND ----------

embedding_model = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "embedding_model", debugValue=embedding_model)
embedding_model_source = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "embedding_model_source", debugValue=embedding_model_source)
llm_model = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "llm_model", debugValue=llm_model)
llm_model_source = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "llm_model_source", debugValue=llm_model_source)
llm_provisionless = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "llm_provisionless", debugValue=llm_provisionless)
embedding_provisionless = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "embedding_provisionless", debugValue=embedding_provisionless)
catalog_name = dbutils.jobs.taskValues.get(
    taskKey="Parse_Entity_Model_JSON",
    key="catalog_name",
    debugValue=dbutils.widgets.get("catalog_name")
)
experiment_id = dbutils.widgets.get("experiment_id")

# COMMAND ----------

# MAGIC %run ../utils/execute_utils

# COMMAND ----------

# MAGIC %run ../utils/model_serving

# COMMAND ----------

if embedding_model_source == 'databricks_foundation':
  if embedding_provisionless:
    embedding_endpoint_name = f"{embedding_model}"
  else:
    pt_endpoint = f"lakefusion-{embedding_model}"
    if is_endpoint_healthy(pt_endpoint):
      embedding_endpoint_name = pt_endpoint
    else:
      logger.warning(f"PT endpoint '{pt_endpoint}' not found or failed - falling back to pay-per-token")
      embedding_endpoint_name = f"{embedding_model}"
elif embedding_model_source == 'databricks_custom_hugging_face':
  embedding_endpoint_name = embedding_model.replace('/', '-').replace('.', '_').lower()

if llm_model_source == 'databricks_foundation':
    if llm_provisionless:
        llm_endpoint_name = f"{llm_model}"
    else:
        pt_endpoint = f"lakefusion-{llm_model}"
        if is_endpoint_healthy(pt_endpoint):
            llm_endpoint_name = pt_endpoint
        else:
            logger.warning(f"PT endpoint '{pt_endpoint}' not found or failed - falling back to pay-per-token")
            llm_endpoint_name = f"{llm_model}"
elif llm_model_source == 'databricks_custom_hugging_face':
    llm_endpoint_name = llm_model.replace('/', '-').replace('.', '_').lower()

# COMMAND ----------

embedding_endpoint_name, llm_endpoint_name

# COMMAND ----------

dbutils.jobs.taskValues.set("embedding_model_endpoint", embedding_endpoint_name)
dbutils.jobs.taskValues.set("llm_model_endpoint", llm_endpoint_name)

# COMMAND ----------

# DBTITLE 1,Compose notebook return value
output_json = {
    "embedding_model_endpoint" : embedding_endpoint_name,
    "llm_model_endpoint" : llm_endpoint_name
}
logger_instance.shutdown()
dbutils.notebook.exit(output_json)
