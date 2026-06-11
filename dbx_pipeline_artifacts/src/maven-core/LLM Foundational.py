# Databricks notebook source
# MAGIC %pip install --upgrade "mlflow-skinny[databricks]" databricks-vectorsearch "databricks-sdk>=0.85.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

dbutils.widgets.text("llm_model", "", "LLM Model Foundational")
dbutils.widgets.text("llm_provisionless", "", "LLM Model Foundational Provisionless?")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")

# COMMAND ----------

llm_model = dbutils.widgets.get("llm_model")
llm_provisionless = dbutils.widgets.get("llm_provisionless")

# COMMAND ----------

llm_model = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "llm_model", debugValue=llm_model)
llm_provisionless = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "llm_provisionless", debugValue=llm_provisionless)
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

served_entity = resolve_entity_name(llm_model)

# COMMAND ----------

# MAGIC %run ../utils/parse_utils

# COMMAND ----------

# MAGIC %run ../utils/taskvalues_enum

# COMMAND ----------

import json

# Get PT models config from task values
pt_models_config_str = dbutils.jobs.taskValues.get(
    "Parse_Entity_Model_JSON",
    TaskValueKey.PT_MODELS_CONFIG.value,
    debugValue="{}"
)
pt_models_config = json.loads(pt_models_config_str)

logger.info(f"Loaded PT config for {len(pt_models_config)} models")

# COMMAND ----------

if(llm_provisionless==False):
    llm_endpoint_name = f"lakefusion-{llm_model}"
    should_create = check_and_cleanup_failed_endpoint(llm_endpoint_name)

    if should_create:
        # Get PT config for this model
        pt_config = pt_models_config.get(served_entity)

        if pt_config:
            logger.info(f"PT config found for {served_entity}: {pt_config}")
            create_serving_endpoint_foundational(
                endpoint_name=llm_endpoint_name,
                serving_entity=served_entity,
                pt_config=pt_config
            )
        else:
            logger.warning(f"No PT config found for {served_entity}.")
            logger.warning(f"Falling back to pay-per-token mode without provisioned throughput.")
            llm_endpoint_name = llm_model  # Remove lakefusion- prefix
            logger.info(f"Updated endpoint name to: {llm_endpoint_name}")

    else:
       logger.info("Endpoint already exists and is healthy")
else:
    llm_endpoint_name = f"{llm_model}"


# COMMAND ----------



# COMMAND ----------

dbutils.jobs.taskValues.set("served_entity", served_entity)
dbutils.jobs.taskValues.set("llm_model_endpoint", llm_endpoint_name)


# COMMAND ----------

# DBTITLE 1,Compose notebook return value
output_json = {
    "served_entity": served_entity,
    "llm_model_endpoint" : llm_endpoint_name
}
logger_instance.shutdown()
dbutils.notebook.exit(output_json)
