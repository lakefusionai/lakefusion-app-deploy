# Databricks notebook source
%pip install "transformers>=4.51.0,<5.0.0"
%pip install llama-index
%pip install --upgrade mlflow-skinny
%pip install databricks-vectorsearch
%pip install "accelerate>=1.12.0"
%pip install torchvision

%restart_python

# COMMAND ----------

import mlflow
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

dbutils.widgets.text("llm_model", "", "LLM Model Hugging Face")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")

# COMMAND ----------

llm_model = dbutils.widgets.get("llm_model")
catalog_name = dbutils.widgets.get("catalog_name")
experiment_id = dbutils.widgets.get("experiment_id")

# COMMAND ----------

llm_model = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "llm_model", debugValue=llm_model)

# COMMAND ----------

llm_model_endpoint = llm_model.replace('/', '-').replace('.', '_').lower()
mlflow_model_name = f"{catalog_name}.llm.{llm_model_endpoint}"

# COMMAND ----------

llm_model_endpoint, llm_model, mlflow_model_name

# COMMAND ----------

# MAGIC %run ../utils/execute_utils

# COMMAND ----------

# MAGIC %run ../utils/model_serving

# COMMAND ----------

# MAGIC %run ../utils/parse_utils

# COMMAND ----------

tags = get_resource_tags()
if tags is None:
    tags = {}

# COMMAND ----------

try:
   import mlflow
   client = mlflow.MlflowClient()
   get_model = client.get_registered_model(mlflow_model_name)
   latest_version_model = max(client.search_model_versions(f"name='{mlflow_model_name}'"), key=lambda mv: int(mv.version)).version
   
except Exception as err:
    if("RESOURCE_DOES_NOT_EXIST" in str(err)):
        import mlflow
        import transformers
        
        try:
            dolly = transformers.pipeline(
                task="text-generation",
                model=llm_model,
                dtype="auto",
                device_map="auto",
                trust_remote_code=True,
            )
        except Exception as e:
            raise RuntimeError(f"Failed to download/load HuggingFace model '{llm_model}': {str(e)}") from e
        
        local_model_path = llm_model.replace("/", "-").replace(".", "_").lower()
        artifact_path = local_model_path
        hf_model_name = llm_model.split("/")[1]
        
        with mlflow.start_run():
            model_info = mlflow.transformers.log_model(
                transformers_model=dolly,
                artifact_path=artifact_path,
                input_example="The capital of France is",
                registered_model_name=mlflow_model_name,
                pip_requirements=[
                "mlflow>=2.0",
                "torch>=2.0",
                "transformers>=4.51.0,<5.0.0",
                "accelerate>=1.12.0",
                "torchvision",
            ],
            )

        # Resource Tagging
        try:
            apply_tags_to_uc_resource("models", mlflow_model_name, tags)
        except Exception as e:
            logger.error(f"Exception occurred while tagging the model. Reason - {str(e)}")
       
        latest_version_model=model_info.registered_model_version 

    else:
        raise RuntimeError(f"Failed to check/register model '{mlflow_model_name}': {str(err)}") from err
        
     
  

# COMMAND ----------

should_create = check_and_cleanup_failed_endpoint(llm_model_endpoint)

# COMMAND ----------

try:
    if should_create:
        endpoint = create_serving_endpoint(
            llm_model_endpoint,
            mlflow_model_name,
            entity_version=latest_version_model,
        )
        logger.info(f"Endpoint '{llm_model_endpoint}' is ready")
    else:
        logger.info(f"Endpoint '{llm_model_endpoint}' already exists and is healthy.")
except Exception as err:
    raise RuntimeError(f"Failed to create serving endpoint '{llm_model_endpoint}': {str(err)}") from err

# COMMAND ----------

dbutils.jobs.taskValues.set("served_entity", mlflow_model_name)
dbutils.jobs.taskValues.set("served_entity_version", latest_version_model)
dbutils.jobs.taskValues.set("llm_model_endpoint", llm_model_endpoint)


# COMMAND ----------

# DBTITLE 1,Compose notebook return value
output_json = {
    "served_entity": mlflow_model_name,
    "served_entity_version": latest_version_model,
    "llm_model_endpoint" : llm_model_endpoint
}
logger_instance.shutdown()
dbutils.notebook.exit(output_json)
