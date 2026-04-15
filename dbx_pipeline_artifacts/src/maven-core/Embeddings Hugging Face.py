# Databricks notebook source
# MAGIC %pip install "transformers>=4.51.0,<5.0.0"
# MAGIC %pip install llama-index
# MAGIC %pip install --upgrade mlflow-skinny
# MAGIC %pip install sentence-transformers
# MAGIC %pip install databricks-vectorsearch
# MAGIC %pip install "accelerate>=1.12.0"
# MAGIC %pip install torchvision

# COMMAND ----------

pip install mlflow[databricks]

# COMMAND ----------

pip install accelerate

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("embedding_model", "", "Embedding Model Hugging Face")
dbutils.widgets.text("catalog_name", "", "lakefusion catalog name")
dbutils.widgets.text("experiment_id", "", "Experiment ID")

# COMMAND ----------

embedding_model = dbutils.widgets.get("embedding_model")
catalog_name = dbutils.widgets.get("catalog_name")
experiment_id = dbutils.widgets.get("experiment_id")

# COMMAND ----------

embedding_model = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "embedding_model", debugValue=embedding_model)

# COMMAND ----------

embedding_model_endpoint = embedding_model.replace('/', '-').replace('.', '_').lower()
mlflow_model_name = f"{catalog_name}.embedding.{embedding_model_endpoint}"

# COMMAND ----------

embedding_model_endpoint, embedding_model, mlflow_model_name

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
   #from transformers.modeling_utils import init_empty_weights
   client = mlflow.MlflowClient()
   get_model = client.get_registered_model(mlflow_model_name)
   latest_version_model = max(client.search_model_versions(f"name='{mlflow_model_name}'"), key=lambda mv: int(mv.version)).version
   
except Exception as err:
    if("RESOURCE_DOES_NOT_EXIST" in str(err)):
        import mlflow
        from sentence_transformers import SentenceTransformer
        
        model_path = embedding_model.replace("/", "-").replace(".", "_").lower()
        artifact_path = model_path
        
        try:
            model = SentenceTransformer(embedding_model)
        except Exception as e:
            raise RuntimeError(f"Failed to download/load HuggingFace embedding model '{embedding_model}': {str(e)}") from e
        with mlflow.start_run():
            model_info = mlflow.sentence_transformers.log_model(
                model=model,
                artifact_path=artifact_path,
                input_example="The capital of France is",
                registered_model_name=mlflow_model_name,
                pip_requirements=["mlflow>=2.0", "torch>=2.0", "transformers>=4.51.0,<5.0.0", "accelerate>=1.12.0", "torchvision", "sentence_transformers"],
            )

        # Resource Tagging
        try:
            apply_tags_to_uc_resource("models", mlflow_model_name, tags)
        except Exception as e:
            logger.error(f"Exception occurred while tagging the model. Reason - {str(e)}")

        latest_version_model=model_info.registered_model_version

    else:
        raise RuntimeError(f"Failed to check/register embedding model '{mlflow_model_name}': {str(err)}") from err
        
     
  

# COMMAND ----------

should_create = check_and_cleanup_failed_endpoint(embedding_model_endpoint)

# COMMAND ----------

try:
    if should_create:
        endpoint = create_serving_endpoint(
            embedding_model_endpoint,
            mlflow_model_name,
            entity_version=latest_version_model,
        )
        logger.info(f"Endpoint '{embedding_model_endpoint}' is ready")
    else:
        logger.info(f"Endpoint '{embedding_model_endpoint}' already exists and is healthy.")
except Exception as err:
    raise RuntimeError(f"Failed to create serving endpoint '{embedding_model_endpoint}': {str(err)}") from err

# COMMAND ----------

dbutils.jobs.taskValues.set("served_entity", mlflow_model_name)
dbutils.jobs.taskValues.set("served_entity_version", latest_version_model)
dbutils.jobs.taskValues.set("embedding_model_endpoint", embedding_model_endpoint)

# COMMAND ----------

output_json = {
    "served_entity": mlflow_model_name,
    "served_entity_version": latest_version_model,
    "embedding_model_endpoint" : embedding_model_endpoint
}
logger_instance.shutdown()
dbutils.notebook.exit(output_json)
