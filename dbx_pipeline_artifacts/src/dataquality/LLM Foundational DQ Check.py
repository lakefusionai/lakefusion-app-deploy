# Databricks notebook source
# MAGIC %pip install --upgrade "mlflow-skinny[databricks]"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json
from typing import Dict, List, Any
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

class HttpResponse:
    def __init__(self, message=None, status=None, data=None, file=None, has_more=False, totalCount=None, session_token=None):
        self.message = message
        self.status = status
        self.data = data
        self.file = file
        self.has_more = has_more
        self.totalCount = totalCount
        self.session_token = session_token

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__)

def exit_with_response(message: str, status: int, data=None, file=None, has_more=False, totalCount=None, session_token=None):
    response = HttpResponse(
        message=message,
        status=status,
        data=data,
        file=file,
        has_more=has_more,
        totalCount=totalCount,
        session_token=session_token
    )
    dbutils.notebook.exit(response.to_json())

# COMMAND ----------

dbutils.widgets.text("transformations_config", "")

# COMMAND ----------

transformations_config = dbutils.widgets.get("transformations_config")
if not transformations_config:
    exit_with_response("Error: transformations_config is required", 400)

# Parse transformations configuration
try:
    transformations = json.loads(transformations_config)
    
except json.JSONDecodeError as e:
    exit_with_response(f"Error: Invalid transformations configuration JSON: {str(e)}", 400)

# COMMAND ----------

llm_model='databricks-meta-llama-3-3-70b-instruct' # temp: added Roz King 2025-04-30

try:
    list_llm_endpoint=[]
    for i in transformations:
        if(i['type']=='lookup'):
          llm_model=i['config']['llm_model']
except Exception as e:
  print(e)

# COMMAND ----------

model_path_mappings = {
  "databricks-meta-llama-3-1-70b-instruct": "system.ai.llama_v3_1_70b_instruct",
  "databricks-meta-llama-3-3-70b-instruct": "system.ai.llama_v3_3_70b_instruct",
  "databricks-gte-large-en": "system.ai.gte_large_en_v1_5",
  "databricks-meta-llama-3-1-405b-instruct": "system.ai.meta_llama_v3_1_70b_instruct",
  "databricks-dbrx-instruct": "system.ai.dbrx_instruct",
  "databricks-mixtral-8x7b-instruct": "system.ai.mixtral_8x7b_instruct_v0_1",
  "databricks-bge-large-en": "system.ai.bge_large_en_v1_5"
}
served_entity = model_path_mappings.get(llm_model, '')

# COMMAND ----------

# DBTITLE 1,Import model serving utils
# MAGIC %run ../utils/model_serving

# COMMAND ----------

llm_endpoint_name = f"lakefusion-{llm_model}"

# COMMAND ----------

# DBTITLE 1,Create model serving endpoint
endpoint_name_exists = False
try:
  client = get_deploy_client("databricks")
  client.get_endpoint(llm_endpoint_name)
  endpoint_name_exists = True
except:
  pass

# COMMAND ----------

dbutils.jobs.taskValues.set("is_llm_endpoint", endpoint_name_exists)
