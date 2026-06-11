# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

# COMMAND ----------

# The following line automatically generates a PAT Token for authentication
client = VectorSearchClient()

# The following line uses the service principal token for authentication
# client = VectorSearch(service_principal_client_id=<CLIENT_ID>,service_principal_client_secret=<CLIENT_SECRET>)

client.create_endpoint(
    name="embeddings_endpoint",
    endpoint_type="STANDARD"
)


# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE lakefusion_workspace.default.patient_data SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

client = VectorSearchClient()

index = client.create_delta_sync_index(
  endpoint_name="embeddings_endpoint",
  source_table_name="lakefusion_workspace.default.patient_data",
  index_name="lakefusion_workspace.default.patient_data_bronze_index",
  pipeline_type="TRIGGERED",
  primary_key="Patient_ID",
  embedding_source_column="Name",
  embedding_model_endpoint_name="mpnetv2",
  sync_computed_embeddings=True
)


# COMMAND ----------

index.wait_until_ready()


# COMMAND ----------

results = index.similarity_search(
    query_text="Carol",
    columns=["Patient_ID", "Name"],
    num_results=2
    )
print(results)

# COMMAND ----------

# MAGIC %md
# MAGIC #Test mini-LM-v6
# MAGIC

# COMMAND ----------

client = VectorSearchClient()

index = client.create_delta_sync_index(
  endpoint_name="embeddings_endpoint",
  source_table_name="lakefusion_workspace.default.patient_data",
  index_name="lakefusion_workspace.default.patient_data_bronze_index_minilm",
  pipeline_type="TRIGGERED",
  primary_key="Patient_ID",
  embedding_source_column="Name",
  embedding_model_endpoint_name="123",
  sync_computed_embeddings=True
)

# COMMAND ----------

index.wait_until_ready()


# COMMAND ----------


