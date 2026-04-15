# Databricks notebook source
# MAGIC %run ../utils/taskvalues_enum

# COMMAND ----------

# MAGIC %run ../utils/execute_utils

# COMMAND ----------

logger.info("Parsing Entity and Model JSON")

# COMMAND ----------

dbutils.widgets.text('entity_id', '', 'Entity ID')
dbutils.widgets.text('experiment_id', '', 'Experiment ID')
dbutils.widgets.text('process_records', '', 'No of records to be proceesed')
dbutils.widgets.text("is_integration_hub", "", "Integration Hub Pipeline")
dbutils.widgets.text("catalog_name", "", "lakefusion catalog name")

# COMMAND ----------

entity_id = dbutils.widgets.get('entity_id')
experiment_id = dbutils.widgets.get('experiment_id')
process_records=dbutils.widgets.get('process_records')
is_integration_hub=dbutils.widgets.get('is_integration_hub')
catalog_name=dbutils.widgets.get('catalog_name')
meta_info_table = f"{catalog_name}.silver.table_meta_info"

# COMMAND ----------

experiment_path = 'prod'
if experiment_id and experiment_id != 'prod':
  experiment_path = f'experiment_{experiment_id}'
entity_json_path = f'/Volumes/{catalog_name}/metadata/metadata_files/entity_{entity_id}_{experiment_path}_entity.json'
model_json_path = f'/Volumes/{catalog_name}/metadata/metadata_files/entity_{entity_id}_{experiment_path}_model.json'
entity_reference_config_json_path = f'/Volumes/{catalog_name}/metadata/metadata_files/entity_{entity_id}_{experiment_path}_reference.json'

# COMMAND ----------

import json

# COMMAND ----------

entity_json_str = ''
with open(entity_json_path, 'r') as f:
  entity_json_str = f.read()

# COMMAND ----------

entity_json = json.loads(entity_json_str)

# COMMAND ----------

# MAGIC %run ../utils/parse_utils

# COMMAND ----------

entity = entity_json.get("name", "entity").lower().replace(" ", "_")
master_table = entity_json.get("path")
primary_table = get_primary_dataset_path(entity_json["dataset_mappings"])
dataset_tables, dataset_objects = get_dataset_tables(entity_json["dataset_mappings"])
entity_attributes = [item["name"] for item in entity_json.get("attributes")]
attributes_mapping = parse_attributes_mapping_json(entity_json["dataset_mappings"])
entity_attributes_datatype = {item["name"]: item["type"] for item in entity_json.get("attributes")}
primary_key = next(
    (attr['name'] for attr in entity_json.get("attributes") if attr.get('is_primary_key') == True), 
    entity_attributes[0]
)
id_key = "lakefusion_id"
default_survivorship_rules = get_default_survivorship_rules(entity_json.get("survivorship", []))
validation_functions = entity_json.get("validation_functions")
dataset_tables_len=len(dataset_tables)
entity_attributes_datatype = {item["name"]: item["type"] for item in entity_json.get("attributes")}
rdm_flag = 1 if any(v == "REFERENCE_ENTITY" for v in entity_attributes_datatype.values()) else 0
is_entity_dnb_enabled_temp=entity_json.get("dnb_integration") or {}
if(is_entity_dnb_enabled_temp!={}):
   is_entity_dnb_enabled=str(is_entity_dnb_enabled_temp.get("is_active", False))
else:
    is_entity_dnb_enabled="False"
entity_dnb_settings = entity_json.get("dnb_integration",{})

# Check if single source (only one dataset table)
is_single_source = dataset_tables_len == 1

dbutils.jobs.taskValues.set(TaskValueKey.ENTITY.value, entity)
dbutils.jobs.taskValues.set(TaskValueKey.MASTER_TABLE.value, master_table)
dbutils.jobs.taskValues.set(TaskValueKey.PRIMARY_TABLE.value, primary_table)
dbutils.jobs.taskValues.set(TaskValueKey.ID_KEY.value, id_key)
dbutils.jobs.taskValues.set(TaskValueKey.PRIMARY_KEY.value, primary_key)
dbutils.jobs.taskValues.set(TaskValueKey.DATASET_TABLES.value, json.dumps(dataset_tables))
dbutils.jobs.taskValues.set(TaskValueKey.DATASET_OBJECTS.value, json.dumps(dataset_objects))
dbutils.jobs.taskValues.set(TaskValueKey.ENTITY_ATTRIBUTES.value, json.dumps(entity_attributes))
dbutils.jobs.taskValues.set(TaskValueKey.ATTRIBUTES_MAPPING.value, json.dumps(attributes_mapping))
dbutils.jobs.taskValues.set(TaskValueKey.ENTITY_ATTRIBUTES_DATATYPE.value, json.dumps(entity_attributes_datatype))
dbutils.jobs.taskValues.set(TaskValueKey.DEFAULT_SURVIVORSHIP_RULES.value, json.dumps(default_survivorship_rules))
dbutils.jobs.taskValues.set(TaskValueKey.VALIDATION_FUNCTIONS.value, json.dumps(validation_functions))
dbutils.jobs.taskValues.set(TaskValueKey.IS_GOLDEN_DEDUPLICATION.value, dataset_tables_len)
dbutils.jobs.taskValues.set(TaskValueKey.IS_SINGLE_SOURCE.value, is_single_source)
dbutils.jobs.taskValues.set(TaskValueKey.IS_ENTITY_DNB_ENABLED.value, is_entity_dnb_enabled)
dbutils.jobs.taskValues.set(TaskValueKey.ENTITY_DNB_SETTINGS.value, json.dumps(entity_dnb_settings))
dbutils.jobs.taskValues.set(TaskValueKey.CATALOG_NAME.value, catalog_name)
dbutils.jobs.taskValues.set(TaskValueKey.META_INFO_TABLE.value, meta_info_table)

# COMMAND ----------

entity_type=entity_json.get("entity_type", "reference")

# COMMAND ----------

if(entity_type=="reference"):
  entity_reference_config_json_path_str = ''
  with open(entity_reference_config_json_path, 'r') as f:
    entity_reference_config_json_path_str = f.read()
  entity_reference_config_json = json.loads(entity_reference_config_json_path_str)
  entity_reference_config = entity_reference_config_json.get('merge_config', '')
  dbutils.jobs.taskValues.set(TaskValueKey.REFERENCE_CONFIG.value, entity_reference_config)
  dbutils.notebook.exit("Reference Entity Found")
else:
  pass


# COMMAND ----------

model_json_str = ''
with open(model_json_path, 'r') as f:
  model_json_str = f.read()

# COMMAND ----------

model_json = json.loads(model_json_str)

# COMMAND ----------

llm_model_source = model_json.get('llm_model_source', 'databricks_foundation')
llm_model_source_str = llm_model_source
embedding_model_source = model_json.get('embedding_model_source', 'databricks_foundation')
embedding_model_source_str = embedding_model_source
attribute_objects = model_json.get('attributes', '')
attributes = [attribute.get('name') for attribute in attribute_objects]
config_thresholds = model_json.get('config_thresold')
vs_endpoint = model_json.get('vs_endpoint')
additional_instructions = model_json.get('additional_instructions', '').replace("'", "''")
max_potential_matches = model_json.get('max_potential_matches')
base_prompt = model_json.get('base_prompt', {})
if(not base_prompt):
    base_prompt=None
else:
    base_prompt=base_prompt.get('content','')

# COMMAND ----------

llm_model = ''
llm_model_endpoint = ''
llm_temperature = 0.0  # Default temperature if not set
if llm_model_source == 'databricks_foundation':
  foundation_config = model_json.get("llm_model", {}).get("databricks_foundation", {})
  llm_model = foundation_config.get("foundation_model_name")
  llm_provisionless = foundation_config.get("provisionless", False)  # Default True if not provided
  llm_temperature = foundation_config.get("temperature", 0.0)  # Default 0.0 if not set
  if not llm_model:
      raise ValueError("Missing 'foundation_model_name' in databricks_foundation config")
  # If llm provisionless → use directly (no endpoint creation)
  if llm_provisionless:
      llm_model_endpoint = llm_model
      logger.info(f"Using provisionless Databricks foundation model: {llm_model_endpoint}")
elif llm_model_source == 'databricks_custom':
  custom_model = model_json.get('llm_model', {})
  llm_model_source_str = f"databricks_custom_{custom_model.get('databricks_custom', {}).get('modeltype', '')}"
  llm_model = model_json.get('llm_model', {}).get('databricks_custom', {}).get('modelname')
  llm_temperature = custom_model.get('databricks_custom', {}).get('temperature', 0.0)  # Default 0.0 if not set

# COMMAND ----------

embedding_model = ''
embedding_model_endpoint = ''
embedding_provisionless = False
if embedding_model_source == 'databricks_foundation':
  foundation_config = model_json.get('embedding_model', {}).get('databricks_foundation', {})
  embedding_model = foundation_config.get('foundation_model_name')
  embedding_provisionless = foundation_config.get('provisionless', False)
  # If embedding provisionless → use directly (no endpoint creation)
  if embedding_provisionless:
    embedding_model_endpoint = embedding_model
    logger.info(f"Using provisionless Databricks foundation embedding model: {embedding_model_endpoint}")
elif embedding_model_source == 'databricks_custom':
  custom_model_embedding = model_json.get('embedding_model', {})
  embedding_model_source_str = f"databricks_custom_{custom_model_embedding.get('databricks_custom', {}).get('modeltype', '')}"
  embedding_model = model_json.get('embedding_model', {}).get('databricks_custom', {}).get('modelname')


# COMMAND ----------

deterministic_rules_config = model_json.get('deterministic_rules', None)        
if deterministic_rules_config is not None and deterministic_rules_config.get('enabled', False):
    deterministic_rules = deterministic_rules_config.get('rules', [])
else:
    deterministic_rules = []

# Load PT models config from Volume
pt_models_config = {}
try:
    pt_config_path = f"/Volumes/{catalog_name}/metadata/metadata_files/pt_models_config.json"
    with open(pt_config_path, 'r') as f:
        pt_data = json.load(f)

    # Build lookup: entity_name -> pt_config
    pt_models_config = {
        model["entity_name"]: {
            "pt_type": model["pt_type"],
            "entity_version": model.get("entity_version", "1"),
            "chunk_size": model["chunk_size"],
            "provisioned_model_units": model.get("provisioned_model_units"),
            "min_provisioned_throughput": model.get("min_provisioned_throughput"),
            "max_provisioned_throughput": model.get("max_provisioned_throughput"),
        }
        for model in pt_data.get("pt_models", [])
        if model.get("is_active")
    }
    logger.info(f"Loaded {len(pt_models_config)} active PT model configs")
except FileNotFoundError:
    logger.warning(f"No PT config file found, all models will use pay-per-token mode")
except Exception as e:
    logger.warning(f"Failed to load PT config: {e}. All models will use pay-per-token mode.")

# COMMAND ----------


dbutils.jobs.taskValues.set(TaskValueKey.LLM_MODEL_SOURCE.value, llm_model_source_str)
dbutils.jobs.taskValues.set(TaskValueKey.LLM_MODEL.value, llm_model)
dbutils.jobs.taskValues.set(TaskValueKey.LLM_MODEL_ENDPOINT.value, llm_model_endpoint)
dbutils.jobs.taskValues.set(TaskValueKey.EMBEDDING_MODEL_SOURCE.value, embedding_model_source_str)
dbutils.jobs.taskValues.set(TaskValueKey.EMBEDDING_MODEL.value, embedding_model)
dbutils.jobs.taskValues.set(TaskValueKey.EMBEDDING_MODEL_ENDPOINT.value, embedding_model_endpoint)
dbutils.jobs.taskValues.set(TaskValueKey.MATCH_ATTRIBUTES.value, json.dumps(attributes))
dbutils.jobs.taskValues.set(TaskValueKey.CONFIG_THRESHOLDS.value, json.dumps(config_thresholds))
dbutils.jobs.taskValues.set(TaskValueKey.PROCESS_RECORDS.value, process_records)
dbutils.jobs.taskValues.set(TaskValueKey.VS_ENDPOINT.value, vs_endpoint)
dbutils.jobs.taskValues.set(TaskValueKey.ADDITIONAL_INSTRUCTIONS.value, additional_instructions)
dbutils.jobs.taskValues.set(TaskValueKey.MAX_POTENTIAL_MATCHES.value, max_potential_matches)
dbutils.jobs.taskValues.set(TaskValueKey.LLM_PROVISIONLESS.value, llm_provisionless)
dbutils.jobs.taskValues.set(TaskValueKey.EMBEDDING_PROVISIONLESS.value, embedding_provisionless)
dbutils.jobs.taskValues.set(TaskValueKey.LLM_TEMPERATURE.value, llm_temperature)
dbutils.jobs.taskValues.set(TaskValueKey.BASE_PROMPT.value,base_prompt)
dbutils.jobs.taskValues.set(TaskValueKey.PT_MODELS_CONFIG.value, json.dumps(pt_models_config))
dbutils.jobs.taskValues.set(TaskValueKey.DETERMINISTIC_RULES.value, json.dumps(deterministic_rules))

# COMMAND ----------

logger_instance.shutdown()
