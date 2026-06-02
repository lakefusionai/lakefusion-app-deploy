# Databricks notebook source
# MAGIC %run ../utils/taskvalues_enum

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

# MAGIC %md
# MAGIC %run execute_utils MUST run AFTER catalog_name is read above — its
# MAGIC init_logger needs catalog_name to resolve the pipeline_logs volume path.

# COMMAND ----------

# MAGIC %run ../utils/execute_utils

# COMMAND ----------

logger.info("Parsing Entity and Model JSON")

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
# Full attribute records carry name + type + is_array + struct_definition
# so downstream notebooks (Create_Tables, Incremental_Load_To_Unified, …)
# can resolve STRUCT / ARRAY columns to nested Spark types.
entity_attribute_records = [
    {
        "name": item.get("name"),
        "type": item.get("type"),
        "is_array": bool(item.get("is_array", False)),
        "struct_definition": item.get("struct_definition"),
    }
    for item in entity_json.get("attributes", [])
]
attributes_mapping = parse_attributes_mapping_json(entity_json["dataset_mappings"])
# Full mapping records preserving mode + sub_field_map for complex-type
# loaders. Legacy callers continue to read ATTRIBUTES_MAPPING.
attributes_mapping_full = parse_attributes_mapping_full(entity_json["dataset_mappings"])
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


# ── Build one config dict per REFERENCE_ENTITY attribute PER source table ─────
# Iterates ALL dataset_mappings (primary + secondary), not just the first one.
# Each source table may map the same entity attribute to a different source column.

# Mapping-table suffix — same convention as other entity tables
# (unified, master, error tables): bare for prod, "_<experiment_id>" for
# experiments. Each experiment gets its own isolated mapping table — they're
# NOT shared with prod or other experiments.
mapping_experiment_suffix = ""
if experiment_id and experiment_id != "prod":
    mapping_experiment_suffix = f"_{experiment_id}"

rdm_configs = []
if(rdm_flag==1):

    reference_dataset_lookup = {
        rd["reference_entity_id"]: rd
        for rd in entity_json.get("reference_dataset", [])
    }

    # Collect REFERENCE_ENTITY attribute metadata (shared across all sources)
    ref_entity_attrs = []
    for attr in entity_json.get("attributes", []):
        if attr.get("type") != "REFERENCE_ENTITY":
            continue
        ref_entity_attrs.append(attr)

    # Iterate ALL dataset_mappings (primary + all secondary sources)
    for dataset_mapping in entity_json.get("dataset_mappings", []):
        source_table = dataset_mapping["dataset"]["path"]
        source_id    = dataset_mapping.get("dataset_id") or dataset_mapping.get("dataset", {}).get("id")

        # Build attribute lookup for THIS source table
        # Includes match_strategy and ref_attr per attribute per source
        dataset_attr_lookup = {
            m["entity_attribute"]: m
            for m in dataset_mapping.get("attributes", [])
        }

        # Build one config per REFERENCE_ENTITY attribute for this source
        for attr in ref_entity_attrs:
            attribute_name = attr["name"]
            type_config    = attr.get("type_config") or {}
            ref_entity_id  = type_config.get("reference_entity_id")
            ref_dataset    = reference_dataset_lookup.get(ref_entity_id, {})

            # Get THIS source's mapping for this attribute
            ds_attr_entry = dataset_attr_lookup.get(attribute_name, {})
            source_attr   = ds_attr_entry.get("dataset_attribute")
            ref_attr      = ds_attr_entry.get("ref_attribute") or attribute_name

            # Skip if this source doesn't map this attribute
            if not source_attr:
                continue

            # match_strategy comes from the dataset mapping, NOT from entity type_config
            # e.g. {"match_strategy_type": "fuzzy", "fuzzy_config": {...}}
            ds_match_strategy = ds_attr_entry.get("match_strategy") or {}
            match_strategy    = ds_match_strategy.get("match_strategy_type", "exact")

            fuzzy_config     = ds_match_strategy.get("fuzzy_config") or {}
            fuzzy_algorithm  = fuzzy_config.get("algorithm",        "")
            high_threshold   = fuzzy_config.get("high_threshold",   0.92)
            low_threshold    = fuzzy_config.get("low_threshold",    0.75)
            case_insensitive = fuzzy_config.get("case_insensitive", True)

            ref_entity_name = ref_dataset.get("reference_entity_name", "")
            mapping_table   = f"{catalog_name}.gold.{ref_entity_name}_reference_mappings{mapping_experiment_suffix}"
            ref_output_attr = type_config.get("reference_entity_output_attr") or attribute_name

            rdm_configs.append({
                "attribute_name":   attribute_name,   # entity column  e.g. "SPECIALTY_CODE"
                "source_table":     source_table,      # THIS source dataset path
                "source_id":        source_id,         # THIS source's dataset_id
                "source_attr":      source_attr,       # raw source col for THIS source
                "ref_table":        ref_dataset.get("reference_entity_table_path"),
                "ref_attr":         ref_attr,          # ref table key col (varies per source)
                "ref_output_attr":  ref_output_attr,   # display column in ref table
                "match_strategy":   match_strategy,    # "exact" | "fuzzy" (per source)
                "fuzzy_algorithm":  fuzzy_algorithm,   # per source
                "high_threshold":   high_threshold,
                "low_threshold":    low_threshold,
                "case_insensitive": case_insensitive,
                "unified_table":    f"{catalog_name}.silver.{entity}_unified_prod",
                "master_table":    f"{catalog_name}.gold.{entity}_master_prod",
                "mapping_table":    mapping_table,
            })
else:
    pass

# ── Build reference_attribute_config for load steps ─────────────────────────
# Maps: {attr_name: {ref_table, output_attr, mapping_table}} for REFERENCE_ENTITY attrs
# Used by load steps to:
#   1. Lookup mapping_table: source_value → ref_lakefusion_id (stored in column)
#   2. LEFT JOIN ref_table on ref_lakefusion_id → output_attr (used in attributes_combined)
reference_attribute_config = {}
if rdm_flag == 1:
    for attr in entity_json.get("attributes", []):
        if attr.get("type") != "REFERENCE_ENTITY":
            continue
        attribute_name = attr["name"]
        type_config = attr.get("type_config") or {}
        ref_entity_id = type_config.get("reference_entity_id")
        output_attr = type_config.get("reference_entity_output_attr") or attribute_name
        ref_dataset = reference_dataset_lookup.get(ref_entity_id, {})
        ref_table = ref_dataset.get("reference_entity_table_path")
        ref_entity_name = ref_dataset.get("reference_entity_name", "")
        mapping_table = (
            f"{catalog_name}.gold.{ref_entity_name}_reference_mappings{mapping_experiment_suffix}"
            if ref_entity_name else None
        )
        if ref_table:
            reference_attribute_config[attribute_name] = {
                "ref_table": ref_table,
                "output_attr": output_attr,
                "mapping_table": mapping_table,
            }

dbutils.jobs.taskValues.set(TaskValueKey.ENTITY.value, entity)
dbutils.jobs.taskValues.set(TaskValueKey.MASTER_TABLE.value, master_table)
dbutils.jobs.taskValues.set(TaskValueKey.PRIMARY_TABLE.value, primary_table)
dbutils.jobs.taskValues.set(TaskValueKey.ID_KEY.value, id_key)
dbutils.jobs.taskValues.set(TaskValueKey.PRIMARY_KEY.value, primary_key)
dbutils.jobs.taskValues.set(TaskValueKey.DATASET_TABLES.value, json.dumps(dataset_tables))
dbutils.jobs.taskValues.set(TaskValueKey.DATASET_OBJECTS.value, json.dumps(dataset_objects))
dbutils.jobs.taskValues.set(TaskValueKey.ENTITY_ATTRIBUTES.value, json.dumps(entity_attributes))
dbutils.jobs.taskValues.set(TaskValueKey.ATTRIBUTES_MAPPING.value, json.dumps(attributes_mapping))
dbutils.jobs.taskValues.set(TaskValueKey.ATTRIBUTES_MAPPING_FULL.value, json.dumps(attributes_mapping_full))
dbutils.jobs.taskValues.set(TaskValueKey.ENTITY_ATTRIBUTES_DATATYPE.value, json.dumps(entity_attributes_datatype))
dbutils.jobs.taskValues.set(TaskValueKey.ENTITY_ATTRIBUTE_RECORDS.value, json.dumps(entity_attribute_records))
dbutils.jobs.taskValues.set(TaskValueKey.DEFAULT_SURVIVORSHIP_RULES.value, json.dumps(default_survivorship_rules))
dbutils.jobs.taskValues.set(TaskValueKey.VALIDATION_FUNCTIONS.value, json.dumps(validation_functions))
dbutils.jobs.taskValues.set(TaskValueKey.IS_GOLDEN_DEDUPLICATION.value, dataset_tables_len)
dbutils.jobs.taskValues.set(TaskValueKey.IS_SINGLE_SOURCE.value, is_single_source)
dbutils.jobs.taskValues.set(TaskValueKey.IS_ENTITY_DNB_ENABLED.value, is_entity_dnb_enabled)
dbutils.jobs.taskValues.set(TaskValueKey.ENTITY_DNB_SETTINGS.value, json.dumps(entity_dnb_settings))
dbutils.jobs.taskValues.set(TaskValueKey.CATALOG_NAME.value, catalog_name)
dbutils.jobs.taskValues.set(TaskValueKey.META_INFO_TABLE.value, meta_info_table)
dbutils.jobs.taskValues.set(TaskValueKey.RDM_CONFIGS.value, rdm_configs)
dbutils.jobs.taskValues.set(TaskValueKey.RDM_FLAG.value, rdm_flag)
dbutils.jobs.taskValues.set(TaskValueKey.REFERENCE_ATTRIBUTE_CONFIG.value, json.dumps(reference_attribute_config))


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
# Sub-field selection per model-config attribute (only meaningful for STRUCT
# / ARRAY<STRUCT> targets). Empty / missing = use all sub-fields.
model_selected_sub_fields = {
    a.get('name'): a.get('selected_sub_fields') or []
    for a in attribute_objects
    if a.get('name') and a.get('selected_sub_fields')
}
config_thresholds = model_json.get('config_thresold')
vs_endpoint = model_json.get('vs_endpoint')
additional_instructions = model_json.get('additional_instructions', '').replace("'", "''")
max_potential_matches = model_json.get('max_potential_matches')
embedding_mode = model_json.get('embedding_mode', 'managed')
base_prompt = model_json.get('base_prompt', {})
if(not base_prompt):
    base_prompt=None
else:
    base_prompt=base_prompt.get('content','')

# COMMAND ----------

# Playground default LLM params — mirrors DEFAULT_PLAYGROUND_CONFIG in the
# portal (lakefusion-main-portal/src/components/matchmaven/playground/constants.ts).
# null = "off" (skip the field in the downstream LLM API call). Defaults
# match the portal: temperature on at 0.0; max_tokens and reasoning_effort
# off because many endpoints reject explicit max_tokens and only OpenAI-
# style models support reasoning_effort. Keep these two definitions in sync.
LLM_PARAM_DEFAULTS = {
    "temperature": 0.0,
    "max_tokens": None,
    "reasoning_effort": None,
}

def _resolve_llm_param(source_obj, key):
    """
    Read a single LLM param from the active source sub-object. Accepts both
    the new 3-field null=off shape and the legacy 6-field shape with
    *_enabled flags: an explicit `<key>_enabled: false` overrides whatever
    value is present and marks the field as off. Missing values fall back
    to LLM_PARAM_DEFAULTS.
    """
    if source_obj is None:
        return LLM_PARAM_DEFAULTS[key]
    if source_obj.get(key + "_enabled") is False:
        return None
    if key not in source_obj:
        return LLM_PARAM_DEFAULTS[key]
    return source_obj[key]

llm_model = ''
llm_model_endpoint = ''
llm_temperature = LLM_PARAM_DEFAULTS["temperature"]
llm_provisionless = False
if llm_model_source == 'databricks_foundation':
  foundation_config = model_json.get("llm_model", {}).get("databricks_foundation", {})
  llm_model = foundation_config.get("foundation_model_name")
  llm_provisionless = foundation_config.get("provisionless", False)
  llm_temperature = _resolve_llm_param(foundation_config, "temperature")
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
  llm_temperature = _resolve_llm_param(custom_model.get('databricks_custom', {}), "temperature")

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

# Extract advanced LLM config from the active model type object (same place as temperature).
# Resolution via _resolve_llm_param: returns None when the field is "off"
# (either explicit null in the new shape or legacy *_enabled: false), or the
# stored value/default otherwise.
if llm_model_source == 'databricks_foundation':
    _active_config = model_json.get("llm_model", {}).get("databricks_foundation", {})
elif llm_model_source == 'databricks_custom':
    _active_config = model_json.get("llm_model", {}).get("databricks_custom", {})
else:
    _active_config = model_json.get("llm_model", {}).get("external_model_api", {})

try:
    widget_reasoning = dbutils.widgets.get("reasoning_effort")
except Exception:
    widget_reasoning = None
try:
    widget_max_tokens = dbutils.widgets.get("llm_max_tokens")
except Exception:
    widget_max_tokens = None

# llm_temperature was already resolved above (foundation/custom branches).
# Resolve max_tokens and reasoning_effort the same way. Widget overrides
# beat the model config when present.
resolved_max_tokens = _resolve_llm_param(_active_config, "max_tokens")
resolved_reasoning = _resolve_llm_param(_active_config, "reasoning_effort")

llm_max_tokens = int(widget_max_tokens) if widget_max_tokens else (int(resolved_max_tokens) if resolved_max_tokens is not None else None)
reasoning_effort_value = widget_reasoning if widget_reasoning else resolved_reasoning

# Downstream task-value contract preserved: reasoning_effort is the string
# 'disabled' when off (the integration-core LLM-layer notebooks check
# `reasoning_effort != 'disabled'`). llm_temperature and llm_max_tokens stay
# as None when off.
reasoning_effort = reasoning_effort_value if reasoning_effort_value is not None else 'disabled'

logger.info(f"Advanced LLM Config: temperature={llm_temperature}, max_tokens={llm_max_tokens}, reasoning_effort={reasoning_effort}")

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
dbutils.jobs.taskValues.set(
    TaskValueKey.MODEL_SELECTED_SUB_FIELDS.value, json.dumps(model_selected_sub_fields)
)
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
dbutils.jobs.taskValues.set(TaskValueKey.EMBEDDING_MODE.value, embedding_mode)

# Advanced LLM config task values
dbutils.jobs.taskValues.set("llm_max_tokens", llm_max_tokens)
dbutils.jobs.taskValues.set("reasoning_effort", reasoning_effort)

# Log all task values being set
logger.info("=" * 60)
logger.info("TASK VALUES SET FOR DOWNSTREAM NOTEBOOKS")
logger.info("=" * 60)
logger.info(f"  entity:                {entity}")
logger.info(f"  catalog_name:          {catalog_name}")
logger.info(f"  llm_model_source:      {llm_model_source_str}")
logger.info(f"  llm_model:             {llm_model}")
logger.info(f"  llm_model_endpoint:    {llm_model_endpoint}")
logger.info(f"  llm_temperature:       {llm_temperature}")
logger.info(f"  llm_max_tokens:        {llm_max_tokens}")
logger.info(f"  reasoning_effort:      {reasoning_effort}")
logger.info(f"  embedding_model_source:{embedding_model_source_str}")
logger.info(f"  embedding_model:       {embedding_model}")
logger.info(f"  embedding_endpoint:    {embedding_model_endpoint}")
logger.info(f"  match_attributes:      {attributes}")
logger.info(f"  config_thresholds:     {config_thresholds}")
logger.info(f"  max_potential_matches: {max_potential_matches}")
logger.info(f"  vs_endpoint:           {vs_endpoint}")
logger.info(f"  llm_provisionless:     {llm_provisionless}")
logger.info(f"  embedding_provisionless:{embedding_provisionless}")
logger.info(f"  base_prompt:           {base_prompt[:100] if base_prompt else '(none)'}...")
logger.info(f"  additional_instructions:{additional_instructions[:100] if additional_instructions else '(none)'}...")
logger.info(f"  embedding_mode:        {embedding_mode}")
logger.info(f"  deterministic_rules:   {len(deterministic_rules)} rules")
logger.info("=" * 60)

# COMMAND ----------

logger_instance.shutdown()
