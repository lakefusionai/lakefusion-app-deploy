from enum import Enum
import json

class TaskValueKey(str, Enum):
    """Enum for task value keys used across notebooks"""
    
    # Entity and table configuration
    ENTITY = "entity"
    MASTER_TABLE = "master_table"
    PRIMARY_TABLE = "primary_table"
    ID_KEY = "id_key"
    PRIMARY_KEY = "primary_key"
    CATALOG_NAME = "catalog_name"
    META_INFO_TABLE = "meta_info_table"
    IS_SINGLE_SOURCE = "is_single_source"
    IS_INITIAL_LOAD = "is_initial_load"
    
    # Dataset configuration
    DATASET_TABLES = "dataset_tables"
    DATASET_OBJECTS = "dataset_objects"
    
    # Entity attributes and mapping
    ENTITY_ATTRIBUTES = "entity_attributes"
    ATTRIBUTES_MAPPING = "attributes_mapping"
    ENTITY_ATTRIBUTES_DATATYPE = "entity_attributes_datatype"
    RDM_FLAG = "rdm_flag"
    
    # Rules and validation
    DEFAULT_SURVIVORSHIP_RULES = "default_survivorship_rules"
    VALIDATION_FUNCTIONS = "validation_functions"
    
    # Deduplication and DNB
    IS_GOLDEN_DEDUPLICATION = "is_golden_deduplication"
    IS_ENTITY_DNB_ENABLED = "is_entity_dnb_enabled"
    ENTITY_DNB_SETTINGS = "entity_dnb_settings"
    
    # LLM configuration
    LLM_MODEL_SOURCE = "llm_model_source"
    LLM_MODEL = "llm_model"
    LLM_MODEL_ENDPOINT = "llm_model_endpoint"
    LLM_PROVISIONLESS = "llm_provisionless"
    LLM_TEMPERATURE = "llm_temperature"
    
    # Embedding configuration
    EMBEDDING_MODEL_SOURCE = "embedding_model_source"
    EMBEDDING_MODEL = "embedding_model"
    EMBEDDING_MODEL_ENDPOINT = "embedding_model_endpoint"
    EMBEDDING_PROVISIONLESS = "embedding_provisionless"
    
    # Matching configuration
    MATCH_ATTRIBUTES = "match_attributes"
    CONFIG_THRESHOLDS = "config_thresholds"
    PROCESS_RECORDS = "process_records"
    VS_ENDPOINT = "vs_endpoint"
    ADDITIONAL_INSTRUCTIONS = "additional_instructions"
    MAX_POTENTIAL_MATCHES = "max_potential_matches"
    BASE_PROMPT = "base_prompt"

   

    # Provisioned Throughput configuration
    PT_MODELS_CONFIG = "pt_models_config"
    DETERMINISTIC_RULES="deterministic_rules"

    # Schema Evolution
    SCHEMA_EVOLUTION_JOB_ID = "schema_evolution_job_id"
    SCHEMA_EVOLUTION_EDITS = "schema_evolution_edits"
    SCHEMA_EVOLUTION_MASTER_TABLE = "schema_evolution_master_table"
    SCHEMA_EVOLUTION_UNIFIED_TABLE = "schema_evolution_unified_table"
    SCHEMA_EVOLUTION_AVS_TABLE = "schema_evolution_avs_table"
    SCHEMA_EVOLUTION_ENTITY_NAME = "schema_evolution_entity_name"
    SCHEMA_EVOLUTION_CATALOG_NAME = "schema_evolution_catalog_name"
    SCHEMA_EVOLUTION_ID_KEY = "schema_evolution_id_key"
    SCHEMA_EVOLUTION_EXPERIMENT_ID = "schema_evolution_experiment_id"

    #Reference entity
    REFERENCE_CONFIG="entity_reference_config"