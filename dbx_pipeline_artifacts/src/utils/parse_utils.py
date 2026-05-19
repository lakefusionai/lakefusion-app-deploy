# Databricks notebook source
def get_primary_dataset_path(dataset_mappings):
    for dataset in dataset_mappings:
        if dataset["is_primary"]:
            return dataset["dataset"]["path"]

def get_dataset_tables(dataset_mappings):
    dataset_objects = {}
    dataset_paths = []
    
    for dataset in dataset_mappings:
        dataset_info = dataset.get("dataset", {})  # Extract dataset dictionary
        path = dataset_info.get("path", "")
        dataset_paths.append(path)
        # Store in dataset_objects
        dataset_objects[path] = dataset_info
    return dataset_paths, dataset_objects

# COMMAND ----------

# def parse_attributes_mapping(dataset_mappings):
#     attribute_mappings=[]
#     for dataset in dataset_mappings:
#         attr_map = {}
#         for attr in dataset["attributes"]:
#             attr_map[attr["dataset_attribute"]] = attr["entity_attribute"]
#             if(dataset.get("dataset", {}).get("quality_task_enabled",'')):
#                 dataset_path=dataset["dataset"]["path"]+"_cleaned"   
#             else:
#                 dataset_path=dataset["dataset"]["path"]
#         attribute_mappings.append({dataset_path:attr_map})
#     return attribute_mappings

# COMMAND ----------

# def parse_attributes_mapping(dataset_mappings):
#     attribute_mappings = []
#     for dataset in dataset_mappings:
#         attr_map = {}
#         for attr in dataset["attributes"]:
#             attr_map[f"`{attr['dataset_attribute']}`"] = attr["entity_attribute"]
#         # attribute_mappings.append(attr_map)
       
#         attribute_mappings.append({dataset['dataset']['path']:attr_map})

#     return attribute_mappings

def parse_attributes_mapping(dataset_mappings):
    attribute_mappings = []
    for dataset in dataset_mappings:
        attr_map = {}
        for attr in dataset["attributes"]:
            attr_map[attr["dataset_attribute"]] = attr["entity_attribute"]
        # attribute_mappings.append(attr_map)
        attribute_mappings.append({dataset["dataset"]["path"]:attr_map})

    return attribute_mappings

def parse_attributes_mapping_json(dataset_mappings):
    """
    Parse attribute mappings from dataset_mappings.
    
    Creates mapping: {entity_attribute: dataset_attribute}
    This handles cases where multiple entity attributes map from the same dataset column.
    
    Args:
        dataset_mappings: List of dataset mapping objects from entity JSON
        
    Returns:
        List of dicts: [{table_path: {entity_attr: dataset_attr, ...}}, ...]
    """
    attribute_mappings = []
    
    for dataset in dataset_mappings:
        attr_map = {}
        
        # Build mapping with entity_attribute as key
        for attr in dataset["attributes"]:
            entity_attr = attr["entity_attribute"]
            dataset_attr = attr["dataset_attribute"]
            attr_map[entity_attr] = dataset_attr
        
        attribute_mappings.append({dataset["dataset"]["path"]:attr_map})
    
    return attribute_mappings
    
# def get_primary_dataset_path(dataset_mappings):
#     for dataset in dataset_mappings:
#         if dataset["is_primary"]:
#             return dataset["dataset"]["path"]

# def get_dataset_tables(dataset_mappings):
#     dataset_paths = []
#     dataset_objects = {}
#     for dataset in dataset_mappings:
#         path = dataset.get('dataset', {}).get('path', '')
#         dataset_paths.append(path)
#         dataset_objects[path] = dataset.get('dataset', {})
#     return dataset_paths, dataset_objects

def get_default_survivorship_rules(survivorship_groups):
    default_survivorship_rules = []
    for survivorship_group in survivorship_groups:
        if survivorship_group.get('is_default', False):
            return survivorship_group.get('rules', [])
    return default_survivorship_rules

# COMMAND ----------

def get_primary_attr_mapping(attributes_mapping_json):
    print
    for attr_map in attributes_mapping_json:
        print(type(attr_map))
        for dataset_path, mapping in attr_map.items():
            if primary_table.startswith(dataset_path):
                print(mapping)
                return mapping
            
def remove_primary_from_attr_mapping(attributes_mapping_json):
    new_attr_mapping_json = []
    for attr_map in attributes_mapping_json:
        for dataset_path, mapping in attr_map.items():
            if not primary_table.startswith(dataset_path):
                new_attr_mapping_json.append(attr_map)
    return new_attr_mapping_json

# COMMAND ----------

from pyspark.sql.functions import udf, col
from pyspark.sql.types import BooleanType
import re
import inspect

def validation_function_exec(validation_function_list, df):

    for rule in validation_function_list:
        # isolated namespace for exec
        local_ns = {}

        # execute function code safely
        exec(rule["function_definition"], {"re": re}, local_ns)

        # extract the first function defined in the code
        functions = [
            v for v in local_ns.values()
            if callable(v)
        ]

        if not functions:
            raise ValueError(f"No function found in rule {rule['validation_id']}")

        validation_function = functions[0]

        # register UDF
        validate_udf = udf(validation_function, BooleanType())

        df = df.withColumn(
            f"is_valid_{rule['attribute_name']}",
            validate_udf(col(rule["attribute_name"]))
        )

    # drop original attributes if needed
    df = df.drop(*[i["attribute_name"] for i in validation_function_list])
    return df


# COMMAND ----------

import os
import sys
from pathlib import Path
from typing import Optional
import json

# COMMAND ----------

def get_version_info() -> Optional[dict]:
    """
    Loads product version info from version.json file.
    
    Searches for version.json by traversing up the directory tree until
    it finds the dbx_pipeline_artifacts folder.
    
    Returns:
        dict containing version info, or empty dict if file not found.
    """
    # Start from current working directory
    current_path = Path(os.getcwd())
    
    # Traverse up the directory tree to find dbx_pipeline_artifacts
    for parent in [current_path] + list(current_path.parents):
        # Check if we're in or under dbx_pipeline_artifacts
        if 'dbx_pipeline_artifacts' in parent.parts:
            # Find the dbx_pipeline_artifacts directory
            parts = parent.parts
            dbx_index = None
            for i, part in enumerate(parts):
                if part == 'dbx_pipeline_artifacts':
                    dbx_index = i
                    break
            
            if dbx_index is not None:
                # Construct path to dbx_pipeline_artifacts
                dbx_path = Path(*parts[:dbx_index + 1])
                version_file = dbx_path / "version.json"
                
                if version_file.exists():
                    try:
                        with open(version_file) as f:
                            return json.load(f)
                    except json.JSONDecodeError:
                        return dict()
    
    # If not found, return empty dict
    return dict()

# COMMAND ----------

def get_custom_tags() -> dict:
    """
    Loads custom tags from custom_tags.json as a dictionary.

    Fault tolerant:
      - If file doesn't exist → {}
      - If file is empty → {}
      - If JSON invalid → {}
      - If JSON is not a dict → {}

    Returns:
        dict of custom tags
    """
    TAGS_FILE = Path("../../custom_tags.json")

    if not TAGS_FILE.exists():
        return {}

    try:
        content = TAGS_FILE.read_text().strip()
        if not content:
            return {}

        data = json.loads(content)

        # Ensure it’s actually a dict
        return data if isinstance(data, dict) else {}

    except Exception:
        return {}

# COMMAND ----------

def get_resource_tags(as_list: bool = False):
    """
    Get the resource tags for the Databricks API requests.

    Args:
        as_list (bool, optional): If True, return tags as a list of {"key", "value"}.
                                  If False, return as a dictionary. Defaults to False.

    Returns:
        dict | list: Resource tags in the specified format.
    """

    version = get_version_info()
    custom_tags = get_custom_tags()  # <-- Using custom tags

    # Base tags
    tags_dict = {
        "product": version.get("productName", "LakeFusion"),
        "version": version.get("productVersion", "1.0.0"),
        "partner": version.get("partner", version.get("productName", "LakeFusion")),
        "environment": version.get("environment", "production"),
    }

    # Merge custom tags (custom overrides defaults if conflict)
    if isinstance(custom_tags, dict):
        tags_dict.update(custom_tags)

    if as_list:
        return [{"key": k, "value": v} for k, v in tags_dict.items()]

    return tags_dict

# COMMAND ----------

from databricks.sdk import WorkspaceClient

# Create a single WorkspaceClient (reuse across functions)
w = WorkspaceClient()

def append_schema_tags(catalog: str, schema: str, new_tags: dict):
    """
    Append tags to a Unity Catalog schema without overwriting existing ones.

    Args:
        catalog (str): The catalog name (e.g., "lakefusion_ai").
        schema (str): The schema (database) name (e.g., "demo_schema").
        new_tags (dict): Tags to append, e.g. {"env": "staging", "team": "mlops"}.
    """

    # Construct SQL to set all tags
    tags_sql = ", ".join([f"'{k}' = '{v}'" for k, v in new_tags.items()])
    sql_stmt = f"ALTER SCHEMA {catalog}.{schema} SET TAGS ({tags_sql})"

    # Execute the SQL to update tags
    spark.sql(sql_stmt)


def append_table_tags(catalog: str, schema: str, table: str, new_tags: dict):
    """
    Append tags to a Unity Catalog table without overwriting existing ones.

    Args:
        catalog (str): The catalog name (e.g., "lakefusion_ai").
        schema (str): The schema (database) name (e.g., "demo_schema").
        table (str): The table name (e.g., "building_demo").
        new_tags (dict): Tags to append, e.g. {"quality": "gold", "owner": "mlops"}.
    """

    # Construct SQL to set all tags
    tags_sql = ", ".join([f"'{k}' = '{v}'" for k, v in new_tags.items()])
    sql_stmt = f"ALTER TABLE {catalog}.{schema}.{table} SET TAGS ({tags_sql})"

    # Execute the SQL to update tags
    spark.sql(sql_stmt)


def apply_tags_to_uc_resource(resource_type: str, full_name: str, tags: dict):
    """
    resource_type can be: 'catalogs', 'schemas', 'tables', 'volumes', 'functions', 'models'
    full_name = 'catalog.schema.table' or 'catalog.schema.model' etc.
    """
    if not tags:
        return

    if resource_type == "catalogs":
        w.catalogs.update(name=full_name, properties=tags)
    elif resource_type == "schemas":
        append_schema_tags(full_name.split(".")[0], full_name.split(".")[1], tags)
        w.schemas.update(full_name, properties=tags)
    elif resource_type == "tables":
        append_table_tags(full_name.split(".")[0], full_name.split(".")[1], full_name.split(".")[2], tags)
    elif resource_type == "volumes":
        w.volumes.update(full_name, properties=tags)
    elif resource_type == "functions":
        w.functions.update(full_name, properties=tags)
    elif resource_type == "models":
        for key, value in tags.items():
            client.set_registered_model_tag(
                name=full_name,
                key=key,
                value=value
            )

def get_user_agent_header_sql():
    """
    Get the user agent header for the Databricks SQL API requests.

    This function retrieves the user agent string that includes product and version information.
    """

    # Set product name and version (Semantic Versioning required)
    version = get_version_info()
    product_name = version.get("productName", "LakeFusion")
    product_version = version.get("productVersion", "1.0.0")

    print(f"Setting user agent with product name: {product_name} & product version: {product_version}")

    return f"{product_name}/{product_version}"

# COMMAND ----------

# MAGIC %md
# MAGIC
