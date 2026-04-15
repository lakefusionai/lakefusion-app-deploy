from fastapi import APIRouter, Depends, HTTPException
from lakefusion_utility.utils.logging_utils import get_logger
from app.lakefusion_databricks_service.services.catalog_service import list_catalogs, list_schemas, list_tables, list_uc_objects
from sqlalchemy.orm import Session
from app.lakefusion_databricks_service.utils.app_db import get_db,token_required_wrapper
from typing import List, Union
from databricks.sdk.service.catalog import CatalogInfo, SchemaInfo, TableInfo, VolumeInfo, FunctionInfo, RegisteredModelInfo
from enum import Enum

# Initialize logger for the Catalog Management API
app_logger = get_logger(__name__)

# Enum for valid Unity Catalog object types
class ObjectType(str, Enum):
    """Valid Unity Catalog object types."""
    catalogs = "catalogs"
    schemas = "schemas"
    tables = "tables"
    volumes = "volumes"
    functions = "functions"
    models = "models"

# Initialize API Router for catalog-related operations with the prefix '/catalog'
catalog_router = APIRouter(tags=["Catalog Management API"], prefix='/catalog')

# Route to list all catalogs in Databricks
# This route requires a valid token for authentication and connects to a database session.
# Args:
#     check (dict): The token check dependency to ensure the request is authenticated.
#     db (Session): The database session, if needed (though unused in this route).
# Returns:
#     List of CatalogInfo objects representing the catalogs in Databricks.
@catalog_router.get("/list", response_model=List[CatalogInfo])
async def list_catalogs_route(check: dict = Depends(token_required_wrapper), db: Session = Depends(get_db)):
    try:
        # Extract the token from the authentication check
        token = check.get('token')
        # Fetch the list of catalogs using the CatalogService
        return list_catalogs(token=token,db=db)
    except Exception as e:
        # Log the exception and return an HTTP 500 error with a custom message
        app_logger.exception("Failed to fetch catalogs")
        raise HTTPException(status_code=500, detail="Failed to get catalogs list")

# Route to get schemas from a specified catalog in Databricks
# Args:
#     catalog_name (str): The name of the catalog from which schemas are fetched.
#     check (dict): Token check to validate authentication.
#     db (Session): The database session, if needed (though unused in this route).
# Returns:
#     List of SchemaInfo objects representing the schemas in the specified catalog.
@catalog_router.get("/get-schemas", response_model=List[SchemaInfo])
async def get_schemas_route(catalog_name: str, check: dict = Depends(token_required_wrapper), db: Session = Depends(get_db)):
    try:
        # Extract the token from the authentication check
        token = check.get('token')
        # Fetch the schemas for the specified catalog using the CatalogService
        return list_schemas(token=token, catalog_name=catalog_name,db=db)
    except Exception as e:
        # Log the exception and return an HTTP 500 error with a custom message
        app_logger.exception(f"Failed to fetch schemas for catalog {catalog_name}")
        raise HTTPException(status_code=500, detail="Failed to get schemas list")

# Route to get tables from a specified catalog and schema in Databricks
# Args:
#     catalog_name (str): The name of the catalog from which tables are fetched.
#     schema_name (str): The name of the schema from which tables are fetched.
#     check (dict): Token check to validate authentication.
#     db (Session): The database session, if needed (though unused in this route).
# Returns:
#     List of TableInfo objects representing the tables in the specified catalog and schema.
@catalog_router.get("/get-tables", response_model=List[TableInfo])
async def get_tables_route(
    catalog_name: str,
    schema_name: str,
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db)
):
    try:
        # Extract the token from the authentication check
        token = check.get('token')
        # Call the list_tables function to fetch tables from the specified catalog and schema
        return list_tables(token=token, catalog_name=catalog_name, schema_name=schema_name,db=db)
    except Exception as e:
        # Log the exception and return an HTTP 500 error with a custom message
        app_logger.exception(f"Failed to fetch tables for catalog {catalog_name} and schema {schema_name}")
        raise HTTPException(status_code=500, detail="Failed to get tables list")

# Unified route to list any Unity Catalog object type
# This route requires a valid token for authentication and accepts type parameter to specify UC object type
# Args:
#     type (ObjectType): The type of UC object to list (catalogs, schemas, tables, volumes, functions, models)
#     catalog_name (str, optional): Catalog name (required for schemas, tables, volumes, functions; optional for models)
#     schema_name (str, optional): Schema name (required for tables, volumes, functions; optional for models)
#     check (dict): Token check to validate authentication
#     db (Session): The database session
# Returns:
#     List of UC objects of the specified type
@catalog_router.get(
    "/list-objects",
    response_model=Union[
        List[CatalogInfo],
        List[SchemaInfo],
        List[TableInfo],
        List[VolumeInfo],
        List[FunctionInfo],
        List[RegisteredModelInfo]
    ],
    summary="List Unity Catalog Objects",
    description="""
    Unified endpoint to list any Unity Catalog object type.

    **Parameter Requirements by Type:**
    - `catalogs`: No additional parameters needed
    - `schemas`: Requires `catalog_name`
    - `tables`: Requires `catalog_name` and `schema_name`
    - `volumes`: Requires `catalog_name` and `schema_name`
    - `functions`: Requires `catalog_name` and `schema_name`
    - `models`: Optional `catalog_name` and `schema_name` (list all if not provided)

    **Examples:**
    - List catalogs: `?type=catalogs`
    - List schemas: `?type=schemas&catalog_name=main`
    - List tables: `?type=tables&catalog_name=main&schema_name=default`
    - List volumes: `?type=volumes&catalog_name=main&schema_name=default`
    - List functions: `?type=functions&catalog_name=main&schema_name=default`
    - List models: `?type=models` or `?type=models&catalog_name=main&schema_name=default`
    """
)
async def list_uc_objects_route(
    type: ObjectType,
    catalog_name: str = None,
    schema_name: str = None,
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db)
):
    try:
        # Extract the token from the authentication check
        token = check.get('token')
        # Call the unified list_uc_objects function
        return list_uc_objects(
            token=token,
            object_type=type.value,
            catalog_name=catalog_name,
            schema_name=schema_name,
            db=db
        )
    except Exception as e:
        # HttpResponse exceptions from service layer will be raised directly
        if hasattr(e, 'status_code'):
            raise e
        # Other exceptions are internal server errors
        app_logger.exception(f"Failed to fetch {type.value}")
        raise HTTPException(status_code=500, detail=f"Failed to list {type.value}")
