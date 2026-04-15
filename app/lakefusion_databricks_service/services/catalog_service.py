from fastapi.security import HTTPBearer
from lakefusion_utility.utils.logging_utils import get_logger
from lakefusion_utility.utils.databricks_util import CatalogService
from lakefusion_utility.models.httpresponse import HttpResponse
import traceback
from sqlalchemy.orm import Session

# Initialize an HTTPBearer security scheme for token-based authentication
token_auth_scheme = HTTPBearer()

# Initialize logger for the application
app_logger = get_logger(__name__)

# Function to list catalogs available in Databricks.
# This function requires an authentication token and interacts with the CatalogService.
# Args:
#     token (str): The authentication token used to validate and fetch catalogs.
# Returns:
#     List of catalogs retrieved from the CatalogService if successful.
# Raises:
#     HttpResponse: Returns a 500 internal server error response if an exception occurs.
def list_catalogs(token: str,db:Session):
    try:
        # Initialize the CatalogService with the provided token
        cs = CatalogService(token=token,db=db)
        # Fetch the list of catalogs from Databricks
        data = cs.list_catalogs()
        # Return the retrieved catalog list
        return data
    except Exception as e:
        # Capture the traceback in case of an exception and log the error message
        message = traceback.format_exc()
        app_logger.exception(f'Unable to fetch the catalogs list. Reason - {message}')
        # Raise an HttpResponse with a 500 status code, indicating internal server error
        raise HttpResponse(
            status_code=500,
            detail="An internal server error occurred while fetching the catalogs list",
            headers={"WWW-Authenticate": "Bearer"},
        )

# Function to fetch schemas from a given catalog in Databricks.
# This function also requires an authentication token and the catalog name.
# Args:
#     token (str): The authentication token used to validate and fetch schemas.
#     catalog_name (str): The name of the catalog from which schemas are to be fetched.
# Returns:
#     List of schemas retrieved from the specified catalog if successful.
# Raises:
#     HttpResponse: Returns a 500 internal server error response if an exception occurs.
def list_schemas(token: str, catalog_name: str,db:Session):
    try:
        # Initialize the CatalogService with the provided token
        cs = CatalogService(token=token,db=db)
        # Fetch the list of schemas for the specified catalog
        data = cs.get_schemas(catalog_name=catalog_name)
        # Return the retrieved schema list
        return data
    except Exception as e:
        # Capture the traceback in case of an exception and log the error message
        message = traceback.format_exc()
        app_logger.exception(f'Unable to fetch the schemas for catalog {catalog_name}. Reason - {message}')
        # Raise an HttpResponse with a 500 status code, indicating internal server error
        raise HttpResponse(
            status_code=500,
            detail=f"An internal server error occurred while fetching the schemas for catalog {catalog_name}",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
# Function to fetch tables from a given catalog and schema in Databricks.
# This function requires an authentication token, catalog name, and schema name.
# Args:
#     token (str): The authentication token used to validate and fetch tables.
#     catalog_name (str): The name of the catalog from which tables are to be fetched.
#     schema_name (str): The name of the schema from which tables are to be fetched.
# Returns:
#     List of tables retrieved from the specified catalog and schema if successful.
# Raises:
#     HttpResponse: Returns a 500 internal server error response if an exception occurs.
def list_tables(token: str, catalog_name: str, schema_name: str,db:Session):
    try:
        # Initialize the CatalogService with the provided token
        cs = CatalogService(token=token,db=db)
        # Fetch the list of tables for the specified catalog and schema
        data = cs.get_tables(catalog_name=catalog_name, schema_name=schema_name)
        # Return the retrieved table list
        return data
    except Exception as e:
        # Capture the traceback in case of an exception and log the error message
        message = traceback.format_exc()
        app_logger.exception(f'Unable to fetch the tables for catalog {catalog_name} and schema {schema_name}. Reason - {message}')
        # Raise an HttpResponse with a 500 status code, indicating internal server error
        raise HttpResponse(
            status_code=500,
            detail=f"An internal server error occurred while fetching the tables for catalog {catalog_name} and schema {schema_name}",
            headers={"WWW-Authenticate": "Bearer"},
        )

# Unified service function to list any Unity Catalog object type.
# Args:
#     token (str): The authentication token used to validate and fetch UC objects.
#     object_type (str): Type of UC object ('catalogs', 'schemas', 'tables', 'volumes', 'functions', 'models')
#     catalog_name (str, optional): Catalog name (required for specific types)
#     schema_name (str, optional): Schema name (required for specific types)
#     db (Session): The database session.
# Returns:
#     List of UC objects of the specified type if successful.
# Raises:
#     HttpResponse: 400 for validation errors, 500 for internal server errors.
def list_uc_objects(token: str, object_type: str, catalog_name: str = None, schema_name: str = None, db: Session = None):
    try:
        # Validate object_type
        valid_types = ['catalogs', 'schemas', 'tables', 'volumes', 'functions', 'models']
        if object_type not in valid_types:
            raise HttpResponse(
                status_code=400,
                detail=f"Invalid object_type: {object_type}. Must be one of: {', '.join(valid_types)}",
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Initialize CatalogService
        cs = CatalogService(token=token, db=db)

        # Call the unified method
        data = cs.list_uc_objects(object_type=object_type, catalog_name=catalog_name, schema_name=schema_name)
        return data

    except ValueError as ve:
        # Handle validation errors from CatalogService
        app_logger.warning(f'Validation error while fetching {object_type}: {str(ve)}')
        raise HttpResponse(
            status_code=400,
            detail=str(ve),
            headers={"WWW-Authenticate": "Bearer"},
        )
    except Exception as e:
        # Capture the traceback and log the error
        message = traceback.format_exc()
        scope_parts = [object_type]
        if catalog_name:
            scope_parts.append(f"catalog={catalog_name}")
        if schema_name:
            scope_parts.append(f"schema={schema_name}")
        scope = ", ".join(scope_parts)

        app_logger.exception(f'Unable to fetch UC objects: {scope}. Reason - {message}')
        raise HttpResponse(
            status_code=500,
            detail=f"An internal server error occurred while fetching {object_type}",
            headers={"WWW-Authenticate": "Bearer"},
        )

