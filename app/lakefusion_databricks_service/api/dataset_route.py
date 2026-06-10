from fastapi import APIRouter, Depends, HTTPException, Query, Body
from lakefusion_utility.models.httpresponse import HttpResponse
from lakefusion_utility.models.dbconfig import DBConfigProperties
from typing import Optional, Any

from lakefusion_utility.utils.logging_utils import get_logger
from app.lakefusion_databricks_service.services.dataset_service import fetch_dataset, fetch_metadata_dataset,fetch_metadata_cleansed_dataset, fetch_metadata_dataset_by_path, update_metadata_dataset, update_metadata_dataset_description 
from app.lakefusion_databricks_service.services.dataset_service import fetch_columns_by_datatype, delete_record, create_dataset_record, update_dataset_records
from sqlalchemy.orm import Session
from app.lakefusion_databricks_service.utils.app_db import get_db,token_required_wrapper

# Initialize the logger for capturing log messages and handling errors
app_logger = get_logger(__name__)

# Initialize the FastAPI router for handling dataset-related API endpoints
dataset_router = APIRouter(tags=["Dataset API"], prefix='/dataset')

# Endpoint to execute and fetch a dataset by its ID.
# This function handles the request and response for executing a dataset.
# Args:
#     dataset_id (int): The unique ID of the dataset to execute.
#     warehouse_id (str): The ID of the SQL warehouse to execute the dataset.
#     check (dict): Authentication information passed by the Depends function.
#     db (Session): The database session to interact with the database.
# Returns:
#     The fetched dataset if successful, or raises an HTTPException if an error occurs.
@dataset_router.get("/{dataset_id}/sample-data")
async def execute_fetch_dataset(
    dataset_id: int, 
    warehouse_id: str, 
    page: int = 1,
    page_size: int = 1000,
    filters: Optional[str] = None,
    check: dict = Depends(token_required_wrapper), 
    db: Session = Depends(get_db)
):
    try:
        # Extract the token from the authentication check
        token = check.get('token')
        # Call the fetch_dataset function to execute the dataset
        return fetch_dataset(
            token=token, 
            db=db, 
            dataset_id=dataset_id, 
            warehouse_id=warehouse_id, 
            page=page,
            page_size=page_size,
            filters=filters,
            )
    except Exception as e:
        # Log the error and raise an HTTPException if execution fails
        app_logger.error(f"Failed to execute dataset {dataset_id}. Reason: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to execute dataset : " + str(e))
    

# Endpoint to execute and fetch the metadata of a dataset by its ID.
# This function handles the request and response for executing a metadata fetch of the dataset.
# Args:
#     dataset_id (int): The unique ID of the dataset to fetch metadata for.
#     warehouse_id (str): The ID of the SQL warehouse to execute the metadata query.
#     check (dict): Authentication information passed by the Depends function.
#     db (Session): The database session to interact with the database.
# Returns:
#     The fetched metadata if successful, or raises an HTTPException if an error occurs.
@dataset_router.get("/{dataset_id}/meta-data")
async def execute_fetch_metadata_dataset_for_dataset_id(
    dataset_id: int, 
    warehouse_id: str, 
    check: dict = Depends(token_required_wrapper), 
    db: Session = Depends(get_db)
):
    token = check.get('token')
    return fetch_metadata_dataset(token=token, db=db, dataset_id=dataset_id, warehouse_id=warehouse_id)


@dataset_router.patch("/{dataset_id}/meta-data/description")
async def execute_update_description_dataset_function(
    dataset_id: int, 
    warehouse_id: str, 
    description: str = Body(..., embed=True),
    check: dict = Depends(token_required_wrapper), 
    db: Session = Depends(get_db)
):
    token = check.get('token')
    return update_metadata_dataset_description(token=token, db=db, dataset_id=dataset_id, warehouse_id=warehouse_id, description=description)

    
@dataset_router.patch("/{dataset_id}/meta-data")
async def execute_update_metadata_dataset_function(
    dataset_id: int, 
    warehouse_id: str, 
    update_data: dict = Body(...),
    check: dict = Depends(token_required_wrapper), 
    db: Session = Depends(get_db)
):
    
    token = check.get('token')
    return update_metadata_dataset(token=token, db=db, dataset_id=dataset_id, warehouse_id=warehouse_id, metadata_updates=update_data,)
    
@dataset_router.get("/meta-data")
async def execute_fetch_metadata_dataset_function(
    dataset_path: str, 
    warehouse_id: str, 
    check: dict = Depends(token_required_wrapper), 
    db: Session = Depends(get_db)
):
    try:
        # Extract the token from the authentication check
        token = check.get('token')
        # Call the fetch_metadata_dataset function to execute the metadata fetch
        return fetch_metadata_dataset_by_path(token=token, db=db, dataset_path=dataset_path, warehouse_id=warehouse_id)
    except Exception as e:
        # Log the error and raise an HTTPException if fetching metadata fails
        app_logger.error(f"Failed to fetch metadata for dataset {dataset_path}. Reason: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to execute metadata : " + str(e))
    
@dataset_router.get("/{dataset_id}/meta-data-cleansed")
async def execute_fetch_metadata_dataset_function(
    dataset_id: int, 
    use_cleaned_table: Optional[bool],
    warehouse_id: str, 
    check: dict = Depends(token_required_wrapper), 
    db: Session = Depends(get_db)
):
    try:
        # Extract the token from the authentication check
        token = check.get('token')
        # Call the fetch_metadata_dataset function to execute the metadata fetch
        return fetch_metadata_cleansed_dataset(token=token, db=db, dataset_id=dataset_id, use_cleaned_table=use_cleaned_table, warehouse_id=warehouse_id)
    except Exception as e:
        # Log the error and raise an HTTPException if fetching metadata fails
        app_logger.error(f"Failed to fetch metadata for dataset {dataset_id}. Reason: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to execute metadata : " + str(e))

@dataset_router.get("/{dataset_id}/columns-by-datatypes")
async def execute_fetch_columns_by_datatype(
    dataset_id: int, 
    warehouse_id: str, 
    data_types: Optional[str],
    check: dict = Depends(token_required_wrapper), 
    db: Session = Depends(get_db)
):
    try:
        # Extract the token from the authentication check
        token = check.get('token')
        
        # Process the comma-separated dataTypes into a list
        data_types_list = []
        if data_types:
            data_types_list = [dt.strip() for dt in data_types.split(',')]
        
        # Call the fetch_columns_by_datatype function with the parsed list
        return fetch_columns_by_datatype(
            token=token, 
            db=db, 
            dataset_id=dataset_id, 
            warehouse_id=warehouse_id, 
            datatypes=data_types_list
        )
    except Exception as e:
        # Log the error and raise an HTTPException if fetching metadata fails
        app_logger.error(f"Failed to fetch metadata for dataset {dataset_id}. Reason: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to execute metadata : " + str(e))
    

# Endpoint to insert a new record into a dataset.
# Args:
#     dataset_id (int): The unique ID of the dataset to insert into.
#     warehouse_id (str): The ID of the SQL warehouse to execute the insert.
#     record_data (dict): The data to be inserted as key-value pairs.
#     check (dict): Authentication information passed by the Depends function.
#     db (Session): The database session to interact with the database.
    
# Returns:
#     success message and number of affected rows,
#     or raises HTTPException if an error occurs.
@dataset_router.post("/{dataset_id}/records")
async def execute_insert_record(
    dataset_id: int,
    warehouse_id: str,
    data_object: dict = Body(...),
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db)
):
    """
    Endpoint to insert a new record into a dataset.
    
    Args:
        dataset_id (int): The unique ID of the dataset to insert into.
        warehouse_id (str): The ID of the SQL warehouse to execute the insert.
        record_data (dict): The data to be inserted as key-value pairs.
        check (dict): Authentication information passed by the Depends function.
        db (Session): The database session to interact with the database.
        
    Returns:
        HttpResponse with success message and number of affected rows,
        or raises HTTPException if an error occurs.
    """

    # Extract the token from the authentication check
    token = check.get('token')
        
    # Call the insert_record service function
    result = create_dataset_record(
        token=token,
        db=db,
        dataset_id=dataset_id,
        warehouse_id=warehouse_id,
        data_object=data_object
    )
    return result

    
# Endpoint to update multiple records in a dataset.
# This function handles the request and response for updating records.
# Args:
#     dataset_id (int): The unique ID of the dataset to update records in.
#     warehouse_id (str): The ID of the SQL warehouse to execute the updates.
#     update_data (dict): The update data containing primary_field and updates array.
#     check (dict): Authentication information passed by the Depends function.
#     db (Session): The database session to interact with the database.
# Returns:
#     HttpResponse with success message and number of affected rows,
#     or raises HTTPException if an error occurs.
@dataset_router.patch("/{dataset_id}/records")
async def update_dataset_records_endpoint(
    dataset_id: int,
    warehouse_id: str,
    update_data: dict = Body(...),
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db)
):
    try:
        # Extract the token from the authentication check
        token = check.get('token')
        
        # Call the update_dataset_records service function
        result = update_dataset_records(
            token=token,
            db=db,
            dataset_id=dataset_id,
            warehouse_id=warehouse_id,
            primary_field=update_data.get('primary_field'),
            updates=update_data.get('updates')
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        # Log the error and raise an HTTPException if update fails
        app_logger.error(f"Failed to update records in dataset {dataset_id}. Reason: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to update records : " + str(e))
    


# Endpoint to delete a record from a dataset by its primary key.
# This function handles the request and response for deleting a record.
# Args:
#     dataset_id (int): The unique ID of the dataset containing the record.
#     delete_data (dict): Request body containing primary_key and its value.
#     warehouse_id (str): The ID of the SQL warehouse to execute the deletion.
#     check (dict): Authentication information passed by the Depends function.
#     db (Session): The database session to interact with the database.
# Returns:
#     Success message if deletion is successful, or raises an HTTPException if an error occurs.
@dataset_router.delete("/{dataset_id}/records")
async def delete_dataset_record(
    dataset_id: int,
    warehouse_id: str,
    primary_field: str = Body(..., embed=True),
    primary_field_value: Any = Body(..., embed=True),
    check: dict = Depends(token_required_wrapper),
    db: Session = Depends(get_db)
):
    try:
        # Extract the token from the authentication check
        token = check.get('token')
        
        if not primary_field or primary_field_value is None:
            raise HTTPException(status_code=400, detail="primary_key and primary_key_value are required in request body")
        
        # Call the delete_record function to delete the record
        result = delete_record(
            token=token,
            db=db,
            dataset_id=dataset_id,
            primary_field=primary_field,
            primary_field_value=primary_field_value,
            warehouse_id=warehouse_id
        )
        return {"message": f"Record with {primary_field} : {primary_field_value} deleted successfully", "affected_rows": result}
    except HTTPException:
        raise
    except Exception as e:
        # Log the error and raise an HTTPException if deletion fails
        app_logger.error(f"Failed to delete record from dataset {dataset_id}. Reason: {str(e)}")
        # raise HTTPException(status_code=500, detail="Failed to delete record")
        raise HTTPException(status_code=500, detail="Failed to delete record : " + str(e))