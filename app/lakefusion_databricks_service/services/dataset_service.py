import numpy as np
from fastapi import HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from lakefusion_utility.utils.logging_utils import get_logger
from lakefusion_utility.utils.databricks_util import DataSetSQLService, ComputeService, CommonUtilities
from lakefusion_utility.models.httpresponse import HttpResponse
from lakefusion_utility.services.dataset_service import DatasetService
from lakefusion_utility.services.quality_tasks import QualityTaskService
from databricks import sql
import traceback
import json
import re

# Initialize security scheme for token-based authentication and logger for logging activities
token_auth_scheme = HTTPBearer()
app_logger = get_logger(__name__)
common_utils = CommonUtilities()

def build_filter_clauses(filters):
        try:
            if isinstance(filters, str):
                if '%' in filters:
                    try:
                        import urllib.parse
                        filters = urllib.parse.unquote(filters)
                        print(f"URL-decoded filters: {filters}")
                    except Exception as e:
                        app_logger.error(f"Error URL-decoding filters: {str(e)}")
                
                if filters.strip():
                    try:
                        filters = json.loads(filters)
                        print(f"Parsed JSON filters: {filters}")
                    except json.JSONDecodeError as e:
                        app_logger.error(f"Invalid JSON filter string: {e}")
                        return "", []
                else:
                    return "", []
                
            if not filters or not isinstance(filters, list):
                return "", []
                
            filter_clauses = []
            for i, filter_item in enumerate(filters):
                attribute_name = filter_item.get('attributeName')
                operator = filter_item.get('operator')
                value = filter_item.get('value')
                conjunction = filter_item.get('conjunction', 'AND')
                
                if not attribute_name or not operator:
                    continue
                    
                # column = f"{table_prefix}.{attribute_name}"
                
                sql_clause = ""
                
                # Text/String operators
                if operator == 'eq':  # is equal to
                    sql_clause = f"{attribute_name} = '{value}'"
                elif operator == 'neq':  # is not equal to
                    sql_clause = f"{attribute_name} != '{value}'"
                elif operator == 'contains':  # contains
                    sql_clause = f"{attribute_name} LIKE '%{value}%'"
                elif operator == 'icontains':  # contains (case-insensitive)
                    sql_clause = f"{attribute_name} ILIKE '%{value}%'"
                elif operator == 'not_contains':  # does not contain
                    sql_clause = f"{attribute_name} NOT LIKE '%{value}%'"
                elif operator == 'starts_with':  # starts with
                    sql_clause = f"{attribute_name} LIKE '{value}%'"
                elif operator == 'ends_with':  # ends with
                    sql_clause = f"{attribute_name} LIKE '%{value}'"
                
                # Numeric operators
                elif operator == 'gt':  # is greater than
                    sql_clause = f"{attribute_name} > {value}"
                elif operator == 'lt':  # is less than
                    sql_clause = f"{attribute_name} < {value}"
                elif operator == 'gte':  # is greater than or equal to
                    sql_clause = f"{attribute_name} >= {value}"
                elif operator == 'lte':  # is less than or equal to
                    sql_clause = f"{attribute_name} <= {value}"
                
                # Null operators
                elif operator == 'is_null':  # is null
                    sql_clause = f"{attribute_name} IS NULL"
                elif operator == 'is_not_null':  # is not null
                    sql_clause = f"{attribute_name} IS NOT NULL"
                
                # Boolean operators
                elif operator == 'eq_true':  # is true
                    sql_clause = f"{attribute_name} = TRUE"
                elif operator == 'eq_false':  # is false
                    sql_clause = f"{attribute_name} = FALSE"
                
                if sql_clause:
                    if i > 0:
                        filter_clauses.append(conjunction)
                    filter_clauses.append(sql_clause)
            
            if filter_clauses:
                return " ".join(filter_clauses), []
            return "", []
        except Exception as e:
            app_logger.error(f"Error building filter clauses: {str(e)}")
            return "", []
        
def create_dataset_record(token: str, db, dataset_id: int, warehouse_id: str, data_object: dict):
    """
    Function to create a new record in a dataset.

    Args:
        token (str): The authentication token used to validate the user.
        db: The database session used for retrieving dataset information.
        dataset_id (int): The unique ID of the dataset where the record will be created.
        warehouse_id (str): The ID of the SQL warehouse where the dataset is stored.
        data_object (dict): The data object containing the record details to be created.

    Returns:
        HttpResponse: An HttpResponse object containing the number of affected rows and the created record if successful.

    Raises:
        HTTPException: Returns a 400 or 409 error if the primary field is not set or if a duplicate record exists.
    """
    try:
        dataset_conn = DatasetService(db)
        dataset = dataset_conn.get_dataset_by_id(dataset_id)
        primary_field = getattr(dataset, "primary_field", None)
        path = getattr(dataset, "path", None)

        if primary_field is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Primary field is not set for the dataset",
            )
        sqlservice_conn = DataSetSQLService(token, warehouse_id)

        # Get the primary field value
        primary_field_value = data_object[primary_field]

        # Check if the primary field value already exists
        check_query = f"SELECT COUNT(*) FROM {common_utils.apply_tilde(path)} WHERE {common_utils.apply_tilde(primary_field)} = '{primary_field_value}'"
        existing_count = sqlservice_conn.execute_count_query(check_query)

        if existing_count > 0:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"A record with {primary_field} '{primary_field_value}' already exists",
                headers={"X-Error": "ResourceAlreadyExists"}
            )

        # Extract columns and values
        columns = list(data_object.keys())
        values = list(data_object.values())

        # Create the column part of the query
        columns_str = ", ".join([common_utils.apply_tilde(col) for col in columns])

        # Create placeholders for values (for parameterized queries)
        placeholders = ", ".join(["?"] * len(values))

        # Generate the INSERT query
        create_query = f"INSERT INTO {common_utils.apply_tilde(path)} ({columns_str}) VALUES ({placeholders})"
        affected_rows = sqlservice_conn.execute_insert_query(create_query, values)

        # create a map of column names to their values for logging
        column_value_map = {col: val for col, val in zip(columns, values)}
        app_logger.info(f"Executing create query: {column_value_map}")

        return HttpResponse(
            status=200,
            message=f"Record inserted successfully!",
            data={
                "affected_rows": affected_rows,
                "created_record": [column_value_map]
            }
        )
    except HTTPException as http_exc:
        app_logger.warning(f"HTTPException while creating record: {http_exc.status_code} - {http_exc.detail}")
        raise http_exc
    except Exception as e:
        app_logger.error(f"Error creating dataset record: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while creating the dataset record.",
            headers={"X-Error": "UnexpectedServerError"}
        )

# Function to update multiple records in a dataset.
# Args:
#     token (str): The authentication token used to validate the user.
#     db: The database session used for retrieving dataset information.
#     dataset_id (int): The unique ID of the dataset where records will be updated.
#     warehouse_id (str): The ID of the SQL warehouse where the dataset is stored.
#     primary_field (str): The primary field name to use for WHERE conditions.
#     updates (list): List of update objects containing primary_field_value and data.
# Returns:
#     An HttpResponse object containing the number of affected rows if successful.
# Raises:
#     HttpResponse: Returns a 500 internal server error response if an exception occurs.
def update_dataset_records(token: str, db, dataset_id: int, warehouse_id: str, primary_field: str, updates: list):
    try:
        dataset_conn = DatasetService(db)
        dataset = dataset_conn.get_dataset_by_id(dataset_id)
        
        # Initialize DataSetSQLService to execute SQL queries on the dataset
        sqlservice_conn = DataSetSQLService(token, warehouse_id)
        
        # Use all updates without validation check
        valid_updates = updates
        
        # Collect all unique columns that need to be updated
        all_columns = set()
        for update in valid_updates:
            all_columns.update(update.get('data', {}).keys())
        
        # Build CASE WHEN clauses for each column
        set_clauses = []
        query_parameters = []
        
        for column in all_columns:
            case_when_parts = []
            
            for update in valid_updates:
                primary_field_value = update.get('primary_field_value')
                data = update.get('data', {})
                
                if column in data:
                    case_when_parts.append(f"WHEN {common_utils.apply_tilde(primary_field)} = ? THEN ?")
                    query_parameters.extend([primary_field_value, data[column]])
            
            if case_when_parts:
                case_when_clause = f"""
                {common_utils.apply_tilde(column)} = CASE 
                    {' '.join(case_when_parts)}
                    ELSE {common_utils.apply_tilde(column)}
                END"""
                set_clauses.append(case_when_clause)
        
        # Build WHERE IN clause
        valid_primary_values = [update.get('primary_field_value') for update in valid_updates]
        where_placeholders = ', '.join(['?' for _ in valid_primary_values])
        query_parameters.extend(valid_primary_values)
        
        # Construct the final UPDATE query
        set_clause_str = ',\n    '.join(set_clauses)
        update_query = f"""
        UPDATE {common_utils.apply_tilde(dataset.path)}
        SET
            {set_clause_str}
        WHERE {common_utils.apply_tilde(primary_field)} IN ({where_placeholders})
        """
        
        app_logger.info(f"Executing bulk update query: {update_query}")
        app_logger.info(f"Query parameters: {query_parameters}")
        
        # DEBUG SECTION - Print the final query with parameters substituted
        debug_query = update_query
        for param in query_parameters:
            # Replace ? with the actual parameter value (properly quoted for strings)
            if isinstance(param, str):
                debug_query = debug_query.replace('?', f"'{param}'", 1)
            elif param is None:
                debug_query = debug_query.replace('?', 'NULL', 1)
            else:
                debug_query = debug_query.replace('?', str(param), 1)
        
        print("="*80)
        print("DEBUG: FINAL UPDATE QUERY WITH PARAMETERS")
        print("="*80)
        print(debug_query)
        print("="*80)
        # END DEBUG SECTION
        
        # Execute the single update query
        affected_rows = sqlservice_conn.execute_update_query(update_query, query_parameters)
        
        # Prepare response
        response_data = {
            "message": f"Bulk update operation completed successfully",
            "total_updates_requested": len(updates),
            "successful_updates": len(valid_updates),
            "total_affected_rows": affected_rows
        }
        
        return {
            "status": 200,
            "data": response_data
        }
    
    except Exception as e:
        # Log the full exception traceback and error message
        message = traceback.format_exc()
        app_logger.exception(f'Unable to update records. Reason - {message}')
        # Raise a 500 internal server error response with appropriate headers
        raise HttpResponse(
            status=500,
            message="An internal server error occurred while updating records",
        )

# Function to fetch dataset data by dataset ID.
# Args:
#     token (str): The authentication token used to validate the user.
#     db: The database session used for retrieving dataset information.
#     dataset_id (int): The unique ID of the dataset to fetch.
#     warehouse_id (str): The ID of the SQL warehouse where the dataset is stored.
# Returns:
#     An HttpResponse object containing the dataset data if successful.
# Raises:
#     HttpResponse: Returns a 500 internal server error response if an exception occurs.

# Helper function to normalize response data
# TODO: Move to utility file if used in multiple places
def normalize_response(data):
    normalized = []
    for item in data:
        obj = dict(item)

        # Handle columns field - can be numpy array, JSON string, or already a list
        columns = obj.get("columns")
        if isinstance(columns, np.ndarray):
            obj["columns"] = columns.tolist()
        elif isinstance(columns, str):
            # Databricks SQL connector returns complex types as JSON strings
            try:
                obj["columns"] = json.loads(columns)
            except json.JSONDecodeError:
                obj["columns"] = []

        normalized.append(obj)
    return normalized

def fetch_dataset(token: str, db, dataset_id: int, warehouse_id: str, page: int = 1, page_size: int = 1000, filters=None):
    try:
        # Initialize the DatasetService to interact with the dataset metadata
        dataset_conn = DatasetService(db)
        # Fetch the dataset path by its ID from the database
        dataset_path = dataset_conn.get_dataset_by_id(dataset_id)
        # Initialize DataSetSQLService to execute SQL queries on the dataset
        sqlservice_conn = DataSetSQLService(token, warehouse_id)
        
        # Build WHERE clause for filters
        where_clause = ""
        if filters:
            filter_clause, _ = build_filter_clauses(filters)
            if filter_clause:
                where_clause = f"WHERE {filter_clause}"

        # First, get the total count of records
        count_query = f"SELECT COUNT(*) FROM {common_utils.apply_tilde(dataset_path.path)} {where_clause}"
        total_count = sqlservice_conn.execute_count_query(count_query)
        
        # Calculate pagination values
        offset = (page - 1) * page_size
        current_record_end = page * page_size
        has_more = current_record_end < total_count
        
        # Build and execute the main data query
        data_query = f"SELECT * FROM {common_utils.apply_tilde(dataset_path.path)} {where_clause} LIMIT {page_size} OFFSET {offset}"
        data = sqlservice_conn.execute_dataset(data_query)
        
        # Return a successful HTTP response with the fetched data and pagination info
        return HttpResponse(
            status=200, 
            data=data, 
            totalCount=total_count,
            has_more=has_more,
        )
    except Exception as e:
        error_message = str(e)
        match = re.search(r'(\[.*?\].*)', error_message, re.DOTALL)
        detail = match.group(1).strip() if match else error_message
        app_logger.exception(f'Unable to fetch the data. Reason - {error_message}')
        raise HTTPException(
            status_code=500,
            detail=detail,
        )

def fetch_metadata_dataset(token: str, db, dataset_id: int, warehouse_id: str):
    """
    Fetch metadata information for a dataset by dataset ID.

    Args:
        token (str): Authentication token.
        db: Database session.
        dataset_id (int): Dataset ID.
        warehouse_id (str): SQL warehouse ID.

    Returns:
        HttpResponse: Metadata for the dataset.
    """

    try:
        # Initialize services
        dataset_conn = DatasetService(db)
        dataset_path_obj = dataset_conn.get_dataset_by_id(dataset_id)
        if not dataset_path_obj or not getattr(dataset_path_obj, "path", None):
            app_logger.error(f"Dataset path not found for dataset_id: {dataset_id}")
            raise HTTPException(
                status_code=400,
                detail="Dataset path is not defined.",
            )
        
        sqlservice_conn = DataSetSQLService(token, warehouse_id)
        table_path = dataset_path_obj.path.split('.')
        if len(table_path) != 3:
            app_logger.error(f"Invalid dataset path format: {dataset_path_obj.path}")
            raise HTTPException(
                status_code=400,
                detail="Invalid dataset path format.",
            )

        catalog, schema, table = table_path

        query_new = f"""
        SELECT 
            t.table_name,
            t.table_schema,
            t.comment AS table_comment,
            collect_list(
                named_struct(
                'col_name', c1.column_name,
                'data_type', c1.data_type,
                'comment', c1.comment,
                'tags_map', COALESCE(c2.tags_map, map())
                )
            ) AS columns
        FROM {catalog}.information_schema.tables t
        JOIN {catalog}.information_schema.COLUMNS c1
            ON t.table_name = c1.table_name
            AND t.table_schema = c1.table_schema
        LEFT JOIN (
            SELECT column_name,
                map_from_arrays(collect_list(tag_name), collect_list(tag_value)) AS tags_map
            FROM {catalog}.information_schema.column_tags
            WHERE table_name = '{table}'
            GROUP BY column_name
        ) c2 
            ON c1.column_name = c2.column_name
        WHERE t.table_name = '{table}'
            AND t.table_schema = '{schema}'
        GROUP BY t.table_name, t.table_schema, t.comment
        """

        app_logger.info(f"Executing metadata fetch query for dataset_id {dataset_id}: {query_new}")

        data_new = sqlservice_conn.execute_dataset(query_new)
        normalized = normalize_response(data_new)

        if not normalized or not isinstance(normalized, list) or not normalized[0]:
            app_logger.warning(f"No metadata found for dataset_id: {dataset_id}")
            return HttpResponse(status=200, data={})

        return HttpResponse(status=200, data=normalized[0])
    except HTTPException as http_exc:
        app_logger.warning(f"HTTPException while fetching metadata: {http_exc.status_code} - {http_exc.detail}")
        raise http_exc
    except Exception as e:
        message = traceback.format_exc()
        app_logger.exception(f'Unable to fetch metadata. Reason - {message}')
        raise HTTPException(
            status_code=500,
            detail=f"An internal server error occurred while fetching the metadata. Reason: {str(e)}",
        )
    
def update_metadata_dataset_description(token: str, db, dataset_id: int, warehouse_id: str, description: str):
    """
    Update the description of a dataset by dataset ID.

    Args:
        token (str): The authentication token used to validate the user.
        db: The database session used for retrieving dataset information.
        dataset_id (int): The unique ID of the dataset to update the description for.
        warehouse_id (str): The ID of the SQL warehouse where the dataset is stored.
        description (str): The new description for the dataset.

    Returns:
        HttpResponse: An HttpResponse object containing the update result if successful.

    Raises:
        HTTPException: Returns a 400 error if the dataset path is missing, or a 500 error if an exception occurs.
    """
    try:
        if not description or not isinstance(description, str):
            raise HTTPException(
                status_code=400,
                detail="Description must be a non-empty string.",
            )

        dataset_conn = DatasetService(db)
        dataset = dataset_conn.get_dataset_by_id(dataset_id)
        dataset_path = getattr(dataset, "path", None)

        if not dataset_path:
            raise HTTPException(
                status_code=400,
                detail="Dataset path is not defined.",
            )

        sqlservice_conn = DataSetSQLService(token, warehouse_id)
        query = f"COMMENT ON TABLE {common_utils.apply_tilde(dataset_path)} IS '{description}'"
        app_logger.info(f"Executing description update query: {query}")

        affected_rows = sqlservice_conn.execute_update_query(query)
        app_logger.info(f"Description updated for dataset {dataset_id}. Affected rows: {affected_rows}")

        return HttpResponse(
            status=200,
            message="Description updated successfully.",
            data={"affected_rows": affected_rows}
        )
    except HTTPException as http_exc:
        app_logger.warning(f"HTTPException while updating description: {http_exc.status_code} - {http_exc.detail}")
        raise http_exc
    except Exception as e:
        message = traceback.format_exc()
        app_logger.exception(f'Unable to update description. Reason - {message}')
        raise HTTPException(
            status_code=500,
            detail=f"An internal server error occurred while updating the description. Reason: {str(e)}",
        )
def update_metadata_dataset(token: str, db, dataset_id: int, warehouse_id: str, metadata_updates: dict):
    """
    Update column comments and tags for a dataset using metadata_updates dict.

    Args:
        token (str): Authentication token.
        db: Database session.
        dataset_id (int): Dataset ID.
        warehouse_id (str): SQL warehouse ID.
        metadata_updates (dict): {
            col_name: {
                "comment": str (optional),
                "type": str (optional),
                "tags": [{"key": str, "value": str}] (optional)
            }
        }

    Returns:
        HttpResponse: Update result.
    """
    try:
        dataset_conn = DatasetService(db)
        dataset = dataset_conn.get_dataset_by_id(dataset_id)
        sqlservice_conn = DataSetSQLService(token, warehouse_id)
        dataset_path = getattr(dataset, "path", None)

        if not dataset_path:
            raise HTTPException(
                status_code=400,
                detail="Dataset path is not defined.",
            )

        # Parse the dataset path to get catalog, schema, and table
        table_path = dataset_path.split('.')
        if len(table_path) != 3:
            app_logger.error(f"Invalid dataset path format: {dataset_path}")
            raise HTTPException(
                status_code=400,
                detail="Invalid dataset path format.",
            )

        catalog, schema, table = table_path

        # First, fetch existing tags for all columns that need tag updates
        columns_with_tag_updates = [col_name for col_name, meta in metadata_updates.items() if "tags" in meta]
        
        existing_tags_map = {}
        if columns_with_tag_updates:
            # Create a separate connection for fetching tags
            tags_sqlservice_conn = DataSetSQLService(token, warehouse_id)
            
            # Fetch existing tags for columns
            tags_query = f"""
            SELECT 
                column_name,
                map_from_arrays(collect_list(tag_name), collect_list(tag_value)) AS tags_map
            FROM {catalog}.information_schema.column_tags
            WHERE table_name = '{table}'
                AND column_name IN ({', '.join([f"'{col}'" for col in columns_with_tag_updates])})
            GROUP BY column_name
            """
            
            app_logger.info(f"Fetching existing tags: {tags_query}")
            existing_tags_data = tags_sqlservice_conn.execute_dataset(tags_query)
            
            # Parse existing tags into a map
            for row in existing_tags_data:
                col_name = row.get('column_name')
                tags_map = row.get('tags_map', [])
                
                app_logger.info(f"Column: {col_name}, Raw tags_map: {tags_map}, Type: {type(tags_map)}")
                
                # Convert tags_map to list of keys
                # tags_map format can be: [("key1", "value1"), ("key2", "value2")] (tuples)
                # or [["key1", "value1"], ["key2", "value2"]] (lists)
                if tags_map and isinstance(tags_map, list):
                    # Extract the first element (key) from each tag pair (tuple or list)
                    existing_tags_map[col_name] = [
                        tag[0] for tag in tags_map 
                        if (isinstance(tag, (list, tuple)) and len(tag) > 0)
                    ]
                else:
                    existing_tags_map[col_name] = []
                    
                app_logger.info(f"Parsed tag keys for {col_name}: {existing_tags_map[col_name]}")
            
            # Log all columns that should have tags but don't
            for col_name in columns_with_tag_updates:
                if col_name not in existing_tags_map:
                    app_logger.info(f"No existing tags found for column: {col_name}")

        queries = []
        affected_rows = 0

        # Now create a new connection for executing update queries
        update_sqlservice_conn = DataSetSQLService(token, warehouse_id)

        for col_name, meta in metadata_updates.items():
            comment = meta.get("comment")
            col_type = meta.get("type")
            tags = meta.get("tags")

            # Handle tags update
            if tags is not None:
                # Step 1: Delete existing tags if any
                existing_tag_keys = existing_tags_map.get(col_name, [])
                app_logger.info(f"Column {col_name}: Found {len(existing_tag_keys)} existing tag keys: {existing_tag_keys}")
                
                if existing_tag_keys:
                    unset_tags_str = ', '.join([f"'{key}'" for key in existing_tag_keys])
                    unset_query = f"ALTER TABLE {common_utils.apply_tilde(dataset_path)} ALTER COLUMN {common_utils.apply_tilde(col_name)} UNSET TAGS ({unset_tags_str})"
                    queries.append(unset_query)
                    app_logger.info(f"Executing tag deletion query: {unset_query}")
                    affected_rows += update_sqlservice_conn.execute_update_query(unset_query)
                else:
                    app_logger.info(f"No existing tags to delete for column: {col_name}")

                # Step 2: Add new tags if provided
                if tags and len(tags) > 0:
                    set_tags_parts = [f"'{tag['key']}' = '{tag['value']}'" for tag in tags]
                    set_tags_str = ', '.join(set_tags_parts)
                    set_query = f"ALTER TABLE {common_utils.apply_tilde(dataset_path)} ALTER COLUMN {common_utils.apply_tilde(col_name)} SET TAGS ({set_tags_str})"
                    queries.append(set_query)
                    app_logger.info(f"Executing tag update query: {set_query}")
                    affected_rows += update_sqlservice_conn.execute_update_query(set_query)

            # Handle comment and type update
            if comment is not None and col_type is not None:
                comment_query = f"ALTER TABLE {common_utils.apply_tilde(dataset_path)} CHANGE COLUMN {common_utils.apply_tilde(col_name)} {common_utils.apply_tilde(col_name)} {col_type} COMMENT '{comment}'"
                queries.append(comment_query)
                app_logger.info(f"Executing metadata update query: {comment_query}")
                affected_rows += update_sqlservice_conn.execute_update_query(comment_query)

        return HttpResponse(
            status=200,
            message="Metadata updated successfully",
            data={
                "affected_rows": affected_rows,
                "queries_executed": len(queries)
            }
        )
    except HTTPException as http_exc:
        app_logger.warning(f"HTTPException while updating metadata: {http_exc.status_code} - {http_exc.detail}")
        raise http_exc
    except Exception as e:
        message = traceback.format_exc()
        app_logger.exception(f'Unable to update metadata. Reason - {message}')
        raise HTTPException(
            status_code=500,
            detail=f"An internal server error occurred while updating the metadata. Reason: {str(e)}",
        )
    
    
def fetch_metadata_dataset_by_path(token: str, db, dataset_path: str, warehouse_id: str):
    try:
        # Initialize DataSetSQLService to execute SQL queries on the dataset
        sqlservice_conn = DataSetSQLService(token, warehouse_id)
        # Build and execute the SQL query to fetch metadata (DESCRIBE TABLE)
        table_path = common_utils.apply_tilde(dataset_path).split('.')
        #
        query = f"""SELECT c1.column_name AS col_name,\
c1.data_type,\
c1.comment,\
COALESCE(c2.tags_map, map()) AS tags_map \
FROM {table_path[0]}.information_schema.COLUMNS c1 \
LEFT JOIN ( SELECT column_name,map_from_arrays(collect_list(tag_name), collect_list(tag_value)) AS tags_map FROM \
{table_path[0]}.information_schema.column_tags WHERE table_name = '{table_path[2].replace("`", "")}' GROUP BY column_name ) c2 ON c1.column_name = c2.column_name \
WHERE c1.table_name = '{table_path[2].replace("`", "")}' AND c1.table_schema = '{table_path[1].replace("`", "")}';"""
        print(query)
        #query = f"DESCRIBE TABLE {dataset_path.path}"
        data = sqlservice_conn.execute_dataset(query)
        # Return a successful HTTP response with the fetched metadata
        return HttpResponse(status=200, data=data)
    except Exception as e:
        # Log the full exception traceback and error message
        message = traceback.format_exc()
        app_logger.exception(f'Unable to fetch the metadata. Reason - {message}')
        # Raise a 500 internal server error response with appropriate headers
        raise HttpResponse(
            status_code=500,
            detail="An internal server error occurred while fetching the metadata",
            headers={"WWW-Authenticate": "Bearer"},
        )

def fetch_metadata_cleansed_dataset(token: str, db, dataset_id: int, use_cleaned_table: bool, warehouse_id: str):
    try:
        # Initialize the DatasetService to interact with the dataset metadata
        dataset_conn = DatasetService(db)
        # Fetch the dataset path by its ID from the database
        dataset_path = dataset_conn.get_dataset_by_id(dataset_id)
        if(use_cleaned_table):
            table_path=common_utils.apply_tilde((dataset_path.path +"_cleaned")).split('.')
        else:
            table_path=common_utils.apply_tilde(dataset_path.path).split('.')
        sqlservice_conn = DataSetSQLService(token, warehouse_id)
        # Build and execute the SQL query to fetch metadata (DESCRIBE TABLE)

        #
        query = f"""SELECT c1.column_name AS col_name,\
c1.data_type,\
c1.comment,\
COALESCE(c2.tags_map, map()) AS tags_map \
FROM {table_path[0]}.information_schema.COLUMNS c1 \
LEFT JOIN ( SELECT column_name,map_from_arrays(collect_list(tag_name), collect_list(tag_value)) AS tags_map FROM \
{table_path[0]}.information_schema.column_tags WHERE table_name = '{table_path[2].replace("`", "")}' GROUP BY column_name ) c2 ON c1.column_name = c2.column_name \
WHERE c1.table_name = '{table_path[2].replace("`", "")}' AND c1.table_schema = '{table_path[1].replace("`", "")}';"""
        #query = f"DESCRIBE TABLE {dataset_path.path}"
        data = sqlservice_conn.execute_dataset(query)
        # Return a successful HTTP response with the fetched metadata
        return HttpResponse(status=200, data=data)
    except Exception as e:
        # Log the full exception traceback and error message
        message = traceback.format_exc()
        app_logger.exception(f'Unable to fetch the metadata. Reason - {message}')
        # Raise a 500 internal server error response with appropriate headers
        raise HttpResponse(
            status_code=500,
            detail="An internal server error occurred while fetching the metadata",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
def fetch_columns_by_datatype(token: str, db, dataset_id: int, warehouse_id: str, datatypes: list = None):
    """
    Fetch columns and their datatypes from a dataset based on specified datatypes.
    If no datatypes are provided, returns all columns.
    
    Args:
        token (str): Authentication token
        db: Database connection
        dataset_id (int): ID of the dataset
        warehouse_id (str): ID of the warehouse
        datatypes (list, optional): List of datatypes to filter columns by. If None, returns all columns.
        
    Returns:
        HttpResponse: Response with column data
    """
    try:
        # Initialize the DatasetService to interact with the dataset metadata
        dataset_conn = DatasetService(db)
        # Fetch the dataset path by its ID from the database
        dataset_path = dataset_conn.get_dataset_by_id(dataset_id)
        
        # Check if the dataset has been cleaned
        quality_conn = QualityTaskService(db)
        list_dataset_id = [i.dataset_id for i in quality_conn.get_all_tasks()]
        
        if dataset_path.id in list_dataset_id:
            table_path = common_utils.apply_tilde((dataset_path.path + "_cleaned")).split('.')
        else:
            table_path = common_utils.apply_tilde(dataset_path.path).split('.')
        
        # Initialize SQL service
        sqlservice_conn = DataSetSQLService(token, warehouse_id)
        
        # Create the SQL query - with or without datatype filter
        if datatypes and len(datatypes) > 0:
            # Filter by specified datatypes
            datatypes_formatted = ", ".join([f"'{dt.upper()}'" for dt in datatypes])
            
            query = f"""
            SELECT 
                c1.column_name AS col_name,
                c1.data_type
            FROM {table_path[0]}.information_schema.COLUMNS c1
            WHERE 
                c1.table_name = '{table_path[2]}'
                AND UPPER(c1.data_type) IN ({datatypes_formatted})
            ORDER BY c1.column_name;
            """
        else:
            # Return all columns when no datatypes are specified
            query = f"""
            SELECT 
                c1.column_name AS col_name,
                c1.data_type
            FROM {table_path[0]}.information_schema.COLUMNS c1
            WHERE 
                c1.table_name = '{table_path[2]}'
            ORDER BY c1.column_name;
            """
        
        # Execute the query
        data = sqlservice_conn.execute_dataset(query)
        
        # Return a successful HTTP response with the metadata
        return HttpResponse(status=200, data=data)
    
    except Exception as e:
        # Log the full exception traceback and error message
        message = traceback.format_exc()
        app_logger.exception(f'Unable to fetch columns by datatype. Reason - {message}')
        
        # Raise a 500 internal server error response with appropriate headers
        raise HttpResponse(
            status_code=500,
            detail="An internal server error occurred while fetching columns by datatype",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
# Function to delete a record from dataset by primary key.
# Args:
#     token (str): The authentication token used to validate the user.
#     db: The database session used for retrieving dataset information.
#     dataset_id (int): The unique ID of the dataset containing the record.
#     primary_key (str): The column name of the primary key.
#     primary_key_value: The value of the primary key to match for deletion.
#     warehouse_id (str): The ID of the SQL warehouse where the dataset is stored.
# Returns:
#     An HttpResponse object containing the deletion result if successful.
# Raises:
#     HttpResponse: Returns a 500 internal server error response if an exception occurs.
def delete_record(token: str, db, dataset_id: int, primary_field: str, primary_field_value, warehouse_id: str):
    try:
        # Initialize the DatasetService to interact with the dataset metadata
        dataset_conn = DatasetService(db)
        # Fetch the dataset path by its ID from the database
        dataset_path = dataset_conn.get_dataset_by_id(dataset_id)
        # Initialize DataSetSQLService to execute SQL queries on the dataset
        sqlservice_conn = DataSetSQLService(token, warehouse_id)
        
        # Properly escape the primary key value based on its type
        if isinstance(primary_field_value, str):
            escaped_value = f"'{primary_field_value}'"
        else:
            escaped_value = str(primary_field_value)
        
        # Build and execute the DELETE SQL query
        query = f"DELETE FROM {common_utils.apply_tilde(dataset_path.path)} WHERE {primary_field} = {escaped_value}"
        affected_rows = sqlservice_conn.execute_delete_query(query)
        
        # Return a successful HTTP response with the deletion result
        # return HttpResponse(status=200, data={
        #     "message": f"Record deleted successfully",
        #     "deleted_record": {primary_field: primary_field_value},
        #     # "affected_rows": affected_rows
        # })
        return affected_rows
    except Exception as e:
        # Log the full exception traceback and error message
        message = traceback.format_exc()
        app_logger.exception(f'Unable to delete the record. Reason - {message}')
        # Raise a 500 internal server error response with appropriate headers
        raise HttpResponse(
            status_code=500,
            detail="An internal server error occurred while deleting the record",
            headers={"WWW-Authenticate": "Bearer"},
        )