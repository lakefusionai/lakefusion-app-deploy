from fastapi import HTTPException, Depends, APIRouter
from sqlalchemy.orm import Session
from lakefusion_utility.utils.databricks_util import DataSetSQLService
from app.lakefusion_middlelayer_service.utils.app_db import get_db  # Importing get_db from the specified location
from lakefusion_utility.models.business_glossary_categories import BusinessGlossaryCategoriesCreate, BusinessGlossaryCategoriesResponse  # Import your Pydantic models
from lakefusion_utility.services.business_glossary_categories_service import BusinessGlossaryCategoriesService  # Import the BusinessGlossaryCategoriesService class
from app.lakefusion_middlelayer_service.utils.app_db import get_db,token_required_wrapper


# Initialize the router with a prefix and tag
business_glossary_categories_router = APIRouter(tags=["BusinessGlossaryCategories API"], prefix='/BusinessGlossaryCategories')

# Create a new business glossary categories
@business_glossary_categories_router.post("/", response_model=BusinessGlossaryCategoriesResponse)
def create_business_glossary_categories(business_glossary_categories: BusinessGlossaryCategoriesCreate, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    business_glossary_categories.created_by = check.get('decoded', {}).get('sub', '')
    service = BusinessGlossaryCategoriesService(db)  # Create an instance of BusinessGlossaryCategoriesService
    return service.create_business_glossary_categories(business_glossary_categories)

# Read all business glossary categories
@business_glossary_categories_router.get("/")
def read_business_glossary_categories( db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = BusinessGlossaryCategoriesService(db)
    return service.read_business_glossary_categories()

# Delete a business glossary categories by name
@business_glossary_categories_router.delete("/categories_delete")
def delete_business_glossary_categories(category_id: str, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = BusinessGlossaryCategoriesService(db)  # Create an instance of BusinessGlossaryCategoriesService
    return service.delete_business_glossary_categories(category_id)

# Retrieve tables names by category name
@business_glossary_categories_router.get("/get_tables")
def get_tables_names(category_id: str, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    service = BusinessGlossaryCategoriesService(db)  # Create an instance of BusinessGlossaryCategoriesService
    table_names = service.get_tables_by_business_glossary_categories(category_id)
    if table_names is None:
        raise HTTPException(status_code=404, detail="Tables not found")
    return table_names

# Retrieve all tags
@business_glossary_categories_router.post("/{warehouse_id}")
def get_all_tags(warehouse_id: str,db: Session = Depends(get_db),check: dict = Depends(token_required_wrapper)):
    token = check.get('token')
    service = BusinessGlossaryCategoriesService(db) 
    data = service.get_all_tags_info(token, warehouse_id)
    return data

# Retrieve tags by table name
@business_glossary_categories_router.post("/{warehouse_id}/{table_name}")
def get_tags_by_table_name(warehouse_id: str, table_name: str ,db: Session = Depends(get_db),check: dict = Depends(token_required_wrapper)):
    token = check.get('token')
    service = BusinessGlossaryCategoriesService(db) 
    table_name = f"('{table_name}')"
    return service.get_tags_info_by_table_name(token, warehouse_id,table_name)

# Retrieve tags by category id
@business_glossary_categories_router.get("/{warehouse_id}/{category_id}")
def get_tags_by_category_id(warehouse_id: str,category_id: str, db: Session = Depends(get_db), check: dict = Depends(token_required_wrapper)):
    token = check.get('token')
    service = BusinessGlossaryCategoriesService(db)  # Create an instance of BusinessGlossaryCategoriesService
    category_details = service.get_tables_by_business_glossary_categories(category_id)
    if category_details is None:
        raise HTTPException(status_code=404, detail="Tables not found")
    service = BusinessGlossaryCategoriesService(db) 
    return service.get_tags_info_by_table_name(token, warehouse_id,category_details)
   
