from fastapi import HTTPException, Depends, APIRouter
from sqlalchemy.orm import Session
from app.lakefusion_middlelayer_service.utils.app_db import get_db,token_required_wrapper
from lakefusion_utility.models.httpresponse import HttpResponse
from lakefusion_utility.services.business_glossary_service import BusinessGlossary_Service

# Initialize the router with a prefix and tag
business_glossary_router = APIRouter(tags=["Business Glossary API"], prefix='/business-glossary')

# create business glossary
@business_glossary_router.put("/")
async def create_business_glossary(path: str = '/', check: dict = Depends(token_required_wrapper)):
    token = check.get('token')
    BusinessGlossaryService = BusinessGlossary_Service(token)
    msg, file_path = BusinessGlossaryService.upload_file(path)
    job_response = BusinessGlossaryService.create_run_job(file_path)
    return msg,job_response
