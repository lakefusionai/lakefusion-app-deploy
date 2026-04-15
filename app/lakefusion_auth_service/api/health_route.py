from fastapi import APIRouter, Depends, HTTPException
from lakefusion_utility.models.httpresponse import HttpResponse
from lakefusion_utility.models.dbconfig import DBConfigProperties

health_router = APIRouter(tags=["Health Check API"], prefix='/health')

@health_router.get("/ping")
def health():
    return {"status": "ok"}