from fastapi import APIRouter

health_router = APIRouter(tags=["Health check API"], prefix='/health')

@health_router.get("/ping")
def health():
    return {"status": "ok"}
