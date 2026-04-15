from pydantic import BaseModel
class FetchData(BaseModel):
    tablename: str
    rows: int