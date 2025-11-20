from pydantic import BaseModel

class EntsoeConfig(BaseModel):
    start_date: str


class Config(BaseModel):
    entsoe: EntsoeConfig