from pydantic import BaseModel, Field
from typing import Optional

class CatalogItem(BaseModel):
    """Base class for catalog items."""
    name: str = Field(..., description="name of the data table")
    description: str = Field(..., description="description of the data table")
    layer: str = Field(..., description="layer of the data table in the pipeline (ingestion, staging, curated etc.)")
    group_name: str = Field(..., description="group name of the data table")
    tags: Optional[dict[str, str]] = Field({}, description="tags for the data table")
    deps: Optional[list[str]] = Field(None, description="name of the data table that has direct upstream dependencies")
    kwargs: Optional[dict[str, str]] = Field({}, description="kwargs that may be used for the functions")