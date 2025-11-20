from pydantic import BaseModel
from typing import Optional

class CatalogItem(BaseModel):
    """Base class for catalog items."""
    name: str
    description: str
    layer: str
    group_name: str
    tags: Optional[dict[str, str]] = {}
    deps: Optional[list[str]] = None
    kwargs: Optional[dict[str, str]] = {}