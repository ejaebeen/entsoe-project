from pydantic import BaseModel
from ...models import CatalogItem


class EntsoeCatalogItemKwargs(BaseModel):
    country_code: str


class EntsoeCatalogItem(CatalogItem):
    kwargs: EntsoeCatalogItemKwargs


class IngestionCatalog(BaseModel):
    entsoe: list[EntsoeCatalogItem]