from typing import Optional, Union, List
from pathlib import Path
import yaml
import logging
from .typed_defs.catalog import CatalogItem

logger = logging.getLogger(__name__)

class CatalogReader:
    """Reads and caches catalog definitions."""
    
    def __init__(self, catalog_path: Union[str, Path]):
        self.catalog_path = Path(catalog_path)
        self._items: Optional[List[CatalogItem]] = None
        self._lookup_map: dict[str, CatalogItem] = {}

    def _load_catalog(self) -> None:
        """Internal method to load data from disk and populate cache."""
        catalog_file_path = self.catalog_path / "catalog.yaml"
        
        if not catalog_file_path.exists():
            raise FileNotFoundError(f"Catalog file not found at: {catalog_file_path}")

        try:
            with open(catalog_file_path, "r") as f:
                catalog_data = yaml.safe_load(f) or [] # Handle empty files
                
            self._items = [CatalogItem(**item) for item in catalog_data]
            # Create a lookup map for O(1) access by name
            self._lookup_map = {item.name: item for item in self._items}
            
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML: {e}")
            raise

    def get_all_items(self, refresh: bool = False) -> List[CatalogItem]:
        """Returns all catalog items, loading them if necessary."""
        if self._items is None or refresh:
            self._load_catalog()
        # We assume self._items is populated now; using 'assert' helps type checkers
        assert self._items is not None 
        return self._items

    def filter_catalog(
            self, 
            group_name: Optional[Union[List[str], str]] = None,
            layer: Optional[Union[List[str], str]] = None,
        ) -> List[CatalogItem]:
        
        items = self.get_all_items()

        # Normalize inputs to sets for faster O(1) lookups
        groups = {group_name} if isinstance(group_name, str) else set(group_name or [])
        layers = {layer} if isinstance(layer, str) else set(layer or [])

        return [
            item for item in items
            if (not groups or item.group_name in groups) 
            and (not layers or item.layer in layers)
        ]

    def select_catalog(self, table_name: str) -> Optional[CatalogItem]:
        """Finds a specific catalog item by name. Returns None if not found."""
        # Ensure data is loaded
        if self._items is None:
            self.get_all_items()
            
        return self._lookup_map.get(table_name)