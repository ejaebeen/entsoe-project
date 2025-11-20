from dagster_duckdb import DuckDBResource
import dagster as dg
from entsoe import EntsoePandasClient
from entsoe_project.reader import CatalogReader

# duckdb resource
database_resource = DuckDBResource(database=dg.EnvVar("DUCKDB_PATH").get_value())

# entsoe client resource
entsoe_client_resource = EntsoePandasClient(api_key=dg.EnvVar("ENTSOE_ACCESS_TOKEN").get_value())

# catalog reader resource
catalog_reader_resource = CatalogReader(catalog_path=dg.EnvVar("CATALOG_PATH").get_value())

# config
class Config(dg.ConfigurableResource):
    data_dir: str = "./data"
    entsoe_start_date: str = "20251113"


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "duckdb": database_resource,
            "entsoe_client": entsoe_client_resource,
            "config": Config(
                data_dir=dg.EnvVar("DATA_DIR"),
                entsoe_start_date="20251113",
            ),
            "catalog_reader": catalog_reader_resource,
        }
    )