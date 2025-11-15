from dagster_duckdb import DuckDBResource
import dagster as dg
from entsoe import EntsoePandasClient

# duckdb resource
database_resource = DuckDBResource(database=dg.EnvVar("DUCKDB_PATH").get_value())

# entsoe client resource
entsoe_client_resource = EntsoePandasClient(api_key=dg.EnvVar("ENTSOE_ACCESS_TOKEN").get_value())

# config
class Config(dg.Config):
    data_dir: str = "./data"


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "duckdb": database_resource,
            "entsoe_client": entsoe_client_resource,
            "config": Config(data_dir=dg.EnvVar("DATA_DIR")),
        }
    )