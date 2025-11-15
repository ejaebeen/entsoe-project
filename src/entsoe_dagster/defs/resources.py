from dagster_duckdb import DuckDBResource
import dagster as dg
from entsoe import EntsoePandasClient

database_resource = DuckDBResource(database=dg.EnvVar("DUCKDB_PATH").get_value())
entsoe_client_resource = EntsoePandasClient(api_key=dg.EnvVar("ENTSOE_ACCESS_TOKEN").get_value())

@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "duckdb": database_resource,
            "entsoe_client": entsoe_client_resource
        }
    )