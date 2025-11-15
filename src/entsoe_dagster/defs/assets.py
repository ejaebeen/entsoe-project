import dagster as dg
from dagster_duckdb import DuckDBResource
import pandas as pd
from datetime import datetime, timedelta
from entsoe import EntsoePandasClient

def _load_entsoe_load_data(
        country_code: str, 
        start_date: str, 
        end_date: str, 
        entsoe_client: dg.ResourceParam[EntsoePandasClient],
        duckdb: DuckDBResource,
        table_name: str
) -> pd.DataFrame:

    start = pd.Timestamp(start_date, tz='Europe/Brussels')
    end = pd.Timestamp(end_date, tz='Europe/Brussels')

    df = entsoe_client.query_load(country_code, start=start, end=end)
    df = df.reset_index(drop=False)

    with duckdb.get_connection() as conn:
        conn.execute(
            f"""
            create or replace table {table_name} as (
                select * from df
            )
            """
        )
    return df


@dg.asset
def raw_load_belgium(
    duckdb: DuckDBResource,
    entsoe_client: dg.ResourceParam[EntsoePandasClient],
):
    country_code = "BE"
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    table_name = f"raw__entsoe__load__{country_code.lower()}"

    _load_entsoe_load_data(
        country_code=country_code,
        start_date='20251113',
        end_date=end_date,
        entsoe_client=entsoe_client,
        duckdb=duckdb,
        table_name=table_name
    )


@dg.asset
def raw_load_france(
    duckdb: DuckDBResource,
    entsoe_client: dg.ResourceParam[EntsoePandasClient],
):
    country_code = "FR"
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    table_name = f"raw__entsoe__load__{country_code.lower()}"

    _load_entsoe_load_data(
        country_code=country_code,
        start_date='20251113',
        end_date=end_date,
        entsoe_client=entsoe_client,
        duckdb=duckdb,
        table_name=table_name
    )


@dg.asset
def raw_load_netherlands(
    duckdb: DuckDBResource,
    entsoe_client: dg.ResourceParam[EntsoePandasClient],
):
    country_code = "NL"
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    table_name = f"raw__entsoe__load__{country_code.lower()}"

    _load_entsoe_load_data(
        country_code=country_code,
        start_date='20251113',
        end_date=end_date,
        entsoe_client=entsoe_client,
        duckdb=duckdb,
        table_name=table_name
    )