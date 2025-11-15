import dagster as dg
import pandas as pd
from datetime import datetime, timedelta
from entsoe import EntsoePandasClient
import polars as pl
from pathlib import Path
from .resources import Config

def _load_entsoe_load_data(
        country_code: str, 
        start_date: str, 
        end_date: str, 
        entsoe_client: dg.ResourceParam[EntsoePandasClient],
        table_name: str,
        config: Config,
) -> pd.DataFrame:

    start = pd.Timestamp(start_date, tz='Europe/Brussels')
    end = pd.Timestamp(end_date, tz='Europe/Brussels')

    df = entsoe_client.query_load(country_code, start=start, end=end)
    df = df.reset_index(drop=False)
    
    df = pl.from_pandas(df)
    output_dir = Path(config.data_dir)
    output_file_path = output_dir / "raw" / "entsoe" / f"{table_name}.parquet"
    output_file_path.parent.mkdir(parents=True, exist_ok=True)

    df.write_parquet(output_file_path)

@dg.asset
def raw_load_belgium(
    entsoe_client: dg.ResourceParam[EntsoePandasClient],
    config: Config
):
    country_code = "BE"
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    table_name = f"raw__entsoe__load__{country_code.lower()}"

    _load_entsoe_load_data(
        country_code=country_code,
        start_date='20251113',
        end_date=end_date,
        entsoe_client=entsoe_client,
        table_name=table_name,
        config=config
    )


@dg.asset
def raw_load_france(
    entsoe_client: dg.ResourceParam[EntsoePandasClient],
    config: Config
):
    country_code = "FR"
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    table_name = f"raw__entsoe__load__{country_code.lower()}"

    _load_entsoe_load_data(
        country_code=country_code,
        start_date='20251113',
        end_date=end_date,
        entsoe_client=entsoe_client,
        table_name=table_name,
        config=config
    )


@dg.asset
def raw_load_netherlands(
    entsoe_client: dg.ResourceParam[EntsoePandasClient],
    config: Config
):
    country_code = "NL"
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    table_name = f"raw__entsoe__load__{country_code.lower()}"

    _load_entsoe_load_data(
        country_code=country_code,
        start_date='20251113',
        end_date=end_date,
        entsoe_client=entsoe_client,
        table_name=table_name,
        config=config
    )