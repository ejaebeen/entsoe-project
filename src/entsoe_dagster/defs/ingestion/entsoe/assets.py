import dagster as dg
import pandas as pd
from datetime import datetime, timedelta
from entsoe import EntsoePandasClient
import polars as pl
from pathlib import Path
from ...resources import Config
from .models import EntsoeCatalogItem, IngestionCatalog
from ....utils import generate_file_path_from_asset_key
import yaml


def _load_entsoe_load_data(
        country_code: str, 
        start_date: str, 
        end_date: str, 
        entsoe_client: dg.ResourceParam[EntsoePandasClient],
) -> pd.DataFrame:
    """load entsoe load data from entsoe client"""

    start = pd.Timestamp(start_date, tz='Europe/Brussels')
    end = pd.Timestamp(end_date, tz='Europe/Brussels')

    df = entsoe_client.query_load(country_code, start=start, end=end)
    df = df.reset_index(drop=False)
    
    df = pl.from_pandas(df)

    return df


def _save_data(df: pl.DataFrame, output_dir: Path, asset_key: list[str]):
    """save parquet file"""
    output_file_path = output_dir / generate_file_path_from_asset_key(asset_key)
    output_file_path.parent.mkdir(parents=True, exist_ok=True)

    df.write_parquet(output_file_path)


def build_etl_job(entsoe_catalog_item: EntsoeCatalogItem) -> dg.Definitions:
    # obtain parameters
    name = entsoe_catalog_item.name
    country_code = entsoe_catalog_item.kwargs.country_code
    description = entsoe_catalog_item.description

    asset_key = ["raw", "entsoe", name]

    @dg.asset(
        key=asset_key,
        description=description,
        group_name="ingestion_entsoe",
    )
    def etl_asset(
        entsoe_client: dg.ResourceParam[EntsoePandasClient],
        config: Config
    ):
        df = _load_entsoe_load_data(
            country_code=country_code,
            start_date=config.entsoe_start_date,
            end_date=(datetime.now() - timedelta(days=1)).strftime("%Y%m%d"),
            entsoe_client=entsoe_client,
        )
        _save_data(
            df=df,
            output_dir=Path(config.data_dir),
            asset_key=asset_key,
        )

    return dg.Definitions(
        assets=[etl_asset],
    )


def load_etl_job():

    with open("ingestion_jobs.yaml", "r") as f:
        ingestion_catalog = yaml.safe_load(f)
    ingestion_catalog = IngestionCatalog.model_validate(ingestion_catalog)

    return dg.Definitions.merge(
        *[
            build_etl_job(catalog_item)
            for catalog_item in ingestion_catalog.entsoe
        ]
    )

@dg.definitions
def defs():
    return load_etl_job()
