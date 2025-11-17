import dagster as dg
from ..ingestion.entsoe.models import IngestionCatalog
import yaml
from ..resources import Config
from pathlib import Path
from ...utils import generate_file_path_from_asset_key
import polars as pl

with open("ingestion_jobs.yaml", "r") as f:
    ingestion_catalog = yaml.safe_load(f)
ingestion_catalog = IngestionCatalog.model_validate(ingestion_catalog)


@dg.asset(
    key=["stg", "entsoe", "stg_load"],
    description="Staging area for ENTSOE load data",
    group_name="staging_entsoe",
    deps=[
        dg.AssetKey(["raw", "entsoe", item.name]) 
        for item in ingestion_catalog.entsoe 
        if "raw_load_" in item.name
    ]
)
def stg_load(
        config: Config
):
    
    dfs = []

    # Extract
    for item in ingestion_catalog.entsoe:
        if "raw_load_" in item.name:
            input_file_path = (
                Path(config.data_dir) /
                generate_file_path_from_asset_key(["raw", "entsoe", item.name])
            )
            df = pl.read_parquet(input_file_path)

            dfs.append((item.kwargs.country_code, df))

    # Transform
    processed_dfs = []
    for country_code, df in dfs:
        df = (
            df
            .with_columns([
                pl.col("index").dt.convert_time_zone("UTC").alias("index"),
                pl.lit(country_code).alias("country_code"),
                pl.col("index").dt.date().alias("date"),
            ])
        )
        processed_dfs.append(df)
    df_all = pl.concat(processed_dfs, how="vertical")

    # load
    output_file_path = (
        Path(config.data_dir) /
        generate_file_path_from_asset_key(["stg", "entsoe", "stg_load"])
    )
    output_file_path.parent.mkdir(parents=True, exist_ok=True)

    df_all.write_parquet(
        output_file_path,
        partition_by=["date", "country_code"]
    )