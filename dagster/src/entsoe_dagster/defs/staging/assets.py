import dagster as dg
from ..ingestion.entsoe.models import IngestionCatalog
import yaml
from ..resources import Config, catalog_reader_resource
from pathlib import Path
from ...utils import generate_file_path_from_asset_key
import polars as pl
from entsoe_project.typed_defs.catalog import CatalogItem

with open("ingestion_jobs.yaml", "r") as f:
    ingestion_catalog = yaml.safe_load(f)
ingestion_catalog = IngestionCatalog.model_validate(ingestion_catalog)

stg_load_catalog = catalog_reader_resource.select_catalog("stg_load")

def generate_asset_key(catalog_item: CatalogItem) -> dg.AssetKey:
    return dg.AssetKey([
        catalog_item.layer,
        catalog_item.group_name,
        catalog_item.name,
    ])

@dg.asset(
    key=generate_asset_key(stg_load_catalog),
    description="Staging area for ENTSOE load data",
    group_name="staging_entsoe",
    deps=[
        generate_asset_key(catalog_reader_resource.select_catalog(dep_name))
        for dep_name in stg_load_catalog.deps
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

    yield dg.MaterializeResult(
        metadata={
            "dagster/row_count": df_all.height
        }
    )