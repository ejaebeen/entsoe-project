import dagster as dg
from ..resources import Config, catalog_reader_resource
from pathlib import Path
from ...utils import generate_file_path_from_asset_key
import polars as pl
from entsoe_project.typed_defs.catalog import CatalogItem
from dagster_pandera import pandera_schema_to_dagster_type
from entsoe_project.schema.staging import SCHEMA
from entsoe_project.transform_defs.staging.entsoe import stg_entsoe_data


def generate_asset_key(catalog_item: CatalogItem) -> dg.AssetKey:
    return dg.AssetKey([
        catalog_item.layer,
        catalog_item.group_name,
        catalog_item.name,
    ])

stg_load_catalog = catalog_reader_resource.select_catalog("stg_load")
stg_generation_catalog = catalog_reader_resource.select_catalog("stg_generation")

@dg.asset(
    key=generate_asset_key(stg_load_catalog),
    description="Staging area for ENTSOE load data",
    group_name="staging_entsoe",
    deps=[
        generate_asset_key(catalog_reader_resource.select_catalog(dep_name))
        for dep_name in stg_load_catalog.deps
    ],
    tags=stg_load_catalog.tags,
    metadata={
        "dagster/uri": str(generate_file_path_from_asset_key(
            ["stg", "entsoe", "stg_load"], 
            partition=True
        )),
        "dagster/column_schema": pandera_schema_to_dagster_type(SCHEMA["stg_load"]).metadata.get("schema"),
    }
)
def stg_load(
        config: Config
):
    dfs = []

    # Extract
    for dep_name in stg_load_catalog.deps:
        item = catalog_reader_resource.select_catalog(dep_name)
        input_file_path = (
            Path(config.data_dir) /
            generate_file_path_from_asset_key(["ingestion", "entsoe", dep_name])
        )
        df = pl.read_parquet(input_file_path)

        dfs.append((item.kwargs.get("country_code"), df))

    # Transform
    df_all = stg_entsoe_data(dfs)

    # load
    output_file_path = (
        Path(config.data_dir) /
        generate_file_path_from_asset_key(["stg", "entsoe", "stg_load"], partition=True)
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


@dg.asset(
    key=generate_asset_key(stg_generation_catalog),
    description="Staging area for ENTSOE load data",
    group_name="staging_entsoe",
    deps=[
        generate_asset_key(catalog_reader_resource.select_catalog(dep_name))
        for dep_name in stg_generation_catalog.deps
    ],
    tags=stg_generation_catalog.tags,
    metadata={
        "dagster/uri": str(generate_file_path_from_asset_key(
            ["stg", "entsoe", "stg_generation"], 
            partition=True
        )),
        "dagster/column_schema": pandera_schema_to_dagster_type(SCHEMA["stg_generation"]).metadata.get("schema"),
    }
)
def stg_generation(
        config: Config
):
    dfs = []

    # Extract
    for dep_name in stg_generation_catalog.deps:
        item = catalog_reader_resource.select_catalog(dep_name)
        input_file_path = (
            Path(config.data_dir) /
            generate_file_path_from_asset_key(["ingestion", "entsoe", dep_name])
        )
        df = pl.read_parquet(input_file_path)

        dfs.append((item.kwargs.get("country_code"), df))

    # Transform
    df_all = stg_entsoe_data(dfs)

    # load
    output_file_path = (
        Path(config.data_dir) /
        generate_file_path_from_asset_key(["stg", "entsoe", "stg_generation"], partition=True)
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