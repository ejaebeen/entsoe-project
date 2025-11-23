import dagster as dg
from entsoe import EntsoePandasClient
import polars as pl
from pathlib import Path
from ...resources import Config, catalog_reader_resource
from entsoe_project.typed_defs.catalog import CatalogItem
from ....utils import generate_file_path_from_asset_key
from entsoe_project.transform_defs.ingestion.entsoe import DOMAIN_PROCESSORS
from entsoe_project.schema.ingestion import SCHEMA
from dagster_pandera import pandera_schema_to_dagster_type


def _save_data(df: pl.DataFrame, output_dir: Path, asset_key: list[str]):
    """save parquet file"""
    output_file_path = output_dir / generate_file_path_from_asset_key(asset_key)
    output_file_path.parent.mkdir(parents=True, exist_ok=True)

    df.write_parquet(output_file_path)


def build_etl_job(catalog_item: CatalogItem) -> dg.Definitions:
    # obtain parameters
    name = catalog_item.name
    kwargs = catalog_item.kwargs
    description = catalog_item.description
    group_name = catalog_item.group_name
    layer = catalog_item.layer
    domain = catalog_item.tags.get("domain")
    asset_group_name = f"{layer}_{group_name}"

    asset_key = [layer, group_name, name]

    @dg.asset(
        key=asset_key,
        description=description,
        group_name=asset_group_name,
        metadata={
            "dagster/uri": generate_file_path_from_asset_key(asset_key),
            "dagster/column_schema": pandera_schema_to_dagster_type(SCHEMA[name]).metadata.get("schema"),
        },
        tags=catalog_item.tags
    )
    def etl_asset(
        entsoe_client: dg.ResourceParam[EntsoePandasClient],
        config: Config
    ):
        df = DOMAIN_PROCESSORS[domain](
            start_date=config.entsoe_start_date,
            entsoe_client=entsoe_client,
            kwargs=kwargs
        )
        _save_data(
            df=df,
            output_dir=Path(config.data_dir),
            asset_key=asset_key,
        )

        yield dg.MaterializeResult(
            metadata={
                "dagster/row_count": df.height,
            }
        )

    return dg.Definitions(
        assets=[etl_asset],
    )


def load_etl_job():
    ingestion_entsoe_catalog = catalog_reader_resource.filter_catalog(
        group_name="entsoe", layer="ingestion"
    )

    return dg.Definitions.merge(
        *[
            build_etl_job(catalog_item)
            for catalog_item in ingestion_entsoe_catalog
        ]
    )

@dg.definitions
def defs():
    return load_etl_job()
