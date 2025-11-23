from entsoe import EntsoePandasClient
import polars as pl
from datetime import datetime, timedelta
from ...typed_defs.catalog import CatalogItem
from ...typed_defs.config import Config
import pandas as pd

def query_entsoe_load(
        start_date: str,
        country_code: str,
        entsoe_client: EntsoePandasClient,
) -> pl.DataFrame:
    """Read raw load data from EntsoePandasClient"""

    # prepare date
    start_date = pd.Timestamp(start_date, tz='utc')
    end_date = pd.Timestamp(
        (datetime.now() - timedelta(days=1)).strftime("%Y%m%d"),
        tz='utc'
    )

    # load data
    df = entsoe_client.query_load(
        country_code, 
        start=start_date, 
        end=end_date
    )
    df = df.reset_index(drop=False)

    df = pl.from_pandas(df)

    return df


def query_entsoe_generation(
        start_date: str,
        country_code: str,
        entsoe_client: EntsoePandasClient,
        psr_type: list[str] | None = None,
) -> pl.DataFrame:
    """Read raw generation data from EntsoePandasClient"""

    # prepare date
    start_date = pd.Timestamp(start_date, tz='utc')
    end_date = pd.Timestamp(
        (datetime.now() - timedelta(days=1)).strftime("%Y%m%d"),
        tz='utc'
    )

    # load data
    df = entsoe_client.query_generation(
        country_code, 
        start=start_date, 
        end=end_date,
        psr_type=psr_type,
    )

    # rename columns
    df.columns = [
        f"{psr} | {var_name}" for psr, var_name in df.columns
    ]
    df = df.reset_index(drop=False)

    df = pl.from_pandas(df)

    return df

# catalog_item: CatalogItem,
# entsoe_client: EntsoePandasClient,
# config: Config