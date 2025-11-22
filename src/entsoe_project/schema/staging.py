import pandera.polars as pa
import polars as pl

SCHEMA = {
    "stg_load": pa.DataFrameSchema({
        "index": pa.Column(pl.Datetime("ns", "UTC")),
        "Actual Load": pa.Column(float),
        "country_code": pa.Column(str),
        "date": pa.Column(pl.Date),
    }),
}   