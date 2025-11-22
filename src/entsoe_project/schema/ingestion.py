import pandera.polars as pa
import polars as pl
from pandera import Field

SCHEMA = {
    "raw_load_belgium": pa.DataFrameSchema({
        "index": pa.Column(pl.Datetime("ns", "Europe/Brussels")),
        "Actual Load": pa.Column(float)
    }),
    "raw_load_france": pa.DataFrameSchema({
        "index": pa.Column(pl.Datetime("ns", "Europe/Paris")),
        "Actual Load": pa.Column(float)
    }),
    "raw_load_netherlands": pa.DataFrameSchema({
        "index": pa.Column(pl.Datetime("ns", "Europe/Amsterdam")),
        "Actual Load": pa.Column(float)
    }),
    "raw_load_germany": pa.DataFrameSchema({
        "index": pa.Column(pl.Datetime("ns", "Europe/Berlin")),
        "Actual Load": pa.Column(float)
    }),
    "raw_load_spain": pa.DataFrameSchema({
        "index": pa.Column(pl.Datetime("ns", "Europe/Madrid")),
        "Actual Load": pa.Column(float)
    }),
    "raw_load_italy": pa.DataFrameSchema({
        "index": pa.Column(pl.Datetime("ns", "Europe/Rome")),
        "Actual Load": pa.Column(float)
    }),
}