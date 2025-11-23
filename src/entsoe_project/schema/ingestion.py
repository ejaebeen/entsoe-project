import pandera.polars as pa
import polars as pl
from pandera import Field

SCHEMA = {
    "raw_load_belgium": pa.DataFrameSchema({
        "index": pa.Column(
            pl.Datetime("ns", "Europe/Brussels"),
            description="time of the measurement",
            nullable=False
        ),
        "Actual Load": pa.Column(
            float,
            description="Actual Load (MW)",
            checks=[pa.Check.ge(0)]
        )
    }),
    "raw_load_france": pa.DataFrameSchema({
        "index": pa.Column(pl.Datetime("ns", "Europe/Paris")),
        "Actual Load": pa.Column(
            float,
            description="Actual Load (MW)",
            checks=[pa.Check.ge(0)]
        )
    }),
    "raw_load_netherlands": pa.DataFrameSchema({
        "index": pa.Column(pl.Datetime("ns", "Europe/Amsterdam")),
        "Actual Load": pa.Column(
            float,
            description="Actual Load (MW)",
            checks=[pa.Check.ge(0)]
        )
    }),
    "raw_load_germany": pa.DataFrameSchema({
        "index": pa.Column(pl.Datetime("ns", "Europe/Berlin")),
        "Actual Load": pa.Column(
            float,
            description="Actual Load (MW)",
            checks=[pa.Check.ge(0)]
        )
    }),
    "raw_load_spain": pa.DataFrameSchema({
        "index": pa.Column(pl.Datetime("ns", "Europe/Madrid")),
        "Actual Load": pa.Column(
            float,
            description="Actual Load (MW)",
            checks=[pa.Check.ge(0)]
        )
    }),
    "raw_load_italy": pa.DataFrameSchema({
        "index": pa.Column(pl.Datetime("ns", "Europe/Rome")),
        "Actual Load": pa.Column(
            float,
            description="Actual Load (MW)",
            checks=[pa.Check.ge(0)]
        )
    }),
    "raw_generation_belgium": pa.DataFrameSchema({}),
    "raw_generation_france": pa.DataFrameSchema({}),
    "raw_generation_netherlands": pa.DataFrameSchema({}),
    "raw_generation_germany": pa.DataFrameSchema({}),
    "raw_generation_spain": pa.DataFrameSchema({}),
    "raw_generation_italy": pa.DataFrameSchema({}),
    "raw_import_belgium": pa.DataFrameSchema({}),
    "raw_import_france": pa.DataFrameSchema({}),
    "raw_import_netherlands": pa.DataFrameSchema({}),
    "raw_import_germany_luxembourg": pa.DataFrameSchema({}),
    "raw_import_spain": pa.DataFrameSchema({}),
    "raw_import_italy": pa.DataFrameSchema({}),
}