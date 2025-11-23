import pandera.polars as pa
import polars as pl

GENERATION_METRICS = [
    'Biomass | Actual Aggregated',
    'Energy storage | Actual Aggregated',
    'Fossil Gas | Actual Aggregated',
    'Fossil Oil | Actual Aggregated',
    'Hydro Pumped Storage | Actual Aggregated',
    'Hydro Pumped Storage | Actual Consumption',
    'Hydro Run-of-river and poundage | Actual Aggregated',
    'Nuclear | Actual Aggregated',
    'Other | Actual Aggregated',
    'Solar | Actual Aggregated',
    'Waste | Actual Aggregated',
    'Wind Offshore | Actual Aggregated',
    'Wind Offshore | Actual Consumption',
    'Wind Onshore | Actual Aggregated',
    'Energy storage | Actual Consumption',
    'Fossil Hard coal | Actual Aggregated',
    'Fossil Hard coal | Actual Consumption',
    'Hydro Water Reservoir | Actual Aggregated',
    'Biomass | Actual Consumption',
    'Fossil Gas | Actual Consumption',
    'Hydro Run-of-river and poundage | Actual Consumption',
    'Nuclear | Actual Consumption',
    'Other | Actual Consumption',
    'Solar | Actual Consumption',
    'Waste | Actual Consumption',
    'Wind Onshore | Actual Consumption',
    'Fossil Brown coal/Lignite | Actual Aggregated',
    'Fossil Coal-derived gas | Actual Aggregated',
    'Fossil Oil shale | Actual Aggregated',
    'Fossil Peat | Actual Aggregated',
    'Geothermal | Actual Aggregated',
    'Marine | Actual Aggregated',
    'Other renewable | Actual Aggregated',
]
GENERATION_METRICS.sort()

SCHEMA = {
    "stg_load": pa.DataFrameSchema({
        "index": pa.Column(pl.Datetime("ns", "UTC")),
        "Actual Load": pa.Column(float),
        "country_code": pa.Column(
            str,
            checks=[
                pa.Check.str_matches(r"^[A-Za-z]{2}$")
            ]
        ),
        "date": pa.Column(pl.Date),
    }),
    "stg_generation": pa.DataFrameSchema({
        "index": pa.Column(pl.Datetime("ns", "UTC")),
        "country_code": pa.Column(
            str,
            checks=[
                pa.Check.str_matches(r"^[A-Za-z]{2}$")
            ]
        ),
        "date": pa.Column(pl.Date),
        **{
            col: pa.Column(
                pl.Float64,
                nullable=True,
                description=f"generation for {col.split(" | ")[0]} - {col.split(" | ")[1]}"
            )
            for col in GENERATION_METRICS
        }

    }),
    "stg_import": pa.DataFrameSchema({
        "index": pa.Column(pl.Datetime("ns", "UTC")),
        "country_code": pa.Column(
            str,
            checks=[
                # pa.Check.str_matches(r"^[A-Za-z]{2}$")
            ]
        ),
        "date": pa.Column(pl.Date),
    }),
}