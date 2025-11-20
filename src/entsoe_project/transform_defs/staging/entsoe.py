import polars as pl

def stg_entsoe_data(dfs: list[tuple[str, pl.DataFrame]]):
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

    return df_all