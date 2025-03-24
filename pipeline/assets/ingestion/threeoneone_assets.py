import requests
import polars as pl
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dagster import asset, OpExecutionContext

from pipeline.resources.socrata_resource import SocrataResource

def process_threeoneone_df(df: pl.DataFrame) -> pl.DataFrame:
    """
    Processes the 311 DataFrame by casting key columns to correct data types.
    """
    cast_map = {
        "unique_key": pl.Utf8,
        "created_date": pl.Datetime,
        "closed_date": pl.Datetime,
        "agency": pl.Utf8,
        "agency_name": pl.Utf8,
        "latitude": pl.Float64,
        "longitude": pl.Float64,
    }
    cast_expressions = []
    for col_name, dtype in cast_map.items():
        if col_name in df.columns:
            if dtype == pl.Datetime:
                cast_expressions.append(
                    pl.col(col_name).str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%.f", strict=False)
                )
            else:
                cast_expressions.append(pl.col(col_name).cast(dtype))
    if cast_expressions:
        df = df.with_columns(cast_expressions)
    return df

def transform_threeoneone_df(df: pl.DataFrame) -> pl.DataFrame:
    """
    Applies in-memory cleaning and enrichment:
      - Calculates resolution time in hours,
      - Flags if closed_date equals the Excel error date ('1900-01-01'),
      - Flags if closed_date is earlier than created_date,
      - Extracts cd_number from community_board and flags invalid community_board values.
    
    Error markers: if a field is in error, the marker column is 1; if normal, 0.
    """
    # Marker: Excel date error – closed_date equals '1900-01-01'
    df = df.with_columns([
        pl.when(
            pl.col("closed_date") == pl.lit("1900-01-01").str.strptime(pl.Datetime, format="%Y-%m-%d", strict=False)
        ).then(1).otherwise(0).alias("is_excel_date_error")
    ])
    
    # Marker: Resolution time in hours (if both dates exist)
    df = df.with_columns([
        pl.when(
            (pl.col("closed_date").is_not_null()) & (pl.col("created_date").is_not_null())
        ).then(
            (pl.col("closed_date").cast(pl.Int64) - pl.col("created_date").cast(pl.Int64)) / 3600000000.0
        ).otherwise(None).alias("resolution_time_hours")
    ])
    
    # Marker: Invalid resolution time – 1 if closed_date exists and is earlier than created_date.
    df = df.with_columns([
        pl.when(
            (pl.col("closed_date").is_not_null()) &
            (pl.col("created_date").is_not_null()) &
            (pl.col("closed_date") < pl.col("created_date"))
        ).then(1).otherwise(0).alias("is_invalid_resolution_time")
    ])
    
    # Marker: Extract cd_number from community_board.
    df = df.with_columns([
        pl.col("community_board").str.extract(r"^\s*(\d+)", 1).alias("cd_num_str"),
        pl.col("community_board").str.extract(r"^\s*\d+\s+([A-Z ]+)", 1).alias("cd_borough")
    ]).with_columns([
        pl.when(pl.col("cd_borough") == "BRONX")
          .then(200 + pl.col("cd_num_str").cast(pl.Int64))
          .when(pl.col("cd_borough") == "BROOKLYN")
          .then(300 + pl.col("cd_num_str").cast(pl.Int64))
          .when(pl.col("cd_borough") == "MANHATTAN")
          .then(100 + pl.col("cd_num_str").cast(pl.Int64))
          .when(pl.col("cd_borough") == "QUEENS")
          .then(400 + pl.col("cd_num_str").cast(pl.Int64))
          .when(pl.col("cd_borough") == "STATEN ISLAND")
          .then(500 + pl.col("cd_num_str").cast(pl.Int64))
          .otherwise(None)
          .alias("cd_number")
    ]).drop(["cd_num_str", "cd_borough"])
    
    # Marker: Invalid cd_number – 1 if cd_number is null.
    df = df.with_columns([
        pl.when(pl.col("cd_number").is_null())
          .then(1).otherwise(0).alias("is_invalid_cd_number")
    ])
    
    return df

@asset(
    name="nyc_threeoneone_requests",
    group_name="NYC_311",
    io_manager_key="large_socrata_io_manager",
    required_resource_keys={"socrata", "large_socrata_io_manager"},
)
def nyc_threeoneone_requests(context: OpExecutionContext):
    """
    Fetches NYC 311 service requests in monthly 500K-row chunks.
    Processes and transforms data, then writes each chunk to Parquet.
    """
    endpoint = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
    start_date = datetime(2010, 1, 1)
    end_date = datetime(2025, 3, 1)
    asset_name = "nyc_threeoneone_requests"
    manager = context.resources.large_socrata_io_manager
    current = start_date
    total_chunks = 0
    while current <= end_date:
        next_month = current + relativedelta(months=1)
        year, month = current.year, current.month
        offset, batch_num = 0, 1
        context.log.info(f"🚀 Fetching data for {year}-{month:02d}...")
        while True:
            where_clause = (
                f"created_date >= '{current:%Y-%m-%dT00:00:00}' "
                f"AND created_date < '{next_month:%Y-%m-%dT00:00:00}'"
            )
            query_params = {
                "$limit": 500000,
                "$offset": offset,
                "$order": "created_date ASC",
                "$where": where_clause,
            }
            records = context.resources.socrata.fetch_data(endpoint, query_params)
            if not records:
                context.log.info(f"✅ No more data for {year}-{month:02d}, offset={offset}.")
                break
            df = pl.DataFrame(records)
            df = process_threeoneone_df(df)
            df = transform_threeoneone_df(df)
            if df is None:
                context.log.error("Transformed DataFrame is None.")
                raise ValueError("Transformation returned None.")
            try:
                manager.write_chunk(asset_name, year, month, batch_num, df)
                context.log.info(f"💾 Saved batch {batch_num} for {year}-{month:02d}.")
            except Exception as e:
                context.log.error(f"❌ Failed to save batch {batch_num} for {year}-{month:02d}: {e}")
                raise
            del df
            offset += 500000
            batch_num += 1
            total_chunks += 1
        current = next_month
    context.log.info(
        f"🏁 Completed backfill: {start_date:%Y-%m-%d} to {end_date:%Y-%m-%d}. Total chunks: {total_chunks}."
    )
    return f"Wrote {total_chunks} chunks"
