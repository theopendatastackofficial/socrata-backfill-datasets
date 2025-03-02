# mta/assets/ingestion/mta_subway/mta_assets.py

import os
import gc
import requests
import polars as pl
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dagster import asset, OpExecutionContext
from mta.constants import BASE_PATH
from mta.resources.socrata_resource import SocrataResource


##########################################################
# 1) MTA Subway Hourly Ridership
#    - Data Processing
##########################################################

def process_mta_subway_hourly_ridership_df(df: pl.DataFrame) -> pl.DataFrame:
    """
    Cast columns to their appropriate types for MTA Subway Hourly Ridership.
    Handles cases where 'transfers' and 'ridership' have decimal strings.
    """

    cast_map = {
        "transit_timestamp": pl.Datetime,
        "transit_mode": pl.Utf8,
        "station_complex_id": pl.Utf8,
        "station_complex": pl.Utf8,
        "borough": pl.Utf8,
        "payment_method": pl.Utf8,
        "fare_class_category": pl.Utf8,
        "ridership": pl.Float64,     # Handles cases like "2.0"
        "transfers": pl.Float64,     # Handles cases like "0.0" -> Float64
        "latitude": pl.Float64,
        "longitude": pl.Float64,
    }

    exprs = []
    for col_name, dtype in cast_map.items():
        if col_name in df.columns:
            if dtype == pl.Datetime:
                exprs.append(pl.col(col_name).str.strptime(pl.Datetime, strict=False))
            else:
                exprs.append(pl.col(col_name).cast(dtype))

    if exprs:
        df = df.with_columns(exprs)

    return df


@asset(
    name="mta_subway_hourly_ridership",
    group_name="MTA",
    io_manager_key="large_socrata_io_manager",  # Our new manager
    required_resource_keys={"socrata", "large_socrata_io_manager"},
)
def mta_subway_hourly_ridership(context: OpExecutionContext):
    """
    Fetch MTA Subway hourly ridership (March 2022 -> Jan 2025) in 500K-row chunks,
    writing each chunk to Parquet immediately.
    """
    endpoint = "https://data.ny.gov/resource/wujg-7c2s.geojson"
    start_date = datetime(2022, 3, 1)
    end_date = datetime(2025, 1, 1)  # Example date range
    asset_name = "mta_subway_hourly_ridership"
    manager = context.resources.large_socrata_io_manager

    current = start_date
    total_chunks = 0

    while current < end_date:
        next_month = current + relativedelta(months=1)
        year = current.year
        month = current.month
        offset = 0
        batch_num = 1

        while True:
            where_clause = (
                f"transit_timestamp >= '{current:%Y-%m-%dT00:00:00}' "
                f"AND transit_timestamp < '{next_month:%Y-%m-%dT00:00:00}'"
            )
            query_params = {
                "$limit": 500000,
                "$offset": offset,
                "$order": "transit_timestamp ASC",
                "$where": where_clause,
            }

            records = context.resources.socrata.fetch_data(endpoint, query_params)
            if not records:
                context.log.info(
                    f"No more data for {asset_name} {year}-{month:02d} offset={offset}."
                )
                break

            df = pl.DataFrame(records)
            # Process columns
            df = process_mta_subway_hourly_ridership_df(df)

            context.log.info(
                f"Fetched {df.shape[0]} rows => Writing chunk {batch_num}, offset={offset}"
            )

            manager.write_chunk(asset_name, year, month, batch_num, df)
            del df  # free memory

            offset += 500000
            batch_num += 1
            total_chunks += 1

        current = next_month

    context.log.info(f"Done. Wrote {total_chunks} total chunks for {asset_name}.")
    return f"Wrote {total_chunks} chunks"


##########################################################
# 2) MTA Subway Origin-Destination 2023
#    - Data Processing
##########################################################

def process_mta_subway_origin_destination_2023_df(df: pl.DataFrame) -> pl.DataFrame:
    """
    Cast columns to their appropriate types for MTA Subway Origin-Destination 2023.
    Columns (from data dictionary):
      - year: Number -> Int64
      - month: Number -> Int64
      - day_of_week: Text -> Utf8
      - hour_of_day: Number -> Int64
      - timestamp: Floating Timestamp -> pl.Datetime
      - origin_station_complex_id: Number -> Int64
      - origin_station_complex_name: Text -> Utf8
      - origin_latitude: Number -> Float64
      - origin_longitude: Number -> Float64
      - destination_station_complex_id: Number -> Int64
      - destination_station_complex_name: Text -> Utf8
      - destination_latitude: Number -> Float64
      - destination_longitude: Number -> Float64
      - estimated_average_ridership: Number -> Float64
    """
    cast_map = {
        "year": pl.Int64,
        "month": pl.Int64,
        "day_of_week": pl.Utf8,
        "hour_of_day": pl.Int64,
        "timestamp": pl.Datetime,
        "origin_station_complex_id": pl.Int64,
        "origin_station_complex_name": pl.Utf8,
        "origin_latitude": pl.Float64,
        "origin_longitude": pl.Float64,
        "destination_station_complex_id": pl.Int64,
        "destination_station_complex_name": pl.Utf8,
        "destination_latitude": pl.Float64,
        "destination_longitude": pl.Float64,
        "estimated_average_ridership": pl.Float64,
    }

    exprs = []
    for col_name, dtype in cast_map.items():
        if col_name in df.columns:
            if dtype == pl.Datetime:
                exprs.append(pl.col(col_name).str.strptime(pl.Datetime, strict=False))
            else:
                exprs.append(pl.col(col_name).cast(dtype))

    if exprs:
        df = df.with_columns(exprs)

    return df

@asset(
    name="mta_subway_origin_destination_2023",
    group_name="MTA",
    io_manager_key="large_socrata_io_manager",
    required_resource_keys={"socrata", "large_socrata_io_manager"},
)
def mta_subway_origin_destination_2023(context: OpExecutionContext):
    """
    Fetch 2023 MTA Subway origin-destination data in 500K-row chunks, writing immediately.
    Uses 'Timestamp' column for monthly partitioning: Jan 2023 -> Jan 2024
    """
    endpoint = "https://data.ny.gov/resource/uhf3-t34z.json"
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 1, 1)  
    asset_name = "mta_subway_origin_destination_2023"
    manager = context.resources.large_socrata_io_manager

    current = start_date
    total_chunks = 0

    while current < end_date:
        next_month = current + relativedelta(months=1)
        year = current.year
        month = current.month
        offset = 0
        batch_num = 1

        while True:
            where_clause = (
                f"Timestamp >= '{current:%Y-%m-%dT00:00:00}' "
                f"AND Timestamp < '{next_month:%Y-%m-%dT00:00:00}'"
            )
            query_params = {
                "$limit": 500000,
                "$offset": offset,
                "$order": "Timestamp ASC",
                "$where": where_clause,
            }

            records = context.resources.socrata.fetch_data(endpoint, query_params)
            if not records:
                context.log.info(
                    f"No more data for {asset_name} {year}-{month:02d} offset={offset}."
                )
                break

            df = pl.DataFrame(records)
            # Process columns for OD 2023
            df = process_mta_subway_origin_destination_2023_df(df)

            context.log.info(
                f"Fetched {df.shape[0]} rows => Writing chunk {batch_num}, offset={offset}"
            )

            manager.write_chunk(asset_name, year, month, batch_num, df)
            del df

            offset += 500000
            batch_num += 1
            total_chunks += 1

        current = next_month

    context.log.info(f"Done. Wrote {total_chunks} total chunks for {asset_name}.")
    return f"Wrote {total_chunks} chunks"


##########################################################
# 3) MTA Subway Origin-Destination 2024
#    - Data Processing
##########################################################

def process_mta_subway_origin_destination_2024_df(df: pl.DataFrame) -> pl.DataFrame:
    """
    Cast columns to their appropriate types for MTA Subway Origin-Destination 2024.
    Identical to 2023 in terms of columns, so we can reuse the same schema.
    """
    cast_map = {
        "year": pl.Int64,
        "month": pl.Int64,
        "day_of_week": pl.Utf8,
        "hour_of_day": pl.Int64,
        "timestamp": pl.Datetime,
        "origin_station_complex_id": pl.Int64,
        "origin_station_complex_name": pl.Utf8,
        "origin_latitude": pl.Float64,
        "origin_longitude": pl.Float64,
        "destination_station_complex_id": pl.Int64,
        "destination_station_complex_name": pl.Utf8,
        "destination_latitude": pl.Float64,
        "destination_longitude": pl.Float64,
        "estimated_average_ridership": pl.Float64,
    }

    exprs = []
    for col_name, dtype in cast_map.items():
        if col_name in df.columns:
            if dtype == pl.Datetime:
                exprs.append(pl.col(col_name).str.strptime(pl.Datetime, strict=False))
            else:
                exprs.append(pl.col(col_name).cast(dtype))

    if exprs:
        df = df.with_columns(exprs)

    return df

@asset(
    name="mta_subway_origin_destination_2024",
    group_name="MTA",
    io_manager_key="large_socrata_io_manager",
    required_resource_keys={"socrata", "large_socrata_io_manager"},
)
def mta_subway_origin_destination_2024(context: OpExecutionContext):
    """
    Fetch 2024 MTA Subway origin-destination data in 500K-row chunks, writing immediately.
    Uses 'Timestamp' column for monthly partitioning: Jan 2024 -> Jan 2025
    """
    endpoint = "https://data.ny.gov/resource/jsu2-fbtj.json"
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2025, 1, 1)  
    asset_name = "mta_subway_origin_destination_2024"
    manager = context.resources.large_socrata_io_manager

    current = start_date
    total_chunks = 0

    while current < end_date:
        next_month = current + relativedelta(months=1)
        year = current.year
        month = current.month
        offset = 0
        batch_num = 1

        while True:
            where_clause = (
                f"Timestamp >= '{current:%Y-%m-%dT00:00:00}' "
                f"AND Timestamp < '{next_month:%Y-%m-%dT00:00:00}'"
            )
            query_params = {
                "$limit": 500000,
                "$offset": offset,
                "$order": "Timestamp ASC",
                "$where": where_clause,
            }

            records = context.resources.socrata.fetch_data(endpoint, query_params)
            if not records:
                context.log.info(
                    f"No more data for {asset_name} {year}-{month:02d} offset={offset}."
                )
                break

            df = pl.DataFrame(records)
            # Process columns for OD 2024
            df = process_mta_subway_origin_destination_2024_df(df)

            context.log.info(
                f"Fetched {df.shape[0]} rows => Writing chunk {batch_num}, offset={offset}"
            )

            manager.write_chunk(asset_name, year, month, batch_num, df)
            del df

            offset += 500000
            batch_num += 1
            total_chunks += 1

        current = next_month

    context.log.info(f"Done. Wrote {total_chunks} total chunks for {asset_name}.")
    return f"Wrote {total_chunks} chunks"