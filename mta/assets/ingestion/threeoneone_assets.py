# File: mta/assets/ingestion/threeoneone_assets.py

import polars as pl
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dagster import asset, OpExecutionContext

from mta.resources.socrata_resource import SocrataResource

def process_threeoneone_df(df: pl.DataFrame) -> pl.DataFrame:
    """
    Processes the 311 DataFrame by casting key columns to correct data types.
    """

    # Mapping of column name => target Polars dtype
    cast_map = {
        # "unique_key": The text ID of the service request
        "unique_key": pl.Utf8,

        # "created_date" / "closed_date" => Datetime
        "created_date": pl.Datetime,
        "closed_date": pl.Datetime,

        # "agency", "agency_name", etc. => text
        "agency": pl.Utf8,
        "agency_name": pl.Utf8,

        # "latitude" / "longitude"
        "latitude": pl.Float64,
        "longitude": pl.Float64,
    }

    cast_expressions = []
    for col_name, dtype in cast_map.items():
        if col_name in df.columns:
            if dtype == pl.Datetime:
                # We'll parse these from strings into date/datetime
                cast_expressions.append(
                    pl.col(col_name).str.strptime(pl.Datetime, strict=False)
                )
            else:
                cast_expressions.append(pl.col(col_name).cast(dtype))

    if cast_expressions:
        df = df.with_columns(cast_expressions)

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
    Casts columns to correct types, then writes each chunk to Parquet immediately.
    """

    endpoint = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

    # Example: fetch from Jan 2025 to March 2025
    start_date = datetime(2010, 1, 1)
    end_date = datetime(2025, 3, 1)

    asset_name = "nyc_threeoneone_requests"
    manager = context.resources.large_socrata_io_manager

    current = start_date
    total_chunks = 0

    while current <= end_date:
        # We'll do monthly partitioning based on 'created_date'
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

            # Process columns
            df = process_threeoneone_df(df)

            # Write chunk
            try:
                manager.write_chunk(asset_name, year, month, batch_num, df)
                context.log.info(f"💾 Saved batch {batch_num} for {year}-{month:02d}.")
            except Exception as e:
                context.log.error(f"❌ Failed to save batch {batch_num} for {year}-{month:02d}: {e}")
                raise

            # Free memory
            del df

            offset += 500000
            batch_num += 1
            total_chunks += 1

        current = next_month

    context.log.info(
        f"🏁 Completed backfill: {start_date:%Y-%m-%d} to {end_date:%Y-%m-%d}. Total chunks: {total_chunks}."
    )
    return f"Wrote {total_chunks} chunks"
