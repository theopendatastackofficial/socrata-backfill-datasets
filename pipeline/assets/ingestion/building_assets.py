# pipeline/assets/ingestion/building_assets.py

import polars as pl
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dagster import asset, OpExecutionContext
from pipeline.utils.bbl import bbl

# Resource imports (assuming they're set up similarly to your other ingestion assets)
from pipeline.resources.socrata_resource import SocrataResource

def process_nyc_hpd_complaints_df(df: pl.DataFrame) -> pl.DataFrame:
    """
    Casts columns for the NYC HPD complaints dataset to their appropriate types,
    based on the official data dictionary.
    """

    # Define a map of column_name -> Polars data type
    cast_map = {
        "received_date": pl.Datetime,
        "problem_id": pl.Int64,
        "complaint_id": pl.Int64,
        "building_id": pl.Int64,
        "borough": pl.Utf8,
        "house_number": pl.Utf8,
        "street_name": pl.Utf8,
        "post_code": pl.Utf8,
        "block": pl.Int64,
        "lot": pl.Int64,
        "apartment": pl.Utf8,
        "community_board": pl.Int64,
        "unit_type": pl.Utf8,
        "space_type": pl.Utf8,
        "type": pl.Utf8,
        "major_category": pl.Utf8,
        "minor_category": pl.Utf8,
        "problem_code": pl.Utf8,
        "complaint_status": pl.Utf8,
        "complaint_status_date": pl.Datetime,
        "problem_status": pl.Utf8,
        "problem_status_date": pl.Datetime,
        "status_description": pl.Utf8,
        "problem_duplicate_flag": pl.Utf8,
        "complaint_anonymous_flag": pl.Utf8,
        "unique_key": pl.Utf8,
        "latitude": pl.Float64,
        "longitude": pl.Float64,
        "council_district": pl.Int64,
        "census_tract": pl.Int64,
        "bin": pl.Int64,
        "bbl": pl.Int64,
        "nta": pl.Utf8,
    }

    cast_expressions = []
    for col_name, dtype in cast_map.items():
        if col_name in df.columns:
            # For Datetime columns, let's handle potential string timestamps
            if dtype == pl.Datetime:
                cast_expressions.append(
                    pl.col(col_name).str.strptime(pl.Datetime, strict=False)
                )
            else:
                cast_expressions.append(pl.col(col_name).cast(dtype))

    if cast_expressions:
        df = df.with_columns(cast_expressions)

    return df

@asset(
    name="nyc_hpd_complaints",
    group_name="NYC_Housing",
    io_manager_key="large_socrata_io_manager",
    required_resource_keys={"socrata", "large_socrata_io_manager"},
)
def nyc_hpd_complaints(context: OpExecutionContext):
    """
    Fetch NYC HPD complaints data from the Socrata endpoint, chunked by monthly ranges.
    For each month, we retrieve data in blocks of 500,000 rows using 'received_date' 
    for ordering. The data is saved to partitioned Parquet via our large_socrata_io_manager.
    """
    endpoint = "https://data.cityofnewyork.us/resource/ygpa-z7cr.json"

    # Example date range (customize as needed)
    start_date = datetime(2010, 1, 1)
    end_date   = datetime(2025, 1, 1)

    asset_name = "nyc_hpd_complaints"
    manager    = context.resources.large_socrata_io_manager
    current    = start_date
    total_chunks = 0

    while current < end_date:
        next_month = current + relativedelta(months=1)
        year, month = current.year, current.month
        offset, batch_num = 0, 1

        context.log.info(f"🚀 Fetching data for {year}-{month:02d}...")

        while True:
            # Build the query params for this chunk
            where_clause = (
                f"received_date >= '{current:%Y-%m-%dT00:00:00}' "
                f"AND received_date < '{next_month:%Y-%m-%dT00:00:00}'"
            )
            query_params = {
                "$limit": 500000,
                "$offset": offset,
                "$order": "received_date ASC",
                "$where": where_clause,
            }

            # Fetch data from Socrata
            records = context.resources.socrata.fetch_data(endpoint, query_params)
            if not records:
                context.log.info(f"✅ No more data for {year}-{month:02d}, offset={offset}.")
                break

            # Convert records to Polars for Parquet writing
            df = pl.DataFrame(records)
            df = process_nyc_hpd_complaints_df(df)

            # No further processing for now, just write directly.
            manager.write_chunk(asset_name, year, month, batch_num, df)
            context.log.info(f"💾 Saved batch {batch_num} for {year}-{month:02d} with {df.shape[0]} rows.")

            offset += 500000
            batch_num += 1
            total_chunks += 1

        current = next_month

    context.log.info(
        f"🏁 Completed backfill: {start_date:%Y-%m-%d} to {end_date:%Y-%m-%d}. Total chunks: {total_chunks}."
    )
    return f"Wrote {total_chunks} chunks"


def process_nyc_dob_violations_df(df: pl.DataFrame) -> pl.DataFrame:
    """
    1) Rename columns based on a data dictionary (uppercase -> snake_case).
    2) Cast columns to correct data types.
    3) Attempt to parse date columns ("issue_date", "disposition_date")
       using multiple formats. If none match, the date becomes null, and
       we mark 'bad_issue_date' or 'bad_disposition_date' as 1.
    4) Create a 'bbl' column if "boro", "block", and "lot" exist.
    """

    # ----------------------------------------------------------------------------
    # (1) Rename columns to snake_case
    # ----------------------------------------------------------------------------
    rename_map = {
        "ISN_DOB_BIS_VIOL": "isn_dob_bis_viol",
        "BORO": "boro",
        "BIN": "bin",
        "BLOCK": "block",
        "LOT": "lot",
        "ISSUE_DATE": "issue_date",
        "VIOLATION_TYPE_CODE": "violation_type_code",
        "VIOLATION_NUMBER": "violation_number",
        "HOUSE_NUMBER": "house_number",
        "STREET": "street",
        "DISPOSITION_DATE": "disposition_date",
        "DISPOSITION_COMMENTS": "disposition_comments",
        "DEVICE_NUMBER": "device_number",
        "DESCRIPTION": "description",
        "ECB_NUMBER": "ecb_number",
        "NUMBER": "number",
        "VIOLATION_CATEGORY": "violation_category",
        "VIOLATION_TYPE": "violation_type",
    }

    df = df.rename(
        {old_col: new_col for old_col, new_col in rename_map.items() if old_col in df.columns}
    )

    # ----------------------------------------------------------------------------
    # (2) Cast columns to correct data types (excluding date columns)
    # ----------------------------------------------------------------------------
    cast_map = {
        "isn_dob_bis_viol": pl.Utf8,
        "boro": pl.Utf8,
        "bin": pl.Int64,
        "block": pl.Int64,
        "lot": pl.Int64,
        "violation_type_code": pl.Utf8,
        "violation_number": pl.Utf8,
        "house_number": pl.Utf8,
        "street": pl.Utf8,
        "disposition_comments": pl.Utf8,
        "device_number": pl.Utf8,
        "description": pl.Utf8,
        "ecb_number": pl.Utf8,
        "number": pl.Utf8,
        "violation_category": pl.Utf8,
        "violation_type": pl.Utf8,
    }

    cast_expressions = []
    for col_name, dtype in cast_map.items():
        if col_name in df.columns:
            cast_expressions.append(pl.col(col_name).cast(dtype, strict=False))

    if cast_expressions:
        df = df.with_columns(cast_expressions)

    # ----------------------------------------------------------------------------
    # (3) Parse date columns with multiple fallback formats
    # ----------------------------------------------------------------------------
    def parse_date_chain(col: str) -> pl.Expr:
        """
        Attempt parsing col with multiple date/time formats in succession.
        If the string doesn't match any format, final result is null.
        """
        return (
            pl.col(col)
            # 1) e.g. '2023-01-02T12:34:56.789'
            .str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%.f", strict=False)
            # 2) e.g. '2023-01-02T12:34:56'
            .fill_null(pl.col(col).str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S", strict=False))
            # 3) e.g. '2023-01-02'
            .fill_null(pl.col(col).str.strptime(pl.Datetime, format="%Y-%m-%d", strict=False))
            # 4) e.g. '19751021' => 1975-10-21
            .fill_null(pl.col(col).str.strptime(pl.Datetime, format="%Y%m%d", strict=False))
        )

    for date_col in ["issue_date", "disposition_date"]:
        if date_col in df.columns:
            # Keep original string to detect parse failures
            df = df.with_columns(pl.col(date_col).alias(f"__orig_{date_col}"))

            parsed_expr = parse_date_chain(date_col).alias(date_col)
            df = df.with_columns(parsed_expr)

            # Indicate parse failures:
            # original was non-null, final parsed is null => parse fail
            df = df.with_columns(
                pl.when(
                    (pl.col(f"__orig_{date_col}").is_not_null()) & (pl.col(date_col).is_null())
                )
                .then(1)
                .otherwise(0)
                .alias(f"bad_{date_col}")
            )

            df = df.drop([f"__orig_{date_col}"])

    # ----------------------------------------------------------------------------
    # (4) Create a new bbl column if "boro", "block", and "lot" exist
    # ----------------------------------------------------------------------------
    if {"boro", "block", "lot"}.issubset(df.columns):
        temp_df = df.select(pl.struct(["boro", "block", "lot"]).alias("temp_struct"))
        struct_series = temp_df["temp_struct"].to_list()  # list of dicts

        bbl_values = [bbl(d["boro"], d["block"], d["lot"]) for d in struct_series]
        bbl_series = pl.Series("bbl", bbl_values)
        df = df.with_columns(bbl_series)

    return df

@asset(
    group_name="NYC_Buildings",
    io_manager_key="medium_socrata_io_manager",
    required_resource_keys={"socrata", "medium_socrata_io_manager"},
)
def nyc_dob_violations(context: OpExecutionContext):
    """
    Fetch DOB Violations data using a continuous offset approach:
      1) No monthly partitioning; increment offset by 'limit' each time.
      2) If there's no more data, we stop.
      3) Each chunk is written to a single folder: <base_dir>/nyc_dob_violations/
    """

    endpoint = "https://data.cityofnewyork.us/resource/3h2n-5cm9.json"
    asset_name = "nyc_dob_violations"
    manager = context.resources.medium_socrata_io_manager

    limit = 1_000_000
    offset = 0
    batch_num = 1
    total_rows = 0

    while True:
        context.log.info(f"Fetching data at offset={offset}, limit={limit}...")

        query_params = {
            "$limit": limit,
            "$offset": offset,
            "$order": "issue_date ASC",
        }

        records = context.resources.socrata.fetch_data(endpoint, query_params)
        if not records:
            context.log.info(f"No more data returned at offset={offset}. Done.")
            break

        df = pl.DataFrame(records)

        # Process the DataFrame: rename cols, parse dates, add bbl, etc.
        df = process_nyc_dob_violations_df(df)

        manager.write_chunk(asset_name, batch_num, df)
        rows_in_chunk = df.shape[0]
        total_rows += rows_in_chunk

        context.log.info(f"💾 Wrote batch {batch_num} ({rows_in_chunk} rows).")

        del df
        offset += limit
        batch_num += 1

    context.log.info(
        f"✅ Completed ingestion. Total rows downloaded: {total_rows} in {batch_num - 1} batches."
    )
    return f"Wrote {batch_num - 1} chunks, {total_rows} rows total."


@asset(
    group_name="NYC_Buildings",
    io_manager_key="medium_socrata_io_manager",
    required_resource_keys={"socrata", "medium_socrata_io_manager"},
)
def nyc_dob_ecb_violations(context: OpExecutionContext):
    """
    Fetches NYC arrests data using a continuous offset approach, without
    monthly partitioning. We:
      1) Start from a minimal date (or no date filter if you want the entire dataset).
      2) Increase offset by "limit" each time until no more data is returned.
      3) Write each chunk to a Parquet file in the same folder (no partitioning by date).
    """

    endpoint = "https://data.cityofnewyork.us/resource/6bgk-3dad.json"
    asset_name = "nyc_dob_ecb_violations"
    manager = context.resources.medium_socrata_io_manager

    # If you want to start from a specific earliest date, set a where clause:
    # e.g., arrest_date >= '2013-01-01T00:00:00'
    # Remove or adjust as needed for your use case.
    #where_clause = "arrest_date >= '2013-01-01T00:00:00'"

    limit = 1_000_000
    offset = 0
    batch_num = 1
    total_rows = 0

    while True:
        context.log.info(f"Fetching data at offset={offset}, limit={limit}...")

        query_params = {
            "$limit": limit,
            "$offset": offset,
            "$order": "issue_date ASC",     # or whichever ordering field is relevant
           # "$where": where_clause           # optional; remove if not needed
        }

        records = context.resources.socrata.fetch_data(endpoint, query_params)
        if not records:
            context.log.info(f"No more data returned at offset={offset}. Done.")
            break

        df = pl.DataFrame(records)
        # Optional: If you want to cast columns or do any Polars-based transformations,
        # you can insert that step here, e.g.: df = process_crime_df(df)

        # Write this chunk as a parquet file, all in one folder
        manager.write_chunk(asset_name, batch_num, df)

        rows_in_chunk = df.shape[0]
        total_rows += rows_in_chunk
        context.log.info(f"💾 Wrote batch {batch_num} ({rows_in_chunk} rows).")

        # Clean up
        del df

        # Prepare for next iteration
        offset += limit
        batch_num += 1

    context.log.info(
        f"✅ Completed ingestion. Total rows downloaded: {total_rows} in {batch_num - 1} batches."
    )
    return f"Wrote {batch_num - 1} chunks, {total_rows} rows total."