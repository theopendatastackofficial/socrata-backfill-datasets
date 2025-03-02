import os
from dagster import asset, AssetExecutionContext, AssetIn
from mta.datasets import (
    PARTITIONED_ASSETS_PATHS,
)
from mta.constants import LAKE_PATH
from mta.utils.duckdb_wrapper import DuckDBWrapper




@asset(
    compute_kind="DuckDB",
    group_name="Transformation",
)
def duckdb_warehouse(
    context: AssetExecutionContext,
):
    """
    Creates a persistent DuckDB file at LAKE_PATH and registers each
    asset as a DuckDB view. Partitioned assets use a different method.
    """

    duckdb_wrapper = DuckDBWrapper(LAKE_PATH)

    # Register non-partitioned assets (MTA, OTHER_MTA, WEATHER)
    non_partitioned = {}
    for table_name, directory_path in non_partitioned.items():
        duckdb_wrapper.register_local_data_skip_errors(directory_path, table_name)

    # Register partitioned assets
    for table_name, base_path in PARTITIONED_ASSETS_PATHS.items():
        duckdb_wrapper.register_partitioned_data_skip_errors(base_path, table_name)

    duckdb_wrapper.con.close()
    context.log.info("Connection to DuckDB closed.")
    return None