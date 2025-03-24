# mta/definitions.py

import dagster
import os

from dagster import Definitions, load_assets_from_modules

# --- Local imports ---
# IO Managers
from pipeline.resources.io_managers.large_socrata_polars_parquet_io_manager import large_socrata_polars_parquet_io_manager

# Socrata resource
from pipeline.resources.socrata_resource import SocrataResource


# Asset modules (import them with an alias to avoid naming conflicts)
from pipeline.assets.ingestion import mta_assets as mta_assets_module, crime_assets as crime_assets_module, threeoneone_assets as threeoneone_assets_module

# Constants
from pipeline.constants import LAKE_PATH

# --- Load ingestion assets ---
mta_assets_list = load_assets_from_modules([mta_assets_module])
crime_assets_list = load_assets_from_modules([crime_assets_module])
threeoneone_assets_list = load_assets_from_modules([threeoneone_assets_module])

# Large Socrata IO Manager (for large batch-parquet ingestion of millions of rows)
large_socrata_io_manager = large_socrata_polars_parquet_io_manager.configured(
    {"base_dir": LAKE_PATH}
)

# ---  Resources ---
resources = {
    # Socrata API Resource
    "socrata": SocrataResource(),

    # Large Socrata IO Manager (for large single-folder parquet batches)
    "large_socrata_io_manager": large_socrata_io_manager,
}

# --- 5) Gather all assets ---
all_assets = mta_assets_list + crime_assets_list + threeoneone_assets_list

# --- 6) Dagster Definitions ---
defs = Definitions(
    assets=all_assets,
    resources=resources,
)
