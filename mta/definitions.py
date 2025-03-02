# File: mta/definitions.py

import dagster
import os

from dagster import Definitions, FilesystemIOManager, load_assets_from_modules

# --- Local imports ---
# IO Managers
from mta.resources.io_managers.large_socrata_polars_parquet_io_manager import large_socrata_polars_parquet_io_manager

# Socrata resource
from mta.resources.socrata_resource import SocrataResource

# Asset modules
from mta.assets.ingestion import mta_assets, crime_assets, threeoneone_assets

# Constants
from mta.constants import BASE_PATH

# --- Load ingestion assets ---
mta_assets = load_assets_from_modules([mta_assets])
crime_assets = load_assets_from_modules([crime_assets])
threeoneone_assets= load_assets_from_modules([threeoneone_assets])


# Large Socrata IO Manager (for large batch-parquet ingestion of millions of rows)
large_socrata_io_mgr = large_socrata_polars_parquet_io_manager.configured(
    {"base_dir": BASE_PATH}
)


# ---  Resources ---
resources = {
    # Socrata API Resource
    "socrata": SocrataResource(),

    # Large Socrata IO Manager (for large single-folder parquet batches)
    "large_socrata_io_manager": large_socrata_io_mgr,
}

# --- 5) Gather all assets ---
all_assets = mta_assets + crime_assets + threeoneone_assets 

# --- 6) Dagster Definitions ---
defs = Definitions(
    assets=all_assets,
    resources=resources,
)
