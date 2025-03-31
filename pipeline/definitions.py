# mta/definitions.py

import dagster
import os

from dagster import Definitions, load_assets_from_modules

# --- Local imports ---
# IO Managers
from pipeline.resources.io_managers.large_socrata_polars_parquet_io_manager import LargeSocrataPolarsParquetIOManager
from pipeline.resources.io_managers.medium_socrata_polars_parquet_io_manager import MediumSocrataPolarsParquetIOManager

# Socrata resource
from pipeline.resources.socrata_resource import SocrataResource


# Asset modules (import them with an alias to avoid naming conflicts)
from pipeline.assets.ingestion import mta_assets, crime_assets, threeoneone_assets, building_assets

# Constants
from pipeline.constants import LAKE_PATH

# --- Load ingestion assets ---
mta_assets = load_assets_from_modules([mta_assets])
crime_assets = load_assets_from_modules([crime_assets])
threeoneone_assets = load_assets_from_modules([threeoneone_assets])
building_assets = load_assets_from_modules([building_assets])

# Large Socrata IO Manager (for large batch-parquet ingestion of millions of rows)
large_socrata_io_manager = LargeSocrataPolarsParquetIOManager(base_dir= LAKE_PATH)

medium_socrata_io_manager = MediumSocrataPolarsParquetIOManager(base_dir=LAKE_PATH)



resources = {
    # Socrata API Resource
    "socrata": SocrataResource(),

    # Large Socrata IO Manager (for large single-folder parquet batches)
    "large_socrata_io_manager": large_socrata_io_manager,
    "medium_socrata_io_manager": medium_socrata_io_manager,
    
}

# --- 5) Gather all assets ---
all_assets = mta_assets + crime_assets + threeoneone_assets + building_assets

# --- 6) Dagster Definitions ---
defs = Definitions(
    assets=all_assets,
    resources=resources,
)
