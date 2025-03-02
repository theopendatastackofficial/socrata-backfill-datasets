import os
from mta.constants import BASE_PATH


# Partitioned Asset Names
PARTITIONED_ASSETS_NAMES = [
    "crime_nypd_arrests",
    "mta_subway_hourly_ridership",
    "mta_subway_origin_destination_2023",
    "mta_subway_origin_destination_2024",
    "nyc_threeoneone_requests"
]


PARTITIONED_ASSETS_PATHS = {
    asset_name: f"{BASE_PATH}/{asset_name}"
    for asset_name in PARTITIONED_ASSETS_NAMES
}
