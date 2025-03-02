import os

# Define the base path relative to the location of the current file
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "opendata"))

# LAKE_PATH for DuckDB
LAKE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "duckDB", "data.duckdb"))


DAGSTER_PATH=os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "logs"))