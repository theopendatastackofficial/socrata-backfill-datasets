# File: mta/resources/io_managers/medium_socrata_polars_parquet_io_manager.py

import os
import polars as pl
from dagster import IOManager, io_manager, OutputContext, InputContext, get_dagster_logger

class MediumSocrataPolarsParquetIOManager(IOManager):
    """
    A "medium" Socrata I/O manager for chunked API writes:
      - Like the LargeSocrataPolarsParquetIOManager, we write individual chunks as Parquet files.
      - Unlike “large”, we do NOT create subfolders by year=YYYY/month=MM.
      - Instead, we store all .parquet chunks in a single directory:
          {base_dir}/{asset_name}/
        Each chunk is named: {asset_name}_{batch_num}.parquet
      - handle_output() is essentially no-op for final write, because chunk files are written
        during ingestion in real-time.
    """

    def __init__(self, base_dir: str):
        self._base_dir = base_dir
        self._logger = get_dagster_logger()

    def write_chunk(
        self,
        asset_name: str,
        batch_num: int,
        df: pl.DataFrame
    ):
        """
        Write the chunk of data immediately to a parquet file called:
            {base_dir}/{asset_name}/{asset_name}_{batch_num}.parquet
        """
        out_dir = os.path.join(self._base_dir, asset_name)
        os.makedirs(out_dir, exist_ok=True)

        file_name = f"{asset_name}_{batch_num}.parquet"
        file_path = os.path.join(out_dir, file_name)

        self._logger.info(
            f"[MediumSocrataPolarsParquetIOManager] Writing chunk {batch_num} "
            f"({df.shape[0]} rows) to {file_path}"
        )

        df.write_parquet(file_path)

    def handle_output(self, context: OutputContext, obj):
        """
        This is called by Dagster after the asset function completes. Since we already
        write chunks during ingestion, no final single-file write is needed here.
        """
        self._logger.info(
            f"[MediumSocrataPolarsParquetIOManager] Asset '{context.asset_key}' "
            f"has completed. No additional writes needed."
        )

    def load_input(self, context: InputContext):
        """
        Not implemented here. If needed, you can gather all chunked parquet files
        and combine them into a single DataFrame or use some other read logic.
        """
        raise NotImplementedError(
            "[MediumSocrataPolarsParquetIOManager] load_input() not implemented."
        )


