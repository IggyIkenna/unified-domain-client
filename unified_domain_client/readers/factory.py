"""Reader factory."""

from __future__ import annotations

from unified_cloud_interface import StorageClient

from ..paths import ReadMode
from .athena import AthenaReader
from .base import BaseDataReader
from .bq_external import BigQueryExternalReader
from .direct import DirectReader


def get_reader(
    storage_client: StorageClient | None = None,
    mode: ReadMode = ReadMode.AUTO,
    **kwargs: object,
) -> BaseDataReader:
    """
    Get a data reader for the specified mode.

    Args:
        storage_client: UCLI StorageClient (required for AUTO mode)
        mode: ReadMode.AUTO (direct), ReadMode.BQ_EXTERNAL, or ReadMode.ATHENA
        **kwargs: Additional args for BQ/Athena readers (project_id, bq_dataset, etc.)
    """
    if mode == ReadMode.AUTO:
        if storage_client is None:
            raise ValueError("storage_client required for ReadMode.AUTO")
        return DirectReader(storage_client)
    if mode == ReadMode.BQ_EXTERNAL:
        project_id_raw = kwargs.get("project_id")
        if not project_id_raw:
            raise ValueError("project_id is required for ReadMode.BQ_EXTERNAL")
        bq_dataset_raw = kwargs.get("bq_dataset")
        if not bq_dataset_raw:
            raise ValueError("bq_dataset is required for ReadMode.BQ_EXTERNAL")
        return BigQueryExternalReader(project_id=str(project_id_raw), bq_dataset=str(bq_dataset_raw))
    if mode == ReadMode.ATHENA:
        account_id_raw = kwargs.get("account_id")
        if not account_id_raw:
            raise ValueError("account_id is required for ReadMode.ATHENA")
        glue_database_raw = kwargs.get("glue_database")
        if not glue_database_raw:
            raise ValueError("glue_database is required for ReadMode.ATHENA")
        region_raw = kwargs.get("region", "us-east-1")
        return AthenaReader(
            account_id=str(account_id_raw), glue_database=str(glue_database_raw), region=str(region_raw)
        )
    raise ValueError(f"Unknown ReadMode: {mode}")
