"""Writer factory."""

from __future__ import annotations

from unified_cloud_interface import StorageClient

from unified_domain_client.writers.base import BaseDataWriter
from unified_domain_client.writers.direct import DirectWriter


def get_writer(dataset_name: str, storage_client: StorageClient | None = None) -> BaseDataWriter:
    """
    Get a data writer for the given dataset.

    Args:
        dataset_name: Dataset name (must be in PATH_REGISTRY)
        storage_client: UCLI StorageClient for direct writes
    """
    if storage_client is None:
        raise ValueError(f"storage_client required for get_writer('{dataset_name}')")
    return DirectWriter(storage_client)
