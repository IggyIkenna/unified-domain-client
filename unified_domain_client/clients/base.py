"""Base domain client for all domain data access."""

from __future__ import annotations

import io
import logging
from abc import ABC

import pandas as pd
from unified_cloud_interface import StorageClient
from unified_config_interface import UnifiedCloudConfig

logger = logging.getLogger(__name__)


class BaseDataClient(ABC):
    """Abstract base for typed domain data clients using StorageClient injection."""

    def __init__(self, storage_client: StorageClient, config: UnifiedCloudConfig) -> None:
        self._storage = storage_client
        self._config = config

    def _read_parquet(self, bucket: str, path: str) -> pd.DataFrame:
        """Download a parquet blob and return as DataFrame."""
        data = self._storage.download_bytes(bucket, path)
        return pd.read_parquet(io.BytesIO(data))

    def _list_blobs(self, bucket: str, prefix: str) -> list[str]:
        """List blob names under a prefix."""
        return [b.name for b in self._storage.list_blobs(bucket, prefix=prefix)]
