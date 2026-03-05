"""Direct (UCLI StorageClient) reader."""

from __future__ import annotations

import io
import logging

import pandas as pd
from unified_cloud_interface import StorageClient

from unified_domain_client.readers.base import BaseDataReader

logger = logging.getLogger(__name__)


class DirectReader(BaseDataReader):
    """Reads data directly via UCLI StorageClient."""

    def __init__(self, storage_client: StorageClient) -> None:
        self._storage = storage_client

    def read(self, bucket: str, path: str) -> pd.DataFrame:
        data = self._storage.download_bytes(bucket, path)
        return pd.read_parquet(io.BytesIO(data))

    def list_available(self, bucket: str, prefix: str) -> list[str]:
        return [blob.name for blob in self._storage.list_blobs(bucket, prefix)]
