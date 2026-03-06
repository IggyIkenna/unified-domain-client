"""Direct (UCLI StorageClient) writer."""

from __future__ import annotations

import io
import json
import logging

import pandas as pd
from unified_cloud_interface import StorageClient

from .base import BaseDataWriter

logger = logging.getLogger(__name__)


class DirectWriter(BaseDataWriter):
    """Writes data directly via UCLI StorageClient."""

    def __init__(self, storage_client: StorageClient) -> None:
        self._storage = storage_client

    def write(self, df: pd.DataFrame, bucket: str, path: str) -> None:
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        self._storage.upload_bytes(bucket, path, buf.read())

    def write_json(self, data: dict[str, object], bucket: str, path: str) -> None:
        content = json.dumps(data, default=str).encode("utf-8")
        self._storage.upload_bytes(bucket, path, content)
