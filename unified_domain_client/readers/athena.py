"""AWS Athena reader."""

from __future__ import annotations

import logging

import pandas as pd

from .base import BaseDataReader

logger = logging.getLogger(__name__)


class AthenaReader(BaseDataReader):
    """Reads data via AWS Athena."""

    def __init__(self, account_id: str, glue_database: str, region: str = "us-east-1") -> None:
        self._account_id = account_id
        self._glue_database = glue_database
        self._region = region

    def read(self, bucket: str, path: str) -> pd.DataFrame:
        raise NotImplementedError(
            "AthenaReader.read() — implement with boto3 Athena client from UCLI"
        )

    def list_available(self, bucket: str, prefix: str) -> list[str]:
        raise NotImplementedError("AthenaReader.list_available() — implement with S3 list via UCLI")
