"""BigQuery external table reader."""

from __future__ import annotations

import logging

import pandas as pd

from unified_domain_client.readers.base import BaseDataReader

logger = logging.getLogger(__name__)


class BigQueryExternalReader(BaseDataReader):
    """Reads data via BigQuery external table over GCS."""

    def __init__(self, project_id: str, bq_dataset: str) -> None:
        self._project_id = project_id
        self._bq_dataset = bq_dataset

    def read(self, bucket: str, path: str) -> pd.DataFrame:
        raise NotImplementedError("BigQueryExternalReader.read() — implement with BQ client from UCLI")

    def list_available(self, bucket: str, prefix: str) -> list[str]:
        raise NotImplementedError("BigQueryExternalReader.list_available() — implement with BQ client")
