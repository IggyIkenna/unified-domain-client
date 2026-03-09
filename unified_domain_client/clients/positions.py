"""Positions domain client."""

from __future__ import annotations

import io
import logging
from functools import lru_cache

import pandas as pd
from unified_cloud_interface import get_storage_client
from unified_config_interface import UnifiedCloudConfig, build_bucket, build_path

from ..standardized_service import StandardizedDomainCloudService

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _get_cloud_config() -> UnifiedCloudConfig:
    """Return singleton UnifiedCloudConfig instance."""
    return UnifiedCloudConfig()


class PositionsDomainClient:
    """Client for reading position snapshots."""

    def __init__(
        self,
        project_id: str | None = None,
        storage_bucket: str | None = None,
    ) -> None:
        self._project_id = project_id or _get_cloud_config().gcp_project_id
        bucket = storage_bucket or build_bucket("positions", project_id=self._project_id)
        self.cloud_service = StandardizedDomainCloudService(domain="positions", bucket=bucket)
        self._bucket = bucket

    def get_positions(self, date: str, account_key: str, snapshot_type: str) -> pd.DataFrame:
        """Get position snapshot for a date, account, and snapshot type."""
        path = (
            build_path("positions", date=date, account_key=account_key, snapshot_type=snapshot_type)
            + "positions.parquet"
        )
        try:
            client = get_storage_client(project_id=self._project_id)
            raw = client.download_bytes(self._bucket, path)
            return pd.read_parquet(io.BytesIO(raw))
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to load positions %s/%s: %s", date, account_key, e)
            return pd.DataFrame()

    def get_available_accounts(self) -> list[str]:
        """List all account keys that have position snapshots."""
        try:
            client = get_storage_client(project_id=self._project_id)
            blobs = client.list_blobs(self._bucket, prefix="by_date/")
            accounts: set[str] = set()
            for blob in blobs:
                for part in blob.name.split("/"):
                    if part.startswith("account="):
                        accounts.add(part[8:])
            return sorted(accounts)
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to list accounts: %s", e)
            return []
