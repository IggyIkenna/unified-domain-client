"""PnL attribution domain client."""

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


class PnLDomainClient:
    """Client for reading PnL attribution data."""

    def __init__(
        self,
        project_id: str | None = None,
        storage_bucket: str | None = None,
    ) -> None:
        self._project_id = project_id or _get_cloud_config().gcp_project_id
        bucket = storage_bucket or build_bucket("pnl_attribution", project_id=self._project_id)
        self.cloud_service = StandardizedDomainCloudService(domain="pnl", bucket=bucket)
        self._bucket = bucket

    def get_pnl_attribution(self, date: str, strategy_id: str) -> pd.DataFrame:
        """Get PnL attribution for a specific date and strategy."""
        path = (
            build_path("pnl_attribution", date=date, strategy_id=strategy_id)
            + "pnl_attribution.parquet"
        )
        try:
            client = get_storage_client(project_id=self._project_id)
            raw = client.download_bytes(self._bucket, path)
            return pd.read_parquet(io.BytesIO(raw))
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to load PnL attribution %s/%s: %s", date, strategy_id, e)
            return pd.DataFrame()

    def get_available_strategies(self) -> list[str]:
        """List all strategy IDs that have PnL attribution data."""
        try:
            client = get_storage_client(project_id=self._project_id)
            blobs = client.list_blobs(self._bucket, prefix="by_date/")
            strategies: set[str] = set()
            for blob in blobs:
                for part in blob.name.split("/"):
                    if part.startswith("strategy_id="):
                        strategies.add(part[12:])
            return sorted(strategies)
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to list strategies: %s", e)
            return []
