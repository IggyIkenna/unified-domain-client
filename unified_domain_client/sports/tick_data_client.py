"""Sports tick data domain client — typed read/write for sports tick data."""

from __future__ import annotations

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


class SportsTickDataDomainClient:
    """Client for sports tick data partitioned by venue/date."""

    def __init__(
        self,
        project_id: str | None = None,
        storage_bucket: str | None = None,
    ) -> None:
        self._project_id = project_id or _get_cloud_config().gcp_project_id
        bucket = storage_bucket or build_bucket("sports_tick_data", project_id=self._project_id)
        self.cloud_service = StandardizedDomainCloudService(domain="sports", bucket=bucket)

    def read_ticks(self, venue: str, date: str) -> pd.DataFrame:
        """Read tick data for a specific venue and date.

        Args:
            venue: Betting venue/exchange (e.g. "betfair", "pinnacle").
            date: Date string in YYYY-MM-DD format.

        Returns:
            DataFrame of tick data, or empty DataFrame on failure.
        """
        path = build_path("sports_tick_data", venue=venue, date=date) + "ticks.parquet"
        try:
            result = self.cloud_service.download_from_gcs(gcs_path=path, format="parquet")
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to read sports tick data: %s", e)
            return pd.DataFrame()

    def write_ticks(self, df: pd.DataFrame, venue: str, date: str) -> str:
        """Write tick data for a specific venue and date.

        Args:
            df: DataFrame of tick data to upload.
            venue: Betting venue/exchange (e.g. "betfair", "pinnacle").
            date: Date string in YYYY-MM-DD format.

        Returns:
            GCS URI of the uploaded file.
        """
        path = build_path("sports_tick_data", venue=venue, date=date) + "ticks.parquet"
        return self.cloud_service.upload_artifact(df, path, format="parquet")

    def get_available_dates(self, venue: str) -> list[str]:
        """List dates that have tick data for a given venue."""
        try:
            bucket_name = build_bucket("sports_tick_data", project_id=self._project_id)
            client = get_storage_client(project_id=self._project_id)
            blobs = client.list_blobs(bucket_name, prefix=f"sports/venue={venue}/")
            dates: list[str] = []
            for blob in blobs:
                for part in blob.name.split("/"):
                    if part.startswith("date="):
                        dates.append(part[5:])
            return sorted(set(dates))
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to list sports tick data dates: %s", e)
            return []
