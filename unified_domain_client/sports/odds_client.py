"""Sports odds domain client — typed read/write for raw odds data."""

from __future__ import annotations

import logging

import pandas as pd
from unified_cloud_interface import get_storage_client
from unified_config_interface import UnifiedCloudConfig

from unified_domain_client.cloud_target import CloudTarget
from unified_domain_client.paths import build_bucket, build_path
from unified_domain_client.standardized_service import StandardizedDomainCloudService

logger = logging.getLogger(__name__)


class SportsOddsDomainClient:
    """Client for raw sports odds partitioned by provider/league/date."""

    def __init__(
        self,
        project_id: str | None = None,
        gcs_bucket: str | None = None,
    ) -> None:
        self._project_id = project_id or UnifiedCloudConfig().gcp_project_id
        bucket = gcs_bucket or build_bucket("sports_raw_odds", project_id=self._project_id)
        cloud_target = CloudTarget(
            project_id=self._project_id,
            gcs_bucket=bucket,
            bigquery_dataset="sports_raw_odds",
        )
        self.cloud_service = StandardizedDomainCloudService(domain="sports", cloud_target=cloud_target)

    def read_odds(self, provider: str, league: str, date: str) -> pd.DataFrame:
        """Read raw odds data for a specific provider, league, and date.

        Args:
            provider: Odds provider key (e.g. "pinnacle", "betfair").
            league: League identifier (e.g. "epl").
            date: Date string in YYYY-MM-DD format.

        Returns:
            DataFrame of odds data, or empty DataFrame on failure.
        """
        path = build_path("sports_raw_odds", provider=provider, league=league, date=date) + "odds.parquet"
        try:
            result = self.cloud_service.download_from_gcs(gcs_path=path, format="parquet")
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to read sports odds: %s", e)
            return pd.DataFrame()

    def write_odds(self, df: pd.DataFrame, provider: str, league: str, date: str) -> str:
        """Write raw odds data for a specific provider, league, and date.

        Args:
            df: DataFrame of odds data to upload.
            provider: Odds provider key (e.g. "pinnacle", "betfair").
            league: League identifier (e.g. "epl").
            date: Date string in YYYY-MM-DD format.

        Returns:
            GCS URI of the uploaded file.
        """
        path = build_path("sports_raw_odds", provider=provider, league=league, date=date) + "odds.parquet"
        return self.cloud_service.upload_to_gcs(data=df, gcs_path=path, format="parquet")

    def get_available_dates(self, provider: str, league: str) -> list[str]:
        """List dates that have odds data for a given provider and league."""
        try:
            bucket_name = build_bucket("sports_raw_odds", project_id=self._project_id)
            client = get_storage_client(project_id=self._project_id)
            blobs = client.list_blobs(bucket_name, prefix=f"raw_odds/provider={provider}/league={league}/")
            dates: list[str] = []
            for blob in blobs:
                for part in blob.name.split("/"):
                    if part.startswith("date="):
                        dates.append(part[5:])
            return sorted(set(dates))
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to list sports odds dates: %s", e)
            return []
