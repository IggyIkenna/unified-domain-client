"""Sports fixtures domain client — typed read/write for canonical fixture data."""

from __future__ import annotations

import logging

import pandas as pd
from unified_cloud_interface import get_storage_client
from unified_config_interface import UnifiedCloudConfig

from ..paths import build_bucket, build_path
from ..standardized_service import StandardizedDomainCloudService

logger = logging.getLogger(__name__)


class SportsFixturesDomainClient:
    """Client for canonical sports fixtures partitioned by season/league/date."""

    def __init__(
        self,
        project_id: str | None = None,
        storage_bucket: str | None = None,
    ) -> None:
        self._project_id = project_id or UnifiedCloudConfig().gcp_project_id
        bucket = storage_bucket or build_bucket("sports_fixtures", project_id=self._project_id)
        self.cloud_service = StandardizedDomainCloudService(domain="sports", bucket=bucket)

    def read_fixtures(self, season: str, league: str, date: str) -> pd.DataFrame:
        """Read canonical fixture data for a specific season, league, and date.

        Args:
            season: Season identifier (e.g. "2025-2026").
            league: League identifier (e.g. "epl", "nba").
            date: Date string in YYYY-MM-DD format.

        Returns:
            DataFrame of fixture data, or empty DataFrame on failure.
        """
        path = (
            build_path("sports_fixtures", season=season, league=league, date=date)
            + "fixtures.parquet"
        )
        try:
            result = self.cloud_service.download_from_gcs(gcs_path=path, format="parquet")
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to read sports fixtures: %s", e)
            return pd.DataFrame()

    def write_fixtures(self, df: pd.DataFrame, season: str, league: str, date: str) -> str:
        """Write canonical fixture data for a specific season, league, and date.

        Args:
            df: DataFrame of fixture data to upload.
            season: Season identifier (e.g. "2025-2026").
            league: League identifier (e.g. "epl", "nba").
            date: Date string in YYYY-MM-DD format.

        Returns:
            GCS URI of the uploaded file.
        """
        path = (
            build_path("sports_fixtures", season=season, league=league, date=date)
            + "fixtures.parquet"
        )
        return self.cloud_service.upload_artifact(df, path, format="parquet")

    def get_available_dates(self, season: str, league: str) -> list[str]:
        """List dates that have fixture data for a given season and league."""
        try:
            bucket_name = build_bucket("sports_fixtures", project_id=self._project_id)
            client = get_storage_client(project_id=self._project_id)
            blobs = client.list_blobs(
                bucket_name, prefix=f"fixtures/season={season}/league={league}/"
            )
            dates: list[str] = []
            for blob in blobs:
                for part in blob.name.split("/"):
                    if part.startswith("date="):
                        dates.append(part[5:])
            return sorted(set(dates))
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to list sports fixture dates: %s", e)
            return []
