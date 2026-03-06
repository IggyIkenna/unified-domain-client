"""Sports features domain client — typed read/write for sports feature vectors."""

from __future__ import annotations

import logging

import pandas as pd
from unified_cloud_interface import get_storage_client
from unified_config_interface import UnifiedCloudConfig

from ..paths import build_bucket, build_path
from ..standardized_service import StandardizedDomainCloudService

logger = logging.getLogger(__name__)


class SportsFeaturesDomainClient:
    """Client for sports feature vectors partitioned by horizon/date/league."""

    def __init__(
        self,
        project_id: str | None = None,
        gcs_bucket: str | None = None,
    ) -> None:
        self._project_id = project_id or UnifiedCloudConfig().gcp_project_id
        bucket = gcs_bucket or build_bucket("sports_features", project_id=self._project_id)
        self.cloud_service = StandardizedDomainCloudService(domain="sports", bucket=bucket)

    def read_features(self, horizon: str, date: str, league: str) -> pd.DataFrame:
        """Read sports feature vectors for a specific horizon, date, and league.

        Args:
            horizon: Prediction horizon (e.g. "1d", "7d").
            date: Date string in YYYY-MM-DD format.
            league: League identifier (e.g. "epl", "nba").

        Returns:
            DataFrame of feature vectors, or empty DataFrame on failure.
        """
        path = build_path("sports_features", horizon=horizon, date=date, league=league) + "features.parquet"
        try:
            result = self.cloud_service.download_from_gcs(gcs_path=path, format="parquet")
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to read sports features: %s", e)
            return pd.DataFrame()

    def write_features(self, df: pd.DataFrame, horizon: str, date: str, league: str) -> str:
        """Write sports feature vectors for a specific horizon, date, and league.

        Args:
            df: DataFrame of feature vectors to upload.
            horizon: Prediction horizon (e.g. "1d", "7d").
            date: Date string in YYYY-MM-DD format.
            league: League identifier (e.g. "epl", "nba").

        Returns:
            GCS URI of the uploaded file.
        """
        path = build_path("sports_features", horizon=horizon, date=date, league=league) + "features.parquet"
        return self.cloud_service.upload_to_gcs(data=df, gcs_path=path, format="parquet")

    def get_available_dates(self, horizon: str, league: str) -> list[str]:
        """List dates that have sports features for a given horizon and league."""
        try:
            bucket_name = build_bucket("sports_features", project_id=self._project_id)
            client = get_storage_client(project_id=self._project_id)
            blobs = client.list_blobs(bucket_name, prefix=f"features/horizon={horizon}/")
            dates: list[str] = []
            for blob in blobs:
                if f"/league={league}/" in blob.name:
                    for part in blob.name.split("/"):
                        if part.startswith("date="):
                            dates.append(part[5:])
            return sorted(set(dates))
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to list sports feature dates: %s", e)
            return []
