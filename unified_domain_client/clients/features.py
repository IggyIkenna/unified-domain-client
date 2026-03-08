"""Features domain clients — one typed client per feature group."""

from __future__ import annotations

import logging
from functools import lru_cache

import pandas as pd
from unified_cloud_interface import get_storage_client
from unified_config_interface import UnifiedCloudConfig

from ..paths import build_bucket, build_path
from ..standardized_service import StandardizedDomainCloudService

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _get_cloud_config() -> UnifiedCloudConfig:
    """Return singleton UnifiedCloudConfig instance."""
    return UnifiedCloudConfig()


class FeaturesDeltaOneDomainClient:
    """Client for delta-one features (cross-sectional price/volume features)."""

    def __init__(
        self,
        project_id: str | None = None,
        storage_bucket: str | None = None,
        category: str = "cefi",
        analytics_dataset: str | None = None,
    ) -> None:
        self._category = category
        self._project_id = project_id or _get_cloud_config().gcp_project_id
        bucket = storage_bucket or build_bucket(
            "delta_one_features", project_id=self._project_id, category=category
        )
        self.cloud_service = StandardizedDomainCloudService(domain="features", bucket=bucket)

    def get_features(
        self,
        date: str,
        instrument_id: str,
        feature_group: str,
        timeframe: str,
    ) -> pd.DataFrame:
        """Get delta-one features for a date, instrument, feature group, and timeframe."""
        path = (
            build_path(
                "delta_one_features", date=date, feature_group=feature_group, timeframe=timeframe
            )
            + f"{instrument_id}.parquet"
        )
        try:
            result = self.cloud_service.download_from_gcs(gcs_path=path, format="parquet")
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to load delta-one features: %s", e)
            return pd.DataFrame()

    def get_available_dates(self, feature_group: str, timeframe: str) -> list[str]:
        """List dates that have delta-one features for this group and timeframe."""
        try:
            bucket_name = build_bucket(
                "delta_one_features", project_id=self._project_id, category=self._category
            )
            client = get_storage_client(project_id=self._project_id)
            blobs = client.list_blobs(bucket_name, prefix="by_date/")
            dates: list[str] = []
            for blob in blobs:
                if (
                    f"/feature_group={feature_group}/" in blob.name
                    and f"/timeframe={timeframe}/" in blob.name
                ):
                    for part in blob.name.split("/"):
                        if part.startswith("day="):
                            dates.append(part[4:])
            return sorted(set(dates))
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to list delta-one dates: %s", e)
            return []


class FeaturesCalendarDomainClient:
    """Client for calendar features (macro events, expiry calendars, trading sessions)."""

    def __init__(
        self,
        project_id: str | None = None,
        storage_bucket: str | None = None,
        analytics_dataset: str | None = None,
    ) -> None:
        self._project_id = project_id or _get_cloud_config().gcp_project_id
        bucket = storage_bucket or build_bucket("calendar_features", project_id=self._project_id)
        self.cloud_service = StandardizedDomainCloudService(domain="features", bucket=bucket)

    def get_features(self, date: str, category: str = "cefi") -> pd.DataFrame:
        """Get calendar features for a specific date.

        Path uses day={date} (equals sign), never YYYY/MM/DD.
        """
        path = build_path("calendar_features", category=category, date=date) + "features.parquet"
        try:
            result = self.cloud_service.download_from_gcs(gcs_path=path, format="parquet")
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to load calendar features: %s", e)
            return pd.DataFrame()

    def get_available_dates(self, category: str = "cefi") -> list[str]:
        """List dates that have calendar features for this category."""
        try:
            bucket_name = build_bucket("calendar_features", project_id=self._project_id)
            client = get_storage_client(project_id=self._project_id)
            blobs = client.list_blobs(bucket_name, prefix=f"calendar/{category}/by_date/")
            dates: list[str] = []
            for blob in blobs:
                for part in blob.name.split("/"):
                    if part.startswith("day="):
                        dates.append(part[4:])
            return sorted(set(dates))
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to list calendar dates: %s", e)
            return []


class FeaturesOnchainDomainClient:
    """Client for on-chain features (DeFi/blockchain metrics)."""

    def __init__(
        self,
        project_id: str | None = None,
        storage_bucket: str | None = None,
        analytics_dataset: str | None = None,
    ) -> None:
        self._project_id = project_id or _get_cloud_config().gcp_project_id
        bucket = storage_bucket or build_bucket("onchain_features", project_id=self._project_id)
        self.cloud_service = StandardizedDomainCloudService(domain="features", bucket=bucket)

    def get_features(self, date: str, feature_group: str) -> pd.DataFrame:
        """Get on-chain features for a specific date and feature group."""
        path = (
            build_path("onchain_features", date=date, feature_group=feature_group)
            + "features.parquet"
        )
        try:
            result = self.cloud_service.download_from_gcs(gcs_path=path, format="parquet")
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to load on-chain features: %s", e)
            return pd.DataFrame()

    def get_available_dates(self, feature_group: str) -> list[str]:
        """List dates that have on-chain features for this feature group."""
        try:
            bucket_name = build_bucket("onchain_features", project_id=self._project_id)
            client = get_storage_client(project_id=self._project_id)
            blobs = client.list_blobs(bucket_name, prefix="by_date/")
            dates: list[str] = []
            for blob in blobs:
                if f"/feature_group={feature_group}/" in blob.name:
                    for part in blob.name.split("/"):
                        if part.startswith("day="):
                            dates.append(part[4:])
            return sorted(set(dates))
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to list on-chain dates: %s", e)
            return []


class FeaturesVolatilityDomainClient:
    """Client for volatility surface features (options-derived vol metrics)."""

    def __init__(
        self,
        project_id: str | None = None,
        storage_bucket: str | None = None,
        category: str = "cefi",
        analytics_dataset: str | None = None,
    ) -> None:
        self._category = category
        self._project_id = project_id or _get_cloud_config().gcp_project_id
        bucket = storage_bucket or build_bucket(
            "volatility_features", project_id=self._project_id, category=category
        )
        self.cloud_service = StandardizedDomainCloudService(domain="features", bucket=bucket)

    def get_features(self, date: str, underlying: str, feature_group: str) -> pd.DataFrame:
        """Get volatility features for a specific date, underlying, and feature group."""
        path = (
            build_path("volatility_features", date=date, feature_group=feature_group)
            + f"{underlying}.parquet"
        )
        try:
            result = self.cloud_service.download_from_gcs(gcs_path=path, format="parquet")
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to load volatility features: %s", e)
            return pd.DataFrame()

    def get_available_dates(self, feature_group: str) -> list[str]:
        """List dates that have volatility features for this feature group."""
        try:
            bucket_name = build_bucket(
                "volatility_features", project_id=self._project_id, category=self._category
            )
            client = get_storage_client(project_id=self._project_id)
            blobs = client.list_blobs(bucket_name, prefix="by_date/")
            dates: list[str] = []
            for blob in blobs:
                if f"/feature_group={feature_group}/" in blob.name:
                    for part in blob.name.split("/"):
                        if part.startswith("day="):
                            dates.append(part[4:])
            return sorted(set(dates))
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to list volatility dates: %s", e)
            return []
