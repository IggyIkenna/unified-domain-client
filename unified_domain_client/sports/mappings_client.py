"""Sports mappings domain client — typed read/write for entity mapping data."""

from __future__ import annotations

import logging
from functools import lru_cache

import pandas as pd
from unified_config_interface import UnifiedCloudConfig, build_bucket, build_path

from ..standardized_service import StandardizedDomainCloudService

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _get_cloud_config() -> UnifiedCloudConfig:
    """Return singleton UnifiedCloudConfig instance."""
    return UnifiedCloudConfig()


class SportsMappingsDomainClient:
    """Client for cross-provider entity mappings partitioned by entity type."""

    def __init__(
        self,
        project_id: str | None = None,
        storage_bucket: str | None = None,
    ) -> None:
        self._project_id = project_id or _get_cloud_config().gcp_project_id
        bucket = storage_bucket or build_bucket("sports_mappings", project_id=self._project_id)
        self.cloud_service = StandardizedDomainCloudService(domain="sports", bucket=bucket)

    def read_mappings(self, entity_type: str) -> pd.DataFrame:
        """Read entity mappings for a specific entity type.

        Args:
            entity_type: Type of entity (e.g. "teams", "fixtures", "players").

        Returns:
            DataFrame of mapping data, or empty DataFrame on failure.
        """
        path = build_path("sports_mappings", entity_type=entity_type) + "mappings.parquet"
        try:
            result = self.cloud_service.download_from_gcs(gcs_path=path, format="parquet")
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to read sports mappings: %s", e)
            return pd.DataFrame()

    def write_mappings(self, df: pd.DataFrame, entity_type: str) -> str:
        """Write entity mappings for a specific entity type.

        Args:
            df: DataFrame of mapping data to upload.
            entity_type: Type of entity (e.g. "teams", "fixtures", "players").

        Returns:
            GCS URI of the uploaded file.
        """
        path = build_path("sports_mappings", entity_type=entity_type) + "mappings.parquet"
        return self.cloud_service.upload_artifact(df, path, format="parquet")
