"""Sports mappings domain client — typed read/write for entity mapping data."""

from __future__ import annotations

import logging

import pandas as pd
from unified_config_interface import UnifiedCloudConfig

from unified_domain_client.cloud_target import CloudTarget
from unified_domain_client.paths import build_bucket, build_path
from unified_domain_client.standardized_service import StandardizedDomainCloudService

logger = logging.getLogger(__name__)


class SportsMappingsDomainClient:
    """Client for cross-provider entity mappings partitioned by entity type."""

    def __init__(
        self,
        project_id: str | None = None,
        gcs_bucket: str | None = None,
    ) -> None:
        self._project_id = project_id or UnifiedCloudConfig().gcp_project_id
        bucket = gcs_bucket or build_bucket("sports_mappings", project_id=self._project_id)
        cloud_target = CloudTarget(
            project_id=self._project_id,
            gcs_bucket=bucket,
            bigquery_dataset="sports_mappings",
        )
        self.cloud_service = StandardizedDomainCloudService(domain="sports", cloud_target=cloud_target)

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
        return self.cloud_service.upload_to_gcs(data=df, gcs_path=path, format="parquet")
