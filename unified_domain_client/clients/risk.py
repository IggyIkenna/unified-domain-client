"""Risk metrics domain client."""

from __future__ import annotations

import io
import logging

import pandas as pd
from unified_cloud_interface import get_storage_client
from unified_config_interface import UnifiedCloudConfig

from ..paths import build_bucket, build_path
from ..standardized_service import StandardizedDomainCloudService

logger = logging.getLogger(__name__)


class RiskDomainClient:
    """Client for reading risk metrics snapshots."""

    def __init__(
        self,
        project_id: str | None = None,
        storage_bucket: str | None = None,
    ) -> None:
        self._project_id = project_id or UnifiedCloudConfig().gcp_project_id
        bucket = storage_bucket or build_bucket("risk_metrics", project_id=self._project_id)
        self.cloud_service = StandardizedDomainCloudService(domain="risk", bucket=bucket)
        self._bucket = bucket

    def get_risk_metrics(self, date: str, risk_type: str) -> pd.DataFrame:
        """Get risk metrics for a specific date and risk type."""
        path = build_path("risk_metrics", date=date, risk_type=risk_type) + "risk_metrics.parquet"
        try:
            client = get_storage_client(project_id=self._project_id)
            raw = client.download_bytes(self._bucket, path)
            return pd.read_parquet(io.BytesIO(raw))
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to load risk metrics %s/%s: %s", date, risk_type, e)
            return pd.DataFrame()

    def get_available_risk_types(self) -> list[str]:
        """List all risk types that have stored metrics."""
        try:
            client = get_storage_client(project_id=self._project_id)
            blobs = client.list_blobs(self._bucket, prefix="by_date/")
            risk_types: set[str] = set()
            for blob in blobs:
                for part in blob.name.split("/"):
                    if part.startswith("risk_type="):
                        risk_types.add(part[10:])
            return sorted(risk_types)
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to list risk types: %s", e)
            return []
