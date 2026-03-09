"""ML domain clients — models store and predictions."""

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


class MLModelsDomainClient:
    """Client for reading trained ML models and their metadata."""

    def __init__(
        self,
        project_id: str | None = None,
        storage_bucket: str | None = None,
    ) -> None:
        self._project_id = project_id or _get_cloud_config().gcp_project_id
        bucket = storage_bucket or build_bucket("ml_models", project_id=self._project_id)
        self.cloud_service = StandardizedDomainCloudService(domain="ml_models", bucket=bucket)
        self._bucket = bucket

    def get_model(self, model_id: str, training_period: str) -> bytes:
        """Download a serialised model artifact (joblib bytes)."""
        path = (
            build_path("ml_models", model_id=model_id, training_period=training_period)
            + "model.joblib"
        )
        try:
            raw = self.cloud_service.download_from_gcs(gcs_path=path, format="bytes")
            return raw if isinstance(raw, bytes) else b""
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to load model %s/%s: %s", model_id, training_period, e)
            return b""

    def get_metadata(self, model_id: str, training_period: str) -> dict[str, str | int | float]:
        """Get model metadata JSON."""
        path = (
            build_path("ml_model_metadata", model_id=model_id, training_period=training_period)
            + "metadata.json"
        )
        try:
            raw = self.cloud_service.download_from_gcs(gcs_path=path, format="json")
            if isinstance(raw, dict):
                return {k: v for k, v in raw.items() if isinstance(v, (str, int, float))}
            return {}
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to load metadata %s/%s: %s", model_id, training_period, e)
            return {}

    def list_models(self) -> list[str]:
        """List all model IDs in the models store."""
        try:
            client = get_storage_client(project_id=self._project_id)
            blobs = client.list_blobs(self._bucket, prefix="models/")
            model_ids: set[str] = set()
            for blob in blobs:
                parts = blob.name.split("/")
                if len(parts) >= 2 and parts[0] == "models":
                    model_ids.add(parts[1])
            return sorted(model_ids)
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to list models: %s", e)
            return []


class MLPredictionsDomainClient:
    """Client for reading ML predictions.

    Paths use day={date}/mode={mode}/ partition keys — never YYYY/MM/DD.
    """

    def __init__(
        self,
        project_id: str | None = None,
        storage_bucket: str | None = None,
    ) -> None:
        self._project_id = project_id or _get_cloud_config().gcp_project_id
        bucket = storage_bucket or build_bucket("ml_predictions", project_id=self._project_id)
        self.cloud_service = StandardizedDomainCloudService(domain="ml_predictions", bucket=bucket)
        self._bucket = bucket

    def get_predictions(self, date: str, mode: str) -> pd.DataFrame:
        """Get predictions for a date and mode (batch | live).

        Reads all parquet files under predictions/by_date/day={date}/mode={mode}/.
        """
        prefix = build_path("ml_predictions", date=date, mode=mode)
        try:
            client = get_storage_client(project_id=self._project_id)
            blobs = [
                b
                for b in client.list_blobs(self._bucket, prefix=prefix)
                if b.name.endswith(".parquet")
            ]
            if not blobs:
                return pd.DataFrame()

            dfs: list[pd.DataFrame] = []
            for blob in blobs:
                raw = client.download_bytes(self._bucket, blob.name)
                dfs.append(pd.read_parquet(io.BytesIO(raw)))
            return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to load predictions %s/%s: %s", date, mode, e)
            return pd.DataFrame()

    def get_available_dates(self, mode: str) -> list[str]:
        """List dates for which predictions exist for the given mode."""
        try:
            client = get_storage_client(project_id=self._project_id)
            blobs = client.list_blobs(self._bucket, prefix="predictions/by_date/")
            dates: list[str] = []
            for blob in blobs:
                if f"/mode={mode}/" in blob.name:
                    for part in blob.name.split("/"):
                        if part.startswith("day="):
                            dates.append(part[4:])
            return sorted(set(dates))
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to list prediction dates: %s", e)
            return []
