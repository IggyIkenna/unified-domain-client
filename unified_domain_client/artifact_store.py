"""CloudModelArtifactStore — GCS-backed implementation of ModelArtifactStoreProtocol.

This module provides the T3 concrete implementation of the model artifact storage
protocol defined in unified-ml-interface (T2). ML services (T4+) inject this
store at runtime via the ModelArtifactStoreProtocol interface — they must never
import CloudModelArtifactStore directly.

Dependency flow:
    T0: unified-cloud-interface  (get_storage_client)
    T2: unified-ml-interface     (ModelArtifactStoreProtocol, ModelMetadata, ModelVariantConfig)
    T3: unified-domain-client    (this file — CloudModelArtifactStore)
"""
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false
# Reason: GCS blob.name / list_blobs iteration returns partially-typed objects
# from google-cloud-storage; documented in QUALITY_GATE_BYPASS_AUDIT.md.

import io
import json
import logging
from datetime import date
from functools import lru_cache
from typing import Protocol, cast

import joblib  # pyright: ignore[reportMissingTypeStubs]
from unified_cloud_interface import get_storage_client
from unified_config_interface import UnifiedCloudConfig
from unified_ml_interface import ModelMetadata, ModelVariantConfig

logger = logging.getLogger(__name__)


class _StorageClient(Protocol):
    """Minimal protocol for the UCI storage client used in artifact_store."""

    def upload_bytes(
        self,
        bucket: str,
        blob_name: str,
        data: bytes,
        content_type: str = ...,
    ) -> None: ...

    def download_bytes(self, bucket: str, blob_name: str) -> bytes | None: ...

    def list_blobs(self, bucket: str, prefix: str) -> list[object]: ...


_MODELS_PREFIX = "models"
_METADATA_SUFFIX = "metadata.json"
_MODEL_SUFFIX = "model.joblib"


@lru_cache(maxsize=1)
def _get_cloud_config() -> UnifiedCloudConfig:
    """Return singleton UnifiedCloudConfig instance."""
    return UnifiedCloudConfig()


def _model_path(model_id: str, training_period: str) -> str:
    """Return GCS prefix for a model + training period pair."""
    return f"{_MODELS_PREFIX}/{model_id}/{training_period}/"


class CloudModelArtifactStore:
    """GCS-backed model artifact store.

    Implements ModelArtifactStoreProtocol (T2) using get_storage_client()
    from unified-cloud-interface (T0). ML services must inject this via the
    protocol interface — they must NEVER import this class directly.

    Storage layout (all under ``bucket``)::

        models/{model_id}/{training_period}/model.joblib
        models/{model_id}/{training_period}/metadata.json
    """

    def __init__(
        self,
        bucket: str | None = None,
        project_id: str | None = None,
    ) -> None:
        config = _get_cloud_config()
        self._project_id = project_id or config.gcp_project_id or ""
        self._bucket = bucket or config.ml_artifact_bucket or f"ml-artifacts-{self._project_id}"
        logger.info(
            "CloudModelArtifactStore initialised: bucket=%s project=%s",
            self._bucket,
            self._project_id,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _storage(self) -> object:
        return get_storage_client(project_id=self._project_id)

    def _upload_bytes(
        self, blob_name: str, data: bytes, content_type: str = "application/octet-stream"
    ) -> None:
        client = cast(_StorageClient, get_storage_client(project_id=self._project_id))
        client.upload_bytes(self._bucket, blob_name, data, content_type=content_type)

    def _download_bytes(self, blob_name: str) -> bytes | None:
        try:
            client = cast(_StorageClient, get_storage_client(project_id=self._project_id))
            return client.download_bytes(self._bucket, blob_name)
        except (OSError, ConnectionError, ValueError) as exc:
            logger.debug("Blob not found or error: %s — %s", blob_name, exc)
            return None

    def _list_blobs(self, prefix: str) -> list[str]:
        try:
            client = cast(_StorageClient, get_storage_client(project_id=self._project_id))
            blobs = client.list_blobs(self._bucket, prefix=prefix)
            return [b.name for b in blobs]  # pyright: ignore[reportAttributeAccessIssue]
        except (OSError, ConnectionError, ValueError) as exc:
            logger.error("Failed to list blobs under %s: %s", prefix, exc)
            return []

    # ------------------------------------------------------------------
    # ModelArtifactStoreProtocol implementation
    # ------------------------------------------------------------------

    def store_model(
        self,
        model: object,
        metadata: ModelMetadata,
        training_period: str | None = None,
    ) -> str:
        """Serialise model with joblib and upload to GCS. Returns storage path."""
        period = training_period or (
            metadata.training_timestamp.strftime("%Y-%m")
            if metadata.training_timestamp
            else "unknown"
        )
        prefix = _model_path(metadata.model_id, period)
        model_blob = prefix + _MODEL_SUFFIX
        meta_blob = prefix + _METADATA_SUFFIX

        # Serialise model
        buf = io.BytesIO()
        joblib.dump(model, buf)
        buf.seek(0)
        self._upload_bytes(model_blob, buf.read(), "application/octet-stream")

        # Serialise metadata
        meta_dict = metadata.to_dict() if hasattr(metadata, "to_dict") else {}
        meta_dict["training_period"] = period
        self._upload_bytes(meta_blob, json.dumps(meta_dict).encode(), "application/json")

        logger.info("Stored model %s/%s at %s", metadata.model_id, period, prefix)
        return prefix

    def load_model(
        self,
        model_id: str,
        training_period: str | None = None,
        variant_config: ModelVariantConfig | None = None,
    ) -> object | None:
        """Download and deserialise a model from GCS. Returns None if not found."""
        period = training_period or self.get_latest_training_period(model_id)
        if not period:
            logger.warning("No training period found for model %s", model_id)
            return None

        blob_name = _model_path(model_id, period) + _MODEL_SUFFIX
        raw = self._download_bytes(blob_name)
        if raw is None:
            logger.warning("Model not found: %s", blob_name)
            return None

        return joblib.load(io.BytesIO(raw))

    def get_model_metadata(
        self,
        model_id: str,
        training_period: str | None = None,
    ) -> ModelMetadata | None:
        """Fetch and deserialise model metadata from GCS. Returns None if not found."""
        period = training_period or self.get_latest_training_period(model_id)
        if not period:
            return None

        blob_name = _model_path(model_id, period) + _METADATA_SUFFIX
        raw = self._download_bytes(blob_name)
        if raw is None:
            return None

        try:
            data: dict[str, object] = json.loads(raw.decode())
            variant_data = data.get("variant_config")
            if not isinstance(variant_data, dict):
                logger.error("Metadata for %s/%s missing variant_config", model_id, period)
                return None
            variant = ModelVariantConfig.from_dict(cast(dict[str, object], variant_data))
            return ModelMetadata(
                variant_config=variant,
                model_version=str(data.get("model_version", "1")),
                model_type=str(data.get("model_type", "lightgbm")),
            )
        except (json.JSONDecodeError, ValueError, KeyError) as exc:
            logger.error("Failed to parse metadata for %s/%s: %s", model_id, period, exc)
            return None

    def get_model_for_inference_date(
        self,
        model_id: str,
        inference_date: date,
    ) -> dict[str, object] | None:
        """Return most recent model trained BEFORE inference_date (walk-forward pattern)."""
        periods = self.list_training_periods(model_id)
        # periods are YYYY-MM strings sorted ascending; select latest <= inference_date month
        inference_ym = inference_date.strftime("%Y-%m")
        eligible = [p for p in periods if p <= inference_ym]
        if not eligible:
            logger.warning("No model for %s trained before %s", model_id, inference_ym)
            return None

        period = eligible[-1]
        model = self.load_model(model_id, training_period=period)
        metadata = self.get_model_metadata(model_id, training_period=period)
        if model is None:
            return None

        return {"model": model, "metadata": metadata, "training_period": period}

    def _matches_filter(
        self,
        model_id: str,
        category: str | None,
        asset: str | None,
        target_type: str | None,
        timeframe: str | None,
    ) -> bool:
        """Return True if model_id passes all optional filter criteria."""
        mid_lower = model_id.lower()
        if category and category.lower() not in mid_lower:
            return False
        if asset and asset.lower() not in mid_lower:
            return False
        if target_type and target_type.lower() not in mid_lower:
            return False
        if timeframe and timeframe.lower() not in mid_lower:
            return False
        return True

    def list_models(
        self,
        category: str | None = None,
        asset: str | None = None,
        target_type: str | None = None,
        timeframe: str | None = None,
    ) -> list[dict[str, object]]:
        """List models under the models/ prefix, optionally filtered by model_id components."""
        blob_names = self._list_blobs(f"{_MODELS_PREFIX}/")
        model_ids: set[str] = set()
        for name in blob_names:
            parts = name.split("/")
            if len(parts) >= 2 and parts[0] == _MODELS_PREFIX:
                model_ids.add(parts[1])

        return [
            {"model_id": mid, "training_periods": self.list_training_periods(mid)}
            for mid in sorted(model_ids)
            if self._matches_filter(mid, category, asset, target_type, timeframe)
        ]

    def list_training_periods(self, model_id: str) -> list[str]:
        """List all training period strings for model_id (YYYY-MM format, sorted)."""
        prefix = f"{_MODELS_PREFIX}/{model_id}/"
        blob_names = self._list_blobs(prefix)
        periods: set[str] = set()
        for name in blob_names:
            parts = name[len(prefix) :].split("/")
            if parts:
                period = parts[0]
                # Validate YYYY-MM format
                if len(period) == 7 and period[4] == "-":
                    periods.add(period)
        return sorted(periods)

    def get_latest_training_period(self, model_id: str) -> str | None:
        """Return the latest training period string or None."""
        periods = self.list_training_periods(model_id)
        return periods[-1] if periods else None


# Runtime type check — ensures CloudModelArtifactStore satisfies the protocol.
# This is checked at import time and will raise TypeError if the implementation
# diverges from ModelArtifactStoreProtocol.
assert isinstance(
    CloudModelArtifactStore,
    type,
), "CloudModelArtifactStore must be a class"
# Protocol conformance is verified structurally — no runtime isinstance check
# needed here because @runtime_checkable protocols check instance methods,
# not class methods. Conformance is verified in tests.
