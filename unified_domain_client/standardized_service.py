"""Standardized Domain Cloud Service.

Cloud-agnostic: uses unified_cloud_interface for all storage I/O.
Takes bucket: str directly — no GCS-specific CloudTarget wrapper.
"""

import io
import json
import logging
from typing import cast

import pandas as pd
from unified_cloud_interface import (
    download_from_storage,
    upload_to_storage,
)

logger = logging.getLogger(__name__)


class StandardizedDomainCloudService:
    """Domain cloud service using UCI for storage operations."""

    def __init__(self, domain: str, bucket: str) -> None:
        self.domain: str = domain
        self.bucket: str = bucket

    def download_from_gcs(
        self,
        gcs_path: str,
        format: str = "parquet",
        log_errors: bool = True,
    ) -> pd.DataFrame | dict[str, object]:
        """Download data from cloud storage."""
        path = gcs_path.lstrip("/")
        try:
            data = download_from_storage(self.bucket, path)
            if format == "parquet":
                return pd.read_parquet(io.BytesIO(data))
            if format == "csv":
                return pd.read_csv(io.BytesIO(data))
            if format == "json":
                return cast("dict[str, object]", json.loads(data.decode("utf-8")))
            if log_errors:
                logger.warning("Unknown format %s, returning empty DataFrame", format)
            return pd.DataFrame()
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            if log_errors:
                logger.error("Failed to download %s/%s: %s", self.bucket, path, e)
            raise

    def upload_artifact(
        self,
        data: pd.DataFrame,
        gcs_path: str,
        format: str = "parquet",
    ) -> str:
        """Upload data to cloud storage."""
        path = gcs_path.lstrip("/")
        if format == "parquet":
            buf = io.BytesIO()
            data.to_parquet(buf, index=False)
            _ = buf.seek(0)
            raw = buf.read()
        elif format == "csv":
            raw = data.to_csv(index=False).encode("utf-8")
        else:
            raise ValueError("Unsupported format: " + format)
        return upload_to_storage(self.bucket, path, raw)

    def query_bigquery(
        self,
        query: str,
        parameters: dict[str, object] | None = None,
    ) -> pd.DataFrame:
        _ = (query, parameters)
        raise NotImplementedError(
            "BigQuery is not supported in StandardizedDomainCloudService. "
            + "Use get_analytics_client() from unified_cloud_interface.factory in the service layer."  # noqa: E501
        )


def create_domain_cloud_service(
    domain: str,
    bucket: "str | object" = "",
) -> StandardizedDomainCloudService:
    """Factory for StandardizedDomainCloudService.

    ``bucket`` may be a plain str or a CloudTarget-like object with a
    ``storage_bucket`` attribute (backward compatibility).
    """
    if isinstance(bucket, str):
        resolved = bucket
    else:
        # CloudTarget or any object with storage_bucket attribute
        resolved = getattr(bucket, "storage_bucket", "") or ""
    return StandardizedDomainCloudService(domain=domain, bucket=resolved)


def make_domain_service(
    domain: str,
    bucket: str,
    project_id: str = "",
    dataset: str = "",
) -> StandardizedDomainCloudService:
    """Factory that creates a StandardizedDomainCloudService for a domain + bucket.

    Args:
        domain: Domain name (e.g. "market_data", "strategy")
        bucket: Cloud storage bucket name
        project_id: Ignored — kept for backward compatibility only
        dataset: Ignored — kept for backward compatibility only

    Returns:
        StandardizedDomainCloudService ready for upload/download
    """
    return StandardizedDomainCloudService(domain=domain, bucket=bucket)


__all__ = ["StandardizedDomainCloudService", "create_domain_cloud_service", "make_domain_service"]
