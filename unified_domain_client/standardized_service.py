"""Standardized Domain Cloud Service.

Tier 2 compliance: Uses unified_cloud_interface for GCS I/O only.
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

from unified_domain_client import CloudTarget

logger = logging.getLogger(__name__)


class StandardizedDomainCloudService:
    """Domain cloud service using UCLI for GCS operations."""

    def __init__(self, domain: str, cloud_target: CloudTarget) -> None:
        self.domain: str = domain
        self.cloud_target: CloudTarget = cloud_target

    def download_from_gcs(
        self,
        gcs_path: str,
        format: str = "parquet",
        log_errors: bool = True,
    ) -> pd.DataFrame | dict[str, object]:
        """Download data from GCS."""
        bucket = self.cloud_target.gcs_bucket
        path = gcs_path.lstrip("/")
        try:
            data = download_from_storage(bucket, path)
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
                logger.error("Failed to download %s/%s: %s", bucket, path, e)
            raise

    def upload_to_gcs(
        self,
        data: pd.DataFrame,
        gcs_path: str,
        format: str = "parquet",
    ) -> str:
        """Upload data to GCS."""
        bucket = self.cloud_target.gcs_bucket
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
        return upload_to_storage(bucket, path, raw)

    def query_bigquery(
        self,
        query: str,
        parameters: dict[str, object] | None = None,
    ) -> pd.DataFrame:
        _ = (query, parameters)
        raise NotImplementedError(
            "BigQuery is not supported in Tier 2 UDS. "
            + "Use unified-trading-library or google-cloud-bigquery in the service layer."
        )


def create_domain_cloud_service(
    domain: str,
    cloud_target: CloudTarget,
) -> StandardizedDomainCloudService:
    """Factory for StandardizedDomainCloudService."""
    return StandardizedDomainCloudService(domain=domain, cloud_target=cloud_target)


def make_domain_service(domain: str, bucket: str, project_id: str = "", dataset: str = "") -> StandardizedDomainCloudService:
    """Factory that hides CloudTarget construction from service code.

    Services should use this instead of constructing CloudTarget directly.

    Args:
        domain: Domain name (e.g. "market_data", "strategy")
        bucket: GCS bucket name
        project_id: GCP project ID (optional, default "")
        dataset: BigQuery dataset name (optional, default "")

    Returns:
        StandardizedDomainCloudService ready for upload/download
    """
    target = CloudTarget(gcs_bucket=bucket, project_id=project_id, bigquery_dataset=dataset)
    return StandardizedDomainCloudService(domain=domain, cloud_target=target)


__all__ = ["StandardizedDomainCloudService", "create_domain_cloud_service"]
