"""Factory functions for domain-specific cloud services.

Tier 2 compliance: Uses UDS StandardizedDomainCloudService.
"""

from unified_domain_client.cloud_target import CloudTarget
from unified_domain_client.standardized_service import StandardizedDomainCloudService


def create_backtesting_cloud_service(
    project_id: str,
    gcs_bucket: str | None = None,
    bigquery_dataset: str | None = None,
) -> StandardizedDomainCloudService:
    """Create backtesting domain cloud service."""
    target = CloudTarget(
        project_id=project_id,
        gcs_bucket=gcs_bucket or f"backtest-store-{project_id}",
        bigquery_dataset=bigquery_dataset or "backtest",
    )
    return StandardizedDomainCloudService(domain="backtest", cloud_target=target)


def create_features_cloud_service(
    project_id: str,
    gcs_bucket: str | None = None,
    bigquery_dataset: str | None = None,
) -> StandardizedDomainCloudService:
    """Create features domain cloud service."""
    target = CloudTarget(
        project_id=project_id,
        gcs_bucket=gcs_bucket or f"features-store-{project_id}",
        bigquery_dataset=bigquery_dataset or "features",
    )
    return StandardizedDomainCloudService(domain="features", cloud_target=target)


def create_instruments_cloud_service(
    project_id: str,
    gcs_bucket: str | None = None,
    bigquery_dataset: str | None = None,
) -> StandardizedDomainCloudService:
    """Create instruments domain cloud service."""
    target = CloudTarget(
        project_id=project_id,
        gcs_bucket=gcs_bucket or f"instruments-store-{project_id}",
        bigquery_dataset=bigquery_dataset or "instruments",
    )
    return StandardizedDomainCloudService(domain="instruments", cloud_target=target)


def create_market_data_cloud_service(
    project_id: str,
    gcs_bucket: str | None = None,
    bigquery_dataset: str | None = None,
) -> StandardizedDomainCloudService:
    """Create market data domain cloud service."""
    target = CloudTarget(
        project_id=project_id,
        gcs_bucket=gcs_bucket or f"market-data-tick-{project_id}",
        bigquery_dataset=bigquery_dataset or "market_data_hft",
    )
    return StandardizedDomainCloudService(domain="market_data", cloud_target=target)


def create_strategy_cloud_service(
    project_id: str,
    gcs_bucket: str | None = None,
    bigquery_dataset: str | None = None,
) -> StandardizedDomainCloudService:
    """Create strategy domain cloud service."""
    target = CloudTarget(
        project_id=project_id,
        gcs_bucket=gcs_bucket or f"strategy-store-{project_id}",
        bigquery_dataset=bigquery_dataset or "strategy",
    )
    return StandardizedDomainCloudService(domain="strategy", cloud_target=target)


__all__ = [
    "create_backtesting_cloud_service",
    "create_features_cloud_service",
    "create_instruments_cloud_service",
    "create_market_data_cloud_service",
    "create_strategy_cloud_service",
]
