"""Factory functions for domain-specific cloud services.

Cloud-agnostic: passes bucket: str directly to StandardizedDomainCloudService.
project_id and analytics_dataset params kept for legacy callers (deprecated) but ignored.
"""

from .standardized_service import StandardizedDomainCloudService


def create_backtesting_cloud_service(
    bucket: str = "",
    project_id: str = "",
    storage_bucket: str = "",
    analytics_dataset: str = "",
) -> StandardizedDomainCloudService:
    """Create backtesting domain cloud service."""
    resolved_bucket = bucket or storage_bucket or "backtest-store"
    return StandardizedDomainCloudService(domain="backtest", bucket=resolved_bucket)


def create_features_cloud_service(
    bucket: str = "",
    project_id: str = "",
    storage_bucket: str = "",
    analytics_dataset: str = "",
) -> StandardizedDomainCloudService:
    """Create features domain cloud service."""
    resolved_bucket = bucket or storage_bucket or "features-store"
    return StandardizedDomainCloudService(domain="features", bucket=resolved_bucket)


def create_instruments_cloud_service(
    bucket: str = "",
    project_id: str = "",
    storage_bucket: str = "",
    analytics_dataset: str = "",
) -> StandardizedDomainCloudService:
    """Create instruments domain cloud service."""
    resolved_bucket = bucket or storage_bucket or "instruments-store"
    return StandardizedDomainCloudService(domain="instruments", bucket=resolved_bucket)


def create_market_data_cloud_service(
    bucket: str = "",
    project_id: str = "",
    storage_bucket: str = "",
    analytics_dataset: str = "",
) -> StandardizedDomainCloudService:
    """Create market data domain cloud service."""
    resolved_bucket = bucket or storage_bucket or "market-data-tick"
    return StandardizedDomainCloudService(domain="market_data", bucket=resolved_bucket)


def create_strategy_cloud_service(
    bucket: str = "",
    project_id: str = "",
    storage_bucket: str = "",
    analytics_dataset: str = "",
) -> StandardizedDomainCloudService:
    """Create strategy domain cloud service."""
    resolved_bucket = bucket or storage_bucket or "strategy-store"
    return StandardizedDomainCloudService(domain="strategy", bucket=resolved_bucket)


__all__ = [
    "create_backtesting_cloud_service",
    "create_features_cloud_service",
    "create_instruments_cloud_service",
    "create_market_data_cloud_service",
    "create_strategy_cloud_service",
]
