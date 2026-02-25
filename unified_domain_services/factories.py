"""
Factory Functions for Domain-Specific Cloud Services

NOTE: Legacy factories removed as part of UCS refactoring.
Use StandardizedDomainCloudService directly or cloud-agnostic clients.

Example:
    from unified_cloud_services import StandardizedDomainCloudService
    from unified_cloud_services import CloudTarget

    # Create domain service directly
    cloud_target = CloudTarget(
        gcs_bucket="my-bucket",
        bigquery_dataset="my_dataset"
    )
    service = StandardizedDomainCloudService(
        domain="market_data",
        cloud_target=cloud_target
    )
"""


# Legacy factory functions removed - use StandardizedDomainCloudService directly
def create_market_data_cloud_service():
    """DEPRECATED: Use StandardizedDomainCloudService(domain='market_data') instead."""
    raise NotImplementedError(
        "Factory functions have been removed. Use StandardizedDomainCloudService(domain='market_data') instead."
    )


def create_features_cloud_service():
    """DEPRECATED: Use StandardizedDomainCloudService(domain='features') instead."""
    raise NotImplementedError(
        "Factory functions have been removed. Use StandardizedDomainCloudService(domain='features') instead."
    )


def create_strategy_cloud_service():
    """DEPRECATED: Use StandardizedDomainCloudService(domain='strategy') instead."""
    raise NotImplementedError(
        "Factory functions have been removed. Use StandardizedDomainCloudService(domain='strategy') instead."
    )


def create_backtesting_cloud_service():
    """DEPRECATED: Use StandardizedDomainCloudService(domain='ml') instead."""
    raise NotImplementedError(
        "Factory functions have been removed. Use StandardizedDomainCloudService(domain='ml') instead."
    )


def create_instruments_cloud_service():
    """DEPRECATED: Use StandardizedDomainCloudService(domain='instruments') instead."""
    raise NotImplementedError(
        "Factory functions have been removed. Use StandardizedDomainCloudService(domain='instruments') instead."
    )


__all__ = [
    "create_backtesting_cloud_service",
    "create_features_cloud_service",
    "create_instruments_cloud_service",
    "create_market_data_cloud_service",
    "create_strategy_cloud_service",
]
