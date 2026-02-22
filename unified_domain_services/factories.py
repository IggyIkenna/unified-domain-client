"""
Factory Functions for Domain-Specific Cloud Services

Re-exports from unified_cloud_services.domain.factories.
DomainCloudOperations was removed from UCS; factories now return UnifiedCloudService.
"""

from unified_cloud_services.domain.factories import (
    create_backtesting_cloud_service,
    create_features_cloud_service,
    create_instruments_cloud_service,
    create_market_data_cloud_service,
    create_strategy_cloud_service,
)

__all__ = [
    "create_backtesting_cloud_service",
    "create_features_cloud_service",
    "create_instruments_cloud_service",
    "create_market_data_cloud_service",
    "create_strategy_cloud_service",
]
