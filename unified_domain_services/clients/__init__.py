"""
Domain clients for unified cloud services.

Re-exports all domain client classes and factory functions for convenient access.
"""

from unified_domain_services import (
    ClientConfig,
    ExecutionDomainClient,
    FeaturesClientConfig,
    FeaturesDomainClient,
    InstrumentsDomainClient,
    MarketCandleDataDomainClient,
    MarketTickDataDomainClient,
    create_execution_client,
    create_features_client,
    create_instruments_client,
    create_market_candle_data_client,
    create_market_tick_data_client,
)

__all__ = [
    # Base types
    "ClientConfig",
    "ExecutionDomainClient",
    "FeaturesClientConfig",
    "FeaturesDomainClient",
    # Client classes
    "InstrumentsDomainClient",
    "MarketCandleDataDomainClient",
    "MarketTickDataDomainClient",
    "create_execution_client",
    "create_features_client",
    # Factory functions
    "create_instruments_client",
    "create_market_candle_data_client",
    "create_market_tick_data_client",
]
