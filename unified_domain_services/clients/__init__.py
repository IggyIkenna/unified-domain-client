"""
Domain clients for unified cloud services.

Re-exports all domain client classes and factory functions for convenient access.
"""

from unified_domain_services.clients.base import ClientConfig, FeaturesClientConfig
from unified_domain_services.clients.execution import ExecutionDomainClient
from unified_domain_services.clients.factory import (
    create_execution_client,
    create_features_client,
    create_instruments_client,
    create_market_candle_data_client,
    create_market_tick_data_client,
)
from unified_domain_services.clients.features import FeaturesDomainClient
from unified_domain_services.clients.instruments import InstrumentsDomainClient
from unified_domain_services.clients.market_data import MarketCandleDataDomainClient, MarketTickDataDomainClient

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
