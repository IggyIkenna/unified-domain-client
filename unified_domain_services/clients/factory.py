"""
Factory functions for creating domain clients.

Provides convenience factory functions for creating various domain clients
with proper type checking and configuration handling.
"""

from __future__ import annotations

from typing import Unpack

from unified_domain_services.clients.base import ClientConfig
from unified_domain_services.clients.execution import ExecutionDomainClient
from unified_domain_services.clients.features import FeaturesDomainClient
from unified_domain_services.clients.instruments import InstrumentsDomainClient
from unified_domain_services.clients.market_data import MarketCandleDataDomainClient, MarketTickDataDomainClient


def create_instruments_client(**kwargs: Unpack[ClientConfig]) -> InstrumentsDomainClient:
    """Factory function to create InstrumentsDomainClient."""
    return InstrumentsDomainClient(**kwargs)


def create_market_candle_data_client(**kwargs: Unpack[ClientConfig]) -> MarketCandleDataDomainClient:
    """Factory function to create MarketCandleDataDomainClient."""
    return MarketCandleDataDomainClient(**kwargs)


def create_market_tick_data_client(**kwargs: Unpack[ClientConfig]) -> MarketTickDataDomainClient:
    """Factory function to create MarketTickDataDomainClient."""
    return MarketTickDataDomainClient(**kwargs)


def create_execution_client(**kwargs: Unpack[ClientConfig]) -> ExecutionDomainClient:
    """Factory function to create ExecutionDomainClient."""
    return ExecutionDomainClient(**kwargs)


def create_features_client(feature_type: str = "delta_one", **kwargs: Unpack[ClientConfig]) -> FeaturesDomainClient:
    """Factory function to create FeaturesDomainClient."""
    return FeaturesDomainClient(feature_type=feature_type, **kwargs)
