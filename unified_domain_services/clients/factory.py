"""
Factory functions for creating domain clients.

Provides convenience factory functions for creating various domain clients
with proper type checking and configuration handling.
"""

from __future__ import annotations

import warnings
from typing import Unpack

from unified_domain_services import (
    ClientConfig,
    ExecutionDomainClient,
    FeaturesDomainClient,
    InstrumentsDomainClient,
    MarketCandleDataDomainClient,
    MarketDataDomainClient,
    MarketTickDataDomainClient,
)


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


# Deprecated: Keep for backward compatibility
def create_market_data_client(**kwargs: Unpack[ClientConfig]) -> MarketDataDomainClient:
    """
    ⚠️ DEPRECATED: Use create_market_candle_data_client() or create_market_tick_data_client() instead.

    Factory function to create MarketDataDomainClient (deprecated).
    """
    warnings.warn(
        "create_market_data_client() is deprecated. Use create_market_candle_data_client() "
        "or create_market_tick_data_client() instead. "
        "See docs/CLIENTS_DEPRECATION_GUIDE.md for migration details.",
        DeprecationWarning,
        stacklevel=2,
    )
    return MarketDataDomainClient(**kwargs)


def create_features_client(feature_type: str = "delta_one", **kwargs: Unpack[ClientConfig]) -> FeaturesDomainClient:
    """Factory function to create FeaturesDomainClient."""
    return FeaturesDomainClient(feature_type=feature_type, **kwargs)
