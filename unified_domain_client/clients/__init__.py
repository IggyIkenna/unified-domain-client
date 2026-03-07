"""Domain clients subpackage — 17 typed clients covering the full data platform."""

from __future__ import annotations

# 11. Execution
from unified_domain_client.clients.execution import ExecutionDomainClient, create_execution_client

# 4-7. Features (typed per group)
from unified_domain_client.clients.features import (
    FeaturesCalendarDomainClient,
    FeaturesDeltaOneDomainClient,
    FeaturesOnchainDomainClient,
    FeaturesVolatilityDomainClient,
)

# 1. Instruments
from unified_domain_client.clients.instruments import InstrumentsDomainClient

# 15-17. Liquidity (L2 checkpoints, liquidation clusters, liquidity features)
from unified_domain_client.clients.liquidity import (
    L2BookCheckpointClient,
    LiquidationClustersClient,
    LiquidityFeaturesClient,
)

# Backward-compat factory functions
# 2-3. Market data (thin typed + legacy rich)
from unified_domain_client.clients.market_data import (
    MarketCandleDataDomainClient,
    MarketCandleDomainClient,
    MarketDataDomainClient,
    MarketTickDataDomainClient,
    MarketTickDomainClient,
    create_market_candle_data_client,
    create_market_data_client,
    create_market_tick_data_client,
)

# 8-9. ML
from unified_domain_client.clients.ml import MLModelsDomainClient, MLPredictionsDomainClient

# 13. PnL
from unified_domain_client.clients.pnl import PnLDomainClient

# 12. Positions
from unified_domain_client.clients.positions import PositionsDomainClient

# 14. Risk
from unified_domain_client.clients.risk import RiskDomainClient

# 10. Strategy
from unified_domain_client.clients.strategy import StrategyDomainClient


def create_instruments_client(
    project_id: str | None = None,
    storage_bucket: str | None = None,
    analytics_dataset: str | None = None,
) -> InstrumentsDomainClient:
    """Factory function to create InstrumentsDomainClient."""
    return InstrumentsDomainClient(
        project_id=project_id,
        storage_bucket=storage_bucket,
        analytics_dataset=analytics_dataset,
    )


def create_features_client(
    feature_type: str = "delta_one",
    project_id: str | None = None,
    storage_bucket: str | None = None,
    category: str | None = None,
) -> FeaturesDeltaOneDomainClient:
    """Factory function to create FeaturesDeltaOneDomainClient (legacy: feature_type param ignored)."""
    return FeaturesDeltaOneDomainClient(
        project_id=project_id,
        storage_bucket=storage_bucket,
        category=category if category is not None else "cefi",
    )


__all__ = [
    # 14 canonical typed clients
    "InstrumentsDomainClient",
    "MarketTickDomainClient",
    "MarketCandleDomainClient",
    "FeaturesDeltaOneDomainClient",
    "FeaturesCalendarDomainClient",
    "FeaturesOnchainDomainClient",
    "FeaturesVolatilityDomainClient",
    "MLModelsDomainClient",
    "MLPredictionsDomainClient",
    "StrategyDomainClient",
    "ExecutionDomainClient",
    "PositionsDomainClient",
    "PnLDomainClient",
    "RiskDomainClient",
    # 15-17. Liquidity
    "L2BookCheckpointClient",
    "LiquidationClustersClient",
    "LiquidityFeaturesClient",
    # Legacy rich clients (backward compat)
    "MarketCandleDataDomainClient",
    "MarketTickDataDomainClient",
    "MarketDataDomainClient",
    # Factory functions (backward compat)
    "create_instruments_client",
    "create_market_candle_data_client",
    "create_market_tick_data_client",
    "create_market_data_client",
    "create_execution_client",
    "create_features_client",
]
