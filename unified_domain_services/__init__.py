"""Unified Domain Services - Trading domain logic (clients, validation, cloud service wrappers).

Lightweight imports (schemas, date_filter, date_validation) load immediately.
Clients/standardized_service/factories/cloud_data_provider use lazy imports.
StandardizedDomainCloudService re-exports from unified_cloud_services.domain.
"""
# pyright: reportUnsupportedDunderAll=false, reportUnknownVariableType=false

from unified_cloud_services.domain import (
    FEATURE_GROUP_LOOKBACK,
    MAX_LOOKBACK_DAYS_BY_TIMEFRAME,
    TIMEFRAME_SECONDS,
    TimestampAlignmentResult,
    TimestampDateValidator,
    validate_timestamp_date_alignment,
)

from unified_domain_services.date_validation import (
    DateValidationResult,
    DateValidator,
    get_earliest_valid_date,
    get_validator,
    should_skip_date,
)
from unified_domain_services.instrument_date_filter import DateFilterService
from unified_domain_services.schemas import (
    CLOB_VENUES,
    CONFIG_SCHEMA,
    DEX_VENUES,
    INSTRUCTION_COLUMNS,
    INSTRUCTION_SCHEMA,
    INSTRUMENT_TYPE_FOLDER_MAP,
    OPTIONAL_CONFIG_FIELDS,
    REQUIRED_CONFIG_FIELDS,
    VALID_ALGORITHMS,
    VALID_INSTRUCTION_TYPES,
    VENUE_CATEGORY_MAP,
    ZERO_ALPHA_VENUES,
    ConfigValidationError,
    ConfigValidator,
    InstructionValidationError,
    InstructionValidator,
    InstrumentKey,
    validate_config,
    validate_config_file,
    validate_instruction_dataframe,
    validate_instruction_parquet,
)
from unified_domain_services.validation import DomainValidationConfig, DomainValidationService

# Lazy: clients, standardized_service, factories, cloud_data_provider
_LAZY_NAMES = {
    "ExecutionDomainClient",
    "InstrumentsDomainClient",
    "MarketCandleDataDomainClient",
    "MarketDataDomainClient",
    "MarketTickDataDomainClient",
    "create_execution_client",
    "create_features_client",
    "create_instruments_client",
    "create_market_candle_data_client",
    "create_market_data_client",
    "create_market_tick_data_client",
    "create_backtesting_cloud_service",
    "create_features_cloud_service",
    "create_market_data_cloud_service",
    "create_strategy_cloud_service",
    "StandardizedDomainCloudService",
    "CloudDataProviderBase",
    "FeaturesDataProvider",
    "InstrumentsDataProvider",
    "MarketDataProvider",
}


def __getattr__(name: str) -> object:
    """Lazy import for clients/standardized_service/factories."""
    if name not in _LAZY_NAMES:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    if name in ("CloudDataProviderBase", "FeaturesDataProvider", "InstrumentsDataProvider", "MarketDataProvider"):
        from unified_domain_services.cloud_data_provider import (
            CloudDataProviderBase,
            FeaturesDataProvider,
            InstrumentsDataProvider,
            MarketDataProvider,
        )

        return {
            "CloudDataProviderBase": CloudDataProviderBase,
            "FeaturesDataProvider": FeaturesDataProvider,
            "InstrumentsDataProvider": InstrumentsDataProvider,
            "MarketDataProvider": MarketDataProvider,
        }[name]
    if name == "StandardizedDomainCloudService":
        from unified_domain_services.standardized_service import StandardizedDomainCloudService

        return StandardizedDomainCloudService
    if name.startswith("create_") and "cloud_service" in name:
        from unified_domain_services.factories import (
            create_backtesting_cloud_service,
            create_features_cloud_service,
            create_market_data_cloud_service,
            create_strategy_cloud_service,
        )

        return {
            "create_backtesting_cloud_service": create_backtesting_cloud_service,
            "create_features_cloud_service": create_features_cloud_service,
            "create_market_data_cloud_service": create_market_data_cloud_service,
            "create_strategy_cloud_service": create_strategy_cloud_service,
        }[name]
    from unified_domain_services.clients import (
        ExecutionDomainClient,
        InstrumentsDomainClient,
        MarketCandleDataDomainClient,
        MarketDataDomainClient,
        MarketTickDataDomainClient,
        create_execution_client,
        create_features_client,
        create_instruments_client,
        create_market_candle_data_client,
        create_market_data_client,
        create_market_tick_data_client,
    )

    return {
        "ExecutionDomainClient": ExecutionDomainClient,
        "InstrumentsDomainClient": InstrumentsDomainClient,
        "MarketCandleDataDomainClient": MarketCandleDataDomainClient,
        "MarketDataDomainClient": MarketDataDomainClient,
        "MarketTickDataDomainClient": MarketTickDataDomainClient,
        "create_execution_client": create_execution_client,
        "create_features_client": create_features_client,
        "create_instruments_client": create_instruments_client,
        "create_market_candle_data_client": create_market_candle_data_client,
        "create_market_data_client": create_market_data_client,
        "create_market_tick_data_client": create_market_tick_data_client,
    }[name]


__all__ = [
    "DateFilterService",
    "StandardizedDomainCloudService",
    "DomainValidationService",
    "DomainValidationConfig",
    "TimestampDateValidator",
    "TimestampAlignmentResult",
    "validate_timestamp_date_alignment",
    "DateValidator",
    "DateValidationResult",
    "should_skip_date",
    "get_earliest_valid_date",
    "get_validator",
    "FEATURE_GROUP_LOOKBACK",
    "MAX_LOOKBACK_DAYS_BY_TIMEFRAME",
    "TIMEFRAME_SECONDS",
    "create_market_data_cloud_service",
    "create_features_cloud_service",
    "create_strategy_cloud_service",
    "create_backtesting_cloud_service",
    "InstrumentsDomainClient",
    "MarketCandleDataDomainClient",
    "MarketTickDataDomainClient",
    "ExecutionDomainClient",
    "MarketDataDomainClient",
    "create_instruments_client",
    "create_market_candle_data_client",
    "create_market_tick_data_client",
    "create_execution_client",
    "create_market_data_client",
    "create_features_client",
    "CloudDataProviderBase",
    "FeaturesDataProvider",
    "InstrumentsDataProvider",
    "MarketDataProvider",
    # Config and instruction validation schemas
    "CLOB_VENUES",
    "CONFIG_SCHEMA",
    "ConfigValidator",
    "ConfigValidationError",
    "DEX_VENUES",
    "INSTRUMENT_TYPE_FOLDER_MAP",
    "INSTRUCTION_COLUMNS",
    "INSTRUCTION_SCHEMA",
    "InstructionValidator",
    "InstructionValidationError",
    "OPTIONAL_CONFIG_FIELDS",
    "REQUIRED_CONFIG_FIELDS",
    "VALID_ALGORITHMS",
    "VALID_INSTRUCTION_TYPES",
    "VENUE_CATEGORY_MAP",
    "ZERO_ALPHA_VENUES",
    "validate_config",
    "validate_config_file",
    "validate_instruction_dataframe",
    "validate_instruction_parquet",
    "InstrumentKey",
]
