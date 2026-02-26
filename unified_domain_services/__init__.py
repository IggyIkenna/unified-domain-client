"""Unified Domain Services - Trading domain logic (clients, validation, cloud service wrappers).

Lightweight imports (schemas, date_filter, date_validation) load immediately.
Clients/standardized_service/factories/cloud_data_provider use lazy imports.
StandardizedDomainCloudService re-exports from unified_cloud_services.domain.
"""

# pyright: reportUnsupportedDunderAll=false, reportUnknownVariableType=false

from unified_cloud_services import (
    FEATURE_GROUP_LOOKBACK,
    MAX_LOOKBACK_DAYS_BY_TIMEFRAME,
    TIMEFRAME_SECONDS,
    TimestampAlignmentResult,
    TimestampDateValidator,
    validate_timestamp_date_alignment,
)
from unified_config_interface import (
    CLOB_VENUES,
    CONFIG_SCHEMA,
    DEX_VENUES,
    INSTRUMENT_TYPE_FOLDER_MAP,
    OPTIONAL_CONFIG_FIELDS,
    REQUIRED_CONFIG_FIELDS,
    VALID_ALGORITHMS,
    VALID_INSTRUCTION_TYPES,
    VENUE_CATEGORY_MAP,
    ZERO_ALPHA_VENUES,
    ConfigValidationError,
    ConfigValidator,
    validate_config,
    validate_config_file,
)

from unified_domain_services.date_validation import (
    DateValidationResult,
    DateValidator,
    get_earliest_valid_date,
    get_validator,
    should_skip_date,
)
from unified_domain_services.instrument_date_filter import DateFilterService
from unified_domain_services.schemas.instruction_schema import (
    INSTRUCTION_COLUMNS,
    INSTRUCTION_SCHEMA,
    InstructionValidationError,
    InstructionValidator,
    validate_instruction_dataframe,
    validate_instruction_parquet,
)
from unified_domain_services.schemas.instrument_key import InstrumentKey
from unified_domain_services.validation import DomainValidationConfig, DomainValidationService

# Lazy: clients, standardized_service, factories, cloud_data_provider
_LAZY_NAMES = {
    "ExecutionDomainClient",
    "FeaturesDomainClient",
    "InstrumentsDomainClient",
    "MarketCandleDataDomainClient",
    "MarketTickDataDomainClient",
    "create_execution_client",
    "create_features_client",
    "create_instruments_client",
    "create_market_candle_data_client",
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
    "cloud_data_provider",
}


def __getattr__(name: str) -> object:
    """Lazy import for clients/standardized_service/factories."""
    if name not in _LAZY_NAMES:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    if name == "cloud_data_provider":
        import unified_domain_services.cloud_data_provider as cloud_data_provider

        return cloud_data_provider
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
        from unified_cloud_services import StandardizedDomainCloudService

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

    return {
        "ExecutionDomainClient": ExecutionDomainClient,
        "FeaturesDomainClient": FeaturesDomainClient,
        "InstrumentsDomainClient": InstrumentsDomainClient,
        "MarketCandleDataDomainClient": MarketCandleDataDomainClient,
        "MarketTickDataDomainClient": MarketTickDataDomainClient,
        "create_execution_client": create_execution_client,
        "create_features_client": create_features_client,
        "create_instruments_client": create_instruments_client,
        "create_market_candle_data_client": create_market_candle_data_client,
        "create_market_tick_data_client": create_market_tick_data_client,
    }[name]


__all__ = [
    # Config and instruction validation schemas
    "CLOB_VENUES",
    "CONFIG_SCHEMA",
    "DEX_VENUES",
    "FEATURE_GROUP_LOOKBACK",
    "INSTRUCTION_COLUMNS",
    "INSTRUCTION_SCHEMA",
    "INSTRUMENT_TYPE_FOLDER_MAP",
    "MAX_LOOKBACK_DAYS_BY_TIMEFRAME",
    "OPTIONAL_CONFIG_FIELDS",
    "REQUIRED_CONFIG_FIELDS",
    "TIMEFRAME_SECONDS",
    "VALID_ALGORITHMS",
    "VALID_INSTRUCTION_TYPES",
    "VENUE_CATEGORY_MAP",
    "ZERO_ALPHA_VENUES",
    "CloudDataProviderBase",
    "ConfigValidationError",
    "ConfigValidator",
    "DateFilterService",
    "DateValidationResult",
    "DateValidator",
    "DomainValidationConfig",
    "DomainValidationService",
    "ExecutionDomainClient",
    "FeaturesDataProvider",
    "FeaturesDomainClient",
    "InstructionValidationError",
    "InstructionValidator",
    "InstrumentKey",
    "InstrumentsDataProvider",
    "InstrumentsDomainClient",
    "MarketCandleDataDomainClient",
    "MarketDataProvider",
    "MarketTickDataDomainClient",
    "StandardizedDomainCloudService",
    "TimestampAlignmentResult",
    "TimestampDateValidator",
    "create_backtesting_cloud_service",
    "create_execution_client",
    "create_features_client",
    "create_features_cloud_service",
    "create_instruments_client",
    "create_market_candle_data_client",
    "create_market_data_cloud_service",
    "create_market_tick_data_client",
    "create_strategy_cloud_service",
    "get_earliest_valid_date",
    "get_validator",
    "should_skip_date",
    "validate_config",
    "validate_config_file",
    "validate_instruction_dataframe",
    "validate_instruction_parquet",
    "validate_timestamp_date_alignment",
]
