"""Unified Domain Services - Trading domain logic (clients, validation, cloud service wrappers).

Lightweight imports (schemas, date_filter, date_validation) load immediately.
Clients/standardized_service/factories/cloud_data_provider use lazy imports.
"""
# pyright: reportUnsupportedDunderAll=false, reportUnknownVariableType=false

from unified_domain_client.catalog import BigQueryCatalog, GlueCatalog
from unified_domain_client.cloud_target import CloudTarget
from unified_domain_client.data_completion import DataCompletionChecker, get_available_date_range, make_completion_checker
from unified_domain_client.date_validation import (
    DateValidationResult,
    DateValidator,
    get_earliest_valid_date,
    get_validator,
    should_skip_date,
)
from unified_domain_client.instrument_date_filter import DateFilterService
from unified_domain_client.lookback_constants import (
    FEATURE_GROUP_LOOKBACK,
    MAX_LOOKBACK_DAYS_BY_TIMEFRAME,
    TIMEFRAME_SECONDS,
)
from unified_domain_client.paths import (
    PATH_REGISTRY,
    DataSetSpec,
    PathRegistry,
    ReadMode,
    build_bucket,
    build_full_uri,
    build_path,
    get_spec,
)
from unified_domain_client.readers import (
    AthenaReader,
    BaseDataReader,
    BaseReader,
    BigQueryExternalReader,
    DirectReader,
    FeaturesReader,
    MarketDataReader,
    MLReader,
    get_reader,
)
from unified_domain_client.schemas import (
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
from unified_domain_client.timestamp_validation import (
    TimestampAlignmentResult,
    TimestampDateValidator,
    validate_timestamp_date_alignment,
)
from unified_domain_client.validation import DomainValidationConfig, DomainValidationService
from unified_domain_client.writers import (
    BaseDataWriter,
    BaseWriter,
    DirectWriter,
    FeaturesWriter,
    MarketDataWriter,
    MLWriter,
    get_writer,
)

# Lazy: clients, standardized_service, factories, cloud_data_provider
_LAZY_NAMES = {
    # Legacy rich clients (backward compat)
    "ExecutionDomainClient",
    "InstrumentsDomainClient",
    "MarketCandleDataDomainClient",
    "MarketDataDomainClient",
    "MarketTickDataDomainClient",
    # New typed clients
    "MarketTickDomainClient",
    "MarketCandleDomainClient",
    "FeaturesDeltaOneDomainClient",
    "FeaturesCalendarDomainClient",
    "FeaturesOnchainDomainClient",
    "FeaturesVolatilityDomainClient",
    "MLModelsDomainClient",
    "MLPredictionsDomainClient",
    "StrategyDomainClient",
    "PositionsDomainClient",
    "PnLDomainClient",
    "RiskDomainClient",
    # Factory functions
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
    """Lazy import for clients/standardized_service/factories.

    Deferred imports: intentional lazy loading to keep package import lightweight.
    """
    if name not in _LAZY_NAMES:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    if name in ("CloudDataProviderBase", "FeaturesDataProvider", "InstrumentsDataProvider", "MarketDataProvider"):
        from unified_domain_client.cloud_data_provider import (
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
        from unified_domain_client.standardized_service import StandardizedDomainCloudService

        return StandardizedDomainCloudService
    if name.startswith("create_") and "cloud_service" in name:
        from unified_domain_client.factories import (
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
    from unified_domain_client.clients import (
        ExecutionDomainClient,
        FeaturesCalendarDomainClient,
        FeaturesDeltaOneDomainClient,
        FeaturesOnchainDomainClient,
        FeaturesVolatilityDomainClient,
        InstrumentsDomainClient,
        MarketCandleDataDomainClient,
        MarketCandleDomainClient,
        MarketDataDomainClient,
        MarketTickDataDomainClient,
        MarketTickDomainClient,
        MLModelsDomainClient,
        MLPredictionsDomainClient,
        PnLDomainClient,
        PositionsDomainClient,
        RiskDomainClient,
        StrategyDomainClient,
        create_execution_client,
        create_features_client,
        create_instruments_client,
        create_market_candle_data_client,
        create_market_data_client,
        create_market_tick_data_client,
    )

    return {
        "ExecutionDomainClient": ExecutionDomainClient,
        "FeaturesCalendarDomainClient": FeaturesCalendarDomainClient,
        "FeaturesDeltaOneDomainClient": FeaturesDeltaOneDomainClient,
        "FeaturesOnchainDomainClient": FeaturesOnchainDomainClient,
        "FeaturesVolatilityDomainClient": FeaturesVolatilityDomainClient,
        "InstrumentsDomainClient": InstrumentsDomainClient,
        "MLModelsDomainClient": MLModelsDomainClient,
        "MLPredictionsDomainClient": MLPredictionsDomainClient,
        "MarketCandleDomainClient": MarketCandleDomainClient,
        "MarketCandleDataDomainClient": MarketCandleDataDomainClient,
        "MarketDataDomainClient": MarketDataDomainClient,
        "MarketTickDomainClient": MarketTickDomainClient,
        "MarketTickDataDomainClient": MarketTickDataDomainClient,
        "PnLDomainClient": PnLDomainClient,
        "PositionsDomainClient": PositionsDomainClient,
        "RiskDomainClient": RiskDomainClient,
        "StrategyDomainClient": StrategyDomainClient,
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
    # Domain clients — 14 typed
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
    # Legacy rich clients (backward compat)
    "MarketCandleDataDomainClient",
    "MarketTickDataDomainClient",
    "MarketDataDomainClient",
    # Factory functions (backward compat)
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
    # Readers
    "BaseReader",
    "MarketDataReader",
    "FeaturesReader",
    "MLReader",
    "BaseDataReader",
    "DirectReader",
    "BigQueryExternalReader",
    "AthenaReader",
    "get_reader",
    # Writers
    "BaseWriter",
    "MarketDataWriter",
    "FeaturesWriter",
    "MLWriter",
    "BaseDataWriter",
    "DirectWriter",
    "get_writer",
    # Catalog
    "BigQueryCatalog",
    "GlueCatalog",
    # Data completion and path registry
    "CloudTarget",
    "DataCompletionChecker",
    "get_available_date_range",
    "make_completion_checker",
    "PathRegistry",
    "PATH_REGISTRY",
    "DataSetSpec",
    "ReadMode",
    "get_spec",
    "build_bucket",
    "build_path",
    "build_full_uri",
]
