"""Unified Domain Services - Trading domain logic (clients, validation, cloud service wrappers).

Lightweight imports (schemas, date_filter, date_validation) load immediately.
Clients/standardized_service/factories/cloud_data_provider use lazy imports.
"""

__version__ = "0.1.45"

# pyright: reportUnsupportedDunderAll=false, reportUnknownVariableType=false

from unified_config_interface.lookback_constants import (  # noqa: deep-import
    FEATURE_GROUP_LOOKBACK,
    MAX_LOOKBACK_DAYS_BY_TIMEFRAME,
    TIMEFRAME_SECONDS,
)
from unified_config_interface.paths import (  # noqa: deep-import
    PATH_REGISTRY,
    DataSetSpec,
    PathRegistry,
    ReadMode,
    build_bucket,
    build_full_uri,
    build_path,
    get_spec,
)

from unified_domain_client.catalog import BigQueryCatalog, GlueCatalog  # noqa: deep-import
from unified_domain_client.data_completion import (  # noqa: deep-import
    DataCompletionChecker,
    get_available_date_range,
    make_completion_checker,
)
from unified_domain_client.date_validation import (  # noqa: deep-import
    DateValidationResult,
    DateValidator,
    get_earliest_valid_date,
    get_validator,
    should_skip_date,
)
from unified_domain_client.instrument_date_filter import DateFilterService  # noqa: deep-import
from unified_domain_client.readers import (  # noqa: deep-import
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
from unified_domain_client.schemas import (  # noqa: deep-import
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
from unified_domain_client.timestamp_validation import (  # noqa: deep-import
    TimestampAlignmentResult,
    TimestampDateValidator,
    validate_timestamp_date_alignment,
)
from unified_domain_client.validation import (  # noqa: deep-import
    DomainValidationConfig,
    DomainValidationService,
)
from unified_domain_client.writers import (  # noqa: deep-import
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
    # Artifact store
    "CloudModelArtifactStore",
    # Legacy rich clients (legacy)
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
    # Liquidity clients
    "L2BookCheckpointClient",
    # Sports domain clients
    "SportsFeaturesDomainClient",
    "SportsFixturesDomainClient",
    "SportsMappingsDomainClient",
    "SportsOddsDomainClient",
    "SportsTickDataDomainClient",
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


def _load_sports(name: str) -> object:
    """Lazy load sports domain clients."""
    from unified_domain_client.sports import (  # noqa: deep-import
        SportsFeaturesDomainClient,
        SportsFixturesDomainClient,
        SportsMappingsDomainClient,
        SportsOddsDomainClient,
        SportsTickDataDomainClient,
    )

    return {
        "SportsFeaturesDomainClient": SportsFeaturesDomainClient,
        "SportsFixturesDomainClient": SportsFixturesDomainClient,
        "SportsMappingsDomainClient": SportsMappingsDomainClient,
        "SportsOddsDomainClient": SportsOddsDomainClient,
        "SportsTickDataDomainClient": SportsTickDataDomainClient,
    }[name]


def _load_clients(name: str) -> object:
    """Lazy load main domain clients and factory functions."""
    from unified_domain_client.clients import (  # noqa: deep-import
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


_SPORTS_NAMES = frozenset(
    {
        "SportsFeaturesDomainClient",
        "SportsFixturesDomainClient",
        "SportsMappingsDomainClient",
        "SportsOddsDomainClient",
        "SportsTickDataDomainClient",
    }
)
_CLOUD_PROVIDER_NAMES = frozenset(
    {
        "CloudDataProviderBase",
        "CloudModelArtifactStore",
        "FeaturesDataProvider",
        "InstrumentsDataProvider",
        "MarketDataProvider",
    }
)
_FACTORY_SERVICE_NAMES = frozenset(
    {
        "create_backtesting_cloud_service",
        "create_features_cloud_service",
        "create_market_data_cloud_service",
        "create_strategy_cloud_service",
    }
)


def __getattr__(name: str) -> object:
    """Lazy import for clients/standardized_service/factories.

    Deferred imports: intentional lazy loading to keep package import lightweight.
    """
    if name not in _LAZY_NAMES:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    if name == "L2BookCheckpointClient":
        from unified_domain_client.clients.liquidity import (  # noqa: deep-import
            L2BookCheckpointClient,
        )

        return L2BookCheckpointClient
    if name in _SPORTS_NAMES:
        return _load_sports(name)
    if name in _CLOUD_PROVIDER_NAMES:
        from unified_domain_client.artifact_store import (  # noqa: deep-import
            CloudModelArtifactStore,
        )
        from unified_domain_client.cloud_data_provider import (  # noqa: deep-import
            CloudDataProviderBase,
            FeaturesDataProvider,
            InstrumentsDataProvider,
            MarketDataProvider,
        )

        return {
            "CloudDataProviderBase": CloudDataProviderBase,
            "CloudModelArtifactStore": CloudModelArtifactStore,
            "FeaturesDataProvider": FeaturesDataProvider,
            "InstrumentsDataProvider": InstrumentsDataProvider,
            "MarketDataProvider": MarketDataProvider,
        }[name]
    if name in ("StandardizedDomainCloudService", "make_domain_service"):
        from unified_domain_client.standardized_service import (  # noqa: deep-import
            StandardizedDomainCloudService,
            make_domain_service,
        )

        return {
            "StandardizedDomainCloudService": StandardizedDomainCloudService,
            "make_domain_service": make_domain_service,
        }[name]
    if name in _FACTORY_SERVICE_NAMES:
        from unified_domain_client.factories import (  # noqa: deep-import
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
    return _load_clients(name)


__all__ = [
    "CloudModelArtifactStore",
    "DateFilterService",
    "StandardizedDomainCloudService",
    "make_domain_service",
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
    # Legacy rich clients (legacy)
    "MarketCandleDataDomainClient",
    "MarketTickDataDomainClient",
    "MarketDataDomainClient",
    # Liquidity clients
    "L2BookCheckpointClient",
    # Sports domain clients
    "SportsFeaturesDomainClient",
    "SportsFixturesDomainClient",
    "SportsMappingsDomainClient",
    "SportsOddsDomainClient",
    "SportsTickDataDomainClient",
    # Factory functions (legacy)
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
