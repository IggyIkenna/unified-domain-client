"""
unified_domain_services — compatibility re-export alias for unified_domain_client.

This package provides the NEW canonical import path for all consumers. During the
rename transition (T3 STEP C: lib-phase2-rename-step2), all 14 services and T2 libs
will update their imports to use ``unified_domain_services`` instead of
``unified_domain_client``. Once the cascade is complete, the ``unified_domain_client``
package directory will be removed.

Usage (new canonical path):
    from unified_domain_services import InstrumentsDomainClient, PathRegistry

Old path (deprecated — do NOT add new imports using this):
    from unified_domain_client import InstrumentsDomainClient, PathRegistry

STEP 1 (this file): Add alias package — both paths work simultaneously.
STEP 2 (T3 STEP C):  Update all 14 services + T2 libs; remove unified_domain_client/.
"""

# Re-export the entire unified_domain_client public API under the new package name.
# Redundant aliases (X as X) are the ruff-correct pattern for explicit re-exports.
from unified_domain_client import (
    CLOB_VENUES as CLOB_VENUES,
)
from unified_domain_client import (
    CONFIG_SCHEMA as CONFIG_SCHEMA,
)
from unified_domain_client import (
    DEX_VENUES as DEX_VENUES,
)
from unified_domain_client import (
    FEATURE_GROUP_LOOKBACK as FEATURE_GROUP_LOOKBACK,
)
from unified_domain_client import (
    INSTRUCTION_COLUMNS as INSTRUCTION_COLUMNS,
)
from unified_domain_client import (
    INSTRUCTION_SCHEMA as INSTRUCTION_SCHEMA,
)
from unified_domain_client import (
    INSTRUMENT_TYPE_FOLDER_MAP as INSTRUMENT_TYPE_FOLDER_MAP,
)
from unified_domain_client import (
    MAX_LOOKBACK_DAYS_BY_TIMEFRAME as MAX_LOOKBACK_DAYS_BY_TIMEFRAME,
)
from unified_domain_client import (
    OPTIONAL_CONFIG_FIELDS as OPTIONAL_CONFIG_FIELDS,
)
from unified_domain_client import (
    PATH_REGISTRY as PATH_REGISTRY,
)
from unified_domain_client import (
    REQUIRED_CONFIG_FIELDS as REQUIRED_CONFIG_FIELDS,
)
from unified_domain_client import (
    TIMEFRAME_SECONDS as TIMEFRAME_SECONDS,
)
from unified_domain_client import (
    VALID_ALGORITHMS as VALID_ALGORITHMS,
)
from unified_domain_client import (
    VALID_INSTRUCTION_TYPES as VALID_INSTRUCTION_TYPES,
)
from unified_domain_client import (
    VENUE_CATEGORY_MAP as VENUE_CATEGORY_MAP,
)
from unified_domain_client import (
    ZERO_ALPHA_VENUES as ZERO_ALPHA_VENUES,
)
from unified_domain_client import (
    AthenaReader as AthenaReader,
)
from unified_domain_client import (
    BaseDataReader as BaseDataReader,
)
from unified_domain_client import (
    BaseDataWriter as BaseDataWriter,
)
from unified_domain_client import (
    BaseReader as BaseReader,
)
from unified_domain_client import (
    BaseWriter as BaseWriter,
)
from unified_domain_client import (
    BigQueryCatalog as BigQueryCatalog,
)
from unified_domain_client import (
    BigQueryExternalReader as BigQueryExternalReader,
)
from unified_domain_client import (
    CloudDataProviderBase as CloudDataProviderBase,
)
from unified_domain_client import (
    CloudModelArtifactStore as CloudModelArtifactStore,
)
from unified_domain_client import (
    ConfigValidationError as ConfigValidationError,
)
from unified_domain_client import (
    ConfigValidator as ConfigValidator,
)
from unified_domain_client import (
    DataCompletionChecker as DataCompletionChecker,
)
from unified_domain_client import (
    DataSetSpec as DataSetSpec,
)
from unified_domain_client import (
    DateFilterService as DateFilterService,
)
from unified_domain_client import (
    DateValidationResult as DateValidationResult,
)
from unified_domain_client import (
    DateValidator as DateValidator,
)
from unified_domain_client import (
    DirectReader as DirectReader,
)
from unified_domain_client import (
    DirectWriter as DirectWriter,
)
from unified_domain_client import (
    DomainValidationConfig as DomainValidationConfig,
)
from unified_domain_client import (
    DomainValidationService as DomainValidationService,
)
from unified_domain_client import (
    ExecutionDomainClient as ExecutionDomainClient,
)
from unified_domain_client import (
    FeaturesCalendarDomainClient as FeaturesCalendarDomainClient,
)
from unified_domain_client import (
    FeaturesDataProvider as FeaturesDataProvider,
)
from unified_domain_client import (
    FeaturesDeltaOneDomainClient as FeaturesDeltaOneDomainClient,
)
from unified_domain_client import (
    FeaturesOnchainDomainClient as FeaturesOnchainDomainClient,
)
from unified_domain_client import (
    FeaturesReader as FeaturesReader,
)
from unified_domain_client import (
    FeaturesVolatilityDomainClient as FeaturesVolatilityDomainClient,
)
from unified_domain_client import (
    FeaturesWriter as FeaturesWriter,
)
from unified_domain_client import (
    GlueCatalog as GlueCatalog,
)
from unified_domain_client import (
    InstructionValidationError as InstructionValidationError,
)
from unified_domain_client import (
    InstructionValidator as InstructionValidator,
)
from unified_domain_client import (
    InstrumentKey as InstrumentKey,
)
from unified_domain_client import (
    InstrumentsDataProvider as InstrumentsDataProvider,
)
from unified_domain_client import (
    InstrumentsDomainClient as InstrumentsDomainClient,
)
from unified_domain_client import (
    L2BookCheckpointClient as L2BookCheckpointClient,
)
from unified_domain_client import (
    MarketCandleDataDomainClient as MarketCandleDataDomainClient,
)
from unified_domain_client import (
    MarketCandleDomainClient as MarketCandleDomainClient,
)
from unified_domain_client import (
    MarketDataDomainClient as MarketDataDomainClient,
)
from unified_domain_client import (
    MarketDataProvider as MarketDataProvider,
)
from unified_domain_client import (
    MarketDataReader as MarketDataReader,
)
from unified_domain_client import (
    MarketDataWriter as MarketDataWriter,
)
from unified_domain_client import (
    MarketTickDataDomainClient as MarketTickDataDomainClient,
)
from unified_domain_client import (
    MarketTickDomainClient as MarketTickDomainClient,
)
from unified_domain_client import (
    MLModelsDomainClient as MLModelsDomainClient,
)
from unified_domain_client import (
    MLPredictionsDomainClient as MLPredictionsDomainClient,
)
from unified_domain_client import (
    MLReader as MLReader,
)
from unified_domain_client import (
    MLWriter as MLWriter,
)
from unified_domain_client import (
    PathRegistry as PathRegistry,
)
from unified_domain_client import (
    PnLDomainClient as PnLDomainClient,
)
from unified_domain_client import (
    PositionsDomainClient as PositionsDomainClient,
)
from unified_domain_client import (
    ReadMode as ReadMode,
)
from unified_domain_client import (
    RiskDomainClient as RiskDomainClient,
)
from unified_domain_client import (
    SportsFeaturesDomainClient as SportsFeaturesDomainClient,
)
from unified_domain_client import (
    SportsFixturesDomainClient as SportsFixturesDomainClient,
)
from unified_domain_client import (
    SportsMappingsDomainClient as SportsMappingsDomainClient,
)
from unified_domain_client import (
    SportsOddsDomainClient as SportsOddsDomainClient,
)
from unified_domain_client import (
    SportsTickDataDomainClient as SportsTickDataDomainClient,
)
from unified_domain_client import (
    StandardizedDomainCloudService as StandardizedDomainCloudService,
)
from unified_domain_client import (
    StrategyDomainClient as StrategyDomainClient,
)
from unified_domain_client import (
    TimestampAlignmentResult as TimestampAlignmentResult,
)
from unified_domain_client import (
    TimestampDateValidator as TimestampDateValidator,
)
from unified_domain_client import (
    __all__ as __all__,
)
from unified_domain_client import (
    __version__ as __version__,
)
from unified_domain_client import (
    build_bucket as build_bucket,
)
from unified_domain_client import (
    build_full_uri as build_full_uri,
)
from unified_domain_client import (
    build_path as build_path,
)
from unified_domain_client import (
    create_backtesting_cloud_service as create_backtesting_cloud_service,
)
from unified_domain_client import (
    create_execution_client as create_execution_client,
)
from unified_domain_client import (
    create_features_client as create_features_client,
)
from unified_domain_client import (
    create_features_cloud_service as create_features_cloud_service,
)
from unified_domain_client import (
    create_instruments_client as create_instruments_client,
)
from unified_domain_client import (
    create_market_candle_data_client as create_market_candle_data_client,
)
from unified_domain_client import (
    create_market_data_client as create_market_data_client,
)
from unified_domain_client import (
    create_market_data_cloud_service as create_market_data_cloud_service,
)
from unified_domain_client import (
    create_market_tick_data_client as create_market_tick_data_client,
)
from unified_domain_client import (
    create_strategy_cloud_service as create_strategy_cloud_service,
)
from unified_domain_client import (
    get_available_date_range as get_available_date_range,
)
from unified_domain_client import (
    get_earliest_valid_date as get_earliest_valid_date,
)
from unified_domain_client import (
    get_reader as get_reader,
)
from unified_domain_client import (
    get_spec as get_spec,
)
from unified_domain_client import (
    get_validator as get_validator,
)
from unified_domain_client import (
    get_writer as get_writer,
)
from unified_domain_client import (
    make_completion_checker as make_completion_checker,
)
from unified_domain_client import (
    should_skip_date as should_skip_date,
)
from unified_domain_client import (
    validate_config as validate_config,
)
from unified_domain_client import (
    validate_config_file as validate_config_file,
)
from unified_domain_client import (
    validate_instruction_dataframe as validate_instruction_dataframe,
)
from unified_domain_client import (
    validate_instruction_parquet as validate_instruction_parquet,
)
from unified_domain_client import (
    validate_timestamp_date_alignment as validate_timestamp_date_alignment,
)
