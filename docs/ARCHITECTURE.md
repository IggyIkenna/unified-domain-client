# unified-domain-client — Architecture

## Purpose

`unified-domain-client` (UDC) is the intent-based data access library for the unified trading system. Services never construct raw cloud paths or issue raw storage calls — they call typed domain clients that know the partition layout, bucket conventions, schema, and validation rules for each dataset.

UDC is a T1 library (depends on T0 contracts). It depends on `unified-cloud-interface` for all storage I/O and on `unified-trading-library` for validation infrastructure. Services at T2+ depend on UDC.

## Architecture Overview

```
Service code
    ↓ calls
Domain Client (e.g. MarketCandleDomainClient)
    ↓ resolves path via
PathRegistry / build_path()
    ↓ reads/writes via
StandardizedDomainCloudService
    ↓ delegates to
unified-cloud-interface (StorageClient / AnalyticsClient)
    ↓ dispatches to
GCS / S3 / local backend
```

## Component Map

### Domain Clients (`clients/`)

17 typed domain clients across the full data platform, all lazy-loaded on first access to keep package import cheap:

| Client                           | Domain      | Dataset                                    |
| -------------------------------- | ----------- | ------------------------------------------ |
| `InstrumentsDomainClient`        | Instruments | Instrument availability, corporate actions |
| `MarketTickDomainClient`         | Market data | Raw tick data (per instrument, venue)      |
| `MarketCandleDomainClient`       | Market data | OHLCV candles (multi-timeframe)            |
| `FeaturesDeltaOneDomainClient`   | Features    | Delta-one feature groups (multi-timeframe) |
| `FeaturesCalendarDomainClient`   | Features    | Calendar / economic events features        |
| `FeaturesOnchainDomainClient`    | Features    | On-chain blockchain features               |
| `FeaturesVolatilityDomainClient` | Features    | Volatility surface features                |
| `MLModelsDomainClient`           | ML          | Trained model artifacts                    |
| `MLPredictionsDomainClient`      | ML          | Inference prediction snapshots             |
| `StrategyDomainClient`           | Strategy    | Strategy configs and signals               |
| `ExecutionDomainClient`          | Execution   | Trade instructions and fills               |
| `PositionsDomainClient`          | Risk        | Position snapshots                         |
| `PnLDomainClient`                | Risk        | Profit and loss data                       |
| `RiskDomainClient`               | Risk        | Risk metrics and exposure                  |
| `L2BookCheckpointClient`         | Liquidity   | L2 order book checkpoints                  |
| `SportsOddsDomainClient`         | Sports      | Sports odds and book updates               |
| `SportsTickDataDomainClient`     | Sports      | Sports event tick data                     |

Legacy rich clients (`MarketDataDomainClient`, `MarketTickDataDomainClient`, `MarketCandleDataDomainClient`) are preserved for backward compatibility.

### Path Registry (`paths/`)

`PathRegistry` is a static registry of `DataSetSpec` objects mapping dataset names to their bucket templates and path templates. Paths follow a consistent Hive-style partition layout:

```
{bucket}/{path_template}/file_template
```

Example specs:

- `raw_tick_data`: `market-data-tick-{category}-{project_id}/raw_tick_data/by_date/day={date}/data_type={data_type}/instrument_type={instrument_type}/venue={venue}/{instrument_key}.parquet`
- `processed_candles`: `market-data-tick-{category}-{project_id}/processed_candles/by_date/day={date}/timeframe={timeframe}/.../{instrument_id}.parquet`
- `delta_one_features`: `features-delta-one-{category}-{project_id}/by_date/day={date}/feature_group={feature_group}/timeframe={timeframe}/{instrument_id}.parquet`

Use `build_path()`, `build_bucket()`, and `build_full_uri()` to generate paths from `DataSetSpec`:

```python
from unified_domain_client import build_path, build_bucket, get_spec

spec = get_spec("raw_tick_data")
bucket = build_bucket(spec, category="CEFI", project_id="my-project")
path = build_path(spec, date="2024-01-15", data_type="spot", instrument_type="crypto", venue="binance")
```

### Readers and Writers (`readers/`, `writers/`)

Domain readers and writers abstract the parquet I/O for each dataset family:

| Class                                         | Purpose                                                       |
| --------------------------------------------- | ------------------------------------------------------------- |
| `BaseDataReader` / `BaseDataWriter`           | Core parquet read/write logic via UCI storage                 |
| `MarketDataReader` / `MarketDataWriter`       | Market candle and tick data with timestamp validation         |
| `FeaturesReader` / `FeaturesWriter`           | Feature datasets with UTC alignment checks                    |
| `MLReader` / `MLWriter`                       | Model artifacts and prediction snapshots                      |
| `DirectReader` / `DirectWriter`               | Raw path access for ad-hoc reads                              |
| `BigQueryExternalReader`                      | Read from BigQuery external tables over GCS                   |
| `AthenaReader`                                | Read from AWS Athena tables                                   |
| `get_reader(dataset)` / `get_writer(dataset)` | Factory: returns the correct reader/writer for a dataset name |

### Validation (`validation.py`, `date_validation.py`, `timestamp_validation.py`)

Domain-specific validation rules enforced at read/write time:

| Module                                   | Validates                                                                                        |
| ---------------------------------------- | ------------------------------------------------------------------------------------------------ |
| `DomainValidationService`                | Per-domain flag presets: candle count, midnight boundaries, UTC alignment, sparse event ordering |
| `DateValidator` / `DateValidationResult` | Date range validity, earliest-available date per instrument/venue                                |
| `TimestampDateValidator`                 | UTC timestamp alignment — timestamps must match the declared date partition                      |
| `DataCompletionChecker`                  | Whether a date partition is fully written (all expected files present)                           |

Domain configs:

- `market_data`: validate candle count + midnight boundaries + UTC alignment
- `features`: UTC alignment only (derived data, no candle count)
- `strategy`: sparse event ordering + UTC for orders only
- `execution`: sparse event ordering only
- `ml`: no domain validation (transformation only)

### Schema Validation (`schemas/`)

Config and instruction validation for the execution pipeline:

- `ConfigValidator` / `validate_config()` — validates strategy config files (algorithm, venue, instrument type)
- `InstructionValidator` / `validate_instruction_dataframe()` — validates instruction DataFrames against `INSTRUCTION_SCHEMA`
- `VALID_INSTRUCTION_TYPES` — TRADE, SWAP, LEND, BORROW, STAKE, TRANSFER
- `VALID_ALGORITHMS` — supported execution algorithms
- `VENUE_CATEGORY_MAP` — maps venues to CEFI / TRADFI / DEFI categories

### StandardizedDomainCloudService (`standardized_service.py`)

Lower-level cloud service base used by domain clients internally. Wraps UCI `upload_to_storage()` and `download_from_storage()` with parquet/csv/json deserialization. Domain clients compose this with path resolution logic rather than extending it.

### Catalog (`catalog/`)

`BigQueryCatalog` and `GlueCatalog` for metadata catalog operations (table registration, schema management) on GCP and AWS respectively.

### Sports Domain (`sports/`)

Specialized clients for sports betting data: odds ticks, match fixtures, model mappings, and computed sports features. Lazy-loaded to keep base import weight minimal.

### Date Filtering (`instrument_date_filter.py`, `lookback_constants.py`)

`DateFilterService` filters instrument lists by availability date. `FEATURE_GROUP_LOOKBACK` and `MAX_LOOKBACK_DAYS_BY_TIMEFRAME` define lookback windows per feature group and timeframe. `TIMEFRAME_SECONDS` maps timeframe labels (`1min`, `5min`, `1h`, `1d`) to seconds.

## Lazy Loading Pattern

Heavy modules (cloud SDK dependencies, pandas I/O) load only when accessed. The `__getattr__` hook in `__init__.py` defers all client/factory/cloud-provider imports:

```python
# This does NOT import pandas or google-cloud-storage:
import unified_domain_client

# This triggers the lazy import of clients module:
client = unified_domain_client.MarketCandleDomainClient(...)
```

## References

- `docs/CONFIGURATION.md` — environment variables
- `docs/TESTING.md` — running tests
- `unified-cloud-interface` — storage backend
- `unified-trading-codex/03-observability/` — domain timestamp semantics
