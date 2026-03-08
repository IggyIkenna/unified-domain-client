# unified-domain-client — Testing

## Run Tests

```bash
pytest tests/ -v
```

Coverage target: 70%+. All unit tests mock cloud dependencies — no real GCP or AWS calls required.

## Test Structure

```
tests/
└── unit/
    ├── test_clients.py                   # Domain client methods (mocked StandardizedDomainCloudService)
    ├── test_cloud_data_provider.py        # CloudDataProviderBase, FeaturesDataProvider, etc.
    ├── test_date_filter_service.py        # DateFilterService, instrument date filtering
    ├── test_instruction_schema.py         # ConfigValidator, InstructionValidator, validate_config()
    ├── test_readers.py                    # BaseDataReader, MarketDataReader, FeaturesReader, get_reader()
    ├── test_validation.py                 # DomainValidationService domain configs
    ├── test_udc_instruments_methods.py    # InstrumentsDomainClient-specific methods
    ├── test_coverage_boost_udc.py         # Additional coverage for edge cases
    ├── test_imports.py                    # Verify all public API exports are importable
    ├── test_imports_part2.py              # Verify lazy-loaded names via __getattr__
    └── conftest.py                        # Shared fixtures
```

## Testing Domain Clients

Domain clients use `StandardizedDomainCloudService` internally. Mock it to test business logic without cloud calls:

```python
from unittest.mock import MagicMock, patch
import pandas as pd

@patch("unified_domain_client.clients.instruments.StandardizedDomainCloudService")
@patch("unified_domain_client.clients.instruments.UnifiedCloudConfig")
def test_instruments_client(mock_config, mock_service):
    from unified_domain_client.clients import InstrumentsDomainClient

    mock_config.return_value.gcp_project_id = "test-project"
    mock_config.return_value.instruments_gcs_bucket = "instruments-store-cefi-test-project"
    mock_config.return_value.instruments_bigquery_dataset = "instruments"

    client = InstrumentsDomainClient(
        project_id="test-project",
        storage_bucket="instruments-store-cefi-test-project",
        analytics_dataset="instruments",
    )

    # Mock the download to return a test DataFrame
    client.get_instruments_for_date = MagicMock(return_value=pd.DataFrame([
        {"instrument_key": "BTC-USD", "tick_size": "0.01", "ccxt_symbol": "BTCUSDT",
         "ccxt_exchange": "binance", "data_types": "trades,book_snapshot_5"}
    ]))

    result = client.get_trading_parameters("2024-01-15", "BTC-USD")
    assert result is not None
    assert result["ccxt_symbol"] == "BTCUSDT"
```

## Testing Path Registry

Path resolution is pure computation — no mocking needed:

```python
from unified_domain_client import build_path, build_bucket, build_full_uri, get_spec

def test_path_registry_raw_tick():
    spec = get_spec("raw_tick_data")
    bucket = build_bucket(spec, category="CEFI", project_id="my-project")
    assert bucket == "market-data-tick-cefi-my-project"

    path = build_path(
        spec,
        date="2024-01-15",
        data_type="spot",
        instrument_type="crypto",
        venue="binance",
    )
    assert "day=2024-01-15" in path
    assert "venue=binance" in path

    uri = build_full_uri(spec, category="CEFI", project_id="my-project",
                         date="2024-01-15", data_type="spot",
                         instrument_type="crypto", venue="binance",
                         instrument_key="BTC-USD")
    assert uri.startswith("gs://market-data-tick-cefi-my-project/")
    assert uri.endswith("BTC-USD.parquet")
```

## Testing Validation

`DomainValidationService` validates DataFrames according to domain-specific rules:

```python
from unittest.mock import MagicMock
from unified_domain_client import DomainValidationService, DomainValidationConfig
import pandas as pd

def test_features_validation_skips_candle_count():
    config = DomainValidationConfig(domain="features")
    service = DomainValidationService(config)

    df = pd.DataFrame({"timestamp": pd.date_range("2024-01-15", periods=10, freq="1min", tz="UTC"),
                       "feature_a": range(10)})
    result = service.validate(df, date="2024-01-15")
    assert result.is_valid
```

## Testing Schema Validation

Config and instruction validators are pure functions — no cloud dependencies:

```python
from unified_domain_client import validate_config, ConfigValidationError

def test_config_validation_valid():
    config = {
        "algorithm": "twap",
        "venue": "binance",
        "instrument_type": "spot",
        "instruments": ["BTC-USD"],
    }
    result = validate_config(config)
    assert result.is_valid

def test_config_validation_invalid_algorithm():
    import pytest
    config = {"algorithm": "unknown-algo", "venue": "binance", "instrument_type": "spot"}
    with pytest.raises(ConfigValidationError):
        validate_config(config)
```

## Testing Date Filtering

```python
from unified_domain_client import DateFilterService, DateValidator

def test_date_filter_skips_before_earliest():
    validator = DateValidator(earliest_dates={"BTC-USD:binance": "2021-01-01"})
    service = DateFilterService(validator)

    instruments = ["BTC-USD:binance", "ETH-USD:binance"]
    filtered = service.filter_for_date("2020-12-31", instruments, venue="binance")
    assert "BTC-USD:binance" not in filtered  # before earliest date

def test_date_filter_includes_after_earliest():
    validator = DateValidator(earliest_dates={"BTC-USD:binance": "2021-01-01"})
    service = DateFilterService(validator)

    filtered = service.filter_for_date("2024-01-15", ["BTC-USD:binance"], venue="binance")
    assert "BTC-USD:binance" in filtered
```

## Testing Lazy Imports

Verify that lazy-loaded names can be accessed without import errors:

```python
def test_lazy_domain_clients_importable():
    import unified_domain_client as udc

    # These trigger __getattr__ lazy imports
    assert udc.MarketCandleDomainClient is not None
    assert udc.FeaturesDeltaOneDomainClient is not None
    assert udc.MLPredictionsDomainClient is not None
    assert udc.SportsOddsDomainClient is not None
```

## Test Isolation

UDC clients call into UCI, which maintains singleton caches. If tests set `CLOUD_PROVIDER=local` and create clients, clear UCI caches between tests:

```python
import pytest

@pytest.fixture(autouse=True)
def reset_uci_caches():
    from unified_cloud_interface.factory import clear_client_caches
    clear_client_caches()
    yield
    clear_client_caches()
```

## Running a Specific Test File

```bash
pytest tests/unit/test_clients.py -v
pytest tests/unit/test_instruction_schema.py -v -k "test_config_validation"
```
