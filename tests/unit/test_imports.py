"""Test package imports and basic exports."""

from datetime import UTC

import pytest


def test_import_unified_domain_client():
    """Test that unified_domain_client can be imported (lightweight exports)."""
    import unified_domain_client

    # Lightweight exports - always available (no gcs_operations)
    assert hasattr(unified_domain_client, "validate_timestamp_date_alignment")
    assert hasattr(unified_domain_client, "DateFilterService")
    assert hasattr(unified_domain_client, "validate_config")
    assert hasattr(unified_domain_client, "InstrumentKey")

    # Heavy exports (clients, standardized_service) require gcs_operations - skip if unavailable
    try:
        _ = getattr(unified_domain_client, "StandardizedDomainCloudService")
        _ = getattr(unified_domain_client, "create_market_data_cloud_service")
    except ModuleNotFoundError:
        pass  # gcs_operations not in UCS - lazy load fails when accessed


def test_date_validator_import():
    """Test DateValidator and related exports."""
    from unified_domain_client import DateValidator, get_validator, should_skip_date

    assert DateValidator is not None
    assert callable(get_validator)
    assert callable(should_skip_date)


# ===========================================================================
# id_conventions
# ===========================================================================


def test_validate_strategy_id_valid():
    """Test validate_strategy_id with valid ID."""
    from unified_domain_client.id_conventions import validate_strategy_id

    assert validate_strategy_id("CEFI_BTC_momentum_SCE_1H_V1") is True
    assert validate_strategy_id("DEFI_ETH_lido-staking_SCE_5M_V1") is True
    assert validate_strategy_id("TRADFI_ES_mean-rev_HUF_24H_V3") is True


def test_validate_strategy_id_invalid():
    """Test validate_strategy_id with invalid ID."""
    from unified_domain_client.id_conventions import validate_strategy_id

    # Invalid category
    assert validate_strategy_id("INVALID_BTC_momentum_SCE_1H_V1") is False
    # Missing version
    assert validate_strategy_id("CEFI_BTC_momentum_SCE_1H") is False
    # Empty
    assert validate_strategy_id("") is False
    # Completely wrong format
    assert validate_strategy_id("not-a-strategy-id") is False


# ===========================================================================
# market_category
# ===========================================================================


def test_get_bucket_for_category_default():
    """Test get_bucket_for_category with default args."""
    from unified_domain_client.market_category import get_bucket_for_category

    result = get_bucket_for_category("CEFI", "my-project")
    assert result == "cefi-store-my-project"


def test_get_bucket_for_category_test_mode():
    """Test get_bucket_for_category with test_mode=True."""
    from unified_domain_client.market_category import get_bucket_for_category

    result = get_bucket_for_category("DEFI", "my-project", test_mode=True)
    assert result == "defi-store-my-project_test"


def test_get_bucket_for_category_empty():
    """Test get_bucket_for_category with empty category falls back to cefi."""
    from unified_domain_client.market_category import get_bucket_for_category

    result = get_bucket_for_category("", "proj")
    assert result == "cefi-store-proj"


# ===========================================================================
# factories
# ===========================================================================


def test_create_backtesting_cloud_service():
    """Test create_backtesting_cloud_service factory."""
    from unified_domain_client.factories import create_backtesting_cloud_service

    svc = create_backtesting_cloud_service("test-project")
    assert svc.domain == "backtest"
    assert svc.cloud_target.project_id == "test-project"
    assert "backtest-store" in svc.cloud_target.gcs_bucket


def test_create_features_cloud_service():
    """Test create_features_cloud_service factory."""
    from unified_domain_client.factories import create_features_cloud_service

    svc = create_features_cloud_service("test-project", gcs_bucket="custom-bucket")
    assert svc.domain == "features"
    assert svc.cloud_target.gcs_bucket == "custom-bucket"


def test_create_instruments_cloud_service():
    """Test create_instruments_cloud_service factory."""
    from unified_domain_client.factories import create_instruments_cloud_service

    svc = create_instruments_cloud_service("test-project")
    assert svc.domain == "instruments"


def test_create_market_data_cloud_service():
    """Test create_market_data_cloud_service factory."""
    from unified_domain_client.factories import create_market_data_cloud_service

    svc = create_market_data_cloud_service("test-project")
    assert svc.domain == "market_data"


def test_create_strategy_cloud_service():
    """Test create_strategy_cloud_service factory."""
    from unified_domain_client.factories import create_strategy_cloud_service

    svc = create_strategy_cloud_service("test-project")
    assert svc.domain == "strategy"


# ===========================================================================
# standardized_service
# ===========================================================================


def test_standardized_service_init():
    """Test StandardizedDomainCloudService init."""
    from unified_domain_client.cloud_target import CloudTarget
    from unified_domain_client.standardized_service import StandardizedDomainCloudService

    target = CloudTarget(project_id="p", gcs_bucket="b", bigquery_dataset="d")
    svc = StandardizedDomainCloudService(domain="test", cloud_target=target)
    assert svc.domain == "test"
    assert svc.cloud_target.gcs_bucket == "b"


def test_standardized_service_query_bigquery_raises():
    """Test query_bigquery raises NotImplementedError."""
    from unified_domain_client.cloud_target import CloudTarget
    from unified_domain_client.standardized_service import StandardizedDomainCloudService

    target = CloudTarget(project_id="p", gcs_bucket="b", bigquery_dataset="d")
    svc = StandardizedDomainCloudService(domain="test", cloud_target=target)
    with pytest.raises(NotImplementedError):
        svc.query_bigquery("SELECT 1")


def test_make_domain_service():
    """Test make_domain_service factory function."""
    from unified_domain_client.standardized_service import make_domain_service

    svc = make_domain_service("instruments", bucket="my-bucket", project_id="proj", dataset="my_ds")
    assert svc.domain == "instruments"
    assert svc.cloud_target.gcs_bucket == "my-bucket"


def test_create_domain_cloud_service():
    """Test create_domain_cloud_service factory function."""
    from unified_domain_client.cloud_target import CloudTarget
    from unified_domain_client.standardized_service import create_domain_cloud_service

    target = CloudTarget(project_id="p", gcs_bucket="bucket", bigquery_dataset="ds")
    svc = create_domain_cloud_service("market_data", target)
    assert svc.domain == "market_data"


# ===========================================================================
# schemas/instrument_key
# ===========================================================================


def test_instrument_key_from_string():
    """Test InstrumentKey.from_string parses correctly."""
    from unified_domain_client.schemas.instrument_key import InstrumentKey

    key = InstrumentKey.from_string("BINANCE-SPOT:SPOT_PAIR:BTC-USDT")
    assert key.symbol == "BTC-USDT"
    assert str(key).startswith("BINANCE-SPOT")


def test_instrument_key_from_string_invalid():
    """Test InstrumentKey.from_string raises on bad format."""
    from unified_domain_client.schemas.instrument_key import InstrumentKey

    with pytest.raises(ValueError):
        InstrumentKey.from_string("INVALID")


def test_instrument_key_parse_for_tardis_binance():
    """Test InstrumentKey.parse_for_tardis for Binance."""
    from unified_domain_client.schemas.instrument_key import InstrumentKey

    result = InstrumentKey.parse_for_tardis("BINANCE-SPOT:SPOT_PAIR:SOL-USDT")
    assert result["venue"] == "BINANCE-SPOT"
    assert "tardis_symbol" in result


def test_instrument_key_parse_for_tardis_deribit():
    """Test InstrumentKey.parse_for_tardis for Deribit."""
    from unified_domain_client.schemas.instrument_key import InstrumentKey

    result = InstrumentKey.parse_for_tardis("DERIBIT:OPTION:BTC-USD")
    assert result["venue"] == "DERIBIT"


def test_instrument_key_parse_for_tardis_upbit():
    """Test InstrumentKey.parse_for_tardis for Upbit (reverses BASE-QUOTE)."""
    from unified_domain_client.schemas.instrument_key import InstrumentKey

    result = InstrumentKey.parse_for_tardis("UPBIT:SPOT_PAIR:VET-KRW")
    assert "KRW-VET" in result.get("tardis_symbol", "")


def test_instrument_key_parse_for_tardis_coinbase():
    """Test InstrumentKey.parse_for_tardis for Coinbase."""
    from unified_domain_client.schemas.instrument_key import InstrumentKey

    result = InstrumentKey.parse_for_tardis("COINBASE:SPOT_PAIR:SOL-USD")
    assert result.get("tardis_symbol", "").upper() == "SOL-USD"


def test_instrument_key_parse_for_tardis_invalid():
    """Test InstrumentKey.parse_for_tardis raises on bad format."""
    from unified_domain_client.schemas.instrument_key import InstrumentKey

    with pytest.raises(ValueError):
        InstrumentKey.parse_for_tardis("ONLY_ONE_PART")


# ===========================================================================
# paths/registry
# ===========================================================================


def test_paths_registry_get_spec_known():
    """Test get_spec returns spec for known dataset."""
    from unified_domain_client.paths.registry import get_spec

    spec = get_spec("processed_candles")
    assert spec.name == "processed_candles"


def test_paths_registry_get_spec_unknown():
    """Test get_spec raises KeyError for unknown dataset."""
    from unified_domain_client.paths.registry import get_spec

    with pytest.raises(KeyError, match="not in PATH_REGISTRY"):
        get_spec("nonexistent_dataset")


def test_paths_registry_build_bucket():
    """Test build_bucket returns correct bucket name."""
    from unified_domain_client.paths.registry import build_bucket

    bucket = build_bucket("instruments", project_id="test-proj", category="cefi")
    assert "test-proj" in bucket


def test_paths_registry_build_path():
    """Test build_path returns correct path."""
    from unified_domain_client.paths.registry import build_path

    path = build_path("instruments", date="2024-01-15", venue="BINANCE")
    assert "2024-01-15" in path


def test_paths_registry_build_full_uri():
    """Test build_full_uri returns gs:// URI."""
    from unified_domain_client.paths.registry import build_full_uri

    uri = build_full_uri("instruments", project_id="test-proj", category="cefi", date="2024-01-15", venue="BINANCE")
    assert uri.startswith("gs://")


def test_path_registry_class_all_patterns():
    """Test PathRegistry.all_patterns returns dict."""
    from unified_domain_client.paths.registry import PathRegistry

    patterns = PathRegistry.all_patterns()
    assert isinstance(patterns, dict)
    assert len(patterns) > 0


def test_path_registry_class_format():
    """Test PathRegistry.format substitutes variables."""
    from unified_domain_client.paths.registry import PathRegistry

    result = PathRegistry.format(PathRegistry.MARKET_TICK_RAW, date="2024-01-15", instrument="BTC-USDT")
    assert "2024-01-15" in result
    assert "BTC-USDT" in result


# ===========================================================================
# readers/factory
# ===========================================================================


def test_get_reader_auto_requires_storage_client():
    """Test get_reader(AUTO) raises ValueError without storage_client."""
    from unified_domain_client.paths import ReadMode
    from unified_domain_client.readers.factory import get_reader

    with pytest.raises(ValueError, match="storage_client required"):
        get_reader(storage_client=None, mode=ReadMode.AUTO)


def test_get_reader_bq_external_requires_project_id():
    """Test get_reader(BQ_EXTERNAL) raises ValueError without project_id."""
    from unified_domain_client.paths import ReadMode
    from unified_domain_client.readers.factory import get_reader

    with pytest.raises(ValueError, match="project_id"):
        get_reader(mode=ReadMode.BQ_EXTERNAL)


def test_get_reader_bq_external_requires_bq_dataset():
    """Test get_reader(BQ_EXTERNAL) raises ValueError without bq_dataset."""

    from unified_domain_client.paths import ReadMode
    from unified_domain_client.readers.factory import get_reader

    with pytest.raises(ValueError, match="bq_dataset"):
        get_reader(mode=ReadMode.BQ_EXTERNAL, project_id="p")


def test_get_reader_bq_external_returns_reader():
    """Test get_reader(BQ_EXTERNAL) returns BigQueryExternalReader."""
    from unified_domain_client.paths import ReadMode
    from unified_domain_client.readers.bq_external import BigQueryExternalReader
    from unified_domain_client.readers.factory import get_reader

    reader = get_reader(mode=ReadMode.BQ_EXTERNAL, project_id="p", bq_dataset="ds")
    assert isinstance(reader, BigQueryExternalReader)


def test_get_reader_athena_requires_account_id():
    """Test get_reader(ATHENA) raises ValueError without account_id."""
    from unified_domain_client.paths import ReadMode
    from unified_domain_client.readers.factory import get_reader

    with pytest.raises(ValueError, match="account_id"):
        get_reader(mode=ReadMode.ATHENA)


def test_get_reader_athena_requires_glue_database():
    """Test get_reader(ATHENA) raises ValueError without glue_database."""
    from unified_domain_client.paths import ReadMode
    from unified_domain_client.readers.factory import get_reader

    with pytest.raises(ValueError, match="glue_database"):
        get_reader(mode=ReadMode.ATHENA, account_id="123")


def test_get_reader_athena_returns_reader():
    """Test get_reader(ATHENA) returns AthenaReader."""
    from unified_domain_client.paths import ReadMode
    from unified_domain_client.readers.athena import AthenaReader
    from unified_domain_client.readers.factory import get_reader

    reader = get_reader(mode=ReadMode.ATHENA, account_id="123456789", glue_database="db")
    assert isinstance(reader, AthenaReader)


def test_get_reader_unknown_mode_raises():
    """Test get_reader raises ValueError for unknown mode."""

    from unified_domain_client.readers.factory import get_reader

    with pytest.raises(ValueError, match="Unknown ReadMode"):
        get_reader(mode="invalid_mode")  # type: ignore[arg-type]


# ===========================================================================
# sports clients (smoke tests - just import and instantiate)
# ===========================================================================


def test_sports_clients_importable():
    """Test sports domain clients can be imported."""
    from unified_domain_client.sports import (
        SportsFeaturesDomainClient,
        SportsFixturesDomainClient,
        SportsMappingsDomainClient,
        SportsOddsDomainClient,
        SportsTickDataDomainClient,
    )

    assert SportsFeaturesDomainClient is not None
    assert SportsFixturesDomainClient is not None
    assert SportsMappingsDomainClient is not None
    assert SportsOddsDomainClient is not None
    assert SportsTickDataDomainClient is not None


def test_sports_clients_instantiate_with_mocks():
    """Test sports clients can be instantiated with mocked deps."""
    from unittest.mock import patch

    with patch("unified_domain_client.sports.features_client.UnifiedCloudConfig") as mock_cfg, \
         patch("unified_domain_client.sports.features_client.StandardizedDomainCloudService"):
        mock_cfg.return_value.gcp_project_id = "test-project"
        from unified_domain_client.sports.features_client import SportsFeaturesDomainClient
        client = SportsFeaturesDomainClient(project_id="test-project", gcs_bucket="test-bucket")
        assert client is not None

    with patch("unified_domain_client.sports.fixtures_client.UnifiedCloudConfig") as mock_cfg, \
         patch("unified_domain_client.sports.fixtures_client.StandardizedDomainCloudService"):
        mock_cfg.return_value.gcp_project_id = "test-project"
        from unified_domain_client.sports.fixtures_client import SportsFixturesDomainClient
        client2 = SportsFixturesDomainClient(project_id="test-project", gcs_bucket="test-bucket")
        assert client2 is not None

    with patch("unified_domain_client.sports.mappings_client.UnifiedCloudConfig") as mock_cfg, \
         patch("unified_domain_client.sports.mappings_client.StandardizedDomainCloudService"):
        mock_cfg.return_value.gcp_project_id = "test-project"
        from unified_domain_client.sports.mappings_client import SportsMappingsDomainClient
        client3 = SportsMappingsDomainClient(project_id="test-project", gcs_bucket="test-bucket")
        assert client3 is not None

    with patch("unified_domain_client.sports.odds_client.UnifiedCloudConfig") as mock_cfg, \
         patch("unified_domain_client.sports.odds_client.StandardizedDomainCloudService"):
        mock_cfg.return_value.gcp_project_id = "test-project"
        from unified_domain_client.sports.odds_client import SportsOddsDomainClient
        client4 = SportsOddsDomainClient(project_id="test-project", gcs_bucket="test-bucket")
        assert client4 is not None

    with patch("unified_domain_client.sports.tick_data_client.UnifiedCloudConfig") as mock_cfg, \
         patch("unified_domain_client.sports.tick_data_client.StandardizedDomainCloudService"):
        mock_cfg.return_value.gcp_project_id = "test-project"
        from unified_domain_client.sports.tick_data_client import SportsTickDataDomainClient
        client5 = SportsTickDataDomainClient(project_id="test-project", gcs_bucket="test-bucket")
        assert client5 is not None


# ===========================================================================
# timestamp_validation
# ===========================================================================


def test_timestamp_date_validator_empty_df():
    """Test TimestampDateValidator with empty DataFrame returns valid."""
    from datetime import date

    import pandas as pd

    from unified_domain_client.timestamp_validation import TimestampDateValidator

    validator = TimestampDateValidator()
    result = validator.validate(pd.DataFrame(), expected_date=date(2024, 1, 15))
    assert result.valid is True
    assert result.alignment_percentage == 100.0


def test_timestamp_date_validator_no_timestamp_col():
    """Test TimestampDateValidator fails when no timestamp column found."""
    from datetime import date

    import pandas as pd

    from unified_domain_client.timestamp_validation import TimestampDateValidator

    validator = TimestampDateValidator()
    result = validator.validate(pd.DataFrame({"x": [1]}), expected_date=date(2024, 1, 15), timestamp_col="ts")
    assert result.valid is False
    assert "No timestamp column found" in result.errors[0]


def test_timestamp_date_validator_aligned():
    """Test TimestampDateValidator succeeds when timestamps align with expected date."""
    from datetime import date
    from datetime import datetime as dt

    import pandas as pd

    from unified_domain_client.timestamp_validation import TimestampDateValidator

    # 2024-01-15 00:00:00 UTC in nanoseconds
    ts_ns = int(dt(2024, 1, 15, 12, 0, 0, tzinfo=UTC).timestamp() * 1e9)
    df = pd.DataFrame({"timestamp": [ts_ns, ts_ns + 1000]})
    validator = TimestampDateValidator(alignment_threshold=90.0, timestamp_unit="ns")
    result = validator.validate(df, expected_date=date(2024, 1, 15))
    assert result.valid


def test_timestamp_date_validator_wrong_date():
    """Test TimestampDateValidator fails when timestamps don't match expected date."""
    from datetime import date
    from datetime import datetime as dt

    import pandas as pd

    from unified_domain_client.timestamp_validation import TimestampDateValidator

    # 2024-01-16 timestamps
    ts_ns = int(dt(2024, 1, 16, 12, 0, 0, tzinfo=UTC).timestamp() * 1e9)
    df = pd.DataFrame({"timestamp": [ts_ns, ts_ns + 1000]})
    validator = TimestampDateValidator(alignment_threshold=90.0, timestamp_unit="ns")
    result = validator.validate(df, expected_date=date(2024, 1, 15))
    assert not result.valid
    assert result.alignment_percentage == 0.0


def test_timestamp_date_validator_us_unit():
    """Test TimestampDateValidator with microsecond timestamps."""
    from datetime import date
    from datetime import datetime as dt

    import pandas as pd

    from unified_domain_client.timestamp_validation import TimestampDateValidator

    ts_us = int(dt(2024, 1, 15, 0, 0, 0, tzinfo=UTC).timestamp() * 1e6)
    df = pd.DataFrame({"timestamp": [ts_us]})
    validator = TimestampDateValidator(alignment_threshold=90.0, timestamp_unit="us")
    result = validator.validate(df, expected_date=date(2024, 1, 15))
    assert result.valid


def test_timestamp_date_validator_ms_unit():
    """Test TimestampDateValidator with millisecond timestamps."""
    from datetime import date
    from datetime import datetime as dt

    import pandas as pd

    from unified_domain_client.timestamp_validation import TimestampDateValidator

    ts_ms = int(dt(2024, 1, 15, 0, 0, 0, tzinfo=UTC).timestamp() * 1e3)
    df = pd.DataFrame({"timestamp": [ts_ms]})
    validator = TimestampDateValidator(alignment_threshold=90.0, timestamp_unit="ms")
    result = validator.validate(df, expected_date=date(2024, 1, 15))
    assert result.valid


def test_validate_timestamp_date_alignment_none_df():
    """Test validate_timestamp_date_alignment with None df returns valid."""
    from unified_domain_client.timestamp_validation import validate_timestamp_date_alignment

    result = validate_timestamp_date_alignment(None)
    assert result.valid is True


def test_validate_timestamp_date_alignment_empty_df():
    """Test validate_timestamp_date_alignment with empty df returns valid."""
    import pandas as pd

    from unified_domain_client.timestamp_validation import validate_timestamp_date_alignment

    result = validate_timestamp_date_alignment(pd.DataFrame())
    assert result.valid is True


def test_validate_timestamp_date_alignment_with_date_string():
    """Test validate_timestamp_date_alignment with string date."""
    from datetime import datetime as dt

    import pandas as pd

    from unified_domain_client.timestamp_validation import validate_timestamp_date_alignment

    ts_ns = int(dt(2024, 1, 15, 12, 0, 0, tzinfo=UTC).timestamp() * 1e9)
    df = pd.DataFrame({"timestamp": [ts_ns]})
    result = validate_timestamp_date_alignment(df, expected_date="2024-01-15")
    assert result.valid


def test_validate_timestamp_date_alignment_with_path():
    """Test validate_timestamp_date_alignment extracts date from path."""
    from datetime import datetime as dt

    import pandas as pd

    from unified_domain_client.timestamp_validation import validate_timestamp_date_alignment

    ts_ns = int(dt(2024, 1, 15, 12, 0, 0, tzinfo=UTC).timestamp() * 1e9)
    df = pd.DataFrame({"timestamp": [ts_ns]})
    result = validate_timestamp_date_alignment(df, path="market_data/day=2024-01-15/data.parquet")
    assert result.valid


def test_validate_timestamp_date_alignment_no_date():
    """Test validate_timestamp_date_alignment with no date info returns valid."""
    import pandas as pd

    from unified_domain_client.timestamp_validation import validate_timestamp_date_alignment

    df = pd.DataFrame({"timestamp": [1000000]})
    result = validate_timestamp_date_alignment(df)
    assert result.valid is True


def test_validate_timestamp_date_alignment_with_date_object():
    """Test validate_timestamp_date_alignment with date object."""
    from datetime import date
    from datetime import datetime as dt

    import pandas as pd

    from unified_domain_client.timestamp_validation import validate_timestamp_date_alignment

    ts_ns = int(dt(2024, 1, 15, 12, 0, 0, tzinfo=UTC).timestamp() * 1e9)
    df = pd.DataFrame({"timestamp": [ts_ns]})
    result = validate_timestamp_date_alignment(df, expected_date=date(2024, 1, 15))
    assert result.valid


# ===========================================================================
# date_validation
# ===========================================================================


def test_date_validator_empty_config_returns_valid():
    """Test DateValidator with no config file returns valid (no start date configured)."""
    from pathlib import Path

    from unified_domain_client.date_validation import DateValidator

    # Point to a nonexistent path so config is empty
    validator = DateValidator(config_path=Path("/nonexistent/path.yaml"))
    result = validator.check_date("2023-01-01", "CEFI")
    assert result.is_valid is True
    assert result.reason == "No start date configured - allowing processing"


def test_date_validator_returns_valid_for_recent_date():
    """Test DateValidator.check_date returns valid for recent date when no config."""
    from pathlib import Path

    from unified_domain_client.date_validation import DateValidator

    validator = DateValidator(config_path=Path("/nonexistent/path.yaml"))
    result = validator.check_date("2024-01-15", "CEFI", venue="BINANCE", timeframe="1h")
    assert result.is_valid is True


def test_date_validator_get_earliest_raw_data_date_no_config():
    """Test get_earliest_raw_data_date returns None when no config."""
    from pathlib import Path

    from unified_domain_client.date_validation import DateValidator

    validator = DateValidator(config_path=Path("/nonexistent/path.yaml"))
    result = validator.get_earliest_raw_data_date("market-tick-data-handler", "CEFI")
    assert result is None


def test_date_validator_get_earliest_valid_feature_date_no_config():
    """Test get_earliest_valid_feature_date returns None when no config."""
    from pathlib import Path

    from unified_domain_client.date_validation import DateValidator

    validator = DateValidator(config_path=Path("/nonexistent/path.yaml"))
    result = validator.get_earliest_valid_feature_date("CEFI", venue="BINANCE", timeframe="24h")
    assert result is None


def test_date_validator_get_earliest_valid_ml_date_no_config():
    """Test get_earliest_valid_ml_date returns None when no config."""
    from pathlib import Path

    from unified_domain_client.date_validation import DateValidator

    validator = DateValidator(config_path=Path("/nonexistent/path.yaml"))
    result = validator.get_earliest_valid_ml_date("CEFI", timeframe="24h")
    assert result is None


def test_date_validator_calculate_lookback_days_default():
    """Test calculate_lookback_days returns positive number for default timeframe."""
    from unified_domain_client.date_validation import DateValidator

    validator = DateValidator()
    days = validator.calculate_lookback_days("24h")
    assert days > 0


def test_date_validator_calculate_lookback_days_with_groups():
    """Test calculate_lookback_days with specific feature groups."""
    from unified_domain_client.date_validation import DateValidator

    validator = DateValidator()
    days = validator.calculate_lookback_days("1h", feature_groups=["trend", "momentum"])
    assert days > 0


def test_should_skip_date_no_config():
    """Test should_skip_date returns False when no config (no earliest date)."""
    from pathlib import Path
    from unittest.mock import patch

    from unified_domain_client import date_validation as dv

    # Reset module-level validator to ensure fresh state
    original = dv._validator
    dv._validator = None
    try:
        with patch.object(dv, "get_validator") as mock_get_validator:
            from unified_domain_client.date_validation import DateValidator
            v = DateValidator(config_path=Path("/nonexistent/path.yaml"))
            mock_get_validator.return_value = v
            result = dv.should_skip_date("2024-01-15", "CEFI")
            assert result is False
    finally:
        dv._validator = original


def test_get_earliest_valid_date_module_level():
    """Test get_earliest_valid_date module-level function."""
    from unittest.mock import patch

    from unified_domain_client import date_validation as dv

    with patch.object(dv, "get_validator") as mock_get_validator:
        mock_validator = mock_get_validator.return_value
        mock_validator.get_earliest_valid_feature_date.return_value = "2023-05-01"
        result = dv.get_earliest_valid_date("CEFI", venue="BINANCE")
        assert result == "2023-05-01"


def test_date_validator_inline_config():
    """Test DateValidator with inline config dict (via YAML file)."""
    import os
    import tempfile

    from unified_domain_client.date_validation import DateValidator

    config_content = """
features-delta-one-service:
  CEFI:
    category_start: "2023-05-01"
earliest_valid_features:
  24h:
    CEFI: "2024-01-01"
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(config_content)
        tmp_path = f.name

    try:
        from pathlib import Path
        validator = DateValidator(config_path=Path(tmp_path))
        # Date after earliest valid — should be valid
        result = validator.check_date("2024-06-01", "CEFI")
        assert result.is_valid is True

        # Date before earliest valid — should be invalid
        result2 = validator.check_date("2023-01-01", "CEFI")
        assert result2.is_valid is False
        assert result2.days_until_valid is not None
        assert result2.days_until_valid > 0
    finally:
        os.unlink(tmp_path)


# ===========================================================================
# standardized_service — download/upload method coverage
# ===========================================================================


def test_standardized_service_download_raises_on_error():
    """Test download_from_gcs re-raises on connection error."""
    from unittest.mock import patch

    from unified_domain_client.cloud_target import CloudTarget
    from unified_domain_client.standardized_service import StandardizedDomainCloudService

    target = CloudTarget(project_id="p", gcs_bucket="b", bigquery_dataset="d")
    svc = StandardizedDomainCloudService(domain="test", cloud_target=target)

    with patch("unified_domain_client.standardized_service.download_from_storage") as mock_dl:
        mock_dl.side_effect = OSError("network error")
        with pytest.raises(OSError):
            svc.download_from_gcs("some/path.parquet", format="parquet")


def test_standardized_service_download_json_format():
    """Test download_from_gcs with json format."""
    import json
    from unittest.mock import patch

    from unified_domain_client.cloud_target import CloudTarget
    from unified_domain_client.standardized_service import StandardizedDomainCloudService

    target = CloudTarget(project_id="p", gcs_bucket="b", bigquery_dataset="d")
    svc = StandardizedDomainCloudService(domain="test", cloud_target=target)

    with patch("unified_domain_client.standardized_service.download_from_storage") as mock_dl:
        mock_dl.return_value = json.dumps({"key": "value"}).encode()
        result = svc.download_from_gcs("some/path.json", format="json")
        assert isinstance(result, dict)
        assert result["key"] == "value"


def test_standardized_service_download_unknown_format():
    """Test download_from_gcs with unknown format returns empty DataFrame."""
    from unittest.mock import patch

    import pandas as pd

    from unified_domain_client.cloud_target import CloudTarget
    from unified_domain_client.standardized_service import StandardizedDomainCloudService

    target = CloudTarget(project_id="p", gcs_bucket="b", bigquery_dataset="d")
    svc = StandardizedDomainCloudService(domain="test", cloud_target=target)

    with patch("unified_domain_client.standardized_service.download_from_storage") as mock_dl:
        mock_dl.return_value = b"data"
        result = svc.download_from_gcs("some/path.txt", format="unknown_format")
        assert isinstance(result, pd.DataFrame)


def test_standardized_service_upload_parquet():
    """Test upload_to_gcs with parquet format."""
    from unittest.mock import patch

    import pandas as pd

    from unified_domain_client.cloud_target import CloudTarget
    from unified_domain_client.standardized_service import StandardizedDomainCloudService

    target = CloudTarget(project_id="p", gcs_bucket="b", bigquery_dataset="d")
    svc = StandardizedDomainCloudService(domain="test", cloud_target=target)
    df = pd.DataFrame({"x": [1, 2]})

    with patch("unified_domain_client.standardized_service.upload_to_storage") as mock_ul:
        mock_ul.return_value = "gs://b/some/path.parquet"
        result = svc.upload_to_gcs(df, "some/path.parquet", format="parquet")
        assert result == "gs://b/some/path.parquet"
        mock_ul.assert_called_once()


def test_standardized_service_upload_csv():
    """Test upload_to_gcs with csv format."""
    from unittest.mock import patch

    import pandas as pd

    from unified_domain_client.cloud_target import CloudTarget
    from unified_domain_client.standardized_service import StandardizedDomainCloudService

    target = CloudTarget(project_id="p", gcs_bucket="b", bigquery_dataset="d")
    svc = StandardizedDomainCloudService(domain="test", cloud_target=target)
    df = pd.DataFrame({"x": [1, 2]})

    with patch("unified_domain_client.standardized_service.upload_to_storage") as mock_ul:
        mock_ul.return_value = "gs://b/some/path.csv"
        result = svc.upload_to_gcs(df, "some/path.csv", format="csv")
        assert result == "gs://b/some/path.csv"


def test_standardized_service_upload_unsupported_format_raises():
    """Test upload_to_gcs with unsupported format raises ValueError."""
    import pandas as pd

    from unified_domain_client.cloud_target import CloudTarget
    from unified_domain_client.standardized_service import StandardizedDomainCloudService

    target = CloudTarget(project_id="p", gcs_bucket="b", bigquery_dataset="d")
    svc = StandardizedDomainCloudService(domain="test", cloud_target=target)
    df = pd.DataFrame({"x": [1]})
    with pytest.raises(ValueError, match="Unsupported format"):
        svc.upload_to_gcs(df, "path.xyz", format="xml")


# ===========================================================================
# sports clients — method-level smoke tests via cloud_service injection
# ===========================================================================


def test_sports_features_read_features_returns_empty_on_error():
    """Test SportsFeaturesDomainClient.read_features returns empty on error."""
    from unittest.mock import MagicMock, patch

    import pandas as pd

    from unified_domain_client.sports.features_client import SportsFeaturesDomainClient

    with patch("unified_domain_client.sports.features_client.UnifiedCloudConfig") as mock_cfg, \
         patch("unified_domain_client.sports.features_client.StandardizedDomainCloudService"):
        mock_cfg.return_value.gcp_project_id = "test-project"
        client = SportsFeaturesDomainClient(project_id="test-project", gcs_bucket="sports-bucket")

    mock_svc = MagicMock()
    mock_svc.download_from_gcs.side_effect = OSError("not found")
    client.cloud_service = mock_svc
    result = client.read_features(horizon="1d", date="2024-01-15", league="epl")
    assert isinstance(result, pd.DataFrame)


def test_sports_features_write_features():
    """Test SportsFeaturesDomainClient.write_features calls upload."""
    from unittest.mock import MagicMock, patch

    import pandas as pd

    from unified_domain_client.sports.features_client import SportsFeaturesDomainClient

    with patch("unified_domain_client.sports.features_client.UnifiedCloudConfig") as mock_cfg, \
         patch("unified_domain_client.sports.features_client.StandardizedDomainCloudService"):
        mock_cfg.return_value.gcp_project_id = "test-project"
        client = SportsFeaturesDomainClient(project_id="test-project", gcs_bucket="sports-bucket")

    mock_svc = MagicMock()
    mock_svc.upload_to_gcs.return_value = "gs://sports-bucket/path"
    client.cloud_service = mock_svc
    df = pd.DataFrame({"feature": [1.0]})
    result = client.write_features(df, horizon="1d", date="2024-01-15", league="epl")
    assert result == "gs://sports-bucket/path"


def test_sports_fixtures_read_fixtures_returns_empty_on_error():
    """Test SportsFixturesDomainClient.read_fixtures returns empty on error."""
    from unittest.mock import MagicMock, patch

    import pandas as pd

    from unified_domain_client.sports.fixtures_client import SportsFixturesDomainClient

    with patch("unified_domain_client.sports.fixtures_client.UnifiedCloudConfig") as mock_cfg, \
         patch("unified_domain_client.sports.fixtures_client.StandardizedDomainCloudService"):
        mock_cfg.return_value.gcp_project_id = "test-project"
        client = SportsFixturesDomainClient(project_id="test-project", gcs_bucket="sports-bucket")

    mock_svc = MagicMock()
    mock_svc.download_from_gcs.side_effect = OSError("not found")
    client.cloud_service = mock_svc
    result = client.read_fixtures(season="2024", date="2024-01-15", league="epl")
    assert isinstance(result, pd.DataFrame)


def test_sports_odds_read_odds_returns_empty_on_error():
    """Test SportsOddsDomainClient.read_odds returns empty on error."""
    from unittest.mock import MagicMock, patch

    import pandas as pd

    from unified_domain_client.sports.odds_client import SportsOddsDomainClient

    with patch("unified_domain_client.sports.odds_client.UnifiedCloudConfig") as mock_cfg, \
         patch("unified_domain_client.sports.odds_client.StandardizedDomainCloudService"):
        mock_cfg.return_value.gcp_project_id = "test-project"
        client = SportsOddsDomainClient(project_id="test-project", gcs_bucket="sports-bucket")

    mock_svc = MagicMock()
    mock_svc.download_from_gcs.side_effect = OSError("not found")
    client.cloud_service = mock_svc
    result = client.read_odds(provider="bet365", date="2024-01-15", league="epl")
    assert isinstance(result, pd.DataFrame)


def test_sports_tick_data_read_ticks_returns_empty_on_error():
    """Test SportsTickDataDomainClient.read_ticks returns empty on error."""
    from unittest.mock import MagicMock, patch

    import pandas as pd

    from unified_domain_client.sports.tick_data_client import SportsTickDataDomainClient

    with patch("unified_domain_client.sports.tick_data_client.UnifiedCloudConfig") as mock_cfg, \
         patch("unified_domain_client.sports.tick_data_client.StandardizedDomainCloudService"):
        mock_cfg.return_value.gcp_project_id = "test-project"
        client = SportsTickDataDomainClient(project_id="test-project", gcs_bucket="sports-bucket")

    mock_svc = MagicMock()
    mock_svc.download_from_gcs.side_effect = OSError("not found")
    client.cloud_service = mock_svc
    result = client.read_ticks(venue="betfair", date="2024-01-15")
    assert isinstance(result, pd.DataFrame)


def test_sports_mappings_read_mappings_returns_empty_on_error():
    """Test SportsMappingsDomainClient.read_mappings returns empty on error."""
    from unittest.mock import MagicMock, patch

    import pandas as pd

    from unified_domain_client.sports.mappings_client import SportsMappingsDomainClient

    with patch("unified_domain_client.sports.mappings_client.UnifiedCloudConfig") as mock_cfg, \
         patch("unified_domain_client.sports.mappings_client.StandardizedDomainCloudService"):
        mock_cfg.return_value.gcp_project_id = "test-project"
        client = SportsMappingsDomainClient(project_id="test-project", gcs_bucket="sports-bucket")

    mock_svc = MagicMock()
    mock_svc.download_from_gcs.side_effect = OSError("not found")
    client.cloud_service = mock_svc
    result = client.read_mappings(entity_type="player")
    assert isinstance(result, pd.DataFrame)


# ===========================================================================
# clients/base — BaseDataClient coverage
# ===========================================================================


def test_base_data_client_read_parquet_returns_dataframe():
    """Test BaseDataClient._read_parquet returns DataFrame."""
    import io
    from unittest.mock import MagicMock

    import pandas as pd

    from unified_domain_client.clients.market_data import MarketTickDomainClient

    mock_storage = MagicMock()
    mock_config = MagicMock()
    mock_config.gcp_project_id = "proj"

    # Create a parquet bytes payload
    buf = io.BytesIO()
    pd.DataFrame({"x": [1, 2]}).to_parquet(buf, index=False)
    buf.seek(0)
    mock_storage.download_bytes.return_value = buf.read()

    client = MarketTickDomainClient(storage_client=mock_storage, config=mock_config)
    result = client._read_parquet("bucket", "path/to/data.parquet")
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2


def test_base_data_client_list_blobs_returns_names():
    """Test BaseDataClient._list_blobs returns list of names."""
    from unittest.mock import MagicMock

    from unified_domain_client.clients.market_data import MarketTickDomainClient

    mock_storage = MagicMock()
    mock_config = MagicMock()
    mock_config.gcp_project_id = "proj"

    blob1 = MagicMock()
    blob1.name = "path/to/file1.parquet"
    blob2 = MagicMock()
    blob2.name = "path/to/file2.parquet"
    mock_storage.list_blobs.return_value = [blob1, blob2]

    client = MarketTickDomainClient(storage_client=mock_storage, config=mock_config)
    result = client._list_blobs("bucket", "path/to/")
    assert result == ["path/to/file1.parquet", "path/to/file2.parquet"]


def test_market_tick_domain_client_get_tick_data():
    """Test MarketTickDomainClient.get_tick_data calls storage."""
    import io
    from unittest.mock import MagicMock

    import pandas as pd

    from unified_domain_client.clients.market_data import MarketTickDomainClient

    mock_storage = MagicMock()
    mock_config = MagicMock()
    mock_config.gcp_project_id = "proj"

    buf = io.BytesIO()
    pd.DataFrame({"ts": [1]}).to_parquet(buf, index=False)
    buf.seek(0)
    mock_storage.download_bytes.return_value = buf.read()

    client = MarketTickDomainClient(storage_client=mock_storage, config=mock_config)
    result = client.get_tick_data(
        date="2024-01-15",
        venue="BINANCE",
        instrument_key="BTCUSDT",
        data_type="trades",
        instrument_type="perpetual",
    )
    assert isinstance(result, pd.DataFrame)


def test_market_tick_domain_client_get_available_dates():
    """Test MarketTickDomainClient.get_available_dates returns dates."""
    from unittest.mock import MagicMock

    from unified_domain_client.clients.market_data import MarketTickDomainClient

    mock_storage = MagicMock()
    mock_config = MagicMock()
    mock_config.gcp_project_id = "proj"

    blob = MagicMock()
    blob.name = "raw_tick_data/by_date/day=2024-01-15/venue=BINANCE/data.parquet"
    mock_storage.list_blobs.return_value = [blob]

    client = MarketTickDomainClient(storage_client=mock_storage, config=mock_config)
    result = client.get_available_dates("BINANCE")
    assert "2024-01-15" in result


def test_market_candle_domain_client_get_candles():
    """Test MarketCandleDomainClient.get_candles calls storage."""
    import io
    from unittest.mock import MagicMock

    import pandas as pd

    from unified_domain_client.clients.market_data import MarketCandleDomainClient

    mock_storage = MagicMock()
    mock_config = MagicMock()
    mock_config.gcp_project_id = "proj"

    buf = io.BytesIO()
    pd.DataFrame({"close": [100.0]}).to_parquet(buf, index=False)
    buf.seek(0)
    mock_storage.download_bytes.return_value = buf.read()

    client = MarketCandleDomainClient(storage_client=mock_storage, config=mock_config)
    result = client.get_candles(
        date="2024-01-15",
        venue="BINANCE",
        instrument_id="BTCUSDT",
        timeframe="1h",
        data_type="trades",
        instrument_type="perpetual",
    )
    assert isinstance(result, pd.DataFrame)


def test_market_candle_domain_client_get_available_timeframes():
    """Test MarketCandleDomainClient.get_available_timeframes returns timeframes."""
    from unittest.mock import MagicMock

    from unified_domain_client.clients.market_data import MarketCandleDomainClient

    mock_storage = MagicMock()
    mock_config = MagicMock()
    mock_config.gcp_project_id = "proj"

    blob = MagicMock()
    blob.name = "processed_candles/by_date/day=2024-01-15/timeframe=1h/venue=BINANCE/data.parquet"
    mock_storage.list_blobs.return_value = [blob]

    client = MarketCandleDomainClient(storage_client=mock_storage, config=mock_config)
    result = client.get_available_timeframes("BINANCE")
    assert "1h" in result


# ===========================================================================
# instrument_date_filter — DateFilterService coverage
# ===========================================================================


def test_validate_config_is_callable():
    """Test validate_config can be imported and is callable."""
    from unified_domain_client import validate_config

    assert callable(validate_config)


# ===========================================================================
# writers — base, direct, factory coverage
# ===========================================================================


def test_base_writer_write_parquet():
    """Test BaseWriter.write_parquet uploads parquet data."""
    from unittest.mock import patch

    import pandas as pd

    from unified_domain_client.cloud_target import CloudTarget
    from unified_domain_client.writers.base import BaseWriter

    target = CloudTarget(project_id="p", gcs_bucket="b", bigquery_dataset="d")
    writer = BaseWriter(cloud_target=target)
    df = pd.DataFrame({"x": [1, 2]})

    with patch("unified_domain_client.writers.base.upload_to_storage") as mock_upload:
        mock_upload.return_value = "gs://b/some/path.parquet"
        result = writer.write_parquet(df, "some/path.parquet")
        assert result == "gs://b/some/path.parquet"
        mock_upload.assert_called_once()


def test_base_writer_write_json():
    """Test BaseWriter.write_json uploads JSON data."""
    from unittest.mock import patch

    from unified_domain_client.cloud_target import CloudTarget
    from unified_domain_client.writers.base import BaseWriter

    target = CloudTarget(project_id="p", gcs_bucket="b", bigquery_dataset="d")
    writer = BaseWriter(cloud_target=target)

    with patch("unified_domain_client.writers.base.upload_to_storage") as mock_upload:
        mock_upload.return_value = "gs://b/path.json"
        result = writer.write_json({"key": "value"}, "path.json")
        assert result == "gs://b/path.json"


def test_market_data_writer_write_tick():
    """Test MarketDataWriter.write_tick generates correct path."""
    from unittest.mock import patch

    import pandas as pd

    from unified_domain_client.cloud_target import CloudTarget
    from unified_domain_client.writers.base import MarketDataWriter

    target = CloudTarget(project_id="p", gcs_bucket="b", bigquery_dataset="d")
    writer = MarketDataWriter(cloud_target=target)
    df = pd.DataFrame({"ts": [1000]})

    with patch("unified_domain_client.writers.base.upload_to_storage") as mock_upload:
        mock_upload.return_value = "gs://b/path"
        result = writer.write_tick(df, instrument="BTC-USDT", date="2024-01-15")
        assert result == "gs://b/path"
        # Verify path contains expected values
        call_args = mock_upload.call_args
        assert "2024-01-15" in call_args[0][1]


def test_features_writer_write_delta_one():
    """Test FeaturesWriter.write_delta_one generates correct path."""
    from unittest.mock import patch

    import pandas as pd

    from unified_domain_client.cloud_target import CloudTarget
    from unified_domain_client.writers.base import FeaturesWriter

    target = CloudTarget(project_id="p", gcs_bucket="b", bigquery_dataset="d")
    writer = FeaturesWriter(cloud_target=target)
    df = pd.DataFrame({"feature": [1.0]})

    with patch("unified_domain_client.writers.base.upload_to_storage") as mock_upload:
        mock_upload.return_value = "gs://b/path"
        result = writer.write_delta_one(df, instrument="BTC-USDT", date="2024-01-15")
        assert result == "gs://b/path"


def test_ml_writer_write_predictions():
    """Test MLWriter.write_predictions generates correct path."""
    from unittest.mock import patch

    import pandas as pd

    from unified_domain_client.cloud_target import CloudTarget
    from unified_domain_client.writers.base import MLWriter

    target = CloudTarget(project_id="p", gcs_bucket="b", bigquery_dataset="d")
    writer = MLWriter(cloud_target=target)
    df = pd.DataFrame({"prediction": [0.9]})

    with patch("unified_domain_client.writers.base.upload_to_storage") as mock_upload:
        mock_upload.return_value = "gs://b/path"
        result = writer.write_predictions(df, instrument="BTC-USDT", date="2024-01-15")
        assert result == "gs://b/path"


def test_direct_writer_write():
    """Test DirectWriter.write calls storage.upload_bytes."""
    from unittest.mock import MagicMock

    import pandas as pd

    from unified_domain_client.writers.direct import DirectWriter

    mock_storage = MagicMock()
    writer = DirectWriter(storage_client=mock_storage)
    df = pd.DataFrame({"x": [1, 2]})
    writer.write(df, "my-bucket", "path/data.parquet")
    mock_storage.upload_bytes.assert_called_once()


def test_direct_writer_write_json():
    """Test DirectWriter.write_json calls storage.upload_bytes."""
    from unittest.mock import MagicMock

    from unified_domain_client.writers.direct import DirectWriter

    mock_storage = MagicMock()
    writer = DirectWriter(storage_client=mock_storage)
    writer.write_json({"key": "value"}, "my-bucket", "path/data.json")
    mock_storage.upload_bytes.assert_called_once()


def test_get_writer_without_storage_raises():
    """Test get_writer raises ValueError when storage_client is None."""
    from unified_domain_client.writers.factory import get_writer

    with pytest.raises(ValueError, match="storage_client required"):
        get_writer("some_dataset", storage_client=None)


def test_get_writer_with_storage_returns_direct_writer():
    """Test get_writer returns DirectWriter when storage_client provided."""
    from unittest.mock import MagicMock

    from unified_domain_client.writers.direct import DirectWriter
    from unified_domain_client.writers.factory import get_writer

    mock_storage = MagicMock()
    writer = get_writer("some_dataset", storage_client=mock_storage)
    assert isinstance(writer, DirectWriter)


# ===========================================================================
# sports clients write/available_dates methods
# ===========================================================================


def test_sports_features_get_available_dates_returns_empty_on_error():
    """Test SportsFeaturesDomainClient.get_available_dates returns empty list on error."""
    from unittest.mock import patch

    from unified_domain_client.sports.features_client import SportsFeaturesDomainClient

    with patch("unified_domain_client.sports.features_client.UnifiedCloudConfig") as mock_cfg, \
         patch("unified_domain_client.sports.features_client.StandardizedDomainCloudService"), \
         patch("unified_domain_client.sports.features_client.get_storage_client") as mock_storage:
        mock_cfg.return_value.gcp_project_id = "test-project"
        mock_storage.side_effect = OSError("not found")
        client = SportsFeaturesDomainClient(project_id="test-project", gcs_bucket="sports-bucket")
        result = client.get_available_dates(horizon="1d", league="epl")
        assert result == []


def test_sports_fixtures_write_fixtures():
    """Test SportsFixturesDomainClient.write_fixtures calls upload."""
    from unittest.mock import MagicMock, patch

    import pandas as pd

    from unified_domain_client.sports.fixtures_client import SportsFixturesDomainClient

    with patch("unified_domain_client.sports.fixtures_client.UnifiedCloudConfig") as mock_cfg, \
         patch("unified_domain_client.sports.fixtures_client.StandardizedDomainCloudService"):
        mock_cfg.return_value.gcp_project_id = "test-project"
        client = SportsFixturesDomainClient(project_id="test-project", gcs_bucket="sports-bucket")

    mock_svc = MagicMock()
    mock_svc.upload_to_gcs.return_value = "gs://sports-bucket/fixtures"
    client.cloud_service = mock_svc
    df = pd.DataFrame({"fixture": [1]})
    result = client.write_fixtures(df, season="2024", date="2024-01-15", league="epl")
    assert result == "gs://sports-bucket/fixtures"


def test_sports_odds_write_odds():
    """Test SportsOddsDomainClient.write_odds calls upload."""
    from unittest.mock import MagicMock, patch

    import pandas as pd

    from unified_domain_client.sports.odds_client import SportsOddsDomainClient

    with patch("unified_domain_client.sports.odds_client.UnifiedCloudConfig") as mock_cfg, \
         patch("unified_domain_client.sports.odds_client.StandardizedDomainCloudService"):
        mock_cfg.return_value.gcp_project_id = "test-project"
        client = SportsOddsDomainClient(project_id="test-project", gcs_bucket="sports-bucket")

    mock_svc = MagicMock()
    mock_svc.upload_to_gcs.return_value = "gs://sports-bucket/odds"
    client.cloud_service = mock_svc
    df = pd.DataFrame({"odds": [2.5]})
    result = client.write_odds(df, provider="bet365", date="2024-01-15", league="epl")
    assert result == "gs://sports-bucket/odds"


def test_sports_tick_data_write_ticks():
    """Test SportsTickDataDomainClient.write_ticks calls upload."""
    from unittest.mock import MagicMock, patch

    import pandas as pd

    from unified_domain_client.sports.tick_data_client import SportsTickDataDomainClient

    with patch("unified_domain_client.sports.tick_data_client.UnifiedCloudConfig") as mock_cfg, \
         patch("unified_domain_client.sports.tick_data_client.StandardizedDomainCloudService"):
        mock_cfg.return_value.gcp_project_id = "test-project"
        client = SportsTickDataDomainClient(project_id="test-project", gcs_bucket="sports-bucket")

    mock_svc = MagicMock()
    mock_svc.upload_to_gcs.return_value = "gs://sports-bucket/ticks"
    client.cloud_service = mock_svc
    df = pd.DataFrame({"tick": [1]})
    result = client.write_ticks(df, venue="betfair", date="2024-01-15")
    assert result == "gs://sports-bucket/ticks"


def test_sports_mappings_write_mappings():
    """Test SportsMappingsDomainClient.write_mappings calls upload."""
    from unittest.mock import MagicMock, patch

    import pandas as pd

    from unified_domain_client.sports.mappings_client import SportsMappingsDomainClient

    with patch("unified_domain_client.sports.mappings_client.UnifiedCloudConfig") as mock_cfg, \
         patch("unified_domain_client.sports.mappings_client.StandardizedDomainCloudService"):
        mock_cfg.return_value.gcp_project_id = "test-project"
        client = SportsMappingsDomainClient(project_id="test-project", gcs_bucket="sports-bucket")

    mock_svc = MagicMock()
    mock_svc.upload_to_gcs.return_value = "gs://sports-bucket/mappings"
    client.cloud_service = mock_svc
    df = pd.DataFrame({"id": [1]})
    result = client.write_mappings(df, entity_type="player")
    assert result == "gs://sports-bucket/mappings"


def test_sports_mappings_has_cloud_service():
    """Test SportsMappingsDomainClient has cloud_service attribute."""
    from unittest.mock import patch

    from unified_domain_client.sports.mappings_client import SportsMappingsDomainClient

    with patch("unified_domain_client.sports.mappings_client.UnifiedCloudConfig") as mock_cfg, \
         patch("unified_domain_client.sports.mappings_client.StandardizedDomainCloudService"):
        mock_cfg.return_value.gcp_project_id = "test-project"
        client = SportsMappingsDomainClient(project_id="test-project", gcs_bucket="sports-bucket")
        assert hasattr(client, "cloud_service")


# ===========================================================================
# readers/base and readers/direct — coverage
# ===========================================================================


def test_base_reader_read_parquet():
    """Test BaseReader.read_parquet downloads and deserializes parquet."""
    import io
    from unittest.mock import patch

    import pandas as pd

    from unified_domain_client.cloud_target import CloudTarget
    from unified_domain_client.readers.base import BaseReader

    target = CloudTarget(project_id="p", gcs_bucket="b", bigquery_dataset="d")
    reader = BaseReader(cloud_target=target)

    buf = io.BytesIO()
    pd.DataFrame({"x": [1]}).to_parquet(buf, index=False)
    buf.seek(0)

    with patch("unified_domain_client.readers.base.download_from_storage") as mock_dl:
        mock_dl.return_value = buf.read()
        result = reader.read_parquet("some/path.parquet")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1


def test_base_reader_read_json():
    """Test BaseReader.read_json downloads and parses JSON."""
    import json
    from unittest.mock import patch

    from unified_domain_client.cloud_target import CloudTarget
    from unified_domain_client.readers.base import BaseReader

    target = CloudTarget(project_id="p", gcs_bucket="b", bigquery_dataset="d")
    reader = BaseReader(cloud_target=target)

    with patch("unified_domain_client.readers.base.download_from_storage") as mock_dl:
        mock_dl.return_value = json.dumps({"key": "val"}).encode()
        result = reader.read_json("some/path.json")
        assert result == {"key": "val"}


def test_base_reader_exists():
    """Test BaseReader.exists checks if file exists."""
    from unittest.mock import patch

    from unified_domain_client.cloud_target import CloudTarget
    from unified_domain_client.readers.base import BaseReader

    target = CloudTarget(project_id="p", gcs_bucket="b", bigquery_dataset="d")
    reader = BaseReader(cloud_target=target)

    with patch("unified_domain_client.readers.base.storage_exists") as mock_exists:
        mock_exists.return_value = True
        assert reader.exists("some/path.parquet") is True
        mock_exists.return_value = False
        assert reader.exists("missing.parquet") is False


def test_market_data_reader_read_tick():
    """Test MarketDataReader.read_tick calls read_parquet."""
    import io
    from unittest.mock import patch

    import pandas as pd

    from unified_domain_client.cloud_target import CloudTarget
    from unified_domain_client.readers.base import MarketDataReader

    target = CloudTarget(project_id="p", gcs_bucket="b", bigquery_dataset="d")
    reader = MarketDataReader(cloud_target=target)
    buf = io.BytesIO()
    pd.DataFrame({"ts": [1]}).to_parquet(buf, index=False)
    buf.seek(0)

    with patch("unified_domain_client.readers.base.download_from_storage") as mock_dl:
        mock_dl.return_value = buf.read()
        result = reader.read_tick("BTC-USDT", "2024-01-15")
        assert isinstance(result, pd.DataFrame)


def test_market_data_reader_read_candles_timeframes():
    """Test MarketDataReader.read_candles for various timeframes."""
    import io
    from unittest.mock import patch

    import pandas as pd

    from unified_domain_client.cloud_target import CloudTarget
    from unified_domain_client.readers.base import MarketDataReader

    target = CloudTarget(project_id="p", gcs_bucket="b", bigquery_dataset="d")
    reader = MarketDataReader(cloud_target=target)
    buf = io.BytesIO()
    pd.DataFrame({"close": [100.0]}).to_parquet(buf, index=False)

    for timeframe in ["1m", "1h", "24h", "4h"]:
        buf.seek(0)
        with patch("unified_domain_client.readers.base.download_from_storage") as mock_dl:
            mock_dl.return_value = buf.read()
            result = reader.read_candles("BTC-USDT", "2024-01-15", timeframe=timeframe)
            assert isinstance(result, pd.DataFrame)


def test_features_reader_read_delta_one():
    """Test FeaturesReader.read_delta_one calls read_parquet."""
    import io
    from unittest.mock import patch

    import pandas as pd

    from unified_domain_client.cloud_target import CloudTarget
    from unified_domain_client.readers.base import FeaturesReader

    target = CloudTarget(project_id="p", gcs_bucket="b", bigquery_dataset="d")
    reader = FeaturesReader(cloud_target=target)
    buf = io.BytesIO()
    pd.DataFrame({"feature": [1.0]}).to_parquet(buf, index=False)
    buf.seek(0)

    with patch("unified_domain_client.readers.base.download_from_storage") as mock_dl:
        mock_dl.return_value = buf.read()
        result = reader.read_delta_one("BTC-USDT", "2024-01-15")
        assert isinstance(result, pd.DataFrame)


def test_ml_reader_read_predictions():
    """Test MLReader.read_predictions calls read_parquet."""
    import io
    from unittest.mock import patch

    import pandas as pd

    from unified_domain_client.cloud_target import CloudTarget
    from unified_domain_client.readers.base import MLReader

    target = CloudTarget(project_id="p", gcs_bucket="b", bigquery_dataset="d")
    reader = MLReader(cloud_target=target)
    buf = io.BytesIO()
    pd.DataFrame({"pred": [0.9]}).to_parquet(buf, index=False)
    buf.seek(0)

    with patch("unified_domain_client.readers.base.download_from_storage") as mock_dl:
        mock_dl.return_value = buf.read()
        result = reader.read_predictions("BTC-USDT", "2024-01-15")
        assert isinstance(result, pd.DataFrame)


def test_direct_reader_read():
    """Test DirectReader.read downloads and deserializes parquet."""
    import io
    from unittest.mock import MagicMock

    import pandas as pd

    from unified_domain_client.readers.direct import DirectReader

    mock_storage = MagicMock()
    buf = io.BytesIO()
    pd.DataFrame({"x": [1]}).to_parquet(buf, index=False)
    buf.seek(0)
    mock_storage.download_bytes.return_value = buf.read()

    reader = DirectReader(storage_client=mock_storage)
    result = reader.read("bucket", "path/data.parquet")
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1


def test_direct_reader_list_available():
    """Test DirectReader.list_available returns blob names."""
    from unittest.mock import MagicMock

    from unified_domain_client.readers.direct import DirectReader

    mock_storage = MagicMock()
    blob1 = MagicMock()
    blob1.name = "path/file1.parquet"
    blob2 = MagicMock()
    blob2.name = "path/file2.parquet"
    mock_storage.list_blobs.return_value = [blob1, blob2]

    reader = DirectReader(storage_client=mock_storage)
    result = reader.list_available("bucket", "path/")
    assert result == ["path/file1.parquet", "path/file2.parquet"]
