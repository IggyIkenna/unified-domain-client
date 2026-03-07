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

    svc = create_backtesting_cloud_service(bucket="my-backtest-bucket")
    assert svc.domain == "backtest"
    assert svc.bucket == "my-backtest-bucket"


def test_create_features_cloud_service():
    """Test create_features_cloud_service factory."""
    from unified_domain_client.factories import create_features_cloud_service

    svc = create_features_cloud_service(storage_bucket="custom-bucket")
    assert svc.domain == "features"
    assert svc.bucket == "custom-bucket"


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
    from unified_domain_client.standardized_service import StandardizedDomainCloudService

    svc = StandardizedDomainCloudService(domain="test", bucket="b")
    assert svc.domain == "test"
    assert svc.bucket == "b"


def test_standardized_service_query_bigquery_raises():
    """Test query_bigquery raises NotImplementedError."""
    from unified_domain_client.standardized_service import StandardizedDomainCloudService

    svc = StandardizedDomainCloudService(domain="test", bucket="b")
    with pytest.raises(NotImplementedError):
        svc.query_bigquery("SELECT 1")


def test_make_domain_service():
    """Test make_domain_service factory function."""
    from unified_domain_client.standardized_service import make_domain_service

    svc = make_domain_service("instruments", bucket="my-bucket", project_id="proj", dataset="my_ds")
    assert svc.domain == "instruments"
    assert svc.bucket == "my-bucket"


def test_create_domain_cloud_service():
    """Test create_domain_cloud_service factory function."""
    from unified_domain_client.cloud_target import CloudTarget
    from unified_domain_client.standardized_service import create_domain_cloud_service

    target = CloudTarget(project_id="p", storage_bucket="bucket", analytics_dataset="ds")
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

    uri = build_full_uri(
        "instruments", project_id="test-proj", category="cefi", date="2024-01-15", venue="BINANCE"
    )
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

    result = PathRegistry.format(
        PathRegistry.MARKET_TICK_RAW, date="2024-01-15", instrument="BTC-USDT"
    )
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

    with (
        patch("unified_domain_client.sports.features_client.UnifiedCloudConfig") as mock_cfg,
        patch("unified_domain_client.sports.features_client.StandardizedDomainCloudService"),
    ):
        mock_cfg.return_value.gcp_project_id = "test-project"
        from unified_domain_client.sports.features_client import SportsFeaturesDomainClient

        client = SportsFeaturesDomainClient(project_id="test-project", storage_bucket="test-bucket")
        assert client is not None

    with (
        patch("unified_domain_client.sports.fixtures_client.UnifiedCloudConfig") as mock_cfg,
        patch("unified_domain_client.sports.fixtures_client.StandardizedDomainCloudService"),
    ):
        mock_cfg.return_value.gcp_project_id = "test-project"
        from unified_domain_client.sports.fixtures_client import SportsFixturesDomainClient

        client2 = SportsFixturesDomainClient(
            project_id="test-project", storage_bucket="test-bucket"
        )
        assert client2 is not None

    with (
        patch("unified_domain_client.sports.mappings_client.UnifiedCloudConfig") as mock_cfg,
        patch("unified_domain_client.sports.mappings_client.StandardizedDomainCloudService"),
    ):
        mock_cfg.return_value.gcp_project_id = "test-project"
        from unified_domain_client.sports.mappings_client import SportsMappingsDomainClient

        client3 = SportsMappingsDomainClient(
            project_id="test-project", storage_bucket="test-bucket"
        )
        assert client3 is not None

    with (
        patch("unified_domain_client.sports.odds_client.UnifiedCloudConfig") as mock_cfg,
        patch("unified_domain_client.sports.odds_client.StandardizedDomainCloudService"),
    ):
        mock_cfg.return_value.gcp_project_id = "test-project"
        from unified_domain_client.sports.odds_client import SportsOddsDomainClient

        client4 = SportsOddsDomainClient(project_id="test-project", storage_bucket="test-bucket")
        assert client4 is not None

    with (
        patch("unified_domain_client.sports.tick_data_client.UnifiedCloudConfig") as mock_cfg,
        patch("unified_domain_client.sports.tick_data_client.StandardizedDomainCloudService"),
    ):
        mock_cfg.return_value.gcp_project_id = "test-project"
        from unified_domain_client.sports.tick_data_client import SportsTickDataDomainClient

        client5 = SportsTickDataDomainClient(
            project_id="test-project", storage_bucket="test-bucket"
        )
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
    result = validator.validate(
        pd.DataFrame({"x": [1]}), expected_date=date(2024, 1, 15), timestamp_col="ts"
    )
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
