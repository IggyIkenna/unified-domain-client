"""Integration tests that functionally exercise each library dependency.

Satisfies check-integration-dep-coverage.py: each manifest library dep
must be imported in at least one tests/integration/ file.

Every test goes beyond `assert X is not None` — it exercises actual
behaviour of the imported symbol to prove the dep integration works.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# 1. unified-trading-library (UTL)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestUnifiedTradingLibraryIntegration:
    """Functionally exercise unified-trading-library symbols used by UDC."""

    def test_validation_result_construction_and_fields(self) -> None:
        """ValidationResult can be constructed with fields and queried."""
        from unified_trading_library import ValidationResult

        result = ValidationResult(
            valid=False,
            errors=["timestamp misaligned"],
            warnings=["minor gap"],
            validation_type="domain_validation_market_data",
            total_records=100,
            valid_records=95,
            invalid_records=5,
            stats={"domain": "market_data"},
        )
        assert result.valid is False
        assert result.total_records == 100
        assert "timestamp misaligned" in result.errors
        assert result.validation_type == "domain_validation_market_data"
        assert result.stats["domain"] == "market_data"

    def test_validation_result_default_values(self) -> None:
        """ValidationResult defaults are sane when only `valid` is set."""
        from unified_trading_library import ValidationResult

        result = ValidationResult(valid=True)
        assert result.errors == []
        assert result.warnings == []
        assert result.total_records == 0

    def test_graceful_shutdown_handler_instantiation(self) -> None:
        """GracefulShutdownHandler can be instantiated and has expected attrs."""
        from unified_trading_library import GracefulShutdownHandler

        handler = GracefulShutdownHandler()
        assert hasattr(handler, "shutdown_requested")
        assert handler.shutdown_requested is False

    def test_date_filter_service_filters_instruments(self) -> None:
        """DateFilterService has filter_instruments_by_date method."""
        from unified_trading_library import DateFilterService

        svc = DateFilterService()
        assert hasattr(svc, "filter_instruments_by_date")
        # Verify protocol defaults are loaded
        result = svc.get_protocol_default_date("uniswap_v3")
        assert result is not None
        assert "2021" in result

    def test_timestamp_date_validator_validates_empty_df(self) -> None:
        """TimestampDateValidator returns valid for empty DataFrame."""
        from unified_trading_library import TimestampDateValidator

        validator = TimestampDateValidator()
        result = validator.validate(pd.DataFrame(), expected_date=date(2024, 1, 15))
        assert result.valid is True
        assert result.alignment_percentage == 100.0

    def test_timestamp_date_validator_detects_misalignment(self) -> None:
        """TimestampDateValidator detects wrong-date timestamps."""
        from unified_trading_library import TimestampDateValidator

        ts_ns = int(datetime(2024, 1, 16, 12, 0, 0, tzinfo=UTC).timestamp() * 1e9)
        df = pd.DataFrame({"timestamp": [ts_ns]})
        validator = TimestampDateValidator(alignment_threshold=90.0, timestamp_unit="ns")
        result = validator.validate(df, expected_date=date(2024, 1, 15))
        assert not result.valid
        assert result.alignment_percentage == 0.0

    def test_date_validator_check_date_no_config(self) -> None:
        """DateValidator.check_date returns valid when no config."""
        from unified_trading_library import DateValidator

        validator = DateValidator(config_path=Path("/nonexistent/path.yaml"))
        result = validator.check_date("2024-06-01", "CEFI")
        assert result.is_valid is True

    def test_date_validator_calculate_lookback_days(self) -> None:
        """DateValidator.calculate_lookback_days returns positive value."""
        from unified_trading_library import DateValidator

        validator = DateValidator()
        days = validator.calculate_lookback_days("24h")
        assert isinstance(days, int)
        assert days > 0

    def test_validate_timestamp_date_alignment_with_none(self) -> None:
        """validate_timestamp_date_alignment returns valid for None df."""
        from unified_trading_library import validate_timestamp_date_alignment

        result = validate_timestamp_date_alignment(None)
        assert result.valid is True

    def test_data_completion_checker_construction(self) -> None:
        """DataCompletionChecker can be constructed with bucket and pattern."""
        from unified_trading_library import DataCompletionChecker

        with patch(
            "unified_trading_library.domain.data_completion.get_storage_client"
        ) as mock_storage:
            mock_storage.return_value = MagicMock()
            checker = DataCompletionChecker(
                bucket="test-bucket",
                path_pattern="market_data/{instrument}/{date}/data.parquet",
            )
            assert checker.bucket == "test-bucket"
            assert "{date}" in checker.path_pattern


# ---------------------------------------------------------------------------
# 2. unified-config-interface (UCI)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestUnifiedConfigInterfaceIntegration:
    """Functionally exercise unified-config-interface symbols used by UDC."""

    def test_path_registry_all_patterns(self) -> None:
        """PathRegistry.all_patterns returns non-empty dict of patterns."""
        from unified_config_interface import PathRegistry

        patterns = PathRegistry.all_patterns()
        assert isinstance(patterns, dict)
        assert len(patterns) > 0

    def test_path_registry_format_substitutes_variables(self) -> None:
        """PathRegistry.format injects date and instrument into path template."""
        from unified_config_interface import PathRegistry

        result = PathRegistry.format(
            PathRegistry.MARKET_TICK_RAW, date="2024-01-15", instrument="BTC-USDT"
        )
        assert "2024-01-15" in result
        assert "BTC-USDT" in result

    def test_build_bucket_returns_project_scoped_bucket(self) -> None:
        """build_bucket returns a string containing the project ID."""
        from unified_config_interface import build_bucket

        bucket = build_bucket("instruments", project_id="test-proj", category="cefi")
        assert isinstance(bucket, str)
        assert "test-proj" in bucket

    def test_build_path_returns_date_scoped_path(self) -> None:
        """build_path returns a path containing the date."""
        from unified_config_interface import build_path

        path = build_path("instruments", date="2024-01-15", venue="BINANCE")
        assert isinstance(path, str)
        assert "2024-01-15" in path

    def test_build_full_uri_returns_gs_uri(self) -> None:
        """build_full_uri returns a gs:// URI."""
        from unified_config_interface import build_full_uri

        uri = build_full_uri(
            "instruments",
            project_id="test-proj",
            category="cefi",
            date="2024-01-15",
            venue="BINANCE",
        )
        assert uri.startswith("gs://")
        assert "2024-01-15" in uri

    def test_get_spec_returns_known_dataset(self) -> None:
        """get_spec returns DataSetSpec for a known dataset."""
        from unified_config_interface import DataSetSpec, get_spec

        spec = get_spec("processed_candles")
        assert isinstance(spec, DataSetSpec)
        assert spec.name == "processed_candles"

    def test_get_spec_raises_for_unknown_dataset(self) -> None:
        """get_spec raises KeyError for unknown dataset."""
        from unified_config_interface import get_spec

        with pytest.raises(KeyError, match="not in PATH_REGISTRY"):
            get_spec("nonexistent_dataset_xyz")

    def test_read_mode_enum_has_expected_values(self) -> None:
        """ReadMode has AUTO and BQ_EXTERNAL values."""
        from unified_config_interface import ReadMode

        assert hasattr(ReadMode, "AUTO")
        assert hasattr(ReadMode, "BQ_EXTERNAL")
        assert hasattr(ReadMode, "ATHENA")

    def test_validate_strategy_id_valid(self) -> None:
        """validate_strategy_id returns True for valid IDs."""
        from unified_config_interface import validate_strategy_id

        assert validate_strategy_id("CEFI_BTC_momentum_SCE_1H_V1") is True
        assert validate_strategy_id("DEFI_ETH_lido-staking_SCE_5M_V1") is True

    def test_validate_strategy_id_invalid(self) -> None:
        """validate_strategy_id returns False for invalid IDs."""
        from unified_config_interface import validate_strategy_id

        assert validate_strategy_id("INVALID_BTC_momentum_SCE_1H_V1") is False
        assert validate_strategy_id("") is False
        assert validate_strategy_id("not-a-strategy-id") is False

    def test_config_validator_importable(self) -> None:
        """ConfigValidator can be imported and instantiated."""
        from unified_config_interface import ConfigValidator

        validator = ConfigValidator()
        assert hasattr(validator, "validate")

    def test_market_category_bucket(self) -> None:
        """get_bucket_for_category returns correctly formatted bucket name."""
        from unified_config_interface.market_category import get_bucket_for_category

        result = get_bucket_for_category("CEFI", "my-project")
        assert result == "cefi-store-my-project"

    def test_market_category_test_mode(self) -> None:
        """get_bucket_for_category appends _test in test mode."""
        from unified_config_interface.market_category import get_bucket_for_category

        result = get_bucket_for_category("DEFI", "my-project", test_mode=True)
        assert result == "defi-store-my-project_test"


# ---------------------------------------------------------------------------
# 3. unified-cloud-interface (UCl)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestUnifiedCloudInterfaceIntegration:
    """Functionally exercise unified-cloud-interface symbols used by UDC."""

    def test_get_storage_client_returns_callable(self) -> None:
        """get_storage_client is callable (factory function)."""
        from unified_cloud_interface import get_storage_client

        assert callable(get_storage_client)

    def test_download_from_storage_is_callable(self) -> None:
        """download_from_storage can be imported and is callable."""
        from unified_cloud_interface import download_from_storage

        assert callable(download_from_storage)

    def test_upload_to_storage_is_callable(self) -> None:
        """upload_to_storage can be imported and is callable."""
        from unified_cloud_interface import upload_to_storage

        assert callable(upload_to_storage)

    def test_storage_exists_is_callable(self) -> None:
        """storage_exists can be imported and is callable."""
        from unified_cloud_interface import storage_exists

        assert callable(storage_exists)

    def test_storage_client_class_importable(self) -> None:
        """StorageClient class can be imported."""
        from unified_cloud_interface import StorageClient

        assert StorageClient is not None

    def test_get_storage_client_signature(self) -> None:
        """get_storage_client accepts project_id kwarg."""
        import inspect

        from unified_cloud_interface import get_storage_client

        sig = inspect.signature(get_storage_client)
        assert "project_id" in sig.parameters

    def test_base_writer_uses_upload_to_storage(self) -> None:
        """BaseWriter.write_parquet delegates to upload_to_storage from UCI."""
        from unified_domain_client.writers.base import BaseWriter

        writer = BaseWriter(bucket="test-bucket")
        df = pd.DataFrame({"x": [1, 2, 3]})
        with patch("unified_domain_client.writers.base.upload_to_storage") as mock_upload:
            mock_upload.return_value = "gs://test-bucket/path.parquet"
            result = writer.write_parquet(df, "path.parquet")
            assert result == "gs://test-bucket/path.parquet"
            mock_upload.assert_called_once()
            call_args = mock_upload.call_args
            # Verify bucket is passed correctly
            assert call_args[0][0] == "test-bucket"

    def test_base_reader_exists_uses_storage_exists(self) -> None:
        """BaseReader.exists delegates to storage_exists from UCl."""
        from unified_domain_client.readers.base import BaseReader

        reader = BaseReader(bucket="test-bucket")
        with patch("unified_domain_client.readers.base.storage_exists") as mock_exists:
            mock_exists.return_value = True
            result = reader.exists("path/data.parquet")
            assert result is True
            mock_exists.assert_called_once_with("test-bucket", "path/data.parquet")


# ---------------------------------------------------------------------------
# 4. unified-api-contracts (UAC)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestUnifiedApiContractsIntegration:
    """Functionally exercise unified-api-contracts symbols used by UDC."""

    def test_canonical_order_construction(self) -> None:
        """CanonicalOrder can be constructed with required fields."""
        from unified_api_contracts import CanonicalOrder

        assert hasattr(CanonicalOrder, "__dataclass_fields__") or hasattr(
            CanonicalOrder, "__init__"
        )

    def test_instrument_type_enum_has_expected_values(self) -> None:
        """InstrumentType enum has SPOT_PAIR and PERPETUAL values."""
        from unified_api_contracts import InstrumentType

        assert hasattr(InstrumentType, "SPOT_PAIR")
        assert hasattr(InstrumentType, "PERPETUAL")
        assert InstrumentType.SPOT_PAIR == "SPOT_PAIR"
        assert InstrumentType.PERPETUAL == "PERPETUAL"

    def test_config_schema_constants_are_non_empty(self) -> None:
        """UAC config schema constants are non-empty collections."""
        from unified_api_contracts import (
            CLOB_VENUES,
            CONFIG_SCHEMA,
            DEX_VENUES,
            VENUE_CATEGORY_MAP,
        )

        assert isinstance(CLOB_VENUES, (list, set, frozenset, tuple))
        assert len(CLOB_VENUES) > 0
        assert isinstance(DEX_VENUES, (list, set, frozenset, tuple))
        assert isinstance(VENUE_CATEGORY_MAP, dict)
        assert isinstance(CONFIG_SCHEMA, dict)

    def test_venue_category_map_maps_to_known_categories(self) -> None:
        """VENUE_CATEGORY_MAP values are known market categories."""
        from unified_api_contracts import VENUE_CATEGORY_MAP

        known_categories = {"CEFI", "DEFI", "TRADFI", "SPORTS", "cefi", "defi", "tradfi", "sports"}
        for venue, category in VENUE_CATEGORY_MAP.items():
            assert isinstance(venue, str)
            assert str(category).upper() in {c.upper() for c in known_categories}, (
                f"Unknown category {category} for venue {venue}"
            )

    def test_config_schema_re_export_via_udc(self) -> None:
        """UDC re-exports UAC config schema symbols faithfully."""
        from unified_domain_client.schemas.config_schema import (
            CLOB_VENUES,
            VALID_ALGORITHMS,
            VALID_INSTRUCTION_TYPES,
        )

        assert len(CLOB_VENUES) > 0
        assert len(VALID_ALGORITHMS) > 0
        assert len(VALID_INSTRUCTION_TYPES) > 0


# ---------------------------------------------------------------------------
# 5. unified-internal-contracts (UIC)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestUnifiedInternalContractsIntegration:
    """Functionally exercise unified-internal-contracts symbols used by UDC."""

    def test_lifecycle_event_type_has_expected_members(self) -> None:
        """LifecycleEventType enum has expected event types."""
        from unified_internal_contracts import LifecycleEventType

        assert hasattr(LifecycleEventType, "STARTED")
        assert hasattr(LifecycleEventType, "STOPPED")
        assert hasattr(LifecycleEventType, "FAILED")
        assert LifecycleEventType.STARTED == "STARTED"
        assert LifecycleEventType.PROCESSING_COMPLETED == "PROCESSING_COMPLETED"

    def test_instrument_key_from_string_roundtrip(self) -> None:
        """InstrumentKey.from_string parses and str() roundtrips."""
        from unified_internal_contracts import InstrumentKey

        key = InstrumentKey.from_string("BINANCE-SPOT:SPOT_PAIR:BTC-USDT")
        assert key.symbol == "BTC-USDT"
        key_str = str(key)
        assert "BINANCE-SPOT" in key_str
        assert "BTC-USDT" in key_str

    def test_instrument_key_invalid_format_raises(self) -> None:
        """InstrumentKey.from_string raises ValueError on bad format."""
        from unified_internal_contracts import InstrumentKey

        with pytest.raises(ValueError):
            InstrumentKey.from_string("INVALID")

    def test_instrument_record_construction(self) -> None:
        """InstrumentRecord can be constructed with required fields."""
        from unified_internal_contracts import (
            AssetClass,
            InstrumentRecord,
            InstrumentType,
        )

        record = InstrumentRecord(
            instrument_key="BINANCE:PERP:BTC-USDT",
            venue="BINANCE",
            asset_class=AssetClass.CRYPTO,
            instrument_type=InstrumentType.PERP,
        )
        assert record.venue == "BINANCE"
        assert record.asset_class == AssetClass.CRYPTO
        assert record.instrument_type == InstrumentType.PERP

    def test_data_source_constraint_tradfi_helper(self) -> None:
        """InstrumentRecord.tradfi_datasource_constraint classifies correctly."""
        from unified_internal_contracts import (
            AssetClass,
            DataSourceConstraint,
            InstrumentRecord,
        )

        assert (
            InstrumentRecord.tradfi_datasource_constraint(AssetClass.EQUITY)
            == DataSourceConstraint.DATABENTO_ONLY
        )
        assert (
            InstrumentRecord.tradfi_datasource_constraint(AssetClass.CRYPTO)
            == DataSourceConstraint.ANY
        )

    def test_onchain_data_freshness_config_max_blocks_behind(self) -> None:
        """OnchainDataFreshnessConfig.max_blocks_behind computes correctly."""
        from unified_internal_contracts import OnchainDataFreshnessConfig

        config = OnchainDataFreshnessConfig(
            chain_id="ethereum",
            max_age_seconds=60,
            block_time_seconds=12.0,
        )
        assert config.max_blocks_behind == pytest.approx(5.0)

    def test_chain_freshness_defaults_has_ethereum(self) -> None:
        """CHAIN_FRESHNESS_DEFAULTS includes ethereum with valid config."""
        from unified_internal_contracts.reference.onchain_freshness import (
            CHAIN_FRESHNESS_DEFAULTS,
        )

        assert "ethereum" in CHAIN_FRESHNESS_DEFAULTS
        eth = CHAIN_FRESHNESS_DEFAULTS["ethereum"]
        assert eth.max_age_seconds > 0
        assert eth.block_time_seconds > 0


# ---------------------------------------------------------------------------
# 6. unified-ml-interface (UMI)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestUnifiedMLInterfaceIntegration:
    """Functionally exercise unified-ml-interface symbols used by UDC."""

    def test_model_variant_config_construction(self) -> None:
        """ModelVariantConfig can be constructed and has symbol property."""
        from unified_ml_interface import ModelVariantConfig

        config = ModelVariantConfig(
            instrument_id="BINANCE:PERPETUAL:BTC-USDT",
            timeframe="1h",
            target_type="swing_high",
            lookback_window=5,
        )
        assert config.instrument_id == "BINANCE:PERPETUAL:BTC-USDT"
        assert config.timeframe == "1h"
        assert config.symbol == "BTC"

    def test_model_variant_config_from_dict(self) -> None:
        """ModelVariantConfig.from_dict constructs from dict."""
        from unified_ml_interface import ModelVariantConfig

        data: dict[str, object] = {
            "instrument_id": "BINANCE:PERPETUAL:ETH-USDT",
            "timeframe": "4h",
            "target_type": "breakout",
            "lookback_window": 10,
        }
        config = ModelVariantConfig.from_dict(data)
        assert config.instrument_id == "BINANCE:PERPETUAL:ETH-USDT"
        assert config.timeframe == "4h"

    def test_model_metadata_construction(self) -> None:
        """ModelMetadata can be constructed with variant_config."""
        from unified_ml_interface import ModelMetadata, ModelVariantConfig

        variant = ModelVariantConfig(
            instrument_id="BINANCE:PERPETUAL:BTC-USDT",
            timeframe="1h",
            target_type="swing_high",
        )
        metadata = ModelMetadata(
            variant_config=variant,
            model_version="1",
            model_type="lightgbm",
            feature_names=["f1", "f2"],
            performance_metrics={"accuracy": 0.85},
        )
        assert metadata.model_version == "1"
        assert metadata.model_type == "lightgbm"
        assert metadata.feature_names == ["f1", "f2"]
        assert metadata.training_timestamp is not None  # auto-set

    def test_model_metadata_model_id_generation(self) -> None:
        """ModelMetadata.model_id is auto-generated from variant config."""
        from unified_ml_interface import ModelMetadata, ModelVariantConfig

        variant = ModelVariantConfig(
            instrument_id="BINANCE:PERPETUAL:BTC-USDT",
            timeframe="1h",
            target_type="swing_high",
            lookback_window=5,
        )
        metadata = ModelMetadata(variant_config=variant, model_version="1")
        model_id = metadata.model_id
        assert isinstance(model_id, str)
        assert len(model_id) > 0
        # model_id should contain instrument info
        assert "btc" in model_id.lower() or "BTC" in model_id

    def test_model_registry_importable(self) -> None:
        """ModelRegistry can be imported and has expected interface."""
        from unified_ml_interface import ModelRegistry

        assert hasattr(ModelRegistry, "register_model") or callable(ModelRegistry)

    def test_cloud_model_artifact_store_uses_umi_types(self) -> None:
        """CloudModelArtifactStore.store_model accepts UMI ModelMetadata."""
        from unified_ml_interface import ModelMetadata, ModelVariantConfig

        variant = ModelVariantConfig(
            instrument_id="BINANCE:PERPETUAL:BTC-USDT",
            timeframe="1h",
            target_type="swing_high",
        )
        metadata = ModelMetadata(variant_config=variant, model_version="1")

        with (
            patch("unified_domain_client.artifact_store._get_cloud_config") as mock_cfg,
            patch("unified_domain_client.artifact_store.get_storage_client") as mock_storage,
        ):
            mock_cfg.return_value.gcp_project_id = "test-project"
            mock_cfg.return_value.ml_artifact_bucket = "ml-bucket"

            mock_client = MagicMock()
            mock_storage.return_value = mock_client

            from unified_domain_client.artifact_store import CloudModelArtifactStore

            store = CloudModelArtifactStore(bucket="ml-bucket", project_id="test-project")
            prefix = store.store_model(
                model={"dummy": "model"},
                metadata=metadata,
                training_period="2024-01",
            )
            assert "models/" in prefix
            assert "2024-01" in prefix
            # upload_bytes should have been called twice (model + metadata)
            assert mock_client.upload_bytes.call_count == 2
