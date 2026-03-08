"""Unit tests for DomainValidationService."""

import pandas as pd
import pytest

from unified_domain_client import DomainValidationConfig, DomainValidationService


class TestDomainValidationService:
    """Test DomainValidationService."""

    def test_init_raises_for_unknown_domain(self):
        """Test init raises for unknown domain."""
        with pytest.raises(ValueError, match="Unknown domain"):
            DomainValidationService(domain="unknown_domain")

    def test_init_succeeds_for_valid_domains(self):
        """Test init succeeds for valid domains."""
        for domain in ["market_data", "features", "strategy", "execution", "ml", "instruments"]:
            svc = DomainValidationService(domain=domain)
            assert svc.domain == domain
            assert svc.domain_config is not None

    def test_validate_for_domain_empty_dataframe(self):
        """Test validate_for_domain returns valid for empty DataFrame."""
        svc = DomainValidationService(domain="market_data")
        result = svc.validate_for_domain(pd.DataFrame())
        assert result.valid is True
        assert result.total_records == 0

    def test_validate_for_domain_ml_always_valid(self):
        """Test ML domain always returns valid (no domain-specific validation)."""
        svc = DomainValidationService(domain="ml")
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        result = svc.validate_for_domain(df)
        assert result.valid is True
        assert result.total_records == 2

    def test_validate_for_domain_market_data_candle_count(self):
        """Test market_data domain candle count validation."""
        svc = DomainValidationService(domain="market_data")
        df = pd.DataFrame({"timestamp": [1, 2]})
        result = svc.validate_for_domain(df)
        assert result.validation_type == "domain_validation_market_data"

    def test_validate_timestamp_semantics_external_io_missing_columns(self):
        """Test timestamp semantics validation for external I/O with missing columns."""
        svc = DomainValidationService(domain="market_data")
        df = pd.DataFrame({"timestamp": [1]})
        result = svc.validate_timestamp_semantics(df, data_type="trades")
        assert result.valid is False
        assert any("local_timestamp" in str(e) for e in (result.errors or []))

    def test_validate_timestamp_semantics_internal_domain_missing_timestamp_out(self):
        """Test timestamp semantics for internal domain missing timestamp_out."""
        svc = DomainValidationService(domain="strategy")
        df = pd.DataFrame({"timestamp": [1], "timestamp_in": [1]})
        result = svc.validate_timestamp_semantics(df, data_type="orders")
        assert result.valid is False
        assert any("timestamp_out" in str(e) for e in (result.errors or []))

    def test_validate_bigquery_upload_delegates(self):
        """Test validate_bigquery_upload delegates to validate_for_domain."""
        svc = DomainValidationService(domain="ml")
        df = pd.DataFrame({"x": [1]})
        result = svc.validate_bigquery_upload(df, table_name="test_table")
        assert result.valid is True
        assert result.validation_type == "domain_validation_ml"

    def test_validate_for_domain_execution_with_timestamp(self):
        """Test execution domain validates timestamp ordering."""
        svc = DomainValidationService(domain="execution")
        # Monotonically increasing timestamps — valid
        df = pd.DataFrame({"timestamp": [1000, 2000, 3000]})
        result = svc.validate_for_domain(df)
        assert result.validation_type == "domain_validation_execution"

    def test_validate_for_domain_execution_out_of_order(self):
        """Test execution domain catches out-of-order timestamps."""
        svc = DomainValidationService(domain="execution")
        df = pd.DataFrame({"timestamp": [3000, 1000, 2000]})
        result = svc.validate_for_domain(df)
        assert result.valid is False
        assert any("out-of-order" in str(e) for e in (result.errors or []))

    def test_validate_for_domain_strategy_orders_utc(self):
        """Test strategy domain validates UTC alignment for orders."""
        svc = DomainValidationService(domain="strategy")
        df = pd.DataFrame({"timestamp": [1000], "timestamp_out": [2000], "timestamp_in": [500]})
        result = svc.validate_for_domain(df, data_type="orders")
        assert result.validation_type == "domain_validation_strategy"

    def test_validate_for_domain_features_with_data(self):
        """Test features domain validates UTC alignment and feature completeness."""
        svc = DomainValidationService(domain="features")
        df = pd.DataFrame(
            {"timestamp": [1000000, 2000000], "feature_1": [1.0, 2.0], "feature_2": [3.0, 4.0]}
        )
        result = svc.validate_for_domain(df)
        assert result.validation_type == "domain_validation_features"

    def test_validate_for_domain_instruments_always_valid(self):
        """Test instruments domain is always valid (no domain-specific validation)."""
        svc = DomainValidationService(domain="instruments")
        df = pd.DataFrame({"instrument_id": ["BTC-USDT"], "venue": ["BINANCE"]})
        result = svc.validate_for_domain(df)
        assert result.validation_type == "domain_validation_instruments"

    def test_validate_timestamp_semantics_no_data_type(self):
        """Test validate_timestamp_semantics with no data_type skips all checks."""
        svc = DomainValidationService(domain="market_data")
        df = pd.DataFrame({"x": [1, 2]})
        result = svc.validate_timestamp_semantics(df, data_type=None)
        assert result.valid is True

    def test_validate_timestamp_semantics_external_io_with_columns(self):
        """Test timestamp semantics for external I/O with both required columns."""
        svc = DomainValidationService(domain="market_data")
        df = pd.DataFrame(
            {
                "timestamp": [1000000, 2000000],
                "local_timestamp": [1100000, 2100000],
            }
        )
        result = svc.validate_timestamp_semantics(df, data_type="trades")
        assert result.valid is True

    def test_validate_timestamp_semantics_external_io_local_before_exchange(self):
        """Test timestamp semantics warns when local_timestamp < timestamp."""
        svc = DomainValidationService(domain="market_data")
        df = pd.DataFrame(
            {
                "timestamp": [2000000, 3000000],
                "local_timestamp": [1000000, 4000000],  # first row: local < exchange
            }
        )
        result = svc.validate_timestamp_semantics(df, data_type="fills")
        # Should still be valid=True (warning, not error)
        assert result.valid is True
        assert any("local_timestamp < timestamp" in str(w) for w in (result.warnings or []))

    def test_validate_timestamp_semantics_internal_domain_with_all_columns(self):
        """Test timestamp semantics for internal domain with all columns."""
        svc = DomainValidationService(domain="strategy")
        df = pd.DataFrame(
            {
                "timestamp": [1000000],
                "timestamp_in": [900000],
                "timestamp_out": [1100000],
            }
        )
        result = svc.validate_timestamp_semantics(df, data_type="orders")
        assert result.valid is True

    def test_validate_timestamp_semantics_internal_domain_missing_timestamp(self):
        """Test timestamp semantics for internal domain missing timestamp column."""
        svc = DomainValidationService(domain="strategy")
        df = pd.DataFrame({"timestamp_in": [1000], "timestamp_out": [2000]})
        result = svc.validate_timestamp_semantics(df, data_type="execution_logs")
        assert result.valid is False

    def test_validate_timestamp_semantics_internal_timestamp_ordering_warning(self):
        """Test timestamp semantics warns when timestamp_out < timestamp."""
        svc = DomainValidationService(domain="strategy")
        df = pd.DataFrame(
            {
                "timestamp": [2000000],
                "timestamp_in": [1000000],
                "timestamp_out": [500000],  # out < timestamp (bad)
            }
        )
        result = svc.validate_timestamp_semantics(df, data_type="orders")
        assert any("timestamp_out < timestamp" in str(w) for w in (result.warnings or []))

    def test_candle_count_empty_dataframe(self):
        """Test _validate_candle_count returns invalid for empty DataFrame."""
        svc = DomainValidationService(domain="market_data")
        result = svc._validate_candle_count(pd.DataFrame())
        assert result.valid is False

    def test_midnight_boundary_missing_timestamp_column(self):
        """Test _validate_midnight_boundaries fails without timestamp column."""
        svc = DomainValidationService(domain="market_data")
        result = svc._validate_midnight_boundaries(pd.DataFrame({"x": [1]}))
        assert result.valid is False

    def test_midnight_boundary_no_midnight_candles(self):
        """Test _validate_midnight_boundaries fails when no midnight candles."""
        svc = DomainValidationService(domain="market_data")
        # 1 hour into day in microseconds
        df = pd.DataFrame({"timestamp": [3600 * 1_000_000]})
        result = svc._validate_midnight_boundaries(df)
        assert result.valid is False

    def test_midnight_boundary_with_midnight_candle(self):
        """Test _validate_midnight_boundaries succeeds with midnight candle."""
        svc = DomainValidationService(domain="market_data")
        # 0 microseconds = midnight UTC on 1970-01-01
        df = pd.DataFrame({"timestamp": [0]})
        result = svc._validate_midnight_boundaries(df)
        assert result.valid is True

    def test_utc_alignment_missing_column(self):
        """Test _validate_utc_alignment fails when timestamp column missing."""
        svc = DomainValidationService(domain="market_data")
        result = svc._validate_utc_alignment(pd.DataFrame({"x": [1]}))
        assert result.valid is False

    def test_utc_alignment_with_valid_timestamps(self):
        """Test _validate_utc_alignment succeeds with valid timestamps."""
        svc = DomainValidationService(domain="market_data")
        df = pd.DataFrame({"timestamp": [1000000, 2000000]})
        result = svc._validate_utc_alignment(df)
        assert result.valid is True

    def test_timestamp_ordering_missing_columns(self):
        """Test _validate_timestamp_ordering fails when no timestamp columns."""
        svc = DomainValidationService(domain="execution")
        result = svc._validate_timestamp_ordering(pd.DataFrame({"x": [1]}))
        assert result.valid is False

    def test_timestamp_ordering_sorted(self):
        """Test _validate_timestamp_ordering succeeds for sorted timestamps."""
        svc = DomainValidationService(domain="execution")
        df = pd.DataFrame({"timestamp": [1000, 2000, 3000]})
        result = svc._validate_timestamp_ordering(df)
        assert result.valid is True

    def test_feature_completeness_no_feature_cols(self):
        """Test _validate_feature_completeness warns when no feature columns."""
        svc = DomainValidationService(domain="features")
        df = pd.DataFrame({"timestamp": [1], "venue": ["BINANCE"], "symbol": ["BTC"]})
        result = svc._validate_feature_completeness(df)
        assert result.valid is True
        assert any("No feature columns" in str(w) for w in (result.warnings or []))

    def test_feature_completeness_high_nan_warning(self):
        """Test _validate_feature_completeness warns for >10% NaN columns."""
        import numpy as np

        svc = DomainValidationService(domain="features")
        # 5 rows, feature_1 has 4 NaN (80%)
        df = pd.DataFrame({"feature_1": [1.0, np.nan, np.nan, np.nan, np.nan]})
        result = svc._validate_feature_completeness(df)
        assert any("NaN" in str(w) for w in (result.warnings or []))

    def test_validate_for_domain_custom_config(self):
        """Test validate_for_domain with custom config disabling checks."""
        config = DomainValidationConfig(
            enable_candle_count_validation=False,
            enable_midnight_boundary_validation=False,
            enable_utc_alignment_validation=False,
        )
        svc = DomainValidationService(domain="market_data", config=config)
        df = pd.DataFrame({"timestamp": [1000, 2000]})
        result = svc.validate_for_domain(df)
        assert result.validation_type == "domain_validation_market_data"


class TestDomainValidationConfig:
    """Test DomainValidationConfig."""

    def test_default_values(self):
        """Test DomainValidationConfig default values."""
        config = DomainValidationConfig()
        assert config.enable_candle_count_validation is True
        assert config.enable_midnight_boundary_validation is True
        assert config.skip_candle_count_for_sparse is True
        assert config.validate_timestamp_ordering is True

    def test_custom_values(self):
        """Test DomainValidationConfig with custom values."""
        config = DomainValidationConfig(
            enable_candle_count_validation=False,
            enable_utc_alignment_validation=False,
        )
        assert config.enable_candle_count_validation is False
        assert config.enable_utc_alignment_validation is False
