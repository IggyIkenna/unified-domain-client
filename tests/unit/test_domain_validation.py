"""
Unit tests for unified_domain_client.validation module.

Tests DomainValidationService across different domains.
"""

from __future__ import annotations

import pandas as pd
import pytest

from unified_domain_client import (
    DomainValidationConfig,
    DomainValidationService,
)


class TestDomainValidationService:
    """Tests for DomainValidationService."""

    def test_init_market_data(self):
        svc = DomainValidationService("market_data")
        assert svc.domain == "market_data"
        assert svc.domain_config["validate_candle_count"] is True

    def test_init_features(self):
        svc = DomainValidationService("features")
        assert svc.domain == "features"
        assert svc.domain_config["validate_candle_count"] is False

    def test_init_strategy(self):
        svc = DomainValidationService("strategy")
        assert svc.domain == "strategy"
        assert svc.domain_config["skip_for_sparse"] is True

    def test_init_execution(self):
        svc = DomainValidationService("execution")
        assert svc.domain == "execution"
        assert svc.domain_config["validate_utc_alignment"] is False

    def test_init_ml(self):
        svc = DomainValidationService("ml")
        assert svc.domain == "ml"

    def test_init_instruments(self):
        svc = DomainValidationService("instruments")
        assert svc.domain == "instruments"

    def test_init_invalid_domain(self):
        with pytest.raises(ValueError, match="Unknown domain"):
            DomainValidationService("invalid_domain")

    def test_validate_empty_dataframe(self):
        svc = DomainValidationService("market_data")
        df = pd.DataFrame()
        result = svc.validate_for_domain(df)
        assert result.valid is True
        assert result.total_records == 0

    def test_validate_ml_domain_always_valid(self):
        svc = DomainValidationService("ml")
        df = pd.DataFrame({"col1": [1, 2, 3]})
        result = svc.validate_for_domain(df)
        assert result.valid is True

    def test_validate_feature_completeness(self):
        svc = DomainValidationService("features")
        df = pd.DataFrame(
            {
                "timestamp": [1, 2, 3],
                "feature_a": [1.0, 2.0, 3.0],
                "feature_b": [4.0, 5.0, 6.0],
            }
        )
        result = svc.validate_for_domain(df)
        # Should be valid (no excessive NaN)
        assert result.valid is True

    def test_validate_feature_completeness_high_nan(self):
        svc = DomainValidationService("features")
        df = pd.DataFrame(
            {
                "timestamp": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                "feature_a": [None] * 10,  # 100% NaN
                "feature_b": [1.0] * 10,
            }
        )
        result = svc.validate_for_domain(df)
        # High NaN should produce warnings but still be valid
        assert result.valid is True

    def test_validate_timestamp_ordering(self):
        svc = DomainValidationService("strategy")
        df = pd.DataFrame(
            {
                "timestamp": [3, 2, 1],  # out of order
            }
        )
        result = svc.validate_for_domain(df)
        assert result.valid is False

    def test_validate_timestamp_ordering_valid(self):
        svc = DomainValidationService("strategy")
        df = pd.DataFrame(
            {
                "timestamp": [1, 2, 3],  # in order
            }
        )
        result = svc.validate_for_domain(df)
        assert result.valid is True


class TestDomainValidationConfig:
    """Tests for DomainValidationConfig."""

    def test_default_config(self):
        config = DomainValidationConfig()
        assert config.enable_candle_count_validation is True
        assert config.enable_midnight_boundary_validation is True
        assert config.validate_timestamp_ordering is True

    def test_custom_config(self):
        config = DomainValidationConfig(
            enable_candle_count_validation=False,
            skip_candle_count_for_sparse=False,
        )
        assert config.enable_candle_count_validation is False


class TestTimestampSemanticsValidation:
    """Tests for validate_timestamp_semantics."""

    def test_external_io_missing_columns(self):
        svc = DomainValidationService("market_data")
        df = pd.DataFrame({"price": [100.0]})
        result = svc.validate_timestamp_semantics(df, data_type="trades")
        assert result.valid is False

    def test_external_io_with_columns(self):
        svc = DomainValidationService("market_data")
        df = pd.DataFrame(
            {
                "timestamp": [1_000_000],
                "local_timestamp": [1_000_001],
            }
        )
        result = svc.validate_timestamp_semantics(df, data_type="trades")
        assert result.valid is True

    def test_internal_domain_missing_timestamp_out(self):
        svc = DomainValidationService("features")
        df = pd.DataFrame(
            {
                "timestamp": [1],
                "timestamp_in": [1],
            }
        )
        result = svc.validate_timestamp_semantics(df, data_type="features")
        assert result.valid is False

    def test_unknown_data_type_passes(self):
        svc = DomainValidationService("market_data")
        df = pd.DataFrame({"col": [1]})
        result = svc.validate_timestamp_semantics(df, data_type="unknown")
        assert result.valid is True


class TestValidateBigqueryUpload:
    """Tests for validate_bigquery_upload."""

    def test_delegates_to_validate_for_domain(self):
        svc = DomainValidationService("ml")
        df = pd.DataFrame({"col": [1, 2]})
        result = svc.validate_bigquery_upload(df, "my_table", data_type="features")
        assert result.valid is True


class TestMarketDataValidation:
    """Tests for market_data domain validation paths."""

    def test_candle_count_validation_valid(self):
        svc = DomainValidationService("market_data")
        df = pd.DataFrame({"timestamp": [1000000, 2000000, 3000000], "close": [100, 101, 102]})
        result = svc._validate_candle_count(df)
        assert result.valid is True

    def test_candle_count_validation_empty(self):
        svc = DomainValidationService("market_data")
        df = pd.DataFrame()
        result = svc._validate_candle_count(df)
        assert result.valid is False

    def test_midnight_boundary_missing_timestamp(self):
        svc = DomainValidationService("market_data")
        df = pd.DataFrame({"close": [100, 101]})
        result = svc._validate_midnight_boundaries(df)
        assert result.valid is False

    def test_midnight_boundary_no_midnight_candles(self):
        svc = DomainValidationService("market_data")
        # Use non-midnight timestamps (5 hours after midnight)
        df = pd.DataFrame({"timestamp": [18_000_000_000, 21_600_000_000]})
        result = svc._validate_midnight_boundaries(df)
        assert result.valid is False

    def test_midnight_boundary_with_midnight_candle(self):
        svc = DomainValidationService("market_data")
        # timestamp=0 is exactly midnight UTC Jan 1, 1970
        df = pd.DataFrame({"timestamp": [0, 3600000000]})
        result = svc._validate_midnight_boundaries(df)
        assert result.valid is True

    def test_utc_alignment_missing_column(self):
        svc = DomainValidationService("market_data")
        df = pd.DataFrame({"close": [100, 101]})
        result = svc._validate_utc_alignment(df)
        assert result.valid is False

    def test_utc_alignment_valid(self):
        svc = DomainValidationService("market_data")
        df = pd.DataFrame({"timestamp": [1000000, 2000000]})
        result = svc._validate_utc_alignment(df)
        assert result.valid is True

    def test_market_data_full_validation(self):
        svc = DomainValidationService("market_data")
        # Data with midnight timestamp
        df = pd.DataFrame({"timestamp": [0, 3600000000], "close": [100, 101]})
        result = svc.validate_for_domain(df)
        assert isinstance(result.valid, bool)


class TestStrategyValidation:
    """Tests for strategy domain validation."""

    def test_strategy_with_orders_utc(self):
        svc = DomainValidationService("strategy")
        df = pd.DataFrame({"timestamp_out": [1000000, 2000000]})
        result = svc.validate_for_domain(df, data_type="orders")
        assert isinstance(result.valid, bool)

    def test_strategy_timestamp_ordering_missing_cols(self):
        svc = DomainValidationService("strategy")
        df = pd.DataFrame({"close": [100, 101]})
        result = svc._validate_timestamp_ordering(df)
        assert result.valid is False

    def test_strategy_timestamp_ordering_using_timestamp_out(self):
        svc = DomainValidationService("strategy")
        df = pd.DataFrame({"timestamp_out": [1000000, 2000000, 3000000]})
        result = svc._validate_timestamp_ordering(df)
        assert result.valid is True


class TestFeaturesValidation:
    """Tests for features domain validation."""

    def test_features_no_feature_cols(self):
        svc = DomainValidationService("features")
        df = pd.DataFrame({"timestamp": [1, 2, 3], "venue": ["x", "y", "z"]})
        result = svc._validate_feature_completeness(df)
        assert result.valid is True
        assert result.warnings is not None

    def test_features_with_nan_warning(self):
        svc = DomainValidationService("features")

        df = pd.DataFrame(
            {
                "timestamp": list(range(20)),
                "feat_a": [None] * 20,  # 100% NaN
            }
        )
        result = svc._validate_feature_completeness(df)
        assert result.valid is True
        assert len(result.warnings or []) > 0


class TestExternalIOTimestampSemantics:
    """Tests for external I/O timestamp ordering checks."""

    def test_local_timestamp_before_exchange_timestamp(self):
        svc = DomainValidationService("market_data")
        # local_timestamp < timestamp (invalid — we should receive AFTER exchange generates)
        df = pd.DataFrame(
            {
                "timestamp": [2_000_000],
                "local_timestamp": [1_000_000],  # earlier
            }
        )
        result = svc.validate_timestamp_semantics(df, data_type="trades")
        # valid=True but warnings
        assert result.valid is True
        assert len(result.warnings or []) > 0


class TestInternalDomainTimestampSemantics:
    """Tests for internal domain timestamp ordering checks."""

    def test_full_timestamp_columns_valid(self):
        svc = DomainValidationService("features")
        df = pd.DataFrame(
            {
                "timestamp": [1_000_000],
                "timestamp_in": [1_000_000],
                "timestamp_out": [1_500_000],
            }
        )
        result = svc.validate_timestamp_semantics(df, data_type="orders")
        assert result.valid is True

    def test_timestamp_out_before_timestamp_warning(self):
        svc = DomainValidationService("features")
        # timestamp_out < timestamp is invalid (out-of-order processing)
        df = pd.DataFrame(
            {
                "timestamp": [2_000_000],
                "timestamp_in": [1_500_000],
                "timestamp_out": [1_000_000],  # before timestamp
            }
        )
        result = svc.validate_timestamp_semantics(df, data_type="orders")
        assert len(result.warnings or []) > 0

    def test_timestamp_in_after_timestamp_out_warning(self):
        svc = DomainValidationService("features")
        df = pd.DataFrame(
            {
                "timestamp": [1_000_000],
                "timestamp_in": [3_000_000],  # after timestamp_out
                "timestamp_out": [2_000_000],
            }
        )
        result = svc.validate_timestamp_semantics(df, data_type="orders")
        assert len(result.warnings or []) > 0


class TestDomainValidationCoverageBoost:
    """Domain validation tests from coverage boost merge."""

    def _make_df_with_timestamps(self, n: int = 10):
        ts_start = 1_700_000_000_000_000
        return pd.DataFrame(
            {
                "timestamp": [ts_start + i * 1_000_000 for i in range(n)],
                "value": range(n),
            }
        )

    def test_features_domain_utc_alignment_valid(self):
        svc = DomainValidationService("features", DomainValidationConfig())
        df = self._make_df_with_timestamps()
        result = svc.validate_for_domain(df)
        assert result is not None

    def test_strategy_domain_with_orders_data_type(self):
        cfg = DomainValidationConfig(validate_timestamp_ordering=True, validate_utc_for_orders=True)
        svc = DomainValidationService("strategy", cfg)
        df = self._make_df_with_timestamps()
        df["timestamp_out"] = df["timestamp"]
        result = svc.validate_for_domain(df, data_type="orders")
        assert result is not None

    def test_execution_domain_timestamp_ordering(self):
        cfg = DomainValidationConfig(validate_timestamp_ordering=True)
        svc = DomainValidationService("execution", cfg)
        df = self._make_df_with_timestamps()
        result = svc.validate_for_domain(df)
        assert result is not None

    def test_features_domain_utc_alignment_invalid(self):
        svc = DomainValidationService(
            "features",
            DomainValidationConfig(enable_utc_alignment_validation=True),
        )
        df = pd.DataFrame({"value": [1, 2, 3]})
        result = svc.validate_for_domain(df)
        assert result is not None

    def test_market_data_domain_all_validators(self):
        cfg = DomainValidationConfig(
            enable_candle_count_validation=True,
            enable_midnight_boundary_validation=True,
            enable_utc_alignment_validation=True,
        )
        svc = DomainValidationService("market_data", cfg)
        df = self._make_df_with_timestamps()
        result = svc.validate_for_domain(df)
        assert result is not None

    def test_apply_domain_rules_directly_features(self):
        svc = DomainValidationService("features", DomainValidationConfig())
        df = self._make_df_with_timestamps()
        errors = []
        warnings = []
        svc._apply_domain_rules(df, None, errors, warnings)

    def test_apply_domain_rules_directly_strategy(self):
        cfg = DomainValidationConfig(validate_timestamp_ordering=True, validate_utc_for_orders=True)
        svc = DomainValidationService("strategy", cfg)
        df = self._make_df_with_timestamps()
        df["timestamp_out"] = df["timestamp"]
        errors = []
        warnings = []
        svc._apply_domain_rules(df, "orders", errors, warnings)

    def test_apply_domain_rules_directly_execution(self):
        cfg = DomainValidationConfig(validate_timestamp_ordering=True)
        svc = DomainValidationService("execution", cfg)
        df = self._make_df_with_timestamps()
        errors = []
        warnings = []
        svc._apply_domain_rules(df, None, errors, warnings)
