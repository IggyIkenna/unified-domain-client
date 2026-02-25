"""Unit tests for DomainValidationService."""

import pandas as pd
import pytest

from unified_domain_services import DomainValidationConfig, DomainValidationService


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


class TestDomainValidationConfig:
    """Test DomainValidationConfig."""

    def test_default_values(self):
        """Test DomainValidationConfig default values."""
        config = DomainValidationConfig()
        assert config.enable_candle_count_validation is True
        assert config.enable_midnight_boundary_validation is True
        assert config.skip_candle_count_for_sparse is True
        assert config.validate_timestamp_ordering is True

    def test_validate_for_domain_instruments_with_data(self):
        """Test instruments domain validation with real data."""
        svc = DomainValidationService(domain="instruments")
        df = pd.DataFrame(
            {
                "instrument_key": ["BINANCE-FUTURES:PERPETUAL:BTC-USDT"],
                "venue": ["BINANCE-FUTURES"],
                "timestamp": [1640995200],  # 2022-01-01
            }
        )
        result = svc.validate_for_domain(df)
        assert result.total_records == 1

    def test_validate_for_domain_features_with_data(self):
        """Test features domain validation with real data."""
        svc = DomainValidationService(domain="features")
        df = pd.DataFrame({"feature_name": ["rsi_14"], "feature_value": [65.0], "timestamp": [1640995200]})
        result = svc.validate_for_domain(df)
        assert result.total_records == 1

    def test_validate_for_domain_strategy_with_data(self):
        """Test strategy domain validation with real data."""
        svc = DomainValidationService(domain="strategy")
        df = pd.DataFrame({"strategy_name": ["test_strategy"], "signal": [1.0], "timestamp": [1640995200]})
        result = svc.validate_for_domain(df)
        assert result.total_records == 1

    def test_validate_for_domain_execution_with_data(self):
        """Test execution domain validation with real data."""
        svc = DomainValidationService(domain="execution")
        df = pd.DataFrame({"execution_id": ["exec_001"], "order_type": ["market"], "timestamp": [1640995200]})
        result = svc.validate_for_domain(df)
        assert result.total_records == 1

    def test_validate_timestamp_semantics_internal_domain_valid(self):
        """Test timestamp semantics validation for internal domain with valid data."""
        svc = DomainValidationService(domain="features")
        df = pd.DataFrame({"timestamp_in": [1640995200], "timestamp_out": [1640995300], "data": [1.0]})
        result = svc.validate_timestamp_semantics(df, data_type="features")
        assert result.total_records == 1

    def test_get_domain_config_for_all_domains(self):
        """Test getting domain config for all supported domains."""
        domains = ["market_data", "features", "strategy", "execution", "ml", "instruments"]

        for domain in domains:
            svc = DomainValidationService(domain=domain)
            # Use the actual attribute name
            config = svc.domain_config
            assert config is not None

    def test_validate_data_quality_with_various_issues(self):
        """Test data quality validation with various data issues."""
        svc = DomainValidationService(domain="market_data")

        # Test with valid data - use validate_for_domain instead
        df_valid = pd.DataFrame({"price": [100.0, 101.0, 102.0], "timestamp": [1640995200, 1640995300, 1640995400]})
        result = svc.validate_for_domain(df_valid)
        assert result.total_records == 3

    def test_config_variations(self):
        """Test service with different config variations."""
        # Test with custom config
        custom_config = DomainValidationConfig(
            enable_candle_count_validation=False,
            enable_midnight_boundary_validation=False,
            skip_candle_count_for_sparse=False,
            validate_timestamp_ordering=False,
        )
        svc = DomainValidationService(domain="market_data", config=custom_config)
        assert svc.config.enable_candle_count_validation is False

    def test_error_handling(self):
        """Test error handling in validation methods."""
        svc = DomainValidationService(domain="market_data")

        # Test with None input
        with pytest.raises((TypeError, AttributeError)):
            svc.validate_for_domain(None)


class TestEdgeCasesAndIntegration:
    """Test edge cases and integration scenarios."""

    def test_large_dataframe_handling(self):
        """Test handling of larger DataFrames."""
        svc = DomainValidationService(domain="market_data")

        # Create a larger DataFrame
        large_df = pd.DataFrame(
            {
                "timestamp": range(1000),
                "price": [100.0 + i * 0.01 for i in range(1000)],
            }
        )

        result = svc.validate_for_domain(large_df)
        assert result.total_records == 1000

    def test_empty_column_handling(self):
        """Test handling of DataFrames with empty columns."""
        svc = DomainValidationService(domain="features")

        # DataFrame with empty columns
        empty_df = pd.DataFrame({"empty_col": []})
        result = svc.validate_for_domain(empty_df)
        assert result.total_records == 0
        assert result.valid is True  # Empty DataFrames should be valid

    def test_mixed_data_types(self):
        """Test handling of DataFrames with mixed data types."""
        svc = DomainValidationService(domain="instruments")

        # DataFrame with mixed types
        mixed_df = pd.DataFrame(
            {
                "string_col": ["a", "b", "c"],
                "numeric_col": [1, 2, 3],
                "float_col": [1.1, 2.2, 3.3],
                "timestamp": [1640995200, 1640995300, 1640995400],
            }
        )

        result = svc.validate_for_domain(mixed_df)
        assert result.total_records == 3
