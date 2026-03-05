"""Unit tests for DomainValidationService."""

import pandas as pd
import pytest

from unified_domain_client.validation import DomainValidationConfig, DomainValidationService


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
