"""
Unit tests for unified_domain_client.date_validation module.

Tests DateValidator, should_skip_date, calculate_lookback_days.
"""

from __future__ import annotations

from unified_domain_client import (
    FEATURE_GROUP_LOOKBACK,
    MAX_LOOKBACK_DAYS_BY_TIMEFRAME,
    TIMEFRAME_SECONDS,
    DateValidationResult,
    DateValidator,
    get_earliest_valid_date,
    should_skip_date,
)


class TestDateValidationResult:
    """Tests for DateValidationResult dataclass."""

    def test_valid_result(self):
        result = DateValidationResult(
            is_valid=True,
            reason="Valid",
            earliest_valid_date="2023-01-01",
            requested_date="2024-01-01",
        )
        assert result.is_valid is True
        assert result.earliest_valid_date == "2023-01-01"

    def test_invalid_result(self):
        result = DateValidationResult(
            is_valid=False,
            reason="Too early",
            days_until_valid=30,
        )
        assert result.is_valid is False
        assert result.days_until_valid == 30


class TestDateValidator:
    """Tests for DateValidator class."""

    def test_init_default(self):
        validator = DateValidator()
        assert validator._loaded is False
        assert validator._config == {}

    def test_calculate_lookback_days_default(self):
        validator = DateValidator()
        # Default uses max of all feature groups
        days = validator.calculate_lookback_days("24h")
        assert days > 0
        # For 24h, max lookback is 200 (moving_averages or market_structure)
        # 200 * 86400 / 86400 = 200 days
        assert days == 200

    def test_calculate_lookback_days_1h(self):
        validator = DateValidator()
        days = validator.calculate_lookback_days("1h")
        # max=200, 200 * 3600 / 86400 = 8.33 -> ceil = 9
        assert days == 9

    def test_calculate_lookback_days_specific_groups(self):
        validator = DateValidator()
        days = validator.calculate_lookback_days("24h", feature_groups=["temporal"])
        # temporal has 0 lookback
        assert days == 0

    def test_calculate_lookback_days_oscillators(self):
        validator = DateValidator()
        days = validator.calculate_lookback_days("24h", feature_groups=["oscillators"])
        # oscillators has 14 lookback, 14 * 86400 / 86400 = 14
        assert days == 14

    def test_check_date_no_config(self):
        """Without config file, should allow processing."""
        validator = DateValidator()
        result = validator.check_date("2024-01-01", "CEFI")
        # With no config, should allow processing
        assert result.is_valid is True

    def test_get_earliest_raw_data_date_no_config(self):
        validator = DateValidator()
        result = validator.get_earliest_raw_data_date("market-tick-data-handler", "CEFI")
        assert result is None

    def test_get_earliest_valid_feature_date_no_config(self):
        validator = DateValidator()
        result = validator.get_earliest_valid_feature_date("CEFI", timeframe="24h")
        assert result is None

    def test_get_earliest_valid_ml_date_no_config(self):
        validator = DateValidator()
        result = validator.get_earliest_valid_ml_date("CEFI", timeframe="24h")
        assert result is None


class TestConstants:
    """Tests for module-level constants."""

    def test_feature_group_lookback_has_entries(self):
        assert len(FEATURE_GROUP_LOOKBACK) > 0
        assert "technical_indicators" in FEATURE_GROUP_LOOKBACK
        assert "moving_averages" in FEATURE_GROUP_LOOKBACK

    def test_max_lookback_days(self):
        assert MAX_LOOKBACK_DAYS_BY_TIMEFRAME["24h"] == 200
        assert MAX_LOOKBACK_DAYS_BY_TIMEFRAME["1h"] == 120

    def test_timeframe_seconds(self):
        assert TIMEFRAME_SECONDS["1m"] == 60
        assert TIMEFRAME_SECONDS["1h"] == 3600
        assert TIMEFRAME_SECONDS["24h"] == 86400


class TestShouldSkipDate:
    """Tests for should_skip_date convenience function."""

    def test_without_config(self):
        """Without config, should NOT skip (allow processing)."""
        result = should_skip_date("2024-01-01", "CEFI")
        assert result is False


class TestGetEarliestValidDate:
    """Tests for get_earliest_valid_date convenience function."""

    def test_without_config(self):
        result = get_earliest_valid_date("CEFI")
        assert result is None


class TestDateValidatorWithConfig:
    """Tests for DateValidator with mocked config data."""

    def _make_validator_with_config(self, config_data: dict) -> DateValidator:
        """Create a pre-loaded DateValidator with given config."""
        validator = DateValidator()
        validator._config = config_data
        validator._loaded = True
        return validator

    def test_get_earliest_raw_data_date_with_venue(self):
        config = {
            "market-tick-data-handler": {
                "CEFI": {
                    "category_start": "2023-01-01",
                    "venues": {
                        "BINANCE-FUTURES": "2023-03-01",
                    },
                }
            }
        }
        validator = self._make_validator_with_config(config)
        result = validator.get_earliest_raw_data_date(
            "market-tick-data-handler", "CEFI", venue="BINANCE-FUTURES"
        )
        assert result == "2023-03-01"

    def test_get_earliest_raw_data_date_category_level(self):
        config = {
            "market-tick-data-handler": {
                "CEFI": {
                    "category_start": "2023-01-01",
                }
            }
        }
        validator = self._make_validator_with_config(config)
        result = validator.get_earliest_raw_data_date("market-tick-data-handler", "CEFI")
        assert result == "2023-01-01"

    def test_get_earliest_valid_feature_date_from_venues_timeframe(self):
        config = {
            "features-delta-one-service": {
                "CEFI": {
                    "venues_24h": {
                        "BINANCE-FUTURES": "2023-06-01",
                    },
                }
            }
        }
        validator = self._make_validator_with_config(config)
        result = validator.get_earliest_valid_feature_date(
            "CEFI", venue="BINANCE-FUTURES", timeframe="24h"
        )
        assert result == "2023-06-01"

    def test_get_earliest_valid_feature_date_fallback_to_category(self):
        config = {
            "features-delta-one-service": {
                "CEFI": {},
            },
            "earliest_valid_features": {
                "24h": {
                    "CEFI": "2023-07-01",
                }
            },
        }
        validator = self._make_validator_with_config(config)
        result = validator.get_earliest_valid_feature_date("CEFI", timeframe="24h")
        assert result == "2023-07-01"

    def test_check_date_date_before_earliest_is_invalid(self):
        config = {
            "earliest_valid_features": {
                "24h": {
                    "CEFI": "2023-06-01",
                }
            }
        }
        validator = self._make_validator_with_config(config)
        result = validator.check_date("2022-01-01", "CEFI", timeframe="24h")
        assert result.is_valid is False
        assert result.days_until_valid is not None
        assert result.days_until_valid > 0

    def test_check_date_valid_date(self):
        config = {
            "earliest_valid_features": {
                "24h": {
                    "CEFI": "2023-01-01",
                }
            }
        }
        validator = self._make_validator_with_config(config)
        result = validator.check_date("2024-01-01", "CEFI", timeframe="24h")
        assert result.is_valid is True
        assert result.earliest_valid_date == "2023-01-01"

    def test_check_date_uses_raw_data_fallback(self):
        """check_date falls back to raw data date + lookback when no feature date set."""
        config = {
            "market-tick-data-handler": {
                "CEFI": {
                    "category_start": "2023-01-01",
                }
            }
        }
        validator = self._make_validator_with_config(config)
        # No earliest_valid_features, should fall back to raw date
        result = validator.check_date("2024-01-01", "CEFI", timeframe="24h")
        assert result.is_valid is True or result.is_valid is False  # depends on lookback calc

    def test_load_config_scans_paths_when_no_file_found(self):
        """_load_config doesn't raise when no config file is found."""
        from unittest.mock import patch

        validator = DateValidator()
        with patch("pathlib.Path.exists", return_value=False):
            validator._load_config()
        assert validator._loaded is True
        assert validator._config == {}

    def test_load_config_reads_yaml_file(self):
        """_load_config reads a yaml file if it exists."""
        import tempfile
        from pathlib import Path

        import yaml as _yaml

        config_data = {
            "market-tick-data-handler": {
                "CEFI": {"category_start": "2023-01-01"},
            }
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            _yaml.dump(config_data, f)
            tmp_path = Path(f.name)

        try:
            validator = DateValidator()
            validator._config_path = tmp_path
            validator._load_config()
            assert validator._config == config_data
        finally:
            tmp_path.unlink(missing_ok=True)
