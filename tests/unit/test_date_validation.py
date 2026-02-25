"""Unit tests for date validation utilities."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest

from unified_domain_services import (
    DateValidationResult,
    DateValidator,
    get_earliest_valid_date,
    get_validator,
    should_skip_date,
)


class TestDateValidationResult:
    """Test DateValidationResult class."""

    def test_init_valid_result(self):
        """Test DateValidationResult initialization for valid result."""
        result = DateValidationResult(
            is_valid=True,
            reason="Date is valid for processing",
            earliest_valid_date="2023-01-01",
            requested_date="2023-01-15",
        )

        assert result.is_valid is True
        assert result.reason == "Date is valid for processing"
        assert result.earliest_valid_date == "2023-01-01"
        assert result.requested_date == "2023-01-15"
        assert result.days_until_valid is None

    def test_init_invalid_result(self):
        """Test DateValidationResult initialization for invalid result."""
        result = DateValidationResult(
            is_valid=False,
            reason="Date too early",
            earliest_valid_date="2023-01-15",
            requested_date="2023-01-01",
            days_until_valid=14,
        )

        assert result.is_valid is False
        assert result.reason == "Date too early"
        assert result.earliest_valid_date == "2023-01-15"
        assert result.requested_date == "2023-01-01"
        assert result.days_until_valid == 14


class TestDateValidator:
    """Test DateValidator class."""

    def test_init_default(self):
        """Test DateValidator initialization with defaults."""
        validator = DateValidator()

        assert validator._config == {}
        assert validator._config_path is None
        assert validator._loaded is False

    def test_init_with_config_path(self):
        """Test DateValidator initialization with config path."""
        config_path = Path("/test/config.yaml")
        validator = DateValidator(config_path=config_path)

        assert validator._config_path == config_path
        assert validator._loaded is False

    @patch("builtins.open", mock_open(read_data="test_service:\n  CEFI:\n    category_start: '2023-01-01'"))
    @patch("pathlib.Path.exists", return_value=True)
    def test_load_config_success(self, mock_exists):
        """Test successful config loading."""
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as tmp:
            config_path = Path(tmp.name)

        validator = DateValidator(config_path=config_path)
        validator._load_config()

        assert validator._loaded is True
        assert "test_service" in validator._config

    @patch("pathlib.Path.exists", return_value=False)
    def test_load_config_not_found(self, mock_exists):
        """Test config loading when file not found."""
        validator = DateValidator()
        validator._load_config()

        assert validator._loaded is True
        assert validator._config == {}

    @patch(
        "builtins.open",
        mock_open(
            read_data="market-tick-data-handler:\n  CEFI:\n    category_start: '2023-01-01'\n    venues:\n      BINANCE: '2023-01-15'"
        ),
    )
    @patch("pathlib.Path.exists", return_value=True)
    def test_get_earliest_raw_data_date_category_level(self, mock_exists):
        """Test getting earliest raw data date at category level."""
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as tmp:
            config_path = Path(tmp.name)

        validator = DateValidator(config_path=config_path)
        result = validator.get_earliest_raw_data_date("market-tick-data-handler", "CEFI")

        assert result == "2023-01-01"

    @patch(
        "builtins.open",
        mock_open(
            read_data="market-tick-data-handler:\n  CEFI:\n    category_start: '2023-01-01'\n    venues:\n      BINANCE: '2023-01-15'"
        ),
    )
    @patch("pathlib.Path.exists", return_value=True)
    def test_get_earliest_raw_data_date_venue_level(self, mock_exists):
        """Test getting earliest raw data date at venue level."""
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as tmp:
            config_path = Path(tmp.name)

        validator = DateValidator(config_path=config_path)
        result = validator.get_earliest_raw_data_date("market-tick-data-handler", "CEFI", "BINANCE")

        assert result == "2023-01-15"

    def test_get_earliest_raw_data_date_service_not_found(self):
        """Test getting earliest raw data date for missing service."""
        validator = DateValidator()
        validator._config = {}
        validator._loaded = True

        result = validator.get_earliest_raw_data_date("missing-service", "CEFI")
        assert result is None

    def test_get_earliest_raw_data_date_category_not_found(self):
        """Test getting earliest raw data date for missing category."""
        validator = DateValidator()
        validator._config = {"test-service": {}}
        validator._loaded = True

        result = validator.get_earliest_raw_data_date("test-service", "MISSING")
        assert result is None

    @patch(
        "builtins.open",
        mock_open(
            read_data="features-delta-one-service:\n  CEFI:\n    venues_24h:\n      BINANCE: '2023-02-01'\neligible_valid_features:\n  24h:\n    CEFI: '2023-01-20'"
        ),
    )
    @patch("pathlib.Path.exists", return_value=True)
    def test_get_earliest_valid_feature_date_venue_specific(self, mock_exists):
        """Test getting earliest valid feature date for specific venue."""
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as tmp:
            config_path = Path(tmp.name)

        validator = DateValidator(config_path=config_path)
        result = validator.get_earliest_valid_feature_date("CEFI", "BINANCE", "24h")

        assert result == "2023-02-01"

    @patch("builtins.open", mock_open(read_data="earliest_valid_features:\n  24h:\n    CEFI: '2023-01-20'"))
    @patch("pathlib.Path.exists", return_value=True)
    def test_get_earliest_valid_feature_date_category_fallback(self, mock_exists):
        """Test getting earliest valid feature date with category fallback."""
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as tmp:
            config_path = Path(tmp.name)

        validator = DateValidator(config_path=config_path)
        result = validator.get_earliest_valid_feature_date("CEFI", venue=None, timeframe="24h")

        assert result == "2023-01-20"

    def test_get_earliest_valid_feature_date_not_found(self):
        """Test getting earliest valid feature date when not found."""
        validator = DateValidator()
        validator._config = {}
        validator._loaded = True

        result = validator.get_earliest_valid_feature_date("CEFI")
        assert result is None

    @patch(
        "builtins.open",
        mock_open(read_data="ml-training-service:\n  earliest_valid_ml:\n    24h:\n      CEFI: '2023-02-15'"),
    )
    @patch("pathlib.Path.exists", return_value=True)
    def test_get_earliest_valid_ml_date(self, mock_exists):
        """Test getting earliest valid ML date."""
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as tmp:
            config_path = Path(tmp.name)

        validator = DateValidator(config_path=config_path)
        result = validator.get_earliest_valid_ml_date("CEFI", "24h")

        assert result == "2023-02-15"

    def test_get_earliest_valid_ml_date_not_found(self):
        """Test getting earliest valid ML date when not found."""
        validator = DateValidator()
        validator._config = {}
        validator._loaded = True

        result = validator.get_earliest_valid_ml_date("CEFI")
        assert result is None

    @patch("unified_domain_services.date_validation.FEATURE_GROUP_LOOKBACK", {"group1": 100, "group2": 200})
    @patch("unified_domain_services.date_validation.TIMEFRAME_TO_SECONDS", {"1h": 3600, "24h": 86400})
    def test_calculate_lookback_days_with_feature_groups(self):
        """Test calculating lookback days with specific feature groups."""
        validator = DateValidator()
        result = validator.calculate_lookback_days("1h", ["group1", "group2"])

        # Max lookback is 200, timeframe is 1h (3600s)
        # Total seconds = 200 * 3600 = 720000
        # Days = ceil(720000 / 86400) = 9
        assert result == 9

    @patch("unified_domain_services.date_validation.FEATURE_GROUP_LOOKBACK", {"group1": 100, "group2": 200})
    @patch("unified_domain_services.date_validation.TIMEFRAME_TO_SECONDS", {"24h": 86400})
    def test_calculate_lookback_days_all_groups(self):
        """Test calculating lookback days for all feature groups."""
        validator = DateValidator()
        result = validator.calculate_lookback_days("24h", None)

        # Max lookback is 200, timeframe is 24h (86400s)
        # Total seconds = 200 * 86400 = 17280000
        # Days = ceil(17280000 / 86400) = 200
        assert result == 200

    def test_check_date_valid(self):
        """Test checking a valid date."""
        validator = DateValidator()

        # Mock the method to return an early date
        validator.get_earliest_valid_feature_date = MagicMock(return_value="2023-01-01")

        result = validator.check_date("2023-01-15", "CEFI")

        assert result.is_valid is True
        assert result.reason == "Date is valid for processing"
        assert result.earliest_valid_date == "2023-01-01"
        assert result.requested_date == "2023-01-15"

    def test_check_date_invalid(self):
        """Test checking an invalid date."""
        validator = DateValidator()

        # Mock the method to return a later date
        validator.get_earliest_valid_feature_date = MagicMock(return_value="2023-01-15")

        result = validator.check_date("2023-01-01", "CEFI")

        assert result.is_valid is False
        assert "Date 2023-01-01 is before earliest valid date 2023-01-15" in result.reason
        assert result.earliest_valid_date == "2023-01-15"
        assert result.requested_date == "2023-01-01"
        assert result.days_until_valid == 14

    @patch("unified_domain_services.date_validation.MAX_LOOKBACK_DAYS_BY_TIMEFRAME", {"24h": 30})
    def test_check_date_fallback_to_raw_data(self):
        """Test checking date with fallback to raw data."""
        validator = DateValidator()

        # Mock methods
        validator.get_earliest_valid_feature_date = MagicMock(return_value=None)
        validator.get_earliest_raw_data_date = MagicMock(return_value="2023-01-01")

        result = validator.check_date("2023-02-01", "CEFI")

        # Raw date + 30 days = 2023-01-31, so 2023-02-01 should be valid
        assert result.is_valid is True
        assert result.reason == "Date is valid for processing"

    def test_check_date_no_config(self):
        """Test checking date when no configuration is available."""
        validator = DateValidator()

        # Mock methods to return None
        validator.get_earliest_valid_feature_date = MagicMock(return_value=None)
        validator.get_earliest_raw_data_date = MagicMock(return_value=None)

        result = validator.check_date("2023-01-01", "CEFI")

        assert result.is_valid is True
        assert result.reason == "No start date configured - allowing processing"
        assert result.requested_date == "2023-01-01"


class TestModuleLevelFunctions:
    """Test module-level convenience functions."""

    @patch("unified_domain_services.date_validation._validator", None)
    def test_get_validator_creates_instance(self):
        """Test get_validator creates instance if none exists."""
        validator = get_validator()

        assert validator is not None
        assert isinstance(validator, type(validator))

    def test_get_validator_returns_existing(self):
        """Test get_validator returns existing instance."""
        # Get first instance
        validator1 = get_validator()

        # Get second instance - should be the same
        validator2 = get_validator()

        assert validator1 is validator2

    def test_should_skip_date_true(self):
        """Test should_skip_date returns True for invalid date."""
        with patch("unified_domain_services.date_validation.get_validator") as mock_get_validator:
            mock_validator = MagicMock()
            mock_result = MagicMock()
            mock_result.is_valid = False
            mock_validator.check_date.return_value = mock_result
            mock_get_validator.return_value = mock_validator

            result = should_skip_date("2023-01-01", "CEFI", "BINANCE", "24h")

            assert result is True
            mock_validator.check_date.assert_called_once_with(
                date="2023-01-01", category="CEFI", venue="BINANCE", timeframe="24h"
            )

    def test_should_skip_date_false(self):
        """Test should_skip_date returns False for valid date."""
        with patch("unified_domain_services.date_validation.get_validator") as mock_get_validator:
            mock_validator = MagicMock()
            mock_result = MagicMock()
            mock_result.is_valid = True
            mock_validator.check_date.return_value = mock_result
            mock_get_validator.return_value = mock_validator

            result = should_skip_date("2023-01-01", "CEFI", "BINANCE", "24h")

            assert result is False

    def test_get_earliest_valid_date(self):
        """Test get_earliest_valid_date function."""
        with patch("unified_domain_services.date_validation.get_validator") as mock_get_validator:
            mock_validator = MagicMock()
            mock_validator.get_earliest_valid_feature_date.return_value = "2023-01-15"
            mock_get_validator.return_value = mock_validator

            result = get_earliest_valid_date("CEFI", "BINANCE", "1h")

            assert result == "2023-01-15"
            mock_validator.get_earliest_valid_feature_date.assert_called_once_with(
                category="CEFI", venue="BINANCE", timeframe="1h"
            )

    def test_get_earliest_valid_date_none(self):
        """Test get_earliest_valid_date returns None when not found."""
        with patch("unified_domain_services.date_validation.get_validator") as mock_get_validator:
            mock_validator = MagicMock()
            mock_validator.get_earliest_valid_feature_date.return_value = None
            mock_get_validator.return_value = mock_validator

            result = get_earliest_valid_date("CEFI")

            assert result is None


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_load_config_handles_yaml_error(self):
        """Test that config loading handles YAML errors gracefully."""
        # Test with non-existent path
        validator = DateValidator(config_path=Path("/non/existent/path.yaml"))
        validator._load_config()

        assert validator._loaded is True
        assert validator._config == {}

    def test_check_date_invalid_date_format(self):
        """Test check_date with invalid date format."""
        validator = DateValidator()
        validator.get_earliest_valid_feature_date = MagicMock(return_value="2023-01-01")

        with pytest.raises(ValueError):
            validator.check_date("invalid-date", "CEFI")

    @patch("builtins.open", mock_open(read_data="service: not_a_dict"))
    @patch("pathlib.Path.exists", return_value=True)
    def test_get_earliest_raw_data_date_invalid_service_config(self, mock_exists):
        """Test get_earliest_raw_data_date with non-dict service config."""
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as tmp:
            config_path = Path(tmp.name)

        validator = DateValidator(config_path=config_path)
        result = validator.get_earliest_raw_data_date("service", "CEFI")

        assert result is None
