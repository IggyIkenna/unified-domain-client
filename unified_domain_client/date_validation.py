"""
Date Validation Utilities for Expected Start Dates

This module provides utilities for checking if a date has sufficient data
for processing, based on the expected_start_dates.yaml configuration.

Philosophy:
- Services should SKIP dates with insufficient data rather than FAIL
- This enables clean backfill operations without artificial failures
- Provides clear data availability timeline for downstream services

Usage: import DateValidator and should_skip_date from unified_domain_client (top-level).

Quick check::

    if should_skip_date("2023-05-23", "CEFI", "BINANCE-FUTURES", "24h"):
        logger.info("Skipping date - insufficient lookback")

Full validator::

    validator = DateValidator()
    result = validator.check_date("2023-05-23", "CEFI", "BINANCE-FUTURES", "24h")
    if not result.is_valid:
        logger.info("Skipping: %s", result.reason)
"""
# pyright: reportAny=false, reportUnknownVariableType=false, reportUnknownMemberType=false, reportExplicitAny=false
# Reason: YAML config has dynamic nested structure; dict.get() returns cause type inference issues.
# Documented in QUALITY_GATE_BYPASS_AUDIT.md.

import logging
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TypedDict

import yaml

from .lookback_constants import (
    FEATURE_GROUP_LOOKBACK,
    MAX_LOOKBACK_DAYS_BY_TIMEFRAME,
    TIMEFRAME_TO_SECONDS,
)


class DateConfig(TypedDict, total=False):
    """Type for date configuration parsed from YAML."""

    earliest_valid_date: str


logger = logging.getLogger(__name__)


@dataclass
class DateValidationResult:
    """Result of date validation check."""

    is_valid: bool
    reason: str
    earliest_valid_date: str | None = None
    requested_date: str | None = None
    days_until_valid: int | None = None


class DateValidator:
    """
    Validator for checking if dates have sufficient data for processing.

    Uses expected_start_dates.yaml to determine earliest valid dates
    for each category/venue/timeframe combination.
    """

    def __init__(self, config_path: Path | None = None):
        """
        Initialize the validator.

        Args:
            config_path: Path to expected_start_dates.yaml (auto-detected if None)
        """
        self._config: dict[str, object] = {}
        self._config_path = config_path
        self._loaded = False

    def _load_config(self):
        """Lazy load the config file."""
        if self._loaded:
            return

        # Common locations to search for expected_start_dates.yaml
        possible_paths = [
            Path(__file__).parent.parent.parent.parent.parent
            / "unified-trading-deployment-v2/configs/expected_start_dates.yaml",
            Path("/app/configs/expected_start_dates.yaml"),
            Path.cwd() / "configs/expected_start_dates.yaml",
        ]

        if self._config_path is None:
            for path in possible_paths:
                if path.exists():
                    self._config_path = path
                    break

        if self._config_path and self._config_path.exists():
            with open(self._config_path) as f:
                self._config = yaml.safe_load(f) or {}
            logger.debug("Loaded date validation config from %s", self._config_path)
        else:
            logger.error(
                "Could not find expected_start_dates.yaml - searched paths: %s",
                [str(p) for p in possible_paths],
            )
            self._config = {}

        self._loaded = True

    def get_earliest_raw_data_date(
        self,
        service: str,
        category: str,
        venue: str | None = None,
    ) -> str | None:
        """
        Get earliest raw data date for a service/category/venue.

        Args:
            service: Service name (e.g., "market-tick-data-handler")
            category: Category (CEFI, TRADFI, DEFI)
            venue: Optional venue name

        Returns:
            Date string (YYYY-MM-DD) or None if not found
        """
        self._load_config()

        raw_service = self._config.get(service)
        service_config: dict[str, object] = raw_service if isinstance(raw_service, dict) else {}
        raw_category = service_config.get(category)
        category_config: dict[str, object] = raw_category if isinstance(raw_category, dict) else {}

        if venue and "venues" in category_config:
            venues_map = category_config["venues"]
            if isinstance(venues_map, dict):
                venue_val = venues_map.get(venue)
                return str(venue_val) if isinstance(venue_val, str) else None
        raw_start = category_config.get("category_start")
        return str(raw_start) if isinstance(raw_start, str) else None

    def get_earliest_valid_feature_date(
        self,
        category: str,
        venue: str | None = None,
        timeframe: str = "24h",
    ) -> str | None:
        """
        Get earliest date when features can be computed.

        Args:
            category: Category (CEFI, TRADFI, DEFI)
            venue: Optional venue name (uses per-venue dates if available)
            timeframe: Timeframe string (15s, 1m, 1h, 24h, etc.)

        Returns:
            Date string (YYYY-MM-DD) or None if not found
        """
        self._load_config()

        # Try per-venue dates first
        raw_features = self._config.get("features-delta-one-service")
        features_config: dict[str, object] = raw_features if isinstance(raw_features, dict) else {}
        raw_category = features_config.get(category)
        category_config: dict[str, object] = raw_category if isinstance(raw_category, dict) else {}

        timeframe_key = f"venues_{timeframe}"
        if venue and timeframe_key in category_config:
            timeframe_venues = category_config[timeframe_key]
            if isinstance(timeframe_venues, dict):
                venue_date = timeframe_venues.get(venue)
                if venue_date:
                    return str(venue_date) if isinstance(venue_date, str) else None

        # Fall back to category-level computed dates
        raw_earliest = self._config.get("earliest_valid_features")
        earliest_features: dict[str, object] = (
            raw_earliest if isinstance(raw_earliest, dict) else {}
        )
        raw_timeframe = earliest_features.get(timeframe)
        timeframe_dates: dict[str, object] = (
            raw_timeframe if isinstance(raw_timeframe, dict) else {}
        )
        raw_val = timeframe_dates.get(category)
        return str(raw_val) if isinstance(raw_val, str) else None

    def get_earliest_valid_ml_date(
        self,
        category: str,
        timeframe: str = "24h",
    ) -> str | None:
        """
        Get earliest date when ML models can be trained.

        Args:
            category: Category (CEFI, TRADFI, DEFI)
            timeframe: Timeframe string (15s, 1m, 1h, 24h, etc.)

        Returns:
            Date string (YYYY-MM-DD) or None if not found
        """
        self._load_config()

        raw_ml = self._config.get("ml-training-service")
        ml_config: dict[str, object] = raw_ml if isinstance(raw_ml, dict) else {}
        raw_earliest_ml = ml_config.get("earliest_valid_ml")
        earliest_ml: dict[str, object] = (
            raw_earliest_ml if isinstance(raw_earliest_ml, dict) else {}
        )
        raw_timeframe = earliest_ml.get(timeframe)
        timeframe_dates: dict[str, object] = (
            raw_timeframe if isinstance(raw_timeframe, dict) else {}
        )
        raw_val = timeframe_dates.get(category)
        return str(raw_val) if isinstance(raw_val, str) else None

    def calculate_lookback_days(
        self,
        timeframe: str,
        feature_groups: list[str] | None = None,
    ) -> int:
        """
        Calculate required lookback days for given timeframe and feature groups.

        Args:
            timeframe: Timeframe string (15s, 1m, 1h, 24h, etc.)
            feature_groups: List of feature groups (uses max of all if None)

        Returns:
            Number of days needed for lookback
        """
        if feature_groups:
            max_lookback = max(FEATURE_GROUP_LOOKBACK.get(g, 0) for g in feature_groups)
        else:
            max_lookback = max(FEATURE_GROUP_LOOKBACK.values())

        seconds_per_period = TIMEFRAME_TO_SECONDS.get(timeframe, 86400)
        total_seconds = max_lookback * seconds_per_period

        # Round up to days
        return math.ceil(total_seconds / 86400)

    def _resolve_earliest_date(
        self,
        category: str,
        venue: str | None,
        timeframe: str,
    ) -> str | None:
        """Resolve earliest valid date from config or raw data fallback. Returns None if unconfigured."""  # noqa: E501
        earliest_date = self.get_earliest_valid_feature_date(
            category=category,
            venue=venue,
            timeframe=timeframe,
        )
        if earliest_date is not None:
            return earliest_date

        raw_date = self.get_earliest_raw_data_date(
            service="market-tick-data-handler",
            category=category,
            venue=venue,
        )
        if not raw_date:
            return None

        lookback_days = MAX_LOOKBACK_DAYS_BY_TIMEFRAME.get(timeframe, 200)
        raw_dt = datetime.strptime(raw_date, "%Y-%m-%d").replace(tzinfo=UTC)
        earliest_dt = raw_dt + timedelta(days=lookback_days)
        return earliest_dt.strftime("%Y-%m-%d")

    def _compare_dates(
        self,
        date: str,
        earliest_date: str,
        timeframe: str,
    ) -> DateValidationResult:
        """Compare requested date against earliest valid date and return result."""
        requested_dt = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=UTC)
        earliest_dt = datetime.strptime(earliest_date, "%Y-%m-%d").replace(tzinfo=UTC)

        if requested_dt >= earliest_dt:
            return DateValidationResult(
                is_valid=True,
                reason="Date is valid for processing",
                earliest_valid_date=earliest_date,
                requested_date=date,
            )

        days_until_valid = (earliest_dt - requested_dt).days
        return DateValidationResult(
            is_valid=False,
            reason=f"Date {date} is before earliest valid date {earliest_date} "
            f"(need {days_until_valid} more days of data for {timeframe} lookback)",
            earliest_valid_date=earliest_date,
            requested_date=date,
            days_until_valid=days_until_valid,
        )

    def check_date(
        self,
        date: str,
        category: str,
        venue: str | None = None,
        timeframe: str = "24h",
        service: str = "features-delta-one-service",
    ) -> DateValidationResult:
        """Check if a date is valid for processing.

        Args:
            date: Date to check (YYYY-MM-DD)
            category: Category (CEFI, TRADFI, DEFI)
            venue: Optional venue name
            timeframe: Timeframe string
            service: Service name

        Returns:
            DateValidationResult with validation details
        """
        earliest_date = self._resolve_earliest_date(category, venue, timeframe)
        if earliest_date is None:
            return DateValidationResult(
                is_valid=True,
                reason="No start date configured - allowing processing",
                requested_date=date,
            )
        return self._compare_dates(date, earliest_date, timeframe)


# Module-level instance for convenience
_validator: DateValidator | None = None


def get_validator() -> DateValidator:
    """Get or create module-level validator instance."""
    global _validator
    if _validator is None:
        _validator = DateValidator()
    return _validator


def should_skip_date(
    date: str,
    category: str,
    venue: str | None = None,
    timeframe: str = "24h",
) -> bool:
    """
    Quick check if a date should be skipped due to insufficient lookback.

    Args:
        date: Date to check (YYYY-MM-DD)
        category: Category (CEFI, TRADFI, DEFI)
        venue: Optional venue name
        timeframe: Timeframe string

    Returns:
        True if date should be skipped, False if valid
    """
    result = get_validator().check_date(
        date=date,
        category=category,
        venue=venue,
        timeframe=timeframe,
    )
    return not result.is_valid


def get_earliest_valid_date(
    category: str,
    venue: str | None = None,
    timeframe: str = "24h",
) -> str | None:
    """
    Get earliest valid date for feature processing.

    Args:
        category: Category (CEFI, TRADFI, DEFI)
        venue: Optional venue name
        timeframe: Timeframe string

    Returns:
        Date string (YYYY-MM-DD) or None
    """
    return get_validator().get_earliest_valid_feature_date(
        category=category,
        venue=venue,
        timeframe=timeframe,
    )
