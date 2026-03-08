"""
Domain Validation Service

Domain-specific validation rules for different data domains in the unified trading system.

Domains:
- market_data: Candle counting, midnight boundaries, UTC alignment
- features: UTC timestamp validation, feature completeness
- strategy: Sparse event timestamp ordering, UTC alignment for orders only
- execution: Sparse event timestamp ordering only (no UTC alignment)
- ml: No domain-specific validation (just data transformation)

Timestamp Semantics:
- External I/O (market tick data, execution order status, fills):
  - local_timestamp: When WE receive data from external source
  - timestamp: Exchange-generated timestamp
- Internal Domain Data (features, strategy, execution outputs):
  - timestamp_in: When data was received (timestamp_out from upstream domain)
  - timestamp_out: When delayed processing completed (timestamp + processing delay)
  - timestamp: Candle-aligned timestamp for synchronization
"""

import logging
from dataclasses import dataclass

import pandas as pd
from unified_trading_library import ValidationResult

logger = logging.getLogger(__name__)

# =============================================================================
# Domain-specific validation flag presets
# =============================================================================

_DOMAIN_CONFIGS: dict[str, dict[str, bool | str | None]] = {
    "market_data": {
        "validate_candle_count": True,
        "validate_midnight_boundaries": True,
        "validate_utc_alignment": True,
        "skip_for_sparse": False,
        "validate_utc_for_type": None,  # All data needs UTC
    },
    "features": {
        "validate_candle_count": False,  # Features are derived, not candles
        "validate_midnight_boundaries": False,
        "validate_utc_alignment": True,
        "skip_for_sparse": False,
        "validate_utc_for_type": None,
    },
    "strategy": {
        "validate_candle_count": False,  # Sparse events
        "validate_midnight_boundaries": False,
        "validate_utc_alignment": False,  # Only orders need UTC, positions/risk don't
        "skip_for_sparse": True,
        "validate_utc_for_type": "orders",  # Only orders need UTC alignment
    },
    "execution": {
        "validate_candle_count": False,  # Sparse events
        "validate_midnight_boundaries": False,
        "validate_utc_alignment": False,  # Execution doesn't need UTC
        "skip_for_sparse": True,
        "validate_utc_for_type": None,
    },
    "ml": {
        "validate_candle_count": False,
        "validate_midnight_boundaries": False,
        "validate_utc_alignment": False,  # No domain-specific validation
        "skip_for_sparse": False,
        "validate_utc_for_type": None,
    },
    "instruments": {
        "validate_candle_count": False,  # Reference data, not time series
        "validate_midnight_boundaries": False,
        "validate_utc_alignment": False,  # Reference data doesn't need UTC alignment
        "skip_for_sparse": False,
        "validate_utc_for_type": None,
    },
}

# Data types classified as external I/O (exchange-sourced)
_EXTERNAL_IO_TYPES = {"trades", "book_snapshot", "liquidations", "derivative_ticker", "fills"}

# Data types classified as internal domain (processed data)
_INTERNAL_DOMAIN_TYPES = {"orders", "positions", "risk", "features", "execution_logs"}


@dataclass
class DomainValidationConfig:
    """Configuration for domain-specific validation"""

    # Market data validation
    enable_candle_count_validation: bool = True
    enable_midnight_boundary_validation: bool = True
    enable_utc_alignment_validation: bool = True

    # Sparse event handling (strategy/execution)
    skip_candle_count_for_sparse: bool = True
    validate_timestamp_ordering: bool = True
    validate_utc_for_orders: bool = True
    validate_utc_for_execution: bool = False  # Execution doesn't need UTC alignment

    # Timestamp semantics
    validate_external_io_timestamps: bool = True  # local_timestamp vs timestamp
    validate_internal_domain_timestamps: bool = True  # timestamp_in vs timestamp_out


class DomainValidationService:
    """Domain-specific validation rules"""

    def __init__(self, domain: str, config: DomainValidationConfig | None = None):
        """
        Initialize domain validation service.

        Args:
            domain: 'market_data', 'features', 'strategy', 'execution', 'ml', 'instruments'
            config: Optional validation configuration
        """
        self.domain = domain
        self.config = config or DomainValidationConfig()
        self.domain_configs = _DOMAIN_CONFIGS

        if domain not in self.domain_configs:
            raise ValueError(
                f"Unknown domain: {domain}. Must be one of: {list(self.domain_configs.keys())}"
            )

        self.domain_config = self.domain_configs[domain]

        logger.info("✅ DomainValidationService initialized: domain=%s", domain)
        logger.info(
            "Config: candle_count=%s, utc_alignment=%s, sparse=%s",
            self.domain_config["validate_candle_count"],
            self.domain_config["validate_utc_alignment"],
            self.domain_config["skip_for_sparse"],
        )

    def validate_for_domain(
        self, data: pd.DataFrame, data_type: str | None = None
    ) -> ValidationResult:
        """
        Apply domain-specific validation rules.

        Args:
            data: DataFrame to validate
            data_type: Optional data type hint (e.g., 'orders', 'positions', 'execution_logs')

        Returns:
            ValidationResult with validation details
        """
        if data.empty:
            return self._build_domain_result(valid=True, total_records=0)

        if self.domain == "ml":
            return self._build_domain_result(valid=True, total_records=len(data))

        errors: list[str] = []
        warnings: list[str] = []

        # Timestamp semantics validation
        self._collect_timestamp_errors(data, data_type, errors, warnings)

        # Domain-specific validation
        self._validate_domain_specific(data, data_type, errors, warnings)

        return self._build_domain_result(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            total_records=len(data),
            data_type=data_type,
        )

    def _collect_timestamp_errors(
        self, data: pd.DataFrame, data_type: str | None, errors: list[str], warnings: list[str]
    ) -> None:
        """Run timestamp semantics validation and collect errors/warnings."""
        if not (
            self.config.validate_external_io_timestamps
            or self.config.validate_internal_domain_timestamps
        ):
            return
        timestamp_result = self.validate_timestamp_semantics(data, data_type)
        if not timestamp_result.valid:
            errors.extend(timestamp_result.errors or [])
        if timestamp_result.warnings:
            warnings.extend(timestamp_result.warnings)

    def _build_domain_result(
        self,
        *,
        valid: bool,
        total_records: int = 0,
        errors: list[str] | None = None,
        warnings: list[str] | None = None,
        data_type: str | None = None,
    ) -> ValidationResult:
        """Build a ValidationResult for domain validation."""
        return ValidationResult(
            validation_type=f"domain_validation_{self.domain}",
            valid=valid,
            errors=errors if errors else [],
            warnings=warnings if warnings else [],
            total_records=total_records,
            valid_records=total_records if valid else 0,
            invalid_records=len(errors) if errors and not valid else 0,
            stats={
                "domain": self.domain,
                "data_type": data_type,
                "row_count": total_records,
                "error_count": len(errors) if errors else 0,
                "warning_count": len(warnings) if warnings else 0,
            },
        )

    def _validate_domain_specific(
        self,
        data: pd.DataFrame,
        data_type: str | None,
        errors: list[str],
        warnings: list[str],
    ) -> None:
        """Apply domain-specific validation rules based on self.domain."""
        if self.domain == "market_data":
            self._validate_market_data_domain(data, errors)
        elif self.domain == "features":
            self._validate_features_domain(data, errors, warnings)
        elif self.domain == "strategy":
            self._validate_strategy_domain(data, data_type, errors)
        elif self.domain == "execution":
            self._validate_execution_domain(data, errors)

    def _validate_market_data_domain(self, data: pd.DataFrame, errors: list[str]) -> None:
        """Validate market_data domain: candle count, midnight boundaries, UTC alignment."""
        if (
            self.domain_config["validate_candle_count"]
            and self.config.enable_candle_count_validation
        ):
            candle_result = self._validate_candle_count(data)
            if not candle_result.valid:
                errors.extend(candle_result.errors or [])

        if (
            self.domain_config["validate_midnight_boundaries"]
            and self.config.enable_midnight_boundary_validation
        ):
            midnight_result = self._validate_midnight_boundaries(data)
            if not midnight_result.valid:
                errors.extend(midnight_result.errors or [])

        if (
            self.domain_config["validate_utc_alignment"]
            and self.config.enable_utc_alignment_validation
        ):
            utc_result = self._validate_utc_alignment(data)
            if not utc_result.valid:
                errors.extend(utc_result.errors or [])

    def _validate_features_domain(
        self, data: pd.DataFrame, errors: list[str], warnings: list[str]
    ) -> None:
        """Validate features domain: UTC alignment and feature completeness."""
        if (
            self.domain_config["validate_utc_alignment"]
            and self.config.enable_utc_alignment_validation
        ):
            utc_result = self._validate_utc_alignment(data)
            if not utc_result.valid:
                errors.extend(utc_result.errors or [])

        feature_result = self._validate_feature_completeness(data)
        if not feature_result.valid:
            warnings.extend(feature_result.warnings or [])

    def _validate_strategy_domain(
        self, data: pd.DataFrame, data_type: str | None, errors: list[str]
    ) -> None:
        """Validate strategy domain: timestamp ordering and UTC for orders."""
        if self.domain_config["skip_for_sparse"] and self.config.validate_timestamp_ordering:
            ordering_result = self._validate_timestamp_ordering(data)
            if not ordering_result.valid:
                errors.extend(ordering_result.errors or [])

        if data_type == "orders" and self.domain_config["validate_utc_for_type"] == "orders":
            if self.config.validate_utc_for_orders:
                utc_result = self._validate_utc_alignment(data, column="timestamp_out")
                if not utc_result.valid:
                    errors.extend(utc_result.errors or [])

    def _validate_execution_domain(self, data: pd.DataFrame, errors: list[str]) -> None:
        """Validate execution domain: timestamp ordering only."""
        if self.domain_config["skip_for_sparse"] and self.config.validate_timestamp_ordering:
            ordering_result = self._validate_timestamp_ordering(data)
            if not ordering_result.valid:
                errors.extend(ordering_result.errors or [])

    def validate_timestamp_semantics(
        self, data: pd.DataFrame, data_type: str | None = None
    ) -> ValidationResult:
        """
        Validate timestamp semantics based on data type.

        External I/O: local_timestamp (when received) vs timestamp (exchange time)
        Internal Domain: timestamp_in (when read) vs timestamp_out (when output)

        Args:
            data: DataFrame to validate
            data_type: Optional hint ('trades', 'book_snapshot', 'orders', 'execution_logs', etc.)

        Returns:
            ValidationResult
        """
        errors: list[str] = []
        warnings: list[str] = []

        if data_type in _EXTERNAL_IO_TYPES:
            self._validate_external_io_timestamps(data, errors, warnings)
        elif data_type in _INTERNAL_DOMAIN_TYPES:
            self._validate_internal_domain_timestamps(data, errors, warnings)

        passed = len(errors) == 0

        return ValidationResult(
            validation_type="timestamp_semantics_validation",
            valid=passed,
            errors=errors if errors else [],
            warnings=warnings if warnings else [],
            total_records=len(data) if hasattr(data, "__len__") else 0,
        )

    def _validate_external_io_timestamps(
        self, data: pd.DataFrame, errors: list[str], warnings: list[str]
    ) -> None:
        """Validate external I/O timestamp semantics (local_timestamp vs timestamp)."""
        if "local_timestamp" not in data.columns:
            errors.append("Missing 'local_timestamp' column for external I/O data")

        if "timestamp" not in data.columns:
            errors.append("Missing 'timestamp' column for external I/O data")

        if "local_timestamp" in data.columns and "timestamp" in data.columns:
            invalid_mask = pd.to_datetime(
                data["local_timestamp"], unit="us", errors="coerce", utc=True
            ) < pd.to_datetime(data["timestamp"], unit="us", errors="coerce", utc=True)
            invalid_count = invalid_mask.sum()

            if invalid_count > 0:
                warnings.append(
                    f"{invalid_count} rows where local_timestamp < timestamp "
                    f"(expected: local_timestamp >= timestamp for external I/O)"
                )

    def _validate_internal_domain_timestamps(
        self, data: pd.DataFrame, errors: list[str], warnings: list[str]
    ) -> None:
        """Validate internal domain timestamp semantics (timestamp_in, timestamp_out, timestamp)."""
        if "timestamp_in" not in data.columns:
            warnings.append(
                "Missing 'timestamp_in' column for internal domain data (optional, indicates when data was received)"  # noqa: E501
            )

        if "timestamp_out" not in data.columns:
            errors.append(
                "Missing 'timestamp_out' column for internal domain data "
                "(required, indicates when processing completed)"
            )

        if "timestamp" not in data.columns:
            errors.append(
                "Missing 'timestamp' column for internal domain data (required, candle-aligned timestamp)"  # noqa: E501
            )

        if all(col in data.columns for col in ["timestamp", "timestamp_out", "timestamp_in"]):
            self._validate_internal_timestamp_ordering(data, warnings)

    def _validate_internal_timestamp_ordering(
        self, data: pd.DataFrame, warnings: list[str]
    ) -> None:
        """Validate internal domain timestamp ordering relationships."""
        timestamp_dt = pd.to_datetime(data["timestamp"], unit="us", errors="coerce", utc=True)
        timestamp_out_dt = pd.to_datetime(
            data["timestamp_out"], unit="us", errors="coerce", utc=True
        )

        invalid_mask = timestamp_out_dt < timestamp_dt
        invalid_count = invalid_mask.sum()

        if invalid_count > 0:
            warnings.append(
                f"{invalid_count} rows where timestamp_out < timestamp (expected: timestamp_out >= timestamp)"  # noqa: E501
            )

        timestamp_in_dt = pd.to_datetime(data["timestamp_in"], unit="us", errors="coerce", utc=True)
        invalid_mask = timestamp_in_dt > timestamp_out_dt
        invalid_count = invalid_mask.sum()

        if invalid_count > 0:
            warnings.append(
                f"{invalid_count} rows where timestamp_in > timestamp_out (expected: timestamp_in <= timestamp_out)"  # noqa: E501
            )

    def validate_bigquery_upload(
        self, data: pd.DataFrame, table_name: str, data_type: str | None = None
    ) -> ValidationResult:
        """
        Validate data before BigQuery upload.

        Convenience method that calls validate_for_domain.

        Args:
            data: DataFrame to validate
            table_name: BigQuery table name (for context)
            data_type: Optional data type hint

        Returns:
            ValidationResult
        """
        return self.validate_for_domain(data, data_type)

    def _validate_candle_count(self, data: pd.DataFrame) -> ValidationResult:
        """Validate candle count for market data domain"""
        if data.empty:
            return ValidationResult(
                validation_type="candle_count_validation",
                valid=False,
                errors=["Empty DataFrame for candle count validation"],
            )

        return ValidationResult(
            validation_type="candle_count_validation",
            valid=True,
            total_records=len(data),
        )

    def _validate_midnight_boundaries(self, data: pd.DataFrame) -> ValidationResult:
        """Validate midnight boundary candles are present"""
        if "timestamp" not in data.columns:
            return ValidationResult(
                validation_type="midnight_boundary_validation",
                valid=False,
                errors=["Missing 'timestamp' column for midnight boundary validation"],
            )

        timestamps = pd.to_datetime(data["timestamp"], unit="us", errors="coerce", utc=True)
        midnight_time = pd.Timestamp("00:00:00").time()
        midnight_mask = timestamps.dt.time == midnight_time
        midnight_count = midnight_mask.sum()

        if midnight_count == 0:
            return ValidationResult(
                validation_type="midnight_boundary_validation",
                valid=False,
                errors=["No midnight boundary candles found (expected at least one)"],
            )

        return ValidationResult(
            validation_type="midnight_boundary_validation",
            valid=True,
            total_records=len(data),
            stats={"midnight_candles": midnight_count},
        )

    def _validate_utc_alignment(
        self, data: pd.DataFrame, column: str = "timestamp"
    ) -> ValidationResult:
        """Validate UTC alignment of timestamps"""
        if column not in data.columns:
            return ValidationResult(
                validation_type="utc_alignment_validation",
                valid=False,
                errors=[f"Missing '{column}' column for UTC alignment validation"],
            )

        timestamps = pd.to_datetime(data[column], unit="us", errors="coerce", utc=True)

        if timestamps.isna().any():
            return ValidationResult(
                validation_type="utc_alignment_validation",
                valid=False,
                errors=["Some timestamps are invalid (NaT)"],
            )

        return ValidationResult(
            validation_type="utc_alignment_validation",
            valid=True,
            total_records=len(data),
        )

    def _validate_timestamp_ordering(self, data: pd.DataFrame) -> ValidationResult:
        """Validate timestamp ordering for sparse events (strategy/execution)"""
        timestamp_cols = ["timestamp", "timestamp_out"]

        timestamp_col = None
        for col in timestamp_cols:
            if col in data.columns:
                timestamp_col = col
                break

        if timestamp_col is None:
            return ValidationResult(
                validation_type="timestamp_ordering_validation",
                valid=False,
                errors=[f"Missing timestamp columns: {timestamp_cols}"],
            )

        timestamps = pd.to_datetime(data[timestamp_col], unit="us", errors="coerce", utc=True)
        is_sorted = timestamps.is_monotonic_increasing

        if not is_sorted:
            diffs = timestamps.diff()
            out_of_order_count = (diffs < pd.Timedelta(0)).sum()

            return ValidationResult(
                validation_type="timestamp_ordering_validation",
                valid=False,
                errors=[
                    f"{out_of_order_count} out-of-order timestamps found (timestamps must be monotonically increasing)"  # noqa: E501
                ],
            )

        return ValidationResult(
            validation_type="timestamp_ordering_validation",
            valid=True,
            total_records=len(data),
        )

    def _validate_feature_completeness(self, data: pd.DataFrame) -> ValidationResult:
        """Validate feature completeness for features domain"""
        warnings: list[str] = []

        metadata_cols = [
            "timestamp",
            "timestamp_out",
            "timestamp_in",
            "venue",
            "symbol",
            "instrument_id",
        ]
        feature_cols = [col for col in data.columns if col not in metadata_cols]

        if not feature_cols:
            warnings.append("No feature columns found")
            return ValidationResult(
                validation_type="feature_completeness_validation",
                valid=True,
                warnings=warnings,
            )

        nan_counts = data[feature_cols].isna().sum()
        high_nan_cols = nan_counts[nan_counts > len(data) * 0.1]  # More than 10% NaN

        if len(high_nan_cols) > 0:
            warnings.append(
                f"{len(high_nan_cols)} feature columns with >10% NaN values: {list(high_nan_cols.index)}"  # noqa: E501
            )

        return ValidationResult(
            validation_type="feature_completeness_validation",
            valid=True,
            warnings=warnings if warnings else [],
            total_records=len(data),
            stats={
                "feature_count": len(feature_cols),
                "high_nan_count": len(high_nan_cols),
            },
        )
