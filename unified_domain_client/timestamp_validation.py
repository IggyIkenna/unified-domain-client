"""Timestamp-date alignment validation for GCS writes."""
# pyright: reportAny=false, reportUnnecessaryIsInstance=false, reportReturnType=false
# pyright: reportUnknownVariableType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false
# pyright: reportInvalidTypeArguments=false, reportAttributeAccessIssue=false
# Reason: df: object accepts DataFrame | polars.DataFrame; getattr/callable
# for to_pandas() — polymorphic.
# reportUnnecessaryIsInstance: expected_date union narrowing in nested elifs.
# reportUnknown*: pandas Series operations on object-typed columns have incomplete stubs.
# reportReturnType/reportInvalidTypeArguments: pd.to_datetime returns Series[Timestamp] but
# Series[T] is invariant; suppressed since the runtime type is correct.

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from datetime import date as date_type
from typing import cast

import pandas as pd

logger = logging.getLogger(__name__)


_VALID_UNITS = {"ns", "us", "ms", "s", "auto"}


@dataclass
class TimestampAlignmentResult:
    """Result of timestamp-date alignment validation."""

    valid: bool
    alignment_percentage: float
    errors: list[str] = None  # type: ignore[assignment]
    total_records: int = 0
    aligned_records: int = 0
    misaligned_records: int = 0
    expected_date: date_type | None = None
    actual_dates_found: set[str] | None = None

    def __post_init__(self) -> None:
        if self.errors is None:
            self.errors = []

    def to_dict(self) -> dict[str, object]:
        """Convert result to a serializable dictionary."""
        return {
            "valid": self.valid,
            "alignment_percentage": self.alignment_percentage,
            "errors": self.errors,
            "total_records": self.total_records,
            "aligned_records": self.aligned_records,
            "misaligned_records": self.misaligned_records,
            "expected_date": self.expected_date.isoformat() if self.expected_date else None,
            "actual_dates_found": list(self.actual_dates_found)
            if self.actual_dates_found
            else None,
        }


class TimestampDateValidator:
    """Validates that DataFrame timestamps align with expected date."""

    def __init__(
        self,
        alignment_threshold: float = 100.0,
        timestamp_unit: str = "ns",
        allow_next_day_boundary: bool = True,
    ) -> None:
        if not (0.0 <= alignment_threshold <= 100.0):
            raise ValueError(f"alignment_threshold must be 0-100, got {alignment_threshold}")
        if timestamp_unit not in _VALID_UNITS:
            raise ValueError(
                f"Invalid timestamp_unit '{timestamp_unit}'. Must be one of: {_VALID_UNITS}"
            )
        self.alignment_threshold: float = alignment_threshold
        self.timestamp_unit: str = timestamp_unit
        self.allow_next_day_boundary: bool = allow_next_day_boundary

    def _resolve_timestamp_col(self, df: pd.DataFrame, requested: str) -> str | None:
        """Return the effective timestamp column name, or None if not found."""
        if requested in df.columns:
            return requested
        for alt in ["timestamp", "ts_event", "ts_init", "time", "datetime"]:
            if alt in df.columns:
                return alt
        return None

    def _parse_timestamps(self, series: "pd.Series[object]") -> "pd.Series[object]":
        """Convert a numeric/string timestamp series to UTC datetime series."""
        if pd.api.types.is_datetime64_any_dtype(series):
            return pd.to_datetime(series, utc=True, errors="coerce")
        if self.timestamp_unit == "ns":
            return pd.to_datetime(series, unit="ns", utc=True, errors="coerce")
        if self.timestamp_unit == "us":
            return pd.to_datetime(series, unit="us", utc=True, errors="coerce")
        if self.timestamp_unit == "ms":
            return pd.to_datetime(series, unit="ms", utc=True, errors="coerce")
        return pd.to_datetime(series, utc=True, errors="coerce")

    @staticmethod
    def _ok_result(exp_date: date_type, total: int = 0) -> TimestampAlignmentResult:
        return TimestampAlignmentResult(
            valid=True,
            alignment_percentage=100.0,
            errors=[],
            total_records=total,
            expected_date=exp_date,
        )

    @staticmethod
    def _no_col_result(exp_date: date_type, total: int) -> TimestampAlignmentResult:
        return TimestampAlignmentResult(
            valid=False,
            alignment_percentage=0.0,
            errors=["No timestamp column found"],
            total_records=total,
            expected_date=exp_date,
        )

    def validate(
        self,
        df: pd.DataFrame | None,
        expected_date: date_type | datetime,
        timestamp_col: str = "timestamp",
    ) -> TimestampAlignmentResult:
        """Validate timestamp-date alignment."""
        exp_date = expected_date.date() if isinstance(expected_date, datetime) else expected_date
        if df is None or (isinstance(df, pd.DataFrame) and df.empty):
            return self._ok_result(exp_date)
        col = self._resolve_timestamp_col(df, timestamp_col)
        if col is None:
            return self._no_col_result(exp_date, len(df))
        dt = self._parse_timestamps(df[col])
        dates = dt.dt.date
        expected_str = exp_date.isoformat()
        aligned = int((dates.astype(str) == expected_str).sum())
        total = len(dates)
        pct = float(aligned / total * 100) if total > 0 else 0.0
        actual_dates = set(dates.dropna().astype(str).unique())
        valid = bool(pct >= self.alignment_threshold)
        errors: list[str] = []
        if not valid:
            errors.append(
                f"Only {pct:.1f}% of timestamps align with {expected_str}. Found: {actual_dates}"
            )
        return TimestampAlignmentResult(
            valid=valid,
            alignment_percentage=pct,
            errors=errors,
            total_records=total,
            aligned_records=aligned,
            misaligned_records=total - aligned,
            expected_date=exp_date,
            actual_dates_found=actual_dates,
        )


def validate_timestamp_date_alignment(  # noqa: C901
    df: pd.DataFrame | object,
    expected_date: date_type | str | None = None,
    timestamp_col: str = "auto",
    alignment_threshold: float = 90.0,
    timestamp_unit: str = "ns",
    date: date_type | None = None,
    path: str | None = None,
) -> TimestampAlignmentResult:
    """Convenience function for timestamp-date alignment validation.

    Accepts (df, path) for path-based date extraction, or (df, expected_date=...).
    """
    if df is None:
        return TimestampAlignmentResult(valid=True, alignment_percentage=100.0, errors=[])
    if isinstance(df, pd.DataFrame) and df.empty:
        return TimestampAlignmentResult(valid=True, alignment_percentage=100.0, errors=[])

    exp_date: date_type | None = date
    path_to_use = path
    if expected_date is not None:
        if isinstance(expected_date, date_type):
            exp_date = expected_date
        elif isinstance(expected_date, str) and "/" in expected_date:
            path_to_use = expected_date
        elif isinstance(expected_date, str):
            try:
                exp_date = date_type.fromisoformat(expected_date.split("T")[0])
            except ValueError as e:
                logger.debug(
                    "Suppressed %s during validate timestamp date alignment: %s",
                    type(e).__name__,
                    e,
                )
                pass
    if exp_date is None and path_to_use:
        try:
            parts = path_to_use.split("/")
            for p in parts:
                if "day=" in p:
                    exp_date = date_type.fromisoformat(p.split("=")[1])
                    break
        except (ValueError, IndexError) as e:
            logger.debug("Suppressed %s during operation: %s", type(e).__name__, e)
            pass

    if exp_date is None:
        return TimestampAlignmentResult(
            valid=True,
            alignment_percentage=100.0,
            errors=[],
        )

    col = "timestamp" if timestamp_col == "auto" else timestamp_col
    validator = TimestampDateValidator(
        alignment_threshold=alignment_threshold,
        timestamp_unit=timestamp_unit,
    )
    assert exp_date is not None
    df_pd: pd.DataFrame
    if isinstance(df, pd.DataFrame):
        df_pd = df
    elif hasattr(df, "to_pandas") and callable(getattr(df, "to_pandas", None)):
        df_pd = cast(pd.DataFrame, df.to_pandas())  # pyright: ignore[reportAttributeAccessIssue,reportUnknownMemberType]
    else:
        df_pd = pd.DataFrame()
    return validator.validate(df_pd, expected_date=exp_date, timestamp_col=col)


def validate_timestamp_utc(ts: datetime, *, date_only: bool = False) -> None:
    """Validate that a single datetime object is timezone-aware UTC.

    This is the scalar counterpart to validate_timestamp_date_alignment() for use
    at ingestion boundaries where individual timestamps arrive (e.g. API responses,
    message payloads) rather than DataFrames.

    Args:
        ts: The datetime to validate.
        date_only: If True, also assert the time component is midnight (00:00:00).
                   Use this for date-partitioned GCS paths where timestamps must
                   represent the start of a UTC day.

    Raises:
        ValueError: If the timestamp is naive or not UTC.
        ValueError: If date_only=True and the time is not midnight UTC.
    """
    if ts.tzinfo is None or ts.tzinfo.utcoffset(ts) != UTC.utcoffset(ts):
        raise ValueError(f"Timestamp must be UTC, got tzinfo={ts.tzinfo}")
    if date_only and (ts.hour != 0 or ts.minute != 0 or ts.second != 0):
        raise ValueError(f"Expected date-aligned timestamp (midnight UTC), got {ts}")


def detect_timestamp_column_and_unit(df: pd.DataFrame) -> tuple[str | None, str | None]:  # noqa: C901
    """Auto-detect the timestamp column and unit from a DataFrame.

    Detection priority:
    1. ts_event (NautilusTrader format) - nanoseconds
    2. timestamp - auto-detect unit based on magnitude
    3. ts_init - nanoseconds (fallback)
    4. timestamp_out - nanoseconds (features format)

    Returns:
        tuple: (column_name, unit) e.g., ("ts_event", "ns") or ("timestamp", "us")
    """
    candidates: list[tuple[str, str | None]] = [
        ("ts_event", "ns"),
        ("ts_init", "ns"),
        ("timestamp_out", "ns"),
        ("timestamp", None),
    ]

    for col, default_unit in candidates:
        if col in df.columns:
            if default_unit is not None:
                return col, default_unit

            if len(df) > 0:
                sample: object = cast(object, df[col].iloc[0])
                if pd.api.types.is_integer_dtype(df[col]):
                    try:
                        sample_num: float = float(cast(float, sample))
                    except (TypeError, ValueError):
                        sample_num = 0.0
                    if sample_num > 1e18:
                        return col, "ns"
                    elif sample_num > 1e15:
                        return col, "us"
                    elif sample_num > 1e12:
                        return col, "ms"
                    else:
                        return col, "s"
                elif pd.api.types.is_datetime64_any_dtype(df[col]):
                    return col, "ns"

            return col, "us"

    return None, None
