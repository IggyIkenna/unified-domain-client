"""Timestamp-date alignment validation for GCS writes.

Tier 2 compliance: Local implementation, no unified-trading-library dependency.
"""
# pyright: reportAny=false, reportUnnecessaryIsInstance=false, reportReturnType=false
# pyright: reportUnknownVariableType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false
# pyright: reportInvalidTypeArguments=false, reportAttributeAccessIssue=false
# Reason: df: object accepts DataFrame | polars.DataFrame; getattr/callable for to_pandas() — polymorphic.
# reportUnnecessaryIsInstance: expected_date union narrowing in nested elifs.
# reportUnknown*: pandas Series operations on object-typed columns have incomplete stubs.
# reportReturnType/reportInvalidTypeArguments: pd.to_datetime returns Series[Timestamp] but
# Series[T] is invariant; suppressed since the runtime type is correct.

import logging
from dataclasses import dataclass
from datetime import date as date_type
from typing import cast

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class TimestampAlignmentResult:
    """Result of timestamp-date alignment validation."""

    valid: bool
    alignment_percentage: float
    errors: list[str]
    actual_dates_found: set[str] | None = None


class TimestampDateValidator:
    """Validates that DataFrame timestamps align with expected date."""

    def __init__(
        self,
        alignment_threshold: float = 90.0,
        timestamp_unit: str = "ns",
    ) -> None:
        self.alignment_threshold: float = alignment_threshold
        self.timestamp_unit: str = timestamp_unit

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
        if self.timestamp_unit == "ns":
            return pd.to_datetime(series, unit="ns", utc=True, errors="coerce")
        if self.timestamp_unit == "us":
            return pd.to_datetime(series, unit="us", utc=True, errors="coerce")
        if self.timestamp_unit == "ms":
            return pd.to_datetime(series, unit="ms", utc=True, errors="coerce")
        return pd.to_datetime(series, utc=True, errors="coerce")

    def validate(
        self,
        df: pd.DataFrame,
        expected_date: date_type,
        timestamp_col: str = "timestamp",
    ) -> TimestampAlignmentResult:
        """Validate timestamp-date alignment."""
        if df.empty:
            return TimestampAlignmentResult(valid=True, alignment_percentage=100.0, errors=[])

        col = self._resolve_timestamp_col(df, timestamp_col)
        if col is None:
            return TimestampAlignmentResult(
                valid=False, alignment_percentage=0.0, errors=["No timestamp column found"]
            )

        dt = self._parse_timestamps(df[col])
        dates = dt.dt.date
        expected_str = expected_date.isoformat()
        aligned = (dates.astype(str) == expected_str).sum()
        total = len(dates)
        pct = (aligned / total * 100) if total > 0 else 0.0
        actual_dates = set(dates.dropna().astype(str).unique())

        valid = pct >= self.alignment_threshold
        errors: list[str] = []
        if not valid:
            errors.append(
                f"Only {pct:.1f}% of timestamps align with {expected_str}. Found dates: {actual_dates}"
            )
        return TimestampAlignmentResult(
            valid=valid, alignment_percentage=pct, errors=errors, actual_dates_found=actual_dates
        )


def validate_timestamp_date_alignment(
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
