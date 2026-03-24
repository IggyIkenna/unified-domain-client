"""
Unit tests for unified_domain_client.timestamp_validation module.

Tests TimestampDateValidator, detect_timestamp_column_and_unit,
validate_timestamp_date_alignment.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pandas as pd
import pytest

from unified_domain_client import (
    TimestampAlignmentResult,
    TimestampDateValidator,
    detect_timestamp_column_and_unit,
    validate_timestamp_date_alignment,
    validate_timestamp_utc,
)


class TestTimestampDateValidator:
    """Tests for TimestampDateValidator."""

    def test_init_default(self):
        validator = TimestampDateValidator()
        assert validator.alignment_threshold == 100.0
        assert validator.timestamp_unit == "ns"

    def test_init_custom(self):
        validator = TimestampDateValidator(
            alignment_threshold=90.0,
            timestamp_unit="us",
            allow_next_day_boundary=False,
        )
        assert validator.alignment_threshold == 90.0
        assert validator.timestamp_unit == "us"
        assert validator.allow_next_day_boundary is False

    def test_init_invalid_threshold(self):
        with pytest.raises(ValueError, match="alignment_threshold"):
            TimestampDateValidator(alignment_threshold=150.0)

    def test_init_invalid_unit(self):
        with pytest.raises(ValueError, match="Invalid timestamp_unit"):
            TimestampDateValidator(timestamp_unit="invalid")

    def test_validate_empty_dataframe(self):
        validator = TimestampDateValidator()
        df = pd.DataFrame()
        result = validator.validate(df, date(2024, 1, 1))
        assert result.valid is True
        assert result.total_records == 0

    def test_validate_missing_column(self):
        validator = TimestampDateValidator()
        df = pd.DataFrame({"other_col": [1, 2, 3]})
        result = validator.validate(df, date(2024, 1, 1), timestamp_col="ts_event")
        assert result.valid is False
        assert len(result.errors) > 0

    def test_validate_datetime64_aligned(self):
        validator = TimestampDateValidator()
        expected = date(2024, 1, 15)
        timestamps = pd.to_datetime(
            ["2024-01-15 10:00:00", "2024-01-15 14:00:00", "2024-01-15 18:00:00"],
            utc=True,
        )
        df = pd.DataFrame({"ts_event": timestamps})
        result = validator.validate(df, expected)
        assert result.valid is True
        assert result.alignment_percentage == 100.0

    def test_validate_datetime64_misaligned(self):
        validator = TimestampDateValidator(alignment_threshold=90.0)
        expected = date(2024, 1, 15)
        timestamps = pd.to_datetime(
            ["2024-01-14 10:00:00", "2024-01-14 14:00:00", "2024-01-15 18:00:00"],
            utc=True,
        )
        df = pd.DataFrame({"ts_event": timestamps})
        result = validator.validate(df, expected)
        # Only 1 out of 3 aligned = 33.3%, below 90% threshold
        assert result.valid is False

    def test_validate_integer_timestamps_ns(self):
        validator = TimestampDateValidator(timestamp_unit="ns")
        expected = date(2024, 1, 15)
        # Create ns timestamps for 2024-01-15
        base_ns = int(pd.Timestamp("2024-01-15 12:00:00", tz="UTC").value)
        timestamps = [base_ns, base_ns + 1_000_000_000, base_ns + 2_000_000_000]
        df = pd.DataFrame({"ts_event": timestamps})
        result = validator.validate(df, expected)
        assert result.valid is True

    def test_validate_with_datetime_expected(self):
        validator = TimestampDateValidator()
        expected = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        timestamps = pd.to_datetime(
            ["2024-01-15 10:00:00", "2024-01-15 14:00:00"],
            utc=True,
        )
        df = pd.DataFrame({"ts_event": timestamps})
        result = validator.validate(df, expected)
        assert result.valid is True

    def test_validate_none_dataframe(self):
        validator = TimestampDateValidator()
        result = validator.validate(None, date(2024, 1, 1))
        assert result.valid is True
        assert result.total_records == 0

    def test_validate_next_day_boundary(self):
        """Allow small percentage of next-day timestamps (within 5% boundary)."""
        validator = TimestampDateValidator(
            alignment_threshold=90.0,
            allow_next_day_boundary=True,
        )
        expected = date(2024, 1, 15)
        # Use enough records so 1 next-day record is ≤5% (1/21 ≈ 4.76%)
        timestamps = pd.to_datetime(
            [
                "2024-01-15 10:00:00",
                "2024-01-15 14:00:00",
                "2024-01-15 18:00:00",
                "2024-01-15 23:59:59",
            ]
            + ["2024-01-15 12:00:00"] * 17
            + ["2024-01-16 00:00:01"],  # 1 boundary record out of 22
            utc=True,
        )
        df = pd.DataFrame({"ts_event": timestamps})
        result = validator.validate(df, expected)
        assert result.valid is True


class TestTimestampAlignmentResult:
    """Tests for TimestampAlignmentResult."""

    def test_to_dict(self):
        result = TimestampAlignmentResult(
            valid=True,
            expected_date=date(2024, 1, 15),
            alignment_percentage=100.0,
            total_records=100,
            aligned_records=100,
            misaligned_records=0,
        )
        d = result.to_dict()
        assert d["valid"] is True
        assert d["expected_date"] == "2024-01-15"
        assert d["total_records"] == 100


class TestDetectTimestampColumnAndUnit:
    """Tests for detect_timestamp_column_and_unit function."""

    def test_detect_ts_event(self):
        df = pd.DataFrame({"ts_event": [1_000_000_000_000_000_000]})
        col, unit = detect_timestamp_column_and_unit(df)
        assert col == "ts_event"
        assert unit == "ns"

    def test_detect_ts_init(self):
        df = pd.DataFrame({"ts_init": [1_000_000_000_000_000_000]})
        col, unit = detect_timestamp_column_and_unit(df)
        assert col == "ts_init"
        assert unit == "ns"

    def test_detect_timestamp_ns(self):
        df = pd.DataFrame({"timestamp": [1_700_000_000_000_000_000]})
        col, unit = detect_timestamp_column_and_unit(df)
        assert col == "timestamp"
        assert unit == "ns"

    def test_detect_timestamp_us(self):
        df = pd.DataFrame({"timestamp": [1_700_000_000_000_000]})
        col, unit = detect_timestamp_column_and_unit(df)
        assert col == "timestamp"
        assert unit == "us"

    def test_detect_timestamp_ms(self):
        df = pd.DataFrame({"timestamp": [1_700_000_000_000]})
        col, unit = detect_timestamp_column_and_unit(df)
        assert col == "timestamp"
        assert unit == "ms"

    def test_detect_timestamp_s(self):
        df = pd.DataFrame({"timestamp": [1_700_000_000]})
        col, unit = detect_timestamp_column_and_unit(df)
        assert col == "timestamp"
        assert unit == "s"

    def test_no_timestamp_column(self):
        df = pd.DataFrame({"price": [100.0], "volume": [50]})
        col, unit = detect_timestamp_column_and_unit(df)
        assert col is None
        assert unit is None


class TestValidateTimestampDateAlignment:
    """Tests for validate_timestamp_date_alignment convenience function."""

    def test_auto_detect(self):
        expected = date(2024, 1, 15)
        timestamps = pd.to_datetime(
            ["2024-01-15 10:00:00", "2024-01-15 14:00:00"],
            utc=True,
        )
        df = pd.DataFrame({"ts_event": timestamps})
        result = validate_timestamp_date_alignment(df, expected)
        assert result.valid is True

    def test_no_timestamp_column_found(self):
        df = pd.DataFrame({"price": [100.0]})
        result = validate_timestamp_date_alignment(df, date(2024, 1, 15))
        assert result.valid is False
        assert len(result.errors) > 0


class TestValidateTimestampUtc:
    """Tests for the scalar validate_timestamp_utc function."""

    def test_valid_utc_datetime_passes(self) -> None:
        ts = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        validate_timestamp_utc(ts)  # must not raise

    def test_naive_datetime_raises(self) -> None:
        ts = datetime(2024, 1, 15, 12, 0, 0)  # no tzinfo
        with pytest.raises(ValueError, match="UTC"):
            validate_timestamp_utc(ts)

    def test_non_utc_timezone_raises(self) -> None:
        import zoneinfo

        ts = datetime(2024, 1, 15, 12, 0, 0, tzinfo=zoneinfo.ZoneInfo("US/Eastern"))
        with pytest.raises(ValueError, match="UTC"):
            validate_timestamp_utc(ts)

    def test_date_only_midnight_passes(self) -> None:
        ts = datetime(2024, 1, 15, 0, 0, 0, tzinfo=UTC)
        validate_timestamp_utc(ts, date_only=True)  # must not raise

    def test_date_only_non_midnight_raises(self) -> None:
        ts = datetime(2024, 1, 15, 12, 30, 0, tzinfo=UTC)
        with pytest.raises(ValueError, match="midnight UTC"):
            validate_timestamp_utc(ts, date_only=True)

    def test_date_only_false_allows_non_midnight(self) -> None:
        ts = datetime(2024, 1, 15, 12, 30, 45, tzinfo=UTC)
        validate_timestamp_utc(ts, date_only=False)  # must not raise


class TestTimestampValidationEdgeCases:
    """Additional edge cases from coverage boost merge."""

    def test_validate_timestamp_date_alignment_with_path(self):
        import pandas as pd

        df = pd.DataFrame({"timestamp": [1672531200000000000]})
        result = validate_timestamp_date_alignment(
            df, path="gs://bucket/day=2023-01-01/data.parquet"
        )
        assert result is not None

    def test_validate_timestamp_date_alignment_string_path_as_expected(self):
        import pandas as pd

        df = pd.DataFrame({"timestamp": [1672531200000000000]})
        result = validate_timestamp_date_alignment(
            df, expected_date="gs://bucket/day=2023-01-01/data"
        )
        assert result is not None

    def test_validate_timestamp_date_alignment_invalid_string_date(self):
        import pandas as pd

        df = pd.DataFrame({"timestamp": [1672531200000000000]})
        result = validate_timestamp_date_alignment(df, expected_date="not-a-date")
        assert result.valid is True

    def test_validate_timestamp_date_alignment_none_df(self):
        result = validate_timestamp_date_alignment(None)
        assert result.valid is True

    def test_validate_timestamp_date_alignment_no_date_no_path(self):
        import pandas as pd

        df = pd.DataFrame({"timestamp": [1672531200000000000]})
        result = validate_timestamp_date_alignment(df)
        assert result.valid is True

    def test_detect_timestamp_column_and_unit_no_cols(self):
        import pandas as pd

        df = pd.DataFrame({"price": [100.0]})
        col, unit = detect_timestamp_column_and_unit(df)
        assert col is None
        assert unit is None

    def test_detect_ts_event_ns(self):
        import pandas as pd

        df = pd.DataFrame({"ts_event": [1672531200000000000]})
        col, unit = detect_timestamp_column_and_unit(df)
        assert col == "ts_event"
        assert unit == "ns"

    def test_detect_timestamp_ms_range(self):
        import pandas as pd

        df = pd.DataFrame({"timestamp": [1672531200000]})
        col, unit = detect_timestamp_column_and_unit(df)
        assert col == "timestamp"
        assert unit == "ms"

    def test_detect_timestamp_us_range(self):
        import pandas as pd

        df = pd.DataFrame({"timestamp": [1672531200000000]})
        col, unit = detect_timestamp_column_and_unit(df)
        assert col == "timestamp"
        assert unit == "us"

    def test_detect_timestamp_s_range(self):
        import pandas as pd

        df = pd.DataFrame({"timestamp": [1672531200]})
        col, unit = detect_timestamp_column_and_unit(df)
        assert col == "timestamp"
        assert unit == "s"

    def test_detect_timestamp_datetime64(self):
        import pandas as pd

        df = pd.DataFrame({"timestamp": pd.to_datetime(["2023-01-01"])})
        col, unit = detect_timestamp_column_and_unit(df)
        assert col == "timestamp"
        assert unit == "ns"

    def test_detect_timestamp_empty_df_returns_us(self):
        import pandas as pd

        df = pd.DataFrame({"timestamp": pd.Series([], dtype="int64")})
        col, unit = detect_timestamp_column_and_unit(df)
        assert col == "timestamp"
        assert unit == "us"

    def test_timestamp_validator_parse_us(self):
        import pandas as pd

        v = TimestampDateValidator(timestamp_unit="us")
        result = v.validate(
            pd.DataFrame({"timestamp": [1672531200000000]}),
            expected_date=date(2023, 1, 1),
        )
        assert result is not None

    def test_timestamp_validator_parse_ms(self):
        import pandas as pd

        v = TimestampDateValidator(timestamp_unit="ms")
        result = v.validate(
            pd.DataFrame({"timestamp": [1672531200000]}),
            expected_date=date(2023, 1, 1),
        )
        assert result is not None

    def test_timestamp_validator_parse_s(self):
        import pandas as pd

        v = TimestampDateValidator(timestamp_unit="s")
        result = v.validate(
            pd.DataFrame({"timestamp": [1672531200]}),
            expected_date=date(2023, 1, 1),
        )
        assert result is not None

    def test_timestamp_alignment_result_to_dict_with_errors(self):
        result = TimestampAlignmentResult(
            valid=True,
            alignment_percentage=95.0,
            errors=["some error"],
            total_records=100,
            aligned_records=95,
            misaligned_records=5,
            expected_date=date(2023, 1, 1),
            actual_dates_found={"2023-01-01"},
        )
        d = result.to_dict()
        assert d["valid"] is True
        assert d["alignment_percentage"] == 95.0
        assert d["expected_date"] == "2023-01-01"
        assert isinstance(d["actual_dates_found"], list)

    def test_timestamp_alignment_result_to_dict_no_optional(self):
        result = TimestampAlignmentResult(valid=True, alignment_percentage=100.0)
        d = result.to_dict()
        assert d["expected_date"] is None
        assert d["actual_dates_found"] is None
