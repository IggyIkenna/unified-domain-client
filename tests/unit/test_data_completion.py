"""
Unit tests for unified_domain_client.data_completion module.

Tests DataCompletionChecker, _extract_date_from_blob, get_missing_dates,
make_completion_checker, get_available_date_range.
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

from unified_domain_client import (
    DataCompletionChecker,
    get_available_date_range,
    make_completion_checker,
)


def _make_checker(bucket: str = "my-bucket", pattern: str = "data/{date}/file.parquet"):
    mock_client = MagicMock()
    with patch(
        "unified_domain_client.data_completion.get_storage_client",
        return_value=mock_client,
    ):
        checker = DataCompletionChecker(bucket=bucket, path_pattern=pattern)
        checker._client = mock_client
    return checker, mock_client


class TestDataCompletion:
    def test_extract_date_from_blob_valid(self):
        checker, _ = _make_checker()
        blob = MagicMock()
        blob.name = "data/2023-01-01/file.parquet"
        start = datetime(2023, 1, 1)
        end = datetime(2023, 12, 31)
        result = checker._extract_date_from_blob(blob, start, end)
        assert result == "2023-01-01"

    def test_extract_date_from_blob_out_of_range(self):
        checker, _ = _make_checker()
        blob = MagicMock()
        blob.name = "data/2020-01-01/file.parquet"
        start = datetime(2023, 1, 1)
        end = datetime(2023, 12, 31)
        result = checker._extract_date_from_blob(blob, start, end)
        assert result is None

    def test_extract_date_from_blob_no_date_in_path(self):
        checker, _ = _make_checker()
        blob = MagicMock()
        blob.name = "data/no-dates-here/file.parquet"
        start = datetime(2023, 1, 1)
        end = datetime(2023, 12, 31)
        result = checker._extract_date_from_blob(blob, start, end)
        assert result is None

    def test_extract_date_from_blob_boundary_dates(self):
        checker, _ = _make_checker()
        blob = MagicMock()
        blob.name = "data/2023-01-01/file.parquet"
        result = checker._extract_date_from_blob(blob, datetime(2023, 1, 1), datetime(2023, 1, 31))
        assert result == "2023-01-01"

    def test_extract_date_from_blob_at_end_boundary(self):
        checker, _ = _make_checker()
        blob = MagicMock()
        blob.name = "data/2023-01-31/file.parquet"
        result = checker._extract_date_from_blob(blob, datetime(2023, 1, 1), datetime(2023, 1, 31))
        assert result == "2023-01-31"

    def test_get_missing_dates(self):
        checker, mock_client = _make_checker()
        blob = MagicMock()
        blob.name = "data/2023-01-02/file.parquet"
        mock_client.bucket.return_value.list_blobs.return_value = [blob]
        missing = checker.get_missing_dates("2023-01-01", "2023-01-03")
        assert "2023-01-01" in missing
        assert "2023-01-03" in missing
        assert "2023-01-02" not in missing

    def test_get_missing_dates_with_instrument(self):
        checker, mock_client = _make_checker(pattern="data/{instrument}/{date}/file.parquet")
        mock_client.bucket.return_value.list_blobs.return_value = []
        missing = checker.get_missing_dates("2023-01-01", "2023-01-02", instrument="BTC-USDT")
        assert len(missing) == 2

    def test_get_missing_dates_all_present(self):
        checker, mock_client = _make_checker()
        blobs = []
        for d in ["2023-01-01", "2023-01-02", "2023-01-03"]:
            b = MagicMock()
            b.name = f"data/{d}/file.parquet"
            blobs.append(b)
        mock_client.bucket.return_value.list_blobs.return_value = blobs
        missing = checker.get_missing_dates("2023-01-01", "2023-01-03")
        assert missing == []

    def test_is_date_complete_true(self):
        checker, mock_client = _make_checker()
        blob = MagicMock()
        blob.exists.return_value = True
        mock_client.bucket.return_value.blob.return_value = blob
        assert checker.is_date_complete("2023-01-15") is True

    def test_is_date_complete_false(self):
        checker, mock_client = _make_checker()
        blob = MagicMock()
        blob.exists.return_value = False
        mock_client.bucket.return_value.blob.return_value = blob
        assert checker.is_date_complete("2023-01-15") is False

    def test_is_date_complete_with_instrument(self):
        checker, mock_client = _make_checker(pattern="data/{instrument}/{date}/file.parquet")
        blob = MagicMock()
        blob.exists.return_value = True
        mock_client.bucket.return_value.blob.return_value = blob
        assert checker.is_date_complete("2023-01-15", instrument="BTC-USDT") is True
        call_args = mock_client.bucket.return_value.blob.call_args
        assert "BTC-USDT" in str(call_args)

    def test_get_completed_dates_with_instrument(self):
        checker, mock_client = _make_checker(pattern="data/{instrument}/{date}/file.parquet")
        blob = MagicMock()
        blob.name = "data/BTC-USDT/2023-01-05/file.parquet"
        mock_client.bucket.return_value.list_blobs.return_value = [blob]
        completed = checker.get_completed_dates("2023-01-01", "2023-01-10", instrument="BTC-USDT")
        assert "2023-01-05" in completed

    def test_get_completed_dates_no_instrument(self):
        checker, mock_client = _make_checker()
        blob = MagicMock()
        blob.name = "data/2023-06-15/file.parquet"
        mock_client.bucket.return_value.list_blobs.return_value = [blob]
        completed = checker.get_completed_dates("2023-06-01", "2023-06-30")
        assert "2023-06-15" in completed


class TestMakeCompletionChecker:
    def test_make_completion_checker_factory(self):
        mock_client = MagicMock()
        with patch(
            "unified_domain_client.data_completion.get_storage_client",
            return_value=mock_client,
        ):
            checker = make_completion_checker(
                bucket="test-bucket",
                path_pattern="data/{date}/file.parquet",
                dataset_name="ignored",
                project_id="ignored",
            )
            assert checker.bucket == "test-bucket"

    def test_make_completion_checker_with_all_args(self):
        mock_client = MagicMock()
        with patch(
            "unified_domain_client.data_completion.get_storage_client",
            return_value=mock_client,
        ):
            checker = make_completion_checker(
                bucket="my-bucket",
                path_pattern="data/{date}/f.parquet",
                dataset_name="my_dataset",
                project_id="my-project",
            )
        assert checker.bucket == "my-bucket"
        assert checker.path_pattern == "data/{date}/f.parquet"


class TestGetAvailableDateRange:
    def test_get_available_date_range_empty(self):
        mock_client = MagicMock()
        mock_client.bucket.return_value.list_blobs.return_value = []
        with patch(
            "unified_domain_client.data_completion.get_storage_client",
            return_value=mock_client,
        ):
            earliest, latest = get_available_date_range("my-bucket", "data/")
            assert earliest is None
            assert latest is None

    def test_get_available_date_range_with_dates(self):
        mock_client = MagicMock()
        blobs = []
        for d in ["2023-03-15", "2023-01-01", "2023-06-30"]:
            b = MagicMock()
            b.name = f"data/{d}/file.parquet"
            blobs.append(b)
        mock_client.bucket.return_value.list_blobs.return_value = blobs
        with patch(
            "unified_domain_client.data_completion.get_storage_client",
            return_value=mock_client,
        ):
            earliest, latest = get_available_date_range("my-bucket", "data/")
            assert earliest == "2023-01-01"
            assert latest == "2023-06-30"

    def test_get_available_date_range_single_date(self):
        mock_client = MagicMock()
        b = MagicMock()
        b.name = "data/2023-06-15/file.parquet"
        mock_client.bucket.return_value.list_blobs.return_value = [b]
        with patch(
            "unified_domain_client.data_completion.get_storage_client",
            return_value=mock_client,
        ):
            earliest, latest = get_available_date_range("my-bucket", "data/")
            assert earliest == "2023-06-15"
            assert latest == "2023-06-15"

    def test_get_available_date_range_blob_no_date_parts(self):
        mock_client = MagicMock()
        b = MagicMock()
        b.name = "data/no-date-here/metadata.json"
        mock_client.bucket.return_value.list_blobs.return_value = [b]
        with patch(
            "unified_domain_client.data_completion.get_storage_client",
            return_value=mock_client,
        ):
            earliest, latest = get_available_date_range("my-bucket", "data/")
            assert earliest is None
            assert latest is None
