"""Data completion checking for cloud storage datasets.

Cloud-agnostic: uses unified_cloud_interface for all storage access.
Takes bucket: str directly — no GCS-specific CloudTarget wrapper.
"""

import logging
from collections.abc import Iterable
from datetime import datetime, timedelta
from typing import cast

from unified_cloud_interface import get_storage_client

logger = logging.getLogger(__name__)


class DataCompletionChecker:
    """Checks cloud storage for which dates have complete data for a given dataset path pattern."""

    def __init__(
        self,
        bucket: str,
        path_pattern: str,
    ) -> None:
        """Initialize checker with bucket name and path pattern.

        Args:
            bucket: Cloud storage bucket name
            path_pattern: Path pattern e.g. "market_data/{instrument}/{date}/data.parquet"
        """
        self.bucket: str = bucket
        self.path_pattern: str = path_pattern
        self._client = get_storage_client()

    def _extract_date_from_blob(
        self,
        blob: object,
        start_dt: datetime,
        end_dt: datetime,
    ) -> str | None:
        """Extract a YYYY-MM-DD date string from a blob path if it falls within range."""
        blob_path: str = str(getattr(blob, "name", ""))
        for part in blob_path.split("/"):
            try:
                date_dt = datetime.strptime(part, "%Y-%m-%d")
                if start_dt <= date_dt <= end_dt:
                    return part
            except ValueError as e:
                logger.debug("Skipping item due to %s: %s", type(e).__name__, e)
        return None

    def get_completed_dates(
        self,
        start_date: str,
        end_date: str,
        instrument: str | None = None,
    ) -> set[str]:
        """Return set of YYYY-MM-DD strings that have data in cloud storage.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            instrument: Optional instrument filter for path pattern

        Returns:
            Set of date strings with complete data
        """
        bucket_obj = self._client.bucket(self.bucket)
        prefix_pattern = self.path_pattern
        if instrument is not None:
            prefix_pattern = prefix_pattern.replace("{instrument}", instrument)
        base_prefix = prefix_pattern.split("{date}")[0]

        blobs_iterator: Iterable[object] = cast(Iterable[object], bucket_obj.list_blobs(prefix=base_prefix))
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        completed_dates: set[str] = set()
        for blob in blobs_iterator:
            date_str = self._extract_date_from_blob(blob, start_dt, end_dt)
            if date_str:
                completed_dates.add(date_str)
        return completed_dates

    def is_date_complete(self, date: str, instrument: str | None = None) -> bool:
        """Check if a specific date has complete data.

        Args:
            date: Date in YYYY-MM-DD format
            instrument: Optional instrument filter

        Returns:
            True if data exists for the date
        """
        bucket_obj = self._client.bucket(self.bucket)
        full_path = self.path_pattern.replace("{date}", date)
        if instrument is not None:
            full_path = full_path.replace("{instrument}", instrument)
        blob = bucket_obj.blob(full_path)
        return bool(blob.exists())

    def get_missing_dates(
        self,
        start_date: str,
        end_date: str,
        instrument: str | None = None,
    ) -> list[str]:
        """Return sorted list of dates missing data.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            instrument: Optional instrument filter

        Returns:
            Sorted list of missing date strings
        """
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        all_dates: set[str] = set()
        current_dt = start_dt
        while current_dt <= end_dt:
            all_dates.add(current_dt.strftime("%Y-%m-%d"))
            current_dt += timedelta(days=1)

        completed_dates = self.get_completed_dates(start_date, end_date, instrument)
        missing_dates = all_dates - completed_dates
        return sorted(missing_dates)


def make_completion_checker(
    bucket: str,
    path_pattern: str,
    dataset_name: str = "",
    project_id: str = "",
) -> DataCompletionChecker:
    """Factory for DataCompletionChecker.

    Args:
        bucket: Cloud storage bucket name (required)
        path_pattern: Path pattern for storage completion checking
        dataset_name: Ignored — kept for backward compatibility
        project_id: Ignored — kept for backward compatibility

    Returns:
        DataCompletionChecker ready for use
    """
    return DataCompletionChecker(bucket=bucket, path_pattern=path_pattern)


def get_available_date_range(
    bucket: str,
    path_prefix: str,
) -> tuple[str | None, str | None]:
    """Return (earliest_date, latest_date) found under path_prefix in storage, or (None, None).

    Args:
        bucket: Cloud storage bucket name
        path_prefix: Path prefix to search under

    Returns:
        Tuple of (earliest_date, latest_date) as YYYY-MM-DD strings, or (None, None)
    """
    client = get_storage_client()
    bucket_obj = client.bucket(bucket)

    blobs_iterator: Iterable[object] = cast(Iterable[object], bucket_obj.list_blobs(prefix=path_prefix))

    dates: list[datetime] = []

    for blob in blobs_iterator:
        blob_path: str = str(getattr(blob, "name", ""))
        path_parts: list[str] = blob_path.split("/")
        for part in path_parts:
            try:
                date_dt = datetime.strptime(part, "%Y-%m-%d")
                dates.append(date_dt)
                break
            except ValueError as e:
                logger.debug("Skipping item due to %s: %s", type(e).__name__, e)
                continue

    if not dates:
        return (None, None)

    dates.sort()
    earliest = dates[0].strftime("%Y-%m-%d")
    latest = dates[-1].strftime("%Y-%m-%d")

    return (earliest, latest)
