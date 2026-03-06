"""Data completion checking for GCS datasets.

Tier 2 compliance: Only imports from unified_cloud_interface and local modules.
"""

import logging
from datetime import datetime, timedelta
from typing import Iterable, cast

from unified_cloud_interface import get_storage_client

from unified_domain_client.cloud_target import CloudTarget

logger = logging.getLogger(__name__)


class DataCompletionChecker:
    """Checks GCS for which dates have complete data for a given dataset path pattern."""

    def __init__(
        self,
        cloud_target: CloudTarget,
        path_pattern: str,
    ) -> None:
        """Initialize checker with cloud target and path pattern.

        Args:
            cloud_target: CloudTarget configuration for GCS access
            path_pattern: Path pattern e.g. "market_data/{instrument}/{date}/data.parquet"
        """
        self.cloud_target: CloudTarget = cloud_target
        self.path_pattern: str = path_pattern
        self._client = get_storage_client()

    def get_completed_dates(
        self,
        start_date: str,
        end_date: str,
        instrument: str | None = None,
    ) -> set[str]:
        """Return set of YYYY-MM-DD strings that have data in GCS.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            instrument: Optional instrument filter for path pattern

        Returns:
            Set of date strings with complete data
        """
        bucket = self._client.bucket(self.cloud_target.gcs_bucket)

        # Build prefix pattern
        prefix_pattern = self.path_pattern
        if instrument is not None:
            prefix_pattern = prefix_pattern.replace("{instrument}", instrument)

        # Remove the {date} placeholder and anything after it to get base prefix
        base_prefix = prefix_pattern.split("{date}")[0]

        # List all blobs with this prefix
        # GCS client list_blobs() returns untyped iterator; cast to Iterable[object].
        blobs_iterator: Iterable[object] = cast(Iterable[object], bucket.list_blobs(prefix=base_prefix))

        completed_dates: set[str] = set()
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        for blob in blobs_iterator:
            # Extract date from blob name
            blob_path: str = str(getattr(blob, "name", ""))

            # Try to find date pattern in the path
            path_parts: list[str] = blob_path.split("/")
            for part in path_parts:
                try:
                    # Check if this part looks like a date
                    date_dt = datetime.strptime(part, "%Y-%m-%d")
                    if start_dt <= date_dt <= end_dt:
                        completed_dates.add(part)
                        break
                except ValueError as e:
                    logger.debug("Skipping item due to %s: %s", type(e).__name__, e)
                    continue

        return completed_dates

    def is_date_complete(self, date: str, instrument: str | None = None) -> bool:
        """Check if a specific date has complete data.

        Args:
            date: Date in YYYY-MM-DD format
            instrument: Optional instrument filter

        Returns:
            True if data exists for the date
        """
        bucket = self._client.bucket(self.cloud_target.gcs_bucket)

        # Build full path for this date
        full_path = self.path_pattern.replace("{date}", date)
        if instrument is not None:
            full_path = full_path.replace("{instrument}", instrument)

        # Check if blob exists
        blob = bucket.blob(full_path)
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

        # Generate all dates in range
        all_dates: set[str] = set()
        current_dt = start_dt
        while current_dt <= end_dt:
            all_dates.add(current_dt.strftime("%Y-%m-%d"))
            current_dt += timedelta(days=1)

        # Get completed dates
        completed_dates = self.get_completed_dates(start_date, end_date, instrument)

        # Return missing dates sorted
        missing_dates = all_dates - completed_dates
        return sorted(missing_dates)


def make_completion_checker(
    dataset_name: str,
    project_id: str,
    path_pattern: str,
) -> "DataCompletionChecker":
    """Factory for DataCompletionChecker that hides CloudTarget construction.

    Services should use this instead of constructing CloudTarget directly.

    Args:
        dataset_name: Name of the dataset (used as BigQuery dataset and for bucket lookup via build_bucket)
        project_id: GCP project ID
        path_pattern: Path pattern for GCS completion checking

    Returns:
        DataCompletionChecker ready for use
    """
    from unified_domain_client.paths import build_bucket as _build_bucket

    bucket = _build_bucket(dataset_name, project_id=project_id)
    cloud_target = CloudTarget(
        gcs_bucket=bucket,
        bigquery_dataset=dataset_name,
        project_id=project_id,
    )
    return DataCompletionChecker(cloud_target=cloud_target, path_pattern=path_pattern)


def get_available_date_range(
    cloud_target: CloudTarget,
    path_prefix: str,
) -> tuple[str | None, str | None]:
    """Return (earliest_date, latest_date) found under path_prefix in GCS, or (None, None).

    Args:
        cloud_target: CloudTarget configuration for GCS access
        path_prefix: Path prefix to search under

    Returns:
        Tuple of (earliest_date, latest_date) as YYYY-MM-DD strings, or (None, None)
    """
    client = get_storage_client()
    bucket = client.bucket(cloud_target.gcs_bucket)

    # List all blobs with this prefix
    # GCS client list_blobs() returns untyped iterator; cast to Iterable[object].
    blobs_iterator: Iterable[object] = cast(Iterable[object], bucket.list_blobs(prefix=path_prefix))

    dates: list[datetime] = []

    for blob in blobs_iterator:
        # Extract date from blob name
        blob_path: str = str(getattr(blob, "name", ""))

        # Try to find date pattern in the path
        path_parts: list[str] = blob_path.split("/")
        for part in path_parts:
            try:
                # Check if this part looks like a date
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
