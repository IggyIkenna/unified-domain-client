"""Cloud target configuration for domain operations.

Tier 2 compliance: Local dataclass, no unified-trading-library dependency.
"""

from dataclasses import dataclass


@dataclass
class CloudTarget:
    """Runtime-configurable cloud target for specific operations."""

    project_id: str
    storage_bucket: str = ""
    analytics_dataset: str = ""
    region: str = "us-central1"
    bigquery_location: str = "asia-northeast1"

    def __post_init__(self) -> None:
        """Validate required parameters."""
        if not (self.project_id or "").strip():
            raise ValueError("project_id is required")
        if not (self.storage_bucket or "").strip():
            raise ValueError("storage_bucket is required")
        if not (self.analytics_dataset or "").strip():
            raise ValueError("analytics_dataset is required (even for storage-only operations)")
