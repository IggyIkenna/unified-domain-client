"""
Base classes and configurations for domain clients.

Contains shared types, configurations and helper functions used across all domain clients.
"""

from __future__ import annotations

import logging
from typing import TypedDict

logger = logging.getLogger(__name__)


class ClientConfig(TypedDict, total=False):
    """Configuration options for domain clients (project_id, gcs_bucket, bigquery_dataset)."""

    project_id: str | None
    gcs_bucket: str | None
    bigquery_dataset: str | None


class FeaturesClientConfig(TypedDict, total=False):
    """Configuration for FeaturesDomainClient (includes feature_type)."""

    project_id: str | None
    gcs_bucket: str | None
    bigquery_dataset: str | None
    feature_type: str


def _is_empty_or_na(val: object) -> bool:
    """Check if value is None, NaN, or empty (for scalars from DataFrame)."""
    if val is None:
        return True
    if isinstance(val, float) and val != val:  # NaN
        return True
    if val == "":
        return True
    return bool(isinstance(val, str) and not val.strip())
