"""Path registry patterns for cloud storage paths."""

from enum import StrEnum

from unified_domain_client.paths.registry import (
    PATH_REGISTRY,
    DataSetSpec,
    PathRegistry,
    build_bucket,
    build_full_uri,
    build_path,
    get_spec,
)


class ReadMode(StrEnum):
    AUTO = "auto"
    BQ_EXTERNAL = "bq"
    ATHENA = "athena"


__all__ = [
    "PATH_REGISTRY",
    "DataSetSpec",
    "PathRegistry",
    "ReadMode",
    "build_bucket",
    "build_full_uri",
    "build_path",
    "get_spec",
]
