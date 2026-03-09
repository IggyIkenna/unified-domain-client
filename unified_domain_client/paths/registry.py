# Moved to unified-config-interface. Re-exported for backward compatibility.
from unified_config_interface.paths.registry import (  # noqa: F401
    PATH_REGISTRY,
    DataSetSpec,
    PathRegistry,
    build_bucket,
    build_full_uri,
    build_path,
    get_spec,
)

__all__ = [
    "PATH_REGISTRY",
    "DataSetSpec",
    "PathRegistry",
    "build_bucket",
    "build_full_uri",
    "build_path",
    "get_spec",
]
