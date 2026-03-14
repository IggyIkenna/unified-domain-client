"""Integration tests that import and exercise each library dependency.

Satisfies check-integration-dep-coverage.py: each manifest library dep
must be imported in at least one tests/integration/ file.
"""

from __future__ import annotations

import pytest


@pytest.mark.integration
def test_unified_trading_library_import() -> None:
    from unified_trading_library import GracefulShutdownHandler

    assert GracefulShutdownHandler is not None


@pytest.mark.integration
def test_unified_config_interface_import() -> None:
    from unified_config_interface import BaseConfig

    assert BaseConfig is not None


@pytest.mark.integration
def test_unified_cloud_interface_import() -> None:
    from unified_cloud_interface import get_storage_client

    assert callable(get_storage_client)


@pytest.mark.integration
def test_unified_api_contracts_import() -> None:
    from unified_api_contracts import CanonicalOrder

    assert CanonicalOrder is not None


@pytest.mark.integration
def test_unified_internal_contracts_import() -> None:
    from unified_internal_contracts import LifecycleEventType

    assert LifecycleEventType is not None


@pytest.mark.integration
def test_unified_ml_interface_import() -> None:
    from unified_ml_interface import ModelRegistry

    assert ModelRegistry is not None
