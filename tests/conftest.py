"""Pytest configuration for unified-domain-client.

Provides:
- Mock secret client to prevent real cloud access during tests
- Mock unified_events_interface for test isolation
"""

from unittest.mock import MagicMock

import pytest


@pytest.fixture(autouse=True)
def mock_secret_client(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Prevent real secret access during tests."""
    mock = MagicMock(return_value="fake-secret-value")
    monkeypatch.setattr("unified_config_interface.get_secret", mock, raising=False)
    return mock
