"""Unit tests for factory functions."""

import pytest

from unified_domain_services import (
    __all__,
    create_backtesting_cloud_service,
    create_features_cloud_service,
    create_instruments_cloud_service,
    create_market_data_cloud_service,
    create_strategy_cloud_service,
)


class TestFactoryFunctions:
    """Test deprecated factory functions."""

    def test_create_market_data_cloud_service_raises_not_implemented(self):
        """Test create_market_data_cloud_service raises NotImplementedError."""

        with pytest.raises(NotImplementedError) as exc_info:
            create_market_data_cloud_service()

        assert "Factory functions have been removed" in str(exc_info.value)
        assert "StandardizedDomainCloudService(domain='market_data')" in str(exc_info.value)

    def test_create_features_cloud_service_raises_not_implemented(self):
        """Test create_features_cloud_service raises NotImplementedError."""

        with pytest.raises(NotImplementedError) as exc_info:
            create_features_cloud_service()

        assert "Factory functions have been removed" in str(exc_info.value)
        assert "StandardizedDomainCloudService(domain='features')" in str(exc_info.value)

    def test_create_strategy_cloud_service_raises_not_implemented(self):
        """Test create_strategy_cloud_service raises NotImplementedError."""

        with pytest.raises(NotImplementedError) as exc_info:
            create_strategy_cloud_service()

        assert "Factory functions have been removed" in str(exc_info.value)
        assert "StandardizedDomainCloudService(domain='strategy')" in str(exc_info.value)

    def test_create_backtesting_cloud_service_raises_not_implemented(self):
        """Test create_backtesting_cloud_service raises NotImplementedError."""

        with pytest.raises(NotImplementedError) as exc_info:
            create_backtesting_cloud_service()

        assert "Factory functions have been removed" in str(exc_info.value)
        assert "StandardizedDomainCloudService(domain='ml')" in str(exc_info.value)

    def test_create_instruments_cloud_service_raises_not_implemented(self):
        """Test create_instruments_cloud_service raises NotImplementedError."""

        with pytest.raises(NotImplementedError) as exc_info:
            create_instruments_cloud_service()

        assert "Factory functions have been removed" in str(exc_info.value)
        assert "StandardizedDomainCloudService(domain='instruments')" in str(exc_info.value)

    def test_all_exports_present(self):
        """Test that all expected functions are exported in __all__."""

        expected_exports = [
            "create_backtesting_cloud_service",
            "create_features_cloud_service",
            "create_instruments_cloud_service",
            "create_market_data_cloud_service",
            "create_strategy_cloud_service",
        ]

        for export in expected_exports:
            assert export in __all__, f"Missing export: {export}"

        assert len(__all__) == len(expected_exports), f"Unexpected exports: {set(__all__) - set(expected_exports)}"
