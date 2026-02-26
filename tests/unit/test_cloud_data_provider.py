"""Simple unit tests for cloud data provider functionality."""

from unittest.mock import MagicMock, patch

import pandas as pd
from unified_cloud_services import CloudTarget

from unified_domain_services import CloudDataProviderBase, InstrumentsDataProvider, MarketDataProvider


class TestCloudDataProviderBase:
    """Test CloudDataProviderBase basic functionality."""

    @patch("unified_domain_services.cloud_data_provider.StandardizedDomainCloudService")
    def test_initialization_with_cloud_target(self, mock_service: MagicMock):
        """Test CloudDataProviderBase initialization with CloudTarget."""
        target = CloudTarget(
            project_id="test-project",
            gcs_bucket="test-bucket",
            bigquery_dataset="test-dataset",
            bigquery_location="asia-northeast1",
        )

        class ConcreteProvider(CloudDataProviderBase):
            def get_provider_id(self) -> str:
                return "test"

        provider = ConcreteProvider(domain="test", cloud_target=target)

        assert provider is not None
        assert provider.domain == "test"
        assert provider.cloud_target == target

    @patch("unified_domain_services.cloud_data_provider.StandardizedDomainCloudService")
    def test_download_from_gcs_returns_dataframe(self, mock_service: MagicMock):
        """Test download_from_gcs returns DataFrame."""
        target = CloudTarget(
            project_id="test-project",
            gcs_bucket="test-bucket",
            bigquery_dataset="test-dataset",
        )

        class ConcreteProvider(CloudDataProviderBase):
            def get_provider_id(self) -> str:
                return "test"

        provider = ConcreteProvider(domain="test", cloud_target=target)

        # Mock the cloud service
        mock_instance = MagicMock()
        mock_instance.download_from_gcs.return_value = pd.DataFrame({"col1": [1, 2, 3]})
        provider.cloud_service = mock_instance

        result = provider.download_from_gcs("test_path.parquet")

        assert isinstance(result, pd.DataFrame)
        assert not result.empty


class TestInstrumentsDataProvider:
    """Test InstrumentsDataProvider basic functionality."""

    @patch("unified_domain_services.cloud_data_provider._resolve_instruments_bucket_cefi")
    @patch("unified_domain_services.cloud_data_provider.unified_config")
    @patch("unified_domain_services.cloud_data_provider.StandardizedDomainCloudService")
    def test_initialization(self, mock_service: MagicMock, mock_config: MagicMock, mock_resolve: MagicMock):
        """Test InstrumentsDataProvider can be initialized."""
        mock_resolve.return_value = "test-bucket"
        mock_config.instruments_bigquery_dataset = "test-dataset"

        provider = InstrumentsDataProvider()

        assert provider is not None
        assert provider.domain == "instruments"


class TestMarketDataProvider:
    """Test MarketDataProvider basic functionality."""

    @patch("unified_domain_services.cloud_data_provider.unified_config")
    @patch("unified_domain_services.cloud_data_provider.StandardizedDomainCloudService")
    def test_initialization(self, mock_service: MagicMock, mock_config: MagicMock):
        """Test MarketDataProvider can be initialized."""
        mock_config.market_data_gcs_bucket = "test-bucket"
        mock_config.market_data_bigquery_dataset = "test-dataset"

        provider = MarketDataProvider()

        assert provider is not None
        assert provider.domain == "market_data"
