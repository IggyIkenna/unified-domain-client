"""Unit tests for CloudDataProviderBase and subclasses."""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from unified_domain_client.cloud_target import CloudTarget


class TestCloudDataProviderBase:
    """Test CloudDataProviderBase initialization and core methods."""

    @pytest.fixture
    def mock_cloud_target(self):
        """Create mock CloudTarget."""
        return CloudTarget(
            project_id="test-project",
            gcs_bucket="test-bucket",
            bigquery_dataset="test_dataset",
            bigquery_location="asia-northeast1",
        )

    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    def test_init_with_cloud_target(self, mock_service: MagicMock):
        """Test initialization with explicit CloudTarget."""
        from unified_domain_client.cloud_data_provider import CloudDataProviderBase

        class ConcreteProvider(CloudDataProviderBase):
            pass

        target = CloudTarget(
            project_id="proj",
            gcs_bucket="bucket",
            bigquery_dataset="ds",
            bigquery_location="us",
        )
        provider = ConcreteProvider(domain="test", cloud_target=target)
        assert provider.domain == "test"
        # cloud_target is kept for backward compatibility; bucket defaults to config or domain-store
        assert hasattr(provider, "bucket")
        mock_service.assert_called_once()

    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    @patch("unified_domain_client.cloud_data_provider.UnifiedCloudConfig")
    def test_init_raises_without_project(self, mock_config_cls: MagicMock, mock_service: MagicMock):
        """Test initialization succeeds without project_id; bucket defaults to domain-store."""
        from unified_domain_client.cloud_data_provider import CloudDataProviderBase

        mock_config_instance = MagicMock()
        mock_config_instance.gcp_project_id = ""
        mock_config_instance.gcs_bucket = ""
        mock_config_instance.bigquery_dataset = ""
        mock_config_instance.bigquery_location = ""
        mock_config_cls.return_value = mock_config_instance

        class ConcreteProvider(CloudDataProviderBase):
            pass

        provider = ConcreteProvider(domain="test")
        assert provider.domain == "test"
        assert provider.bucket == "test-store"

    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    @patch("unified_domain_client.cloud_data_provider.UnifiedCloudConfig")
    def test_init_with_project_id(self, mock_config_cls: MagicMock, mock_service: MagicMock):
        """Test initialization with explicit project_id."""
        from unified_domain_client.cloud_data_provider import CloudDataProviderBase

        mock_config_instance = MagicMock()
        mock_config_instance.gcp_project_id = ""
        mock_config_instance.gcs_bucket = "b"
        mock_config_instance.bigquery_dataset = "d"
        mock_config_instance.bigquery_location = "loc"
        mock_config_cls.return_value = mock_config_instance

        class ConcreteProvider(CloudDataProviderBase):
            pass

        provider = ConcreteProvider(domain="test", project_id="my-project")
        assert provider.domain == "test"
        # project_id is kept for backward compatibility; bucket resolves from config.gcs_bucket
        assert provider.bucket == "b"

    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    def test_download_from_gcs_returns_empty_on_404(self, mock_service: MagicMock):
        """Test download_from_gcs returns empty DataFrame on 404."""
        from unified_domain_client.cloud_data_provider import CloudDataProviderBase

        target = CloudTarget(
            project_id="p",
            gcs_bucket="b",
            bigquery_dataset="d",
            bigquery_location="loc",
        )
        mock_instance = MagicMock()
        mock_instance.download_from_gcs.side_effect = OSError("404 Not Found")
        mock_service.return_value = mock_instance

        class ConcreteProvider(CloudDataProviderBase):
            pass

        provider = ConcreteProvider(domain="test", cloud_target=target)
        assert provider.cloud_service is mock_instance

        result = provider.download_from_gcs("path/to/file.parquet")
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    def test_download_from_gcs_returns_data(self, mock_service: MagicMock):
        """Test download_from_gcs returns DataFrame when successful."""
        from unified_domain_client.cloud_data_provider import CloudDataProviderBase

        target = CloudTarget(
            project_id="p",
            gcs_bucket="b",
            bigquery_dataset="d",
            bigquery_location="loc",
        )
        expected_df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        mock_instance = MagicMock()
        mock_instance.download_from_gcs.return_value = expected_df
        mock_service.return_value = mock_instance

        class ConcreteProvider(CloudDataProviderBase):
            pass

        provider = ConcreteProvider(domain="test", cloud_target=target)
        result = provider.download_from_gcs("path/to/file.parquet")
        assert not result.empty
        assert len(result) == 2

    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    def test_download_from_gcs_handles_no_such_object(self, mock_service: MagicMock):
        """Test download_from_gcs returns empty on 404/No such object."""
        from unified_domain_client.cloud_data_provider import CloudDataProviderBase

        target = CloudTarget(
            project_id="p",
            gcs_bucket="b",
            bigquery_dataset="d",
            bigquery_location="loc",
        )
        mock_instance = MagicMock()
        mock_instance.download_from_gcs.side_effect = OSError("No such object: gs://b/path")
        mock_service.return_value = mock_instance

        class ConcreteProvider(CloudDataProviderBase):
            pass

        provider = ConcreteProvider(domain="test", cloud_target=target)
        result = provider.download_from_gcs("path")
        assert result.empty

    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    def test_download_from_gcs_handles_no_such_object_variant(self, mock_service: MagicMock):
        """Test download_from_gcs returns empty on Not Found variant."""
        from unified_domain_client.cloud_data_provider import CloudDataProviderBase

        target = CloudTarget(
            project_id="p",
            gcs_bucket="b",
            bigquery_dataset="d",
            bigquery_location="loc",
        )
        mock_instance = MagicMock()
        mock_instance.download_from_gcs.side_effect = OSError("Not Found")
        mock_service.return_value = mock_instance

        class ConcreteProvider(CloudDataProviderBase):
            pass

        provider = ConcreteProvider(domain="test", cloud_target=target)
        result = provider.download_from_gcs("path")
        assert result.empty

    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    def test_check_gcs_exists_true(self, mock_service: MagicMock):
        """Test check_gcs_exists returns True when data exists."""
        from unified_domain_client.cloud_data_provider import CloudDataProviderBase

        target = CloudTarget(
            project_id="p",
            gcs_bucket="b",
            bigquery_dataset="d",
            bigquery_location="loc",
        )
        mock_instance = MagicMock()
        mock_instance.download_from_gcs.return_value = pd.DataFrame({"x": [1]})
        mock_service.return_value = mock_instance

        class ConcreteProvider(CloudDataProviderBase):
            pass

        provider = ConcreteProvider(domain="test", cloud_target=target)
        assert provider.check_gcs_exists("path") is True

    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    def test_check_gcs_exists_false(self, mock_service: MagicMock):
        """Test check_gcs_exists returns False when empty."""
        from unified_domain_client.cloud_data_provider import CloudDataProviderBase

        target = CloudTarget(
            project_id="p",
            gcs_bucket="b",
            bigquery_dataset="d",
            bigquery_location="loc",
        )
        mock_instance = MagicMock()
        mock_instance.download_from_gcs.return_value = pd.DataFrame()
        mock_service.return_value = mock_instance

        class ConcreteProvider(CloudDataProviderBase):
            pass

        provider = ConcreteProvider(domain="test", cloud_target=target)
        assert provider.check_gcs_exists("path") is False


class TestInstrumentsDataProvider:
    """Test InstrumentsDataProvider."""

    @patch("unified_domain_client.cloud_data_provider._resolve_instruments_bucket_cefi")
    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    def test_get_instruments_for_date(
        self,
        mock_service: MagicMock,
        mock_resolve: MagicMock,
    ):
        """Test get_instruments_for_date filters by venue and instrument_type."""
        from unified_domain_client.cloud_data_provider import InstrumentsDataProvider

        mock_resolve.return_value = "instruments-bucket"
        target = CloudTarget(
            project_id="p",
            gcs_bucket="instruments-bucket",
            bigquery_dataset="instruments",
            bigquery_location="loc",
        )
        mock_instance = MagicMock()
        df = pd.DataFrame(
            {
                "venue": ["BINANCE", "BINANCE", "OKX"],
                "instrument_type": ["PERPETUAL", "SPOT", "PERPETUAL"],
            }
        )
        mock_instance.download_from_gcs.return_value = df
        mock_service.return_value = mock_instance

        provider = InstrumentsDataProvider(cloud_target=target)
        provider.cloud_service = mock_instance
        with patch.object(provider, "download_from_gcs", return_value=df):
            result = provider.get_instruments_for_date(
                datetime(2024, 1, 15),
                venue="BINANCE",
                instrument_type="PERPETUAL",
            )
        assert len(result) == 1
        assert result.iloc[0]["venue"] == "BINANCE"
        assert result.iloc[0]["instrument_type"] == "PERPETUAL"


class TestMarketDataProvider:
    """Test MarketDataProvider."""

    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    def test_get_candles_builds_query(self, mock_service: MagicMock):
        """Test get_candles builds correct BigQuery query."""

        from unified_domain_client.cloud_data_provider import MarketDataProvider

        target = CloudTarget(
            project_id="p",
            gcs_bucket="b",
            bigquery_dataset="d",
            bigquery_location="loc",
        )
        mock_instance = MagicMock()
        mock_instance.query_bigquery.return_value = pd.DataFrame()
        mock_service.return_value = mock_instance

        provider = MarketDataProvider(cloud_target=target)
        provider.cloud_service = mock_instance

        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = datetime(2024, 1, 2, tzinfo=UTC)
        result = provider.get_candles("inst-1", "1m", start, end)
        assert isinstance(result, pd.DataFrame)
        mock_instance.query_bigquery.assert_called_once()
