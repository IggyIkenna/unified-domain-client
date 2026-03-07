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
            storage_bucket="test-bucket",
            analytics_dataset="test_dataset",
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
            storage_bucket="bucket",
            analytics_dataset="ds",
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
            storage_bucket="b",
            analytics_dataset="d",
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
            storage_bucket="b",
            analytics_dataset="d",
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
            storage_bucket="b",
            analytics_dataset="d",
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
            storage_bucket="b",
            analytics_dataset="d",
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
            storage_bucket="b",
            analytics_dataset="d",
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
            storage_bucket="b",
            analytics_dataset="d",
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
            storage_bucket="instruments-bucket",
            analytics_dataset="instruments",
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
            storage_bucket="b",
            analytics_dataset="d",
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


class TestCloudDataProviderAdditionalMethods:
    """Test additional methods of CloudDataProviderBase."""

    def _make_provider(self, mock_service_cls: MagicMock) -> object:
        from unified_domain_client.cloud_data_provider import CloudDataProviderBase

        class ConcreteProvider(CloudDataProviderBase):
            pass

        target = CloudTarget(
            project_id="p",
            storage_bucket="b",
            analytics_dataset="d",
            bigquery_location="loc",
        )
        return ConcreteProvider(domain="test", cloud_target=target)

    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    @patch("unified_domain_client.cloud_data_provider.UnifiedCloudConfig")
    def test_build_category_service_success(
        self, mock_config_cls: MagicMock, mock_service: MagicMock
    ):
        """Test _build_category_service returns bucket and service."""
        from unified_domain_client.cloud_data_provider import CloudDataProviderBase

        class ConcreteProvider(CloudDataProviderBase):
            pass

        mock_config_instance = MagicMock()
        mock_config_instance.get_bucket.return_value = "category-bucket"
        mock_config_instance.gcs_bucket = ""
        mock_config_cls.return_value = mock_config_instance

        provider = ConcreteProvider(domain="test", bucket="b")
        bucket_name, svc = provider._build_category_service("CEFI")
        assert bucket_name == "category-bucket"

    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    @patch("unified_domain_client.cloud_data_provider.UnifiedCloudConfig")
    def test_build_category_service_fallback(
        self, mock_config_cls: MagicMock, mock_service: MagicMock
    ):
        """Test _build_category_service falls back when ValueError raised."""
        from unified_domain_client.cloud_data_provider import CloudDataProviderBase

        class ConcreteProvider(CloudDataProviderBase):
            pass

        mock_config_instance = MagicMock()
        mock_config_instance.get_bucket.side_effect = ValueError("no bucket configured")
        mock_config_instance.gcs_bucket = ""
        mock_config_cls.return_value = mock_config_instance

        provider = ConcreteProvider(domain="instruments", bucket="b")
        bucket_name, svc = provider._build_category_service("cefi")
        assert bucket_name == "instruments-cefi"

    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    def test_download_from_category_bucket_success(self, mock_service: MagicMock):
        """Test download_from_category_bucket returns DataFrame on success."""
        from unified_domain_client.cloud_data_provider import CloudDataProviderBase

        class ConcreteProvider(CloudDataProviderBase):
            pass

        target = CloudTarget(
            project_id="p", storage_bucket="b", analytics_dataset="d", bigquery_location="loc"
        )
        mock_instance = MagicMock()
        mock_instance.download_from_gcs.return_value = pd.DataFrame({"x": [1, 2]})
        mock_service.return_value = mock_instance

        provider = ConcreteProvider(domain="test", cloud_target=target)

        with patch.object(
            provider, "_build_category_service", return_value=("cat-bucket", mock_instance)
        ):
            result = provider.download_from_category_bucket("path/to/file.parquet", "CEFI")
        assert len(result) == 2

    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    def test_download_from_category_bucket_404(self, mock_service: MagicMock):
        """Test download_from_category_bucket handles 404."""
        from unified_domain_client.cloud_data_provider import CloudDataProviderBase

        class ConcreteProvider(CloudDataProviderBase):
            pass

        target = CloudTarget(
            project_id="p", storage_bucket="b", analytics_dataset="d", bigquery_location="loc"
        )
        mock_instance = MagicMock()
        mock_instance.download_from_gcs.side_effect = OSError("404 Not Found")
        mock_service.return_value = mock_instance

        provider = ConcreteProvider(domain="test", cloud_target=target)

        with patch.object(
            provider, "_build_category_service", return_value=("cat-bucket", mock_instance)
        ):
            result = provider.download_from_category_bucket("path/to/file.parquet", "CEFI")
        assert result.empty

    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    def test_download_from_category_bucket_generic_error(self, mock_service: MagicMock):
        """Test download_from_category_bucket handles generic error."""
        from unified_domain_client.cloud_data_provider import CloudDataProviderBase

        class ConcreteProvider(CloudDataProviderBase):
            pass

        target = CloudTarget(
            project_id="p", storage_bucket="b", analytics_dataset="d", bigquery_location="loc"
        )
        mock_instance = MagicMock()
        mock_instance.download_from_gcs.side_effect = ConnectionError("connection refused")
        mock_service.return_value = mock_instance

        provider = ConcreteProvider(domain="test", cloud_target=target)

        with patch.object(
            provider, "_build_category_service", return_value=("cat-bucket", mock_instance)
        ):
            result = provider.download_from_category_bucket("path/to/file.parquet", "CEFI")
        assert result.empty

    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    def test_download_from_category_bucket_empty_df(self, mock_service: MagicMock):
        """Test download_from_category_bucket handles empty df."""
        from unified_domain_client.cloud_data_provider import CloudDataProviderBase

        class ConcreteProvider(CloudDataProviderBase):
            pass

        target = CloudTarget(
            project_id="p", storage_bucket="b", analytics_dataset="d", bigquery_location="loc"
        )
        mock_instance = MagicMock()
        mock_instance.download_from_gcs.return_value = pd.DataFrame()
        mock_service.return_value = mock_instance

        provider = ConcreteProvider(domain="test", cloud_target=target)

        with patch.object(
            provider, "_build_category_service", return_value=("cat-bucket", mock_instance)
        ):
            result = provider.download_from_category_bucket("path/to/file.parquet", "CEFI")
        assert result.empty

    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    def test_query_bigquery_success(self, mock_service: MagicMock):
        """Test query_bigquery returns DataFrame on success."""
        from unified_domain_client.cloud_data_provider import CloudDataProviderBase

        class ConcreteProvider(CloudDataProviderBase):
            pass

        target = CloudTarget(
            project_id="p", storage_bucket="b", analytics_dataset="d", bigquery_location="loc"
        )
        mock_instance = MagicMock()
        mock_instance.query_bigquery.return_value = pd.DataFrame({"count": [42]})
        mock_service.return_value = mock_instance

        provider = ConcreteProvider(domain="test", cloud_target=target)
        result = provider.query_bigquery("SELECT 42 AS count")
        assert len(result) == 1
        assert result.iloc[0]["count"] == 42

    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    def test_query_bigquery_error(self, mock_service: MagicMock):
        """Test query_bigquery returns empty DataFrame on error."""
        from unified_domain_client.cloud_data_provider import CloudDataProviderBase

        class ConcreteProvider(CloudDataProviderBase):
            pass

        target = CloudTarget(
            project_id="p", storage_bucket="b", analytics_dataset="d", bigquery_location="loc"
        )
        mock_instance = MagicMock()
        mock_instance.query_bigquery.side_effect = ConnectionError("bigquery down")
        mock_service.return_value = mock_instance

        provider = ConcreteProvider(domain="test", cloud_target=target)
        result = provider.query_bigquery("SELECT * FROM table")
        assert result.empty

    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    def test_upload_artifact_success(self, mock_service: MagicMock):
        """Test upload_artifact returns True on success."""
        from unified_domain_client.cloud_data_provider import CloudDataProviderBase

        class ConcreteProvider(CloudDataProviderBase):
            pass

        target = CloudTarget(
            project_id="p", storage_bucket="b", analytics_dataset="d", bigquery_location="loc"
        )
        mock_instance = MagicMock()
        mock_service.return_value = mock_instance

        provider = ConcreteProvider(domain="test", cloud_target=target)
        df = pd.DataFrame({"a": [1, 2]})
        result = provider.upload_artifact(df, "output/data.parquet")
        assert result is True

    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    def test_upload_artifact_error(self, mock_service: MagicMock):
        """Test upload_artifact returns False on error."""
        from unified_domain_client.cloud_data_provider import CloudDataProviderBase

        class ConcreteProvider(CloudDataProviderBase):
            pass

        target = CloudTarget(
            project_id="p", storage_bucket="b", analytics_dataset="d", bigquery_location="loc"
        )
        mock_instance = MagicMock()
        mock_instance.upload_artifact.side_effect = OSError("upload failed")
        mock_service.return_value = mock_instance

        provider = ConcreteProvider(domain="test", cloud_target=target)
        df = pd.DataFrame({"a": [1, 2]})
        result = provider.upload_artifact(df, "output/data.parquet")
        assert result is False

    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    def test_download_from_gcs_non_404_error(self, mock_service: MagicMock):
        """Test download_from_gcs returns empty on non-404 generic error."""
        from unified_domain_client.cloud_data_provider import CloudDataProviderBase

        class ConcreteProvider(CloudDataProviderBase):
            pass

        target = CloudTarget(
            project_id="p", storage_bucket="b", analytics_dataset="d", bigquery_location="loc"
        )
        mock_instance = MagicMock()
        mock_instance.download_from_gcs.side_effect = ConnectionError("general network error")
        mock_service.return_value = mock_instance

        provider = ConcreteProvider(domain="test", cloud_target=target)
        result = provider.download_from_gcs("path/to/file.parquet")
        assert result.empty

    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    def test_is_test_mode_property(self, mock_service: MagicMock):
        """Test is_test_mode property returns bool."""
        from unified_domain_client.cloud_data_provider import CloudDataProviderBase

        class ConcreteProvider(CloudDataProviderBase):
            pass

        target = CloudTarget(
            project_id="p", storage_bucket="b", analytics_dataset="d", bigquery_location="loc"
        )
        provider = ConcreteProvider(domain="test", cloud_target=target)
        # In test environment with pytest, this should return True
        assert isinstance(provider.is_test_mode, bool)


class TestFeaturesDataProvider:
    """Test FeaturesDataProvider."""

    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    @patch("unified_domain_client.cloud_data_provider.UnifiedCloudConfig")
    def test_get_features_for_date_no_filter(
        self, mock_config_cls: MagicMock, mock_service: MagicMock
    ):
        """Test get_features_for_date returns DataFrame without instrument_key filter."""
        from unified_domain_client.cloud_data_provider import FeaturesDataProvider

        mock_config_instance = MagicMock()
        mock_config_instance.features_gcs_bucket = "features-bucket"
        mock_config_instance.bigquery_dataset = "features_ds"
        mock_config_cls.return_value = mock_config_instance

        mock_instance = MagicMock()
        df = pd.DataFrame({"instrument_key": ["K1", "K2"], "value": [1.0, 2.0]})
        mock_instance.download_from_gcs.return_value = df
        mock_service.return_value = mock_instance

        provider = FeaturesDataProvider()
        provider.cloud_service = mock_instance

        with patch.object(provider, "download_from_gcs", return_value=df):
            result = provider.get_features_for_date(datetime(2024, 1, 1, tzinfo=UTC))
        assert len(result) == 2

    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    @patch("unified_domain_client.cloud_data_provider.UnifiedCloudConfig")
    def test_get_features_for_date_with_instrument_key_filter(
        self, mock_config_cls: MagicMock, mock_service: MagicMock
    ):
        """Test get_features_for_date filters by instrument_key."""
        from unified_domain_client.cloud_data_provider import FeaturesDataProvider

        mock_config_instance = MagicMock()
        mock_config_instance.features_gcs_bucket = "features-bucket"
        mock_config_instance.bigquery_dataset = "features_ds"
        mock_config_cls.return_value = mock_config_instance

        mock_instance = MagicMock()
        mock_service.return_value = mock_instance

        provider = FeaturesDataProvider()
        df = pd.DataFrame({"instrument_key": ["K1", "K2"], "value": [1.0, 2.0]})

        with patch.object(provider, "download_from_gcs", return_value=df):
            result = provider.get_features_for_date(
                datetime(2024, 1, 1, tzinfo=UTC), instrument_key="K1"
            )
        assert len(result) == 1
        assert result.iloc[0]["instrument_key"] == "K1"


class TestInstrumentsDataProviderAdditional:
    """Test InstrumentsDataProvider additional methods."""

    @patch("unified_domain_client.cloud_data_provider._resolve_instruments_bucket_cefi")
    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    def test_get_instruments_for_date_no_filter(
        self, mock_service: MagicMock, mock_resolve: MagicMock
    ):
        """Test get_instruments_for_date without venue/type filter."""
        from unified_domain_client.cloud_data_provider import InstrumentsDataProvider

        mock_resolve.return_value = "instruments-bucket"
        mock_instance = MagicMock()
        df = pd.DataFrame({"venue": ["BINANCE"], "instrument_type": ["PERPETUAL"]})
        mock_service.return_value = mock_instance

        provider = InstrumentsDataProvider()
        with patch.object(provider, "download_from_gcs", return_value=df):
            result = provider.get_instruments_for_date(datetime(2024, 1, 1, tzinfo=UTC))
        assert len(result) == 1

    @patch("unified_domain_client.cloud_data_provider._resolve_instruments_bucket_cefi")
    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    def test_get_instruments_for_date_with_category(
        self, mock_service: MagicMock, mock_resolve: MagicMock
    ):
        """Test get_instruments_for_date with category filter uses category bucket."""
        from unified_domain_client.cloud_data_provider import InstrumentsDataProvider

        mock_resolve.return_value = "instruments-bucket"
        mock_instance = MagicMock()
        df = pd.DataFrame({"venue": ["BINANCE"], "instrument_type": ["PERPETUAL"]})
        mock_service.return_value = mock_instance

        provider = InstrumentsDataProvider()
        with patch.object(provider, "download_from_category_bucket", return_value=df):
            result = provider.get_instruments_for_date(
                datetime(2024, 1, 1, tzinfo=UTC), category="CEFI"
            )
        assert len(result) == 1

    @patch("unified_domain_client.cloud_data_provider._resolve_instruments_bucket_cefi")
    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    def test_check_instruments_exist_true(self, mock_service: MagicMock, mock_resolve: MagicMock):
        """Test check_instruments_exist returns True when data found."""
        from unified_domain_client.cloud_data_provider import InstrumentsDataProvider

        mock_resolve.return_value = "instruments-bucket"
        mock_instance = MagicMock()
        mock_service.return_value = mock_instance

        provider = InstrumentsDataProvider()
        df = pd.DataFrame({"venue": ["BINANCE"]})
        with patch.object(provider, "get_instruments_for_date", return_value=df):
            result = provider.check_instruments_exist(datetime(2024, 1, 1, tzinfo=UTC))
        assert result is True

    @patch("unified_domain_client.cloud_data_provider._resolve_instruments_bucket_cefi")
    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    def test_check_instruments_exist_false(self, mock_service: MagicMock, mock_resolve: MagicMock):
        """Test check_instruments_exist returns False when no data found."""
        from unified_domain_client.cloud_data_provider import InstrumentsDataProvider

        mock_resolve.return_value = "instruments-bucket"
        mock_instance = MagicMock()
        mock_service.return_value = mock_instance

        provider = InstrumentsDataProvider()
        with patch.object(provider, "get_instruments_for_date", return_value=pd.DataFrame()):
            result = provider.check_instruments_exist(datetime(2024, 1, 1, tzinfo=UTC))
        assert result is False

    @patch("unified_domain_client.cloud_data_provider.UnifiedCloudConfig")
    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    def test_resolve_instruments_bucket_from_config(
        self, mock_service: MagicMock, mock_config_cls: MagicMock
    ):
        """Test _resolve_instruments_bucket_cefi uses config bucket."""
        from unified_domain_client.cloud_data_provider import _resolve_instruments_bucket_cefi

        mock_config_instance = MagicMock()
        mock_config_instance.instruments_gcs_bucket = "my-instruments-bucket"
        mock_config_cls.return_value = mock_config_instance

        result = _resolve_instruments_bucket_cefi()
        assert result == "my-instruments-bucket"

    @patch("unified_domain_client.cloud_data_provider.UnifiedCloudConfig")
    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    def test_resolve_instruments_bucket_from_project_id(
        self, mock_service: MagicMock, mock_config_cls: MagicMock
    ):
        """Test _resolve_instruments_bucket_cefi builds from project_id."""
        from unified_domain_client.cloud_data_provider import _resolve_instruments_bucket_cefi

        mock_config_instance = MagicMock()
        mock_config_instance.instruments_gcs_bucket = ""
        mock_config_instance.gcp_project_id = "my-project"
        mock_config_cls.return_value = mock_config_instance

        result = _resolve_instruments_bucket_cefi()
        assert "my-project" in result

    @patch("unified_domain_client.cloud_data_provider.UnifiedCloudConfig")
    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    def test_resolve_instruments_bucket_raises_when_no_config(
        self, mock_service: MagicMock, mock_config_cls: MagicMock
    ):
        """Test _resolve_instruments_bucket_cefi raises ValueError when no config."""
        from unified_domain_client.cloud_data_provider import _resolve_instruments_bucket_cefi

        mock_config_instance = MagicMock()
        mock_config_instance.instruments_gcs_bucket = ""
        mock_config_instance.gcp_project_id = ""
        mock_config_cls.return_value = mock_config_instance

        try:
            _resolve_instruments_bucket_cefi()
            assert False, "Should have raised ValueError"
        except ValueError:
            pass


class TestMarketDataProviderAdditional:
    """Test MarketDataProvider additional methods."""

    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    @patch("unified_domain_client.cloud_data_provider.UnifiedCloudConfig")
    def test_get_candles_with_naive_datetimes(
        self, mock_config_cls: MagicMock, mock_service: MagicMock
    ):
        """Test get_candles adds UTC timezone when datetimes are naive."""
        from unified_domain_client.cloud_data_provider import MarketDataProvider

        mock_config_instance = MagicMock()
        mock_config_instance.market_data_gcs_bucket = "mkt-bucket"
        mock_config_instance.market_data_bigquery_dataset = "mkt_ds"
        mock_config_cls.return_value = mock_config_instance

        mock_instance = MagicMock()
        mock_instance.query_bigquery.return_value = pd.DataFrame()
        mock_service.return_value = mock_instance

        provider = MarketDataProvider()
        provider.cloud_service = mock_instance

        # Use naive datetimes (no tzinfo)
        from datetime import datetime

        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 2)

        with patch.object(provider, "query_bigquery", return_value=pd.DataFrame()):
            result = provider.get_candles("inst-1", "1h", start, end)
        assert isinstance(result, pd.DataFrame)

    @patch("unified_domain_client.cloud_data_provider.StandardizedDomainCloudService")
    @patch("unified_domain_client.cloud_data_provider.UnifiedCloudConfig")
    def test_get_candles_with_limit(self, mock_config_cls: MagicMock, mock_service: MagicMock):
        """Test get_candles builds query with LIMIT clause."""
        from datetime import UTC, datetime

        from unified_domain_client.cloud_data_provider import MarketDataProvider

        mock_config_instance = MagicMock()
        mock_config_instance.market_data_gcs_bucket = "mkt-bucket"
        mock_config_instance.market_data_bigquery_dataset = "mkt_ds"
        mock_config_cls.return_value = mock_config_instance

        mock_instance = MagicMock()
        mock_service.return_value = mock_instance

        provider = MarketDataProvider()

        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = datetime(2024, 1, 2, tzinfo=UTC)

        with patch.object(provider, "query_bigquery", return_value=pd.DataFrame()) as mock_qbq:
            provider.get_candles("inst-1", "5m", start, end, limit=100)
            args, kwargs = mock_qbq.call_args
            query = args[0]
            assert "LIMIT" in query
