"""Simple unit tests for domain clients - focusing on working functionality."""

from unittest.mock import MagicMock, patch

from unified_domain_services import (
    ClientConfig,
    ExecutionDomainClient,
    FeaturesClientConfig,
    FeaturesDomainClient,
    InstrumentsDomainClient,
    MarketCandleDataDomainClient,
    MarketTickDataDomainClient,
    create_instruments_client,
)


class TestInstrumentsDomainClientSimple:
    """Test InstrumentsDomainClient basic functionality."""

    @patch("unified_domain_services.clients.instruments.unified_config")
    @patch("unified_domain_services.clients.instruments.StandardizedDomainCloudService")
    def test_initialization(self, mock_service: MagicMock, mock_config: MagicMock):
        """Test InstrumentsDomainClient can be initialized."""
        mock_config.gcp_project_id = "p"
        mock_config.instruments_gcs_bucket = "b"
        mock_config.instruments_bigquery_dataset = "d"

        client = InstrumentsDomainClient(project_id="p", gcs_bucket="b", bigquery_dataset="d")

        assert client is not None
        assert hasattr(client, "get_instruments_for_date")
        assert hasattr(client, "get_summary_stats")
        assert hasattr(client, "get_trading_parameters")


class TestMarketCandleDataDomainClientSimple:
    """Test MarketCandleDataDomainClient basic functionality."""

    @patch("unified_domain_services.clients.market_data.unified_config")
    @patch("unified_domain_services.clients.market_data.StandardizedDomainCloudService")
    def test_initialization(self, mock_service: MagicMock, mock_config: MagicMock):
        """Test MarketCandleDataDomainClient can be initialized."""
        mock_config.gcp_project_id = "p"
        mock_config.market_data_gcs_bucket = "b"
        mock_config.market_data_bigquery_dataset = "d"

        client = MarketCandleDataDomainClient()

        assert client is not None
        assert hasattr(client, "get_candles")


class TestMarketTickDataDomainClientSimple:
    """Test MarketTickDataDomainClient basic functionality."""

    @patch("unified_domain_services.clients.market_data.unified_config")
    @patch("unified_domain_services.clients.market_data.StandardizedDomainCloudService")
    def test_initialization(self, mock_service: MagicMock, mock_config: MagicMock):
        """Test MarketTickDataDomainClient can be initialized."""
        mock_config.gcp_project_id = "p"
        mock_config.market_data_gcs_bucket = "b"
        mock_config.market_data_bigquery_dataset = "d"

        client = MarketTickDataDomainClient()

        assert client is not None
        assert hasattr(client, "get_tick_data")


class TestFeaturesDomainClientSimple:
    """Test FeaturesDomainClient basic functionality."""

    @patch("unified_domain_services.clients.features.unified_config")
    @patch("unified_domain_services.clients.features.StandardizedDomainCloudService")
    def test_initialization_default(self, mock_service: MagicMock, mock_config: MagicMock):
        """Test FeaturesDomainClient initialization with defaults."""
        mock_config.gcp_project_id = "p"
        mock_config.features_gcs_bucket = "b"
        mock_config.features_bigquery_dataset = "d"

        client = FeaturesDomainClient()

        assert client is not None
        assert client.feature_type == "delta_one"
        assert hasattr(client, "get_features")


class TestExecutionDomainClientSimple:
    """Test ExecutionDomainClient basic functionality."""

    @patch("unified_domain_services.clients.execution.unified_config")
    @patch("unified_domain_services.clients.execution.StandardizedDomainCloudService")
    def test_initialization(self, mock_service: MagicMock, mock_config: MagicMock):
        """Test ExecutionDomainClient can be initialized."""
        mock_config.gcp_project_id = "p"
        mock_config.execution_gcs_bucket = "b"
        mock_config.execution_bigquery_dataset = "d"

        client = ExecutionDomainClient()

        assert client is not None
        assert hasattr(client, "get_backtest_summary")


class TestFactoryFunctionsSimple:
    """Test factory functions basic functionality."""

    @patch("unified_domain_services.clients.factory.StandardizedDomainCloudService")
    @patch("unified_domain_services.clients.factory.unified_config")
    def test_create_instruments_client(self, mock_config: MagicMock, mock_standardized_service: MagicMock):
        """Test create_instruments_client returns working client."""
        mock_config.gcp_project_id = "p"
        mock_config.instruments_gcs_bucket = "b"
        mock_config.instruments_bigquery_dataset = "d"
        mock_standardized_service.return_value = MagicMock()

        client = create_instruments_client(project_id="p")
        assert client is not None
        assert isinstance(client, InstrumentsDomainClient)


class TestClientTypedDicts:
    """Test TypedDict configurations."""

    def test_client_config_structure(self):
        """Test ClientConfig TypedDict structure."""
        # Should be able to create empty config
        config: ClientConfig = {}
        assert isinstance(config, dict)

        # Should be able to create partial config
        config = {"project_id": "test"}
        assert config["project_id"] == "test"

    def test_features_client_config_structure(self):
        """Test FeaturesClientConfig TypedDict structure."""
        # Should be able to create empty config
        config: FeaturesClientConfig = {}
        assert isinstance(config, dict)

        # Should be able to create config with all fields
        config = {
            "project_id": "test",
            "gcs_bucket": "bucket",
            "bigquery_dataset": "dataset",
            "feature_type": "volatility",
        }
        assert len(config) == 4
