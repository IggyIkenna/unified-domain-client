"""Unit tests for domain clients."""

from unittest.mock import MagicMock, patch

import pandas as pd


class TestInstrumentsDomainClient:
    """Test InstrumentsDomainClient."""

    @patch("unified_domain_services.clients.unified_config")
    @patch("unified_domain_services.clients.StandardizedDomainCloudService")
    def test_get_trading_parameters_returns_none_when_not_found(self, mock_service: MagicMock, mock_config: MagicMock):
        """Test get_trading_parameters returns None when instrument not found."""
        from unified_domain_services.clients import InstrumentsDomainClient

        mock_config.gcp_project_id = "p"
        mock_config.instruments_gcs_bucket = "b"
        mock_config.instruments_bigquery_dataset = "d"
        client = InstrumentsDomainClient(project_id="p", gcs_bucket="b", bigquery_dataset="d")
        client.get_instruments_for_date = MagicMock(return_value=pd.DataFrame())

        result = client.get_trading_parameters("2024-01-15", "inst-1")
        assert result is None

    @patch("unified_domain_services.clients.unified_config")
    @patch("unified_domain_services.clients.StandardizedDomainCloudService")
    def test_get_trading_parameters_returns_params_when_found(self, mock_service: MagicMock, mock_config: MagicMock):
        """Test get_trading_parameters returns dict when instrument found."""
        from unified_domain_services.clients import InstrumentsDomainClient

        mock_config.gcp_project_id = "p"
        mock_config.instruments_gcs_bucket = "b"
        mock_config.instruments_bigquery_dataset = "d"
        client = InstrumentsDomainClient(project_id="p", gcs_bucket="b", bigquery_dataset="d")

        instrument_df = pd.DataFrame(
            [
                {
                    "instrument_key": "inst-1",
                    "tick_size": "0.01",
                    "min_size": "0.001",
                    "ccxt_symbol": "BTCUSDT",
                    "ccxt_exchange": "binance",
                    "data_types": "trades,book_snapshot_5",
                }
            ]
        )
        client.get_instruments_for_date = MagicMock(return_value=instrument_df)

        result = client.get_trading_parameters("2024-01-15", "inst-1")
        assert result is not None
        assert result["tick_size"] == "0.01"
        assert result["ccxt_symbol"] == "BTCUSDT"
        data_types = result.get("data_types")
        data_types = data_types if isinstance(data_types, list) else []
        assert "trades" in data_types
        assert "book_snapshot_5" in data_types

    @patch("unified_domain_services.clients.unified_config")
    @patch("unified_domain_services.clients.StandardizedDomainCloudService")
    def test_get_summary_stats_empty(self, mock_service: MagicMock, mock_config: MagicMock):
        """Test get_summary_stats returns error dict when no instruments."""
        from unified_domain_services.clients import InstrumentsDomainClient

        mock_config.gcp_project_id = "p"
        mock_config.instruments_gcs_bucket = "b"
        mock_config.instruments_bigquery_dataset = "d"
        client = InstrumentsDomainClient(project_id="p", gcs_bucket="b", bigquery_dataset="d")
        client.get_instruments_for_date = MagicMock(return_value=pd.DataFrame())

        result = client.get_summary_stats("2024-01-15")
        assert result.get("total_instruments") == 0
        assert "error" in result

    @patch("unified_domain_services.clients.unified_config")
    @patch("unified_domain_services.clients.StandardizedDomainCloudService")
    def test_get_summary_stats_with_data(self, mock_service: MagicMock, mock_config: MagicMock):
        """Test get_summary_stats returns stats when instruments exist."""
        from unified_domain_services.clients import InstrumentsDomainClient

        mock_config.gcp_project_id = "p"
        mock_config.instruments_gcs_bucket = "b"
        mock_config.instruments_bigquery_dataset = "d"
        client = InstrumentsDomainClient(project_id="p", gcs_bucket="b", bigquery_dataset="d")

        df = pd.DataFrame(
            {
                "venue": ["BINANCE", "BINANCE", "OKX"],
                "instrument_type": ["PERPETUAL", "SPOT", "PERPETUAL"],
                "base_asset": ["BTC", "ETH", "BTC"],
                "quote_asset": ["USDT", "USDT", "USDT"],
            }
        )
        client.get_instruments_for_date = MagicMock(return_value=df)

        result = client.get_summary_stats("2024-01-15")
        assert result.get("total_instruments") == 3
        assert result.get("venues") == 2
        assert result.get("instrument_types") == 2


class TestFactoryFunctions:
    """Test factory functions."""

    @patch("unified_domain_services.clients.StandardizedDomainCloudService")
    @patch("unified_domain_services.clients.unified_config")
    def test_create_instruments_client(self, mock_config: MagicMock, mock_standardized_service: MagicMock):
        """Test create_instruments_client returns client."""
        from unified_domain_services.clients import create_instruments_client

        mock_config.gcp_project_id = "p"
        mock_config.instruments_gcs_bucket = "b"
        mock_config.instruments_bigquery_dataset = "d"
        mock_standardized_service.return_value = MagicMock()
        client = create_instruments_client(project_id="p")
        assert client is not None
        assert hasattr(client, "get_instruments_for_date")
