"""Unit tests for domain clients."""

from unittest.mock import MagicMock, patch

import pandas as pd


class TestInstrumentsDomainClient:
    """Test InstrumentsDomainClient."""

    @patch("unified_domain_client.clients.instruments.UnifiedCloudConfig")
    @patch("unified_domain_client.clients.instruments.StandardizedDomainCloudService")
    def test_get_trading_parameters_returns_none_when_not_found(
        self, mock_service: MagicMock, mock_config: MagicMock
    ):
        """Test get_trading_parameters returns None when instrument not found."""
        from unified_domain_client.clients import InstrumentsDomainClient  # noqa: deep-import

        mock_config.return_value.gcp_project_id = "p"
        mock_config.return_value.instruments_gcs_bucket = "b"
        mock_config.return_value.instruments_bigquery_dataset = "d"
        client = InstrumentsDomainClient(project_id="p", storage_bucket="b", analytics_dataset="d")
        client.get_instruments_for_date = MagicMock(return_value=pd.DataFrame())

        result = client.get_trading_parameters("2024-01-15", "inst-1")
        assert result is None

    @patch("unified_domain_client.clients.instruments.UnifiedCloudConfig")
    @patch("unified_domain_client.clients.instruments.StandardizedDomainCloudService")
    def test_get_trading_parameters_returns_params_when_found(
        self, mock_service: MagicMock, mock_config: MagicMock
    ):
        """Test get_trading_parameters returns dict when instrument found."""
        from unified_domain_client.clients import InstrumentsDomainClient  # noqa: deep-import

        mock_config.return_value.gcp_project_id = "p"
        mock_config.return_value.instruments_gcs_bucket = "b"
        mock_config.return_value.instruments_bigquery_dataset = "d"
        client = InstrumentsDomainClient(project_id="p", storage_bucket="b", analytics_dataset="d")

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

    @patch("unified_domain_client.clients.instruments.UnifiedCloudConfig")
    @patch("unified_domain_client.clients.instruments.StandardizedDomainCloudService")
    def test_get_summary_stats_empty(self, mock_service: MagicMock, mock_config: MagicMock):
        """Test get_summary_stats returns error dict when no instruments."""
        from unified_domain_client.clients import InstrumentsDomainClient  # noqa: deep-import

        mock_config.return_value.gcp_project_id = "p"
        mock_config.return_value.instruments_gcs_bucket = "b"
        mock_config.return_value.instruments_bigquery_dataset = "d"
        client = InstrumentsDomainClient(project_id="p", storage_bucket="b", analytics_dataset="d")
        client.get_instruments_for_date = MagicMock(return_value=pd.DataFrame())

        result = client.get_summary_stats("2024-01-15")
        assert result.get("total_instruments") == 0
        assert "error" in result

    @patch("unified_domain_client.clients.instruments.UnifiedCloudConfig")
    @patch("unified_domain_client.clients.instruments.StandardizedDomainCloudService")
    def test_get_summary_stats_with_data(self, mock_service: MagicMock, mock_config: MagicMock):
        """Test get_summary_stats returns stats when instruments exist."""
        from unified_domain_client.clients import InstrumentsDomainClient  # noqa: deep-import

        mock_config.return_value.gcp_project_id = "p"
        mock_config.return_value.instruments_gcs_bucket = "b"
        mock_config.return_value.instruments_bigquery_dataset = "d"
        client = InstrumentsDomainClient(project_id="p", storage_bucket="b", analytics_dataset="d")

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

    @patch("unified_domain_client.clients.instruments.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.instruments.UnifiedCloudConfig")
    def test_create_instruments_client(
        self, mock_config: MagicMock, mock_standardized_service: MagicMock
    ):
        """Test create_instruments_client returns client."""
        from unified_domain_client.clients import create_instruments_client  # noqa: deep-import

        mock_config.return_value.gcp_project_id = "p"
        mock_config.return_value.instruments_gcs_bucket = "b"
        mock_config.return_value.instruments_bigquery_dataset = "d"
        mock_standardized_service.return_value = MagicMock()
        client = create_instruments_client(project_id="p")
        assert client is not None
        assert hasattr(client, "get_instruments_for_date")


# ===========================================================================
# Other domain clients — instantiation + smoke tests
# All use project_id/storage_bucket to avoid UnifiedCloudConfig() calls
# ===========================================================================


class TestExecutionDomainClient:
    """Test ExecutionDomainClient."""

    @patch("unified_domain_client.clients.execution.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.execution.UnifiedCloudConfig")
    def test_instantiate(self, mock_config: MagicMock, mock_svc: MagicMock):
        """Test ExecutionDomainClient can be instantiated."""
        from unified_domain_client.clients.execution import (  # noqa: deep-import
            ExecutionDomainClient,
        )

        mock_config.return_value.gcp_project_id = "proj"
        mock_config.return_value.execution_gcs_bucket = "exec-bucket"
        client = ExecutionDomainClient(project_id="proj", storage_bucket="exec-bucket")
        assert client is not None
        assert hasattr(client, "cloud_service")

    @patch("unified_domain_client.clients.execution.get_storage_client")
    @patch("unified_domain_client.clients.execution.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.execution.UnifiedCloudConfig")
    def test_get_fills_returns_dataframe_on_error(
        self, mock_config: MagicMock, mock_svc: MagicMock, mock_storage: MagicMock
    ):
        """Test get_fills returns empty DataFrame when download fails."""
        from unified_domain_client.clients.execution import (  # noqa: deep-import
            ExecutionDomainClient,
        )

        mock_config.return_value.gcp_project_id = "proj"
        mock_config.return_value.execution_gcs_bucket = "exec-bucket"
        mock_storage_instance = MagicMock()
        mock_storage_instance.download_bytes.side_effect = OSError("not found")
        mock_storage.return_value = mock_storage_instance

        client = ExecutionDomainClient(project_id="proj", storage_bucket="exec-bucket")
        result = client.get_fills("2024-01-15", "cefi")
        assert isinstance(result, pd.DataFrame)

    @patch("unified_domain_client.clients.execution.get_storage_client")
    @patch("unified_domain_client.clients.execution.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.execution.UnifiedCloudConfig")
    def test_get_orders_returns_dataframe_on_error(
        self, mock_config: MagicMock, mock_svc: MagicMock, mock_storage: MagicMock
    ):
        """Test get_orders returns empty DataFrame when download fails."""
        from unified_domain_client.clients.execution import (  # noqa: deep-import
            ExecutionDomainClient,
        )

        mock_config.return_value.gcp_project_id = "proj"
        mock_config.return_value.execution_gcs_bucket = "exec-bucket"
        mock_storage_instance = MagicMock()
        mock_storage_instance.download_bytes.side_effect = OSError("not found")
        mock_storage.return_value = mock_storage_instance

        client = ExecutionDomainClient(project_id="proj", storage_bucket="exec-bucket")
        result = client.get_orders("2024-01-15", "cefi")
        assert isinstance(result, pd.DataFrame)


class TestFeaturesDomainClients:
    """Test feature domain clients."""

    @patch("unified_domain_client.clients.features.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.features.UnifiedCloudConfig")
    def test_delta_one_instantiate(self, mock_config: MagicMock, mock_svc: MagicMock):
        """Test FeaturesDeltaOneDomainClient can be instantiated."""
        from unified_domain_client.clients.features import (  # noqa: deep-import
            FeaturesDeltaOneDomainClient,
        )

        mock_config.return_value.gcp_project_id = "proj"
        client = FeaturesDeltaOneDomainClient(
            project_id="proj", storage_bucket="features-bucket", category="cefi"
        )
        assert client is not None

    @patch("unified_domain_client.clients.features.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.features.UnifiedCloudConfig")
    def test_delta_one_get_features_returns_empty_on_error(
        self, mock_config: MagicMock, mock_svc: MagicMock
    ):
        """Test get_features returns empty DataFrame on error."""
        from unified_domain_client.clients.features import (  # noqa: deep-import
            FeaturesDeltaOneDomainClient,
        )

        mock_config.return_value.gcp_project_id = "proj"
        mock_svc_instance = MagicMock()
        mock_svc_instance.download_from_gcs.side_effect = OSError("not found")
        mock_svc.return_value = mock_svc_instance

        client = FeaturesDeltaOneDomainClient(project_id="proj", storage_bucket="features-bucket")
        result = client.get_features("2024-01-15", "BTC-USDT", "momentum", "1h")
        assert isinstance(result, pd.DataFrame)

    @patch("unified_domain_client.clients.features.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.features.UnifiedCloudConfig")
    def test_calendar_instantiate(self, mock_config: MagicMock, mock_svc: MagicMock):
        """Test FeaturesCalendarDomainClient can be instantiated."""
        from unified_domain_client.clients.features import (  # noqa: deep-import
            FeaturesCalendarDomainClient,
        )

        mock_config.return_value.gcp_project_id = "proj"
        client = FeaturesCalendarDomainClient(project_id="proj", storage_bucket="calendar-bucket")
        assert client is not None

    @patch("unified_domain_client.clients.features.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.features.UnifiedCloudConfig")
    def test_onchain_instantiate(self, mock_config: MagicMock, mock_svc: MagicMock):
        """Test FeaturesOnchainDomainClient can be instantiated."""
        from unified_domain_client.clients.features import (  # noqa: deep-import
            FeaturesOnchainDomainClient,
        )

        mock_config.return_value.gcp_project_id = "proj"
        client = FeaturesOnchainDomainClient(project_id="proj", storage_bucket="onchain-bucket")
        assert client is not None

    @patch("unified_domain_client.clients.features.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.features.UnifiedCloudConfig")
    def test_volatility_instantiate(self, mock_config: MagicMock, mock_svc: MagicMock):
        """Test FeaturesVolatilityDomainClient can be instantiated."""
        from unified_domain_client.clients.features import (  # noqa: deep-import
            FeaturesVolatilityDomainClient,
        )

        mock_config.return_value.gcp_project_id = "proj"
        client = FeaturesVolatilityDomainClient(project_id="proj", storage_bucket="vol-bucket")
        assert client is not None


class TestMLDomainClients:
    """Test ML domain clients."""

    @patch("unified_domain_client.clients.ml.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.ml.UnifiedCloudConfig")
    def test_ml_models_instantiate(self, mock_config: MagicMock, mock_svc: MagicMock):
        """Test MLModelsDomainClient can be instantiated."""
        from unified_domain_client.clients.ml import MLModelsDomainClient  # noqa: deep-import

        mock_config.return_value.gcp_project_id = "proj"
        client = MLModelsDomainClient(project_id="proj", storage_bucket="ml-bucket")
        assert client is not None

    @patch("unified_domain_client.clients.ml.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.ml.UnifiedCloudConfig")
    def test_ml_models_get_model_returns_empty_on_error(
        self, mock_config: MagicMock, mock_svc: MagicMock
    ):
        """Test get_model returns empty bytes on error."""
        from unified_domain_client.clients.ml import MLModelsDomainClient  # noqa: deep-import

        mock_config.return_value.gcp_project_id = "proj"
        mock_svc_instance = MagicMock()
        mock_svc_instance.download_from_gcs.side_effect = OSError("not found")
        mock_svc.return_value = mock_svc_instance

        client = MLModelsDomainClient(project_id="proj", storage_bucket="ml-bucket")
        result = client.get_model("cefi_btc_v1", "2024-01")
        assert result == b""

    @patch("unified_domain_client.clients.ml.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.ml.UnifiedCloudConfig")
    def test_ml_models_get_metadata_returns_empty_on_error(
        self, mock_config: MagicMock, mock_svc: MagicMock
    ):
        """Test get_metadata returns empty dict on error."""
        from unified_domain_client.clients.ml import MLModelsDomainClient  # noqa: deep-import

        mock_config.return_value.gcp_project_id = "proj"
        mock_svc_instance = MagicMock()
        mock_svc_instance.download_from_gcs.side_effect = OSError("not found")
        mock_svc.return_value = mock_svc_instance

        client = MLModelsDomainClient(project_id="proj", storage_bucket="ml-bucket")
        result = client.get_metadata("cefi_btc_v1", "2024-01")
        assert result == {}

    @patch("unified_domain_client.clients.ml.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.ml.UnifiedCloudConfig")
    def test_ml_predictions_instantiate(self, mock_config: MagicMock, mock_svc: MagicMock):
        """Test MLPredictionsDomainClient can be instantiated."""
        from unified_domain_client.clients.ml import MLPredictionsDomainClient  # noqa: deep-import

        mock_config.return_value.gcp_project_id = "proj"
        client = MLPredictionsDomainClient(project_id="proj", storage_bucket="pred-bucket")
        assert client is not None

    @patch("unified_domain_client.clients.ml.get_storage_client")
    @patch("unified_domain_client.clients.ml.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.ml.UnifiedCloudConfig")
    def test_ml_predictions_get_predictions_returns_empty_on_error(
        self, mock_config: MagicMock, mock_svc: MagicMock, mock_storage: MagicMock
    ):
        """Test get_predictions returns empty DataFrame on error."""
        from unified_domain_client.clients.ml import MLPredictionsDomainClient  # noqa: deep-import

        mock_config.return_value.gcp_project_id = "proj"
        mock_storage.side_effect = OSError("not found")

        client = MLPredictionsDomainClient(project_id="proj", storage_bucket="pred-bucket")
        result = client.get_predictions("2024-01-15", "batch")
        assert isinstance(result, pd.DataFrame)


class TestSimpleDomainClients:
    """Test simple single-class domain clients."""

    @patch("unified_domain_client.clients.pnl.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.pnl.UnifiedCloudConfig")
    def test_pnl_instantiate(self, mock_config: MagicMock, mock_svc: MagicMock):
        """Test PnLDomainClient can be instantiated."""
        from unified_domain_client.clients.pnl import PnLDomainClient  # noqa: deep-import

        mock_config.return_value.gcp_project_id = "proj"
        client = PnLDomainClient(project_id="proj", storage_bucket="pnl-bucket")
        assert client is not None

    @patch("unified_domain_client.clients.pnl.get_storage_client")
    @patch("unified_domain_client.clients.pnl.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.pnl.UnifiedCloudConfig")
    def test_pnl_get_returns_empty_on_error(
        self, mock_config: MagicMock, mock_svc: MagicMock, mock_storage: MagicMock
    ):
        """Test get_pnl_attribution returns empty DataFrame on error."""
        from unified_domain_client.clients.pnl import PnLDomainClient  # noqa: deep-import

        mock_config.return_value.gcp_project_id = "proj"
        mock_storage_instance = MagicMock()
        mock_storage_instance.download_bytes.side_effect = OSError("not found")
        mock_storage.return_value = mock_storage_instance

        client = PnLDomainClient(project_id="proj", storage_bucket="pnl-bucket")
        result = client.get_pnl_attribution("2024-01-15", "CEFI_BTC_V1")
        assert isinstance(result, pd.DataFrame)

    @patch("unified_domain_client.clients.positions.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.positions.UnifiedCloudConfig")
    def test_positions_instantiate(self, mock_config: MagicMock, mock_svc: MagicMock):
        """Test PositionsDomainClient can be instantiated."""
        from unified_domain_client.clients.positions import (  # noqa: deep-import
            PositionsDomainClient,
        )

        mock_config.return_value.gcp_project_id = "proj"
        client = PositionsDomainClient(project_id="proj", storage_bucket="positions-bucket")
        assert client is not None

    @patch("unified_domain_client.clients.risk.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.risk.UnifiedCloudConfig")
    def test_risk_instantiate(self, mock_config: MagicMock, mock_svc: MagicMock):
        """Test RiskDomainClient can be instantiated."""
        from unified_domain_client.clients.risk import RiskDomainClient  # noqa: deep-import

        mock_config.return_value.gcp_project_id = "proj"
        client = RiskDomainClient(project_id="proj", storage_bucket="risk-bucket")
        assert client is not None

    @patch("unified_domain_client.clients.strategy.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.strategy.UnifiedCloudConfig")
    def test_strategy_instantiate(self, mock_config: MagicMock, mock_svc: MagicMock):
        """Test StrategyDomainClient can be instantiated."""
        from unified_domain_client.clients.strategy import StrategyDomainClient  # noqa: deep-import

        mock_config.return_value.gcp_project_id = "proj"
        client = StrategyDomainClient(project_id="proj", storage_bucket="strategy-bucket")
        assert client is not None

    @patch("unified_domain_client.clients.strategy.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.strategy.UnifiedCloudConfig")
    def test_strategy_get_orders_returns_empty_on_error(
        self, mock_config: MagicMock, mock_svc: MagicMock
    ):
        """Test get_orders returns empty DataFrame on error."""
        from unified_domain_client.clients.strategy import StrategyDomainClient  # noqa: deep-import

        mock_config.return_value.gcp_project_id = "proj"
        mock_svc_instance = MagicMock()
        mock_svc_instance.download_from_gcs.side_effect = OSError("not found")
        mock_svc.return_value = mock_svc_instance

        client = StrategyDomainClient(project_id="proj", storage_bucket="strategy-bucket")
        result = client.get_orders("2024-01-15", "CEFI_BTC_V1")
        assert isinstance(result, pd.DataFrame)


class TestMarketDataDomainClients:
    """Test MarketCandleDataDomainClient and MarketTickDataDomainClient."""

    @patch("unified_domain_client.clients.market_data.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.market_data.UnifiedCloudConfig")
    def test_candle_data_instantiate(self, mock_config: MagicMock, mock_svc: MagicMock):
        """Test MarketCandleDataDomainClient can be instantiated."""
        from unified_domain_client.clients.market_data import (  # noqa: deep-import
            MarketCandleDataDomainClient,
        )

        mock_config.return_value.gcp_project_id = "proj"
        mock_config.return_value.market_data_gcs_bucket = "mktbucket"
        mock_config.return_value.market_data_bigquery_dataset = "mkt_bq"
        client = MarketCandleDataDomainClient(project_id="proj", storage_bucket="mktbucket")
        assert client is not None

    @patch("unified_domain_client.clients.market_data.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.market_data.UnifiedCloudConfig")
    def test_candle_data_get_candles_returns_empty_on_error(
        self, mock_config: MagicMock, mock_svc: MagicMock
    ):
        """Test get_candles returns empty DataFrame on error."""
        from datetime import UTC, datetime

        from unified_domain_client.clients.market_data import (  # noqa: deep-import
            MarketCandleDataDomainClient,
        )

        mock_config.return_value.gcp_project_id = "proj"
        mock_config.return_value.market_data_gcs_bucket = "mktbucket"
        mock_config.return_value.market_data_bigquery_dataset = "mkt_bq"
        mock_svc_instance = MagicMock()
        mock_svc_instance.download_from_gcs.side_effect = OSError("not found")
        mock_svc.return_value = mock_svc_instance

        client = MarketCandleDataDomainClient(project_id="proj", storage_bucket="mktbucket")
        result = client.get_candles(datetime(2024, 1, 15, tzinfo=UTC), "BINANCE:PERPETUAL:BTC-USDT")
        assert isinstance(result, pd.DataFrame)

    @patch("unified_domain_client.clients.market_data.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.market_data.UnifiedCloudConfig")
    def test_tick_data_instantiate(self, mock_config: MagicMock, mock_svc: MagicMock):
        """Test MarketTickDataDomainClient can be instantiated."""
        from unified_domain_client.clients.market_data import (  # noqa: deep-import
            MarketTickDataDomainClient,
        )

        mock_config.return_value.gcp_project_id = "proj"
        mock_config.return_value.market_data_gcs_bucket = "mktbucket"
        mock_config.return_value.market_data_bigquery_dataset = "mkt_bq"
        client = MarketTickDataDomainClient(project_id="proj", storage_bucket="mktbucket")
        assert client is not None


# ===========================================================================
# Additional method-level tests using cloud_service injection
# ===========================================================================


class TestFeaturesDomainClientMethods:
    """Test individual methods of features domain clients via cloud_service injection."""

    @patch("unified_domain_client.clients.features.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.features.UnifiedCloudConfig")
    def test_delta_one_get_features_returns_dataframe_on_success(
        self, mock_config: MagicMock, mock_svc: MagicMock
    ):
        """Test get_features returns DataFrame when download succeeds."""
        from unified_domain_client.clients.features import (  # noqa: deep-import
            FeaturesDeltaOneDomainClient,
        )

        mock_config.return_value.gcp_project_id = "proj"
        client = FeaturesDeltaOneDomainClient(project_id="proj", storage_bucket="features-bucket")
        mock_svc_instance = MagicMock()
        mock_svc_instance.download_from_gcs.return_value = pd.DataFrame({"feature": [1.0, 2.0]})
        client.cloud_service = mock_svc_instance
        result = client.get_features("2024-01-15", "BTC-USDT", "momentum", "1h")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    @patch("unified_domain_client.clients.features.get_storage_client")
    @patch("unified_domain_client.clients.features.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.features.UnifiedCloudConfig")
    def test_delta_one_get_available_dates_returns_empty_on_error(
        self, mock_config: MagicMock, mock_svc: MagicMock, mock_storage: MagicMock
    ):
        """Test get_available_dates returns empty list on error."""
        from unified_domain_client.clients.features import (  # noqa: deep-import
            FeaturesDeltaOneDomainClient,
        )

        mock_config.return_value.gcp_project_id = "proj"
        mock_storage.side_effect = OSError("not found")
        client = FeaturesDeltaOneDomainClient(project_id="proj", storage_bucket="features-bucket")
        result = client.get_available_dates("momentum", "1h")
        assert result == []

    @patch("unified_domain_client.clients.features.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.features.UnifiedCloudConfig")
    def test_calendar_get_features_returns_empty_on_error(
        self, mock_config: MagicMock, mock_svc: MagicMock
    ):
        """Test FeaturesCalendarDomainClient.get_features returns empty on error."""
        from unified_domain_client.clients.features import (  # noqa: deep-import
            FeaturesCalendarDomainClient,
        )

        mock_config.return_value.gcp_project_id = "proj"
        client = FeaturesCalendarDomainClient(project_id="proj", storage_bucket="calendar-bucket")
        mock_svc_instance = MagicMock()
        mock_svc_instance.download_from_gcs.side_effect = OSError("not found")
        client.cloud_service = mock_svc_instance
        result = client.get_features("2024-01-15", "cefi")
        assert isinstance(result, pd.DataFrame)

    @patch("unified_domain_client.clients.features.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.features.UnifiedCloudConfig")
    def test_onchain_get_features_returns_empty_on_error(
        self, mock_config: MagicMock, mock_svc: MagicMock
    ):
        """Test FeaturesOnchainDomainClient.get_features returns empty on error."""
        from unified_domain_client.clients.features import (  # noqa: deep-import
            FeaturesOnchainDomainClient,
        )

        mock_config.return_value.gcp_project_id = "proj"
        client = FeaturesOnchainDomainClient(project_id="proj", storage_bucket="onchain-bucket")
        mock_svc_instance = MagicMock()
        mock_svc_instance.download_from_gcs.side_effect = OSError("not found")
        client.cloud_service = mock_svc_instance
        result = client.get_features("2024-01-15", "defi_metrics")
        assert isinstance(result, pd.DataFrame)

    @patch("unified_domain_client.clients.features.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.features.UnifiedCloudConfig")
    def test_volatility_get_features_returns_empty_on_error(
        self, mock_config: MagicMock, mock_svc: MagicMock
    ):
        """Test FeaturesVolatilityDomainClient.get_features returns empty on error."""
        from unified_domain_client.clients.features import (  # noqa: deep-import
            FeaturesVolatilityDomainClient,
        )

        mock_config.return_value.gcp_project_id = "proj"
        client = FeaturesVolatilityDomainClient(project_id="proj", storage_bucket="vol-bucket")
        mock_svc_instance = MagicMock()
        mock_svc_instance.download_from_gcs.side_effect = OSError("not found")
        client.cloud_service = mock_svc_instance
        result = client.get_features("2024-01-15", "BTC", "realized_vol")
        assert isinstance(result, pd.DataFrame)

    @patch("unified_domain_client.clients.features.get_storage_client")
    @patch("unified_domain_client.clients.features.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.features.UnifiedCloudConfig")
    def test_calendar_get_available_dates_returns_empty_on_error(
        self, mock_config: MagicMock, mock_svc: MagicMock, mock_storage: MagicMock
    ):
        """Test FeaturesCalendarDomainClient.get_available_dates returns empty on error."""
        from unified_domain_client.clients.features import (  # noqa: deep-import
            FeaturesCalendarDomainClient,
        )

        mock_config.return_value.gcp_project_id = "proj"
        mock_storage.side_effect = OSError("not found")
        client = FeaturesCalendarDomainClient(project_id="proj", storage_bucket="calendar-bucket")
        result = client.get_available_dates("cefi")
        assert result == []

    @patch("unified_domain_client.clients.features.get_storage_client")
    @patch("unified_domain_client.clients.features.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.features.UnifiedCloudConfig")
    def test_onchain_get_available_dates_returns_empty_on_error(
        self, mock_config: MagicMock, mock_svc: MagicMock, mock_storage: MagicMock
    ):
        """Test FeaturesOnchainDomainClient.get_available_dates returns empty on error."""
        from unified_domain_client.clients.features import (  # noqa: deep-import
            FeaturesOnchainDomainClient,
        )

        mock_config.return_value.gcp_project_id = "proj"
        mock_storage.side_effect = OSError("not found")
        client = FeaturesOnchainDomainClient(project_id="proj", storage_bucket="onchain-bucket")
        result = client.get_available_dates("defi_metrics")
        assert result == []

    @patch("unified_domain_client.clients.features.get_storage_client")
    @patch("unified_domain_client.clients.features.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.features.UnifiedCloudConfig")
    def test_volatility_get_available_dates_returns_empty_on_error(
        self, mock_config: MagicMock, mock_svc: MagicMock, mock_storage: MagicMock
    ):
        """Test FeaturesVolatilityDomainClient.get_available_dates returns empty on error."""
        from unified_domain_client.clients.features import (  # noqa: deep-import
            FeaturesVolatilityDomainClient,
        )

        mock_config.return_value.gcp_project_id = "proj"
        mock_storage.side_effect = OSError("not found")
        client = FeaturesVolatilityDomainClient(project_id="proj", storage_bucket="vol-bucket")
        result = client.get_available_dates("realized_vol")
        assert result == []


class TestStrategyDomainClientMethods:
    """Test StrategyDomainClient methods via cloud_service injection."""

    @patch("unified_domain_client.clients.strategy.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.strategy.UnifiedCloudConfig")
    def test_get_instructions_returns_empty_on_error(
        self, mock_config: MagicMock, mock_svc: MagicMock
    ):
        """Test get_instructions returns empty DataFrame on error."""
        from unified_domain_client.clients.strategy import StrategyDomainClient  # noqa: deep-import

        mock_config.return_value.gcp_project_id = "proj"
        client = StrategyDomainClient(project_id="proj", storage_bucket="strategy-bucket")
        mock_svc_instance = MagicMock()
        mock_svc_instance.download_from_gcs.side_effect = OSError("not found")
        client.cloud_service = mock_svc_instance
        result = client.get_instructions("CEFI_BTC_V1", "2024-01-15")
        assert isinstance(result, pd.DataFrame)

    @patch("unified_domain_client.clients.strategy.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.strategy.UnifiedCloudConfig")
    def test_get_backtest_results_returns_empty_dict_on_error(
        self, mock_config: MagicMock, mock_svc: MagicMock
    ):
        """Test get_backtest_results returns empty dict on error."""
        from unified_domain_client.clients.strategy import StrategyDomainClient  # noqa: deep-import

        mock_config.return_value.gcp_project_id = "proj"
        client = StrategyDomainClient(project_id="proj", storage_bucket="strategy-bucket")
        mock_svc_instance = MagicMock()
        mock_svc_instance.download_from_gcs.side_effect = OSError("not found")
        client.cloud_service = mock_svc_instance
        result = client.get_backtest_results("CEFI_BTC_V1", "run_001")
        assert isinstance(result, dict)

    @patch("unified_domain_client.clients.strategy.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.strategy.UnifiedCloudConfig")
    def test_get_backtest_results_returns_dataframes_on_success(
        self, mock_config: MagicMock, mock_svc: MagicMock
    ):
        """Test get_backtest_results returns dict of DataFrames on success."""
        from unified_domain_client.clients.strategy import StrategyDomainClient  # noqa: deep-import

        mock_config.return_value.gcp_project_id = "proj"
        client = StrategyDomainClient(project_id="proj", storage_bucket="strategy-bucket")
        mock_svc_instance = MagicMock()
        mock_svc_instance.download_from_gcs.return_value = pd.DataFrame({"x": [1]})
        client.cloud_service = mock_svc_instance
        result = client.get_backtest_results("CEFI_BTC_V1", "run_001")
        assert isinstance(result, dict)
        assert len(result) == 3

    @patch("unified_domain_client.clients.strategy.get_storage_client")
    @patch("unified_domain_client.clients.strategy.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.strategy.UnifiedCloudConfig")
    def test_list_strategies_returns_empty_on_error(
        self, mock_config: MagicMock, mock_svc: MagicMock, mock_storage: MagicMock
    ):
        """Test list_strategies returns empty list on error."""
        from unified_domain_client.clients.strategy import StrategyDomainClient  # noqa: deep-import

        mock_config.return_value.gcp_project_id = "proj"
        mock_storage.side_effect = OSError("not found")
        client = StrategyDomainClient(project_id="proj", storage_bucket="strategy-bucket")
        result = client.list_strategies()
        assert result == []


class TestMLDomainClientMethods:
    """Test ML domain client methods via cloud_service injection."""

    @patch("unified_domain_client.clients.ml.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.ml.UnifiedCloudConfig")
    def test_ml_models_get_model_returns_bytes_on_success(
        self, mock_config: MagicMock, mock_svc: MagicMock
    ):
        """Test get_model returns bytes when download succeeds."""
        from unified_domain_client.clients.ml import MLModelsDomainClient  # noqa: deep-import

        mock_config.return_value.gcp_project_id = "proj"
        client = MLModelsDomainClient(project_id="proj", storage_bucket="ml-bucket")
        mock_svc_instance = MagicMock()
        mock_svc_instance.download_from_gcs.return_value = b"model_bytes_here"
        client.cloud_service = mock_svc_instance
        result = client.get_model("cefi_btc_v1", "2024-01")
        assert result == b"model_bytes_here"

    @patch("unified_domain_client.clients.ml.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.ml.UnifiedCloudConfig")
    def test_ml_models_get_metadata_returns_dict_on_success(
        self, mock_config: MagicMock, mock_svc: MagicMock
    ):
        """Test get_metadata returns dict when download succeeds."""
        from unified_domain_client.clients.ml import MLModelsDomainClient  # noqa: deep-import

        mock_config.return_value.gcp_project_id = "proj"
        client = MLModelsDomainClient(project_id="proj", storage_bucket="ml-bucket")
        mock_svc_instance = MagicMock()
        mock_svc_instance.download_from_gcs.return_value = {"accuracy": 0.95, "version": "1.0"}
        client.cloud_service = mock_svc_instance
        result = client.get_metadata("cefi_btc_v1", "2024-01")
        assert result == {"accuracy": 0.95, "version": "1.0"}

    @patch("unified_domain_client.clients.ml.get_storage_client")
    @patch("unified_domain_client.clients.ml.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.ml.UnifiedCloudConfig")
    def test_ml_models_list_models_returns_empty_on_error(
        self, mock_config: MagicMock, mock_svc: MagicMock, mock_storage: MagicMock
    ):
        """Test list_models returns empty list on error."""
        from unified_domain_client.clients.ml import MLModelsDomainClient  # noqa: deep-import

        mock_config.return_value.gcp_project_id = "proj"
        mock_storage.side_effect = OSError("not found")
        client = MLModelsDomainClient(project_id="proj", storage_bucket="ml-bucket")
        result = client.list_models()
        assert result == []

    @patch("unified_domain_client.clients.ml.get_storage_client")
    @patch("unified_domain_client.clients.ml.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.ml.UnifiedCloudConfig")
    def test_ml_predictions_get_available_dates_returns_empty_on_error(
        self, mock_config: MagicMock, mock_svc: MagicMock, mock_storage: MagicMock
    ):
        """Test get_available_dates returns empty list on error."""
        from unified_domain_client.clients.ml import MLPredictionsDomainClient  # noqa: deep-import

        mock_config.return_value.gcp_project_id = "proj"
        mock_storage.side_effect = OSError("not found")
        client = MLPredictionsDomainClient(project_id="proj", storage_bucket="pred-bucket")
        result = client.get_available_dates("batch")
        assert result == []


class TestPnLDomainClientMethods:
    """Test PnLDomainClient additional methods."""

    @patch("unified_domain_client.clients.pnl.get_storage_client")
    @patch("unified_domain_client.clients.pnl.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.pnl.UnifiedCloudConfig")
    def test_get_available_strategies_returns_empty_on_error(
        self, mock_config: MagicMock, mock_svc: MagicMock, mock_storage: MagicMock
    ):
        """Test get_available_strategies returns empty list on error."""
        from unified_domain_client.clients.pnl import PnLDomainClient  # noqa: deep-import

        mock_config.return_value.gcp_project_id = "proj"
        mock_storage.side_effect = OSError("not found")
        client = PnLDomainClient(project_id="proj", storage_bucket="pnl-bucket")
        result = client.get_available_strategies()
        assert result == []


class TestPositionsAndRiskDomainClients:
    """Test PositionsDomainClient and RiskDomainClient methods."""

    @patch("unified_domain_client.clients.positions.get_storage_client")
    @patch("unified_domain_client.clients.positions.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.positions.UnifiedCloudConfig")
    def test_positions_get_returns_empty_on_error(
        self, mock_config: MagicMock, mock_svc: MagicMock, mock_storage: MagicMock
    ):
        """Test PositionsDomainClient.get_positions returns empty on error."""
        from unified_domain_client.clients.positions import (  # noqa: deep-import
            PositionsDomainClient,
        )

        mock_config.return_value.gcp_project_id = "proj"
        mock_storage_instance = MagicMock()
        mock_storage_instance.download_bytes.side_effect = OSError("not found")
        mock_storage.return_value = mock_storage_instance
        client = PositionsDomainClient(project_id="proj", storage_bucket="positions-bucket")
        result = client.get_positions("2024-01-15", "ACC-001", "eod")
        assert isinstance(result, pd.DataFrame)

    @patch("unified_domain_client.clients.positions.get_storage_client")
    @patch("unified_domain_client.clients.positions.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.positions.UnifiedCloudConfig")
    def test_positions_get_available_accounts_returns_empty_on_error(
        self, mock_config: MagicMock, mock_svc: MagicMock, mock_storage: MagicMock
    ):
        """Test PositionsDomainClient.get_available_accounts returns empty on error."""
        from unified_domain_client.clients.positions import (  # noqa: deep-import
            PositionsDomainClient,
        )

        mock_config.return_value.gcp_project_id = "proj"
        mock_storage.side_effect = OSError("not found")
        client = PositionsDomainClient(project_id="proj", storage_bucket="positions-bucket")
        result = client.get_available_accounts()
        assert result == []

    @patch("unified_domain_client.clients.risk.get_storage_client")
    @patch("unified_domain_client.clients.risk.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.risk.UnifiedCloudConfig")
    def test_risk_get_metrics_returns_empty_on_error(
        self, mock_config: MagicMock, mock_svc: MagicMock, mock_storage: MagicMock
    ):
        """Test RiskDomainClient.get_risk_metrics returns empty on error."""
        from unified_domain_client.clients.risk import RiskDomainClient  # noqa: deep-import

        mock_config.return_value.gcp_project_id = "proj"
        mock_storage_instance = MagicMock()
        mock_storage_instance.download_bytes.side_effect = OSError("not found")
        mock_storage.return_value = mock_storage_instance
        client = RiskDomainClient(project_id="proj", storage_bucket="risk-bucket")
        result = client.get_risk_metrics("2024-01-15", "var")
        assert isinstance(result, pd.DataFrame)

    @patch("unified_domain_client.clients.risk.get_storage_client")
    @patch("unified_domain_client.clients.risk.StandardizedDomainCloudService")
    @patch("unified_domain_client.clients.risk.UnifiedCloudConfig")
    def test_risk_get_available_risk_types_returns_empty_on_error(
        self, mock_config: MagicMock, mock_svc: MagicMock, mock_storage: MagicMock
    ):
        """Test RiskDomainClient.get_available_risk_types returns empty on error."""
        from unified_domain_client.clients.risk import RiskDomainClient  # noqa: deep-import

        mock_config.return_value.gcp_project_id = "proj"
        mock_storage.side_effect = OSError("not found")
        client = RiskDomainClient(project_id="proj", storage_bucket="risk-bucket")
        result = client.get_available_risk_types()
        assert result == []
