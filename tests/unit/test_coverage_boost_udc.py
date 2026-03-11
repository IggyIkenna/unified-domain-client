"""Coverage boost tests for unified-domain-client.

Targets uncovered code in:
- clients/instruments.py (_to_upper_list, _is_empty_or_na, _apply_filters,
  _filter_by_date_availability, _optional_coverage_stats)
- clients/execution.py (backtest methods, error paths)
- clients/market_data.py (_build_tick_gcs_path, error paths)
- clients/liquidity.py (list_instrument_keys, error paths)
- data_completion.py (_extract_date_from_blob, check_completion)
- catalog/ (bq_catalog, glue_catalog init)
- sports/ clients
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pandas as pd

# ---------------------------------------------------------------------------
# instruments.py pure utility functions
# ---------------------------------------------------------------------------


class TestToUpperList:
    def test_string_input(self):
        from unified_domain_client.clients.instruments import _to_upper_list

        assert _to_upper_list("btc,eth") == ["BTC", "ETH"]

    def test_list_input(self):
        from unified_domain_client.clients.instruments import _to_upper_list

        assert _to_upper_list(["btc", "ETH"]) == ["BTC", "ETH"]

    def test_single_string(self):
        from unified_domain_client.clients.instruments import _to_upper_list

        assert _to_upper_list("BTC") == ["BTC"]


class TestIsEmptyOrNa:
    def test_none(self):
        from unified_domain_client.clients.instruments import _is_empty_or_na

        assert _is_empty_or_na(None)

    def test_nan(self):
        from unified_domain_client.clients.instruments import _is_empty_or_na

        assert _is_empty_or_na(float("nan"))

    def test_empty_string(self):
        from unified_domain_client.clients.instruments import _is_empty_or_na

        assert _is_empty_or_na("")

    def test_whitespace_string(self):
        from unified_domain_client.clients.instruments import _is_empty_or_na

        assert _is_empty_or_na("   ")

    def test_non_empty(self):
        from unified_domain_client.clients.instruments import _is_empty_or_na

        assert not _is_empty_or_na("BTC")
        assert not _is_empty_or_na(0)
        assert not _is_empty_or_na(1.0)


class TestInstrumentsDomainClientFilters:
    def _make_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "venue": ["BINANCE", "OKX", "BINANCE"],
                "instrument_type": ["PERPETUAL", "SPOT", "SPOT"],
                "base_asset": ["BTC", "ETH", "BTC"],
                "quote_asset": ["USDT", "USDT", "USDC"],
                "symbol": ["BTC-USDT", "ETH-USDT", "BTC-USDC"],
                "instrument_key": [
                    "BINANCE:PERPETUAL:BTC-USDT",
                    "OKX:SPOT:ETH-USDT",
                    "BINANCE:SPOT:BTC-USDC",
                ],
                "available_from_datetime": [
                    "2020-01-01T00:00:00Z",
                    "2020-01-01T00:00:00Z",
                    "2025-01-01T00:00:00Z",
                ],
                "available_to_datetime": [None, "2099-12-31T00:00:00Z", None],
            }
        )

    def _make_client(self):
        from unified_domain_client import (
            InstrumentsDomainClient,
        )

        with patch("unified_domain_client.clients.instruments.UnifiedCloudConfig") as mock_cfg:
            mock_cfg.return_value.instruments_gcs_bucket = "test-bucket"
            with patch("unified_domain_client.standardized_service.StandardizedDomainCloudService"):
                return InstrumentsDomainClient(storage_bucket="test-bucket")

    def test_apply_filters_by_venue(self):
        client = self._make_client()
        df = self._make_df()
        result = client._apply_filters(df, venue="BINANCE")
        assert len(result) == 2
        assert all(result["venue"] == "BINANCE")

    def test_apply_filters_by_instrument_type(self):
        client = self._make_client()
        df = self._make_df()
        result = client._apply_filters(df, instrument_type="PERPETUAL")
        assert len(result) == 1

    def test_apply_filters_by_base_currency(self):
        client = self._make_client()
        df = self._make_df()
        result = client._apply_filters(df, base_currency="ETH")
        assert len(result) == 1

    def test_apply_filters_by_quote_currency(self):
        client = self._make_client()
        df = self._make_df()
        result = client._apply_filters(df, quote_currency="USDC")
        assert len(result) == 1

    def test_apply_filters_by_symbol_pattern(self):
        client = self._make_client()
        df = self._make_df()
        result = client._apply_filters(df, symbol_pattern="^BTC")
        assert len(result) == 2

    def test_apply_filters_by_instrument_ids_string(self):
        client = self._make_client()
        df = self._make_df()
        result = client._apply_filters(df, instrument_ids="BINANCE:PERPETUAL:BTC-USDT")
        assert len(result) == 1

    def test_apply_filters_by_instrument_ids_list(self):
        client = self._make_client()
        df = self._make_df()
        result = client._apply_filters(
            df, instrument_ids=["BINANCE:PERPETUAL:BTC-USDT", "OKX:SPOT:ETH-USDT"]
        )
        assert len(result) == 2

    def test_apply_filters_invalid_regex(self):
        client = self._make_client()
        df = self._make_df()
        # Invalid regex should not raise, just return all
        result = client._apply_filters(df, symbol_pattern="[invalid")
        assert len(result) == len(df)

    def test_filter_by_date_availability_empty_df(self):
        client = self._make_client()
        result = client._filter_by_date_availability(
            pd.DataFrame(), datetime(2024, 1, 1, tzinfo=UTC)
        )
        assert result.empty

    def test_filter_by_date_availability_with_from_filter(self):
        client = self._make_client()
        df = self._make_df()
        # Only rows with available_from_datetime <= 2024-01-01 should be included
        result = client._filter_by_date_availability(df, datetime(2024, 1, 1, tzinfo=UTC))
        assert len(result) == 2

    def test_filter_by_date_availability_with_to_filter(self):
        client = self._make_client()
        df = pd.DataFrame(
            {
                "available_from_datetime": ["2020-01-01T00:00:00Z"],
                "available_to_datetime": ["2022-01-01T00:00:00Z"],
            }
        )
        # Target date 2024 > 2022 to_date — should be excluded
        result = client._filter_by_date_availability(df, datetime(2024, 1, 1, tzinfo=UTC))
        assert len(result) == 0

    def test_optional_coverage_stats_no_extra_columns(self):
        client = self._make_client()
        df = pd.DataFrame({"instrument_key": ["K1", "K2"]})
        result = client._optional_coverage_stats(df)
        assert isinstance(result, dict)

    def test_get_instruments_for_date_returns_empty_on_error(self):
        client = self._make_client()
        with patch.object(
            client, "_load_and_filter_for_date", side_effect=OSError("network error")
        ):
            result = client.get_instruments_for_date("2024-01-01")
        assert result.empty

    def test_get_instruments_date_range_empty(self):
        client = self._make_client()
        with patch.object(client, "get_instruments_for_date", return_value=pd.DataFrame()):
            result = client.get_instruments_date_range("2024-01-01", "2024-01-02")
        assert result.empty

    def test_load_and_filter_empty_instruments(self):
        client = self._make_client()
        with patch.object(client, "_load_instruments_by_venue", return_value=pd.DataFrame()):
            result = client._load_and_filter_for_date(
                "2024-01-01", datetime(2024, 1, 1, tzinfo=UTC), None
            )
        assert result.empty


# ---------------------------------------------------------------------------
# execution.py
# ---------------------------------------------------------------------------


class TestExecutionDomainClientMethods:
    def _make_client(self):
        from unified_domain_client import (
            ExecutionDomainClient,
        )

        with patch("unified_domain_client.clients.execution.UnifiedCloudConfig") as mock_cfg:
            mock_cfg.return_value.gcp_project_id = "test-proj"
            mock_cfg.return_value.execution_gcs_bucket = "exec-bucket"
            with patch("unified_domain_client.standardized_service.StandardizedDomainCloudService"):
                return ExecutionDomainClient(storage_bucket="exec-bucket", project_id="test-proj")

    def test_get_fills_returns_empty_on_error(self):
        client = self._make_client()
        with patch("unified_domain_client.clients.execution.get_storage_client") as mock_gsc:
            mock_gsc.return_value.download_bytes.side_effect = OSError("not found")
            result = client.get_fills("2024-01-01")
        assert result.empty

    def test_get_orders_returns_empty_on_error(self):
        client = self._make_client()
        with patch("unified_domain_client.clients.execution.get_storage_client") as mock_gsc:
            mock_gsc.return_value.download_bytes.side_effect = OSError("not found")
            result = client.get_orders("2024-01-01")
        assert result.empty

    def test_get_nautilus_catalog_path(self):
        client = self._make_client()
        path = client.get_nautilus_catalog_path("BTC-USDT")
        assert "BTC-USDT" in path
        assert path.startswith("gs://")

    def test_get_backtest_summary_returns_empty_on_error(self):
        client = self._make_client()
        client.cloud_service = MagicMock()
        client.cloud_service.download_from_gcs.side_effect = OSError("not found")
        result = client.get_backtest_summary("run-001")
        assert result == {}

    def test_get_backtest_fills_returns_empty_on_error(self):
        client = self._make_client()
        client.cloud_service = MagicMock()
        client.cloud_service.download_from_gcs.side_effect = OSError("not found")
        result = client.get_backtest_fills("run-001")
        assert result.empty

    def test_get_backtest_orders_returns_empty_on_error(self):
        client = self._make_client()
        client.cloud_service = MagicMock()
        client.cloud_service.download_from_gcs.side_effect = OSError("not found")
        result = client.get_backtest_orders("run-001")
        assert result.empty

    def test_get_backtest_positions_returns_empty_on_error(self):
        client = self._make_client()
        client.cloud_service = MagicMock()
        client.cloud_service.download_from_gcs.side_effect = OSError("not found")
        result = client.get_backtest_positions("run-001")
        assert result.empty

    def test_get_equity_curve_returns_empty_on_error(self):
        client = self._make_client()
        client.cloud_service = MagicMock()
        client.cloud_service.download_from_gcs.side_effect = OSError("not found")
        result = client.get_equity_curve("run-001")
        assert result.empty

    def test_list_backtest_runs_returns_empty_on_error(self):
        client = self._make_client()
        with patch("unified_domain_client.clients.execution.get_storage_client") as mock_gsc:
            mock_gsc.return_value.list_blobs.side_effect = OSError("not found")
            result = client.list_backtest_runs()
        assert result == []

    def test_get_backtest_summary_returns_dict_on_success(self):
        client = self._make_client()
        client.cloud_service = MagicMock()
        client.cloud_service.download_from_gcs.return_value = {"total_pnl": 100.0}
        result = client.get_backtest_summary("run-001")
        assert result["total_pnl"] == 100.0

    def test_create_execution_client_factory(self):
        from unified_domain_client import (
            create_execution_client,
        )

        with patch("unified_domain_client.clients.execution.UnifiedCloudConfig") as mock_cfg:
            mock_cfg.return_value.gcp_project_id = "proj"
            mock_cfg.return_value.execution_gcs_bucket = "bucket"
            with patch("unified_domain_client.standardized_service.StandardizedDomainCloudService"):
                client = create_execution_client(storage_bucket="bucket")
        assert client is not None


# ---------------------------------------------------------------------------
# market_data.py: _build_tick_gcs_path
# ---------------------------------------------------------------------------


class TestMarketTickDataDomainClientPaths:
    def _make_client(self):
        from unified_domain_client import (
            MarketTickDataDomainClient,
        )

        with patch("unified_domain_client.clients.market_data.UnifiedCloudConfig") as mock_cfg:
            mock_cfg.return_value.market_data_gcs_bucket = "mkt-bucket"
            with patch("unified_domain_client.standardized_service.StandardizedDomainCloudService"):
                return MarketTickDataDomainClient(storage_bucket="mkt-bucket")

    def test_build_tick_path_perpetuals_with_venue(self):
        client = self._make_client()
        path = client._build_tick_gcs_path(
            "2024-01-01", "BINANCE:PERPETUAL:BTC-USDT", "trades", None, "BINANCE", None
        )
        assert "perpetuals" in path
        assert "BINANCE" in path
        assert "BTC-USDT" in path

    def test_build_tick_path_with_hour(self):
        client = self._make_client()
        path = client._build_tick_gcs_path(
            "2024-01-01", "BINANCE:SPOT:ETH-USDT", "trades", 5, "BINANCE", None
        )
        assert "hour=05" in path

    def test_build_tick_path_no_type_folder(self):
        client = self._make_client()
        path = client._build_tick_gcs_path("2024-01-01", "UNKNOWN", "trades", None, None, None)
        assert "UNKNOWN.parquet" in path

    def test_build_tick_path_explicit_type_folder(self):
        client = self._make_client()
        path = client._build_tick_gcs_path(
            "2024-01-01", "X:Y:Z", "trades", None, None, "custom_type"
        )
        assert "custom_type" in path

    def test_get_tick_data_error_path_returns_empty(self):
        client = self._make_client()
        client.cloud_service = MagicMock()
        client.cloud_service.download_from_gcs.side_effect = OSError("not found")
        result = client.get_tick_data(datetime(2024, 1, 1, tzinfo=UTC), "BTC-USDT")
        assert result.empty

    def test_get_tick_data_range_empty(self):
        client = self._make_client()
        client.cloud_service = MagicMock()
        client.cloud_service.download_from_gcs.side_effect = OSError("not found")
        result = client.get_tick_data_range(
            datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, tzinfo=UTC), "BTC-USDT"
        )
        assert result.empty

    def test_get_candles_range_empty(self):
        from unified_domain_client import (
            MarketCandleDataDomainClient,
        )

        with patch("unified_domain_client.clients.market_data.UnifiedCloudConfig") as mock_cfg:
            mock_cfg.return_value.market_data_gcs_bucket = "mkt-bucket"
            with patch("unified_domain_client.standardized_service.StandardizedDomainCloudService"):
                client = MarketCandleDataDomainClient(storage_bucket="mkt-bucket")
        client.cloud_service = MagicMock()
        client.cloud_service.download_from_gcs.side_effect = OSError("not found")
        result = client.get_candles_range(
            datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, tzinfo=UTC), "BTC-USDT"
        )
        assert result.empty


# ---------------------------------------------------------------------------
# data_completion.py
# ---------------------------------------------------------------------------


class TestDataCompletionChecker:
    def _make_checker(self):
        from unified_trading_library.domain.data_completion import (
            DataCompletionChecker,
        )

        with patch("unified_trading_library.domain.data_completion.get_storage_client"):
            return DataCompletionChecker(
                bucket="test-bucket", path_pattern="data/{date}/file.parquet"
            )

    def test_extract_date_from_blob_valid_date(self):
        checker = self._make_checker()
        blob = MagicMock()
        blob.name = "data/2024-01-15/file.parquet"
        result = checker._extract_date_from_blob(blob, datetime(2024, 1, 1), datetime(2024, 1, 31))
        assert result == "2024-01-15"

    def test_extract_date_from_blob_out_of_range(self):
        checker = self._make_checker()
        blob = MagicMock()
        blob.name = "data/2023-12-31/file.parquet"
        result = checker._extract_date_from_blob(blob, datetime(2024, 1, 1), datetime(2024, 1, 31))
        assert result is None

    def test_extract_date_from_blob_no_date(self):
        checker = self._make_checker()
        blob = MagicMock()
        blob.name = "data/no-date-here/file.parquet"
        result = checker._extract_date_from_blob(blob, datetime(2024, 1, 1), datetime(2024, 1, 31))
        assert result is None


# ---------------------------------------------------------------------------
# liquidity.py
# ---------------------------------------------------------------------------


class TestL2BookCheckpointClient:
    def _make_client(self):
        from unified_domain_client import (
            L2BookCheckpointClient,
        )

        mock_storage = MagicMock()
        mock_config = MagicMock()
        mock_config.gcp_project_id = "test-proj"
        return L2BookCheckpointClient(storage_client=mock_storage, config=mock_config)

    def test_list_instrument_keys_basic(self):
        client = self._make_client()
        client._list_blobs = MagicMock(
            return_value=[
                "l2_book/2024-01-01/venue=BINANCE/instrument_key=BTC_USDT.parquet",
                "l2_book/2024-01-01/venue=BINANCE/instrument_key=ETH_USDT.parquet",
            ]
        )
        # Override to return just file names as the method expects
        client._list_blobs = MagicMock(
            return_value=[
                "prefix/instrument_key=BTC_USDT.parquet",
                "prefix/instrument_key=ETH_USDT.parquet",
                "prefix/other_file.txt",
            ]
        )
        result = client.list_instrument_keys("2024-01-01", "BINANCE")
        assert sorted(result) == ["BTC_USDT", "ETH_USDT"]

    def test_get_checkpoints_calls_read_parquet(self):
        client = self._make_client()
        mock_df = pd.DataFrame({"ts": [1, 2]})
        client._read_parquet = MagicMock(return_value=mock_df)
        result = client.get_checkpoints("2024-01-01", "BINANCE", "BINANCE:PERPETUAL:BTC-USDT")
        assert not result.empty


# ---------------------------------------------------------------------------
# catalog/ modules
# ---------------------------------------------------------------------------


class TestCatalogModules:
    def test_bq_catalog_instantiate(self):
        from unified_domain_client.catalog.bq_catalog import BigQueryCatalog

        c = BigQueryCatalog()
        assert c is not None

    def test_glue_catalog_instantiate(self):
        from unified_domain_client.catalog.glue_catalog import GlueCatalog

        c = GlueCatalog()
        assert c is not None


# ---------------------------------------------------------------------------
# sports/ clients
# ---------------------------------------------------------------------------


class TestSportsDomainClients:
    def _make_fixtures_client(self):
        from unified_domain_client import (
            SportsFixturesDomainClient,
        )

        with patch("unified_domain_client.sports.fixtures_client.UnifiedCloudConfig") as mock_cfg:
            mock_cfg.return_value.gcp_project_id = "test-proj"
            mock_cfg.return_value.sports_gcs_bucket = "sports-bucket"
            with patch("unified_domain_client.standardized_service.StandardizedDomainCloudService"):
                return SportsFixturesDomainClient(storage_bucket="sports-bucket")

    def _make_odds_client(self):
        from unified_domain_client import (
            SportsOddsDomainClient,
        )

        with patch("unified_domain_client.sports.odds_client.UnifiedCloudConfig") as mock_cfg:
            mock_cfg.return_value.gcp_project_id = "test-proj"
            mock_cfg.return_value.sports_gcs_bucket = "sports-bucket"
            with patch("unified_domain_client.standardized_service.StandardizedDomainCloudService"):
                return SportsOddsDomainClient(storage_bucket="sports-bucket")

    def _make_tick_client(self):
        from unified_domain_client import (
            SportsTickDataDomainClient,
        )

        with patch("unified_domain_client.sports.tick_data_client.UnifiedCloudConfig") as mock_cfg:
            mock_cfg.return_value.gcp_project_id = "test-proj"
            mock_cfg.return_value.sports_gcs_bucket = "sports-bucket"
            with patch("unified_domain_client.standardized_service.StandardizedDomainCloudService"):
                return SportsTickDataDomainClient(storage_bucket="sports-bucket")

    def test_sports_fixtures_client_read_error(self):
        client = self._make_fixtures_client()
        client.cloud_service = MagicMock()
        client.cloud_service.download_from_gcs.side_effect = OSError("not found")
        result = client.read_fixtures("2024-25", "epl", "2024-01-01")
        assert result.empty

    def test_sports_fixtures_client_get_available_dates_error(self):
        client = self._make_fixtures_client()
        with patch("unified_domain_client.sports.fixtures_client.get_storage_client") as mock_gsc:
            mock_gsc.return_value.list_blobs.side_effect = OSError("not found")
            result = client.get_available_dates("2024-25", "epl")
        assert result == []

    def test_sports_odds_client_read_error(self):
        client = self._make_odds_client()
        client.cloud_service = MagicMock()
        client.cloud_service.download_from_gcs.side_effect = OSError("not found")
        result = client.read_odds("betfair", "epl", "2024-01-01")
        assert result.empty

    def test_sports_odds_client_get_available_dates_error(self):
        client = self._make_odds_client()
        with patch("unified_domain_client.sports.odds_client.get_storage_client") as mock_gsc:
            mock_gsc.return_value.list_blobs.side_effect = OSError("not found")
            result = client.get_available_dates("betfair", "epl")
        assert result == []

    def test_sports_tick_data_client_read_error(self):
        client = self._make_tick_client()
        client.cloud_service = MagicMock()
        client.cloud_service.download_from_gcs.side_effect = OSError("not found")
        result = client.read_ticks("BETFAIR", "2024-01-01")
        assert result.empty

    def test_sports_tick_data_client_get_available_dates_error(self):
        client = self._make_tick_client()
        with patch("unified_domain_client.sports.tick_data_client.get_storage_client") as mock_gsc:
            mock_gsc.return_value.list_blobs.side_effect = OSError("not found")
            result = client.get_available_dates("BETFAIR")
        assert result == []
