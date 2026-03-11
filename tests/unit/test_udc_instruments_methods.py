"""Coverage boost tests for unified-domain-client — instruments additional and catalog methods.

Continuation of test_coverage_boost_udc.py.
Targets: InstrumentsDomainClientAdditionalMethods, DataCompletionAdditional,
CatalogMethodCoverage, TestInitLazyImports.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

# ---------------------------------------------------------------------------
# instruments.py: additional method coverage
# ---------------------------------------------------------------------------


class TestInstrumentsDomainClientAdditionalMethods:
    def _make_client(self):
        from unified_domain_client import (
            InstrumentsDomainClient,
        )

        with patch("unified_domain_client.clients.instruments.UnifiedCloudConfig") as mock_cfg:
            mock_cfg.return_value.instruments_gcs_bucket = "test-bucket"
            with patch("unified_domain_client.standardized_service.StandardizedDomainCloudService"):
                return InstrumentsDomainClient(storage_bucket="test-bucket")

    def _make_df_with_data_types(self) -> pd.DataFrame:
        import pandas as pd

        return pd.DataFrame(
            {
                "venue": ["BINANCE", "OKX"],
                "instrument_type": ["PERPETUAL", "SPOT"],
                "base_asset": ["BTC", "ETH"],
                "quote_asset": ["USDT", "USDT"],
                "symbol": ["BTC-USDT", "ETH-USDT"],
                "instrument_key": ["BINANCE:PERPETUAL:BTC-USDT", "OKX:SPOT:ETH-USDT"],
                "data_types": ["trades,book_snapshot_5", "trades"],
            }
        )

    def test_get_instruments_by_data_type_with_data(self):
        client = self._make_client()
        df = self._make_df_with_data_types()
        client.get_instruments_for_date = MagicMock(return_value=df)
        result = client.get_instruments_by_data_type("2024-01-01", "book_snapshot_5")
        assert len(result) == 1
        assert result.iloc[0]["instrument_key"] == "BINANCE:PERPETUAL:BTC-USDT"

    def test_get_instruments_by_data_type_returns_empty_when_no_instruments(self):
        client = self._make_client()
        client.get_instruments_for_date = MagicMock(return_value=pd.DataFrame())
        result = client.get_instruments_by_data_type("2024-01-01", "trades")
        assert result.empty

    def test_get_instruments_by_data_type_no_data_types_column(self):
        client = self._make_client()
        df = pd.DataFrame({"instrument_key": ["K1"]})
        client.get_instruments_for_date = MagicMock(return_value=df)
        result = client.get_instruments_by_data_type("2024-01-01", "trades")
        assert result.empty

    def test_get_instruments_by_data_type_respects_limit(self):
        client = self._make_client()
        df = pd.DataFrame(
            {
                "instrument_key": [f"K{i}" for i in range(10)],
                "data_types": ["trades"] * 10,
            }
        )
        client.get_instruments_for_date = MagicMock(return_value=df)
        result = client.get_instruments_by_data_type("2024-01-01", "trades", limit=3)
        assert len(result) == 3

    def test_search_instruments_by_symbol(self):
        client = self._make_client()
        # search_instruments_by_symbol delegates filtering to get_instruments_for_date
        # Return a single-row df to simulate that pattern filtering was applied
        filtered_df = pd.DataFrame(
            {
                "venue": ["BINANCE"],
                "instrument_type": ["PERPETUAL"],
                "base_asset": ["BTC"],
                "quote_asset": ["USDT"],
                "symbol": ["BTC-USDT"],
                "instrument_key": ["BINANCE:PERPETUAL:BTC-USDT"],
                "data_types": ["trades"],
            }
        )
        client.get_instruments_for_date = MagicMock(return_value=filtered_df)
        result = client.search_instruments_by_symbol("2024-01-01", "^BTC")
        assert len(result) == 1

    def test_search_instruments_by_symbol_respects_limit(self):
        client = self._make_client()
        df = pd.DataFrame(
            {
                "venue": ["BINANCE"] * 10,
                "instrument_type": ["SPOT"] * 10,
                "base_asset": ["BTC"] * 10,
                "quote_asset": ["USDT"] * 10,
                "symbol": [f"BTC-USDT-{i}" for i in range(10)],
                "instrument_key": [f"K{i}" for i in range(10)],
            }
        )
        client.get_instruments_for_date = MagicMock(return_value=df)
        result = client.search_instruments_by_symbol("2024-01-01", "^BTC", limit=3)
        assert len(result) == 3

    def test_get_expiring_instruments_empty_when_no_instruments(self):
        client = self._make_client()
        client.get_instruments_for_date = MagicMock(return_value=pd.DataFrame())
        result = client.get_expiring_instruments("2024-01-01")
        assert result.empty

    def test_get_expiring_instruments_no_available_to_column(self):
        client = self._make_client()
        df = pd.DataFrame({"instrument_key": ["K1"]})
        client.get_instruments_for_date = MagicMock(return_value=df)
        result = client.get_expiring_instruments("2024-01-01")
        assert result.empty

    def test_get_expiring_instruments_within_days(self):

        client = self._make_client()
        df = pd.DataFrame(
            {
                "instrument_key": ["K1", "K2"],
                "available_to_datetime": [
                    "2024-01-15T00:00:00Z",  # expires within 30 days of 2024-01-01
                    "2025-12-31T00:00:00Z",  # far future
                ],
            }
        )
        client.get_instruments_for_date = MagicMock(return_value=df)
        result = client.get_expiring_instruments("2024-01-01", days_until_expiry=30)
        assert len(result) == 1
        assert result.iloc[0]["instrument_key"] == "K1"

    def test_get_expiring_instruments_all_notna_empty(self):
        client = self._make_client()
        df = pd.DataFrame(
            {
                "instrument_key": ["K1"],
                "available_to_datetime": [None],
            }
        )
        client.get_instruments_for_date = MagicMock(return_value=df)
        result = client.get_expiring_instruments("2024-01-01")
        assert result.empty

    def test_optional_coverage_stats_with_ccxt_column(self):
        client = self._make_client()
        df = pd.DataFrame(
            {
                "instrument_key": ["K1", "K2"],
                "ccxt_symbol": ["BTCUSDT", ""],
            }
        )
        result = client._optional_coverage_stats(df)
        assert "ccxt_coverage" in result
        assert result["ccxt_coverage"]["instruments_with_ccxt"] == 1

    def test_optional_coverage_stats_with_data_types_column(self):
        client = self._make_client()
        df = pd.DataFrame(
            {
                "instrument_key": ["K1", "K2"],
                "data_types": ["trades,book_snapshot_5", "trades"],
            }
        )
        result = client._optional_coverage_stats(df)
        assert "data_type_coverage" in result
        assert result["data_type_coverage"]["trades"] == 2
        assert result["data_type_coverage"]["book_snapshot_5"] == 1

    def test_get_instrument_details_returns_none_when_empty(self):
        client = self._make_client()
        client.get_instruments_for_date = MagicMock(return_value=pd.DataFrame())
        result = client.get_instrument_details("2024-01-01", "K1")
        assert result is None

    def test_get_instrument_details_returns_dict_when_found(self):
        client = self._make_client()
        df = pd.DataFrame({"instrument_key": ["K1"], "venue": ["BINANCE"]})
        client.get_instruments_for_date = MagicMock(return_value=df)
        result = client.get_instrument_details("2024-01-01", "K1")
        assert result is not None
        assert result["instrument_key"] == "K1"

    def test_get_instruments_date_range_with_data(self):
        client = self._make_client()
        df = pd.DataFrame(
            {
                "instrument_key": ["K1", "K2"],
                "venue": ["BINANCE", "OKX"],
            }
        )
        client.get_instruments_for_date = MagicMock(return_value=df)
        result = client.get_instruments_date_range("2024-01-01", "2024-01-02")
        # Two days x 2 instruments, but deduped by instrument_key
        assert len(result) == 2
        assert "query_date" in result.columns

    def test_get_instruments_for_date_with_datetime_object(self):
        from datetime import UTC, datetime

        client = self._make_client()
        client._load_and_filter_for_date = MagicMock(return_value=pd.DataFrame())
        result = client.get_instruments_for_date(datetime(2024, 1, 1, tzinfo=UTC))
        assert result.empty

    def test_get_summary_stats_with_ccxt_and_data_types(self):
        client = self._make_client()
        df = pd.DataFrame(
            {
                "venue": ["BINANCE"],
                "instrument_type": ["PERPETUAL"],
                "base_asset": ["BTC"],
                "quote_asset": ["USDT"],
                "ccxt_symbol": ["BTCUSDT"],
                "data_types": ["trades,book_snapshot_5"],
            }
        )
        client.get_instruments_for_date = MagicMock(return_value=df)
        result = client.get_summary_stats("2024-01-01")
        assert result["total_instruments"] == 1
        assert "ccxt_coverage" in result
        assert "data_type_coverage" in result


# ---------------------------------------------------------------------------
# data_completion.py: additional coverage
# ---------------------------------------------------------------------------


class TestDataCompletionAdditional:
    def _make_checker(self):
        from unified_trading_library.domain.data_completion import (
            DataCompletionChecker,
        )

        with patch("unified_trading_library.domain.data_completion.get_storage_client") as mock_gsc:
            mock_gsc.return_value = MagicMock()
            checker = DataCompletionChecker(
                bucket="test-bucket", path_pattern="data/{instrument}/{date}/file.parquet"
            )
            checker._client = mock_gsc.return_value
            return checker

    def test_get_completed_dates_returns_set(self):
        checker = self._make_checker()
        blob1 = MagicMock()
        blob1.name = "data/BTC/2024-01-15/file.parquet"
        blob2 = MagicMock()
        blob2.name = "data/BTC/2024-01-16/file.parquet"
        blob3 = MagicMock()
        blob3.name = "data/BTC/2023-12-01/file.parquet"  # out of range
        checker._client.bucket.return_value.list_blobs.return_value = [blob1, blob2, blob3]
        result = checker.get_completed_dates("2024-01-01", "2024-01-31", instrument="BTC")
        assert "2024-01-15" in result
        assert "2024-01-16" in result
        assert "2023-12-01" not in result

    def test_get_completed_dates_no_instrument(self):
        checker = self._make_checker()
        blob = MagicMock()
        blob.name = "data/2024-01-10/file.parquet"
        checker._client.bucket.return_value.list_blobs.return_value = [blob]
        result = checker.get_completed_dates("2024-01-01", "2024-01-31")
        assert "2024-01-10" in result

    def test_is_date_complete_true(self):
        checker = self._make_checker()
        checker._client.bucket.return_value.blob.return_value.exists.return_value = True
        result = checker.is_date_complete("2024-01-15")
        assert result is True

    def test_is_date_complete_false(self):
        checker = self._make_checker()
        checker._client.bucket.return_value.blob.return_value.exists.return_value = False
        result = checker.is_date_complete("2024-01-15")
        assert result is False

    def test_is_date_complete_with_instrument(self):
        checker = self._make_checker()
        checker._client.bucket.return_value.blob.return_value.exists.return_value = True
        result = checker.is_date_complete("2024-01-15", instrument="BTC")
        assert result is True

    def test_get_missing_dates_returns_sorted_list(self):
        checker = self._make_checker()
        # Only 2024-01-01 is completed
        blob = MagicMock()
        blob.name = "data/BTC/2024-01-01/file.parquet"
        checker._client.bucket.return_value.list_blobs.return_value = [blob]
        result = checker.get_missing_dates("2024-01-01", "2024-01-03")
        assert "2024-01-02" in result
        assert "2024-01-03" in result
        assert "2024-01-01" not in result
        assert result == sorted(result)

    def test_make_completion_checker_factory(self):
        from unified_domain_client import (
            make_completion_checker,
        )

        with patch("unified_trading_library.domain.data_completion.get_storage_client"):
            checker = make_completion_checker(
                bucket="b", path_pattern="data/{date}/f.parquet", dataset_name="x", project_id="p"
            )
        assert checker.bucket == "b"
        assert checker.path_pattern == "data/{date}/f.parquet"

    def test_get_available_date_range_returns_none_when_no_dates(self):
        from unified_domain_client import (
            get_available_date_range,
        )

        with patch("unified_trading_library.domain.data_completion.get_storage_client") as mock_gsc:
            mock_gsc.return_value.bucket.return_value.list_blobs.return_value = []
            result = get_available_date_range("bucket", "prefix/")
        assert result == (None, None)

    def test_get_available_date_range_returns_min_max(self):
        from unified_domain_client import (
            get_available_date_range,
        )

        blob1 = MagicMock()
        blob1.name = "prefix/2024-01-05/file.parquet"
        blob2 = MagicMock()
        blob2.name = "prefix/2024-01-01/file.parquet"
        blob3 = MagicMock()
        blob3.name = "prefix/no-date-here/file.parquet"

        with patch("unified_trading_library.domain.data_completion.get_storage_client") as mock_gsc:
            mock_gsc.return_value.bucket.return_value.list_blobs.return_value = [
                blob1,
                blob2,
                blob3,
            ]
            result = get_available_date_range("bucket", "prefix/")
        assert result == ("2024-01-01", "2024-01-05")


# ---------------------------------------------------------------------------
# catalog: method coverage
# ---------------------------------------------------------------------------


class TestCatalogMethodCoverage:
    def test_bq_catalog_create_external_table(self):
        from unified_domain_client.catalog.bq_catalog import BigQueryCatalog

        catalog = BigQueryCatalog()
        ddl = catalog.create_external_table(
            dataset_name="raw_tick_data",
            project_id="my-project",
            category="cefi",
            bq_dataset="my_dataset",
        )
        assert "CREATE OR REPLACE EXTERNAL TABLE" in ddl
        assert "my-project" in ddl
        assert "PARQUET" in ddl

    def test_glue_catalog_create_table(self):
        from unified_domain_client.catalog.glue_catalog import GlueCatalog

        catalog = GlueCatalog()
        result = catalog.create_table(
            dataset_name="raw_tick_data",
            account_id="123456789",
            category="cefi",
        )
        assert "DatabaseName" in result
        assert "TableInput" in result
        table_input = result["TableInput"]
        assert isinstance(table_input, dict)
        assert table_input["TableType"] == "EXTERNAL_TABLE"


# ---------------------------------------------------------------------------
# __init__.py lazy import coverage
# ---------------------------------------------------------------------------


class TestInitLazyImports:
    def test_lazy_import_cloud_data_provider_base(self):
        import unified_domain_client as udc

        cls = udc.CloudDataProviderBase
        assert cls is not None

    def test_lazy_import_instruments_data_provider(self):
        import unified_domain_client as udc

        cls = udc.InstrumentsDataProvider
        assert cls is not None

    def test_lazy_import_market_data_provider(self):
        import unified_domain_client as udc

        cls = udc.MarketDataProvider
        assert cls is not None

    def test_lazy_import_features_data_provider(self):
        import unified_domain_client as udc

        cls = udc.FeaturesDataProvider
        assert cls is not None

    def test_lazy_import_standardized_service(self):
        import unified_domain_client as udc

        cls = udc.StandardizedDomainCloudService
        assert cls is not None

    def test_lazy_import_unknown_attr_raises(self):
        import unified_domain_client as udc

        try:
            _ = udc.NonExistentClass  # type: ignore[attr-defined]
            assert False, "Should have raised AttributeError"
        except AttributeError:
            pass

    def test_lazy_import_create_backtesting_cloud_service(self):
        import unified_domain_client as udc

        fn = udc.create_backtesting_cloud_service
        assert callable(fn)
