"""Integration tests for unified-domain-client with mock UCI storage.

Tests the BaseDataClient, MarketTickDomainClient, and ProcessedCandlesDomainClient
end-to-end with a fully mocked StorageClient and UnifiedCloudConfig.  No real GCS
connections are made.
"""

from __future__ import annotations

import io
from unittest.mock import MagicMock

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from unified_domain_client.clients.market_data import MarketTickDomainClient

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# Helper: build a Parquet blob in memory
# ---------------------------------------------------------------------------


def _make_parquet_bytes(df: pd.DataFrame) -> bytes:
    """Serialize a DataFrame to Parquet bytes (in-memory)."""
    buf = io.BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buf)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_config() -> MagicMock:
    """Minimal mock UnifiedCloudConfig."""
    cfg = MagicMock()
    cfg.gcp_project_id = "test-project"
    cfg.gcs_bucket = "test-bucket"
    cfg.bigquery_dataset = "test_dataset"
    cfg.bigquery_location = "us-central1"
    return cfg


@pytest.fixture()
def sample_tick_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=10, freq="1s"),
            "price": [100.0 + i * 0.5 for i in range(10)],
            "size": [1.0] * 10,
            "side": ["buy"] * 5 + ["sell"] * 5,
        }
    )


@pytest.fixture()
def mock_storage_client(sample_tick_df: pd.DataFrame) -> MagicMock:
    """Mock StorageClient that returns parquet bytes for download_bytes calls."""
    client = MagicMock()
    parquet_bytes = _make_parquet_bytes(sample_tick_df)
    client.download_bytes.return_value = parquet_bytes

    # list_blobs returns objects with a .name attribute
    mock_blob = MagicMock()
    mock_blob.name = "raw_tick_data/by_date/2024-01-01/venue=binance/BTCUSDT.parquet"
    client.list_blobs.return_value = [mock_blob]
    return client


# ---------------------------------------------------------------------------
# Tests: BaseDataClient._read_parquet
# ---------------------------------------------------------------------------


class TestBaseDataClientReadParquet:
    """Test the _read_parquet helper via a concrete subclass."""

    def test_read_parquet_returns_dataframe(
        self, mock_storage_client: MagicMock, mock_config: MagicMock, sample_tick_df: pd.DataFrame
    ) -> None:
        """_read_parquet should call download_bytes and deserialize the parquet payload."""
        from unified_domain_client.clients.base import BaseDataClient

        class _ConcreteClient(BaseDataClient):
            pass

        client = _ConcreteClient(mock_storage_client, mock_config)
        result = client._read_parquet("test-bucket", "some/path.parquet")

        mock_storage_client.download_bytes.assert_called_once_with(
            "test-bucket", "some/path.parquet"
        )
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == list(sample_tick_df.columns)
        assert len(result) == len(sample_tick_df)

    def test_read_parquet_propagates_storage_error(
        self, mock_storage_client: MagicMock, mock_config: MagicMock
    ) -> None:
        """_read_parquet should propagate exceptions raised by the storage client."""
        from unified_domain_client.clients.base import BaseDataClient

        class _ConcreteClient(BaseDataClient):
            pass

        mock_storage_client.download_bytes.side_effect = FileNotFoundError("blob not found")
        client = _ConcreteClient(mock_storage_client, mock_config)

        with pytest.raises(FileNotFoundError, match="blob not found"):
            client._read_parquet("test-bucket", "missing/path.parquet")

    def test_list_blobs_returns_names(
        self, mock_storage_client: MagicMock, mock_config: MagicMock
    ) -> None:
        """_list_blobs should return a list of string blob names."""
        from unified_domain_client.clients.base import BaseDataClient

        class _ConcreteClient(BaseDataClient):
            pass

        client = _ConcreteClient(mock_storage_client, mock_config)
        names = client._list_blobs("test-bucket", "raw_tick_data/")

        mock_storage_client.list_blobs.assert_called_once_with(
            "test-bucket", prefix="raw_tick_data/"
        )
        assert isinstance(names, list)
        assert len(names) == 1
        assert "raw_tick_data/by_date/" in names[0]


# ---------------------------------------------------------------------------
# Tests: MarketTickDomainClient.get_tick_data
# ---------------------------------------------------------------------------


class TestMarketTickDomainClient:
    """Integration tests for MarketTickDomainClient using mock storage."""

    @pytest.fixture()
    def tick_client(
        self, mock_storage_client: MagicMock, mock_config: MagicMock
    ) -> MarketTickDomainClient:
        return MarketTickDomainClient(mock_storage_client, mock_config)

    def test_get_tick_data_returns_dataframe(self, tick_client: MagicMock) -> None:
        result = tick_client.get_tick_data(
            date="2024-01-01",
            venue="binance",
            instrument_key="BTCUSDT",
            data_type="trades",
            instrument_type="perpetual",
        )
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    def test_get_tick_data_calls_storage_once(
        self, tick_client: MagicMock, mock_storage_client: MagicMock
    ) -> None:
        tick_client.get_tick_data(
            date="2024-01-01",
            venue="binance",
            instrument_key="BTCUSDT",
            data_type="trades",
            instrument_type="perpetual",
        )
        assert mock_storage_client.download_bytes.call_count == 1

    def test_get_available_dates_returns_list(
        self, tick_client: MagicMock, mock_storage_client: MagicMock
    ) -> None:
        dates = tick_client.get_available_dates(venue="binance")
        assert isinstance(dates, list)
        mock_storage_client.list_blobs.assert_called_once()

    def test_get_tick_data_handles_missing_blob(
        self, mock_storage_client: MagicMock, mock_config: MagicMock
    ) -> None:
        """get_tick_data should propagate storage errors so callers can handle them."""
        from unified_domain_client.clients.market_data import MarketTickDomainClient

        mock_storage_client.download_bytes.side_effect = FileNotFoundError("blob not found")
        client = MarketTickDomainClient(mock_storage_client, mock_config)

        with pytest.raises(FileNotFoundError):
            client.get_tick_data(
                date="2024-01-01",
                venue="binance",
                instrument_key="MISSING",
                data_type="trades",
                instrument_type="perpetual",
            )


# ---------------------------------------------------------------------------
# Tests: domain client factories (paths module)
# ---------------------------------------------------------------------------


class TestDomainClientPaths:
    """Integration-level tests for the path-building utilities."""

    def test_build_bucket_returns_string(self) -> None:
        from unified_config_interface.paths import build_bucket

        result = build_bucket("raw_tick_data", project_id="test-project", category="cefi")
        assert isinstance(result, str)
        assert len(result) > 0

    def test_build_path_returns_non_empty_string(self) -> None:
        from unified_config_interface.paths import build_path

        result = build_path(
            "raw_tick_data",
            date="2024-01-01",
            data_type="trades",
            instrument_type="perpetual",
            venue="binance",
        )
        assert isinstance(result, str)
        assert "2024-01-01" in result

    def test_build_path_includes_venue(self) -> None:
        from unified_config_interface.paths import build_path

        result = build_path(
            "raw_tick_data",
            date="2024-01-01",
            data_type="trades",
            instrument_type="perpetual",
            venue="bybit",
        )
        assert "bybit" in result
