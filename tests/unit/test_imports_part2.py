"""Test package imports and basic exports — part 2 (standardized service, clients, writers, readers)."""

import pytest

# ===========================================================================
# standardized_service — download/upload method coverage
# ===========================================================================


def test_standardized_service_download_raises_on_error():
    """Test download_from_gcs re-raises on connection error."""
    from unittest.mock import patch

    from unified_domain_client.standardized_service import StandardizedDomainCloudService

    svc = StandardizedDomainCloudService(domain="test", bucket="b")

    with patch("unified_domain_client.standardized_service.download_from_storage") as mock_dl:
        mock_dl.side_effect = OSError("network error")
        with pytest.raises(OSError):
            svc.download_from_gcs("some/path.parquet", format="parquet")


def test_standardized_service_download_json_format():
    """Test download_from_gcs with json format."""
    import json
    from unittest.mock import patch

    from unified_domain_client.standardized_service import StandardizedDomainCloudService

    svc = StandardizedDomainCloudService(domain="test", bucket="b")

    with patch("unified_domain_client.standardized_service.download_from_storage") as mock_dl:
        mock_dl.return_value = json.dumps({"key": "value"}).encode()
        result = svc.download_from_gcs("some/path.json", format="json")
        assert isinstance(result, dict)
        assert result["key"] == "value"


def test_standardized_service_download_unknown_format():
    """Test download_from_gcs with unknown format returns empty DataFrame."""
    from unittest.mock import patch

    import pandas as pd

    from unified_domain_client.standardized_service import StandardizedDomainCloudService

    svc = StandardizedDomainCloudService(domain="test", bucket="b")

    with patch("unified_domain_client.standardized_service.download_from_storage") as mock_dl:
        mock_dl.return_value = b"data"
        result = svc.download_from_gcs("some/path.txt", format="unknown_format")
        assert isinstance(result, pd.DataFrame)


def test_standardized_service_upload_parquet():
    """Test upload_to_gcs with parquet format."""
    from unittest.mock import patch

    import pandas as pd

    from unified_domain_client.standardized_service import StandardizedDomainCloudService

    svc = StandardizedDomainCloudService(domain="test", bucket="b")
    df = pd.DataFrame({"x": [1, 2]})

    with patch("unified_domain_client.standardized_service.upload_to_storage") as mock_ul:
        mock_ul.return_value = "gs://b/some/path.parquet"
        result = svc.upload_to_gcs(df, "some/path.parquet", format="parquet")
        assert result == "gs://b/some/path.parquet"
        mock_ul.assert_called_once()


def test_standardized_service_upload_csv():
    """Test upload_to_gcs with csv format."""
    from unittest.mock import patch

    import pandas as pd

    from unified_domain_client.standardized_service import StandardizedDomainCloudService

    svc = StandardizedDomainCloudService(domain="test", bucket="b")
    df = pd.DataFrame({"x": [1, 2]})

    with patch("unified_domain_client.standardized_service.upload_to_storage") as mock_ul:
        mock_ul.return_value = "gs://b/some/path.csv"
        result = svc.upload_to_gcs(df, "some/path.csv", format="csv")
        assert result == "gs://b/some/path.csv"


def test_standardized_service_upload_unsupported_format_raises():
    """Test upload_to_gcs with unsupported format raises ValueError."""
    import pandas as pd

    from unified_domain_client.standardized_service import StandardizedDomainCloudService

    svc = StandardizedDomainCloudService(domain="test", bucket="b")
    df = pd.DataFrame({"x": [1]})
    with pytest.raises(ValueError, match="Unsupported format"):
        svc.upload_to_gcs(df, "path.xyz", format="xml")


# ===========================================================================
# sports clients — method-level smoke tests via cloud_service injection
# ===========================================================================


def test_sports_features_read_features_returns_empty_on_error():
    """Test SportsFeaturesDomainClient.read_features returns empty on error."""
    from unittest.mock import MagicMock, patch

    import pandas as pd

    from unified_domain_client.sports.features_client import SportsFeaturesDomainClient

    with patch("unified_domain_client.sports.features_client.UnifiedCloudConfig") as mock_cfg, \
         patch("unified_domain_client.sports.features_client.StandardizedDomainCloudService"):
        mock_cfg.return_value.gcp_project_id = "test-project"
        client = SportsFeaturesDomainClient(project_id="test-project", gcs_bucket="sports-bucket")

    mock_svc = MagicMock()
    mock_svc.download_from_gcs.side_effect = OSError("not found")
    client.cloud_service = mock_svc
    result = client.read_features(horizon="1d", date="2024-01-15", league="epl")
    assert isinstance(result, pd.DataFrame)


def test_sports_features_write_features():
    """Test SportsFeaturesDomainClient.write_features calls upload."""
    from unittest.mock import MagicMock, patch

    import pandas as pd

    from unified_domain_client.sports.features_client import SportsFeaturesDomainClient

    with patch("unified_domain_client.sports.features_client.UnifiedCloudConfig") as mock_cfg, \
         patch("unified_domain_client.sports.features_client.StandardizedDomainCloudService"):
        mock_cfg.return_value.gcp_project_id = "test-project"
        client = SportsFeaturesDomainClient(project_id="test-project", gcs_bucket="sports-bucket")

    mock_svc = MagicMock()
    mock_svc.upload_to_gcs.return_value = "gs://sports-bucket/path"
    client.cloud_service = mock_svc
    df = pd.DataFrame({"feature": [1.0]})
    result = client.write_features(df, horizon="1d", date="2024-01-15", league="epl")
    assert result == "gs://sports-bucket/path"


def test_sports_fixtures_read_fixtures_returns_empty_on_error():
    """Test SportsFixturesDomainClient.read_fixtures returns empty on error."""
    from unittest.mock import MagicMock, patch

    import pandas as pd

    from unified_domain_client.sports.fixtures_client import SportsFixturesDomainClient

    with patch("unified_domain_client.sports.fixtures_client.UnifiedCloudConfig") as mock_cfg, \
         patch("unified_domain_client.sports.fixtures_client.StandardizedDomainCloudService"):
        mock_cfg.return_value.gcp_project_id = "test-project"
        client = SportsFixturesDomainClient(project_id="test-project", gcs_bucket="sports-bucket")

    mock_svc = MagicMock()
    mock_svc.download_from_gcs.side_effect = OSError("not found")
    client.cloud_service = mock_svc
    result = client.read_fixtures(season="2024", date="2024-01-15", league="epl")
    assert isinstance(result, pd.DataFrame)


def test_sports_odds_read_odds_returns_empty_on_error():
    """Test SportsOddsDomainClient.read_odds returns empty on error."""
    from unittest.mock import MagicMock, patch

    import pandas as pd

    from unified_domain_client.sports.odds_client import SportsOddsDomainClient

    with patch("unified_domain_client.sports.odds_client.UnifiedCloudConfig") as mock_cfg, \
         patch("unified_domain_client.sports.odds_client.StandardizedDomainCloudService"):
        mock_cfg.return_value.gcp_project_id = "test-project"
        client = SportsOddsDomainClient(project_id="test-project", gcs_bucket="sports-bucket")

    mock_svc = MagicMock()
    mock_svc.download_from_gcs.side_effect = OSError("not found")
    client.cloud_service = mock_svc
    result = client.read_odds(provider="bet365", date="2024-01-15", league="epl")
    assert isinstance(result, pd.DataFrame)


def test_sports_tick_data_read_ticks_returns_empty_on_error():
    """Test SportsTickDataDomainClient.read_ticks returns empty on error."""
    from unittest.mock import MagicMock, patch

    import pandas as pd

    from unified_domain_client.sports.tick_data_client import SportsTickDataDomainClient

    with patch("unified_domain_client.sports.tick_data_client.UnifiedCloudConfig") as mock_cfg, \
         patch("unified_domain_client.sports.tick_data_client.StandardizedDomainCloudService"):
        mock_cfg.return_value.gcp_project_id = "test-project"
        client = SportsTickDataDomainClient(project_id="test-project", gcs_bucket="sports-bucket")

    mock_svc = MagicMock()
    mock_svc.download_from_gcs.side_effect = OSError("not found")
    client.cloud_service = mock_svc
    result = client.read_ticks(venue="betfair", date="2024-01-15")
    assert isinstance(result, pd.DataFrame)


def test_sports_mappings_read_mappings_returns_empty_on_error():
    """Test SportsMappingsDomainClient.read_mappings returns empty on error."""
    from unittest.mock import MagicMock, patch

    import pandas as pd

    from unified_domain_client.sports.mappings_client import SportsMappingsDomainClient

    with patch("unified_domain_client.sports.mappings_client.UnifiedCloudConfig") as mock_cfg, \
         patch("unified_domain_client.sports.mappings_client.StandardizedDomainCloudService"):
        mock_cfg.return_value.gcp_project_id = "test-project"
        client = SportsMappingsDomainClient(project_id="test-project", gcs_bucket="sports-bucket")

    mock_svc = MagicMock()
    mock_svc.download_from_gcs.side_effect = OSError("not found")
    client.cloud_service = mock_svc
    result = client.read_mappings(entity_type="player")
    assert isinstance(result, pd.DataFrame)


# ===========================================================================
# clients/base — BaseDataClient coverage
# ===========================================================================


def test_base_data_client_read_parquet_returns_dataframe():
    """Test BaseDataClient._read_parquet returns DataFrame."""
    import io
    from unittest.mock import MagicMock

    import pandas as pd

    from unified_domain_client.clients.market_data import MarketTickDomainClient

    mock_storage = MagicMock()
    mock_config = MagicMock()
    mock_config.gcp_project_id = "proj"

    # Create a parquet bytes payload
    buf = io.BytesIO()
    pd.DataFrame({"x": [1, 2]}).to_parquet(buf, index=False)
    buf.seek(0)
    mock_storage.download_bytes.return_value = buf.read()

    client = MarketTickDomainClient(storage_client=mock_storage, config=mock_config)
    result = client._read_parquet("bucket", "path/to/data.parquet")
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2


def test_base_data_client_list_blobs_returns_names():
    """Test BaseDataClient._list_blobs returns list of names."""
    from unittest.mock import MagicMock

    from unified_domain_client.clients.market_data import MarketTickDomainClient

    mock_storage = MagicMock()
    mock_config = MagicMock()
    mock_config.gcp_project_id = "proj"

    blob1 = MagicMock()
    blob1.name = "path/to/file1.parquet"
    blob2 = MagicMock()
    blob2.name = "path/to/file2.parquet"
    mock_storage.list_blobs.return_value = [blob1, blob2]

    client = MarketTickDomainClient(storage_client=mock_storage, config=mock_config)
    result = client._list_blobs("bucket", "path/to/")
    assert result == ["path/to/file1.parquet", "path/to/file2.parquet"]


def test_market_tick_domain_client_get_tick_data():
    """Test MarketTickDomainClient.get_tick_data calls storage."""
    import io
    from unittest.mock import MagicMock

    import pandas as pd

    from unified_domain_client.clients.market_data import MarketTickDomainClient

    mock_storage = MagicMock()
    mock_config = MagicMock()
    mock_config.gcp_project_id = "proj"

    buf = io.BytesIO()
    pd.DataFrame({"ts": [1]}).to_parquet(buf, index=False)
    buf.seek(0)
    mock_storage.download_bytes.return_value = buf.read()

    client = MarketTickDomainClient(storage_client=mock_storage, config=mock_config)
    result = client.get_tick_data(
        date="2024-01-15",
        venue="BINANCE",
        instrument_key="BTCUSDT",
        data_type="trades",
        instrument_type="perpetual",
    )
    assert isinstance(result, pd.DataFrame)


def test_market_tick_domain_client_get_available_dates():
    """Test MarketTickDomainClient.get_available_dates returns dates."""
    from unittest.mock import MagicMock

    from unified_domain_client.clients.market_data import MarketTickDomainClient

    mock_storage = MagicMock()
    mock_config = MagicMock()
    mock_config.gcp_project_id = "proj"

    blob = MagicMock()
    blob.name = "raw_tick_data/by_date/day=2024-01-15/venue=BINANCE/data.parquet"
    mock_storage.list_blobs.return_value = [blob]

    client = MarketTickDomainClient(storage_client=mock_storage, config=mock_config)
    result = client.get_available_dates("BINANCE")
    assert "2024-01-15" in result


def test_market_candle_domain_client_get_candles():
    """Test MarketCandleDomainClient.get_candles calls storage."""
    import io
    from unittest.mock import MagicMock

    import pandas as pd

    from unified_domain_client.clients.market_data import MarketCandleDomainClient

    mock_storage = MagicMock()
    mock_config = MagicMock()
    mock_config.gcp_project_id = "proj"

    buf = io.BytesIO()
    pd.DataFrame({"close": [100.0]}).to_parquet(buf, index=False)
    buf.seek(0)
    mock_storage.download_bytes.return_value = buf.read()

    client = MarketCandleDomainClient(storage_client=mock_storage, config=mock_config)
    result = client.get_candles(
        date="2024-01-15",
        venue="BINANCE",
        instrument_id="BTCUSDT",
        timeframe="1h",
        data_type="trades",
        instrument_type="perpetual",
    )
    assert isinstance(result, pd.DataFrame)


def test_market_candle_domain_client_get_available_timeframes():
    """Test MarketCandleDomainClient.get_available_timeframes returns timeframes."""
    from unittest.mock import MagicMock

    from unified_domain_client.clients.market_data import MarketCandleDomainClient

    mock_storage = MagicMock()
    mock_config = MagicMock()
    mock_config.gcp_project_id = "proj"

    blob = MagicMock()
    blob.name = "processed_candles/by_date/day=2024-01-15/timeframe=1h/venue=BINANCE/data.parquet"
    mock_storage.list_blobs.return_value = [blob]

    client = MarketCandleDomainClient(storage_client=mock_storage, config=mock_config)
    result = client.get_available_timeframes("BINANCE")
    assert "1h" in result


# ===========================================================================
# instrument_date_filter — DateFilterService coverage
# ===========================================================================


def test_validate_config_is_callable():
    """Test validate_config can be imported and is callable."""
    from unified_domain_client import validate_config

    assert callable(validate_config)


# ===========================================================================
# writers — base, direct, factory coverage
# ===========================================================================


def test_base_writer_write_parquet():
    """Test BaseWriter.write_parquet uploads parquet data."""
    from unittest.mock import patch

    import pandas as pd

    from unified_domain_client.writers.base import BaseWriter

    writer = BaseWriter(bucket="b")
    df = pd.DataFrame({"x": [1, 2]})

    with patch("unified_domain_client.writers.base.upload_to_storage") as mock_upload:
        mock_upload.return_value = "gs://b/some/path.parquet"
        result = writer.write_parquet(df, "some/path.parquet")
        assert result == "gs://b/some/path.parquet"
        mock_upload.assert_called_once()


def test_base_writer_write_json():
    """Test BaseWriter.write_json uploads JSON data."""
    from unittest.mock import patch

    from unified_domain_client.writers.base import BaseWriter

    writer = BaseWriter(bucket="b")

    with patch("unified_domain_client.writers.base.upload_to_storage") as mock_upload:
        mock_upload.return_value = "gs://b/path.json"
        result = writer.write_json({"key": "value"}, "path.json")
        assert result == "gs://b/path.json"


def test_market_data_writer_write_tick():
    """Test MarketDataWriter.write_tick generates correct path."""
    from unittest.mock import patch

    import pandas as pd

    from unified_domain_client.writers.base import MarketDataWriter

    writer = MarketDataWriter(bucket="b")
    df = pd.DataFrame({"ts": [1000]})

    with patch("unified_domain_client.writers.base.upload_to_storage") as mock_upload:
        mock_upload.return_value = "gs://b/path"
        result = writer.write_tick(df, instrument="BTC-USDT", date="2024-01-15")
        assert result == "gs://b/path"
        # Verify path contains expected values
        call_args = mock_upload.call_args
        assert "2024-01-15" in call_args[0][1]


def test_features_writer_write_delta_one():
    """Test FeaturesWriter.write_delta_one generates correct path."""
    from unittest.mock import patch

    import pandas as pd

    from unified_domain_client.writers.base import FeaturesWriter

    writer = FeaturesWriter(bucket="b")
    df = pd.DataFrame({"feature": [1.0]})

    with patch("unified_domain_client.writers.base.upload_to_storage") as mock_upload:
        mock_upload.return_value = "gs://b/path"
        result = writer.write_delta_one(df, instrument="BTC-USDT", date="2024-01-15")
        assert result == "gs://b/path"


def test_ml_writer_write_predictions():
    """Test MLWriter.write_predictions generates correct path."""
    from unittest.mock import patch

    import pandas as pd

    from unified_domain_client.writers.base import MLWriter

    writer = MLWriter(bucket="b")
    df = pd.DataFrame({"prediction": [0.9]})

    with patch("unified_domain_client.writers.base.upload_to_storage") as mock_upload:
        mock_upload.return_value = "gs://b/path"
        result = writer.write_predictions(df, instrument="BTC-USDT", date="2024-01-15")
        assert result == "gs://b/path"


def test_direct_writer_write():
    """Test DirectWriter.write calls storage.upload_bytes."""
    from unittest.mock import MagicMock

    import pandas as pd

    from unified_domain_client.writers.direct import DirectWriter

    mock_storage = MagicMock()
    writer = DirectWriter(storage_client=mock_storage)
    df = pd.DataFrame({"x": [1, 2]})
    writer.write(df, "my-bucket", "path/data.parquet")
    mock_storage.upload_bytes.assert_called_once()


def test_direct_writer_write_json():
    """Test DirectWriter.write_json calls storage.upload_bytes."""
    from unittest.mock import MagicMock

    from unified_domain_client.writers.direct import DirectWriter

    mock_storage = MagicMock()
    writer = DirectWriter(storage_client=mock_storage)
    writer.write_json({"key": "value"}, "my-bucket", "path/data.json")
    mock_storage.upload_bytes.assert_called_once()


def test_get_writer_without_storage_raises():
    """Test get_writer raises ValueError when storage_client is None."""
    from unified_domain_client.writers.factory import get_writer

    with pytest.raises(ValueError, match="storage_client required"):
        get_writer("some_dataset", storage_client=None)


def test_get_writer_with_storage_returns_direct_writer():
    """Test get_writer returns DirectWriter when storage_client provided."""
    from unittest.mock import MagicMock

    from unified_domain_client.writers.direct import DirectWriter
    from unified_domain_client.writers.factory import get_writer

    mock_storage = MagicMock()
    writer = get_writer("some_dataset", storage_client=mock_storage)
    assert isinstance(writer, DirectWriter)


# ===========================================================================
# sports clients write/available_dates methods
# ===========================================================================


def test_sports_features_get_available_dates_returns_empty_on_error():
    """Test SportsFeaturesDomainClient.get_available_dates returns empty list on error."""
    from unittest.mock import patch

    from unified_domain_client.sports.features_client import SportsFeaturesDomainClient

    with patch("unified_domain_client.sports.features_client.UnifiedCloudConfig") as mock_cfg, \
         patch("unified_domain_client.sports.features_client.StandardizedDomainCloudService"), \
         patch("unified_domain_client.sports.features_client.get_storage_client") as mock_storage:
        mock_cfg.return_value.gcp_project_id = "test-project"
        mock_storage.side_effect = OSError("not found")
        client = SportsFeaturesDomainClient(project_id="test-project", gcs_bucket="sports-bucket")
        result = client.get_available_dates(horizon="1d", league="epl")
        assert result == []


def test_sports_fixtures_write_fixtures():
    """Test SportsFixturesDomainClient.write_fixtures calls upload."""
    from unittest.mock import MagicMock, patch

    import pandas as pd

    from unified_domain_client.sports.fixtures_client import SportsFixturesDomainClient

    with patch("unified_domain_client.sports.fixtures_client.UnifiedCloudConfig") as mock_cfg, \
         patch("unified_domain_client.sports.fixtures_client.StandardizedDomainCloudService"):
        mock_cfg.return_value.gcp_project_id = "test-project"
        client = SportsFixturesDomainClient(project_id="test-project", gcs_bucket="sports-bucket")

    mock_svc = MagicMock()
    mock_svc.upload_to_gcs.return_value = "gs://sports-bucket/fixtures"
    client.cloud_service = mock_svc
    df = pd.DataFrame({"fixture": [1]})
    result = client.write_fixtures(df, season="2024", date="2024-01-15", league="epl")
    assert result == "gs://sports-bucket/fixtures"


def test_sports_odds_write_odds():
    """Test SportsOddsDomainClient.write_odds calls upload."""
    from unittest.mock import MagicMock, patch

    import pandas as pd

    from unified_domain_client.sports.odds_client import SportsOddsDomainClient

    with patch("unified_domain_client.sports.odds_client.UnifiedCloudConfig") as mock_cfg, \
         patch("unified_domain_client.sports.odds_client.StandardizedDomainCloudService"):
        mock_cfg.return_value.gcp_project_id = "test-project"
        client = SportsOddsDomainClient(project_id="test-project", gcs_bucket="sports-bucket")

    mock_svc = MagicMock()
    mock_svc.upload_to_gcs.return_value = "gs://sports-bucket/odds"
    client.cloud_service = mock_svc
    df = pd.DataFrame({"odds": [2.5]})
    result = client.write_odds(df, provider="bet365", date="2024-01-15", league="epl")
    assert result == "gs://sports-bucket/odds"


def test_sports_tick_data_write_ticks():
    """Test SportsTickDataDomainClient.write_ticks calls upload."""
    from unittest.mock import MagicMock, patch

    import pandas as pd

    from unified_domain_client.sports.tick_data_client import SportsTickDataDomainClient

    with patch("unified_domain_client.sports.tick_data_client.UnifiedCloudConfig") as mock_cfg, \
         patch("unified_domain_client.sports.tick_data_client.StandardizedDomainCloudService"):
        mock_cfg.return_value.gcp_project_id = "test-project"
        client = SportsTickDataDomainClient(project_id="test-project", gcs_bucket="sports-bucket")

    mock_svc = MagicMock()
    mock_svc.upload_to_gcs.return_value = "gs://sports-bucket/ticks"
    client.cloud_service = mock_svc
    df = pd.DataFrame({"tick": [1]})
    result = client.write_ticks(df, venue="betfair", date="2024-01-15")
    assert result == "gs://sports-bucket/ticks"


def test_sports_mappings_write_mappings():
    """Test SportsMappingsDomainClient.write_mappings calls upload."""
    from unittest.mock import MagicMock, patch

    import pandas as pd

    from unified_domain_client.sports.mappings_client import SportsMappingsDomainClient

    with patch("unified_domain_client.sports.mappings_client.UnifiedCloudConfig") as mock_cfg, \
         patch("unified_domain_client.sports.mappings_client.StandardizedDomainCloudService"):
        mock_cfg.return_value.gcp_project_id = "test-project"
        client = SportsMappingsDomainClient(project_id="test-project", gcs_bucket="sports-bucket")

    mock_svc = MagicMock()
    mock_svc.upload_to_gcs.return_value = "gs://sports-bucket/mappings"
    client.cloud_service = mock_svc
    df = pd.DataFrame({"id": [1]})
    result = client.write_mappings(df, entity_type="player")
    assert result == "gs://sports-bucket/mappings"


def test_sports_mappings_has_cloud_service():
    """Test SportsMappingsDomainClient has cloud_service attribute."""
    from unittest.mock import patch

    from unified_domain_client.sports.mappings_client import SportsMappingsDomainClient

    with patch("unified_domain_client.sports.mappings_client.UnifiedCloudConfig") as mock_cfg, \
         patch("unified_domain_client.sports.mappings_client.StandardizedDomainCloudService"):
        mock_cfg.return_value.gcp_project_id = "test-project"
        client = SportsMappingsDomainClient(project_id="test-project", gcs_bucket="sports-bucket")
        assert hasattr(client, "cloud_service")


# ===========================================================================
# readers/base and readers/direct — coverage
# ===========================================================================


def test_base_reader_read_parquet():
    """Test BaseReader.read_parquet downloads and deserializes parquet."""
    import io
    from unittest.mock import patch

    import pandas as pd

    from unified_domain_client.readers.base import BaseReader

    reader = BaseReader(bucket="b")

    buf = io.BytesIO()
    pd.DataFrame({"x": [1]}).to_parquet(buf, index=False)
    buf.seek(0)

    with patch("unified_domain_client.readers.base.download_from_storage") as mock_dl:
        mock_dl.return_value = buf.read()
        result = reader.read_parquet("some/path.parquet")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1


def test_base_reader_read_json():
    """Test BaseReader.read_json downloads and parses JSON."""
    import json
    from unittest.mock import patch

    from unified_domain_client.readers.base import BaseReader

    reader = BaseReader(bucket="b")

    with patch("unified_domain_client.readers.base.download_from_storage") as mock_dl:
        mock_dl.return_value = json.dumps({"key": "val"}).encode()
        result = reader.read_json("some/path.json")
        assert result == {"key": "val"}


def test_base_reader_exists():
    """Test BaseReader.exists checks if file exists."""
    from unittest.mock import patch

    from unified_domain_client.readers.base import BaseReader

    reader = BaseReader(bucket="b")

    with patch("unified_domain_client.readers.base.storage_exists") as mock_exists:
        mock_exists.return_value = True
        assert reader.exists("some/path.parquet") is True
        mock_exists.return_value = False
        assert reader.exists("missing.parquet") is False


def test_market_data_reader_read_tick():
    """Test MarketDataReader.read_tick calls read_parquet."""
    import io
    from unittest.mock import patch

    import pandas as pd

    from unified_domain_client.readers.base import MarketDataReader

    reader = MarketDataReader(bucket="b")
    buf = io.BytesIO()
    pd.DataFrame({"ts": [1]}).to_parquet(buf, index=False)
    buf.seek(0)

    with patch("unified_domain_client.readers.base.download_from_storage") as mock_dl:
        mock_dl.return_value = buf.read()
        result = reader.read_tick("BTC-USDT", "2024-01-15")
        assert isinstance(result, pd.DataFrame)


def test_market_data_reader_read_candles_timeframes():
    """Test MarketDataReader.read_candles for various timeframes."""
    import io
    from unittest.mock import patch

    import pandas as pd

    from unified_domain_client.readers.base import MarketDataReader

    reader = MarketDataReader(bucket="b")
    buf = io.BytesIO()
    pd.DataFrame({"close": [100.0]}).to_parquet(buf, index=False)

    for timeframe in ["1m", "1h", "24h", "4h"]:
        buf.seek(0)
        with patch("unified_domain_client.readers.base.download_from_storage") as mock_dl:
            mock_dl.return_value = buf.read()
            result = reader.read_candles("BTC-USDT", "2024-01-15", timeframe=timeframe)
            assert isinstance(result, pd.DataFrame)


def test_features_reader_read_delta_one():
    """Test FeaturesReader.read_delta_one calls read_parquet."""
    import io
    from unittest.mock import patch

    import pandas as pd

    from unified_domain_client.readers.base import FeaturesReader

    reader = FeaturesReader(bucket="b")
    buf = io.BytesIO()
    pd.DataFrame({"feature": [1.0]}).to_parquet(buf, index=False)
    buf.seek(0)

    with patch("unified_domain_client.readers.base.download_from_storage") as mock_dl:
        mock_dl.return_value = buf.read()
        result = reader.read_delta_one("BTC-USDT", "2024-01-15")
        assert isinstance(result, pd.DataFrame)


def test_ml_reader_read_predictions():
    """Test MLReader.read_predictions calls read_parquet."""
    import io
    from unittest.mock import patch

    import pandas as pd

    from unified_domain_client.readers.base import MLReader

    reader = MLReader(bucket="b")
    buf = io.BytesIO()
    pd.DataFrame({"pred": [0.9]}).to_parquet(buf, index=False)
    buf.seek(0)

    with patch("unified_domain_client.readers.base.download_from_storage") as mock_dl:
        mock_dl.return_value = buf.read()
        result = reader.read_predictions("BTC-USDT", "2024-01-15")
        assert isinstance(result, pd.DataFrame)


def test_direct_reader_read():
    """Test DirectReader.read downloads and deserializes parquet."""
    import io
    from unittest.mock import MagicMock

    import pandas as pd

    from unified_domain_client.readers.direct import DirectReader

    mock_storage = MagicMock()
    buf = io.BytesIO()
    pd.DataFrame({"x": [1]}).to_parquet(buf, index=False)
    buf.seek(0)
    mock_storage.download_bytes.return_value = buf.read()

    reader = DirectReader(storage_client=mock_storage)
    result = reader.read("bucket", "path/data.parquet")
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1


def test_direct_reader_list_available():
    """Test DirectReader.list_available returns blob names."""
    from unittest.mock import MagicMock

    from unified_domain_client.readers.direct import DirectReader

    mock_storage = MagicMock()
    blob1 = MagicMock()
    blob1.name = "path/file1.parquet"
    blob2 = MagicMock()
    blob2.name = "path/file2.parquet"
    mock_storage.list_blobs.return_value = [blob1, blob2]

    reader = DirectReader(storage_client=mock_storage)
    result = reader.list_available("bucket", "path/")
    assert result == ["path/file1.parquet", "path/file2.parquet"]
