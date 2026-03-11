"""Tests for unified_domain_client readers — base, market data, features, ML, direct."""

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
