"""Base reader classes for GCS data access."""

# pyright: reportAny=false
# json.loads() returns Any per typeshed; this is a stdlib limitation.

import io
import json
import logging
from abc import ABC, abstractmethod

import pandas as pd
from unified_cloud_interface import download_from_storage, storage_exists

from unified_domain_client.cloud_target import CloudTarget
from unified_domain_client.paths import PathRegistry

logger = logging.getLogger(__name__)


class BaseDataReader(ABC):
    """Abstract base for StorageClient-based readers."""

    @abstractmethod
    def read(self, bucket: str, path: str) -> pd.DataFrame:
        """Read a dataset partition into a DataFrame."""

    @abstractmethod
    def list_available(self, bucket: str, prefix: str) -> list[str]:
        """List available blob paths under a prefix."""


class BaseReader:
    """Base class for reading data from GCS."""

    def __init__(self, cloud_target: CloudTarget) -> None:
        self.cloud_target = cloud_target

    def read_parquet(self, gcs_path: str) -> pd.DataFrame:
        """Read parquet file from GCS path (relative to bucket)."""
        data = download_from_storage(self.cloud_target.gcs_bucket, gcs_path.lstrip("/"))
        return pd.read_parquet(io.BytesIO(data))

    def read_json(self, gcs_path: str) -> dict[str, object]:
        """Read JSON file from GCS path."""
        data = download_from_storage(self.cloud_target.gcs_bucket, gcs_path.lstrip("/"))
        return dict(json.loads(data.decode("utf-8")))

    def exists(self, gcs_path: str) -> bool:
        """Check if file exists in GCS."""
        return storage_exists(self.cloud_target.gcs_bucket, gcs_path.lstrip("/"))


class MarketDataReader(BaseReader):
    """Reader for market data files."""

    def read_tick(self, instrument: str, date: str) -> pd.DataFrame:
        """Read tick data for instrument and date."""
        path = PathRegistry.format(PathRegistry.MARKET_TICK_RAW, instrument=instrument, date=date)
        return self.read_parquet(path)

    def read_candles(self, instrument: str, date: str, timeframe: str = "1h") -> pd.DataFrame:
        """Read candle data for instrument, date and timeframe."""
        pattern_map = {
            "1m": PathRegistry.MARKET_CANDLE_1M,
            "1h": PathRegistry.MARKET_CANDLE_1H,
            "24h": PathRegistry.MARKET_CANDLE_24H,
        }
        pattern = pattern_map.get(timeframe, PathRegistry.MARKET_CANDLE_1H)
        return self.read_parquet(PathRegistry.format(pattern, instrument=instrument, date=date))


class FeaturesReader(BaseReader):
    """Reader for feature data files."""

    def read_delta_one(self, instrument: str, date: str, timeframe: str = "24h") -> pd.DataFrame:
        """Read delta one features for instrument, date and timeframe."""
        path = PathRegistry.format(
            PathRegistry.FEATURES_DELTA_ONE, instrument=instrument, timeframe=timeframe, date=date
        )
        return self.read_parquet(path)


class MLReader(BaseReader):
    """Reader for ML data files."""

    def read_predictions(self, instrument: str, date: str, timeframe: str = "24h") -> pd.DataFrame:
        """Read ML predictions for instrument, date and timeframe."""
        path = PathRegistry.format(PathRegistry.ML_PREDICTIONS, instrument=instrument, timeframe=timeframe, date=date)
        return self.read_parquet(path)
