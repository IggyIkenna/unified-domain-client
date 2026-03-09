"""Base reader classes for cloud storage data access."""

# pyright: reportAny=false
# json.loads() returns Any per typeshed; this is a stdlib limitation.

import io
import json
import logging
from abc import ABC, abstractmethod

import pandas as pd
from unified_cloud_interface import download_from_storage, storage_exists
from unified_config_interface import PathRegistry

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
    """Base class for reading data from cloud storage."""

    def __init__(self, bucket: str) -> None:
        self.bucket = bucket

    def read_parquet(self, gcs_path: str) -> pd.DataFrame:
        """Read parquet file from storage path (relative to bucket)."""
        data = download_from_storage(self.bucket, gcs_path.lstrip("/"))
        return pd.read_parquet(io.BytesIO(data))

    def read_json(self, gcs_path: str) -> dict[str, object]:
        """Read JSON file from storage path."""
        data = download_from_storage(self.bucket, gcs_path.lstrip("/"))
        return dict(json.loads(data.decode("utf-8")))

    def exists(self, gcs_path: str) -> bool:
        """Check if file exists in storage."""
        return storage_exists(self.bucket, gcs_path.lstrip("/"))


class MarketDataReader(BaseReader):
    """Reader for market data files."""

    def read_tick(self, instrument: str, date: str) -> pd.DataFrame:
        """Read tick data for instrument and date."""
        path = PathRegistry.format(PathRegistry.MARKET_TICK_RAW, instrument=instrument, date=date)
        return self.read_parquet(path)

    def read_candles(self, instrument: str, date: str, timeframe: str = "1h") -> pd.DataFrame:
        """Read candle data for instrument, date and timeframe."""
        path = PathRegistry.format(
            PathRegistry.MARKET_CANDLES, instrument=instrument, date=date, timeframe=timeframe
        )
        return self.read_parquet(path)


class FeaturesReader(BaseReader):
    """Reader for feature data files."""

    def read_delta_one(self, instrument: str, date: str, timeframe: str = "24h") -> pd.DataFrame:
        """Read delta one features for instrument, date and timeframe."""
        path = PathRegistry.format(
            PathRegistry.FEATURES_DELTA_ONE, instrument=instrument, date=date, timeframe=timeframe
        )
        return self.read_parquet(path)


class MLReader(BaseReader):
    """Reader for ML data files."""

    def read_predictions(self, instrument: str, date: str, timeframe: str = "24h") -> pd.DataFrame:
        """Read ML predictions for instrument, date and timeframe."""
        path = PathRegistry.format(
            PathRegistry.ML_PREDICTIONS, instrument=instrument, date=date, timeframe=timeframe
        )
        return self.read_parquet(path)
