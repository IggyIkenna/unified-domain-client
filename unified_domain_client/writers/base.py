"""Base writer classes for cloud storage data upload."""

import io
import json
import logging
from abc import ABC, abstractmethod

import pandas as pd
from unified_cloud_interface import upload_to_storage
from unified_config_interface import PathRegistry

logger = logging.getLogger(__name__)


class BaseDataWriter(ABC):
    """Abstract base for StorageClient-based writers."""

    @abstractmethod
    def write(self, df: pd.DataFrame, bucket: str, path: str) -> None:
        """Write a DataFrame to cloud storage."""

    @abstractmethod
    def write_json(self, data: dict[str, object], bucket: str, path: str) -> None:
        """Write a JSON-serialisable dict to cloud storage."""


class BaseWriter:
    """Base class for writing data to cloud storage."""

    def __init__(self, bucket: str) -> None:
        self.bucket = bucket

    def write_parquet(self, df: pd.DataFrame, gcs_path: str) -> str:
        """Write DataFrame as parquet to storage path (relative to bucket)."""
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        return upload_to_storage(self.bucket, gcs_path.lstrip("/"), buf.read())

    def write_json(self, data: dict[str, object], gcs_path: str) -> str:
        """Write dict as JSON to storage path."""
        raw = json.dumps(data, default=str).encode("utf-8")
        return upload_to_storage(self.bucket, gcs_path.lstrip("/"), raw)


class MarketDataWriter(BaseWriter):
    """Writer for market data files."""

    def write_tick(self, df: pd.DataFrame, instrument: str, date: str) -> str:
        """Write tick data for instrument and date."""
        path = PathRegistry.format(PathRegistry.MARKET_TICK_RAW, instrument=instrument, date=date)
        return self.write_parquet(df, path)


class FeaturesWriter(BaseWriter):
    """Writer for feature data files."""

    def write_delta_one(
        self, df: pd.DataFrame, instrument: str, date: str, timeframe: str = "24h"
    ) -> str:
        """Write delta one features for instrument, date and timeframe."""
        path = PathRegistry.format(
            PathRegistry.FEATURES_DELTA_ONE, instrument=instrument, timeframe=timeframe, date=date
        )
        return self.write_parquet(df, path)


class MLWriter(BaseWriter):
    """Writer for ML data files."""

    def write_predictions(
        self, df: pd.DataFrame, instrument: str, date: str, timeframe: str = "24h"
    ) -> str:
        """Write ML predictions for instrument, date and timeframe."""
        path = PathRegistry.format(
            PathRegistry.ML_PREDICTIONS, instrument=instrument, timeframe=timeframe, date=date
        )
        return self.write_parquet(df, path)
