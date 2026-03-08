"""Market data domain clients (tick data and processed candles)."""

from __future__ import annotations

# pyright: reportAny=false, reportExplicitAny=false
import logging
import warnings
from datetime import UTC, datetime
from typing import NotRequired, TypedDict, Unpack

import pandas as pd
from unified_config_interface import UnifiedCloudConfig

from ..paths import build_bucket, build_path
from ..standardized_service import StandardizedDomainCloudService
from .base import BaseDataClient

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Thin typed clients (new pattern — constructor injection)
# --------------------------------------------------------------------------- #

_INSTRUMENT_TYPE_FOLDER_MAP: dict[str, str] = {
    "perpetual": "perpetuals",
    "spot": "spot",
    "future": "futures_chain",
    "option": "options_chain",
    "etf": "etf",
    "equity": "equities",
    "pool": "pool",
    "lst": "lst",
}


class MarketTickDomainClient(BaseDataClient):
    """Typed thin client for raw tick data — uses StorageClient injection."""

    def get_tick_data(
        self,
        date: str,
        venue: str,
        instrument_key: str,
        data_type: str,
        instrument_type: str,
        category: str = "cefi",
    ) -> pd.DataFrame:
        """Get tick data for a specific date, venue, and instrument."""
        bucket = build_bucket(
            "raw_tick_data", project_id=self._config.gcp_project_id, category=category
        )
        path = (
            build_path(
                "raw_tick_data",
                date=date,
                data_type=data_type,
                instrument_type=instrument_type,
                venue=venue,
            )
            + f"{instrument_key}.parquet"
        )
        return self._read_parquet(bucket, path)

    def get_available_dates(self, venue: str, category: str = "cefi") -> list[str]:
        """List dates for which tick data exists at this venue."""
        bucket = build_bucket(
            "raw_tick_data", project_id=self._config.gcp_project_id, category=category
        )
        blobs = self._list_blobs(bucket, "raw_tick_data/by_date/")
        dates: list[str] = []
        for blob in blobs:
            if f"/venue={venue}/" in blob:
                for part in blob.split("/"):
                    if part.startswith("day="):
                        dates.append(part[4:])
        return sorted(set(dates))


class MarketCandleDomainClient(BaseDataClient):
    """Typed thin client for processed candles — uses StorageClient injection."""

    def get_candles(
        self,
        date: str,
        venue: str,
        instrument_id: str,
        timeframe: str,
        data_type: str,
        instrument_type: str,
        category: str = "cefi",
    ) -> pd.DataFrame:
        """Get candle data for a specific date, venue, and instrument."""
        bucket = build_bucket(
            "processed_candles", project_id=self._config.gcp_project_id, category=category
        )
        path = (
            build_path(
                "processed_candles",
                date=date,
                timeframe=timeframe,
                data_type=data_type,
                instrument_type=instrument_type,
                venue=venue,
            )
            + f"{instrument_id}.parquet"
        )
        return self._read_parquet(bucket, path)

    def get_available_timeframes(self, venue: str, category: str = "cefi") -> list[str]:
        """List timeframes that have candle data at this venue."""
        bucket = build_bucket(
            "processed_candles", project_id=self._config.gcp_project_id, category=category
        )
        blobs = self._list_blobs(bucket, "processed_candles/by_date/")
        timeframes: list[str] = []
        for blob in blobs:
            if f"/venue={venue}/" in blob:
                for part in blob.split("/"):
                    if part.startswith("timeframe="):
                        timeframes.append(part[10:])
        return sorted(set(timeframes))


# --------------------------------------------------------------------------- #
# Rich legacy clients (migrated from monolithic clients.py, keep full interface)
# --------------------------------------------------------------------------- #


class MarketCandleDataDomainClient:
    """Client for accessing processed candle data from market-data-processing-service."""

    def __init__(
        self,
        project_id: str | None = None,
        storage_bucket: str | None = None,
        analytics_dataset: str | None = None,
    ) -> None:
        bucket = storage_bucket or UnifiedCloudConfig().market_data_gcs_bucket
        self.cloud_service = StandardizedDomainCloudService(domain="market_data", bucket=bucket)
        self._bucket = bucket
        logger.info("MarketCandleDataDomainClient initialized: bucket=%s", bucket)

    def get_candles(
        self,
        date: datetime,
        instrument_id: str,
        timeframe: str = "15s",
        data_type: str = "trades",
        venue: str | None = None,
    ) -> pd.DataFrame:
        """Get processed candles for a specific date and instrument."""
        date_str = date.strftime("%Y-%m-%d")

        if venue:
            gcs_path = (
                f"processed_candles/by_date/day={date_str}/timeframe={timeframe}"
                f"/data_type={data_type}/instrument_type=perpetuals/venue={venue}/{instrument_id}.parquet"
            )
        else:
            gcs_path = (
                f"processed_candles/by_date/day={date_str}/timeframe={timeframe}"
                f"/data_type={data_type}/{instrument_id}.parquet"
            )

        try:
            result = self.cloud_service.download_from_gcs(gcs_path=gcs_path, format="parquet")
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to load candles: %s", e)
            return pd.DataFrame()

    def get_candles_range(
        self,
        start_date: datetime,
        end_date: datetime,
        instrument_id: str,
        timeframe: str = "15s",
        data_type: str = "trades",
        venue: str | None = None,
    ) -> pd.DataFrame:
        """Get processed candles for a date range."""
        all_candles: list[pd.DataFrame] = []
        current_date = start_date
        while current_date <= end_date:
            candles = self.get_candles(
                date=current_date,
                instrument_id=instrument_id,
                timeframe=timeframe,
                data_type=data_type,
                venue=venue,
            )
            if not candles.empty:
                all_candles.append(candles)
            current_date = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
            current_date = current_date.replace(tzinfo=UTC) + pd.Timedelta(days=1)

        return pd.concat(all_candles, ignore_index=True) if all_candles else pd.DataFrame()


class MarketTickDataDomainClient:
    """Client for accessing raw tick data from market-tick-data-handler."""

    def __init__(
        self,
        project_id: str | None = None,
        storage_bucket: str | None = None,
        analytics_dataset: str | None = None,
    ) -> None:
        bucket = storage_bucket or UnifiedCloudConfig().market_data_gcs_bucket
        self.cloud_service = StandardizedDomainCloudService(domain="market_data", bucket=bucket)
        self._bucket = bucket
        logger.info("MarketTickDataDomainClient initialized: bucket=%s", bucket)

    def _build_tick_gcs_path(  # noqa: C901
        self,
        date_str: str,
        instrument_id: str,
        data_type: str,
        hour: int | None,
        venue: str | None,
        instrument_type_folder: str | None,
    ) -> str:
        """Build GCS path for tick data file."""
        type_folder = instrument_type_folder
        if not type_folder:
            parts = instrument_id.split(":")
            if len(parts) >= 2:
                type_folder = _INSTRUMENT_TYPE_FOLDER_MAP.get(parts[1].lower(), parts[1].lower())

        base_path = f"raw_tick_data/by_date/day={date_str}/data_type={data_type}"
        if hour is not None:
            base_path = f"{base_path}/hour={hour:02d}"

        needs_venue = type_folder in {
            "etf",
            "equities",
            "futures_chain",
            "options_chain",
            "indices",
            "pool",
            "lst",
            "a_token",
            "debt_token",
            "perpetuals",
            "spot",
        }
        extracted_venue = venue
        if not extracted_venue and needs_venue:
            parts = instrument_id.split(":")
            if parts:
                extracted_venue = parts[0]

        if type_folder:
            if needs_venue and extracted_venue:
                return (
                    f"{base_path}/instrument_type={type_folder}"
                    f"/venue={extracted_venue}/{instrument_id}.parquet"
                )
            return f"{base_path}/instrument_type={type_folder}/{instrument_id}.parquet"
        return f"{base_path}/{instrument_id}.parquet"

    def get_tick_data(
        self,
        date: datetime,
        instrument_id: str,
        data_type: str = "trades",
        hour: int | None = None,
        venue: str | None = None,
        instrument_type_folder: str | None = None,
    ) -> pd.DataFrame:
        """Get raw tick data for a specific date and instrument."""
        date_str = date.strftime("%Y-%m-%d")
        gcs_path = self._build_tick_gcs_path(
            date_str, instrument_id, data_type, hour, venue, instrument_type_folder
        )
        try:
            result = self.cloud_service.download_from_gcs(gcs_path=gcs_path, format="parquet")
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to load tick data: %s", e)
            return pd.DataFrame()

    def get_tick_data_range(
        self,
        start_date: datetime,
        end_date: datetime,
        instrument_id: str,
        data_type: str = "trades",
        venue: str | None = None,
    ) -> pd.DataFrame:
        """Get raw tick data for a date range."""
        all_ticks: list[pd.DataFrame] = []
        current_date = start_date
        while current_date <= end_date:
            ticks = self.get_tick_data(
                date=current_date,
                instrument_id=instrument_id,
                data_type=data_type,
                venue=venue,
            )
            if not ticks.empty:
                all_ticks.append(ticks)
            current_date = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
            current_date = current_date.replace(tzinfo=UTC) + pd.Timedelta(days=1)

        return pd.concat(all_ticks, ignore_index=True) if all_ticks else pd.DataFrame()


class _ClientConfig(TypedDict, total=False):
    """Typed kwargs for market data client factories."""

    project_id: NotRequired[str | None]
    storage_bucket: NotRequired[str | None]


class MarketDataDomainClient(MarketCandleDataDomainClient):
    """DEPRECATED: Use MarketCandleDataDomainClient or MarketTickDataDomainClient instead."""

    def __init__(
        self,
        project_id: str | None = None,
        storage_bucket: str | None = None,
        analytics_dataset: str | None = None,
    ) -> None:
        warnings.warn(
            "MarketDataDomainClient is deprecated. "
            "Use MarketCandleDataDomainClient or MarketTickDataDomainClient.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(
            project_id=project_id,
            storage_bucket=storage_bucket,
            analytics_dataset=analytics_dataset,
        )


# Factory functions (backward compat)
def create_market_candle_data_client(
    **kwargs: Unpack[_ClientConfig],
) -> MarketCandleDataDomainClient:
    return MarketCandleDataDomainClient(**kwargs)


def create_market_tick_data_client(**kwargs: Unpack[_ClientConfig]) -> MarketTickDataDomainClient:
    return MarketTickDataDomainClient(**kwargs)


def create_market_data_client(**kwargs: Unpack[_ClientConfig]) -> MarketDataDomainClient:
    warnings.warn(
        "create_market_data_client() is deprecated. "
        "Use create_market_candle_data_client() or create_market_tick_data_client().",
        DeprecationWarning,
        stacklevel=2,
    )
    return MarketDataDomainClient(**kwargs)
