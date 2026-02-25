"""
Market data domain clients for accessing candle and tick data.

Provides convenience methods for querying:
- Processed candles (15s, 1m, 5m, 15m, 1h, etc.)
- Raw tick data (trades, book_snapshot_5, liquidations, etc.)
- Multiple data types and timeframes
"""

from __future__ import annotations

import logging
import warnings
from datetime import UTC, datetime
from typing import Unpack

import pandas as pd
from google.cloud import exceptions as gcs_exceptions
from unified_cloud_services import CloudTarget, unified_config

from unified_domain_services import ClientConfig, StandardizedDomainCloudService

logger = logging.getLogger(__name__)


class MarketCandleDataDomainClient:
    """
    Client for accessing processed candle data from market-data-processing-service.

    Provides convenience methods for querying:
    - Processed candles (15s, 1m, 5m, 15m, 1h, etc.)
    - Multiple data types (trades, book_snapshot_5, liquidations, etc.)
    """

    def __init__(
        self,
        project_id: str | None = None,
        gcs_bucket: str | None = None,
        bigquery_dataset: str | None = None,
    ):
        """
        Initialize market candle data domain client.

        Args:
            project_id: GCP project ID (defaults to env var)
            gcs_bucket: GCS bucket name (defaults to env var)
            bigquery_dataset: BigQuery dataset (defaults to env var)
        """
        cloud_target = CloudTarget(
            project_id=project_id or unified_config.gcp_project_id,
            gcs_bucket=gcs_bucket or unified_config.market_data_gcs_bucket,
            bigquery_dataset=bigquery_dataset or unified_config.market_data_bigquery_dataset,
        )

        self.cloud_service = StandardizedDomainCloudService(domain="market_data", cloud_target=cloud_target)
        self.cloud_target = cloud_target

        logger.info(f"✅ MarketCandleDataDomainClient initialized: bucket={cloud_target.gcs_bucket}")

    def get_candles(
        self,
        date: datetime,
        instrument_id: str,
        timeframe: str = "15s",
        data_type: str = "trades",
        venue: str | None = None,
    ) -> pd.DataFrame:
        """
        Get processed candles for a specific date and instrument.

        Args:
            date: Target date
            instrument_id: Instrument ID (e.g., 'BINANCE-FUTURES:PERPETUAL:BTC-USDT')
            timeframe: Candle timeframe (e.g., '15s', '1m', '5m', '1h')
            data_type: Data type (e.g., 'trades', 'book_snapshot_5')
            venue: Optional venue filter

        Returns:
            DataFrame with candles
        """
        date_str = date.strftime("%Y-%m-%d")

        # Build GCS path
        if venue:
            gcs_path = (
                f"processed_candles/by_date/day={date_str}/timeframe={timeframe}/data_type={data_type}"
                f"/instrument_type=perpetuals/venue={venue}/{instrument_id}.parquet"
            )
        else:
            gcs_path = (
                f"processed_candles/by_date/day={date_str}/timeframe={timeframe}/data_type={data_type}"
                f"/{instrument_id}.parquet"
            )

        try:
            logger.info(f"📥 Loading candles: {gcs_path}")
            result = self.cloud_service.download_from_gcs(gcs_path=gcs_path, format="parquet")
            candles_df = result if isinstance(result, pd.DataFrame) else pd.DataFrame()

            if candles_df.empty:
                logger.warning(f"⚠️ No candles found for {instrument_id} on {date_str}")
            else:
                logger.info(f"✅ Loaded {len(candles_df)} candles")

            return candles_df

        except gcs_exceptions.NotFound as e:
            logger.error(f"❌ Candle data not found: {e}")
            return pd.DataFrame()
        except gcs_exceptions.GoogleCloudError as e:
            logger.error(f"❌ GCS error loading candles: {e}")
            return pd.DataFrame()
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"❌ Data processing error loading candles: {e}")
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
        """
        Get processed candles for a date range.

        Args:
            start_date: Start date
            end_date: End date
            instrument_id: Instrument ID
            timeframe: Candle timeframe
            data_type: Data type
            venue: Optional venue filter

        Returns:
            Combined DataFrame with candles for all dates
        """
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

            # Move to next day
            current_date = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
            current_date = current_date.replace(tzinfo=UTC) + pd.Timedelta(days=1)

        if all_candles:
            return pd.concat(all_candles, ignore_index=True)
        else:
            return pd.DataFrame()


class MarketTickDataDomainClient:
    """
    Client for accessing raw tick data from market-tick-data-handler.

    Provides convenience methods for querying:
    - Raw tick data (trades, book_snapshot_5, liquidations, etc.)
    - Tick data by hour
    - Tick data by date range
    """

    def __init__(
        self,
        project_id: str | None = None,
        gcs_bucket: str | None = None,
        bigquery_dataset: str | None = None,
    ):
        """
        Initialize market tick data domain client.

        Args:
            project_id: GCP project ID (defaults to env var)
            gcs_bucket: GCS bucket name (defaults to env var)
            bigquery_dataset: BigQuery dataset (defaults to env var)
        """
        cloud_target = CloudTarget(
            project_id=project_id or unified_config.gcp_project_id,
            gcs_bucket=gcs_bucket or unified_config.market_data_gcs_bucket,
            bigquery_dataset=bigquery_dataset or unified_config.market_data_bigquery_dataset,
        )

        self.cloud_service = StandardizedDomainCloudService(domain="market_data", cloud_target=cloud_target)
        self.cloud_target = cloud_target

        logger.info(f"✅ MarketTickDataDomainClient initialized: bucket={cloud_target.gcs_bucket}")

    def get_tick_data(
        self,
        date: datetime,
        instrument_id: str,
        data_type: str = "trades",
        hour: int | None = None,
        venue: str | None = None,
        instrument_type_folder: str | None = None,
    ) -> pd.DataFrame:
        """
        Get raw tick data for a specific date and instrument.

        Args:
            date: Target date
            instrument_id: Instrument ID (e.g., 'BINANCE-FUTURES:PERPETUAL:BTC-USDT@LIN')
            data_type: Data type (e.g., 'trades', 'book_snapshot_5', 'liquidations')
            hour: Optional hour filter (0-23)
            venue: Optional venue filter
            instrument_type_folder: Optional override for instrument type folder (e.g., 'perpetuals', 'spot')
                                    If not provided, will be derived from instrument_id

        Returns:
            DataFrame with tick data
        """
        date_str = date.strftime("%Y-%m-%d")

        # Derive instrument type folder from instrument_id if not provided
        # instrument_id format: VENUE:TYPE:SYMBOL@SUFFIX (e.g., BINANCE-FUTURES:PERPETUAL:BTC-USDT@LIN)
        type_folder = instrument_type_folder
        if not type_folder:
            parts = instrument_id.split(":")
            if len(parts) >= 2:
                inst_type = parts[1].lower()  # e.g., "perpetual"
                # Map to folder names (match actual GCS folder structure)
                # NOTE: Folder structure varies by bucket:
                # - CeFi: perpetuals/ (plural), spot/
                # - TradFi: etf/ (singular), equities/, futures_chain/, options_chain/
                # - DeFi: pool/, lst/, swaps/
                type_folder_map = {
                    "perpetual": "perpetuals",  # CeFi: perpetuals/ folder
                    "spot": "spot",
                    "future": "futures_chain",
                    "option": "options_chain",
                    "etf": "etf",  # TradFi: etf/ folder (singular)
                    "equity": "equities",  # TradFi: equities/ folder
                    "pool": "pool",  # DeFi: pool/ folder
                    "lst": "lst",  # DeFi: lst/ folder
                }
                type_folder = type_folder_map.get(inst_type, inst_type)

        # Build GCS path
        # Path format varies by category (key=value for BigQuery hive partitioning):
        # - CeFi: raw_tick_data/by_date/day={date}/data_type={type}/instrument_type={type}/
        #          {venue}/{instrument_id}.parquet
        # - TradFi: raw_tick_data/by_date/day={date}/data_type={type}/instrument_type={type}/
        #           {venue}/{instrument_id}.parquet
        # - DeFi: raw_tick_data/by_date/day={date}/data_type={type}/instrument_type={type}/
        #          {venue}/{instrument_id}.parquet
        base_path = f"raw_tick_data/by_date/day={date_str}/data_type={data_type}"

        if hour is not None:
            hour_str = f"{hour:02d}"
            base_path = f"{base_path}/hour={hour_str}"

        # Determine if this instrument needs a venue subfolder
        # All instrument types now have venue subfolders in the GCS bucket structure
        # TradFi: etf, equities, futures_chain, options_chain, indices
        # DeFi: pool, lst, a_token, debt_token
        # CeFi: perpetuals, spot
        needs_venue_subfolder = type_folder in [
            "etf",
            "equities",
            "futures_chain",
            "options_chain",
            "indices",  # TradFi
            "pool",
            "lst",
            "a_token",
            "debt_token",  # DeFi
            "perpetuals",
            "spot",  # CeFi
        ]

        # Extract venue from instrument_id if not explicitly provided
        extracted_venue = venue
        if not extracted_venue and needs_venue_subfolder:
            parts = instrument_id.split(":")
            if len(parts) >= 1:
                extracted_venue = parts[0]  # e.g., "NASDAQ" from "NASDAQ:ETF:SPY-USD"

        if type_folder:
            if needs_venue_subfolder and extracted_venue:
                gcs_path = f"{base_path}/instrument_type={type_folder}/venue={extracted_venue}/{instrument_id}.parquet"
            else:
                gcs_path = f"{base_path}/instrument_type={type_folder}/{instrument_id}.parquet"
        else:
            gcs_path = f"{base_path}/{instrument_id}.parquet"

        try:
            logger.info(f"📥 Loading tick data: {gcs_path}")
            result = self.cloud_service.download_from_gcs(gcs_path=gcs_path, format="parquet")
            tick_df = result if isinstance(result, pd.DataFrame) else pd.DataFrame()

            if tick_df.empty:
                logger.warning(f"⚠️ No tick data found for {instrument_id} on {date_str}")
            else:
                logger.info(f"✅ Loaded {len(tick_df)} tick records")

            return tick_df

        except gcs_exceptions.NotFound as e:
            logger.error(f"❌ Tick data not found: {e}")
            return pd.DataFrame()
        except gcs_exceptions.GoogleCloudError as e:
            logger.error(f"❌ GCS error loading tick data: {e}")
            return pd.DataFrame()
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"❌ Data processing error loading tick data: {e}")
            return pd.DataFrame()

    def get_tick_data_range(
        self,
        start_date: datetime,
        end_date: datetime,
        instrument_id: str,
        data_type: str = "trades",
        venue: str | None = None,
    ) -> pd.DataFrame:
        """
        Get raw tick data for a date range.

        Args:
            start_date: Start date
            end_date: End date
            instrument_id: Instrument ID
            data_type: Data type
            venue: Optional venue filter

        Returns:
            Combined DataFrame with tick data for all dates
        """
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

            # Move to next day
            current_date = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
            current_date = current_date.replace(tzinfo=UTC) + pd.Timedelta(days=1)

        if all_ticks:
            return pd.concat(all_ticks, ignore_index=True)
        else:
            return pd.DataFrame()


# Deprecated: Keep for backward compatibility
class MarketDataDomainClient(MarketCandleDataDomainClient):
    """
    ⚠️ DEPRECATED: Use MarketCandleDataDomainClient or MarketTickDataDomainClient instead.

    This class is kept for backward compatibility only.
    """

    def __init__(self, *args: object, **kwargs: Unpack[ClientConfig]) -> None:
        warnings.warn(
            "MarketDataDomainClient is deprecated. Use MarketCandleDataDomainClient "
            "or MarketTickDataDomainClient instead. "
            "See docs/CLIENTS_DEPRECATION_GUIDE.md for migration details.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)  # pyright: ignore[reportCallIssue]
