"""
Domain Clients for Unified Cloud Services

Convenience wrappers for accessing domain data across all services.
These clients provide domain-specific query patterns and are useful for:
- Analytics platforms that need to access multiple domains
- Cross-service quality gates
- Centralized data access patterns

All clients use StandardizedDomainCloudService under the hood.
"""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime, timedelta
from typing import TypedDict, cast

import pandas as pd
from unified_cloud_services.core.cloud_config import CloudTarget
from unified_cloud_services.core.config import unified_config

from unified_domain_services.standardized_service import (
    StandardizedDomainCloudService,
)

logger = logging.getLogger(__name__)  # logging instance


def _is_empty_or_na(val: object) -> bool:
    """Check if value is None, NaN, or empty (for scalars from DataFrame)."""
    if val is None:
        return True
    if isinstance(val, float) and val != val:  # NaN
        return True
    if val == "":
        return True
    if isinstance(val, str) and not val.strip():
        return True
    return False


class InstrumentSummaryStats(TypedDict, total=False):
    """Type for get_summary_stats return value."""

    total_instruments: int
    error: str
    venues: int
    venue_breakdown: dict[str, int]
    instrument_types: int
    type_breakdown: dict[str, int]
    base_currencies: int
    quote_currencies: int
    top_base_currencies: dict[str, int]
    top_quote_currencies: dict[str, int]
    ccxt_coverage: dict[str, int | float]
    data_type_coverage: dict[str, int]


class InstrumentsDomainClient:
    """
    Client for accessing instruments domain data.

    Provides convenience methods for querying instrument definitions with:
    - Date-based filtering
    - Venue/instrument type filtering
    - Symbol pattern matching
    - Multi-criteria queries
    """

    def __init__(
        self,
        project_id: str | None = None,
        gcs_bucket: str | None = None,
        bigquery_dataset: str | None = None,
    ):
        """
        Initialize instruments domain client.

        Args:
            project_id: GCP project ID (defaults to env var)
            gcs_bucket: GCS bucket name (defaults to env var)
            bigquery_dataset: BigQuery dataset (defaults to env var)
        """
        cloud_target = CloudTarget(
            project_id=project_id or unified_config.gcp_project_id,
            gcs_bucket=gcs_bucket or unified_config.instruments_gcs_bucket,
            bigquery_dataset=bigquery_dataset or unified_config.instruments_bigquery_dataset,
        )

        self.cloud_service = StandardizedDomainCloudService(domain="instruments", cloud_target=cloud_target)
        self.cloud_target = cloud_target

        logger.info(f"✅ InstrumentsDomainClient initialized: bucket={cloud_target.gcs_bucket}")

    def get_instruments_for_date(
        self,
        date: str | datetime,
        venue: str | None = None,
        instrument_type: str | list[str] | None = None,
        base_currency: str | list[str] | None = None,
        quote_currency: str | list[str] | None = None,
        symbol_pattern: str | None = None,
        instrument_ids: list[str] | str | None = None,
        venues: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Get instrument definitions for a specific date with filtering.

        Supports both new by-venue folder structure and legacy single-file structure:
        - New: instrument_availability/by_date/day={date}/venue={venue}/instruments.parquet
        - Legacy: instrument_availability/by_date/day={date}/instruments.parquet

        Args:
            date: Date to get instruments for (YYYY-MM-DD string or datetime)
            venue: Filter by single venue (BINANCE, DERIBIT, BYBIT, OKX, etc.)
            instrument_type: Filter by type (SPOT_PAIR, PERPETUAL, FUTURE, OPTION)
            base_currency: Filter by base asset (BTC, ETH, SOL, etc.)
            quote_currency: Filter by quote asset (USDT, USD, USDC, etc.)
            symbol_pattern: Regex pattern to match symbols
            instrument_ids: List of specific instrument IDs to include
            venues: List of venues to load (more efficient than single venue filter)

        Returns:
            DataFrame with filtered instrument definitions
        """
        # Parse date
        if isinstance(date, str):
            date_obj = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=UTC)
        else:
            date_obj = date

        date_str = date_obj.strftime("%Y-%m-%d")

        try:
            # Determine which venues to load
            venues_to_load = venues or ([venue] if venue else None)

            # By-venue structure ONLY — no legacy single-file fallback.
            # Expected: instrument_availability/by_date/day={date}/venue={VENUE}/instruments.parquet
            instruments_df = self._load_instruments_by_venue(date_str, venues_to_load)

            if instruments_df.empty:
                bucket = self.cloud_target.gcs_bucket if self.cloud_target else "unknown"
                expected_path = (
                    f"gs://{bucket}/instrument_availability/by_date/day={date_str}/venue=<VENUE>/instruments.parquet"
                )
                logger.error(
                    f"❌ No instrument definitions found for {date_str} in by-venue structure. "
                    f"Expected: {expected_path} — "
                    f"Run instruments-service for this date to populate GCS."
                )
                return pd.DataFrame()

            # Filter by date availability
            instruments_df = self._filter_by_date_availability(instruments_df, date_obj)

            if instruments_df.empty:
                logger.warning(f"⚠️ No instruments available for {date_str} after date filtering")
                return pd.DataFrame()

            # Apply filters (venue filter only if not already loaded specific venues)
            venue_filter = venue if not venues_to_load else None
            filtered_df = self._apply_filters(
                instruments_df,
                venue_filter,
                instrument_type,
                base_currency,
                quote_currency,
                symbol_pattern,
                instrument_ids,
            )

            logger.info(f"🔍 Filtered to {len(filtered_df)} instruments")
            return filtered_df

        except Exception as e:
            logger.error(f"❌ Failed to load instruments for {date_str}: {e}")
            return pd.DataFrame()

    def _load_instruments_by_venue(self, date_str: str, venues: list[str] | None = None) -> pd.DataFrame:
        """
        Load instruments from by-venue folder structure.

        Args:
            date_str: Date string (YYYY-MM-DD)
            venues: Optional list of specific venues to load. If None, loads all venues.

        Returns:
            DataFrame with instruments from all requested venues
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        from unified_cloud_services.core.client_factory import get_storage_client

        try:
            # Get storage client (cloud-agnostic) and bucket handle
            client = get_storage_client(project_id=self.cloud_target.project_id)
            bucket = client.bucket(self.cloud_target.gcs_bucket)

            base_prefix = f"instrument_availability/by_date/day={date_str}/"

            if venues:
                # Load specific venues
                venue_folders: list[str] = [f"{base_prefix}venue={v}/" for v in venues]
            else:
                # List all venue folders via list_blobs with delimiter
                iterator = bucket.list_blobs(prefix=base_prefix, delimiter="/")
                list(iterator)  # Consume to populate .prefixes
                venue_folders = [p for p in iterator.prefixes if "venue=" in p]

            if not venue_folders:
                logger.debug(f"No venue folders found for {date_str}")
                return pd.DataFrame()

            logger.info(f"📥 Loading instruments from {len(venue_folders)} venue folder(s) for {date_str}")

            def load_venue_file(venue_prefix: str) -> pd.DataFrame:
                """Load instruments for one venue."""
                try:
                    gcs_path = f"{venue_prefix}instruments.parquet"
                    result = self.cloud_service.download_from_gcs(gcs_path=gcs_path, format="parquet")
                    return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
                except Exception as e:
                    logger.debug(f"Could not load {venue_prefix}: {e}")
                    return pd.DataFrame()

            # Parallel load all venue files
            all_dfs = []
            with ThreadPoolExecutor(max_workers=min(12, len(venue_folders))) as executor:
                futures = {executor.submit(load_venue_file, vf): vf for vf in venue_folders}
                for future in as_completed(futures):
                    df = future.result()
                    if not df.empty:
                        all_dfs.append(df)

            if all_dfs:
                combined = pd.concat(all_dfs, ignore_index=True)
                logger.info(f"✅ Loaded {len(combined)} instruments from {len(all_dfs)} venue files")
                return combined

            return pd.DataFrame()

        except Exception as e:
            logger.debug(f"Could not load by-venue structure: {e}")
            return pd.DataFrame()

    def _filter_by_date_availability(self, df: pd.DataFrame, target_date: datetime) -> pd.DataFrame:
        """Filter instruments by date availability."""
        if df.empty:
            return df

        filtered_df = df.copy()

        # Filter by available_from_datetime
        if "available_from_datetime" in filtered_df.columns:

            def is_available_from(from_datetime_str: object) -> bool:
                if _is_empty_or_na(from_datetime_str):
                    return True
                try:
                    from_date = datetime.fromisoformat(str(from_datetime_str).replace("Z", "+00:00"))
                    if from_date.tzinfo is None:
                        from_date = from_date.replace(tzinfo=UTC)
                    target_date_aware = target_date if target_date.tzinfo else target_date.replace(tzinfo=UTC)
                    return target_date_aware >= from_date
                except (ValueError, AttributeError):
                    return True

            mask = filtered_df["available_from_datetime"].apply(is_available_from)
            filtered_df = filtered_df.loc[mask]

        # Filter by available_to_datetime
        if "available_to_datetime" in filtered_df.columns:

            def is_available_to(to_datetime_str: object) -> bool:
                if _is_empty_or_na(to_datetime_str):
                    return True
                try:
                    to_date = datetime.fromisoformat(str(to_datetime_str).replace("Z", "+00:00"))
                    if to_date.tzinfo is None:
                        to_date = to_date.replace(tzinfo=UTC)
                    target_date_aware = target_date if target_date.tzinfo else target_date.replace(tzinfo=UTC)
                    return target_date_aware <= to_date
                except (ValueError, AttributeError):
                    return True

            mask = filtered_df["available_to_datetime"].apply(is_available_to)
            filtered_df = filtered_df.loc[mask]

        return filtered_df

    def _apply_filters(
        self,
        df: pd.DataFrame,
        venue: str | list[str] | None = None,
        instrument_type: str | list[str] | None = None,
        base_currency: str | list[str] | None = None,
        quote_currency: str | list[str] | None = None,
        symbol_pattern: str | None = None,
        instrument_ids: list[str] | str | None = None,
    ) -> pd.DataFrame:
        """Apply comprehensive filtering to instruments DataFrame"""

        if venue:
            venues = (
                [v.strip().upper() for v in venue.split(",")] if isinstance(venue, str) else [v.upper() for v in venue]
            )
            df = df.loc[df["venue"].isin(venues)]

        if instrument_type:
            types = (
                [t.strip().upper() for t in instrument_type.split(",")]
                if isinstance(instrument_type, str)
                else [t.upper() for t in instrument_type]
            )
            df = df.loc[df["instrument_type"].isin(types)]

        if base_currency:
            bases = (
                [b.strip().upper() for b in base_currency.split(",")]
                if isinstance(base_currency, str)
                else [b.upper() for b in base_currency]
            )
            df = df.loc[df["base_asset"].isin(bases)]

        if quote_currency:
            quotes = (
                [q.strip().upper() for q in quote_currency.split(",")]
                if isinstance(quote_currency, str)
                else [q.upper() for q in quote_currency]
            )
            df = df.loc[df["quote_asset"].isin(quotes)]

        if symbol_pattern:
            try:
                pattern = re.compile(symbol_pattern, re.IGNORECASE)
                df = df.loc[df["symbol"].str.match(pattern)]
            except re.error as e:
                logger.warning(f"⚠️ Invalid regex pattern '{symbol_pattern}': {e}")

        if instrument_ids:
            ids = [i.strip() for i in instrument_ids.split(",")] if isinstance(instrument_ids, str) else instrument_ids
            df = df.loc[df["instrument_key"].isin(ids)]

        return df

    def get_instruments_date_range(
        self,
        start_date: str | datetime,
        end_date: str | datetime,
        venue: str | None = None,
        instrument_type: str | list[str] | None = None,
        base_currency: str | list[str] | None = None,
        quote_currency: str | list[str] | None = None,
        symbol_pattern: str | None = None,
        instrument_ids: list[str] | str | None = None,
        venues: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Get instruments across a date range (union of all dates).

        Args:
            start_date: Start date
            end_date: End date
            venue: Optional venue filter
            instrument_type: Optional instrument type filter
            **kwargs: Additional filters passed to get_instruments_for_date

        Returns:
            DataFrame with unique instruments across date range
        """
        # Parse dates
        if isinstance(start_date, str):
            start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=UTC)
        else:
            start_dt = start_date

        if isinstance(end_date, str):
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=UTC)
        else:
            end_dt = end_date

        # Generate date range
        all_instruments: list[pd.DataFrame] = []
        current_date = start_dt
        while current_date <= end_dt:
            date_instruments = self.get_instruments_for_date(
                current_date,
                venue=venue,
                instrument_type=instrument_type,
                base_currency=base_currency,
                quote_currency=quote_currency,
                symbol_pattern=symbol_pattern,
                instrument_ids=instrument_ids,
                venues=venues,
            )
            if not date_instruments.empty:
                date_instruments["query_date"] = current_date.strftime("%Y-%m-%d")
                all_instruments.append(date_instruments)
            current_date += timedelta(days=1)

        if not all_instruments:
            logger.warning(f"⚠️ No instruments found in date range {start_date} to {end_date}")
            return pd.DataFrame()

        # Combine and deduplicate by instrument_key
        combined_df = pd.concat(all_instruments, ignore_index=True)
        unique_df = combined_df.drop_duplicates(subset=["instrument_key"], keep="first")

        logger.info(f"📊 Date range query: {len(unique_df)} unique instruments across date range")
        return unique_df

    def get_summary_stats(self, date: str | datetime) -> InstrumentSummaryStats:
        """
        Get summary statistics for instruments on a specific date.

        Args:
            date: Date to analyze

        Returns:
            Dictionary with comprehensive statistics
        """
        instruments_df = self.get_instruments_for_date(date)

        if instruments_df.empty:
            return {"total_instruments": 0, "error": "No instruments found"}

        # Calculate statistics (cast dict keys to str for InstrumentSummaryStats)
        def _to_str_dict(counts: pd.Series) -> dict[str, int]:
            return {str(k): int(v) for k, v in counts.to_dict().items()}

        stats: InstrumentSummaryStats = {
            "total_instruments": len(instruments_df),
            "venues": int(instruments_df["venue"].nunique()),
            "venue_breakdown": _to_str_dict(instruments_df["venue"].value_counts()),
            "instrument_types": int(instruments_df["instrument_type"].nunique()),
            "type_breakdown": _to_str_dict(instruments_df["instrument_type"].value_counts()),
            "base_currencies": int(instruments_df["base_asset"].nunique()),
            "quote_currencies": int(instruments_df["quote_asset"].nunique()),
            "top_base_currencies": _to_str_dict(instruments_df["base_asset"].value_counts().head(10)),
            "top_quote_currencies": _to_str_dict(instruments_df["quote_asset"].value_counts().head(10)),
        }

        if "ccxt_symbol" in instruments_df.columns:
            stats["ccxt_coverage"] = {
                "instruments_with_ccxt": len(instruments_df[instruments_df["ccxt_symbol"] != ""]),
                "ccxt_coverage_percent": len(instruments_df[instruments_df["ccxt_symbol"] != ""])
                / len(instruments_df)
                * 100,
            }

        if "data_types" in instruments_df.columns:
            stats["data_type_coverage"] = {
                "trades": len(instruments_df[instruments_df["data_types"].str.contains("trades", na=False)]),
                "book_snapshot_5": len(
                    instruments_df[instruments_df["data_types"].str.contains("book_snapshot_5", na=False)]
                ),
                "derivative_ticker": len(
                    instruments_df[instruments_df["data_types"].str.contains("derivative_ticker", na=False)]
                ),
                "liquidations": len(
                    instruments_df[instruments_df["data_types"].str.contains("liquidations", na=False)]
                ),
                "options_chain": len(
                    instruments_df[instruments_df["data_types"].str.contains("options_chain", na=False)]
                ),
            }

        logger.info(f"📊 Generated summary stats for {date}: {stats['total_instruments']} instruments")
        return stats

    def get_instrument_details(
        self, date: str | datetime, instrument_id: str
    ) -> dict[str, str | int | float | bool | None] | None:
        """
        Get detailed information for a specific instrument ID.

        Args:
            date: Date to check
            instrument_id: Canonical instrument ID

        Returns:
            Dictionary with instrument details or None if not found
        """
        instruments_df = self.get_instruments_for_date(date, instrument_ids=[instrument_id])

        if instruments_df.empty:
            logger.warning(f"⚠️ Instrument not found: {instrument_id} on {date}")
            return None

        # Convert to dictionary
        instrument_data = cast(
            dict[str, str | int | float | bool | None],
            instruments_df.iloc[0].to_dict(),
        )
        logger.info(f"✅ Found instrument details for {instrument_id}")
        return instrument_data

    def get_trading_parameters(
        self, date: str | datetime, instrument_id: str
    ) -> dict[str, str | int | float | bool | list[str] | None] | None:
        """
        Get trading parameters for an instrument (tick_size, min_size, etc.).

        Args:
            date: Date to check
            instrument_id: Canonical instrument ID

        Returns:
            Dictionary with trading parameters or None if not found
        """
        instrument = self.get_instrument_details(date, instrument_id)
        if not instrument:
            return None

        trading_params: dict[str, str | int | float | bool | list[str] | None] = {
            "tick_size": instrument.get("tick_size") or "",  # optional metadata
            "min_size": instrument.get("min_size") or "",  # optional metadata
            "contract_size": instrument.get("contract_size"),
            "ccxt_symbol": instrument.get("ccxt_symbol") or "",  # optional metadata
            "ccxt_exchange": instrument.get("ccxt_exchange") or "",  # optional metadata
            "inverse": instrument.get("inverse", False),
            "data_types": (
                [s.strip() for s in str(instrument.get("data_types")).split(",")]
                if instrument.get("data_types")
                else []
            ),
        }

        logger.info(f"📊 Trading parameters for {instrument_id}: {len(trading_params)} fields")
        return trading_params

    def get_instruments_by_data_type(
        self,
        date: str | datetime,
        data_type: str,
        venue: str | None = None,
        limit: int = 1000,
    ) -> pd.DataFrame:
        """
        Get instruments that support a specific data type.

        Args:
            date: Date to check
            data_type: Data type to filter by (trades, book_snapshot_5, derivative_ticker, etc.)
            venue: Optional venue filter
            limit: Maximum results to return

        Returns:
            DataFrame with instruments supporting the data type
        """
        instruments_df = self.get_instruments_for_date(date, venue=venue)

        if instruments_df.empty:
            return pd.DataFrame()

        # Filter by data type availability
        if "data_types" in instruments_df.columns:

            def has_data_type(data_types_str):
                if not data_types_str:
                    return False
                available_types = [dt.strip() for dt in data_types_str.split(",")]
                return data_type in available_types

            filtered_df = instruments_df[instruments_df["data_types"].apply(has_data_type)]
        else:
            filtered_df = pd.DataFrame()

        if len(filtered_df) > limit:
            logger.info(f"🔍 Limiting results to {limit} instruments (found {len(filtered_df)})")
            filtered_df = filtered_df.head(limit)

        logger.info(f"📊 Found {len(filtered_df)} instruments with {data_type} data")
        return filtered_df

    def search_instruments_by_symbol(
        self,
        date: str | datetime,
        symbol_pattern: str,
        venue: str | None = None,
        limit: int = 100,
    ) -> pd.DataFrame:
        """
        Search instruments by symbol pattern (regex supported).

        Args:
            date: Date to search
            symbol_pattern: Pattern to match (supports regex)
            venue: Optional venue filter
            limit: Maximum results to return

        Returns:
            DataFrame with matching instruments
        """
        instruments_df = self.get_instruments_for_date(date, venue=venue, symbol_pattern=symbol_pattern)

        if len(instruments_df) > limit:
            logger.info(f"🔍 Limiting results to {limit} instruments (found {len(instruments_df)})")
            instruments_df = instruments_df.head(limit)

        return instruments_df

    def get_expiring_instruments(
        self,
        date: str | datetime,
        days_until_expiry: int = 30,
        instrument_type: str | None = None,
    ) -> pd.DataFrame:
        """
        Get instruments expiring within specified days.

        Args:
            date: Reference date
            days_until_expiry: Number of days to look ahead
            instrument_type: Optional filter (FUTURE, OPTION)

        Returns:
            DataFrame with expiring instruments
        """
        instruments_df = self.get_instruments_for_date(date, instrument_type=instrument_type)

        if instruments_df.empty:
            return pd.DataFrame()

        # Filter instruments with expiry dates
        if "available_to_datetime" not in instruments_df.columns:
            return pd.DataFrame()

        expiring_df = instruments_df[instruments_df["available_to_datetime"].notna()]

        if expiring_df.empty:
            return pd.DataFrame()

        # Calculate expiry dates
        if isinstance(date, str):
            ref_date = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=UTC)
        else:
            ref_date = date if date.tzinfo else date.replace(tzinfo=UTC)

        cutoff_date = ref_date + timedelta(days=days_until_expiry)

        def is_expiring_soon(expiry_str):
            if not expiry_str:
                return False
            try:
                expiry_dt = datetime.fromisoformat(str(expiry_str).replace("Z", "+00:00"))
                if expiry_dt.tzinfo is None:
                    expiry_dt = expiry_dt.replace(tzinfo=UTC)
                return ref_date <= expiry_dt <= cutoff_date
            except (ValueError, TypeError):
                # Invalid date format or type
                return False

        expiring_df = expiring_df[expiring_df["available_to_datetime"].apply(is_expiring_soon)]

        logger.info(f"📊 Found {len(expiring_df)} instruments expiring within {days_until_expiry} days")
        return expiring_df


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
            gcs_path = f"processed_candles/by_date/day={date_str}/timeframe={timeframe}/data_type={data_type}/instrument_type=perpetuals/venue={venue}/{instrument_id}.parquet"
        else:
            gcs_path = f"processed_candles/by_date/day={date_str}/timeframe={timeframe}/data_type={data_type}/{instrument_id}.parquet"

        try:
            logger.info(f"📥 Loading candles: {gcs_path}")
            result = self.cloud_service.download_from_gcs(gcs_path=gcs_path, format="parquet")
            candles_df = result if isinstance(result, pd.DataFrame) else pd.DataFrame()

            if candles_df.empty:
                logger.warning(f"⚠️ No candles found for {instrument_id} on {date_str}")
            else:
                logger.info(f"✅ Loaded {len(candles_df)} candles")

            return candles_df

        except Exception as e:
            logger.error(f"❌ Failed to load candles: {e}")
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
        all_candles = []
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
        # - CeFi: raw_tick_data/by_date/day={date}/data_type={type}/instrument_type={type}/{venue}/{instrument_id}.parquet
        # - TradFi: raw_tick_data/by_date/day={date}/data_type={type}/instrument_type={type}/{venue}/{instrument_id}.parquet
        # - DeFi: raw_tick_data/by_date/day={date}/data_type={type}/instrument_type={type}/{venue}/{instrument_id}.parquet
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

        except Exception as e:
            logger.error(f"❌ Failed to load tick data: {e}")
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
        all_ticks = []
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

    def __init__(self, *args, **kwargs):
        import warnings

        warnings.warn(
            "MarketDataDomainClient is deprecated. Use MarketCandleDataDomainClient or MarketTickDataDomainClient instead. "
            "See docs/CLIENTS_DEPRECATION_GUIDE.md for migration details.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)


class FeaturesDomainClient:
    """
    Client for accessing features domain data.

    Provides convenience methods for querying:
    - Delta-one features
    - Volatility features
    - On-chain features
    - Calendar features
    """

    def __init__(
        self,
        project_id: str | None = None,
        gcs_bucket: str | None = None,
        bigquery_dataset: str | None = None,
        feature_type: str = "delta_one",  # 'delta_one', 'volatility', 'onchain', 'calendar'
    ):
        """
        Initialize features domain client.

        Args:
            project_id: GCP project ID (defaults to env var)
            gcs_bucket: GCS bucket name (defaults to env var)
            bigquery_dataset: BigQuery dataset (defaults to env var)
            feature_type: Type of features ('delta_one', 'volatility', 'onchain', 'calendar')
        """
        # Map feature types to datasets (use getattr for optional config fields)
        default_dataset = getattr(unified_config, "features_bigquery_dataset", "features")
        dataset_map: dict[str, str] = {
            "delta_one": getattr(unified_config, "features_bigquery_dataset", default_dataset),
            "volatility": getattr(unified_config, "volatility_features_bigquery_dataset", default_dataset),
            "onchain": getattr(unified_config, "onchain_features_bigquery_dataset", default_dataset),
            "calendar": getattr(unified_config, "calendar_features_bigquery_dataset", default_dataset),
        }

        cloud_target = CloudTarget(
            project_id=project_id or unified_config.gcp_project_id,
            gcs_bucket=gcs_bucket or unified_config.features_gcs_bucket,
            bigquery_dataset=bigquery_dataset or dataset_map.get(feature_type, default_dataset),
        )

        self.cloud_service = StandardizedDomainCloudService(domain="features", cloud_target=cloud_target)
        self.cloud_target = cloud_target
        self.feature_type = feature_type

        logger.info(f"✅ FeaturesDomainClient initialized: bucket={cloud_target.gcs_bucket}, type={feature_type}")

    def get_features(self, date: datetime, instrument_id: str, feature_set: str | None = None) -> pd.DataFrame:
        """
        Get features for a specific date and instrument.

        Args:
            date: Target date
            instrument_id: Instrument ID
            feature_set: Optional feature set filter

        Returns:
            DataFrame with features
        """
        date_str = date.strftime("%Y-%m-%d")

        # Build GCS path based on feature type
        if feature_set:
            gcs_path = (
                f"features/{self.feature_type}/by_date/day={date_str}/feature_set={feature_set}/{instrument_id}.parquet"
            )
        else:
            gcs_path = f"features/{self.feature_type}/by_date/day={date_str}/{instrument_id}.parquet"

        try:
            logger.info(f"📥 Loading {self.feature_type} features: {gcs_path}")
            result = self.cloud_service.download_from_gcs(gcs_path=gcs_path, format="parquet")
            features_df = result if isinstance(result, pd.DataFrame) else pd.DataFrame()

            if features_df.empty:
                logger.warning(f"⚠️ No features found for {instrument_id} on {date_str}")
            else:
                logger.info(f"✅ Loaded {len(features_df)} feature rows")

            return features_df

        except Exception as e:
            logger.error(f"❌ Failed to load features: {e}")
            return pd.DataFrame()


class ExecutionDomainClient:
    """
    Client for accessing execution domain data (backtest results).

    Provides methods for:
    - Loading backtest summaries
    - Loading fills, orders, positions
    - Loading equity curves with byte-range streaming
    - Listing available backtest runs
    """

    def __init__(
        self,
        project_id: str | None = None,
        gcs_bucket: str | None = None,
        bigquery_dataset: str | None = None,
    ):
        """
        Initialize execution domain client.

        Args:
            project_id: GCP project ID (defaults to env var)
            gcs_bucket: GCS bucket name (defaults to unified execution bucket)
            bigquery_dataset: BigQuery dataset (defaults to env var or 'execution')

        Note:
            Unified bucket structure: {date}/{strategy_id}/{instruction_type}/
            Instruction types: TRADE, SWAP, LEND, BORROW, STAKE, TRANSFER
        """
        proj = project_id or unified_config.gcp_project_id
        cloud_target = CloudTarget(
            project_id=proj,
            gcs_bucket=gcs_bucket or getattr(unified_config, "execution_gcs_bucket", f"execution-store-{proj}"),
            bigquery_dataset=bigquery_dataset or getattr(unified_config, "execution_bigquery_dataset", "execution"),
        )

        self.cloud_service = StandardizedDomainCloudService(domain="execution", cloud_target=cloud_target)
        self.cloud_target = cloud_target

        logger.info(f"✅ ExecutionDomainClient initialized: bucket={cloud_target.gcs_bucket}")

    def get_backtest_summary(self, run_id: str) -> dict[str, object]:
        """
        Load backtest summary JSON.

        Args:
            run_id: Backtest run ID (e.g., 'BT-20231223-001')

        Returns:
            Summary dict with pnl, metrics, execution stats
        """
        gcs_path = f"backtest_results/{run_id}/summary.json"

        try:
            logger.info(f"📥 Loading backtest summary: {gcs_path}")
            summary = self.cloud_service.download_from_gcs(gcs_path=gcs_path, format="json")
            return cast(dict[str, object], summary) if isinstance(summary, dict) else {}

        except Exception as e:
            logger.error(f"❌ Failed to load backtest summary: {e}")
            return {}

    def get_backtest_fills(self, run_id: str) -> pd.DataFrame:
        """
        Load all fills/trades from a backtest run.

        Args:
            run_id: Backtest run ID

        Returns:
            DataFrame with fill records
        """
        gcs_path = f"backtest_results/{run_id}/fills.parquet"

        try:
            logger.info(f"📥 Loading backtest fills: {gcs_path}")
            result = self.cloud_service.download_from_gcs(gcs_path=gcs_path, format="parquet")
            fills = result if isinstance(result, pd.DataFrame) else pd.DataFrame()

            if fills.empty:
                logger.warning(f"⚠️ No fills found for run {run_id}")
            else:
                logger.info(f"✅ Loaded {len(fills)} fills")

            return fills

        except Exception as e:
            logger.error(f"❌ Failed to load fills: {e}")
            return pd.DataFrame()

    def get_backtest_orders(self, run_id: str) -> pd.DataFrame:
        """
        Load all orders from a backtest run.

        Args:
            run_id: Backtest run ID

        Returns:
            DataFrame with order records
        """
        gcs_path = f"backtest_results/{run_id}/orders.parquet"

        try:
            logger.info(f"📥 Loading backtest orders: {gcs_path}")
            result = self.cloud_service.download_from_gcs(gcs_path=gcs_path, format="parquet")
            orders = result if isinstance(result, pd.DataFrame) else pd.DataFrame()

            if orders.empty:
                logger.warning(f"⚠️ No orders found for run {run_id}")
            else:
                logger.info(f"✅ Loaded {len(orders)} orders")

            return orders

        except Exception as e:
            logger.error(f"❌ Failed to load orders: {e}")
            return pd.DataFrame()

    def get_backtest_positions(self, run_id: str) -> pd.DataFrame:
        """
        Load position timeline from a backtest run.

        Args:
            run_id: Backtest run ID

        Returns:
            DataFrame with position snapshots
        """
        gcs_path = f"backtest_results/{run_id}/positions.parquet"

        try:
            logger.info(f"📥 Loading backtest positions: {gcs_path}")
            result = self.cloud_service.download_from_gcs(gcs_path=gcs_path, format="parquet")
            positions = result if isinstance(result, pd.DataFrame) else pd.DataFrame()

            if positions.empty:
                logger.warning(f"⚠️ No positions found for run {run_id}")
            else:
                logger.info(f"✅ Loaded {len(positions)} position snapshots")

            return positions

        except Exception as e:
            logger.error(f"❌ Failed to load positions: {e}")
            return pd.DataFrame()

    def get_equity_curve(
        self,
        run_id: str,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> pd.DataFrame:
        """
        Load equity curve with optional time filtering (byte-range streaming).

        Args:
            run_id: Backtest run ID
            start_time: Optional start time for filtering
            end_time: Optional end time for filtering

        Returns:
            DataFrame with equity curve data
        """
        gcs_path = f"backtest_results/{run_id}/equity_curve.parquet"

        try:
            if start_time and end_time:
                logger.info(f"📥 Loading equity curve with time filter: {gcs_path} ({start_time} to {end_time})")
                result = self.cloud_service.download_from_gcs(gcs_path=gcs_path, format="parquet")
                full_equity = result if isinstance(result, pd.DataFrame) else pd.DataFrame()
                if not full_equity.empty and "ts_event" in full_equity.columns:
                    mask = (full_equity["ts_event"] >= start_time) & (full_equity["ts_event"] <= end_time)
                    equity = full_equity.loc[mask]
                else:
                    equity = full_equity
            else:
                logger.info(f"📥 Loading full equity curve: {gcs_path}")
                result = self.cloud_service.download_from_gcs(gcs_path=gcs_path, format="parquet")
                equity = result if isinstance(result, pd.DataFrame) else pd.DataFrame()

            if equity.empty:
                logger.warning(f"⚠️ No equity data found for run {run_id}")
            else:
                logger.info(f"✅ Loaded {len(equity)} equity snapshots")

            return equity

        except Exception as e:
            logger.error(f"❌ Failed to load equity curve: {e}")
            return pd.DataFrame()

    def list_backtest_runs(self, prefix: str = "") -> list[str]:
        """
        List available backtest run IDs.

        Args:
            prefix: Optional prefix filter (e.g., 'BT-2023')

        Returns:
            List of run IDs
        """
        try:
            from unified_cloud_services.core.client_factory import get_storage_client

            logger.info(f"📋 Listing backtest runs (prefix='{prefix}')")

            client = get_storage_client()
            blobs = client.list_blobs(
                bucket=self.cloud_target.gcs_bucket,
                prefix=f"backtest_results/{prefix}",
            )

            # Extract unique run IDs
            run_ids = set()
            for blob_meta in blobs:
                parts = blob_meta.name.replace("backtest_results/", "").split("/")
                if parts and parts[0]:
                    run_ids.add(parts[0])

            run_ids_list = sorted(list(run_ids))
            logger.info(f"✅ Found {len(run_ids_list)} backtest runs")
            return run_ids_list

        except Exception as e:
            logger.error(f"❌ Failed to list backtest runs: {e}")
            return []


# Factory functions for creating domain clients
def create_instruments_client(**kwargs) -> InstrumentsDomainClient:
    """Factory function to create InstrumentsDomainClient."""
    return InstrumentsDomainClient(**kwargs)


def create_market_candle_data_client(**kwargs) -> MarketCandleDataDomainClient:
    """Factory function to create MarketCandleDataDomainClient."""
    return MarketCandleDataDomainClient(**kwargs)


def create_market_tick_data_client(**kwargs) -> MarketTickDataDomainClient:
    """Factory function to create MarketTickDataDomainClient."""
    return MarketTickDataDomainClient(**kwargs)


def create_execution_client(**kwargs) -> ExecutionDomainClient:
    """Factory function to create ExecutionDomainClient."""
    return ExecutionDomainClient(**kwargs)


# Deprecated: Keep for backward compatibility
def create_market_data_client(**kwargs) -> MarketDataDomainClient:
    """
    ⚠️ DEPRECATED: Use create_market_candle_data_client() or create_market_tick_data_client() instead.

    Factory function to create MarketDataDomainClient (deprecated).
    """
    import warnings

    warnings.warn(
        "create_market_data_client() is deprecated. Use create_market_candle_data_client() or create_market_tick_data_client() instead. "
        "See docs/CLIENTS_DEPRECATION_GUIDE.md for migration details.",
        DeprecationWarning,
        stacklevel=2,
    )
    return MarketDataDomainClient(**kwargs)


def create_features_client(feature_type: str = "delta_one", **kwargs) -> FeaturesDomainClient:
    """Factory function to create FeaturesDomainClient."""
    return FeaturesDomainClient(feature_type=feature_type, **kwargs)
