"""
Instruments domain client for accessing instrument definitions.

Provides convenience methods for querying instrument definitions with:
- Date-based filtering
- Venue/instrument type filtering
- Symbol pattern matching
- Multi-criteria queries
"""

# pyright: reportAny=false, reportExplicitAny=false
# Storage client list_blobs/.prefixes and pandas Series index/value have incomplete stubs
# (see QUALITY_GATE_BYPASS_AUDIT.md 2.1).

from __future__ import annotations

import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from typing import TypedDict, cast

import pandas as pd
from google.cloud import exceptions as gcs_exceptions
from unified_cloud_services import CloudTarget, StandardizedDomainCloudService, get_storage_client, unified_config

from unified_domain_services.clients.base import _is_empty_or_na

logger = logging.getLogger(__name__)


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
        date_obj = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=UTC) if isinstance(date, str) else date

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

        except gcs_exceptions.NotFound as e:
            logger.error(f"❌ Instrument data not found for {date_str}: {e}")
            return pd.DataFrame()
        except gcs_exceptions.GoogleCloudError as e:
            logger.error(f"❌ GCS error loading instruments for {date_str}: {e}")
            return pd.DataFrame()
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"❌ Data processing error for instruments on {date_str}: {e}")
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
                # Storage client list_blobs returns iterator with .prefixes (incomplete stubs)
                iterator = bucket.list_blobs(prefix=base_prefix, delimiter="/")
                list(iterator)  # Consume to populate .prefixes
                raw_prefixes = getattr(iterator, "prefixes", None)
                prefixes = list(raw_prefixes) if raw_prefixes else []
                venue_folders = [p for p in prefixes if "venue=" in p]

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
                except gcs_exceptions.NotFound:
                    logger.debug(f"Venue file not found: {venue_prefix}")
                    return pd.DataFrame()
                except gcs_exceptions.GoogleCloudError as e:
                    logger.debug(f"GCS error loading {venue_prefix}: {e}")
                    return pd.DataFrame()

            # Parallel load all venue files
            all_dfs: list[pd.DataFrame] = []
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

        except gcs_exceptions.GoogleCloudError as e:
            logger.debug(f"GCS error in by-venue structure: {e}")
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

        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=UTC) if isinstance(end_date, str) else end_date

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
            result: dict[str, int] = {}
            for k, v in counts.items():
                result[str(k)] = int(v) if isinstance(v, (int, float)) else 0
            return result

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

            def has_data_type(data_types_str: str | object) -> bool:
                if not data_types_str:
                    return False
                available_types: list[str] = [str(dt).strip() for dt in str(data_types_str).split(",")]
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

        def is_expiring_soon(expiry_str: object) -> bool:
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
