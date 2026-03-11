"""Instruments domain client."""

from __future__ import annotations

# pyright: reportAny=false, reportExplicitAny=false
# Storage client list_blobs / pandas Series.to_dict() have incomplete stubs.
import logging
import re
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from typing import TypedDict, cast

import pandas as pd
from unified_cloud_interface import get_storage_client
from unified_config_interface import UnifiedCloudConfig

from ..standardized_service import StandardizedDomainCloudService

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _get_cloud_config() -> UnifiedCloudConfig:
    """Return singleton UnifiedCloudConfig instance."""
    return UnifiedCloudConfig()


def _to_upper_list(val: str | list[str]) -> list[str]:
    """Normalize a comma-string or list to a list of upper-cased strings."""
    if isinstance(val, str):
        return [v.strip().upper() for v in val.split(",")]
    return [v.upper() for v in val]


def _is_empty_or_na(val: object) -> bool:
    if val is None:
        return True
    if isinstance(val, float) and val != val:
        return True
    if val == "":
        return True
    if isinstance(val, str) and not val.strip():
        return True
    return False


def _parse_aware_datetime(dt_str: object) -> datetime | None:
    """Parse an ISO datetime string to timezone-aware datetime. Returns None on failure."""
    if _is_empty_or_na(dt_str):
        return None
    try:
        dt = datetime.fromisoformat(str(dt_str).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt
    except (ValueError, AttributeError):
        return None


def _make_target_aware(target_date: datetime) -> datetime:
    """Return target_date as a timezone-aware datetime (UTC if naive)."""
    return target_date if target_date.tzinfo else target_date.replace(tzinfo=UTC)


def _apply_symbol_filter(df: pd.DataFrame, symbol_pattern: str) -> pd.DataFrame:
    """Filter DataFrame by symbol regex pattern (case-insensitive).

    Returns df unchanged on regex error.
    """
    try:
        pattern = re.compile(symbol_pattern, re.IGNORECASE)
        return df.loc[df["symbol"].str.match(pattern)]
    except re.error as e:
        logger.warning("Invalid regex pattern '%s': %s", symbol_pattern, e)
        return df


def _normalize_instrument_ids(instrument_ids: list[str] | str) -> list[str]:
    """Normalize instrument_ids to a list of stripped strings."""
    if isinstance(instrument_ids, str):
        return [i.strip() for i in instrument_ids.split(",")]
    return instrument_ids


def _parse_ref_date(date: str | datetime) -> datetime:
    """Parse a date string or datetime to a UTC-aware datetime."""
    if isinstance(date, str):
        return datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=UTC)
    return date if date.tzinfo else date.replace(tzinfo=UTC)


def _is_expiring_in_window(expiry_str: object, ref_date: datetime, cutoff_date: datetime) -> bool:
    """Return True if the expiry datetime falls within [ref_date, cutoff_date]."""
    if not expiry_str:
        return False
    try:
        expiry_dt = datetime.fromisoformat(str(expiry_str).replace("Z", "+00:00"))
        if expiry_dt.tzinfo is None:
            expiry_dt = expiry_dt.replace(tzinfo=UTC)
        return ref_date <= expiry_dt <= cutoff_date
    except (ValueError, TypeError):
        return False


class InstrumentSummaryStats(TypedDict, total=False):  # CORRECT-LOCAL
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
    """Client for accessing instruments domain data."""

    def __init__(
        self,
        project_id: str | None = None,
        storage_bucket: str | None = None,
        analytics_dataset: str | None = None,
    ) -> None:
        resolved_bucket = storage_bucket or _get_cloud_config().instruments_gcs_bucket
        self.cloud_service = StandardizedDomainCloudService(
            domain="instruments", bucket=resolved_bucket
        )
        self._bucket = resolved_bucket
        logger.info("InstrumentsDomainClient initialized: bucket=%s", resolved_bucket)

    def _load_and_filter_for_date(
        self,
        date_str: str,
        date_obj: datetime,
        venues_to_load: list[str] | None,
    ) -> pd.DataFrame:
        """Load instruments by venue and apply date-availability filter."""
        instruments_df = self._load_instruments_by_venue(date_str, venues_to_load)
        if instruments_df.empty:
            bucket = self._bucket or "unknown"
            logger.error(
                "No instrument definitions found for %s. Expected: gs://%s/instrument_availability/by_date/day=%s/venue=<VENUE>/instruments.parquet",
                date_str,
                bucket,
                date_str,
            )
            return pd.DataFrame()
        instruments_df = self._filter_by_date_availability(instruments_df, date_obj)
        if instruments_df.empty:
            logger.warning("No instruments available for %s after date filtering", date_str)
        return instruments_df

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
        """Get instrument definitions for a specific date with filtering."""
        date_obj = (
            datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=UTC)
            if isinstance(date, str)
            else date
        )
        date_str = date_obj.strftime("%Y-%m-%d")
        try:
            venues_to_load = venues or ([venue] if venue else None)
            instruments_df = self._load_and_filter_for_date(date_str, date_obj, venues_to_load)
            if instruments_df.empty:
                return pd.DataFrame()
            venue_filter = venue if not venues_to_load else None
            return self._apply_filters(
                instruments_df,
                venue_filter,
                instrument_type,
                base_currency,
                quote_currency,
                symbol_pattern,
                instrument_ids,
            )
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to load instruments for %s: %s", date_str, e)
            return pd.DataFrame()

    def _load_venue_parquet(self, venue_prefix: str) -> pd.DataFrame:
        """Load instruments parquet for a single venue prefix."""
        try:
            gcs_path = f"{venue_prefix}instruments.parquet"
            result = self.cloud_service.download_from_gcs(gcs_path=gcs_path, format="parquet")
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.warning("Could not load %s: %s", venue_prefix, e)
            return pd.DataFrame()

    def _load_instruments_by_venue(
        self, date_str: str, venues: list[str] | None = None
    ) -> pd.DataFrame:
        try:
            client = get_storage_client()
            bucket = client.bucket(self._bucket)
            base_prefix = f"instrument_availability/by_date/day={date_str}/"
            if venues:
                venue_folders: list[str] = [f"{base_prefix}venue={v}/" for v in venues]
            else:
                # GCS client list_blobs() returns untyped object; cast to Iterable.
                iterator: Iterable[object] = cast(
                    Iterable[object], bucket.list_blobs(prefix=base_prefix, delimiter="/")
                )
                list(iterator)
                raw_prefixes: list[str] | None = cast(
                    list[str] | None, getattr(iterator, "prefixes", None)
                )
                prefixes: list[str] = list(raw_prefixes) if raw_prefixes is not None else []
                venue_folders = [p for p in prefixes if "venue=" in p]
            if not venue_folders:
                return pd.DataFrame()
            all_dfs: list[pd.DataFrame] = []
            with ThreadPoolExecutor(max_workers=min(12, len(venue_folders))) as executor:
                futures = {
                    executor.submit(self._load_venue_parquet, vf): vf for vf in venue_folders
                }
                for future in as_completed(futures):
                    df = future.result()
                    if not df.empty:
                        all_dfs.append(df)
            if all_dfs:
                return pd.concat(all_dfs, ignore_index=True)
            return pd.DataFrame()
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.warning("Could not load by-venue structure: %s", e)
            return pd.DataFrame()

    def _filter_by_date_availability(self, df: pd.DataFrame, target_date: datetime) -> pd.DataFrame:
        if df.empty:
            return df

        filtered_df = df.copy()
        target_aware = _make_target_aware(target_date)

        if "available_from_datetime" in filtered_df.columns:

            def is_available_from(from_datetime_str: object) -> bool:
                from_date = _parse_aware_datetime(from_datetime_str)
                return from_date is None or target_aware >= from_date

            filtered_df = filtered_df.loc[
                filtered_df["available_from_datetime"].apply(is_available_from)
            ]

        if "available_to_datetime" in filtered_df.columns:

            def is_available_to(to_datetime_str: object) -> bool:
                to_date = _parse_aware_datetime(to_datetime_str)
                return to_date is None or target_aware <= to_date

            filtered_df = filtered_df.loc[
                filtered_df["available_to_datetime"].apply(is_available_to)
            ]

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
        if venue:
            df = df.loc[df["venue"].isin(_to_upper_list(venue))]
        if instrument_type:
            df = df.loc[df["instrument_type"].isin(_to_upper_list(instrument_type))]
        if base_currency:
            df = df.loc[df["base_asset"].isin(_to_upper_list(base_currency))]
        if quote_currency:
            df = df.loc[df["quote_asset"].isin(_to_upper_list(quote_currency))]
        if symbol_pattern:
            df = _apply_symbol_filter(df, symbol_pattern)
        if instrument_ids:
            ids = _normalize_instrument_ids(instrument_ids)
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
        """Get instruments across a date range (union of all dates)."""
        if isinstance(start_date, str):
            start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=UTC)
        else:
            start_dt = start_date

        if isinstance(end_date, str):
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=UTC)
        else:
            end_dt = end_date

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
            return pd.DataFrame()

        combined_df = pd.concat(all_instruments, ignore_index=True)
        return combined_df.drop_duplicates(subset=["instrument_key"], keep="first")

    def _optional_coverage_stats(self, df: pd.DataFrame) -> InstrumentSummaryStats:
        """Compute optional coverage stats for ccxt_symbol and data_types columns."""
        extra: InstrumentSummaryStats = {}
        if "ccxt_symbol" in df.columns:
            with_ccxt = len(df[df["ccxt_symbol"] != ""])
            extra["ccxt_coverage"] = {
                "instruments_with_ccxt": with_ccxt,
                "ccxt_coverage_percent": with_ccxt / len(df) * 100,
            }
        if "data_types" in df.columns:
            extra["data_type_coverage"] = {
                dt: len(df[df["data_types"].str.contains(dt, na=False)])
                for dt in [
                    "trades",
                    "book_snapshot_5",
                    "derivative_ticker",
                    "liquidations",
                    "options_chain",
                ]
            }
        return extra

    def get_summary_stats(self, date: str | datetime) -> InstrumentSummaryStats:
        """Get summary statistics for instruments on a specific date."""
        instruments_df = self.get_instruments_for_date(date)
        if instruments_df.empty:
            return {"total_instruments": 0, "error": "No instruments found"}

        def _to_str_dict(counts: pd.Series[int]) -> dict[str, int]:
            return {str(k): int(v) for k, v in counts.to_dict().items()}

        stats: InstrumentSummaryStats = {
            "total_instruments": len(instruments_df),
            "venues": int(instruments_df["venue"].nunique()),
            "venue_breakdown": _to_str_dict(instruments_df["venue"].value_counts()),
            "instrument_types": int(instruments_df["instrument_type"].nunique()),
            "type_breakdown": _to_str_dict(instruments_df["instrument_type"].value_counts()),
            "base_currencies": int(instruments_df["base_asset"].nunique()),
            "quote_currencies": int(instruments_df["quote_asset"].nunique()),
            "top_base_currencies": _to_str_dict(
                instruments_df["base_asset"].value_counts().head(10)
            ),
            "top_quote_currencies": _to_str_dict(
                instruments_df["quote_asset"].value_counts().head(10)
            ),
        }
        stats.update(self._optional_coverage_stats(instruments_df))
        return stats

    def get_instrument_details(
        self, date: str | datetime, instrument_id: str
    ) -> dict[str, str | int | float | bool | None] | None:
        """Get detailed information for a specific instrument ID."""
        instruments_df = self.get_instruments_for_date(date, instrument_ids=[instrument_id])

        if instruments_df.empty:
            return None

        return cast(
            dict[str, str | int | float | bool | None],
            instruments_df.iloc[0].to_dict(),
        )

    def get_trading_parameters(
        self, date: str | datetime, instrument_id: str
    ) -> dict[str, str | int | float | bool | list[str] | None] | None:
        """Get trading parameters for an instrument."""
        instrument = self.get_instrument_details(date, instrument_id)
        if not instrument:
            return None

        return {
            "tick_size": instrument.get("tick_size") or "",
            "min_size": instrument.get("min_size") or "",
            "contract_size": instrument.get("contract_size"),
            "ccxt_symbol": instrument.get("ccxt_symbol") or "",
            "ccxt_exchange": instrument.get("ccxt_exchange") or "",
            "inverse": instrument.get("inverse", False),
            "data_types": (
                [s.strip() for s in str(instrument.get("data_types")).split(",")]
                if instrument.get("data_types")
                else []
            ),
        }

    def get_instruments_by_data_type(
        self,
        date: str | datetime,
        data_type: str,
        venue: str | None = None,
        limit: int = 1000,
    ) -> pd.DataFrame:
        """Get instruments that support a specific data type."""
        instruments_df = self.get_instruments_for_date(date, venue=venue)

        if instruments_df.empty:
            return pd.DataFrame()

        if "data_types" in instruments_df.columns:

            def has_data_type(data_types_str: str | object) -> bool:
                if not data_types_str:
                    return False
                return data_type in [str(dt).strip() for dt in str(data_types_str).split(",")]

            filtered_df = instruments_df[instruments_df["data_types"].apply(has_data_type)]
        else:
            filtered_df = pd.DataFrame()

        return filtered_df.head(limit) if len(filtered_df) > limit else filtered_df

    def search_instruments_by_symbol(
        self,
        date: str | datetime,
        symbol_pattern: str,
        venue: str | None = None,
        limit: int = 100,
    ) -> pd.DataFrame:
        """Search instruments by symbol pattern (regex supported)."""
        instruments_df = self.get_instruments_for_date(
            date, venue=venue, symbol_pattern=symbol_pattern
        )
        return instruments_df.head(limit) if len(instruments_df) > limit else instruments_df

    def get_expiring_instruments(
        self,
        date: str | datetime,
        days_until_expiry: int = 30,
        instrument_type: str | None = None,
    ) -> pd.DataFrame:
        """Get instruments expiring within specified days."""
        instruments_df = self.get_instruments_for_date(date, instrument_type=instrument_type)

        if instruments_df.empty or "available_to_datetime" not in instruments_df.columns:
            return pd.DataFrame()

        expiring_df = instruments_df[instruments_df["available_to_datetime"].notna()]

        if expiring_df.empty:
            return pd.DataFrame()

        ref_date = _parse_ref_date(date)
        cutoff_date = ref_date + timedelta(days=days_until_expiry)

        def _check_expiry(expiry_str: object) -> bool:
            return _is_expiring_in_window(expiry_str, ref_date, cutoff_date)

        return expiring_df[expiring_df["available_to_datetime"].apply(_check_expiry)]
