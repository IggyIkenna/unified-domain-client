"""Liquidity domain clients — L2 book checkpoints, liquidation clusters, liquidity features."""

from __future__ import annotations

import logging

import pandas as pd
from unified_config_interface import build_bucket, build_path

from .base import BaseDataClient

logger = logging.getLogger(__name__)


class L2BookCheckpointClient(BaseDataClient):
    """Typed thin client for 1-minute L2 book checkpoints."""

    def get_checkpoints(
        self,
        date: str,
        venue: str,
        instrument_key: str,
        category: str = "cefi",
    ) -> pd.DataFrame:
        """Read 1-minute L2 book checkpoints for one instrument + day.

        Args:
            date: Partition date as YYYY-MM-DD.
            venue: Venue name (e.g. "BINANCE").
            instrument_key: Canonical instrument key (VENUE:TYPE:SYMBOL).
            category: Market category bucket suffix (default "cefi").

        Returns:
            DataFrame with columns: instrument_key, timestamp, sequence,
            bids, asks, bid_levels, ask_levels, best_bid, best_ask, mid_price.
        """
        bucket = build_bucket(
            "l2_book_checkpoints", project_id=self._config.gcp_project_id, category=category
        )
        safe_key = instrument_key.replace("/", "_").replace(":", "_")
        path = (
            build_path("l2_book_checkpoints", date=date, venue=venue)
            + f"instrument_key={safe_key}.parquet"
        )
        return self._read_parquet(bucket, path)

    def list_instrument_keys(
        self,
        date: str,
        venue: str,
        category: str = "cefi",
    ) -> list[str]:
        """List instrument keys that have checkpoint data for a given date+venue."""
        bucket = build_bucket(
            "l2_book_checkpoints", project_id=self._config.gcp_project_id, category=category
        )
        prefix = build_path("l2_book_checkpoints", date=date, venue=venue)
        blobs = self._list_blobs(bucket, prefix)
        keys: list[str] = []
        for blob in blobs:
            fname = blob.split("/")[-1]
            if fname.startswith("instrument_key=") and fname.endswith(".parquet"):
                safe_key = fname[len("instrument_key=") : -len(".parquet")]
                keys.append(safe_key)
        return sorted(keys)


class LiquidationClustersClient(BaseDataClient):
    """Typed thin client for liquidation cluster data (CoinGlass / Hyblock)."""

    def get_clusters(
        self,
        date: str,
        source: str,
        venue: str,
        instrument_key: str,
        category: str = "cefi",
    ) -> pd.DataFrame:
        """Read liquidation cluster data for one instrument + day + source.

        Args:
            date: Partition date as YYYY-MM-DD.
            source: Data provider: "coinglass" or "hyblock".
            venue: Underlying exchange (e.g. "BINANCE").
            instrument_key: Canonical instrument key.
            category: Market category bucket suffix (default "cefi").

        Returns:
            DataFrame with CanonicalLiquidationCluster fields.
        """
        bucket = build_bucket(
            "liquidation_clusters", project_id=self._config.gcp_project_id, category=category
        )
        safe_key = instrument_key.replace("/", "_").replace(":", "_")
        path = (
            build_path("liquidation_clusters", date=date, source=source, venue=venue)
            + f"instrument_key={safe_key}.parquet"
        )
        return self._read_parquet(bucket, path)


class LiquidityFeaturesClient(BaseDataClient):
    """Typed thin client for 1-minute liquidity feature outputs."""

    def get_features(
        self,
        date: str,
        venue: str,
        instrument_key: str,
        category: str = "cefi",
    ) -> pd.DataFrame:
        """Read 1-minute liquidity features for one instrument + day.

        Args:
            date: Partition date as YYYY-MM-DD.
            venue: Venue name (e.g. "BINANCE").
            instrument_key: Canonical instrument key.
            category: Market category bucket suffix (default "cefi").

        Returns:
            DataFrame with all liquidity feature columns at 1-min resolution.
        """
        bucket = build_bucket(
            "liquidity_features_1m", project_id=self._config.gcp_project_id, category=category
        )
        safe_key = instrument_key.replace("/", "_").replace(":", "_")
        path = (
            build_path("liquidity_features_1m", date=date, venue=venue)
            + f"instrument_key={safe_key}.parquet"
        )
        return self._read_parquet(bucket, path)
