"""Strategy domain client — orders, instructions, backtest results."""

from __future__ import annotations

import logging

import pandas as pd
from unified_cloud_interface import get_storage_client
from unified_config_interface import UnifiedCloudConfig

from ..paths import build_bucket, build_path
from ..standardized_service import StandardizedDomainCloudService

logger = logging.getLogger(__name__)


class StrategyDomainClient:
    """Client for reading strategy orders, instructions, and backtest results."""

    def __init__(
        self,
        project_id: str | None = None,
        gcs_bucket: str | None = None,
    ) -> None:
        self._project_id = project_id or UnifiedCloudConfig().gcp_project_id
        bucket = gcs_bucket or build_bucket("strategy_orders", project_id=self._project_id)
        self.cloud_service = StandardizedDomainCloudService(domain="strategy", bucket=bucket)
        self._bucket = bucket

    def get_orders(self, date: str, strategy_id: str) -> pd.DataFrame:
        """Get strategy orders for a specific date and strategy."""
        path = build_path("strategy_orders", date=date, strategy_id=strategy_id) + "orders.parquet"
        try:
            result = self.cloud_service.download_from_gcs(gcs_path=path, format="parquet")
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to load orders %s/%s: %s", date, strategy_id, e)
            return pd.DataFrame()

    def get_instructions(self, strategy_id: str, date: str) -> pd.DataFrame:
        """Get strategy instructions for a specific strategy and date."""
        path = build_path("strategy_instructions", strategy_id=strategy_id, date=date) + "instructions.parquet"
        try:
            result = self.cloud_service.download_from_gcs(gcs_path=path, format="parquet")
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to load instructions %s/%s: %s", strategy_id, date, e)
            return pd.DataFrame()

    def get_backtest_results(self, strategy_id: str, run_id: str) -> dict[str, pd.DataFrame]:
        """Get all backtest result files (instructions, positions, pnl_attribution)."""
        prefix = build_path("backtest_results", strategy_id=strategy_id, run_id=run_id)
        results: dict[str, pd.DataFrame] = {}
        files = ["instructions.parquet", "positions.parquet", "pnl_attribution.parquet"]
        for filename in files:
            path = prefix + filename
            try:
                result = self.cloud_service.download_from_gcs(gcs_path=path, format="parquet")
                if isinstance(result, pd.DataFrame):
                    results[filename.replace(".parquet", "")] = result
            except (ConnectionError, TimeoutError, OSError, ValueError) as e:
                logger.debug("Could not load %s: %s", path, e)
        return results

    def list_strategies(self) -> list[str]:
        """List all strategy IDs that have stored orders."""
        try:
            client = get_storage_client(project_id=self._project_id)
            blobs = client.list_blobs(self._bucket, prefix="strategy_orders/by_date/")
            strategies: set[str] = set()
            for blob in blobs:
                for part in blob.name.split("/"):
                    if part.startswith("strategy_id="):
                        strategies.add(part[12:])
            return sorted(strategies)
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to list strategies: %s", e)
            return []
