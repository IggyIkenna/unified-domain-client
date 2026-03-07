"""Execution domain client — fills, orders, backtest results, nautilus catalog."""

from __future__ import annotations

# pyright: reportAny=false, reportExplicitAny=false
import io
import logging
from datetime import datetime
from typing import NotRequired, TypedDict, Unpack

import pandas as pd
from unified_cloud_interface import get_storage_client
from unified_config_interface import UnifiedCloudConfig

from ..paths import build_bucket, build_path
from ..standardized_service import StandardizedDomainCloudService

logger = logging.getLogger(__name__)


class _ExecClientConfig(TypedDict, total=False):
    """Typed kwargs for ExecutionDomainClient factory."""

    project_id: NotRequired[str | None]
    storage_bucket: NotRequired[str | None]


class ExecutionDomainClient:
    """Client for accessing execution domain data (fills, orders, backtest results)."""

    def __init__(
        self,
        project_id: str | None = None,
        storage_bucket: str | None = None,
        analytics_dataset: str | None = None,
    ) -> None:
        proj = project_id or UnifiedCloudConfig().gcp_project_id
        bucket = storage_bucket or UnifiedCloudConfig().execution_gcs_bucket
        self.cloud_service = StandardizedDomainCloudService(domain="execution", bucket=bucket)
        self._project_id = proj
        self._bucket = bucket
        logger.info("ExecutionDomainClient initialized: bucket=%s", bucket)

    # ---------------------------------------------------------------------- #
    # Fills and orders
    # ---------------------------------------------------------------------- #

    def get_fills(self, date: str, category: str = "cefi") -> pd.DataFrame:
        """Get execution fills for a specific date."""
        bucket = build_bucket("execution_fills", project_id=self._project_id, category=category)
        path = build_path("execution_fills", date=date) + "fills.parquet"
        try:
            client = get_storage_client(project_id=self._project_id)
            raw = client.download_bytes(bucket, path)
            return pd.read_parquet(io.BytesIO(raw))
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to load fills %s: %s", date, e)
            return pd.DataFrame()

    def get_orders(self, date: str, category: str = "cefi") -> pd.DataFrame:
        """Get execution orders for a specific date."""
        bucket = build_bucket("execution_fills", project_id=self._project_id, category=category)
        path = build_path("execution_fills", date=date) + "orders.parquet"
        try:
            client = get_storage_client(project_id=self._project_id)
            raw = client.download_bytes(bucket, path)
            return pd.read_parquet(io.BytesIO(raw))
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to load orders %s: %s", date, e)
            return pd.DataFrame()

    def get_nautilus_catalog_path(self, instrument_id: str, category: str = "cefi") -> str:
        """Get the GCS URI for a nautilus catalog data file."""
        bucket = build_bucket("nautilus_catalog", project_id=self._project_id, category=category)
        path = build_path("nautilus_catalog", instrument_id=instrument_id) + "data.parquet"
        return f"gs://{bucket}/{path}"

    # ---------------------------------------------------------------------- #
    # Backtest helpers (legacy interface retained from monolithic clients.py)
    # ---------------------------------------------------------------------- #

    def get_backtest_summary(self, run_id: str) -> dict[str, object]:
        """Load backtest summary JSON."""
        gcs_path = f"backtest_results/{run_id}/summary.json"
        try:
            summary = self.cloud_service.download_from_gcs(gcs_path=gcs_path, format="json")
            return summary if isinstance(summary, dict) else {}
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to load backtest summary: %s", e)
            return {}

    def get_backtest_fills(self, run_id: str) -> pd.DataFrame:
        """Load all fills from a backtest run."""
        gcs_path = f"backtest_results/{run_id}/fills.parquet"
        try:
            result = self.cloud_service.download_from_gcs(gcs_path=gcs_path, format="parquet")
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to load backtest fills: %s", e)
            return pd.DataFrame()

    def get_backtest_orders(self, run_id: str) -> pd.DataFrame:
        """Load all orders from a backtest run."""
        gcs_path = f"backtest_results/{run_id}/orders.parquet"
        try:
            result = self.cloud_service.download_from_gcs(gcs_path=gcs_path, format="parquet")
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to load backtest orders: %s", e)
            return pd.DataFrame()

    def get_backtest_positions(self, run_id: str) -> pd.DataFrame:
        """Load position timeline from a backtest run."""
        gcs_path = f"backtest_results/{run_id}/positions.parquet"
        try:
            result = self.cloud_service.download_from_gcs(gcs_path=gcs_path, format="parquet")
            return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to load backtest positions: %s", e)
            return pd.DataFrame()

    def get_equity_curve(
        self,
        run_id: str,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> pd.DataFrame:
        """Load equity curve with optional time filtering."""
        gcs_path = f"backtest_results/{run_id}/equity_curve.parquet"
        try:
            result = self.cloud_service.download_from_gcs(gcs_path=gcs_path, format="parquet")
            equity = result if isinstance(result, pd.DataFrame) else pd.DataFrame()

            if not equity.empty and start_time and end_time and "ts_event" in equity.columns:
                mask = (equity["ts_event"] >= start_time) & (equity["ts_event"] <= end_time)
                equity = equity.loc[mask]

            return equity
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to load equity curve: %s", e)
            return pd.DataFrame()

    def list_backtest_runs(self, prefix: str = "") -> list[str]:
        """List available backtest run IDs."""
        try:
            client = get_storage_client()
            blobs = client.list_blobs(
                bucket=self._bucket,
                prefix=f"backtest_results/{prefix}",
            )
            run_ids: set[str] = set()
            for blob_meta in blobs:
                parts = blob_meta.name.replace("backtest_results/", "").split("/")
                if parts and parts[0]:
                    run_ids.add(parts[0])
            return sorted(run_ids)
        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("Failed to list backtest runs: %s", e)
            return []


def create_execution_client(**kwargs: Unpack[_ExecClientConfig]) -> ExecutionDomainClient:
    """Factory function to create ExecutionDomainClient."""
    return ExecutionDomainClient(**kwargs)
