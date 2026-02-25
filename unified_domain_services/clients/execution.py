"""
Execution domain client for accessing backtest results and execution data.

Provides methods for:
- Loading backtest summaries
- Loading fills, orders, positions
- Loading equity curves with byte-range streaming
- Listing available backtest runs
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import cast

import pandas as pd
from google.cloud import exceptions as gcs_exceptions
from unified_cloud_services import CloudTarget, get_storage_client, unified_config

from unified_domain_services import StandardizedDomainCloudService

logger = logging.getLogger(__name__)


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

        except gcs_exceptions.NotFound as e:
            logger.error(f"❌ Backtest summary not found: {e}")
            return {}
        except gcs_exceptions.GoogleCloudError as e:
            logger.error(f"❌ GCS error loading backtest summary: {e}")
            return {}
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"❌ Data processing error loading backtest summary: {e}")
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

        except gcs_exceptions.NotFound as e:
            logger.error(f"❌ Backtest fills not found: {e}")
            return pd.DataFrame()
        except gcs_exceptions.GoogleCloudError as e:
            logger.error(f"❌ GCS error loading backtest fills: {e}")
            return pd.DataFrame()
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"❌ Data processing error loading fills: {e}")
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

        except gcs_exceptions.NotFound as e:
            logger.error(f"❌ Backtest orders not found: {e}")
            return pd.DataFrame()
        except gcs_exceptions.GoogleCloudError as e:
            logger.error(f"❌ GCS error loading backtest orders: {e}")
            return pd.DataFrame()
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"❌ Data processing error loading orders: {e}")
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

        except gcs_exceptions.NotFound as e:
            logger.error(f"❌ Backtest positions not found: {e}")
            return pd.DataFrame()
        except gcs_exceptions.GoogleCloudError as e:
            logger.error(f"❌ GCS error loading backtest positions: {e}")
            return pd.DataFrame()
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"❌ Data processing error loading positions: {e}")
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

        except gcs_exceptions.NotFound as e:
            logger.error(f"❌ Equity curve not found: {e}")
            return pd.DataFrame()
        except gcs_exceptions.GoogleCloudError as e:
            logger.error(f"❌ GCS error loading equity curve: {e}")
            return pd.DataFrame()
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"❌ Data processing error loading equity curve: {e}")
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
            logger.info(f"📋 Listing backtest runs (prefix='{prefix}')")

            client = get_storage_client()
            blobs = client.list_blobs(
                bucket=self.cloud_target.gcs_bucket,
                prefix=f"backtest_results/{prefix}",
            )

            # Extract unique run IDs
            run_ids: set[str] = set()
            for blob_meta in blobs:
                parts = blob_meta.name.replace("backtest_results/", "").split("/")
                if parts and parts[0]:
                    run_ids.add(parts[0])

            run_ids_list = sorted(run_ids)
            logger.info(f"✅ Found {len(run_ids_list)} backtest runs")
            return run_ids_list

        except gcs_exceptions.GoogleCloudError as e:
            logger.error(f"❌ GCS error listing backtest runs: {e}")
            return []
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"❌ Error processing backtest runs list: {e}")
            return []
