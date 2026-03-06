"""
Cloud Data Provider Base

Provides a centralized base class for cloud data access across services.
Each service can extend this class for domain-specific functionality.

Domains supported:
- instruments: Instrument definitions and availability
- market_data: Tick data, candles, HFT features
- features: Computed features (delta-one, volatility, onchain)
- ml: ML models and predictions
"""

import logging
import sys
from abc import ABC
from datetime import UTC, datetime

import pandas as pd
from unified_config_interface import UnifiedCloudConfig

from .standardized_service import (
    StandardizedDomainCloudService,
)

logger = logging.getLogger(__name__)


class CloudDataProviderBase(ABC):
    """
    Base class for cloud data providers.

    Provides common functionality for:
    - GCS read/write operations
    - BigQuery queries
    - Category-specific bucket selection
    - Test mode detection

    Services should extend this class and add domain-specific methods.
    """

    def __init__(
        self,
        domain: str,
        bucket: str = "",
        project_id: str | None = None,
        gcs_bucket: str | None = None,
        bigquery_dataset: str | None = None,
        bigquery_location: str | None = None,
        cloud_target: object | None = None,
    ):
        """
        Initialize cloud data provider.

        Args:
            domain: Domain name (e.g., 'instruments', 'market_data', 'features', 'ml')
            bucket: Cloud storage bucket name (preferred)
            project_id: GCP project ID (used for bucket resolution if bucket not set)
            gcs_bucket: Alternate bucket arg — used if bucket not set
            bigquery_dataset: Ignored — kept for backward compatibility
            bigquery_location: Ignored — kept for backward compatibility
            cloud_target: Ignored — kept for backward compatibility
        """
        self.domain = domain
        config = UnifiedCloudConfig()

        resolved_bucket = bucket or gcs_bucket or config.gcs_bucket or f"{domain}-store"
        self.bucket: str = resolved_bucket

        # Create domain cloud service
        self.cloud_service = StandardizedDomainCloudService(
            domain=domain,
            bucket=resolved_bucket,
        )

        logger.info(
            "✅ CloudDataProviderBase initialized: domain=%s, bucket=%s",
            domain,
            resolved_bucket,
        )

    @property
    def is_test_mode(self) -> bool:
        """Check if running in test mode."""
        config = UnifiedCloudConfig()
        return config.is_testing or "pytest" in sys.modules

    def download_from_gcs(
        self,
        gcs_path: str,
        format: str = "parquet",
        log_errors: bool = True,
    ) -> pd.DataFrame:
        """
        Download data from GCS.

        Args:
            gcs_path: Path within the bucket
            format: File format ('parquet', 'csv', 'json')
            log_errors: Whether to log errors

        Returns:
            DataFrame with downloaded data
        """
        try:
            logger.info("📥 Loading from GCS: %s/%s", self.bucket, gcs_path)
            result = self.cloud_service.download_from_gcs(
                gcs_path=gcs_path,
                format=format,
                log_errors=log_errors,
            )
            df = result if isinstance(result, pd.DataFrame) else pd.DataFrame()

            if df.empty:
                logger.warning("⚠️ No data found at %s", gcs_path)
            else:
                logger.info("✅ Loaded %s rows from GCS", len(df))

            return df

        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            error_msg = str(e)
            # Handle 404/Not Found gracefully
            if "404" in error_msg or "Not Found" in error_msg or "No such object" in error_msg:
                logger.info("ℹ️ No data found (404): %s", gcs_path)
                return pd.DataFrame()

            if log_errors:
                logger.error("❌ Failed to load from GCS: %s", e)
            return pd.DataFrame()

    def _build_category_service(self, category: str) -> tuple[str, "StandardizedDomainCloudService"]:
        """Build a category-specific cloud service. Returns (bucket_name, service)."""
        config = UnifiedCloudConfig()
        try:
            category_bucket = config.get_bucket(self.domain, category)
        except ValueError:
            category_bucket = f"{self.domain}-{category.lower()}"

        category_service = StandardizedDomainCloudService(
            domain=self.domain,
            bucket=category_bucket,
        )
        return category_bucket, category_service

    def download_from_category_bucket(
        self,
        gcs_path: str,
        category: str,
        format: str = "parquet",
    ) -> pd.DataFrame:
        """Download data from a category-specific bucket.

        Args:
            gcs_path: Path within the bucket
            category: Market category ('CEFI', 'TRADFI', 'DEFI')
            format: File format

        Returns:
            DataFrame with downloaded data
        """
        try:
            category_bucket, category_service = self._build_category_service(category)
            logger.info("📥 Loading %s data from: %s/%s", category, category_bucket, gcs_path)
            result = category_service.download_from_gcs(gcs_path=gcs_path, format=format, log_errors=False)
            df = result if isinstance(result, pd.DataFrame) else pd.DataFrame()

            if df.empty:
                logger.warning("⚠️ No %s data found at %s/%s", category, category_bucket, gcs_path)
            else:
                logger.info("✅ Loaded %s %s rows from GCS", len(df), category)
            return df

        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            error_msg = str(e)
            if "404" in error_msg or "Not Found" in error_msg or "No such object" in error_msg:
                logger.info("ℹ️ No %s data found (404): %s", category, gcs_path)
                return pd.DataFrame()
            logger.error("❌ Failed to load %s data from GCS: %s", category, e)
            return pd.DataFrame()

    def query_bigquery(
        self,
        query: str,
        parameters: dict[str, object] | None = None,
    ) -> pd.DataFrame:
        """
        Execute a BigQuery query.

        Args:
            query: SQL query string
            parameters: Query parameters

        Returns:
            DataFrame with query results
        """
        try:
            logger.info("📥 Executing BigQuery query")
            result = self.cloud_service.query_bigquery(
                query=query,
                parameters=parameters or {},
            )
            logger.info("✅ Query returned %s rows", len(result))
            return result

        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("❌ BigQuery query failed: %s", e)
            return pd.DataFrame()

    def upload_to_gcs(
        self,
        df: pd.DataFrame,
        gcs_path: str,
        format: str = "parquet",
    ) -> bool:
        """
        Upload data to GCS.

        Args:
            df: DataFrame to upload
            gcs_path: Path within the bucket
            format: File format

        Returns:
            True if successful
        """
        try:
            logger.info("📤 Uploading %s rows to GCS: %s", len(df), gcs_path)
            self.cloud_service.upload_to_gcs(
                data=df,
                gcs_path=gcs_path,
                format=format,
            )
            logger.info("✅ Upload complete: %s", gcs_path)
            return True

        except (ConnectionError, TimeoutError, OSError, ValueError) as e:
            logger.error("❌ Failed to upload to GCS: %s", e)
            return False

    def check_gcs_exists(self, gcs_path: str) -> bool:
        """
        Check if a GCS path exists.

        Args:
            gcs_path: Path within the bucket

        Returns:
            True if exists
        """
        try:
            df = self.download_from_gcs(gcs_path, log_errors=False)
            return not df.empty
        except (ConnectionError, TimeoutError, OSError, ValueError):
            return False


def _resolve_instruments_bucket_cefi() -> str:
    """Resolve instruments CEFI bucket from config. Fails if not configured."""
    config = UnifiedCloudConfig()
    bucket = config.instruments_gcs_bucket
    if bucket:
        return bucket
    proj = config.gcp_project_id
    if not proj:
        raise ValueError(
            "INSTRUMENTS_GCS_BUCKET or GCP_PROJECT_ID must be set in config. No hardcoded fallbacks allowed."
        )
    return f"instruments-store-cefi-{proj}"


class InstrumentsDataProvider(CloudDataProviderBase):
    """Data provider for instruments domain."""

    def __init__(self, cloud_target: object | None = None):
        config = UnifiedCloudConfig()
        super().__init__(
            domain="instruments",
            gcs_bucket=_resolve_instruments_bucket_cefi(),
            bigquery_dataset=config.instruments_bigquery_dataset,
        )

    def get_instruments_for_date(
        self,
        date: datetime,
        category: str | None = None,
        venue: str | None = None,
        instrument_type: str | None = None,
    ) -> pd.DataFrame:
        """
        Get instruments for a specific date.

        Args:
            date: Target date
            category: Optional market category ('CEFI', 'TRADFI', 'DEFI')
            venue: Optional venue filter
            instrument_type: Optional instrument type filter

        Returns:
            DataFrame with instruments
        """
        date_str = date.strftime("%Y-%m-%d")
        gcs_path = f"instrument_availability/by_date/day={date_str}/instruments.parquet"

        if category:
            df = self.download_from_category_bucket(gcs_path, category)
        else:
            df = self.download_from_gcs(gcs_path)

        # Apply filters
        if venue and not df.empty:
            df = df[df["venue"] == venue]
        if instrument_type and not df.empty:
            df = df[df["instrument_type"] == instrument_type]

        return df

    def check_instruments_exist(
        self,
        date: datetime,
        categories: list[str] | None = None,
    ) -> bool:
        """
        Check if instruments exist for a date.

        Args:
            date: Target date
            categories: Categories to check (default: all)

        Returns:
            True if instruments exist
        """
        if categories is None:
            categories = ["CEFI", "TRADFI", "DEFI"]

        for category in categories:
            df = self.get_instruments_for_date(date, category=category)
            if not df.empty:
                return True

        return False


class MarketDataProvider(CloudDataProviderBase):
    """Data provider for market_data domain."""

    def __init__(self, cloud_target: object | None = None):
        config = UnifiedCloudConfig()
        super().__init__(
            domain="market_data",
            gcs_bucket=config.market_data_gcs_bucket,
            bigquery_dataset=config.market_data_bigquery_dataset,
        )

    def _build_candles_query(
        self,
        instrument_id: str,
        timeframe: str,
        start_date: datetime,
        end_date: datetime,
        limit: int | None,
    ) -> tuple[str, dict[str, object]]:
        """Build BigQuery query and params for candle data retrieval."""
        table_suffix = timeframe.replace("h", "h").replace("m", "m").replace("s", "s")
        table_name = f"candles_{table_suffix}_trades"
        dataset = self.domain
        project_id = ""

        query = f"""
        SELECT *
        FROM `{project_id}.{dataset}.{table_name}`
        WHERE instrument_id = @instrument_id
          AND timestamp >= @start_time
          AND timestamp < @end_time
        ORDER BY timestamp ASC
        """  # nosec B608 — table name from config (project_id/dataset/timeframe), user values use @params

        params: dict[str, object] = {
            "instrument_id": instrument_id,
            "start_time": start_date.isoformat(),
            "end_time": end_date.isoformat(),
        }
        if limit is not None and limit > 0:
            query += " LIMIT @limit"
            params["limit"] = limit
        return query, params

    def get_candles(
        self,
        instrument_id: str,
        timeframe: str,
        start_date: datetime,
        end_date: datetime,
        limit: int | None = None,
    ) -> pd.DataFrame:
        """Get candles from BigQuery.

        Args:
            instrument_id: Canonical instrument key
            timeframe: Candle timeframe ('1m', '5m', '1h', etc.)
            start_date: Start datetime
            end_date: End datetime
            limit: Maximum rows to return

        Returns:
            DataFrame with OHLCV data
        """
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=UTC)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=UTC)
        query, params = self._build_candles_query(instrument_id, timeframe, start_date, end_date, limit)
        return self.query_bigquery(query, params)


class FeaturesDataProvider(CloudDataProviderBase):
    """Data provider for features domain."""

    def __init__(self, cloud_target: object | None = None):
        config = UnifiedCloudConfig()
        super().__init__(
            domain="features",
            gcs_bucket=config.features_gcs_bucket,
            bigquery_dataset=config.bigquery_dataset,
        )

    def get_features_for_date(
        self,
        date: datetime,
        feature_type: str = "delta_one",
        instrument_key: str | None = None,
    ) -> pd.DataFrame:
        """
        Get computed features for a date.

        Args:
            date: Target date
            feature_type: Feature type ('delta_one', 'volatility', 'onchain')
            instrument_key: Optional instrument filter

        Returns:
            DataFrame with features
        """
        date_str = date.strftime("%Y-%m-%d")
        gcs_path = f"{feature_type}/by_date/day={date_str}/features.parquet"

        df = self.download_from_gcs(gcs_path)

        if instrument_key and not df.empty and "instrument_key" in df.columns:
            df = df[df["instrument_key"] == instrument_key]

        return df
