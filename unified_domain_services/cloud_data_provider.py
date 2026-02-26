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
from abc import ABC, abstractmethod
from datetime import UTC, datetime

import pandas as pd
from unified_cloud_services import CloudTarget, StandardizedDomainCloudService, get_bucket_for_category, unified_config

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

    @abstractmethod
    def get_provider_id(self) -> str:
        """Return unique identifier for this provider. Subclasses must implement."""
        ...

    def __init__(
        self,
        domain: str,
        cloud_target: CloudTarget | None = None,
        project_id: str | None = None,
        gcs_bucket: str | None = None,
        bigquery_dataset: str | None = None,
        bigquery_location: str | None = None,
    ):
        """
        Initialize cloud data provider.

        Args:
            domain: Domain name (e.g., 'instruments', 'market_data', 'features', 'ml')
            cloud_target: Optional CloudTarget configuration (auto-detects if not provided)
            project_id: GCP project ID (overrides cloud_target)
            gcs_bucket: GCS bucket name (overrides cloud_target)
            bigquery_dataset: BigQuery dataset name (overrides cloud_target)
            bigquery_location: BigQuery location (overrides cloud_target)
        """
        self.domain = domain

        # Build cloud target from parameters or defaults
        if cloud_target is None:
            proj = project_id or unified_config.gcp_project_id
            if not proj:
                raise ValueError("GCP_PROJECT_ID must be set in config or environment. No hardcoded fallbacks allowed.")
            cloud_target = CloudTarget(
                project_id=proj,
                gcs_bucket=gcs_bucket or unified_config.gcs_bucket,
                bigquery_dataset=bigquery_dataset or getattr(unified_config, f"{domain}_bigquery_dataset", None),
                bigquery_location=bigquery_location or unified_config.bigquery_location,
            )

        # Create domain cloud service
        self.cloud_service = StandardizedDomainCloudService(
            domain=domain,
            cloud_target=cloud_target,
        )
        self.cloud_target = cloud_target

        logger.info(
            f"✅ CloudDataProviderBase initialized: "
            f"domain={domain}, project={cloud_target.project_id}, "
            f"bucket={cloud_target.gcs_bucket}, dataset={cloud_target.bigquery_dataset}"
        )

    @property
    def is_test_mode(self) -> bool:
        """Check if running in test mode."""
        environment = getattr(unified_config, "environment", "development").lower()
        pytest_current_test = getattr(unified_config, "pytest_current_test", "")
        pytest_env = getattr(unified_config, "pytest_env", "unknown")
        return environment in ["test", "testing"] or "pytest" in pytest_env or pytest_current_test != ""

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
            logger.info(f"📥 Loading from GCS: {self.cloud_target.gcs_bucket}/{gcs_path}")
            result = self.cloud_service.download_from_gcs(
                gcs_path=gcs_path,
                format=format,
                log_errors=log_errors,
            )
            df = result if isinstance(result, pd.DataFrame) else pd.DataFrame()

            if df.empty:
                logger.warning(f"⚠️ No data found at {gcs_path}")
            else:
                logger.info(f"✅ Loaded {len(df)} rows from GCS")

            return df

        except (KeyError, ValueError, TypeError) as e:
            if log_errors:
                logger.error(f"❌ Data processing error loading from GCS: {e}")
            return pd.DataFrame()
        except Exception as e:
            error_msg = str(e)
            # Handle 404/Not Found gracefully
            if "404" in error_msg or "Not Found" in error_msg or "No such object" in error_msg:
                logger.info(f"[info] No data found (404): {gcs_path}")
                return pd.DataFrame()

            if log_errors:
                logger.error(f"❌ GCS error loading from cloud: {e}")
            return pd.DataFrame()

    def download_from_category_bucket(
        self,
        gcs_path: str,
        category: str,
        format: str = "parquet",
    ) -> pd.DataFrame:
        """
        Download data from a category-specific bucket.

        Args:
            gcs_path: Path within the bucket
            category: Market category ('CEFI', 'TRADFI', 'DEFI')
            format: File format

        Returns:
            DataFrame with downloaded data
        """
        try:
            # Get bucket for category
            category_bucket = get_bucket_for_category(category, test_mode=self.is_test_mode)

            # Create cloud service for category bucket
            category_target = CloudTarget(
                project_id=self.cloud_target.project_id,
                gcs_bucket=category_bucket,
                bigquery_dataset=self.cloud_target.bigquery_dataset,
                bigquery_location=self.cloud_target.bigquery_location,
            )
            category_service = StandardizedDomainCloudService(
                domain=self.domain,
                cloud_target=category_target,
            )

            logger.info(f"📥 Loading {category} data from: {category_bucket}/{gcs_path}")
            result = category_service.download_from_gcs(
                gcs_path=gcs_path,
                format=format,
                log_errors=False,
            )
            df = result if isinstance(result, pd.DataFrame) else pd.DataFrame()

            if df.empty:
                logger.warning(f"⚠️ No {category} data found at {category_bucket}/{gcs_path}")
            else:
                logger.info(f"✅ Loaded {len(df)} {category} rows from GCS")

            return df

        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"❌ Data processing error loading {category} data: {e}")
            return pd.DataFrame()
        except Exception as e:
            error_msg = str(e)
            if "404" in error_msg or "Not Found" in error_msg or "No such object" in error_msg:
                logger.info(f"[info] No {category} data found (404): {gcs_path}")
                return pd.DataFrame()

            logger.error(f"❌ GCS error loading {category} data from cloud: {e}")
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
            logger.info(f"✅ Query returned {len(result)} rows")
            return result

        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"❌ Data processing error in BigQuery query: {e}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"❌ BigQuery query execution failed: {e}")
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
            logger.info(f"📤 Uploading {len(df)} rows to GCS: {gcs_path}")
            self.cloud_service.upload_to_gcs(
                data=df,
                gcs_path=gcs_path,
                format=format,
            )
            logger.info(f"✅ Upload complete: {gcs_path}")
            return True

        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"❌ Data processing error during GCS upload: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ GCS upload operation failed: {e}")
            return False

    def check_gcs_exists(self, gcs_path: str) -> bool:
        """
        Check if a GCS path exists.

        Args:
            gcs_path: Path within the bucket

        Returns:
            True if exists
        """
        # Use download_from_gcs which already handles GCS exceptions gracefully
        # and returns empty DataFrame for missing files
        df = self.download_from_gcs(gcs_path, log_errors=False)
        return not df.empty


def _resolve_instruments_bucket_cefi() -> str:
    """Resolve instruments CEFI bucket from config. Fails if not configured."""
    bucket = unified_config.instruments_gcs_bucket
    if bucket and str(bucket).strip():
        return str(bucket)
    proj = unified_config.gcp_project_id
    if not proj:
        raise ValueError(
            "Instruments GCS bucket or GCP project ID must be configured. "
            "Check unified_config.instruments_gcs_bucket and unified_config.gcp_project_id."
        )
    return f"instruments-store-cefi-{proj}"


class InstrumentsDataProvider(CloudDataProviderBase):
    """Data provider for instruments domain."""

    def get_provider_id(self) -> str:
        return "instruments"

    def __init__(self, cloud_target: CloudTarget | None = None):
        super().__init__(
            domain="instruments",
            cloud_target=cloud_target,
            gcs_bucket=_resolve_instruments_bucket_cefi(),
            bigquery_dataset=unified_config.instruments_bigquery_dataset,
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

        df = self.download_from_category_bucket(gcs_path, category) if category else self.download_from_gcs(gcs_path)

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

    def get_provider_id(self) -> str:
        return "market_data"

    def __init__(self, cloud_target: CloudTarget | None = None):
        super().__init__(
            domain="market_data",
            cloud_target=cloud_target,
            gcs_bucket=unified_config.market_data_gcs_bucket,
            bigquery_dataset=unified_config.market_data_bigquery_dataset,
        )

    def get_candles(
        self,
        instrument_id: str,
        timeframe: str,
        start_date: datetime,
        end_date: datetime,
        limit: int | None = None,
    ) -> pd.DataFrame:
        """
        Get candles from BigQuery.

        Args:
            instrument_id: Canonical instrument key
            timeframe: Candle timeframe ('1m', '5m', '1h', etc.)
            start_date: Start datetime
            end_date: End datetime
            limit: Maximum rows to return

        Returns:
            DataFrame with OHLCV data
        """

        # Ensure timezone-aware datetimes
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=UTC)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=UTC)

        # Build table name
        table_suffix = timeframe.replace("h", "h").replace("m", "m").replace("s", "s")
        table_name = f"candles_{table_suffix}_trades"

        dataset = self.cloud_target.bigquery_dataset
        project_id = self.cloud_target.project_id

        query = f"""
        SELECT *
        FROM `{project_id}.{dataset}.{table_name}`
        WHERE instrument_id = @instrument_id
          AND timestamp >= @start_time
          AND timestamp < @end_time
        ORDER BY timestamp ASC
        """

        params: dict[str, object] = {
            "instrument_id": instrument_id,
            "start_time": start_date.isoformat(),
            "end_time": end_date.isoformat(),
        }

        if limit and isinstance(limit, int) and limit > 0:
            query += " LIMIT @limit"
            params["limit"] = limit

        return self.query_bigquery(query, params)


class FeaturesDataProvider(CloudDataProviderBase):
    """Data provider for features domain."""

    def get_provider_id(self) -> str:
        return "features"

    def __init__(self, cloud_target: CloudTarget | None = None):
        super().__init__(
            domain="features",
            cloud_target=cloud_target,
            gcs_bucket=unified_config.features_gcs_bucket,
            bigquery_dataset=unified_config.features_bigquery_dataset,
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
