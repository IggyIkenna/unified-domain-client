"""
Features domain client for accessing features data.

Provides convenience methods for querying:
- Delta-one features
- Volatility features
- On-chain features
- Calendar features
"""

from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd
from google.cloud import exceptions as gcs_exceptions
from unified_cloud_services import CloudTarget, unified_config

from unified_domain_services import StandardizedDomainCloudService

logger = logging.getLogger(__name__)


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

        except gcs_exceptions.NotFound as e:
            logger.error(f"❌ Features data not found: {e}")
            return pd.DataFrame()
        except gcs_exceptions.GoogleCloudError as e:
            logger.error(f"❌ GCS error loading features: {e}")
            return pd.DataFrame()
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"❌ Data processing error loading features: {e}")
            return pd.DataFrame()
