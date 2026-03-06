"""BigQuery external table catalog helper."""

from __future__ import annotations

import logging

from ..paths import build_bucket, get_spec

logger = logging.getLogger(__name__)


class BigQueryCatalog:
    """Generates BigQuery external table DDL for Hive-partitioned GCS datasets."""

    def create_external_table(
        self,
        dataset_name: str,
        project_id: str,
        category: str,
        bq_dataset: str,
    ) -> str:
        """
        Generate CREATE EXTERNAL TABLE DDL for a dataset.

        The table is backed by a Hive-partitioned GCS prefix so that BigQuery
        can auto-detect partition columns.

        Args:
            dataset_name: Name in PATH_REGISTRY (e.g. "raw_tick_data")
            project_id: GCP project ID
            category: Bucket category qualifier (e.g. "crypto", "tradfi")
            bq_dataset: BigQuery dataset to create the table in

        Returns:
            DDL string ready for execution via the BigQuery API.
        """
        spec = get_spec(dataset_name)
        bucket = build_bucket(dataset_name, project_id=project_id, category=category)

        # Derive the base prefix (up to the first partition key)
        base_prefix = spec.path_template.split("{")[0].rstrip("/")
        gcs_uri = f"gs://{bucket}/{base_prefix}/*"

        partition_cols = "\n    ".join(f"{key} STRING," for key in spec.partition_keys).rstrip(",")

        table_ref = f"`{project_id}.{bq_dataset}.{dataset_name}`"

        ddl = (
            f"CREATE OR REPLACE EXTERNAL TABLE {table_ref}\n"
            f"WITH PARTITION COLUMNS (\n"
            f"    {partition_cols}\n"
            f")\n"
            f"OPTIONS (\n"
            f"    format = 'PARQUET',\n"
            f"    uris = ['{gcs_uri}'],\n"
            f"    hive_partition_uri_prefix = 'gs://{bucket}/{base_prefix}'\n"
            f");"
        )
        return ddl
