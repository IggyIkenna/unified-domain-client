"""AWS Glue catalog helper for Athena."""

from __future__ import annotations

import logging

from ..paths import get_spec

logger = logging.getLogger(__name__)


class GlueCatalog:
    """Generates AWS Glue table definitions for Athena queries."""

    @staticmethod
    def _parquet_storage_descriptor(s3_location: str) -> dict[str, object]:
        """Build the Glue StorageDescriptor for a Parquet-backed S3 location."""
        return {
            "Location": s3_location,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": (
                    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                ),
            },
            "Columns": [],
        }

    def create_table(
        self,
        dataset_name: str,
        account_id: str,
        category: str,
    ) -> dict[str, object]:
        """Generate an AWS Glue CreateTable input dict for a dataset.

        Args:
            dataset_name: Name in PATH_REGISTRY (e.g. "raw_tick_data")
            account_id: AWS account ID (used to build S3 bucket ARN)
            category: Bucket category qualifier (e.g. "crypto", "tradfi")

        Returns:
            Dict suitable for boto3 glue_client.create_table(**result).
        """
        spec = get_spec(dataset_name)
        bucket_name = spec.bucket_template.format(project_id=account_id, category=category)
        base_prefix = spec.path_template.split("{")[0].rstrip("/")
        s3_location = f"s3://{bucket_name}/{base_prefix}/"
        partition_keys = [{"Name": key, "Type": "string"} for key in spec.partition_keys]
        return {
            "DatabaseName": dataset_name,
            "TableInput": {
                "Name": dataset_name,
                "StorageDescriptor": self._parquet_storage_descriptor(s3_location),
                "PartitionKeys": partition_keys,
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {"classification": "parquet", "EXTERNAL": "TRUE"},
            },
        }
