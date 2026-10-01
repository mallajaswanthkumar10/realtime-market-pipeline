"""One-shot setup: S3 buckets (raw/processed/mart), seeds the reference
data the streaming job enriches against, and registers the Glue Data
Catalog database + tables so Athena can query the partitioned output.

Run against LocalStack (default) or real AWS:

    python -m aws.bootstrap_aws
"""
from __future__ import annotations

import logging

from botocore.exceptions import ClientError

from aws.clients import glue_client, s3_client
from utils import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bootstrap_aws")

BUCKETS = [config.S3_BUCKET_RAW, config.S3_BUCKET_PROCESSED, config.S3_BUCKET_MART]


def create_buckets() -> None:
    s3 = s3_client()
    for bucket in BUCKETS:
        try:
            if config.AWS_REGION == "us-east-1":
                s3.create_bucket(Bucket=bucket)
            else:
                s3.create_bucket(
                    Bucket=bucket,
                    CreateBucketConfiguration={"LocationConstraint": config.AWS_REGION},
                )
            log.info("created bucket %s", bucket)
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                log.info("bucket %s already exists", bucket)
            else:
                raise


def seed_reference_data() -> None:
    s3 = s3_client()
    s3.upload_file("data/reference/symbols.csv", config.S3_BUCKET_RAW, "reference/symbols.csv")
    log.info("uploaded reference/symbols.csv to s3://%s", config.S3_BUCKET_RAW)


def _ensure_database(glue) -> None:
    try:
        glue.create_database(DatabaseInput={"Name": config.GLUE_DATABASE})
        log.info("created Glue database %s", config.GLUE_DATABASE)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "AlreadyExistsException":
            log.info("Glue database %s already exists", config.GLUE_DATABASE)
        else:
            raise


def _table_input(name: str, s3_path: str, columns: list[dict]) -> dict:
    return {
        "Name": name,
        "StorageDescriptor": {
            "Columns": columns,
            "Location": s3_path,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
            },
        },
        "PartitionKeys": [
            {"Name": "date", "Type": "string"},
            {"Name": "hour", "Type": "int"},
        ],
        "TableType": "EXTERNAL_TABLE",
    }


def create_glue_tables() -> None:
    glue = glue_client()
    _ensure_database(glue)

    tables = [
        _table_input(
            "stock_window_aggregates",
            f"s3://{config.S3_BUCKET_MART}/stock_window_aggregates/",
            [
                {"Name": "symbol", "Type": "string"},
                {"Name": "window_start", "Type": "timestamp"},
                {"Name": "window_end", "Type": "timestamp"},
                {"Name": "mean_price", "Type": "double"},
                {"Name": "stddev_price", "Type": "double"},
                {"Name": "min_price", "Type": "double"},
                {"Name": "max_price", "Type": "double"},
                {"Name": "event_count", "Type": "bigint"},
            ],
        ),
        _table_input(
            "stock_enriched",
            f"s3://{config.S3_BUCKET_PROCESSED}/stock_enriched/",
            [
                {"Name": "event_id", "Type": "string"},
                {"Name": "symbol", "Type": "string"},
                {"Name": "price", "Type": "double"},
                {"Name": "event_time", "Type": "timestamp"},
                {"Name": "mean_price", "Type": "double"},
                {"Name": "stddev_price", "Type": "double"},
                {"Name": "zscore", "Type": "double"},
                {"Name": "is_anomaly", "Type": "boolean"},
                {"Name": "sector", "Type": "string"},
                {"Name": "exchange", "Type": "string"},
                {"Name": "avg_52w_price", "Type": "string"},
            ],
        ),
    ]

    for table in tables:
        try:
            glue.create_table(DatabaseName=config.GLUE_DATABASE, TableInput=table)
            log.info("created Glue table %s", table["Name"])
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "AlreadyExistsException":
                log.info("Glue table %s already exists", table["Name"])
            else:
                raise


def main() -> None:
    create_buckets()
    seed_reference_data()
    create_glue_tables()
    log.info("AWS bootstrap complete")


if __name__ == "__main__":
    main()
