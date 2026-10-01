"""Run a SQL query against Athena / the Glue Data Catalog and print the
result rows. Note: Athena query execution against LocalStack's community
edition is limited/emulated - this script works as-is against real AWS;
for local dev, query the same Parquet output directly with
`spark.read.parquet(...)` or DuckDB instead (see docs/ARCHITECTURE.md).

Usage:
    python -m aws.run_athena_query "SELECT * FROM market_pipeline.stock_enriched LIMIT 10" \
        --output-location s3://market-pipeline-mart/athena-results/
"""
from __future__ import annotations

import argparse
import time

from aws.clients import athena_client
from utils import config


def run_query(sql: str, output_location: str, poll_interval: float = 1.0, timeout: float = 60.0) -> list[dict]:
    athena = athena_client()
    exec_id = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": config.GLUE_DATABASE},
        ResultConfiguration={"OutputLocation": output_location},
    )["QueryExecutionId"]

    waited = 0.0
    while waited < timeout:
        status = athena.get_query_execution(QueryExecutionId=exec_id)["QueryExecution"]["Status"]["State"]
        if status in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(poll_interval)
        waited += poll_interval
    else:
        raise TimeoutError(f"Athena query {exec_id} did not finish within {timeout}s")

    if status != "SUCCEEDED":
        reason = athena.get_query_execution(QueryExecutionId=exec_id)["QueryExecution"]["Status"].get(
            "StateChangeReason", "unknown error"
        )
        raise RuntimeError(f"Athena query {exec_id} ended in {status}: {reason}")

    result = athena.get_query_results(QueryExecutionId=exec_id)
    rows = result["ResultSet"]["Rows"]
    if not rows:
        return []
    header = [col.get("VarCharValue") for col in rows[0]["Data"]]
    return [
        dict(zip(header, [col.get("VarCharValue") for col in row["Data"]]))
        for row in rows[1:]
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a SQL query against Athena")
    parser.add_argument("sql")
    parser.add_argument("--output-location", default=f"s3://{config.S3_BUCKET_MART}/athena-results/")
    args = parser.parse_args()

    for row in run_query(args.sql, args.output_location):
        print(row)


if __name__ == "__main__":
    main()
