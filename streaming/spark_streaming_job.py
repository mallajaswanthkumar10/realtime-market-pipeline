"""PySpark Structured Streaming job: the core of the pipeline.

    Kafka(stock.raw) --> parse/validate --> watermark
        |
        +--> [5-min tumbling window aggregates] --> S3 mart layer (partitioned by date/hour)
        |
        +--> foreachBatch:
                 - compute window stats for the batch
                 - flag anomalies (|price - mean| > 2*stddev)
                 - enrich with S3 reference data (broadcast join)
                 - write enriched rows -> S3 processed layer (partitioned by date/hour)
                 - publish anomalies -> Kafka `alerts` topic

Exactly-once / idempotent writes
---------------------------------
Spark's `foreachBatch` is invoked with a deterministic `batch_id` that is
stable across retries of the *same* micro-batch (checkpointing + a
replayable source like Kafka guarantee this). We use that batch_id to name
each batch's output files deterministically and overwrite-by-batch rather
than blind-append, so re-executing a failed batch never produces
duplicate rows in S3 - this is the standard Spark recipe for idempotent
sinks that aren't natively transactional.

Run (requires Java 11/17 + the packages below, or run inside the provided
Docker image - see docs/ARCHITECTURE.md):

    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        streaming/spark_streaming_job.py
"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from streaming import enrichment
from streaming.anomaly_detection import tag_anomalies_df
from streaming.schemas import RAW_EVENT_SCHEMA
from utils import config
from utils.kafka_helpers import build_producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("spark_streaming_job")


def build_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("realtime-market-pipeline")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.streaming.schemaInference", "false")
        # dynamic allocation lets the cluster scale executors with load,
        # which combined with Kafka consumer-group partition reassignment
        # is how this job "auto-scales" under 10k+ msg/sec bursts.
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.minExecutors", "1")
        .config("spark.dynamicAllocation.maxExecutors", "10")
    )

    if config.AWS_ENDPOINT_URL:
        # Point the S3A connector at LocalStack for local dev.
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", config.AWS_ENDPOINT_URL)
            .config("spark.hadoop.fs.s3a.access.key", "test")
            .config("spark.hadoop.fs.s3a.secret.key", "test")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        )

    return builder.getOrCreate()


def read_raw_stream(spark: SparkSession) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", config.TOPIC_STOCK_RAW)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = raw.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.from_json(F.col("value").cast("string"), RAW_EVENT_SCHEMA).alias("data"),
    ).select("kafka_key", "data.*")

    # Drop rows that failed schema parsing instead of crashing the job -
    # a basic data-quality gate on streaming input.
    valid = parsed.filter(F.col("event_id").isNotNull() & F.col("event_time").isNotNull())

    return valid.withWatermark("event_time", config.WATERMARK_DELAY)


def start_windowed_aggregates_sink(events: DataFrame) -> StreamingQuery:
    """Query A: stateful 5-minute tumbling-window aggregates, written
    continuously to the mart layer, partitioned by date/hour."""
    windowed = events.groupBy(
        F.window(F.col("event_time"), config.WINDOW_DURATION), F.col("symbol")
    ).agg(
        F.avg("price").alias("mean_price"),
        F.stddev("price").alias("stddev_price"),
        F.min("price").alias("min_price"),
        F.max("price").alias("max_price"),
        F.count("*").alias("event_count"),
    )

    out = windowed.select(
        "symbol",
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "mean_price",
        "stddev_price",
        "min_price",
        "max_price",
        "event_count",
        F.to_date("window.start").alias("date"),
        F.hour("window.start").alias("hour"),
    )

    mart_path = f"s3a://{config.S3_BUCKET_MART}/stock_window_aggregates"
    return (
        out.writeStream.outputMode("update")
        .format("parquet")
        .option("path", mart_path)
        .option("checkpointLocation", f"{config.CHECKPOINT_LOCATION}_mart")
        .partitionBy("date", "hour")
        .trigger(processingTime="30 seconds")
        .start()
    )


def make_batch_processor(spark: SparkSession, reference_df: DataFrame):
    """Returns the foreachBatch function (Query B): per-micro-batch
    enrichment, anomaly tagging, idempotent S3 write, and alert publish."""

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return

        batch_df = batch_df.persist()
        try:
            windowed_key = batch_df.withColumn(
                "window", F.window(F.col("event_time"), config.WINDOW_DURATION)
            )

            window_stats = windowed_key.groupBy("symbol", "window").agg(
                F.avg("price").alias("mean_price"), F.stddev("price").alias("stddev_price")
            )

            tagged = tag_anomalies_df(windowed_key, window_stats)
            enriched = enrichment.enrich_with_reference(tagged, reference_df)

            enriched = enriched.withColumn("date", F.to_date("event_time")).withColumn(
                "hour", F.hour("event_time")
            )

            # Idempotent write: deterministic path per batch_id means a
            # retried batch overwrites its own prior (partial) output
            # instead of appending duplicates.
            processed_path = f"s3a://{config.S3_BUCKET_PROCESSED}/stock_enriched/batch_id={batch_id}"
            (
                enriched.drop("window")
                .write.mode("overwrite")
                .partitionBy("date", "hour")
                .parquet(processed_path)
            )

            anomalies = enriched.filter(F.col("is_anomaly") == True)  # noqa: E712
            anomaly_count = anomalies.count()
            if anomaly_count:
                log.info("batch %s: publishing %d anomalies to %s", batch_id, anomaly_count, config.TOPIC_ALERTS)
                _publish_alerts(anomalies)
            else:
                log.info("batch %s: %d rows processed, no anomalies", batch_id, batch_df.count())
        finally:
            batch_df.unpersist()

    return process_batch


def _publish_alerts(anomalies: DataFrame) -> None:
    def publish_partition(rows):
        producer = build_producer()
        try:
            for row in rows:
                alert = {
                    "event_id": row["event_id"],
                    "symbol": row["symbol"],
                    "price": row["price"],
                    "window_mean": row["mean_price"],
                    "window_stddev": row["stddev_price"],
                    "zscore": row["zscore"],
                    "event_time": str(row["event_time"]),
                    "alert_type": "price_zscore_anomaly",
                }
                producer.send(config.TOPIC_ALERTS, value=alert, key=row["symbol"])
        finally:
            producer.flush()
            producer.close()

    cols = ["event_id", "symbol", "price", "mean_price", "stddev_price", "zscore", "event_time"]
    anomalies.select(*cols).foreachPartition(publish_partition)


def main() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    reference_df = enrichment.load_reference_data(
        spark, f"s3a://{config.S3_BUCKET_RAW}/reference/symbols.csv"
    )

    events = read_raw_stream(spark)

    agg_query = start_windowed_aggregates_sink(events)

    enriched_query = (
        events.writeStream.foreachBatch(make_batch_processor(spark, reference_df))
        .option("checkpointLocation", f"{config.CHECKPOINT_LOCATION}_enriched")
        .trigger(processingTime="10 seconds")
        .start()
    )

    log.info("streaming job started - window query id=%s, enrich query id=%s", agg_query.id, enriched_query.id)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
