import os
import socket

import time

from typing import List
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType

KAFKA_BROKER = "kafka:9093"  # Internal Docker network address for Kafka.
SPARK_CONNECT_HOST = os.getenv("SPARK_CONNECT_HOST", "localhost")
SPARK_CONNECT_PORT = int(os.getenv("SPARK_CONNECT_PORT", "15002"))
SHARED_SESSION_ID = "123e4567-e89b-12d3-a456-426614174000" # this has to be a valid UUID
SPARK_CONNECT_TARGET = f"sc://{SPARK_CONNECT_HOST}:{SPARK_CONNECT_PORT}/;session_id={SHARED_SESSION_ID}"

RAW_EVENTS_KAFKA_TOPIC = "reddit-events"
ENRICHED_EVENTS_KAFKA_TOPIC = "reddit-events-enriched"


def wait_for_port(host: str, port: int, timeout_seconds: int = 30) -> None:
    """Fail fast with a clear message if Spark Connect is not reachable yet."""
    deadline = time.time() + timeout_seconds
    last_error: Exception | None = None

    while time.time() < deadline:
        try:
            print('Connecting to Spark Connect Server... ')
            with socket.create_connection((host, port), timeout=2):
                return
        except OSError as exc:
            last_error = exc
            time.sleep(1)

    raise RuntimeError(
        f"Spark Connect is not reachable at {host}:{port} after {timeout_seconds}s. "
        f"Start the spark-connect service first, or set SPARK_CONNECT_HOST if you are running inside Docker. "
        f"Last error: {last_error}"
    )


def read_from_kafka(spark, topic: str, json_schema, select_columns: List[Column]):

    print(f"Reading from Kafka topic: {topic}...")

    # 1. Read from Kafka
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    # 2. Parse JSON and Flatten
    parsed_df = df.selectExpr("CAST(value AS STRING) AS payload") \
        .withColumn("data", from_json(col("payload"), json_schema))

    return parsed_df.select(*select_columns)

