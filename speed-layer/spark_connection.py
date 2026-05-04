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



def get_postgres_writer(db_url: str, db_name: str, db_table: str, db_user: str, db_pass: str):
    """Returns a closure to be used in foreachBatch."""

    def write_to_postgres(batch_df: DataFrame, batch_id: int):
        import psycopg2

        print("-------------------------------------------")
        print(f"BATCH ID: {batch_id} | TABLE: {db_table}")
        count = batch_df.count()
        print(f"RECORDS RECEIVED: {count}")

        if count == 0:
            return

        staging_table = f"staging_events_{db_table}_{batch_id}"
        (
            batch_df.write
            .format("jdbc")
            .option("url", db_url)
            .option("dbtable", staging_table)
            .option("user", db_user)
            .option("password", db_pass)
            .option("driver", "org.postgresql.Driver")
            #.option("createTableColumnTypes", "entities JSONB")
            #.option("stringtype", "unspecified")
            .mode("overwrite")  # Overwrite the temp table each time
            .save()
        )
        # 2. Execute raw SQL to merge staging table into the target table, ignoring duplicates
        merge_query = f"""
                INSERT INTO {db_table}
                SELECT * FROM {staging_table}
                ON CONFLICT (event_id, timestamp) DO NOTHING;

                DROP TABLE {staging_table};
            """

        # Execute the merge query using psycopg2
        try:
            conn = psycopg2.connect(
                dbname=db_name,
                user=db_user,
                password=db_pass,
                host="postgres",
                port="5432"
            )
            cur = conn.cursor()
            cur.execute(merge_query)
            conn.commit()
            cur.close()
            conn.close()
            print(f"Successfully merged {count} from {staging_table} to {db_table} records and dropped staging table.")
        except Exception as e:
            print(f"Failed to merge batch {batch_id} from {staging_table} to {db_table}: {e}")

    return write_to_postgres


def run_kafka_to_postgres_stream(
        spark: SparkSession,
        kafka_broker: str,
        topic: str,
        json_schema: StructType,
        select_columns: List[Column],
        db_table: str,
        checkpoint_base_dir: str = "/data/checkpoints/spark_checkpoints_"
):
    """Reusable function to build and start the Kafka-to-Postgres streaming pipeline."""

    # Load Database credentials from environment variables
    db_url = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/reddit")
    db_name = os.getenv("POSTGRES_DATABASE", "reddit")
    db_user = os.getenv("POSTGRES_USER", "reddit")
    db_pass = os.getenv("POSTGRES_PASSWORD", "reddit")

    print(f"Reading from Kafka topic: {topic}...")

    # 1. Read from Kafka
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_broker)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    # 2. Parse JSON and Flatten
    parsed_df = df.selectExpr("CAST(value AS STRING) AS payload") \
        .withColumn("data", from_json(col("payload"), json_schema))

    flattened_df = parsed_df.select(*select_columns)

    # 3. Write to Postgres via foreachBatch
    writer_func = get_postgres_writer(db_url, db_name, db_table, db_user, db_pass)
    checkpoint_location = f"{checkpoint_base_dir}{topic}"

    query = (
        flattened_df
        #.dropDuplicates(["event_id"])
        .writeStream
        .outputMode("append")
        .foreachBatch(writer_func)
        .option("checkpointLocation", checkpoint_location)
        .start()
    )

    # 4. Await termination
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print(f"Stopping stream for topic {topic}...")
        query.stop()
