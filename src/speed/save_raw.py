from pyspark.sql import SparkSession
from .utils.kafka_utils import SPARK_CONNECT_TARGET, read_from_kafka, RAW_EVENTS_KAFKA_TOPIC
from .utils.postgres_utils import write_to_postgres
from .utils.event_json_types import raw_event_json_schema
from .utils.spark_columns import get_raw_columns


def main(max_runtime: int | None = None):
    spark = (
        SparkSession.builder
        .appName(RAW_EVENTS_KAFKA_TOPIC)
        .remote(SPARK_CONNECT_TARGET)
        .getOrCreate()
    )
    print(f"Connected to Spark Connect Server at {SPARK_CONNECT_TARGET}.")

    streaming_df = read_from_kafka(
        spark=spark,
        topic=RAW_EVENTS_KAFKA_TOPIC,
        json_schema=raw_event_json_schema,
        select_columns=get_raw_columns()
    )

    query = write_to_postgres(
        df=streaming_df,
        checkpoint_id=f"to_raw_events",
        db_table="raw_events"
    )

    # 3. Await termination – stop after max_runtime so Airflow can restart us.
    timeout_ms = max_runtime * 1000 if max_runtime else None
    try:
        print(f"Streaming pipeline for {RAW_EVENTS_KAFKA_TOPIC} has started.")
        if timeout_ms:
            query.awaitTermination(timeout_ms)
            print("Max runtime reached, cleanly stopping raw-events stream.")
            query.stop()
        else:
            query.awaitTermination()
    except KeyboardInterrupt:
        print(f"Stopping stream for topic {RAW_EVENTS_KAFKA_TOPIC}...")
        query.stop()
