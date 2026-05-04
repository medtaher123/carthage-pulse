from pyspark.sql import SparkSession
from .utils.kafka_utils import SPARK_CONNECT_TARGET, read_from_kafka, RAW_EVENTS_KAFKA_TOPIC
from .utils.postgres_utils import write_to_postgres
from .utils.event_json_types import raw_event_json_schema
from .utils.spark_columns import get_raw_columns
from .utils.word_count_utils import extract_trending_words # Assuming you saved the transformer here


def main():
    # 1. Initialize Spark (This connects to the same cluster, but as a new job)
    spark = (
        SparkSession.builder
        .appName(f"{RAW_EVENTS_KAFKA_TOPIC}-trending-analytics")
        .remote(SPARK_CONNECT_TARGET)
        .getOrCreate()
    )
    print(f"Connected to Spark Connect. Starting trending analytics...")

    # 2. Read from Kafka (Independent read)
    raw_df = read_from_kafka(
        spark=spark,
        topic=RAW_EVENTS_KAFKA_TOPIC,
        json_schema=raw_event_json_schema,
        select_columns=get_raw_columns()
    )

    # 3. Transform the data into windowed word counts
    trending_words_df = extract_trending_words(raw_df)

    # 4. Write to Postgres
    query = write_to_postgres(
        df=trending_words_df,
        checkpoint_id="trending_words_job", # Unique checkpoint!
        db_table="trending_words_metrics",
        conflict_columns=["window_start", "window_end", "word"]
    )

    # 5. Await termination
    try:
        print(f"Trending words pipeline has started.")
        query.awaitTermination()
    except KeyboardInterrupt:
        print(f"Stopping trending words pipeline...")
        query.stop()